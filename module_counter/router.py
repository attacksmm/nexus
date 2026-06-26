from __future__ import annotations

import csv
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

_db_path: str | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None

MODULE_ID = "counter"
SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
SYSTEM_FIELDS = {"id", "platform_id", "created_at", "updated_at"}
BASE_EXPORT_COLUMNS = ["table", "id", "platform_id", "created_at", "updated_at"]
MAX_QUERY_LIMIT = 1000
MAX_EXPORT_ROWS = 250_000


class ConditionIn(BaseModel):
    field: str = ""
    op: str = "contains"
    value: Any = ""
    value2: Any = ""


class QueryIn(BaseModel):
    tables: list[str] = Field(default_factory=list)
    mode: str = "and"
    conditions: list[ConditionIn] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    raw_json: bool = False
    limit: int = 100
    offset: int = 0


class PresetIn(BaseModel):
    name: str
    description: str = ""
    payload: QueryIn


for _model in (ConditionIn, QueryIn, PresetIn):
    if hasattr(_model, "model_rebuild"):
        _model.model_rebuild()


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = ctx.db_path
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.counter"))
    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db() -> None:
    if not _db_path:
        return
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_presets_updated ON presets(updated_at DESC);
            """
        )
        await db.commit()
    _log("info", "counter initialized")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _customer_db_path() -> Path:
    env_path = os.environ.get("COUNTER_CUSTOMER_DB_PATH", "").strip()
    if env_path:
        return Path(env_path)
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    candidates = [
        _module_dir.parent / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent / "module_customer_db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


def _check_table(name: str) -> str:
    name = str(name or "").strip()
    if not SAFE_NAME.match(name):
        raise HTTPException(400, f"Некорректный лист: {name}")
    return name


async def _customer_tables() -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    result = []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT name, display_name, description, schema_json FROM _cdb_tables ORDER BY id")
        rows = [dict(row) for row in await cur.fetchall()]
        for row in rows:
            name = str(row["name"])
            if not SAFE_NAME.match(name):
                continue
            try:
                count = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{name}")).fetchone())[0]
            except Exception:
                count = 0
            result.append({
                "name": name,
                "display_name": row.get("display_name") or name,
                "description": row.get("description") or "",
                "schema_json": _json_loads(row.get("schema_json"), []),
                "count": count,
            })
    return result


def _json_loads(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _clean_field(field: str) -> str:
    return str(field or "").strip().strip(".")[:300]


def _flatten_for_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        text = str(value).strip().replace(" ", "").replace(",", ".")
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _path_values(data: Any, parts: list[str]) -> list[Any]:
    if not parts:
        return _to_list(data)
    if isinstance(data, list):
        values: list[Any] = []
        for item in data:
            values.extend(_path_values(item, parts))
        return values
    if not isinstance(data, dict):
        return []
    key = parts[0]
    if key.endswith("[]"):
        key = key[:-2]
        value = data.get(key)
        values = value if isinstance(value, list) else []
        if len(parts) == 1:
            return values
        result: list[Any] = []
        for item in values:
            result.extend(_path_values(item, parts[1:]))
        return result
    if key not in data:
        return []
    return _path_values(data.get(key), parts[1:])


def _value_for_field(record: dict[str, Any], field: str) -> list[Any]:
    field = _clean_field(field)
    if not field:
        return []
    if field in SYSTEM_FIELDS or field == "table":
        return [record.get(field)]
    return _path_values(record.get("custom_fields") or {}, field.split("."))


def _field_matches(record: dict[str, Any], condition: ConditionIn) -> bool:
    field = _clean_field(condition.field)
    op = str(condition.op or "").strip().lower()
    values = _value_for_field(record, field)
    expected = condition.value
    expected2 = condition.value2

    if op in {"empty", "is_empty", "пусто"}:
        return not values or all(_is_empty(value) for value in values)
    if op in {"not_empty", "is_not_empty", "не пусто"}:
        return any(not _is_empty(value) for value in values)

    if not values:
        values = [None]

    expected_text = _flatten_for_text(expected).lower()
    expected_items = [
        item.strip().lower()
        for item in re.split(r"[\n,;]+", _flatten_for_text(expected))
        if item.strip()
    ]
    expected_num = _to_number(expected)
    expected2_num = _to_number(expected2)

    if op in {"neq", "not_equals", "!=", "не равно"}:
        return all(_flatten_for_text(raw).lower() != expected_text for raw in values)
    if op in {"not_contains", "не содержит"}:
        return all(expected_text not in _flatten_for_text(raw).lower() for raw in values)

    for raw in values:
        text = _flatten_for_text(raw)
        low = text.lower()
        if op in {"eq", "equals", "=", "равно"} and low == expected_text:
            return True
        if op in {"contains", "содержит"} and expected_text in low:
            return True
        if op in {"starts", "starts_with", "начинается с"} and low.startswith(expected_text):
            return True
        if op in {"ends", "ends_with", "заканчивается на"} and low.endswith(expected_text):
            return True
        if op in {"in", "in_list", "список"} and low in expected_items:
            return True
        if op in {"gt", ">", "больше"}:
            num = _to_number(raw)
            if num is not None and expected_num is not None and num > expected_num:
                return True
        if op in {"lt", "<", "меньше"}:
            num = _to_number(raw)
            if num is not None and expected_num is not None and num < expected_num:
                return True
        if op in {"between", "между"}:
            num = _to_number(raw)
            if num is not None and expected_num is not None and expected2_num is not None and expected_num <= num <= expected2_num:
                return True
    return False


def _matches(record: dict[str, Any], conditions: list[ConditionIn], mode: str) -> bool:
    active = [condition for condition in conditions if _clean_field(condition.field)]
    if not active:
        return True
    if str(mode or "and").lower() == "or":
        return any(_field_matches(record, condition) for condition in active)
    return all(_field_matches(record, condition) for condition in active)


def _record_from_row(table: str, row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "table": table,
        "id": row["id"],
        "platform_id": row["platform_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "custom_fields": _json_loads(row["custom_fields"], {}),
    }


async def _iter_matching_rows(query: QueryIn):
    table_names = [_check_table(table) for table in query.tables]
    if not table_names:
        table_names = [row["name"] for row in await _customer_tables()]
    db_path = _customer_db_path()
    if not db_path.exists():
        return
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        known = {row["name"] for row in await _customer_tables()}
        for table in table_names:
            if table not in known:
                continue
            async with db.execute(f"SELECT * FROM cdb_{table} ORDER BY id DESC") as cur:
                async for row in cur:
                    record = _record_from_row(table, row)
                    if _matches(record, query.conditions, query.mode):
                        yield record


def _project_record(record: dict[str, Any], columns: list[str], raw_json: bool = False) -> dict[str, Any]:
    out = {key: record.get(key) for key in BASE_EXPORT_COLUMNS}
    for field in columns:
        field = _clean_field(field)
        if not field or field in out:
            continue
        values = _value_for_field(record, field)
        if not values:
            out[field] = ""
        elif len(values) == 1:
            out[field] = _flatten_for_text(values[0])
        else:
            out[field] = json.dumps(values, ensure_ascii=False)
    if raw_json:
        out["raw_json"] = json.dumps(record.get("custom_fields") or {}, ensure_ascii=False)
    return out


def _walk_fields(value: Any, prefix: str = ""):
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield path
            yield from _walk_fields(item, path)
    elif isinstance(value, list):
        if prefix:
            yield prefix + "[]"
        for item in value[:5]:
            if isinstance(item, dict):
                yield from _walk_fields(item, prefix + "[]" if prefix else "[]")


def _model_payload(model: QueryIn) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID}


@router.get("/tables")
async def list_tables(request: Request):
    await _require_panel_user(request)
    return {"items": await _customer_tables(), "customer_db_path": str(_customer_db_path())}


@router.get("/fields")
async def list_fields(request: Request, tables: str = "", sample: int = 200):
    await _require_panel_user(request)
    sample = max(10, min(1000, int(sample)))
    selected = [_check_table(item) for item in tables.split(",") if item.strip()]
    if not selected:
        selected = [row["name"] for row in await _customer_tables()]
    known = {row["name"] for row in await _customer_tables()}
    counter: dict[str, int] = {field: 10_000 for field in BASE_EXPORT_COLUMNS}
    db_path = _customer_db_path()
    if db_path.exists():
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            for table in selected:
                if table not in known:
                    continue
                async with db.execute(f"SELECT custom_fields FROM cdb_{table} ORDER BY id DESC LIMIT ?", (sample,)) as cur:
                    async for row in cur:
                        fields = set(_walk_fields(_json_loads(row["custom_fields"], {})))
                        for field in fields:
                            counter[field] = counter.get(field, 0) + 1
    fields = [{"name": name, "count": count if count < 10_000 else None} for name, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))]
    return {"items": fields[:500]}


@router.post("/query")
async def query_records(data: QueryIn, request: Request):
    await _require_panel_user(request)
    data.limit = max(1, min(MAX_QUERY_LIMIT, int(data.limit or 100)))
    data.offset = max(0, int(data.offset or 0))
    total = 0
    per_table: dict[str, int] = {}
    items = []
    async for record in _iter_matching_rows(data):
        total += 1
        per_table[record["table"]] = per_table.get(record["table"], 0) + 1
        if total > data.offset and len(items) < data.limit:
            items.append(_project_record(record, data.columns, data.raw_json))
    return {"total": total, "per_table": per_table, "items": items, "limit": data.limit, "offset": data.offset}


@router.post("/export.csv")
async def export_csv(data: QueryIn, request: Request):
    await _require_panel_user(request)
    columns = []
    for column in data.columns:
        column = _clean_field(column)
        if column and column not in columns and column not in BASE_EXPORT_COLUMNS:
            columns.append(column)
    header = BASE_EXPORT_COLUMNS + columns + (["raw_json"] if data.raw_json else [])
    fd, tmp_name = tempfile.mkstemp(prefix="counter-export-", suffix=".csv")
    os.close(fd)
    count = 0
    with open(tmp_name, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        async for record in _iter_matching_rows(data):
            if count >= MAX_EXPORT_ROWS:
                break
            writer.writerow(_project_record(record, columns, data.raw_json))
            count += 1
    filename = f"counter-export-{count}.csv"
    return FileResponse(tmp_name, filename=filename, media_type="text/csv; charset=utf-8")


@router.get("/presets")
async def list_presets(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM presets ORDER BY updated_at DESC, id DESC")
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["payload"] = _json_loads(row.pop("payload_json", "{}"), {})
    return {"items": rows}


@router.post("/presets")
async def create_preset(data: PresetIn, request: Request):
    await _require_panel_user(request)
    name = str(data.name or "").strip()[:200]
    if not name:
        raise HTTPException(400, "Название обязательно")
    payload = _model_payload(data.payload)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        cur = await db.execute(
            "INSERT INTO presets(name,description,payload_json) VALUES(?,?,?)",
            (name, str(data.description or "").strip()[:1000], json.dumps(payload, ensure_ascii=False)),
        )
        await db.commit()
        preset_id = int(cur.lastrowid)
    return {"ok": True, "id": preset_id}


@router.put("/presets/{preset_id}")
async def update_preset(preset_id: int, data: PresetIn, request: Request):
    await _require_panel_user(request)
    name = str(data.name or "").strip()[:200]
    if not name:
        raise HTTPException(400, "Название обязательно")
    payload = _model_payload(data.payload)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            "UPDATE presets SET name=?, description=?, payload_json=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (name, str(data.description or "").strip()[:1000], json.dumps(payload, ensure_ascii=False), int(preset_id)),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/presets/{preset_id}")
async def delete_preset(preset_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute("DELETE FROM presets WHERE id=?", (int(preset_id),))
        await db.commit()
    return {"ok": True}


@router.get("/stats")
async def stats(request: Request):
    await _require_panel_user(request)
    tables = await _customer_tables()
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        presets = (await (await db.execute("SELECT COUNT(*) FROM presets")).fetchone())[0]
    return {
        "tables": len(tables),
        "rows": sum(int(table.get("count") or 0) for table in tables),
        "presets": presets,
        "customer_db_path": str(_customer_db_path()),
    }
