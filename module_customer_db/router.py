"""
customer-db v2.0.0
Структура: id, platform_id, custom_fields (JSON)
Поддерживает несколько именованных таблиц.
"""
import json
import logging
import re

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter()
_db_path = None
_logger: logging.Logger | None = None

SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
PLACEHOLDER_VALUE = re.compile(r"^#\{[^{}]+\}$")
PROTECTED_ATTRIBUTION_KEYS = {"yclid", "ym_uid", "_ym_uid"}


def setup(ctx):
    global _db_path, _logger
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.customer-db"))
    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS _cdb_tables (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT DEFAULT '',
                schema_json TEXT NOT NULL DEFAULT '[]',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
        """)
        # создаём таблицу по умолчанию если нет ни одной
        cur = await db.execute("SELECT COUNT(*) FROM _cdb_tables")
        (cnt,) = await cur.fetchone()
        if cnt == 0:
            await db.execute(
                "INSERT INTO _cdb_tables (name, display_name, description) VALUES (?, ?, ?)",
                ("default", "Основная", "Таблица клиентов по умолчанию"),
            )
            await db.execute(_create_table_sql("default"))
        cur = await db.execute("SELECT name FROM _cdb_tables ORDER BY id")
        for (name,) in await cur.fetchall():
            if SAFE_NAME.match(name):
                await db.execute(_create_table_sql(name))
                await db.execute(_create_index_sql(name))
        await db.commit()


def _create_table_sql(name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS cdb_{name} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            platform_id TEXT NOT NULL DEFAULT '',
            custom_fields TEXT NOT NULL DEFAULT '{{}}',
            created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
    """


def _create_index_sql(name: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS idx_cdb_{name}_platform_id ON cdb_{name} (platform_id);"


def _log(level: str, message: str, *args):
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _record_log_payload(
    action: str,
    table: str,
    status: str,
    record_id: int | None,
    platform_id: str,
    custom_fields: dict,
    extra: dict | None = None,
) -> str:
    payload = {
        "event": "customer_db_record",
        "action": action,
        "table": table,
        "status": status,
        "record_id": record_id,
        "platform_id": platform_id,
        "custom_keys": sorted(custom_fields.keys())[:30],
    }
    if custom_fields.get("platform"):
        payload["platform"] = custom_fields.get("platform")
    if custom_fields.get("salebot_id"):
        payload["salebot_id"] = custom_fields.get("salebot_id")
    if isinstance(custom_fields.get("utms"), dict):
        payload["utm_keys"] = sorted(custom_fields["utms"].keys())[:20]
    if extra:
        payload.update(extra)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _log_record_event(
    action: str,
    table: str,
    status: str,
    record_id: int | None,
    platform_id: str,
    custom_fields: dict,
    extra: dict | None = None,
):
    _log("info", _record_log_payload(action, table, status, record_id, platform_id, custom_fields, extra))


def _check_name(name: str):
    if not SAFE_NAME.match(name):
        raise HTTPException(400, "Имя таблицы: только латинские буквы, цифры, _. Начинается с буквы.")


def _is_empty_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or text.lower() in {"none", "null", "undefined"} or bool(PLACEHOLDER_VALUE.match(text))
    return False


def _clean_json_value(value):
    if _is_empty_value(value):
        return None
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if _is_empty_value(key):
                continue
            cleaned_item = _clean_json_value(item)
            if cleaned_item is not None:
                cleaned[str(key)] = cleaned_item
        return cleaned or None
    if isinstance(value, list):
        cleaned_items = []
        for item in value:
            cleaned_item = _clean_json_value(item)
            if cleaned_item is not None:
                cleaned_items.append(cleaned_item)
        return cleaned_items or None
    return value


def _clean_record(data: "RecordIn") -> "RecordIn":
    platform_id = str(data.platform_id or "").strip()
    cleaned = _clean_json_value(data.custom_fields or {})
    return RecordIn(platform_id=platform_id, custom_fields=cleaned if isinstance(cleaned, dict) else {})


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _is_protected_attribution_key(key: str) -> bool:
    normalized = str(key).strip().lower()
    return normalized.startswith("utm_") or normalized in PROTECTED_ATTRIBUTION_KEYS


def _deep_merge(existing: dict, incoming: dict) -> dict:
    result = dict(existing or {})
    for key, value in (incoming or {}).items():
        if _is_protected_attribution_key(key) and not _is_empty_value(result.get(key)):
            continue
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


async def _upsert_one(db, table: str, data: "RecordIn") -> tuple[int, str, dict, int]:
    record = _clean_record(data)
    if not record.platform_id:
        raise HTTPException(400, "platform_id обязателен для upsert")

    tbl = f"cdb_{table}"
    await db.execute(_create_index_sql(table))
    cur = await db.execute(
        f"SELECT id, custom_fields FROM {tbl} WHERE platform_id = ? ORDER BY id ASC",
        (record.platform_id,),
    )
    rows = await cur.fetchall()
    if rows:
        record_id = int(rows[0][0])
        stored_fields = _json_dict(rows[0][1])
        duplicate_ids = []
        for row in rows[1:]:
            duplicate_ids.append(int(row[0]))
            stored_fields = _deep_merge(stored_fields, _json_dict(row[1]))
        stored_fields = _deep_merge(stored_fields, record.custom_fields)
        custom_json = json.dumps(stored_fields, ensure_ascii=False)
        await db.execute(
            f"UPDATE {tbl} SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (custom_json, record_id),
        )
        for duplicate_id in duplicate_ids:
            await db.execute(f"DELETE FROM {tbl} WHERE id=?", (duplicate_id,))
        return record_id, "updated", stored_fields, len(duplicate_ids)

    custom_json = json.dumps(record.custom_fields, ensure_ascii=False)
    cur = await db.execute(
        f"INSERT INTO {tbl} (platform_id, custom_fields) VALUES (?, ?)",
        (record.platform_id, custom_json),
    )
    return int(cur.lastrowid), "created", record.custom_fields, 0


# ── Tables management ─────────────────────────────────────────────────────────

@router.get("/tables")
async def list_tables():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM _cdb_tables ORDER BY id")
        rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        r["schema_json"] = json.loads(r["schema_json"])
    return rows


class TableIn(BaseModel):  # type: ignore[override]
    name: str
    display_name: str
    description: str = ""
    schema_json: list = []


@router.post("/tables", status_code=201)
async def create_table(data: TableIn):
    _check_name(data.name)
    async with aiosqlite.connect(_db_path) as db:
        try:
            await db.execute(
                "INSERT INTO _cdb_tables (name, display_name, description, schema_json) VALUES (?,?,?,?)",
                (data.name, data.display_name, data.description, json.dumps(data.schema_json)),
            )
            await db.execute(_create_table_sql(data.name))
            await db.execute(_create_index_sql(data.name))
            await db.commit()
        except Exception as e:
            raise HTTPException(409, f"Таблица уже существует: {e}")
    _log("info", 'customer_db_table action=create table=%s display_name=%s', data.name, data.display_name)
    return {"ok": True, "name": data.name}


@router.delete("/tables/{name}")
async def delete_table(name: str):
    _check_name(name)
    if name == "default":
        raise HTTPException(400, "Нельзя удалить таблицу default")
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM _cdb_tables WHERE name = ?", (name,))
        await db.execute(f"DROP TABLE IF EXISTS cdb_{name}")
        await db.commit()
    _log("info", 'customer_db_table action=delete table=%s', name)
    return {"ok": True}


# ── Records ───────────────────────────────────────────────────────────────────

@router.get("/tables/{table}/records")
async def list_records(
    table: str,
    q: str = Query("", alias="q"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        tbl = f"cdb_{table}"
        if q:
            pat = f"%{q}%"
            cur = await db.execute(
                f"SELECT * FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (pat, pat, limit, offset),
            )
            cnt = await db.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ?",
                (pat, pat),
            )
        else:
            cur = await db.execute(f"SELECT * FROM {tbl} ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
            cnt = await db.execute(f"SELECT COUNT(*) FROM {tbl}")
        rows = [dict(r) for r in await cur.fetchall()]
        (total,) = await cnt.fetchone()

    for r in rows:
        try:
            r["custom_fields"] = json.loads(r["custom_fields"])
        except Exception:
            r["custom_fields"] = {}
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


class RecordIn(BaseModel):
    platform_id: str = ""
    custom_fields: dict = {}


class BatchRecordsIn(BaseModel):
    records: list[RecordIn] = Field(default_factory=list)
    dry_run: bool = False


@router.post("/tables/{table}/records", status_code=201)
async def create_record(table: str, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        record_id, status, stored_fields, deduped = await _upsert_one(db, table, record)
        await db.commit()
    _log_record_event("create", table, status, record_id, record.platform_id, record.custom_fields, {"deduped": deduped})
    return {
        "ok": True,
        "id": record_id,
        "status": status,
        "platform_id": record.platform_id,
        "custom_fields": stored_fields,
    }


@router.post("/tables/{table}/records/upsert")
async def upsert_record(table: str, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        record_id, status, stored_fields, deduped = await _upsert_one(db, table, record)
        await db.commit()
    _log_record_event("upsert", table, status, record_id, record.platform_id, record.custom_fields, {"deduped": deduped})
    return {
        "ok": True,
        "id": record_id,
        "status": status,
        "deduped": deduped,
        "platform_id": record.platform_id,
        "custom_fields": stored_fields,
    }


@router.post("/tables/{table}/records/batch-upsert")
async def batch_upsert_records(table: str, data: BatchRecordsIn):
    _check_name(table)
    if len(data.records) > 1000:
        raise HTTPException(400, "batch-upsert принимает максимум 1000 записей за запрос")

    ordered_platform_ids: list[str] = []
    records_by_platform_id: dict[str, RecordIn] = {}
    skipped = 0
    for record in data.records:
        platform_id = str(record.platform_id or "").strip()
        if not platform_id:
            skipped += 1
            continue
        if platform_id not in records_by_platform_id:
            ordered_platform_ids.append(platform_id)
        records_by_platform_id[platform_id] = _clean_record(RecordIn(
            platform_id=platform_id,
            custom_fields=record.custom_fields or {},
        ))

    if not ordered_platform_ids:
        return {
            "ok": True,
            "total": len(data.records),
            "unique": 0,
            "created": 0,
            "updated": 0,
            "skipped": skipped,
        }

    tbl = f"cdb_{table}"
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(_create_index_sql(table))
        placeholders = ",".join("?" for _ in ordered_platform_ids)
        cur = await db.execute(
            f"SELECT id, platform_id, custom_fields FROM {tbl} WHERE platform_id IN ({placeholders}) ORDER BY id ASC",
            ordered_platform_ids,
        )
        existing: dict[str, dict] = {}
        for row in await cur.fetchall():
            platform_id = row["platform_id"]
            current = existing.get(platform_id)
            if current is None:
                existing[platform_id] = {
                    "id": int(row["id"]),
                    "custom_fields": _json_dict(row["custom_fields"]),
                    "duplicate_ids": [],
                }
            else:
                current["custom_fields"] = _deep_merge(current["custom_fields"], _json_dict(row["custom_fields"]))
                current["duplicate_ids"].append(int(row["id"]))

        if data.dry_run:
            updated = sum(1 for platform_id in ordered_platform_ids if platform_id in existing)
            created = len(ordered_platform_ids) - updated
            _log("info", json.dumps({
                "event": "customer_db_batch_upsert",
                "table": table,
                "total": len(data.records),
                "unique": len(ordered_platform_ids),
                "created": created,
                "updated": updated,
                "skipped": skipped,
                "dry_run": True,
            }, ensure_ascii=False, sort_keys=True))
            return {
                "ok": True,
                "dry_run": True,
                "total": len(data.records),
                "unique": len(ordered_platform_ids),
                "created": created,
                "updated": updated,
                "skipped": skipped,
            }

        created = 0
        updated = 0
        deduped = 0
        ids: dict[str, int] = {}
        for platform_id in ordered_platform_ids:
            record = records_by_platform_id[platform_id]
            existing_record = existing.get(platform_id)
            if existing_record is None:
                custom_json = json.dumps(record.custom_fields, ensure_ascii=False)
                cur = await db.execute(
                    f"INSERT INTO {tbl} (platform_id, custom_fields) VALUES (?, ?)",
                    (platform_id, custom_json),
                )
                record_id = int(cur.lastrowid)
                existing[platform_id] = {"id": record_id, "custom_fields": record.custom_fields}
                created += 1
            else:
                record_id = int(existing_record["id"])
                merged_fields = _deep_merge(existing_record["custom_fields"], record.custom_fields)
                custom_json = json.dumps(merged_fields, ensure_ascii=False)
                await db.execute(
                    f"UPDATE {tbl} SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
                    (custom_json, record_id),
                )
                existing_record["custom_fields"] = merged_fields
                for duplicate_id in existing_record.get("duplicate_ids", []):
                    await db.execute(f"DELETE FROM {tbl} WHERE id=?", (duplicate_id,))
                    deduped += 1
                updated += 1
            ids[platform_id] = record_id

        await db.commit()

    _log("info", json.dumps({
        "event": "customer_db_batch_upsert",
        "table": table,
        "total": len(data.records),
        "unique": len(ordered_platform_ids),
        "created": created,
        "updated": updated,
        "deduped": deduped,
        "skipped": skipped,
        "dry_run": False,
    }, ensure_ascii=False, sort_keys=True))
    return {
        "ok": True,
        "total": len(data.records),
        "unique": len(ordered_platform_ids),
        "created": created,
        "updated": updated,
        "deduped": deduped,
        "skipped": skipped,
        "ids": ids,
    }


@router.get("/tables/{table}/records/{rid}")
async def get_record(table: str, rid: int):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM cdb_{table} WHERE id = ?", (rid,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Запись не найдена")
    r = dict(row)
    r["custom_fields"] = json.loads(r["custom_fields"])
    return r


@router.put("/tables/{table}/records/{rid}")
async def update_record(table: str, rid: int, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            f"UPDATE cdb_{table} SET platform_id=?, custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (record.platform_id, json.dumps(record.custom_fields, ensure_ascii=False), rid),
        )
        await db.commit()
    _log_record_event("update", table, "updated", rid, record.platform_id, record.custom_fields)
    return {"ok": True}


@router.delete("/tables/{table}/records/{rid}")
async def delete_record(table: str, rid: int):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(f"DELETE FROM cdb_{table} WHERE id = ?", (rid,))
        await db.commit()
    _log("info", 'customer_db_record action=delete table=%s record_id=%s', table, rid)
    return {"ok": True}


@router.get("/tables/{table}/stats")
async def table_stats(table: str):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{table}")).fetchone())
        (today,) = (await (await db.execute(
            f"SELECT COUNT(*) FROM cdb_{table} WHERE date(created_at)=date('now')"
        )).fetchone())
    return {"total": total, "today": today}


@router.get("/stats")
async def global_stats():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT name, display_name FROM _cdb_tables ORDER BY id")
        tables = [dict(r) for r in await cur.fetchall()]
        result = []
        for t in tables:
            (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{t['name']}")).fetchone())
            result.append({**t, "total": total})
    return {"tables": result}
