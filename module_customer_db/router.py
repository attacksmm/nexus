"""
customer-db v2.0.0
Структура: id, platform_id, custom_fields (JSON)
Поддерживает несколько именованных таблиц.
"""
import json
import logging
import os
import re
import secrets
import shutil
import time
import gzip
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.auth import ENV_PATH, _read_env_values, _write_env_values
from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()
_db_path = None
_logger: logging.Logger | None = None

SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
PLACEHOLDER_VALUE = re.compile(r"^#\{[^{}]+\}$")
PROTECTED_ATTRIBUTION_KEYS = {"yclid", "ym_uid", "_ym_uid"}
MODULE_ID = "customer-db"
HOT_RETENTION_MONTHS = 6
BACKUP_KEEP_LATEST = 2
COMPACT_TABLES = {"vk_clients", "telegram_clients"}
COMPACT_THRESHOLD_BYTES = 2048
COMPACT_KEEP_KEYS = {
    "platform",
    "source",
    "salebot_id",
    "vk_user_id",
    "tg_user_id",
    "user_id",
    "client_id",
    "name",
    "first_name",
    "last_name",
    "second_name",
    "full_name",
    "username",
    "phone",
    "email",
    "domain",
    "sex",
    "subscribe",
    "tag",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "yclid",
    "ym_uid",
    "_ym_uid",
}


@asynccontextmanager
async def _connect_db():
    db = await aiosqlite.connect(_db_path, timeout=30)
    try:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=FULL")
        yield db
    finally:
        await db.close()


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


def _must_db_path() -> Path:
    if _db_path is None:
        raise HTTPException(500, "customer-db is not initialized")
    return Path(_db_path)


def _data_dir() -> Path:
    return _must_db_path().parent


def _archive_db_path() -> Path:
    return _data_dir() / "archive" / "customer-db-archive.db"


def _backups_root() -> Path:
    # /home/attack/nexus/modules/customer-db/data/customer-db.db -> /home/attack/nexus/backups
    return _must_db_path().parents[3] / "backups"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _retention_cutoff(months: int = HOT_RETENTION_MONTHS) -> str:
    days = max(1, int(months)) * 31
    return f"-{days} days"


@asynccontextmanager
async def _connect_archive_db():
    path = _archive_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(path, timeout=30)
    try:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=FULL")
        await _init_archive_db(db)
        yield db
    finally:
        await db.close()


async def _init_archive_db(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS archive_records (
            table_name TEXT NOT NULL,
            id INTEGER NOT NULL,
            platform_id TEXT NOT NULL DEFAULT '',
            custom_fields TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            PRIMARY KEY(table_name, id)
        );
        CREATE INDEX IF NOT EXISTS idx_archive_table_platform ON archive_records(table_name, platform_id);
        CREATE INDEX IF NOT EXISTS idx_archive_table_created ON archive_records(table_name, created_at);
        CREATE INDEX IF NOT EXISTS idx_archive_platform ON archive_records(platform_id);
        CREATE TABLE IF NOT EXISTS payload_records (
            table_name TEXT NOT NULL,
            id INTEGER NOT NULL,
            payload_gz BLOB NOT NULL,
            payload_bytes INTEGER NOT NULL DEFAULT 0,
            compressed_bytes INTEGER NOT NULL DEFAULT 0,
            stored_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(table_name, id)
        );
        CREATE INDEX IF NOT EXISTS idx_payload_table ON payload_records(table_name);
    """)
    await db.commit()


async def _init_db():
    async with _connect_db() as db:
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
    parts = [
        "customer_db",
        f"operation={action}",
        f"result={status}",
        f"table={table}",
        f"platform_id={platform_id or '-'}",
        f"record_id={record_id if record_id is not None else '-'}",
    ]
    if extra and extra.get("deduped"):
        parts.append(f"deduped={extra['deduped']}")
    if custom_fields.get("platform"):
        parts.append(f"platform={custom_fields.get('platform')}")
    if custom_fields.get("salebot_id"):
        parts.append(f"salebot_id={custom_fields.get('salebot_id')}")
    return " ".join(parts)


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


def _log_db_operation(
    *,
    operation: str,
    table: str,
    result: str,
    platform_id: str = "",
    record_id: int | None = None,
    deduped: int = 0,
    reason: str = "",
):
    parts = [
        "customer_db",
        f"operation={operation}",
        f"result={result}",
        f"table={table}",
        f"platform_id={platform_id or '-'}",
        f"record_id={record_id if record_id is not None else '-'}",
    ]
    if deduped:
        parts.append(f"deduped={deduped}")
    if reason:
        parts.append(f"reason={reason}")
    _log("info", " ".join(parts))


def _check_name(name: str):
    if not SAFE_NAME.match(name):
        raise HTTPException(400, "Имя таблицы: только латинские буквы, цифры, _. Начинается с буквы.")


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _external_api_token() -> str:
    return (
        os.environ.get("NEXUS_CUSTOMER_DB_API_TOKEN", "").strip()
        or os.environ.get("NEXUS_OPENROUTER_API_TOKEN", "").strip()
    )


def _customer_db_api_token() -> tuple[str, str, bool]:
    token = os.environ.get("NEXUS_CUSTOMER_DB_API_TOKEN", "").strip()
    if token:
        return token, "NEXUS_CUSTOMER_DB_API_TOKEN", False

    values = _read_env_values()
    token = values.get("NEXUS_CUSTOMER_DB_API_TOKEN", "").strip()
    if token:
        os.environ["NEXUS_CUSTOMER_DB_API_TOKEN"] = token
        return token, "NEXUS_CUSTOMER_DB_API_TOKEN", False

    token = secrets.token_urlsafe(40)
    values["NEXUS_CUSTOMER_DB_API_TOKEN"] = token
    _write_env_values(values)
    os.environ["NEXUS_CUSTOMER_DB_API_TOKEN"] = token
    return token, "NEXUS_CUSTOMER_DB_API_TOKEN", True


def _token_from_payload(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("token", "api_token", "secret"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


async def _require_bearer(request: Request, payload: dict | None = None) -> None:
    expected = _external_api_token()
    if not expected:
        raise HTTPException(503, "customer-db API token is not configured")
    header = request.headers.get("authorization", "")
    prefix = "Bearer "
    candidates = []
    if header.startswith(prefix):
        candidates.append(header[len(prefix):].strip())
    for key in ("token", "api_token", "secret"):
        value = str(request.query_params.get(key) or "").strip()
        if value:
            candidates.append(value)
    payload_token = _token_from_payload(payload)
    if payload_token:
        candidates.append(payload_token)
    if not any(secrets.compare_digest(candidate, expected) for candidate in candidates):
        raise HTTPException(401, "unauthorized")


async def _require_panel_or_bearer(request: Request, payload: dict | None = None) -> None:
    try:
        await _require_bearer(request, payload)
        return
    except HTTPException as bearer_exc:
        user = await verify_token_from_request(request)
        if user and can_access_module(user, MODULE_ID):
            return
        raise bearer_exc


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


async def _request_payload(request: Request) -> dict:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type.lower():
        try:
            data = await request.json()
        except Exception as exc:
            raise HTTPException(400, f"invalid json: {exc}") from exc
        if not isinstance(data, dict):
            raise HTTPException(400, "JSON body must be an object")
        return data

    form = await request.form()
    data: dict[str, object] = {}
    for key, value in form.multi_items():
        data[str(key)] = value
    return data


def _table_platform(table: str) -> str:
    lower = table.lower()
    if lower.startswith("vk") or "vkontakte" in lower:
        return "vkontakte"
    if lower.startswith("telegram") or lower.startswith("tg"):
        return "telegram"
    return table


def _platform_id_from_payload(table: str, payload: dict) -> str:
    candidates = [
        payload.get("platform_id"),
        payload.get("vk_user_id") if table.lower().startswith("vk") else None,
        payload.get("tg_user_id") if table.lower().startswith(("telegram", "tg")) else None,
        payload.get("user_id"),
        payload.get("client_id"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text and not _is_empty_value(text):
            return text
    return ""


def _custom_fields_from_payload(table: str, payload: dict) -> dict:
    custom = {}
    raw_custom = payload.get("custom_fields")
    if isinstance(raw_custom, dict):
        custom.update(raw_custom)
    elif isinstance(raw_custom, str) and raw_custom.strip().startswith("{"):
        custom.update(_json_dict(raw_custom))

    excluded = {"platform_id", "custom_fields", "records", "dry_run", "secret", "token", "api_token"}
    for key, value in payload.items():
        key = str(key)
        if key in excluded:
            continue
        if key.startswith("custom_fields."):
            nested_key = key.split(".", 1)[1].strip()
            if nested_key:
                custom[nested_key] = value
            continue
        if key in {"vk_user_id", "tg_user_id", "user_id", "client_id"} and not custom.get(key):
            custom[key] = value
            continue
        if key not in custom:
            custom[key] = value

    custom.setdefault("platform", _table_platform(table))
    custom.setdefault("source", "customer_db_table_endpoint")
    return custom


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    cur = await db.execute("SELECT 1 FROM _cdb_tables WHERE name=?", (table,))
    return await cur.fetchone() is not None


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


def _is_payload_marker(value: dict) -> bool:
    marker = value.get("_payload_storage") if isinstance(value, dict) else None
    return isinstance(marker, dict) and marker.get("kind") == "gzip_payload_v1"


def _compact_scalar(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        text = str(value) if value is not None else ""
        return text[:500] if len(text) > 500 else value
    return None


def _compact_nested_dict(value: dict) -> dict:
    result = {}
    for key, item in value.items():
        clean_key = str(key)
        if clean_key in COMPACT_KEEP_KEYS or _is_protected_attribution_key(clean_key):
            scalar = _compact_scalar(item)
            if scalar is not None:
                result[clean_key] = scalar
    return result


def _compact_custom_fields_profile(custom_fields: dict, payload_bytes: int, compressed_bytes: int) -> dict:
    compact: dict = {}
    for key, value in (custom_fields or {}).items():
        clean_key = str(key)
        if clean_key.startswith("_payload_storage"):
            continue
        if clean_key in COMPACT_KEEP_KEYS or _is_protected_attribution_key(clean_key):
            scalar = _compact_scalar(value)
            if scalar is not None:
                compact[clean_key] = scalar
            elif isinstance(value, dict):
                nested = _compact_nested_dict(value)
                if nested:
                    compact[clean_key] = nested
        elif isinstance(value, dict) and clean_key in {"utms", "contact_fields", "possible_accounts", "custom_fields"}:
            nested = _compact_nested_dict(value)
            if nested:
                compact[clean_key] = nested
    compact["_payload_storage"] = {
        "kind": "gzip_payload_v1",
        "full_custom_fields": True,
        "payload_bytes": payload_bytes,
        "compressed_bytes": compressed_bytes,
        "stored_at": _utc_now(),
    }
    return compact


def _should_compact_custom_fields(table: str, custom_fields: dict, threshold: int = COMPACT_THRESHOLD_BYTES) -> bool:
    if table not in COMPACT_TABLES or _is_payload_marker(custom_fields):
        return False
    raw = json.dumps(custom_fields or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return len(raw) >= threshold


def _gzip_json(custom_fields: dict) -> tuple[bytes, int, int]:
    raw = json.dumps(custom_fields or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
    packed = gzip.compress(raw, compresslevel=6)
    return packed, len(raw), len(packed)


async def _store_payload_with_db(archive_db: aiosqlite.Connection, table: str, record_id: int, custom_fields: dict) -> dict:
    packed, payload_bytes, compressed_bytes = _gzip_json(custom_fields)
    now = _utc_now()
    await archive_db.execute(
        """
        INSERT INTO payload_records(table_name, id, payload_gz, payload_bytes, compressed_bytes, stored_at, updated_at)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(table_name, id) DO UPDATE SET
            payload_gz=excluded.payload_gz,
            payload_bytes=excluded.payload_bytes,
            compressed_bytes=excluded.compressed_bytes,
            updated_at=excluded.updated_at
        """,
        (table, record_id, packed, payload_bytes, compressed_bytes, now, now),
    )
    return _compact_custom_fields_profile(custom_fields, payload_bytes, compressed_bytes)


async def _store_payload(table: str, record_id: int, custom_fields: dict) -> dict:
    async with _connect_archive_db() as archive_db:
        compact = await _store_payload_with_db(archive_db, table, record_id, custom_fields)
        await archive_db.commit()
    return compact


async def _load_payload(table: str, record_id: int) -> dict | None:
    path = _archive_db_path()
    if not path.exists():
        return None
    async with _connect_archive_db() as archive_db:
        archive_db.row_factory = aiosqlite.Row
        cur = await archive_db.execute(
            "SELECT payload_gz FROM payload_records WHERE table_name=? AND id=?",
            (table, record_id),
        )
        row = await cur.fetchone()
    if not row:
        return None
    try:
        raw = gzip.decompress(bytes(row["payload_gz"]))
        parsed = json.loads(raw.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception as exc:
        _log("error", "customer_db payload load failed table=%s id=%s error=%s", table, record_id, exc)
        return None


async def _hydrate_custom_fields(table: str, record_id: int, custom_fields: dict) -> dict:
    if not _is_payload_marker(custom_fields):
        return custom_fields
    payload = await _load_payload(table, record_id)
    return payload if isinstance(payload, dict) else custom_fields


async def _custom_fields_for_storage(table: str, record_id: int, custom_fields: dict) -> dict:
    if _should_compact_custom_fields(table, custom_fields):
        return await _store_payload(table, record_id, custom_fields)
    return custom_fields


async def _table_catalog(db: aiosqlite.Connection) -> list[dict[str, str]]:
    db.row_factory = aiosqlite.Row
    cur = await db.execute("SELECT name, display_name FROM _cdb_tables ORDER BY id")
    return [dict(r) for r in await cur.fetchall() if SAFE_NAME.match(str(r["name"]))]


async def _aggregate_records(
    db: aiosqlite.Connection,
    *,
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    tables = await _table_catalog(db)
    if not tables:
        return [], 0

    row_selects: list[str] = []
    row_params: list[object] = []
    count_selects: list[str] = []
    count_params: list[object] = []
    pat = f"%{q}%" if q else ""
    for table in tables:
        name = table["name"]
        display_name = table.get("display_name") or name
        where = ""
        if q:
            where = " WHERE platform_id LIKE ? OR custom_fields LIKE ?"
        row_selects.append(
            f"SELECT ? AS source_table, ? AS source_display_name, id, platform_id, custom_fields, created_at, updated_at FROM cdb_{name}{where}"
        )
        row_params.extend([name, display_name])
        if q:
            row_params.extend([pat, pat])
        count_selects.append(f"SELECT COUNT(*) AS cnt FROM cdb_{name}{where}")
        if q:
            count_params.extend([pat, pat])

    rows_sql = " UNION ALL ".join(row_selects)
    try:
        cur = await db.execute(
            f"SELECT * FROM ({rows_sql}) ORDER BY datetime(created_at) DESC, id DESC LIMIT ? OFFSET ?",
            [*row_params, limit, offset],
        )
        rows = [dict(r) for r in await cur.fetchall()]

        count_sql = " UNION ALL ".join(count_selects)
        cur = await db.execute(f"SELECT SUM(cnt) FROM ({count_sql})", count_params)
        (total,) = await cur.fetchone()
        return rows, int(total or 0)
    except Exception as exc:
        _log("error", "customer_db aggregate query degraded: %s", exc)
        return await _aggregate_records_fallback(db, tables=tables, q=q, limit=limit, offset=offset)


async def _aggregate_records_fallback(
    db: aiosqlite.Connection,
    *,
    tables: list[dict[str, str]],
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    rows: list[dict] = []
    total = 0
    row_limit = offset + limit
    pat = f"%{q}%" if q else ""
    for table in tables:
        name = table["name"]
        display_name = table.get("display_name") or name
        where = " WHERE platform_id LIKE ? OR custom_fields LIKE ?" if q else ""
        params: list[object] = [pat, pat] if q else []
        try:
            cur = await db.execute(
                f"SELECT id, platform_id, custom_fields, created_at, updated_at FROM cdb_{name}{where} "
                "ORDER BY datetime(created_at) DESC, id DESC LIMIT ?",
                [*params, row_limit],
            )
            for row in await cur.fetchall():
                item = dict(row)
                item["source_table"] = name
                item["source_display_name"] = display_name
                rows.append(item)
            cur = await db.execute(f"SELECT COUNT(*) FROM cdb_{name}{where}", params)
            (table_total,) = await cur.fetchone()
            total += int(table_total or 0)
        except Exception as exc:
            _log("error", "customer_db aggregate table skipped table=%s error=%s", name, exc)

    rows.sort(key=lambda r: (str(r.get("created_at") or ""), int(r.get("id") or 0)), reverse=True)
    return rows[offset:offset + limit], total


async def _archive_table_catalog() -> list[dict[str, str]]:
    async with _connect_db() as db:
        return await _table_catalog(db)


async def _archive_records_for_table(
    table: str,
    *,
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    async with _connect_archive_db() as db:
        db.row_factory = aiosqlite.Row
        where = "WHERE table_name=?"
        params: list[object] = [table]
        if q:
            where += " AND (platform_id LIKE ? OR custom_fields LIKE ?)"
            pat = f"%{q}%"
            params.extend([pat, pat])
        cur = await db.execute(
            f"""
            SELECT 1 AS archived, table_name AS source_table, id, platform_id, custom_fields, created_at, updated_at, archived_at
            FROM archive_records
            {where}
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        )
        rows = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute(f"SELECT COUNT(*) FROM archive_records {where}", params)
        (total,) = await cur.fetchone()
    return rows, int(total or 0)


async def _archive_aggregate_records(
    *,
    tables: list[dict[str, str]],
    q: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    table_names = [t["name"] for t in tables]
    if not table_names:
        return [], 0
    display_names = {t["name"]: t.get("display_name") or t["name"] for t in tables}
    placeholders = ",".join("?" for _ in table_names)
    where = f"WHERE table_name IN ({placeholders})"
    params: list[object] = list(table_names)
    if q:
        where += " AND (platform_id LIKE ? OR custom_fields LIKE ?)"
        pat = f"%{q}%"
        params.extend([pat, pat])
    async with _connect_archive_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT 1 AS archived, table_name AS source_table, id, platform_id, custom_fields, created_at, updated_at, archived_at
            FROM archive_records
            {where}
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        )
        rows = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute(f"SELECT COUNT(*) FROM archive_records {where}", params)
        (total,) = await cur.fetchone()
    for row in rows:
        row["source_display_name"] = display_names.get(str(row.get("source_table")), str(row.get("source_table") or ""))
    return rows, int(total or 0)


def _merge_record_pages(live_rows: list[dict], archive_rows: list[dict], *, limit: int, offset: int) -> list[dict]:
    combined = [dict(r, archived=bool(r.get("archived", False))) for r in live_rows]
    combined.extend(dict(r, archived=True) for r in archive_rows)
    combined.sort(key=lambda r: (str(r.get("created_at") or ""), int(r.get("id") or 0)), reverse=True)
    return combined[offset:offset + limit]


async def _archive_get_record(table: str, rid: int) -> dict | None:
    async with _connect_archive_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT 1 AS archived, table_name AS source_table, id, platform_id, custom_fields, created_at, updated_at, archived_at
            FROM archive_records
            WHERE table_name=? AND id=?
            """,
            (table, rid),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def _archive_counts_by_table() -> dict[str, int]:
    path = _archive_db_path()
    if not path.exists():
        return {}
    async with _connect_archive_db() as db:
        cur = await db.execute("SELECT table_name, COUNT(*) FROM archive_records GROUP BY table_name")
        return {str(name): int(count or 0) for name, count in await cur.fetchall()}


async def _payload_stats_by_table() -> dict[str, dict]:
    path = _archive_db_path()
    if not path.exists():
        return {}
    async with _connect_archive_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT table_name, COUNT(*) AS rows, COALESCE(SUM(payload_bytes),0) AS payload_bytes,
                   COALESCE(SUM(compressed_bytes),0) AS compressed_bytes
            FROM payload_records
            GROUP BY table_name
            """
        )
        return {
            str(r["table_name"]): {
                "rows": int(r["rows"] or 0),
                "payload_mb": _mb(int(r["payload_bytes"] or 0)),
                "compressed_mb": _mb(int(r["compressed_bytes"] or 0)),
            }
            for r in await cur.fetchall()
        }


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
        stored_fields = await _hydrate_custom_fields(table, record_id, stored_fields)
        duplicate_ids = []
        for row in rows[1:]:
            duplicate_ids.append(int(row[0]))
            duplicate_id = int(row[0])
            duplicate_fields = await _hydrate_custom_fields(table, duplicate_id, _json_dict(row[1]))
            stored_fields = _deep_merge(stored_fields, duplicate_fields)
        stored_fields = _deep_merge(stored_fields, record.custom_fields)
        storage_fields = await _custom_fields_for_storage(table, record_id, stored_fields)
        custom_json = json.dumps(storage_fields, ensure_ascii=False)
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
    record_id = int(cur.lastrowid)
    storage_fields = await _custom_fields_for_storage(table, record_id, record.custom_fields)
    if storage_fields is not record.custom_fields:
        await db.execute(
            f"UPDATE {tbl} SET custom_fields=? WHERE id=?",
            (json.dumps(storage_fields, ensure_ascii=False), record_id),
        )
    return record_id, "created", record.custom_fields, 0


# ── Tables management ─────────────────────────────────────────────────────────

@router.get("/settings/token")
async def get_settings_token(request: Request):
    await _require_panel_user(request)
    token, source, generated = _customer_db_api_token()
    return {
        "token": token,
        "source": source,
        "generated": generated,
        "env_path": str(ENV_PATH),
        "authorization": f"Bearer {token}",
        "headers_json": {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    }


@router.get("/tables")
async def list_tables(request: Request):
    await _require_panel_user(request)
    async with _connect_db() as db:
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
async def create_table(data: TableIn, request: Request):
    await _require_panel_user(request)
    _check_name(data.name)
    async with _connect_db() as db:
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
async def delete_table(name: str, request: Request):
    await _require_panel_user(request)
    _check_name(name)
    if name == "default":
        raise HTTPException(400, "Нельзя удалить таблицу default")
    async with _connect_db() as db:
        await db.execute("DELETE FROM _cdb_tables WHERE name = ?", (name,))
        await db.execute(f"DROP TABLE IF EXISTS cdb_{name}")
        await db.commit()
    _log("info", 'customer_db_table action=delete table=%s', name)
    return {"ok": True}


# ── Records ───────────────────────────────────────────────────────────────────

@router.get("/tables/{table}/records")
async def list_records(
    request: Request,
    table: str,
    q: str = Query("", alias="q"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    await _require_panel_user(request)
    _check_name(table)
    page_limit = offset + limit
    async with _connect_db() as db:
        db.row_factory = aiosqlite.Row
        if table == "default":
            tables = await _table_catalog(db)
            live_rows, live_total = await _aggregate_records(db, q=q, limit=page_limit, offset=0)
        else:
            tbl = f"cdb_{table}"
            if q:
                pat = f"%{q}%"
                cur = await db.execute(
                    f"SELECT 0 AS archived, * FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?",
                    (pat, pat, page_limit, 0),
                )
                cnt = await db.execute(
                    f"SELECT COUNT(*) FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ?",
                    (pat, pat),
                )
            else:
                cur = await db.execute(f"SELECT 0 AS archived, * FROM {tbl} ORDER BY id DESC LIMIT ? OFFSET ?", (page_limit, 0))
                cnt = await db.execute(f"SELECT COUNT(*) FROM {tbl}")
            live_rows = [dict(r) for r in await cur.fetchall()]
            (live_total,) = await cnt.fetchone()
            tables = []

    if table == "default":
        archive_rows, archive_total = await _archive_aggregate_records(tables=tables, q=q, limit=page_limit, offset=0)
    else:
        archive_rows, archive_total = await _archive_records_for_table(table, q=q, limit=page_limit, offset=0)

    rows = _merge_record_pages(live_rows, archive_rows, limit=limit, offset=offset)
    total = int(live_total or 0) + int(archive_total or 0)

    for r in rows:
        try:
            parsed = json.loads(r["custom_fields"])
        except Exception:
            parsed = {}
        source_table = str(r.get("source_table") or table)
        if source_table == "default":
            source_table = table
        r["custom_fields"] = await _hydrate_custom_fields(source_table, int(r.get("id") or 0), parsed)
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


class RecordIn(BaseModel):
    platform_id: str = ""
    custom_fields: dict = {}


class BatchRecordsIn(BaseModel):
    records: list[RecordIn] = Field(default_factory=list)
    dry_run: bool = False


@router.post("/tables/{table}/records", status_code=201)
async def create_record(table: str, data: RecordIn, request: Request):
    await _require_panel_or_bearer(request)
    _check_name(table)
    record = _clean_record(data)
    async with _connect_db() as db:
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
async def upsert_record(table: str, request: Request):
    _check_name(table)
    payload = await _request_payload(request)
    await _require_panel_or_bearer(request, payload)
    record = _clean_record(RecordIn(
        platform_id=_platform_id_from_payload(table, payload),
        custom_fields=_custom_fields_from_payload(table, payload),
    ))
    if not record.platform_id:
        _log_db_operation(operation="upsert", table=table, result="skipped", reason="missing_platform_id")
        return {
            "ok": False,
            "stored": False,
            "reason": "missing_platform_id",
            "table": table,
            "accepted": True,
        }
    async with _connect_db() as db:
        record_id, status, stored_fields, deduped = await _upsert_one(db, table, record)
        await db.commit()
    _log_db_operation(
        operation="upsert",
        table=table,
        result=status,
        platform_id=record.platform_id,
        record_id=record_id,
        deduped=deduped,
    )
    return {
        "ok": True,
        "id": record_id,
        "status": status,
        "deduped": deduped,
        "platform_id": record.platform_id,
        "custom_fields": stored_fields,
    }


@router.post("/{table}")
async def table_upsert_record(table: str, request: Request):
    """Webhook-friendly table endpoint: /api/vk_clients or /api/telegram_clients."""
    _check_name(table)
    payload = await _request_payload(request)
    await _require_panel_or_bearer(request, payload)
    platform_id = _platform_id_from_payload(table, payload)
    record = _clean_record(RecordIn(
        platform_id=platform_id,
        custom_fields=_custom_fields_from_payload(table, payload),
    ))
    if not record.platform_id:
        _log_db_operation(operation="upsert", table=table, result="skipped", reason="missing_platform_id")
        return {
            "ok": False,
            "stored": False,
            "reason": "missing_platform_id",
            "table": table,
            "accepted": True,
        }

    async with _connect_db() as db:
        if not await _table_exists(db, table):
            raise HTTPException(404, f"Таблица {table!r} не найдена")
        record_id, status, stored_fields, deduped = await _upsert_one(db, table, record)
        await db.commit()

    _log_db_operation(
        operation="upsert",
        table=table,
        result=status,
        platform_id=record.platform_id,
        record_id=record_id,
        deduped=deduped,
    )
    return {
        "ok": True,
        "id": record_id,
        "status": status,
        "deduped": deduped,
        "table": table,
        "platform_id": record.platform_id,
        "custom_fields": stored_fields,
    }


@router.post("/tables/{table}/records/batch-upsert")
async def batch_upsert_records(table: str, data: BatchRecordsIn, request: Request):
    await _require_panel_or_bearer(request)
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
    async with _connect_db() as db:
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
                    "custom_fields": await _hydrate_custom_fields(table, int(row["id"]), _json_dict(row["custom_fields"])),
                    "duplicate_ids": [],
                }
            else:
                duplicate_id = int(row["id"])
                duplicate_fields = await _hydrate_custom_fields(table, duplicate_id, _json_dict(row["custom_fields"]))
                current["custom_fields"] = _deep_merge(current["custom_fields"], duplicate_fields)
                current["duplicate_ids"].append(duplicate_id)

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
                storage_fields = await _custom_fields_for_storage(table, record_id, record.custom_fields)
                if storage_fields is not record.custom_fields:
                    await db.execute(
                        f"UPDATE {tbl} SET custom_fields=? WHERE id=?",
                        (json.dumps(storage_fields, ensure_ascii=False), record_id),
                    )
                existing[platform_id] = {"id": record_id, "custom_fields": record.custom_fields}
                created += 1
            else:
                record_id = int(existing_record["id"])
                merged_fields = _deep_merge(existing_record["custom_fields"], record.custom_fields)
                storage_fields = await _custom_fields_for_storage(table, record_id, merged_fields)
                custom_json = json.dumps(storage_fields, ensure_ascii=False)
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
async def get_record(table: str, rid: int, request: Request):
    await _require_panel_user(request)
    _check_name(table)
    async with _connect_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM cdb_{table} WHERE id = ?", (rid,))
        row = await cur.fetchone()
    if not row:
        archived = await _archive_get_record(table, rid)
        if not archived:
            raise HTTPException(404, "Запись не найдена")
        r = archived
        try:
            r["custom_fields"] = json.loads(r["custom_fields"])
        except Exception:
            r["custom_fields"] = {}
        return r
    r = dict(row)
    r["custom_fields"] = await _hydrate_custom_fields(table, int(r["id"]), json.loads(r["custom_fields"]))
    r["archived"] = False
    return r


@router.put("/tables/{table}/records/{rid}")
async def update_record(table: str, rid: int, data: RecordIn, request: Request):
    await _require_panel_user(request)
    _check_name(table)
    record = _clean_record(data)
    storage_fields = await _custom_fields_for_storage(table, rid, record.custom_fields)
    async with _connect_db() as db:
        await db.execute(
            f"UPDATE cdb_{table} SET platform_id=?, custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (record.platform_id, json.dumps(storage_fields, ensure_ascii=False), rid),
        )
        await db.commit()
    _log_record_event("update", table, "updated", rid, record.platform_id, record.custom_fields)
    return {"ok": True}


@router.delete("/tables/{table}/records/{rid}")
async def delete_record(table: str, rid: int, request: Request):
    await _require_panel_user(request)
    _check_name(table)
    async with _connect_db() as db:
        await db.execute(f"DELETE FROM cdb_{table} WHERE id = ?", (rid,))
        await db.commit()
    async with _connect_archive_db() as archive_db:
        await archive_db.execute("DELETE FROM payload_records WHERE table_name=? AND id=?", (table, rid))
        await archive_db.execute("DELETE FROM archive_records WHERE table_name=? AND id=?", (table, rid))
        await archive_db.commit()
    _log("info", 'customer_db_record action=delete table=%s record_id=%s', table, rid)
    return {"ok": True}


@router.get("/tables/{table}/stats")
async def table_stats(table: str, request: Request):
    await _require_panel_user(request)
    _check_name(table)
    archive_counts = await _archive_counts_by_table()
    async with _connect_db() as db:
        if table == "default":
            tables = await _table_catalog(db)
            total = 0
            today = 0
            archived_total = 0
            for item in tables:
                name = item["name"]
                (table_total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{name}")).fetchone())
                (table_today,) = (await (await db.execute(
                    f"SELECT COUNT(*) FROM cdb_{name} WHERE date(created_at)=date('now')"
                )).fetchone())
                total += int(table_total or 0)
                today += int(table_today or 0)
                archived_total += int(archive_counts.get(name, 0))
        else:
            (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{table}")).fetchone())
            (today,) = (await (await db.execute(
                f"SELECT COUNT(*) FROM cdb_{table} WHERE date(created_at)=date('now')"
            )).fetchone())
            archived_total = int(archive_counts.get(table, 0))
    return {"total": int(total or 0) + archived_total, "hot": int(total or 0), "archived": archived_total, "today": today}


@router.get("/stats")
async def global_stats(request: Request):
    await _require_panel_user(request)
    archive_counts = await _archive_counts_by_table()
    async with _connect_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT name, display_name FROM _cdb_tables ORDER BY id")
        tables = [dict(r) for r in await cur.fetchall()]
        result = []
        for t in tables:
            (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{t['name']}")).fetchone())
            archived = int(archive_counts.get(str(t["name"]), 0))
            result.append({**t, "total": int(total or 0) + archived, "hot": int(total or 0), "archived": archived})
    return {"tables": result}


# ── Storage maintenance ───────────────────────────────────────────────────────

def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return _file_size(path)
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += _file_size(item)
    return total


def _mb(value: int | float) -> float:
    return round(float(value) / 1024 / 1024, 2)


def _backup_candidates() -> list[dict]:
    root = _backups_root()
    if not root.exists():
        return []
    candidates: list[dict] = []
    for path in root.iterdir():
        name = path.name
        if "customer-db" not in name and "customer_db" not in name:
            continue
        db_files = [path] if path.is_file() and path.suffix == ".db" else []
        if path.is_dir():
            db_files = [item for item in path.rglob("*.db") if item.is_file()]
        if not db_files:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        size = _tree_size(path)
        candidates.append({
            "path": str(path),
            "name": name,
            "mtime": stat.st_mtime,
            "mtime_text": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime)),
            "size": size,
            "size_mb": _mb(size),
            "is_dir": path.is_dir(),
            "db_files": [str(item) for item in db_files[:10]],
            "db_file_count": len(db_files),
        })
    candidates.sort(key=lambda item: float(item["mtime"]), reverse=True)
    return candidates


async def _table_size_rows() -> list[dict]:
    async with _connect_db() as db:
        db.row_factory = aiosqlite.Row
        tables = await _table_catalog(db)
        rows: list[dict] = []
        dbstat: dict[str, int] = {}
        try:
            cur = await db.execute("SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name")
            dbstat = {str(r["name"]): int(r["bytes"] or 0) for r in await cur.fetchall()}
        except Exception as exc:
            _log("warning", "customer_db storage dbstat unavailable: %s", exc)
        archive_counts = await _archive_counts_by_table()
        payload_stats = await _payload_stats_by_table()
        for table in tables:
            name = str(table["name"])
            cur = await db.execute(f"SELECT COUNT(*) FROM cdb_{name}")
            (hot_rows,) = await cur.fetchone()
            cur = await db.execute(f"SELECT AVG(LENGTH(custom_fields)), MAX(LENGTH(custom_fields)) FROM cdb_{name}")
            avg_len, max_len = await cur.fetchone()
            rows.append({
                "name": name,
                "display_name": table.get("display_name") or name,
                "hot_rows": int(hot_rows or 0),
                "archived_rows": int(archive_counts.get(name, 0)),
                "total_rows": int(hot_rows or 0) + int(archive_counts.get(name, 0)),
                "db_mb": _mb(dbstat.get(f"cdb_{name}", 0)),
                "avg_custom_fields_bytes": round(float(avg_len or 0), 1),
                "max_custom_fields_bytes": int(max_len or 0),
                "payload": payload_stats.get(name, {"rows": 0, "payload_mb": 0.0, "compressed_mb": 0.0}),
            })
        rows.sort(key=lambda item: item["db_mb"], reverse=True)
        return rows


@router.get("/maintenance/storage")
async def maintenance_storage(request: Request):
    await _require_panel_user(request)
    usage = shutil.disk_usage(str(_data_dir()))
    db_path = _must_db_path()
    archive_path = _archive_db_path()
    backup_candidates = _backup_candidates()
    return {
        "ok": True,
        "retention_months": HOT_RETENTION_MONTHS,
        "disk": {
            "total_mb": _mb(usage.total),
            "used_mb": _mb(usage.used),
            "free_mb": _mb(usage.free),
            "used_percent": round(usage.used / usage.total * 100, 2) if usage.total else 0,
        },
        "files": {
            "live_db": str(db_path),
            "live_db_mb": _mb(_file_size(db_path)),
            "archive_db": str(archive_path),
            "archive_db_mb": _mb(_file_size(archive_path)),
            "backups_root": str(_backups_root()),
            "customer_db_backups_mb": _mb(sum(int(item["size"]) for item in backup_candidates)),
        },
        "tables": await _table_size_rows(),
        "backup_policy": {
            "keep_latest": BACKUP_KEEP_LATEST,
            "candidate_count": len(backup_candidates),
            "delete_count_if_executed": max(0, len(backup_candidates) - BACKUP_KEEP_LATEST),
        },
    }


async def _archive_plan(months: int) -> dict:
    cutoff_expr = _retention_cutoff(months)
    result: dict[str, dict] = {}
    async with _connect_db() as db:
        tables = await _table_catalog(db)
        for table in tables:
            name = table["name"]
            cur = await db.execute(
                f"SELECT COUNT(*), COALESCE(SUM(LENGTH(custom_fields)),0) FROM cdb_{name} WHERE datetime(created_at) < datetime('now', ?)",
                (cutoff_expr,),
            )
            count, bytes_sum = await cur.fetchone()
            result[name] = {
                "rows": int(count or 0),
                "custom_fields_mb": _mb(int(bytes_sum or 0)),
            }
    return {
        "months": months,
        "cutoff": cutoff_expr,
        "tables": result,
        "total_rows": sum(item["rows"] for item in result.values()),
    }


@router.get("/maintenance/archive/plan")
async def maintenance_archive_plan(request: Request, months: int = Query(HOT_RETENTION_MONTHS, ge=1, le=120)):
    await _require_panel_user(request)
    return {"ok": True, **await _archive_plan(months)}


@router.post("/maintenance/archive/run")
async def maintenance_archive_run(
    request: Request,
    months: int = Query(HOT_RETENTION_MONTHS, ge=1, le=120),
    batch_size: int = Query(5000, ge=1, le=50000),
    dry_run: bool = Query(True),
):
    await _require_panel_user(request)
    plan = await _archive_plan(months)
    if dry_run:
        return {"ok": True, "dry_run": True, **plan}

    cutoff_expr = _retention_cutoff(months)
    archived: dict[str, int] = {}
    async with _connect_archive_db() as archive_db:
        async with _connect_db() as db:
            db.row_factory = aiosqlite.Row
            tables = await _table_catalog(db)
            for table in tables:
                name = table["name"]
                moved = 0
                while True:
                    cur = await db.execute(
                        f"""
                        SELECT id, platform_id, custom_fields, created_at, updated_at
                        FROM cdb_{name}
                        WHERE datetime(created_at) < datetime('now', ?)
                        ORDER BY id
                        LIMIT ?
                        """,
                        (cutoff_expr, batch_size),
                    )
                    rows = await cur.fetchall()
                    if not rows:
                        break
                    payload = [
                        (name, int(r["id"]), str(r["platform_id"] or ""), str(r["custom_fields"] or "{}"), str(r["created_at"]), str(r["updated_at"]), _utc_now())
                        for r in rows
                    ]
                    await archive_db.executemany(
                        """
                        INSERT OR IGNORE INTO archive_records(table_name, id, platform_id, custom_fields, created_at, updated_at, archived_at)
                        VALUES(?,?,?,?,?,?,?)
                        """,
                        payload,
                    )
                    ids = [int(r["id"]) for r in rows]
                    placeholders = ",".join("?" for _ in ids)
                    await db.execute(f"DELETE FROM cdb_{name} WHERE id IN ({placeholders})", ids)
                    await db.commit()
                    await archive_db.commit()
                    moved += len(rows)
                archived[name] = moved
    _log("info", "customer_db maintenance=archive months=%s archived=%s", months, json.dumps(archived, ensure_ascii=False, sort_keys=True))
    return {"ok": True, "dry_run": False, "months": months, "archived": archived, "total_archived": sum(archived.values())}


def _compact_table_list(tables: str) -> list[str]:
    result = []
    for item in str(tables or "").split(","):
        name = item.strip()
        if not name:
            continue
        _check_name(name)
        result.append(name)
    return result or sorted(COMPACT_TABLES)


async def _compact_plan(tables: list[str], threshold: int) -> dict:
    result: dict[str, dict] = {}
    total_rows = 0
    total_payload = 0
    total_compressed = 0
    total_live_after = 0
    async with _connect_db() as db:
        db.row_factory = aiosqlite.Row
        for table in tables:
            last_id = 0
            payload_bytes = 0
            compressed_bytes = 0
            live_after_bytes = 0
            compact_rows = 0
            while True:
                cur = await db.execute(
                    f"""
                    SELECT id, custom_fields
                    FROM cdb_{table}
                    WHERE id > ? AND LENGTH(custom_fields) >= ? AND custom_fields NOT LIKE '%"_payload_storage"%'
                    ORDER BY id
                    LIMIT 1000
                    """,
                    (last_id, threshold),
                )
                rows = await cur.fetchall()
                if not rows:
                    break
                for row in rows:
                    last_id = max(last_id, int(row["id"]))
                    fields = _json_dict(row["custom_fields"])
                    if not _should_compact_custom_fields(table, fields, threshold):
                        continue
                    packed, raw_size, packed_size = _gzip_json(fields)
                    compact = _compact_custom_fields_profile(fields, raw_size, packed_size)
                    payload_bytes += raw_size
                    compressed_bytes += len(packed)
                    live_after_bytes += len(json.dumps(compact, ensure_ascii=False).encode("utf-8"))
                    compact_rows += 1
            total_rows += compact_rows
            total_payload += payload_bytes
            total_compressed += compressed_bytes
            total_live_after += live_after_bytes
            result[table] = {
                "rows": compact_rows,
                "payload_mb": _mb(payload_bytes),
                "compressed_payload_mb": _mb(compressed_bytes),
                "estimated_live_profile_mb": _mb(live_after_bytes),
                "estimated_savings_mb": _mb(max(0, payload_bytes - compressed_bytes - live_after_bytes)),
            }
    return {
        "threshold": threshold,
        "tables": result,
        "total_rows": total_rows,
        "payload_mb": _mb(total_payload),
        "compressed_payload_mb": _mb(total_compressed),
        "estimated_live_profile_mb": _mb(total_live_after),
        "estimated_savings_mb": _mb(max(0, total_payload - total_compressed - total_live_after)),
    }


@router.get("/maintenance/compact/plan")
async def maintenance_compact_plan(
    request: Request,
    tables: str = Query("vk_clients,telegram_clients"),
    threshold: int = Query(COMPACT_THRESHOLD_BYTES, ge=512, le=200000),
):
    await _require_panel_user(request)
    table_list = _compact_table_list(tables)
    return {"ok": True, **await _compact_plan(table_list, threshold)}


@router.post("/maintenance/compact/run")
async def maintenance_compact_run(
    request: Request,
    tables: str = Query("vk_clients,telegram_clients"),
    threshold: int = Query(COMPACT_THRESHOLD_BYTES, ge=512, le=200000),
    batch_size: int = Query(2000, ge=1, le=20000),
    dry_run: bool = Query(True),
):
    await _require_panel_user(request)
    table_list = _compact_table_list(tables)
    plan = await _compact_plan(table_list, threshold)
    if dry_run:
        return {"ok": True, "dry_run": True, **plan}

    compacted: dict[str, int] = {}
    async with _connect_archive_db() as archive_db:
        async with _connect_db() as db:
            db.row_factory = aiosqlite.Row
            for table in table_list:
                moved = 0
                last_id = 0
                while True:
                    cur = await db.execute(
                        f"""
                        SELECT id, custom_fields
                        FROM cdb_{table}
                        WHERE id > ? AND LENGTH(custom_fields) >= ? AND custom_fields NOT LIKE '%"_payload_storage"%'
                        ORDER BY id
                        LIMIT ?
                        """,
                        (last_id, threshold, batch_size),
                    )
                    rows = await cur.fetchall()
                    if not rows:
                        break
                    for row in rows:
                        record_id = int(row["id"])
                        last_id = max(last_id, record_id)
                        fields = _json_dict(row["custom_fields"])
                        if not _should_compact_custom_fields(table, fields, threshold):
                            continue
                        storage_fields = await _store_payload_with_db(archive_db, table, record_id, fields)
                        await db.execute(
                            f"UPDATE cdb_{table} SET custom_fields=? WHERE id=?",
                            (json.dumps(storage_fields, ensure_ascii=False), record_id),
                        )
                        moved += 1
                    await db.commit()
                    await archive_db.commit()
                compacted[table] = moved
    _log("info", "customer_db maintenance=compact threshold=%s compacted=%s", threshold, json.dumps(compacted, ensure_ascii=False, sort_keys=True))
    return {"ok": True, "dry_run": False, "threshold": threshold, "compacted": compacted, "total_compacted": sum(compacted.values()), "plan_before": plan}


@router.get("/maintenance/backups/plan")
async def maintenance_backups_plan(request: Request, keep_latest: int = Query(BACKUP_KEEP_LATEST, ge=1, le=20)):
    await _require_panel_user(request)
    candidates = _backup_candidates()
    keep = candidates[:keep_latest]
    delete = candidates[keep_latest:]
    return {
        "ok": True,
        "keep_latest": keep_latest,
        "keep": keep,
        "delete": delete,
        "delete_count": len(delete),
        "delete_mb": _mb(sum(int(item["size"]) for item in delete)),
    }


@router.post("/maintenance/backups/cleanup")
async def maintenance_backups_cleanup(
    request: Request,
    keep_latest: int = Query(BACKUP_KEEP_LATEST, ge=1, le=20),
    execute: bool = Query(False),
):
    await _require_panel_user(request)
    plan = await maintenance_backups_plan(request, keep_latest)
    if not execute:
        return {"dry_run": True, **plan}

    deleted: list[dict] = []
    for item in plan["delete"]:
        path = Path(str(item["path"]))
        backups_root = _backups_root().resolve()
        try:
            resolved = path.resolve()
            if backups_root not in resolved.parents and resolved != backups_root:
                raise RuntimeError("path outside backups root")
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
            deleted.append(item)
        except Exception as exc:
            _log("error", "customer_db backup cleanup failed path=%s error=%s", path, exc)
            raise HTTPException(500, f"failed to delete {path}: {exc}") from exc
    _log("info", "customer_db maintenance=backup_cleanup keep_latest=%s deleted=%s delete_mb=%s", keep_latest, len(deleted), plan["delete_mb"])
    return {"ok": True, "dry_run": False, "deleted": deleted, "deleted_count": len(deleted), "deleted_mb": plan["delete_mb"], "kept": plan["keep"]}
