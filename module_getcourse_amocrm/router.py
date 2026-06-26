from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_field_cache: dict[str, list[dict[str, Any]]] = {}
_sync_task: asyncio.Task | None = None

MODULE_ID = "getcourse-amocrm"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "pipeline_id": "10566818",
    "status_id": "83350598",
    "responsible_user_id": "6269974",
    "getcourse_base_url": "https://club.sobakovod.pro",
    "request_timeout": "15",
    "duplicate_policy": "update",
    "tags": "GC\nАвтооплата",
    "bindings_paused": "0",
    "cdb_sync_enabled": "1",
    "cdb_poll_seconds": "10",
    "cdb_sync_bootstrapped": "0",
}

DEFAULT_BINDINGS = [
    {
        "process": "created",
        "name": "Создан заказ",
        "task_text": "Связаться по новому заказу GetCourse №{number}",
    },
    {
        "process": "partial",
        "name": "Частично оплачен",
        "task_text": "Проверить частичную оплату GetCourse №{number}",
    },
    {
        "process": "paid",
        "name": "Оплачен",
        "task_text": "Проверить оплаченный заказ GetCourse №{number}",
    },
]

DEFAULT_DUPLICATE_SEARCH_RULES = [
    {"field": "№ ГК", "source": "number"},
    {"field": "ГК ID Заказа", "source": "order_id"},
]

BINDING_ALIASES = {
    "": "created",
    "created": "created",
    "unpaid": "created",
    "new": "created",
    "partial": "partial",
    "partially_paid": "partial",
    "paid": "paid",
}

UTM_SPECS = [
    ("utm_source", "utm_source", "UTM_SOURCE"),
    ("utm_medium", "utm_medium", "UTM_MEDIUM"),
    ("utm_campaign", "utm_campaign", "UTM_CAMPAIGN"),
    ("utm_content", "utm_content", "UTM_CONTENT"),
    ("utm_term", "utm_term", "UTM_TERM"),
]


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def setup(ctx):
    global _db_path, _module_dir, _logger, _sync_task
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-amocrm"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        if _sync_task is None or _sync_task.done():
            _sync_task = loop.create_task(_customer_db_sync_loop())
    else:
        loop.run_until_complete(_init_db())


async def _init_db() -> None:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS order_map (
                order_key TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                method TEXT NOT NULL DEFAULT '',
                order_id TEXT NOT NULL DEFAULT '',
                number TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                contact_id TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                ignored INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                raw_payload TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_events_order ON events(order_id, number);
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE TABLE IF NOT EXISTS cdb_sync (
                source_record_id INTEGER PRIMARY KEY,
                source_updated_at TEXT NOT NULL DEFAULT '',
                source_hash TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                last_synced_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                process TEXT UNIQUE NOT NULL DEFAULT 'created',
                name TEXT NOT NULL DEFAULT '',
                pipeline_id TEXT NOT NULL DEFAULT '',
                status_id TEXT NOT NULL DEFAULT '',
                responsible_user_id TEXT NOT NULL DEFAULT '',
                duplicate_policy TEXT NOT NULL DEFAULT 'update',
                duplicate_search_entity TEXT NOT NULL DEFAULT 'leads',
                duplicate_search_fields_json TEXT NOT NULL DEFAULT '',
                task_enabled INTEGER NOT NULL DEFAULT 0,
                task_text TEXT NOT NULL DEFAULT '',
                task_due_minutes INTEGER NOT NULL DEFAULT 60,
                task_type_id INTEGER NOT NULL DEFAULT 1,
                task_responsible_user_id TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            """
        )
        await _ensure_binding_columns(db)
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        for item in DEFAULT_BINDINGS:
            await db.execute(
                """
                INSERT OR IGNORE INTO bindings(
                    process,name,pipeline_id,status_id,responsible_user_id,duplicate_policy,
                    duplicate_search_entity,duplicate_search_fields_json,
                    task_enabled,task_text,task_due_minutes,task_type_id,task_responsible_user_id,active
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                """,
                (
                    item["process"],
                    item["name"],
                    DEFAULT_SETTINGS["pipeline_id"],
                    DEFAULT_SETTINGS["status_id"],
                    DEFAULT_SETTINGS["responsible_user_id"],
                    DEFAULT_SETTINGS["duplicate_policy"],
                    "leads",
                    _default_duplicate_rules_json(),
                    0,
                    item["task_text"],
                    60,
                    1,
                    "",
                ),
            )
        if not _env()["webhook_secret"]:
            cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
            row = await cur.fetchone()
            if not _clean(row[0] if row else "", 300):
                await db.execute(
                    """
                    INSERT INTO settings(key,value) VALUES('webhook_secret',?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (secrets.token_urlsafe(24),),
                )
        await db.commit()
    _log("info", "getcourse-amocrm DB initialized")


async def _ensure_binding_columns(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(bindings)")
    columns = {str(row[1]) for row in await cur.fetchall()}
    if "duplicate_search_entity" not in columns:
        await db.execute("ALTER TABLE bindings ADD COLUMN duplicate_search_entity TEXT NOT NULL DEFAULT 'leads'")
    if "duplicate_search_fields_json" not in columns:
        await db.execute("ALTER TABLE bindings ADD COLUMN duplicate_search_fields_json TEXT NOT NULL DEFAULT ''")
    await db.execute(
        """
        UPDATE bindings
        SET duplicate_search_entity='leads'
        WHERE duplicate_search_entity IS NULL OR duplicate_search_entity=''
        """
    )
    await db.execute(
        """
        UPDATE bindings
        SET duplicate_search_fields_json=?
        WHERE duplicate_search_fields_json IS NULL OR duplicate_search_fields_json=''
        """,
        (_default_duplicate_rules_json(),),
    )


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _env() -> dict[str, str]:
    return {
        "amo_base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "amo_token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "webhook_secret": os.environ.get("GETCOURSE_AMO_WEBHOOK_SECRET", "").strip(),
        "customer_db_path": os.environ.get("GETCOURSE_AMO_CUSTOMER_DB_PATH", "").strip(),
    }


def _timeout(settings: dict[str, str]) -> float:
    try:
        return max(3.0, min(60.0, float(settings.get("request_timeout") or "15")))
    except Exception:
        return 15.0


def _int_or_none(value: Any) -> int | None:
    text = _clean(value, 64)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


async def _settings_map() -> dict[str, str]:
    data = dict(DEFAULT_SETTINGS)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data.update({str(row[0]): str(row[1] or "") for row in rows})
    env = _env()
    if env["webhook_secret"]:
        data["webhook_secret"] = env["webhook_secret"]
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "webhook_secret",
        "pipeline_id",
        "status_id",
        "responsible_user_id",
        "getcourse_base_url",
        "request_timeout",
        "duplicate_policy",
        "tags",
        "cdb_sync_enabled",
        "cdb_poll_seconds",
    }
    clean: dict[str, str] = {}
    for key in allowed:
        if key not in data:
            continue
        value = _clean(data.get(key), 5000)
        if key in {"pipeline_id", "status_id", "responsible_user_id"}:
            value = str(_int_or_none(value) or "")
        elif key == "request_timeout":
            value = str(int(_timeout({"request_timeout": value})))
        elif key == "duplicate_policy":
            value = "create" if value == "create" else "update"
        elif key == "cdb_sync_enabled":
            value = "1" if str(value).lower() in {"1", "true", "yes", "on", "да"} else "0"
        elif key == "cdb_poll_seconds":
            try:
                value = str(max(5, min(300, int(float(value)))))
            except Exception:
                value = DEFAULT_SETTINGS[key]
        elif key == "getcourse_base_url":
            value = value.rstrip("/") or DEFAULT_SETTINGS[key]
        clean[key] = value
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        for key, value in clean.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    return await get_settings()


def _binding_process(value: Any) -> str:
    raw = _clean(value, 80).casefold()
    return BINDING_ALIASES.get(raw, raw if raw in {"created", "partial", "paid"} else "created")


def _clean_duplicate_policy(value: Any) -> str:
    value = _clean(value, 40)
    return value if value in {"update", "create", "skip"} else "update"


def _default_duplicate_rules_json() -> str:
    return json.dumps(DEFAULT_DUPLICATE_SEARCH_RULES, ensure_ascii=False)


def _clean_duplicate_search_entity(value: Any) -> str:
    value = _clean(value, 40).casefold()
    return "contacts" if value in {"contact", "contacts", "контакт", "контакты"} else "leads"


def _clean_duplicate_rule(rule: dict[str, Any]) -> dict[str, str]:
    field = _clean(rule.get("field") or rule.get("name"), 300)
    source = _clean(rule.get("source") or rule.get("value") or rule.get("order_field"), 120)
    source = source.strip("{} ").strip()
    field_id = _clean(rule.get("field_id"), 64)
    field_code = _clean(rule.get("field_code") or rule.get("code"), 120).upper()
    if not field and field_id:
        field = f"id:{field_id}"
    if not field and field_code:
        field = field_code
    if field.upper() in {"PHONE", "EMAIL"} and not field_code:
        field_code = field.upper()
    result = {"field": field, "source": source}
    if field_id:
        result["field_id"] = field_id
    if field_code:
        result["field_code"] = field_code
    return result if result["field"] and result["source"] else {}


def _parse_duplicate_rule_line(line: str) -> dict[str, str]:
    text = line.strip()
    if not text or text.startswith("#"):
        return {}
    if "=" in text:
        field, source = text.split("=", 1)
    elif ":" in text and not text.lower().startswith(("id:", "code:")):
        field, source = text.split(":", 1)
    else:
        return {}
    field = field.strip()
    source = source.strip().strip("{} ").strip()
    rule: dict[str, Any] = {"field": field, "source": source}
    lower = field.casefold()
    if lower.startswith("id:"):
        rule["field_id"] = field.split(":", 1)[1].strip()
    elif lower.startswith("code:"):
        rule["field_code"] = field.split(":", 1)[1].strip()
    return _clean_duplicate_rule(rule)


def _duplicate_rules_payload(value: Any) -> list[dict[str, str]]:
    raw = value
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except Exception:
            raw = [_parse_duplicate_rule_line(line) for line in text.splitlines()]
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    rules = []
    for item in raw:
        if isinstance(item, dict):
            rule = _clean_duplicate_rule(item)
        else:
            rule = _parse_duplicate_rule_line(str(item))
        if rule:
            rules.append(rule)
    return rules[:10]


def _duplicate_rules_from_payload(data: dict[str, Any], existing: dict[str, Any]) -> list[dict[str, str]]:
    for key in ("duplicate_search_fields", "duplicate_search_fields_json", "duplicate_search_fields_text"):
        if key in data:
            return _duplicate_rules_payload(data.get(key))
    if "duplicate_search_fields_json" in existing:
        rules = _duplicate_rules_payload(existing.get("duplicate_search_fields_json"))
        if rules:
            return rules
    return list(DEFAULT_DUPLICATE_SEARCH_RULES)


def _duplicate_rules_json_from_payload(data: dict[str, Any], existing: dict[str, Any]) -> str:
    return json.dumps(_duplicate_rules_from_payload(data, existing), ensure_ascii=False)


def _bool_int(value: Any) -> int:
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "on", "да"} else 0
    return 1 if value else 0


def _clean_binding_payload(data: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    existing = existing or {}
    process = _binding_process(data.get("process") or existing.get("process"))
    name = _clean(data.get("name") if "name" in data else existing.get("name"), 300)
    if not name:
        name = next((item["name"] for item in DEFAULT_BINDINGS if item["process"] == process), process)
    due_minutes = data.get("task_due_minutes", existing.get("task_due_minutes", 60))
    task_type_id = data.get("task_type_id", existing.get("task_type_id", 1))
    try:
        due_minutes = max(1, min(60 * 24 * 30, int(float(due_minutes))))
    except Exception:
        due_minutes = 60
    try:
        task_type_id = max(1, int(float(task_type_id)))
    except Exception:
        task_type_id = 1
    return {
        "process": process,
        "name": name,
        "pipeline_id": str(_int_or_none(data.get("pipeline_id", existing.get("pipeline_id"))) or ""),
        "status_id": str(_int_or_none(data.get("status_id", existing.get("status_id"))) or ""),
        "responsible_user_id": str(_int_or_none(data.get("responsible_user_id", existing.get("responsible_user_id"))) or ""),
        "duplicate_policy": _clean_duplicate_policy(data.get("duplicate_policy", existing.get("duplicate_policy"))),
        "duplicate_search_entity": _clean_duplicate_search_entity(data.get("duplicate_search_entity", existing.get("duplicate_search_entity"))),
        "duplicate_search_fields_json": _duplicate_rules_json_from_payload(data, existing),
        "task_enabled": _bool_int(data.get("task_enabled", existing.get("task_enabled"))),
        "task_text": _clean(data.get("task_text", existing.get("task_text")), 2000),
        "task_due_minutes": due_minutes,
        "task_type_id": task_type_id,
        "task_responsible_user_id": str(_int_or_none(data.get("task_responsible_user_id", existing.get("task_responsible_user_id"))) or ""),
        "active": _bool_int(data.get("active", existing.get("active", 1))),
    }


async def _bindings() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT * FROM bindings
            ORDER BY CASE process WHEN 'created' THEN 1 WHEN 'partial' THEN 2 WHEN 'paid' THEN 3 ELSE 4 END, id
            """
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["duplicate_search_entity"] = _clean_duplicate_search_entity(row.get("duplicate_search_entity"))
        row["duplicate_search_fields"] = _duplicate_rules_payload(row.get("duplicate_search_fields_json")) or list(DEFAULT_DUPLICATE_SEARCH_RULES)
    return rows


async def _binding_for_process(process: str, settings: dict[str, str]) -> dict[str, Any]:
    process = _binding_process(process)
    rows = await _bindings()
    for row in rows:
        if row.get("process") == process and int(row.get("active") or 0):
            return row
    fallback = {
        "id": None,
        "process": process,
        "name": process,
        "pipeline_id": settings.get("pipeline_id", ""),
        "status_id": settings.get("status_id", ""),
        "responsible_user_id": settings.get("responsible_user_id", ""),
        "duplicate_policy": settings.get("duplicate_policy", "update"),
        "duplicate_search_entity": "leads",
        "duplicate_search_fields_json": _default_duplicate_rules_json(),
        "duplicate_search_fields": list(DEFAULT_DUPLICATE_SEARCH_RULES),
        "task_enabled": 0,
        "task_text": "",
        "task_due_minutes": 60,
        "task_type_id": 1,
        "task_responsible_user_id": "",
        "active": 1,
    }
    return fallback


async def _save_binding(data: dict[str, Any]) -> dict[str, Any]:
    binding_id = int(data.get("id") or 0)
    existing = None
    if binding_id:
        async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM bindings WHERE id=?", (binding_id,))
            row = await cur.fetchone()
            existing = dict(row) if row else None
    clean = _clean_binding_payload(data, existing)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        if binding_id and existing:
            await db.execute(
                """
                UPDATE bindings
                SET process=?,name=?,pipeline_id=?,status_id=?,responsible_user_id=?,duplicate_policy=?,
                    duplicate_search_entity=?,duplicate_search_fields_json=?,
                    task_enabled=?,task_text=?,task_due_minutes=?,task_type_id=?,task_responsible_user_id=?,
                    active=?,updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (
                    clean["process"], clean["name"], clean["pipeline_id"], clean["status_id"],
                    clean["responsible_user_id"], clean["duplicate_policy"],
                    clean["duplicate_search_entity"], clean["duplicate_search_fields_json"],
                    clean["task_enabled"], clean["task_text"], clean["task_due_minutes"], clean["task_type_id"],
                    clean["task_responsible_user_id"], clean["active"], binding_id,
                ),
            )
            saved_id = binding_id
        else:
            cur = await db.execute(
                """
                INSERT INTO bindings(
                    process,name,pipeline_id,status_id,responsible_user_id,duplicate_policy,
                    duplicate_search_entity,duplicate_search_fields_json,
                    task_enabled,task_text,task_due_minutes,task_type_id,task_responsible_user_id,active
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(process) DO UPDATE SET
                    name=excluded.name,pipeline_id=excluded.pipeline_id,status_id=excluded.status_id,
                    responsible_user_id=excluded.responsible_user_id,duplicate_policy=excluded.duplicate_policy,
                    duplicate_search_entity=excluded.duplicate_search_entity,
                    duplicate_search_fields_json=excluded.duplicate_search_fields_json,
                    task_enabled=excluded.task_enabled,task_text=excluded.task_text,
                    task_due_minutes=excluded.task_due_minutes,task_type_id=excluded.task_type_id,
                    task_responsible_user_id=excluded.task_responsible_user_id,active=excluded.active,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                """,
                (
                    clean["process"], clean["name"], clean["pipeline_id"], clean["status_id"],
                    clean["responsible_user_id"], clean["duplicate_policy"],
                    clean["duplicate_search_entity"], clean["duplicate_search_fields_json"],
                    clean["task_enabled"], clean["task_text"], clean["task_due_minutes"], clean["task_type_id"],
                    clean["task_responsible_user_id"], clean["active"],
                ),
            )
            saved_id = int(cur.lastrowid or binding_id or 0)
        await db.commit()
    return {"ok": True, "id": saved_id, "binding": clean}


def _secret_ok(request: Request, settings: dict[str, str]) -> bool:
    secret = _clean(settings.get("webhook_secret"), 300)
    if not secret:
        return True
    supplied = (
        request.query_params.get("secret")
        or request.headers.get("X-Nexus-Secret")
        or request.headers.get("X-Webhook-Secret")
        or ""
    )
    return _clean(supplied, 300) == secret


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    if request.method.upper() == "GET":
        payload = {str(k): v for k, v in request.query_params.items()}
        return payload, json.dumps(_mask_secret(payload), ensure_ascii=False)
    content_type = request.headers.get("content-type", "").lower()
    raw_body = await request.body()
    if "application/json" in content_type:
        try:
            data = json.loads(raw_body.decode("utf-8") or "{}")
            payload = data if isinstance(data, dict) else {"raw": data}
            return payload, json.dumps(_mask_secret(payload), ensure_ascii=False)
        except Exception:
            text = raw_body.decode("utf-8", "replace")
            return {"raw_data": text}, json.dumps({"raw_data": text}, ensure_ascii=False)
    try:
        form = await request.form()
        if form:
            payload = {str(k): str(v) for k, v in form.items()}
            return payload, json.dumps(_mask_secret(payload), ensure_ascii=False)
    except Exception:
        pass
    if raw_body:
        text = raw_body.decode("utf-8", "replace")
        return {"raw_data": text}, json.dumps({"raw_data": text}, ensure_ascii=False)
    return {}, "{}"


def _mask_secret(data: dict[str, Any]) -> dict[str, Any]:
    result = dict(data)
    for key in ("secret", "webhook_secret", "access_token"):
        if key in result and result[key]:
            result[key] = "***"
    return result


def _money(value: Any) -> float:
    raw = _clean(value, 80).replace(" ", "").replace("\u00a0", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except Exception:
        return 0.0


def _money_value(value: Any) -> int | float:
    amount = _money(value)
    return int(amount) if float(amount).is_integer() else amount


def _phone_text(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value, 100))
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return "+" + digits


def _jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    raw = _clean(value, 20000)
    if not raw:
        return ""
    for candidate in (raw, raw.replace('\\"', '"')):
        try:
            return json.loads(candidate)
        except Exception:
            pass
    return raw


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def _deal_name_text(value: Any) -> str:
    text = re.sub(r"\s+", " ", _flatten_text(value)).strip()
    text = re.sub(r"(?i)(автооплата|autopay)\s+\d{5,}\s*$", r"\1", text).strip()
    text = re.sub(r"\s+\d{5,}\s*$", "", text).strip()
    return text


def _person_for_lead(payload: dict[str, Any]) -> str:
    name = _clean(payload.get("name"), 500)
    if name:
        return name
    first = _clean(payload.get("firstName"), 250)
    last = _clean(payload.get("lastName"), 250)
    return " ".join(part for part in (last, first) if part).strip()


def _person_for_contact(payload: dict[str, Any]) -> tuple[str, str, str]:
    first = _clean(payload.get("firstName"), 250)
    last = _clean(payload.get("lastName"), 250)
    name = " ".join(part for part in (first, last) if part).strip() or _clean(payload.get("name"), 500)
    return name, first, last


def _normalize_order(payload: dict[str, Any], settings: dict[str, str]) -> dict[str, Any]:
    positions = _jsonish(payload.get("positions", ""))
    offers = _jsonish(payload.get("offers", ""))
    title_source = " ".join(part for part in (_flatten_text(positions), _flatten_text(offers)) if part).strip()
    title = _deal_name_text(title_source)
    order_id = _clean(payload.get("order_id") or payload.get("object.id"), 100)
    number = _clean(payload.get("number"), 100)
    gc_user_id = _clean(payload.get("id"), 100)
    base_url = _clean(settings.get("getcourse_base_url"), 500).rstrip("/")
    payment_link = _clean(payload.get("paymentLink") or payload.get("payment_link"), 2000)
    user_link = f"{base_url}/user/control/user/update/id/{quote(gc_user_id)}" if base_url and gc_user_id else ""
    order_link = f"{base_url}/sales/control/deal/update/id/{quote(order_id)}" if base_url and order_id else ""
    person = _person_for_lead(payload)
    today = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    lead_name = f"ЗАКАЗ №{number or order_id} | {person or 'Без имени'} | {today}"
    utm = {
        "utm_source": _clean(payload.get("utmS") or payload.get("utm_source") or payload.get("user_source"), 500),
        "utm_medium": _clean(payload.get("utmM") or payload.get("utm_medium") or payload.get("user_medium"), 500),
        "utm_campaign": _clean(payload.get("utmCa") or payload.get("utm_campaign") or payload.get("user_campaign"), 500),
        "utm_content": _clean(payload.get("utmCo") or payload.get("utm_content") or payload.get("user_content"), 500),
        "utm_term": _clean(payload.get("utmT") or payload.get("utm_term") or payload.get("user_term"), 500),
    }
    yclid = _clean(payload.get("user_yclid") or payload.get("yclid"), 500)
    ym_uid = _clean(payload.get("user_ym_uid") or payload.get("ym_uid") or payload.get("_ym_uid"), 500)
    return {
        "order_id": order_id,
        "number": number,
        "lead_name": lead_name,
        "contact_name": _person_for_contact(payload)[0],
        "first_name": _person_for_contact(payload)[1],
        "last_name": _person_for_contact(payload)[2],
        "email": _clean(payload.get("email"), 500),
        "phone": _phone_text(payload.get("phone")),
        "title": title,
        "payment_link": payment_link,
        "user_link": user_link,
        "order_link": order_link,
        "cost_money": _money_value(payload.get("costMoney") or payload.get("cost_money")),
        "left_cost_money": _money_value(payload.get("leftCostMoney") or payload.get("left_cost_money")),
        "payed_money": _money_value(payload.get("payedMoney") or payload.get("payed_money")),
        "status": _clean(payload.get("status"), 300),
        "utm": utm,
        "yclid": yclid,
        "ym_uid": ym_uid,
        "vk_dialog": f"https://vk.com/gim225075265/convo/{quote(utm['utm_term'])}" if utm["utm_term"] else "",
        "raw": _mask_secret(payload),
    }


def _payload_from_customer_db(fields: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "number": fields.get("number"),
        "id": fields.get("gc_user_id") or fields.get("id"),
        "order_id": fields.get("order_id"),
        "positions": fields.get("positions"),
        "offers": fields.get("offers"),
        "costMoney": fields.get("cost_money"),
        "leftCostMoney": fields.get("left_cost_money"),
        "payedMoney": fields.get("payed_money"),
        "status": fields.get("status"),
        "paymentLink": fields.get("payment_link"),
        "firstName": fields.get("first_name"),
        "lastName": fields.get("last_name"),
        "name": fields.get("name"),
        "email": fields.get("email"),
        "phone": fields.get("phone"),
        "manager_name": fields.get("manager_name"),
        "manager_email": fields.get("manager_email"),
        "manager_phone": fields.get("manager_phone"),
        "avatarUrl": fields.get("avatar_url"),
        "utmS": fields.get("utm_source"),
        "utmM": fields.get("utm_medium"),
        "utmCa": fields.get("utm_campaign"),
        "utmCo": fields.get("utm_content"),
        "utmT": fields.get("utm_term") or fields.get("vk_id"),
        "user_yclid": fields.get("user_yclid") or fields.get("yclid"),
        "user_ym_uid": fields.get("user_ym_uid") or fields.get("ym_uid"),
        "user_source": fields.get("user_source"),
        "user_content": fields.get("user_content"),
        "user_campaign": fields.get("user_campaign"),
        "user_term": fields.get("user_term"),
        "user_medium": fields.get("user_medium"),
    }
    if not _flatten_text(payload.get("positions")) and fields.get("title"):
        payload["positions"] = fields.get("title")
    for key, value in fields.items():
        payload.setdefault(key, value)
    return payload


def _customer_db_path() -> Path:
    env_path = _env()["customer_db_path"]
    if env_path:
        return Path(env_path)
    if not _module_dir:
        raise RuntimeError("module context is not initialized")
    candidates = [
        _module_dir.parent / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


async def _amo_request(method: str, path: str, settings: dict[str, str], payload: Any = None) -> tuple[Any, str, int]:
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы", 0
    headers = {"Authorization": f"Bearer {env['amo_token']}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            resp = await client.request(method, env["amo_base_url"] + path, headers=headers, json=payload)
        if resp.status_code >= 400:
            return None, f"amoCRM HTTP {resp.status_code}: {resp.text[:1000]}", resp.status_code
        if not resp.text:
            return {}, "", resp.status_code
        return resp.json(), "", resp.status_code
    except Exception as exc:
        return None, str(exc), 0


async def _amo_fields(entity: str, settings: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    if entity in _field_cache:
        return _field_cache[entity], ""
    body, error, _ = await _amo_request("GET", f"/api/v4/{entity}/custom_fields", settings)
    if error:
        return [], error
    fields = ((body or {}).get("_embedded") or {}).get("custom_fields") or []
    result = [field for field in fields if isinstance(field, dict)]
    _field_cache[entity] = result
    return result, ""


def _field_matches(field: dict[str, Any], name: str, code: str = "", field_type: str = "") -> bool:
    if code and _clean(field.get("code")).upper() == code.upper():
        return True
    if _clean(field.get("name")).casefold() != name.casefold():
        return False
    if field_type and _clean(field.get("type")) != field_type:
        return False
    return True


def _lead_field_values(fields: list[dict[str, Any]], order: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []

    def add(name: str, value: Any, field_type: str = "", code: str = "", all_matches: bool = False) -> None:
        if value is None or value == "":
            return
        matches = [field for field in fields if _field_matches(field, name, code, field_type)]
        if not matches:
            return
        for field in (matches if all_matches else matches[:1]):
            item = {"field_id": int(field["id"]), "values": [{"value": value}]}
            values.append(item)

    add("№ ГК", order["number"])
    add("Пользователь в ГК", order["user_link"])
    add("Ссылка на оплату", order["payment_link"])
    add("Заказ в ГК", order["order_link"])
    add("Название тарифа", order["title"])
    add("Оплачено", order["payed_money"])
    add("Осталось оплатить", order["left_cost_money"])
    add("Стоимость тарифа", order["cost_money"])
    for order_key, field_name, code in UTM_SPECS:
        value = order["utm"].get(order_key)
        add(field_name, value, "tracking_data", code)
        add(field_name, value, "text")
    add("yclid", order["yclid"], "tracking_data", "YCLID")
    add("_ym_uid", order["ym_uid"], "tracking_data", "_YM_UID")
    add("UTM_YM_UID", order["ym_uid"], code="UTM_YM_UID")
    add("YM_CLIENT_ID", order["ym_uid"], code="YM_CLIENT_ID")
    add("Диалог ВК", order["vk_dialog"])
    return values


def _contact_field_values(fields: list[dict[str, Any]], order: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    phone_field = next((field for field in fields if _field_matches(field, "Телефон", "PHONE")), None)
    email_field = next((field for field in fields if _field_matches(field, "Email", "EMAIL")), None)
    if phone_field and order["phone"]:
        values.append({"field_id": int(phone_field["id"]), "values": [{"value": order["phone"], "enum_code": "WORK"}]})
    if email_field and order["email"]:
        values.append({"field_id": int(email_field["id"]), "values": [{"value": order["email"], "enum_code": "WORK"}]})
    return values


def _tags(settings: dict[str, str]) -> list[dict[str, str]]:
    names = [item.strip() for item in re.split(r"[\n,;]+", settings.get("tags", "")) if item.strip()]
    return [{"name": name} for name in names]


async def _mapped_lead_id(order: dict[str, Any]) -> str:
    keys = [f"order:{order['order_id']}", f"number:{order['number']}"]
    keys = [key for key in keys if not key.endswith(":")]
    if not keys:
        return ""
    placeholders = ",".join(["?"] * len(keys))
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        cur = await db.execute(
            f"SELECT lead_id FROM order_map WHERE order_key IN ({placeholders}) AND lead_id<>'' ORDER BY updated_at DESC LIMIT 1",
            tuple(keys),
        )
        row = await cur.fetchone()
    return _clean(row[0] if row else "", 64)


async def _remember_lead(order: dict[str, Any], lead_id: str) -> None:
    pairs = [(f"order:{order['order_id']}", lead_id), (f"number:{order['number']}", lead_id)]
    pairs = [(key, value) for key, value in pairs if not key.endswith(":") and value]
    if not pairs:
        return
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        for key, value in pairs:
            await db.execute(
                """
                INSERT INTO order_map(order_key,lead_id,updated_at) VALUES(?,?,?)
                ON CONFLICT(order_key) DO UPDATE SET lead_id=excluded.lead_id, updated_at=excluded.updated_at
                """,
                (key, value, _now()),
            )
        await db.commit()


def _order_source_value(order: dict[str, Any], source: Any) -> str:
    key = _clean(source, 120).strip("{} ").strip()
    if not key:
        return ""
    direct = {
        "number": order.get("number"),
        "order_id": order.get("order_id"),
        "id": order.get("order_id"),
        "gc_user_id": ((order.get("raw") or {}) if isinstance(order.get("raw"), dict) else {}).get("id"),
        "name": order.get("contact_name") or order.get("lead_name"),
        "contact_name": order.get("contact_name"),
        "lead_name": order.get("lead_name"),
        "phone": order.get("phone"),
        "email": order.get("email"),
        "title": order.get("title"),
        "payment_link": order.get("payment_link"),
        "user_link": order.get("user_link"),
        "order_link": order.get("order_link"),
        "cost_money": order.get("cost_money"),
        "left_cost_money": order.get("left_cost_money"),
        "payed_money": order.get("payed_money"),
        "status": order.get("status"),
        "yclid": order.get("yclid"),
        "ym_uid": order.get("ym_uid"),
        "_ym_uid": order.get("ym_uid"),
        "vk_dialog": order.get("vk_dialog"),
    }
    if key in direct:
        return _clean(direct[key], 500)
    if key.startswith("utm."):
        return _clean((order.get("utm") or {}).get(key.split(".", 1)[1]), 500)
    if key.startswith("raw."):
        raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
        return _clean(raw.get(key.split(".", 1)[1]), 500)
    return _clean((order.get("utm") or {}).get(key) or order.get(key), 500)


def _compare_value(left: Any, right: Any) -> bool:
    a = _clean(left, 1000)
    b = _clean(right, 1000)
    if not a or not b:
        return False
    if a == b or a.casefold() == b.casefold():
        return True
    digits_a = re.sub(r"\D+", "", a)
    digits_b = re.sub(r"\D+", "", b)
    return bool(digits_a and digits_b and len(digits_a) >= 6 and digits_a == digits_b)


def _field_rule_matches(field: dict[str, Any], rule: dict[str, str]) -> bool:
    field_id = _clean(rule.get("field_id"), 64)
    field_code = _clean(rule.get("field_code"), 120).upper()
    field_name = _clean(rule.get("field"), 300).casefold()
    if field_id and _clean(field.get("field_id") or field.get("id"), 64) == field_id:
        return True
    if field_code and _clean(field.get("field_code") or field.get("code"), 120).upper() == field_code:
        return True
    return bool(field_name and _clean(field.get("field_name") or field.get("name"), 300).casefold() == field_name)


def _entity_rule_values(entity: dict[str, Any], rule: dict[str, str]) -> list[str]:
    values: list[str] = []
    field_name = _clean(rule.get("field"), 300).casefold()
    if field_name in {"id", "name", "price", "responsible_user_id", "pipeline_id", "status_id"}:
        values.append(_clean(entity.get(field_name), 500))
    for field in entity.get("custom_fields_values") or []:
        if not isinstance(field, dict) or not _field_rule_matches(field, rule):
            continue
        for item in field.get("values") or []:
            if isinstance(item, dict):
                values.append(_clean(item.get("value"), 1000))
    return [value for value in values if value]


async def _contact_linked_lead_id(contact: dict[str, Any], settings: dict[str, str]) -> str:
    for lead in (((contact.get("_embedded") or {}).get("leads")) or []):
        lead_id = _clean((lead or {}).get("id"), 64)
        if lead_id:
            return lead_id
    contact_id = _clean(contact.get("id"), 64)
    if not contact_id:
        return ""
    body, error, _ = await _amo_request("GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", settings)
    if error:
        return ""
    for link in (((body or {}).get("_embedded") or {}).get("links") or []):
        lead_id = _clean((link or {}).get("to_entity_id"), 64)
        if lead_id:
            return lead_id
    return ""


async def _find_existing_lead(order: dict[str, Any], settings: dict[str, str], binding: dict[str, Any]) -> tuple[str, str]:
    mapped = await _mapped_lead_id(order)
    if mapped:
        body, error, _ = await _amo_request("GET", f"/api/v4/leads/{mapped}", settings)
        if body and not error:
            return mapped, "local_map"
    entity = _clean_duplicate_search_entity(binding.get("duplicate_search_entity"))
    rules = _duplicate_rules_payload(binding.get("duplicate_search_fields") or binding.get("duplicate_search_fields_json"))
    for rule in rules:
        query = _order_source_value(order, rule.get("source"))
        if not query:
            continue
        path = f"/api/v4/{entity}?query={quote(query)}"
        if entity == "contacts":
            path += "&with=leads"
        body, error, _ = await _amo_request("GET", path, settings)
        if error:
            return "", error
        items = (((body or {}).get("_embedded") or {}).get(entity)) or []
        for item in items:
            if not any(_compare_value(value, query) for value in _entity_rule_values(item, rule)):
                continue
            if entity == "leads":
                lead_id = _clean(item.get("id"), 64)
            else:
                lead_id = await _contact_linked_lead_id(item, settings)
            if lead_id:
                await _remember_lead(order, lead_id)
                field = rule.get("field_id") or rule.get("field_code") or rule.get("field")
                return lead_id, f"{entity}:{field}"
    return "", ""


async def _create_lead(order: dict[str, Any], settings: dict[str, str], binding: dict[str, Any]) -> tuple[dict[str, Any], str]:
    lead_fields, error = await _amo_fields("leads", settings)
    if error:
        return {}, error
    contact_fields, error = await _amo_fields("contacts", settings)
    if error:
        return {}, error
    contact: dict[str, Any] = {"name": order["contact_name"] or order["lead_name"]}
    if order["first_name"]:
        contact["first_name"] = order["first_name"]
    if order["last_name"]:
        contact["last_name"] = order["last_name"]
    contact_custom = _contact_field_values(contact_fields, order)
    if contact_custom:
        contact["custom_fields_values"] = contact_custom
    lead: dict[str, Any] = {
        "name": order["lead_name"],
        "price": 0,
        "custom_fields_values": _lead_field_values(lead_fields, order),
        "_embedded": {"tags": _tags(settings), "contacts": [contact]},
    }
    for setting_key in ("pipeline_id", "status_id", "responsible_user_id"):
        value = _int_or_none(binding.get(setting_key) or settings.get(setting_key))
        if value:
            lead[setting_key] = value
    body, error, _ = await _amo_request("POST", "/api/v4/leads/complex", settings, [lead])
    if error:
        return {}, error
    item = body[0] if isinstance(body, list) and body else body
    lead_id = _clean((item or {}).get("id"), 64)
    embedded_contacts = ((item or {}).get("_embedded") or {}).get("contacts") or [{}]
    contact_id = _clean((item or {}).get("contact_id") or embedded_contacts[0].get("id"), 64)
    await _remember_lead(order, lead_id)
    return {"lead_id": lead_id, "contact_id": contact_id, "response": body}, ""


async def _update_lead(lead_id: str, order: dict[str, Any], settings: dict[str, str], binding: dict[str, Any]) -> tuple[dict[str, Any], str]:
    lead_fields, error = await _amo_fields("leads", settings)
    if error:
        return {}, error
    payload: dict[str, Any] = {
        "name": order["lead_name"],
        "price": 0,
        "custom_fields_values": _lead_field_values(lead_fields, order),
        "_embedded": {"tags": _tags(settings)},
    }
    for setting_key in ("pipeline_id", "status_id", "responsible_user_id"):
        value = _int_or_none(binding.get(setting_key) or settings.get(setting_key))
        if value:
            payload[setting_key] = value
    body, error, _ = await _amo_request("PATCH", f"/api/v4/leads/{lead_id}", settings, payload)
    if error:
        return {}, error
    await _remember_lead(order, lead_id)
    return {"lead_id": lead_id, "response": body}, ""


def _format_task_text(template: str, order: dict[str, Any]) -> str:
    values = {
        "number": order.get("number", ""),
        "order_id": order.get("order_id", ""),
        "name": order.get("contact_name") or order.get("lead_name") or "",
        "phone": order.get("phone", ""),
        "email": order.get("email", ""),
        "payment_link": order.get("payment_link", ""),
        "title": order.get("title", ""),
        "cost_money": order.get("cost_money", ""),
        "left_cost_money": order.get("left_cost_money", ""),
        "payed_money": order.get("payed_money", ""),
        "status": order.get("status", ""),
    }
    text = template or "Связаться по заказу GetCourse №{number}"
    for key, value in values.items():
        text = text.replace("{" + key + "}", _clean(value, 500))
    return _clean(text, 2000)


async def _create_task_for_lead(
    lead_id: str,
    order: dict[str, Any],
    settings: dict[str, str],
    binding: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    if not int(binding.get("task_enabled") or 0):
        return {"skipped": True, "reason": "task disabled"}, ""
    lead_id_int = _int_or_none(lead_id)
    if not lead_id_int:
        return {}, "lead_id пустой для задачи"
    task_text = _format_task_text(_clean(binding.get("task_text"), 2000), order)
    if not task_text:
        return {}, "текст задачи пустой"
    try:
        due_minutes = max(1, min(60 * 24 * 30, int(binding.get("task_due_minutes") or 60)))
    except Exception:
        due_minutes = 60
    task: dict[str, Any] = {
        "entity_id": lead_id_int,
        "entity_type": "leads",
        "task_type_id": int(binding.get("task_type_id") or 1),
        "text": task_text,
        "complete_till": int(time.time()) + due_minutes * 60,
    }
    responsible_id = _int_or_none(binding.get("task_responsible_user_id")) or _int_or_none(binding.get("responsible_user_id"))
    if responsible_id:
        task["responsible_user_id"] = responsible_id
    body, error, _ = await _amo_request("POST", "/api/v4/tasks", settings, [task])
    if error:
        return {"request": task}, error
    task_id = ""
    try:
        task_id = _clean((((body or {}).get("_embedded") or {}).get("tasks") or [{}])[0].get("id"), 64)
    except Exception:
        task_id = ""
    return {"task_id": task_id, "request": task, "response": body}, ""


async def _store_event(data: dict[str, Any]) -> int:
    keys = ["method", "order_id", "number", "lead_id", "contact_id", "action", "success", "ignored", "error", "details", "raw_payload"]
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        cur = await db.execute(
            f"INSERT INTO events({','.join(keys)}) VALUES({','.join(['?'] * len(keys))})",
            tuple(data.get(key, "") for key in keys),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _process_order_payload(
    payload: dict[str, Any],
    raw_payload: str,
    method: str,
    process: str = "",
) -> dict[str, Any]:
    settings = await _settings_map()
    order = _normalize_order(payload, settings)
    if process:
        order["process"] = process
    if _bindings_paused(settings):
        base_event = {
            "method": method,
            "order_id": order["order_id"],
            "number": order["number"],
            "lead_id": "",
            "contact_id": "",
            "action": "bindings_paused",
            "success": 0,
            "ignored": 1,
            "error": "связки на паузе",
            "details": json.dumps({"order": order}, ensure_ascii=False),
            "raw_payload": raw_payload,
        }
        event_id = await _store_event(base_event)
        return {"ok": True, "stored": False, "event_id": event_id, "ignored": True, "error": "связки на паузе", "status_code": 200}
    binding = await _binding_for_process(order.get("process") or payload.get("payment_state") or payload.get("status") or "", settings)
    base_event = {
        "method": method,
        "order_id": order["order_id"],
        "number": order["number"],
        "lead_id": "",
        "contact_id": "",
        "action": "",
        "success": 0,
        "ignored": 0,
        "error": "",
        "details": "",
        "raw_payload": raw_payload,
    }
    if not (order["order_id"] or order["number"]):
        base_event["ignored"] = 1
        base_event["error"] = "order_id или number обязателен"
        base_event["details"] = json.dumps({"order": order}, ensure_ascii=False)
        event_id = await _store_event(base_event)
        return {"ok": False, "stored": False, "event_id": event_id, "error": base_event["error"], "status_code": 200}

    existing_id = ""
    existing_source = ""
    duplicate_policy = _clean_duplicate_policy(binding.get("duplicate_policy") or settings.get("duplicate_policy"))
    if duplicate_policy != "create":
        existing_id, existing_source = await _find_existing_lead(order, settings, binding)
        if existing_id and not existing_source:
            existing_source = "unknown"
    if existing_id and duplicate_policy == "skip":
        result, error = {"lead_id": existing_id, "skipped_duplicate": True}, ""
        action = "skipped_duplicate"
        base_event["ignored"] = 1
    elif existing_id:
        result, error = await _update_lead(existing_id, order, settings, binding)
        action = "updated"
    else:
        result, error = await _create_lead(order, settings, binding)
        action = "created"
    task_result: dict[str, Any] = {"skipped": True}
    task_error = ""
    if not error and action != "skipped_duplicate":
        task_result, task_error = await _create_task_for_lead(_clean(result.get("lead_id") or existing_id, 64), order, settings, binding)
        if task_error:
            error = f"task: {task_error}"
    base_event["action"] = action
    base_event["lead_id"] = _clean(result.get("lead_id") or existing_id, 64) if result else existing_id
    base_event["contact_id"] = _clean(result.get("contact_id"), 64) if result else ""
    base_event["success"] = 0 if error else 1
    base_event["error"] = error
    base_event["details"] = json.dumps(
        {
            "order": order,
            "binding": {k: v for k, v in binding.items() if k != "task_text"},
            "binding_task_text": binding.get("task_text", ""),
            "duplicate_policy": duplicate_policy,
            "existing_source": existing_source,
            "amo": result,
            "task": task_result,
        },
        ensure_ascii=False,
    )
    event_id = await _store_event(base_event)
    if error:
        _log("warning", "GetCourse order %s/%s -> amoCRM FAIL: %s", order["number"], order["order_id"], error)
    else:
        _log("info", "GetCourse order %s/%s -> amoCRM lead %s %s", order["number"], order["order_id"], base_event["lead_id"], action)
    return {
        "ok": not bool(error),
        "event_id": event_id,
        "action": action,
        "lead_id": base_event["lead_id"],
        "contact_id": base_event["contact_id"],
        "error": error,
        "status_code": 200 if not error else 502,
    }


async def _process_webhook(request: Request, process: str = "") -> JSONResponse:
    settings = await _settings_map()
    payload, raw_payload = await _read_payload(request)
    if not _secret_ok(request, settings):
        event_id = await _store_event({
            "method": request.method,
            "ignored": 1,
            "success": 0,
            "error": "invalid secret",
            "details": "{}",
            "raw_payload": raw_payload,
        })
        return JSONResponse({"ok": False, "stored": False, "event_id": event_id, "error": "invalid secret"}, status_code=200)
    result = await _process_order_payload(payload, raw_payload, request.method, process)
    status_code = int(result.pop("status_code", 200))
    return JSONResponse(result, status_code=status_code)


async def _set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await db.commit()


def _bindings_paused(settings: dict[str, str]) -> bool:
    return str(settings.get("bindings_paused") or "0").strip().lower() in {"1", "true", "yes", "on", "да"}


async def _customer_db_rows(limit: int = 5000) -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT id, platform_id, custom_fields, updated_at
                FROM cdb_getcourse_orders
                ORDER BY updated_at ASC, id ASC
                LIMIT ?
                """,
                (max(1, min(20000, int(limit))),),
            )
            return [dict(row) for row in await cur.fetchall()]
    except Exception as exc:
        _log("warning", "customer-db getcourse_orders read failed: %s", exc)
        return []


async def _sync_state_for(record_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not record_ids:
        return {}
    placeholders = ",".join(["?"] * len(record_ids))
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM cdb_sync WHERE source_record_id IN ({placeholders})",
            tuple(record_ids),
        )
        return {int(row["source_record_id"]): dict(row) for row in await cur.fetchall()}


async def _mark_cdb_sync(record_id: int, updated_at: str, source_hash: str, result: dict[str, Any]) -> None:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            """
            INSERT INTO cdb_sync(source_record_id,source_updated_at,source_hash,lead_id,success,error,last_synced_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(source_record_id) DO UPDATE SET
                source_updated_at=excluded.source_updated_at,
                source_hash=excluded.source_hash,
                lead_id=excluded.lead_id,
                success=excluded.success,
                error=excluded.error,
                last_synced_at=excluded.last_synced_at
            """,
            (
                record_id,
                updated_at,
                source_hash,
                _clean(result.get("lead_id"), 64),
                1 if result.get("ok") else 0,
                _clean(result.get("error"), 2000),
                _now(),
            ),
        )
        await db.commit()


async def _bootstrap_customer_db_sync(rows: list[dict[str, Any]]) -> int:
    states = await _sync_state_for([int(row["id"]) for row in rows])
    count = 0
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        for row in rows:
            record_id = int(row["id"])
            if record_id in states:
                continue
            source_hash = _clean(row.get("custom_fields"), 200000)
            await db.execute(
                """
                INSERT OR IGNORE INTO cdb_sync(source_record_id,source_updated_at,source_hash,success,error,last_synced_at)
                VALUES(?,?,?,?,?,?)
                """,
                (record_id, _clean(row.get("updated_at"), 80), source_hash, 1, "bootstrapped without amo sync", _now()),
            )
            count += 1
        await db.execute(
            "INSERT INTO settings(key,value) VALUES('cdb_sync_bootstrapped','1') ON CONFLICT(key) DO UPDATE SET value='1'"
        )
        await db.commit()
    if count:
        _log("info", "customer-db sync bootstrap marked %s existing GetCourse orders as seen", count)
    return count


async def _sync_customer_db_once(backfill: bool = False, limit: int = 200) -> dict[str, Any]:
    settings = await _settings_map()
    if _bindings_paused(settings):
        return {"ok": True, "paused": True, "source_rows": 0, "processed": 0, "bootstrapped": 0}
    rows = await _customer_db_rows()
    if not rows:
        if settings.get("cdb_sync_bootstrapped") != "1" and not backfill:
            await _set_setting("cdb_sync_bootstrapped", "1")
        return {"ok": True, "source_rows": 0, "processed": 0, "bootstrapped": 0}
    if settings.get("cdb_sync_bootstrapped") != "1" and not backfill:
        bootstrapped = await _bootstrap_customer_db_sync(rows)
        return {"ok": True, "source_rows": len(rows), "processed": 0, "bootstrapped": bootstrapped}
    states = await _sync_state_for([int(row["id"]) for row in rows])
    processed = 0
    errors = []
    for row in rows:
        if processed >= max(1, min(1000, int(limit))):
            break
        record_id = int(row["id"])
        updated_at = _clean(row.get("updated_at"), 80)
        source_hash = _clean(row.get("custom_fields"), 200000)
        state = states.get(record_id)
        if state and state.get("source_updated_at") == updated_at and state.get("source_hash") == source_hash:
            continue
        try:
            fields = json.loads(row.get("custom_fields") or "{}")
            if not isinstance(fields, dict):
                raise ValueError("custom_fields is not an object")
            payload = _payload_from_customer_db(fields)
            raw_payload = json.dumps({"source": "customer-db", "record_id": record_id, "custom_fields": fields}, ensure_ascii=False)
            process = _clean(fields.get("payment_state"), 80)
            result = await _process_order_payload(payload, raw_payload, "customer-db", process)
            await _mark_cdb_sync(record_id, updated_at, source_hash, result)
            processed += 1
            if not result.get("ok"):
                errors.append({"record_id": record_id, "error": result.get("error")})
        except Exception as exc:
            error = str(exc)
            await _mark_cdb_sync(record_id, updated_at, source_hash, {"ok": False, "error": error})
            errors.append({"record_id": record_id, "error": error})
            processed += 1
            _log("warning", "customer-db GetCourse order %s sync failed: %s", record_id, error)
    return {"ok": not errors, "source_rows": len(rows), "processed": processed, "errors": errors[:20]}


async def _customer_db_sync_loop() -> None:
    await asyncio.sleep(5)
    while True:
        sleep_seconds = 10
        try:
            settings = await _settings_map()
            try:
                sleep_seconds = max(5, min(300, int(float(settings.get("cdb_poll_seconds") or "10"))))
            except Exception:
                sleep_seconds = 10
            env = _env()
            if settings.get("cdb_sync_enabled") == "1" and env["amo_base_url"] and env["amo_token"]:
                await _sync_customer_db_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "customer-db sync loop failed: %s", exc)
        await asyncio.sleep(sleep_seconds)


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID}


@router.get("/env-status")
async def env_status(request: Request):
    await _require_panel_user(request)
    env = _env()
    settings = await _settings_map()
    customer_db_path = _customer_db_path()
    return {
        "AMO_BASE_URL": bool(env["amo_base_url"]),
        "AMO_ACCESS_TOKEN": bool(env["amo_token"]),
        "webhook_secret": bool(settings.get("webhook_secret")),
        "customer_db_path": str(customer_db_path),
        "customer_db_ready": customer_db_path.exists(),
        "customer_db_sync_enabled": settings.get("cdb_sync_enabled") == "1",
        "bindings_paused": _bindings_paused(settings),
        "ready": bool(env["amo_base_url"] and env["amo_token"]),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    env = _env()
    secret = settings.get("webhook_secret", "")
    base = "/nexus/getcourse-amocrm/api/webhook"
    params = _getcourse_url_params(secret)
    webhook_urls = {
        "created": f"https://junior.sobakovod.pro{base}/created?{params}",
        "partial": f"https://junior.sobakovod.pro{base}/partial?{params}",
        "paid": f"https://junior.sobakovod.pro{base}/paid?{params}",
        "legacy": f"https://junior.sobakovod.pro{base}?{params}",
    }
    return {
        **settings,
        "webhook_secret_source": "env" if env["webhook_secret"] else "db",
        "amo_base_url": env["amo_base_url"],
        "has_amo_token": bool(env["amo_token"]),
        "webhook_path": f"{base}?{params}",
        "webhook_url": f"https://junior.sobakovod.pro{base}?{params}",
        "webhook_urls": webhook_urls,
    }


@router.post("/settings")
async def post_settings(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.get("/amo/catalog")
async def amo_catalog(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    pipelines_body, pipelines_error, _ = await _amo_request("GET", "/api/v4/leads/pipelines", settings)
    lead_fields, lead_error = await _amo_fields("leads", settings)
    contact_fields, contact_error = await _amo_fields("contacts", settings)
    if pipelines_error or lead_error or contact_error:
        return JSONResponse(
            {"error": pipelines_error or lead_error or contact_error, "pipelines": [], "lead_fields": [], "contact_fields": []},
            status_code=502,
        )
    return {
        "pipelines": ((pipelines_body or {}).get("_embedded") or {}).get("pipelines") or [],
        "lead_fields": lead_fields,
        "contact_fields": contact_fields,
    }


@router.get("/amo/users")
async def amo_users(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    body, error, _ = await _amo_request("GET", "/api/v4/users", settings)
    if error:
        return JSONResponse({"error": error, "users": []}, status_code=502)
    users = []
    for user in (((body or {}).get("_embedded") or {}).get("users") or []):
        if not isinstance(user, dict):
            continue
        users.append({
            "id": _clean(user.get("id"), 64),
            "name": _clean(user.get("name"), 300),
            "email": _clean(user.get("email"), 300),
            "is_active": bool(user.get("is_active", True)),
        })
    return {"users": users}


@router.get("/bindings")
async def list_bindings(request: Request):
    await _require_panel_user(request)
    return await _bindings()


@router.post("/bindings")
async def save_binding(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    if not isinstance(data, dict):
        return JSONResponse({"error": "ожидался JSON object"}, status_code=400)
    return await _save_binding(data)


@router.post("/bindings/pause")
async def pause_bindings(request: Request):
    await _require_panel_user(request)
    await _set_setting("bindings_paused", "1")
    return {"ok": True, "bindings_paused": True}


@router.post("/bindings/resume")
async def resume_bindings(request: Request):
    await _require_panel_user(request)
    await _set_setting("bindings_paused", "0")
    return {"ok": True, "bindings_paused": False}


@router.post("/bindings/toggle-pause")
async def toggle_bindings_pause(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    paused = not _bindings_paused(settings)
    await _set_setting("bindings_paused", "1" if paused else "0")
    return {"ok": True, "bindings_paused": paused}


@router.get("/sync/customer-db/status")
async def customer_db_sync_status(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    rows = await _customer_db_rows(limit=1)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        total = (await (await db.execute("SELECT COUNT(*) FROM cdb_sync")).fetchone())[0]
        success = (await (await db.execute("SELECT COUNT(*) FROM cdb_sync WHERE success=1")).fetchone())[0]
        failed = (await (await db.execute("SELECT COUNT(*) FROM cdb_sync WHERE success=0")).fetchone())[0]
    return {
        "enabled": settings.get("cdb_sync_enabled") == "1",
        "bindings_paused": _bindings_paused(settings),
        "bootstrapped": settings.get("cdb_sync_bootstrapped") == "1",
        "customer_db_path": str(_customer_db_path()),
        "customer_db_ready": _customer_db_path().exists(),
        "source_has_rows": bool(rows),
        "tracked_rows": total,
        "success": success,
        "failed": failed,
    }


@router.post("/sync/customer-db/run")
async def customer_db_sync_run(request: Request, backfill: int = 0, limit: int = 50):
    await _require_panel_user(request)
    return await _sync_customer_db_once(backfill=bool(backfill), limit=limit)


@router.api_route("/webhook", methods=["GET", "POST"])
async def webhook(request: Request):
    return await _process_webhook(request)


@router.api_route("/webhook/created", methods=["GET", "POST"])
async def webhook_created(request: Request):
    return await _process_webhook(request, "created")


@router.api_route("/webhook/partial", methods=["GET", "POST"])
async def webhook_partial(request: Request):
    return await _process_webhook(request, "partial")


@router.api_route("/webhook/paid", methods=["GET", "POST"])
async def webhook_paid(request: Request):
    return await _process_webhook(request, "paid")


@router.get("/events")
async def list_events(request: Request, limit: int = 200, result: str = "all"):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit)))
    where = ""
    if result == "ok":
        where = "WHERE success=1"
    elif result == "error":
        where = "WHERE success=0 AND ignored=0"
    elif result == "ignored":
        where = "WHERE ignored=1"
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(row) for row in await cur.fetchall()]


@router.get("/events/{event_id}")
async def get_event(event_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Не найдено")
    data = dict(row)
    for key in ("details", "raw_payload"):
        try:
            data[key] = json.loads(data[key]) if data[key] else {}
        except Exception:
            data[key] = {"raw": data[key]}
    return data


@router.get("/stats")
async def stats(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        total = (await (await db.execute("SELECT COUNT(*) FROM events")).fetchone())[0]
        success = (await (await db.execute("SELECT COUNT(*) FROM events WHERE success=1")).fetchone())[0]
        errors = (await (await db.execute("SELECT COUNT(*) FROM events WHERE success=0 AND ignored=0")).fetchone())[0]
        ignored = (await (await db.execute("SELECT COUNT(*) FROM events WHERE ignored=1")).fetchone())[0]
        mapped = (await (await db.execute("SELECT COUNT(*) FROM order_map")).fetchone())[0]
        cdb_tracked = (await (await db.execute("SELECT COUNT(*) FROM cdb_sync")).fetchone())[0]
    settings = await _settings_map()
    return {
        "events": total,
        "success": success,
        "errors": errors,
        "ignored": ignored,
        "mapped_orders": mapped,
        "cdb_tracked": cdb_tracked,
        "bindings_paused": _bindings_paused(settings),
    }


def _getcourse_url_params(secret: str) -> str:
    pairs = [
        ("secret", secret),
        ("number", "{object.number}"),
        ("id", "{object.user.id}"),
        ("order_id", "{object.id}"),
        ("positions", "{object.positions}"),
        ("costMoney", "{object.cost_money}"),
        ("leftCostMoney", "{object.left_cost_money}"),
        ("payedMoney", "{object.payed_money}"),
        ("status", "{object.status}"),
        ("paymentLink", "{object.payment_link}"),
        ("firstName", "{object.user.first_name}"),
        ("lastName", "{object.user.last_name}"),
        ("name", "{object.user.name}"),
        ("email", "{object.user.email}"),
        ("phone", "{object.user.phone}"),
        ("manager_name", "{object.manager}"),
        ("manager_email", "{object.manager_email}"),
        ("manager_phone", "{object.manager_phone}"),
        ("offers", "{object.offers}"),
        ("avatarUrl", "{object.user.avatar_url}"),
        ("utmS", "{object.user.create_session.utm_source}"),
        ("utmM", "{object.user.create_session.utm_medium}"),
        ("utmCa", "{object.user.create_session.utm_campaign}"),
        ("utmCo", "{object.user.create_session.utm_content}"),
        ("utmT", "{object.user.create_session.utm_term}"),
        ("user_yclid", "{object.user.yclid}"),
        ("user_ym_uid", "{object.user.ym_uid}"),
        ("user_source", "{object.user.source}"),
        ("user_content", "{object.user.content}"),
        ("user_campaign", "{object.user.campaign}"),
        ("user_term", "{object.user.term}"),
        ("user_medium", "{object.user.medium}"),
    ]
    return urlencode(pairs)
