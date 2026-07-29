from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
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
_sync_lock = asyncio.Lock()
_order_process_lock = asyncio.Lock()

MODULE_ID = "getcourse-amocrm"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
LEGACY_DEFAULT_NOTE_TEMPLATE = (
    "ГЕТКУРС ЗАКАЗ №{payment_number}\n"
    "Название тарифа {positions}\n"
    "стоимость тарифа: {costMoney}\n"
    "осталось оплатить: {leftCostMoney}\n"
    "оплачено: {payedMoney}\n"
    "статус платежа: {payment_status}\n"
    "ссылка на оплату: {paymentLink}\n"
    "Имя: {name} E-mail: {email}\n"
    "Телефон: {phone}\n"
    "\nТестовые примечания\n"
    "{pay_field_user_ym_uid}\n"
    "{reg_field_user_ym_uid}"
)
DEFAULT_NOTE_TEMPLATE = (
    "ГЕТКУРС ЗАКАЗ №{payment_number}\n"
    "ссылка на оплату: {paymentLink}\n"
    "Название тарифа {positions}\n"
    "стоимость тарифа: {costMoney}\n"
    "осталось оплатить: {leftCostMoney}\n"
    "оплачено: {payedMoney}\n"
    "статус платежа: {payment_status}\n"
    "Имя: {name} E-mail: {email}\n"
    "Телефон: {phone}\n"
    "\nТестовые примечания\n"
    "{pay_field_user_ym_uid}\n"
    "{reg_field_user_ym_uid}"
)
DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "pipeline_id": "10566818",
    "status_id": "83350598",
    "responsible_user_id": "6269974",
    "responsible_user_ids_json": "[]",
    "round_robin_cursor": "0",
    "getcourse_base_url": "https://club.sobakovod.pro",
    "request_timeout": "15",
    "duplicate_policy": "update",
    "tags": "GC\nАвтооплата",
    "bindings_paused": "0",
    "cdb_sync_enabled": "1",
    "cdb_poll_seconds": "10",
    "cdb_sync_bootstrapped": "0",
    "sample_preset_json": "{}",
    "lead_name_template": "ЗАКАЗ №{payment_number} | {name} | {date_add}",
    "note_template": DEFAULT_NOTE_TEMPLATE,
    "budget_source": "paid",
    "minicourse_curator_mediums": "irina\nslava\nnastasia",
}

MINICOURSE_PIPELINE_ID = "8493006"
MINICOURSE_PAID_STATUS_ID = "69046790"
MINICOURSE_RESPONSIBLE_USER_ID = "6269974"

DEFAULT_BINDINGS = [
    {
        "process": "created",
        "name": "Создан заказ",
        "status_id": "83350598",
        "task_text": "Связаться заказ ГК",
    },
    {
        "process": "partial",
        "name": "Частично оплачен",
        "status_id": "83350598",
        "task_text": "Связаться заказ ГК",
    },
    {
        "process": "paid",
        "name": "Оплачен",
        "status_id": "142",
        "task_text": "Связаться заказ ГК",
    },
    {
        "process": "surcharge_created",
        "name": "Доплата создана",
        "status_id": "83350598",
        "responsible_user_id": "6269974",
        "duplicate_policy": "create",
        "task_enabled": 0,
        "task_text": "",
    },
    {
        "process": "surcharge_paid",
        "name": "Доплата оплачена",
        "status_id": "142",
        "responsible_user_id": "6269974",
        "duplicate_policy": "update",
        "task_enabled": 0,
        "task_text": "",
    },
]

DEFAULT_DUPLICATE_SEARCH_RULES = [
    {"field": "№ ГК", "source": "number"},
]

CDB_VOLATILE_FIELDS = {
    "chat_fields_updated_at",
}

CDB_FILE_IMPORT_SOURCES = {
    "csv_export",
    "csv_import",
    "file_import",
    "getcourse_csv_export",
    "getcourse_csv_import",
    "getcourse_file_import",
}
CDB_PAGE_SIZE = 1000

DEFAULT_MOVABLE_STATUS_IDS = ["83350594", "83350598", "83350602", "83350606", "85041662", "143"]

BINDING_ALIASES = {
    "": "created",
    "created": "created",
    "unpaid": "created",
    "new": "created",
    "partial": "partial",
    "partially_paid": "partial",
    "paid": "paid",
    "surcharge_created": "surcharge_created",
    "surcharge-created": "surcharge_created",
    "surcharge_paid": "surcharge_paid",
    "surcharge-paid": "surcharge_paid",
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


async def shutdown() -> None:
    global _sync_task
    task, _sync_task = _sync_task, None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


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
            CREATE TABLE IF NOT EXISTS round_robin_cursors (
                pool_key TEXT PRIMARY KEY,
                cursor INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
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
                move_from_statuses_json TEXT NOT NULL DEFAULT '',
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
        await db.execute(
            "UPDATE settings SET value=? WHERE key=? AND value=?",
            (DEFAULT_NOTE_TEMPLATE, "note_template", LEGACY_DEFAULT_NOTE_TEMPLATE),
        )
        for item in DEFAULT_BINDINGS:
            await db.execute(
                """
                INSERT OR IGNORE INTO bindings(
                    process,name,pipeline_id,status_id,responsible_user_id,duplicate_policy,
                    duplicate_search_entity,duplicate_search_fields_json,move_from_statuses_json,
                    task_enabled,task_text,task_due_minutes,task_type_id,task_responsible_user_id,active
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                """,
                (
                    item["process"],
                    item["name"],
                    DEFAULT_SETTINGS["pipeline_id"],
                    item.get("status_id") or DEFAULT_SETTINGS["status_id"],
                    item.get("responsible_user_id", ""),
                    item.get("duplicate_policy") or DEFAULT_SETTINGS["duplicate_policy"],
                    "leads",
                    _default_duplicate_rules_json(),
                    json.dumps(DEFAULT_MOVABLE_STATUS_IDS, ensure_ascii=False),
                    int(item.get("task_enabled", 1)),
                    item["task_text"],
                    60,
                    1,
                    "",
                ),
            )
        cur = await db.execute("SELECT value FROM settings WHERE key='binding_responsible_override_v1'")
        if not await cur.fetchone():
            await db.execute(
                "UPDATE bindings SET responsible_user_id='' "
                "WHERE process IN ('created','partial','paid') AND responsible_user_id=?",
                (DEFAULT_SETTINGS["responsible_user_id"],),
            )
            await db.execute(
                "UPDATE bindings SET responsible_user_id=?, "
                "task_due_minutes=CASE WHEN task_due_minutes<=1 THEN 60 ELSE task_due_minutes END "
                "WHERE process IN ('surcharge_created','surcharge_paid')",
                (MINICOURSE_RESPONSIBLE_USER_ID,),
            )
            await db.execute(
                "INSERT INTO settings(key,value) VALUES('binding_responsible_override_v1','1')"
            )
        cur = await db.execute("SELECT value FROM settings WHERE key='binding_responsible_override_v2'")
        if not await cur.fetchone():
            cur = await db.execute("SELECT value FROM settings WHERE key='responsible_user_id'")
            row = await cur.fetchone()
            legacy_responsible = _clean(row[0] if row else "", 64)
            await db.execute(
                "UPDATE bindings SET responsible_user_id='' "
                "WHERE process IN ('created','partial','paid') AND responsible_user_id IN (?,?)",
                (legacy_responsible, DEFAULT_SETTINGS["responsible_user_id"]),
            )
            await db.execute(
                "INSERT INTO settings(key,value) VALUES('binding_responsible_override_v2','1')"
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
    if "move_from_statuses_json" not in columns:
        await db.execute("ALTER TABLE bindings ADD COLUMN move_from_statuses_json TEXT NOT NULL DEFAULT ''")
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
    await db.execute(
        """
        UPDATE bindings
        SET move_from_statuses_json=?
        WHERE move_from_statuses_json IS NULL OR move_from_statuses_json=''
        """,
        (json.dumps(DEFAULT_MOVABLE_STATUS_IDS, ensure_ascii=False),),
    )


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _customer_db_source_hash(value: Any) -> str:
    fields: Any = value
    if isinstance(value, str):
        try:
            fields = json.loads(value or "{}")
        except Exception:
            fields = value.strip()
    if isinstance(fields, dict):
        fields = {key: item for key, item in fields.items() if key not in CDB_VOLATILE_FIELDS}
    canonical = json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "v2:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _customer_db_file_import(fields: Any) -> bool:
    if not isinstance(fields, dict):
        return False
    source = re.sub(r"[^a-z0-9]+", "_", _clean(fields.get("source"), 200).casefold()).strip("_")
    return source in CDB_FILE_IMPORT_SOURCES or any(
        marker in source for marker in ("csv_export", "csv_import", "file_import")
    )


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


def _selected_responsible_ids(settings: dict[str, str], binding: dict[str, Any] | None = None) -> list[str]:
    binding_responsible = _int_or_none((binding or {}).get("responsible_user_id"))
    if binding_responsible:
        return [str(binding_responsible)]
    if (binding or {}).get("fixed_responsible"):
        return []
    try:
        raw = json.loads(settings.get("responsible_user_ids_json") or "[]")
    except Exception:
        raw = []
    result: list[str] = []
    for item in raw if isinstance(raw, list) else []:
        user_id = _int_or_none(item)
        if user_id and str(user_id) not in result:
            result.append(str(user_id))
    fallback = _int_or_none(settings.get("responsible_user_id"))
    if not result and fallback:
        result.append(str(fallback))
    return result


def _active_amo_user_ids(users: list[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for user in users:
        rights = user.get("rights") if isinstance(user.get("rights"), dict) else {}
        if rights.get("is_active", user.get("is_active", True)) and _int_or_none(user.get("id")):
            result.add(str(user["id"]))
    return result


async def _new_responsible(settings: dict[str, str], binding: dict[str, Any]) -> str:
    users = _selected_responsible_ids(settings, binding)
    if not users:
        return ""
    body, error, _ = await _amo_request("GET", "/api/v4/users?limit=250", settings)
    active = None if error else _active_amo_user_ids((((body or {}).get("_embedded") or {}).get("users") or []))
    candidates = [user_id for user_id in users if active is None or user_id in active]
    if not candidates:
        return ""
    pool_key = json.dumps(users, separators=(",", ":"))
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        cur = await db.execute("SELECT cursor FROM round_robin_cursors WHERE pool_key=?", (pool_key,))
        row = await cur.fetchone()
        cursor = int(row[0] or 0) if row else 0
    return candidates[cursor % len(candidates)]


async def _advance_responsible_cursor(settings: dict[str, str], binding: dict[str, Any]) -> None:
    users = _selected_responsible_ids(settings, binding)
    if not users:
        return
    pool_key = json.dumps(users, separators=(",", ":"))
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            "INSERT INTO round_robin_cursors(pool_key,cursor,updated_at) VALUES(?,1,?) "
            "ON CONFLICT(pool_key) DO UPDATE SET cursor=cursor+1,updated_at=excluded.updated_at",
            (pool_key, _now()),
        )
        await db.commit()


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
        "responsible_user_ids_json",
        "getcourse_base_url",
        "request_timeout",
        "duplicate_policy",
        "tags",
        "cdb_sync_enabled",
        "cdb_poll_seconds",
        "lead_name_template",
        "note_template",
        "budget_source",
        "minicourse_curator_mediums",
    }
    clean: dict[str, str] = {}
    for key in allowed:
        if key not in data:
            continue
        value = _clean(data.get(key), 5000)
        if key in {"pipeline_id", "status_id", "responsible_user_id"}:
            value = str(_int_or_none(value) or "")
        elif key == "responsible_user_ids_json":
            try:
                raw_users = json.loads(value) if isinstance(value, str) else value
            except Exception:
                raw_users = []
            users = []
            for item in raw_users if isinstance(raw_users, list) else []:
                user_id = _int_or_none(item)
                if user_id and str(user_id) not in users:
                    users.append(str(user_id))
            value = json.dumps(users, ensure_ascii=False)
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
        elif key == "budget_source":
            value = value if value in {"paid", "cost", "none"} else "paid"
        clean[key] = value
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        for key, value in clean.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    return await _settings_map()


def _binding_process(value: Any) -> str:
    raw = _clean(value, 80).casefold()
    allowed = {"created", "partial", "paid", "surcharge_created", "surcharge_paid", "minicourse_paid"}
    return BINDING_ALIASES.get(raw, raw if raw in allowed else "created")


def _is_created_process(value: Any) -> bool:
    return _binding_process(value) in {"created", "surcharge_created"}


def _is_paid_process(value: Any) -> bool:
    return _binding_process(value) in {"partial", "paid", "surcharge_paid", "minicourse_paid"}


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


def _status_ids_payload(value: Any, default: list[str] | None = None) -> list[str]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except Exception:
            raw = re.split(r"[\s,;]+", raw)
    if not isinstance(raw, list):
        raw = default or []
    result: list[str] = []
    for item in raw:
        status_id = _int_or_none(item)
        if status_id and str(status_id) not in result:
            result.append(str(status_id))
    return result


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
        "move_from_statuses_json": json.dumps(
            _status_ids_payload(
                data.get("move_from_statuses_json", existing.get("move_from_statuses_json")),
                DEFAULT_MOVABLE_STATUS_IDS,
            ),
            ensure_ascii=False,
        ),
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
            ORDER BY CASE process
                WHEN 'created' THEN 1 WHEN 'partial' THEN 2 WHEN 'paid' THEN 3
                WHEN 'surcharge_created' THEN 4 WHEN 'surcharge_paid' THEN 5 ELSE 6 END, id
            """
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["duplicate_search_entity"] = _clean_duplicate_search_entity(row.get("duplicate_search_entity"))
        row["duplicate_search_fields"] = _duplicate_rules_payload(row.get("duplicate_search_fields_json")) or list(DEFAULT_DUPLICATE_SEARCH_RULES)
        row["move_from_statuses"] = _status_ids_payload(row.get("move_from_statuses_json"), DEFAULT_MOVABLE_STATUS_IDS)
    return rows


async def _binding_for_process(process: str, settings: dict[str, str]) -> dict[str, Any]:
    process = _binding_process(process)
    if process == "minicourse_paid":
        return {
            "id": None,
            "process": process,
            "name": "Мини-курс оплачен",
            "pipeline_id": MINICOURSE_PIPELINE_ID,
            "status_id": MINICOURSE_PAID_STATUS_ID,
            "responsible_user_id": MINICOURSE_RESPONSIBLE_USER_ID,
            "fixed_responsible": 1,
            "duplicate_policy": "update",
            "duplicate_search_entity": "leads",
            "duplicate_search_fields_json": _default_duplicate_rules_json(),
            "duplicate_search_fields": list(DEFAULT_DUPLICATE_SEARCH_RULES),
            "move_from_statuses_json": json.dumps([MINICOURSE_PAID_STATUS_ID], ensure_ascii=False),
            "move_from_statuses": [MINICOURSE_PAID_STATUS_ID],
            "task_enabled": 0,
            "task_text": "",
            "task_due_minutes": 60,
            "task_type_id": 1,
            "task_responsible_user_id": "",
            "active": 1,
        }
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
        "responsible_user_id": "",
        "duplicate_policy": settings.get("duplicate_policy", "update"),
        "duplicate_search_entity": "leads",
        "duplicate_search_fields_json": _default_duplicate_rules_json(),
        "duplicate_search_fields": list(DEFAULT_DUPLICATE_SEARCH_RULES),
        "move_from_statuses_json": json.dumps(DEFAULT_MOVABLE_STATUS_IDS),
        "move_from_statuses": list(DEFAULT_MOVABLE_STATUS_IDS),
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
                    duplicate_search_entity=?,duplicate_search_fields_json=?,move_from_statuses_json=?,
                    task_enabled=?,task_text=?,task_due_minutes=?,task_type_id=?,task_responsible_user_id=?,
                    active=?,updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (
                    clean["process"], clean["name"], clean["pipeline_id"], clean["status_id"],
                    clean["responsible_user_id"], clean["duplicate_policy"],
                    clean["duplicate_search_entity"], clean["duplicate_search_fields_json"], clean["move_from_statuses_json"],
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
                    duplicate_search_entity,duplicate_search_fields_json,move_from_statuses_json,
                    task_enabled,task_text,task_due_minutes,task_type_id,task_responsible_user_id,active
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(process) DO UPDATE SET
                    name=excluded.name,pipeline_id=excluded.pipeline_id,status_id=excluded.status_id,
                    responsible_user_id=excluded.responsible_user_id,duplicate_policy=excluded.duplicate_policy,
                    duplicate_search_entity=excluded.duplicate_search_entity,
                    duplicate_search_fields_json=excluded.duplicate_search_fields_json,
                    move_from_statuses_json=excluded.move_from_statuses_json,
                    task_enabled=excluded.task_enabled,task_text=excluded.task_text,
                    task_due_minutes=excluded.task_due_minutes,task_type_id=excluded.task_type_id,
                    task_responsible_user_id=excluded.task_responsible_user_id,active=excluded.active,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                """,
                (
                    clean["process"], clean["name"], clean["pipeline_id"], clean["status_id"],
                    clean["responsible_user_id"], clean["duplicate_policy"],
                    clean["duplicate_search_entity"], clean["duplicate_search_fields_json"], clean["move_from_statuses_json"],
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


def _phone_identity(value: Any) -> str:
    return re.sub(r"\D+", "", _phone_text(value))


def _floating_identity_keys(order: dict[str, Any]) -> list[str]:
    """Stable identity for the one mutable unpaid attempt per person."""
    phone = _phone_identity(order.get("phone"))
    if phone:
        return [f"floating:phone:{phone}"]
    email = _clean(order.get("email"), 500).casefold()
    return [f"floating:email:{email}"] if email else []


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


def _tag_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        values: list[str] = []
        for key, item in value.items():
            if isinstance(item, (bool, int, float)) and bool(item):
                values.append(str(key))
            else:
                values.extend(_tag_names(item))
        return values
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_tag_names(item))
        return values
    return [item.strip() for item in re.split(r"[|,;\n]+", str(value)) if item.strip()]


def _autopayment_match(payload: dict[str, Any], order: dict[str, Any]) -> tuple[bool, str]:
    keyword = "автооплата"
    title_values = (
        order.get("title"),
        payload.get("title"),
        payload.get("order_name"),
        payload.get("name_order"),
        payload.get("positions"),
    )
    if any(keyword in _flatten_text(value).casefold() for value in title_values):
        return True, "title"

    tag_values = []
    for key in ("tags", "order_tags", "tag_names", "deal_tags", "object.tags"):
        tag_values.extend(_tag_names(payload.get(key)))
    if any(name.casefold() == keyword for name in tag_values):
        return True, "tag"

    marker = _clean(payload.get("autopayment") or payload.get("is_autopayment"), 40).casefold()
    if marker in {"1", "true", "yes", "on", "да", keyword}:
        return True, "tag_condition"
    return False, ""


def _minicourse_match(order: dict[str, Any]) -> bool:
    return bool(re.search(r"(?iu)\bмини[\s-]*курс", _clean(order.get("title"), 4000)))


def _surcharge_match(order: dict[str, Any]) -> bool:
    return bool(re.search(r"(?iu)\bдоплат\w*\s+до(?:\s+тарифа)?\s+(?:vip|вип)\b", _clean(order.get("title"), 4000)))


def _medium_fragments(settings: dict[str, str]) -> list[str]:
    raw = settings.get("minicourse_curator_mediums") or DEFAULT_SETTINGS["minicourse_curator_mediums"]
    return [part.strip().casefold() for part in re.split(r"[\n,;]+", raw) if part.strip()]


def _medium_has_curator(value: Any, settings: dict[str, str]) -> bool:
    medium = _clean(value, 500).casefold()
    return bool(medium and any(fragment in medium for fragment in _medium_fragments(settings)))


def _route_order(order: dict[str, Any], requested_process: Any, settings: dict[str, str]) -> str:
    requested = _binding_process(requested_process)
    if _minicourse_match(order):
        return "minicourse_paid" if requested in {"paid", "surcharge_paid", "minicourse_paid"} else "ignore_minicourse_unpaid"
    if _surcharge_match(order):
        if requested in {"paid", "surcharge_paid"}:
            return "surcharge_paid"
        if requested in {"created", "surcharge_created"}:
            return "surcharge_created"
        return "ignore_surcharge_partial"
    if requested in {"surcharge_created", "surcharge_paid"}:
        return "ignore_surcharge_title"
    return requested


def _apply_attribution(order: dict[str, Any], settings: dict[str, str]) -> None:
    profile = dict(order.get("profile_utm") or {})
    order_medium = _clean(order.get("order_utm_medium") or (order.get("order_utm") or {}).get("utm_medium"), 500)
    process = _binding_process(order.get("process"))
    selected = dict(profile)
    if process in {"surcharge_created", "surcharge_paid"}:
        selected["utm_medium"] = order_medium or profile.get("utm_medium", "")
    elif process == "minicourse_paid" and _medium_has_curator(order_medium, settings):
        selected["utm_medium"] = order_medium
        order["curator_medium_match"] = True
    else:
        order["curator_medium_match"] = False
    order["utm"] = {key: _clean(selected.get(key), 500) for key, _field, _code in UTM_SPECS}
    order["vk_dialog"] = (
        f"https://vk.com/gim225075265/convo/{quote(order['utm']['utm_term'])}"
        if order["utm"].get("utm_term") else ""
    )


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


def _payment_status_text(process: Any, raw_status: Any = "") -> str:
    state = _binding_process(process)
    return {
        "created": "Создан заказ",
        "partial": "Частично оплачен",
        "paid": "Оплачен",
        "surcharge_created": "Доплата создана",
        "surcharge_paid": "Доплата оплачена",
        "minicourse_paid": "Оплачен",
    }.get(state, _clean(raw_status, 300))


def _order_template_values(order: dict[str, Any]) -> dict[str, str]:
    return {
        "payment_number": _clean(order.get("number") or order.get("order_id"), 500),
        "number": _clean(order.get("number") or order.get("order_id"), 500),
        "name": _clean(order.get("contact_name"), 500),
        "date_add": _clean(order.get("date_add"), 100),
        "positions": _clean(order.get("title"), 2000),
        "costMoney": _clean(order.get("cost_money"), 100),
        "leftCostMoney": _clean(order.get("left_cost_money"), 100),
        "payedMoney": _clean(order.get("payed_money"), 100),
        "payment_status": _payment_status_text(order.get("process"), order.get("status")),
        "paymentLink": _clean(order.get("payment_link"), 2000),
        "email": _clean(order.get("email"), 500),
        "phone": _clean(order.get("phone"), 100),
        "pay_field_user_ym_uid": _clean(order.get("pay_field_user_ym_uid"), 500),
        "reg_field_user_ym_uid": _clean(order.get("reg_field_user_ym_uid"), 500),
    }


def _format_order_template(template: Any, order: dict[str, Any], limit: int = 10000) -> str:
    text = str(template or "").replace("\\n", "\n")
    values = _order_template_values(order)
    lines = []
    for line in text.splitlines():
        if line.strip().casefold() == "тестовые примечания" and not (
            values.get("pay_field_user_ym_uid") or values.get("reg_field_user_ym_uid")
        ):
            continue
        placeholders = re.findall(r"\{([A-Za-z0-9_]+)\}", line)
        rendered = line
        for key in placeholders:
            rendered = rendered.replace("{" + key + "}", values.get(key, ""))
        if placeholders and not any(values.get(key, "") for key in placeholders):
            continue
        if rendered.strip():
            lines.append(rendered.rstrip())
    return _clean("\n".join(lines), limit)


def _budget_value(order: dict[str, Any], settings: dict[str, str]) -> int:
    explicit = _money(order.get("budget_money"))
    if explicit > 0:
        return max(0, int(round(explicit)))
    source = settings.get("budget_source") or "paid"
    if source == "none":
        return 0
    value = order.get("cost_money") if source == "cost" else order.get("payed_money")
    return max(0, int(round(_money(value))))


def _payment_rank(process: Any) -> int:
    return {
        "created": 1,
        "surcharge_created": 1,
        "partial": 2,
        "paid": 3,
        "surcharge_paid": 3,
        "minicourse_paid": 3,
    }.get(_binding_process(process), 1)


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
    date_add = _clean(payload.get("date_add") or payload.get("created_at"), 100) or datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    profile_utm = {
        "utm_source": _clean(payload.get("utmS") or payload.get("utm_source") or payload.get("user_source"), 500),
        "utm_medium": _clean(payload.get("utmM") or payload.get("profile_utm_medium") or payload.get("utm_medium") or payload.get("user_medium"), 500),
        "utm_campaign": _clean(payload.get("utmCa") or payload.get("utm_campaign") or payload.get("user_campaign"), 500),
        "utm_content": _clean(payload.get("utmCo") or payload.get("utm_content") or payload.get("user_content"), 500),
        "utm_term": _clean(payload.get("utmT") or payload.get("utm_term") or payload.get("user_term"), 500),
    }
    order_utm = {
        "utm_source": _clean(payload.get("orderUtmS") or payload.get("order_utm_source"), 500),
        "utm_medium": _clean(
            payload.get("orderUtmM") or payload.get("order_utm_medium") or payload.get("order_medium"),
            500,
        ),
        "utm_campaign": _clean(payload.get("orderUtmCa") or payload.get("order_utm_campaign"), 500),
        "utm_content": _clean(payload.get("orderUtmCo") or payload.get("order_utm_content"), 500),
        "utm_term": _clean(payload.get("orderUtmT") or payload.get("order_utm_term"), 500),
    }
    yclid = _clean(payload.get("user_yclid") or payload.get("yclid"), 500)
    ym_uid = _clean(payload.get("user_ym_uid") or payload.get("ym_uid") or payload.get("_ym_uid"), 500)
    order = {
        "order_id": order_id,
        "number": number,
        "lead_name": "",
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
        "date_add": date_add,
        "budget_money": _money_value(payload.get("budgetMoney") or payload.get("budget_money") or payload.get("netMoney") or payload.get("net_money")),
        "pay_field_user_ym_uid": _clean(payload.get("pay_field_user_ym_uid"), 500),
        "reg_field_user_ym_uid": _clean(payload.get("reg_field_user_ym_uid"), 500),
        "profile_utm": profile_utm,
        "order_utm": order_utm,
        "order_utm_medium": order_utm["utm_medium"],
        "utm": dict(profile_utm),
        "yclid": yclid,
        "ym_uid": ym_uid,
        "vk_dialog": f"https://vk.com/gim225075265/convo/{quote(profile_utm['utm_term'])}" if profile_utm["utm_term"] else "",
        "raw": _mask_secret(payload),
    }
    order["lead_name"] = _format_order_template(settings.get("lead_name_template") or DEFAULT_SETTINGS["lead_name_template"], order, 500)
    return order


def _payload_from_customer_db(fields: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "number": fields.get("number"),
        "id": fields.get("gc_user_id") or fields.get("id"),
        "order_id": fields.get("order_id"),
        "positions": fields.get("positions"),
        "offers": fields.get("offers"),
        "tags": fields.get("tags"),
        "order_tags": fields.get("order_tags"),
        "tag_names": fields.get("tag_names"),
        "autopayment": fields.get("autopayment") or fields.get("is_autopayment"),
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
        "orderUtmS": fields.get("order_utm_source"),
        "orderUtmM": fields.get("order_utm_medium") or fields.get("order_medium"),
        "orderUtmCa": fields.get("order_utm_campaign"),
        "orderUtmCo": fields.get("order_utm_content"),
        "orderUtmT": fields.get("order_utm_term"),
        "user_yclid": fields.get("user_yclid") or fields.get("yclid"),
        "user_ym_uid": fields.get("user_ym_uid") or fields.get("ym_uid"),
        "pay_field_user_ym_uid": fields.get("pay_field_user_ym_uid"),
        "reg_field_user_ym_uid": fields.get("reg_field_user_ym_uid"),
        "budgetMoney": fields.get("budget_money") or fields.get("net_money"),
        "date_add": fields.get("date_add") or fields.get("created_at") or fields.get("date_creation"),
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
    body, error, _ = await _amo_request("GET", f"/api/v4/{entity}/custom_fields?limit=250", settings)
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
    add("ГК ID Заказа", order["order_id"])
    add("Дата создания", order["date_add"])
    add("Пользователь в ГК", order["user_link"])
    add("Ссылка на оплату", order["payment_link"])
    add("Заказ в ГК", order["order_link"])
    add("Название тарифа", order["title"])
    add("Оплачено", order["payed_money"])
    add("Осталось оплатить", order["left_cost_money"])
    add("Стоимость тарифа", order["cost_money"])
    if _binding_process(order.get("process")) == "minicourse_paid":
        tariff_field = next(
            (field for field in fields if _field_matches(field, "Тариф") and _clean(field.get("type")) == "select"),
            None,
        )
        if tariff_field:
            enum = next(
                (item for item in (tariff_field.get("enums") or []) if _clean(item.get("value")).casefold() == "мини курс"),
                None,
            )
            if enum and _int_or_none(enum.get("id")):
                values.append({"field_id": int(tariff_field["id"]), "values": [{"enum_id": int(enum["id"])}]})
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


async def _find_contact_for_order(order: dict[str, Any], settings: dict[str, str]) -> tuple[dict[str, Any] | None, str]:
    checks = [
        ("phone", _clean(order.get("phone"), 100), {"field_code": "PHONE"}),
        ("email", _clean(order.get("email"), 500), {"field_code": "EMAIL"}),
    ]
    for _kind, query, rule in checks:
        if not query:
            continue
        body, error, _ = await _amo_request("GET", f"/api/v4/contacts?query={quote(query)}&limit=50", settings)
        if error:
            return None, error
        for contact in (((body or {}).get("_embedded") or {}).get("contacts") or []):
            if any(_compare_value(value, query) for value in _entity_rule_values(contact, rule)):
                return contact, ""
    return None, ""


async def _fill_empty_contact_fields(
    contact: dict[str, Any],
    order: dict[str, Any],
    contact_fields: list[dict[str, Any]],
    settings: dict[str, str],
) -> tuple[dict[str, Any], str]:
    contact_id = _int_or_none(contact.get("id"))
    if not contact_id:
        return {}, "Контакт amoCRM найден без ID"
    existing_values = contact.get("custom_fields_values") or []
    missing_order = dict(order)
    if any(_entity_rule_values(contact, {"field_code": "PHONE"})):
        missing_order["phone"] = ""
    if any(_entity_rule_values(contact, {"field_code": "EMAIL"})):
        missing_order["email"] = ""
    custom_values = _contact_field_values(contact_fields, missing_order)
    payload: dict[str, Any] = {}
    if custom_values:
        payload["custom_fields_values"] = custom_values
    if not _clean(contact.get("name"), 500) and order.get("contact_name"):
        payload["name"] = order["contact_name"]
    if not payload:
        return {"contact_id": str(contact_id), "updated": False}, ""
    body, error, _ = await _amo_request("PATCH", f"/api/v4/contacts/{contact_id}", settings, payload)
    return {"contact_id": str(contact_id), "updated": not bool(error), "response": body}, error


def _tags(settings: dict[str, str], order: dict[str, Any] | None = None) -> list[dict[str, str]]:
    names = [item.strip() for item in re.split(r"[\n,;]+", settings.get("tags", "")) if item.strip()]
    process = _binding_process((order or {}).get("process"))
    if process in {"surcharge_created", "surcharge_paid", "minicourse_paid"}:
        names = [name for name in names if name.casefold() != "автооплата"]
    if process in {"surcharge_created", "surcharge_paid"}:
        names.append("Доплата")
    elif process == "minicourse_paid":
        names.append("Мини-курс")
    unique: list[str] = []
    for name in names:
        if name.casefold() not in {item.casefold() for item in unique}:
            unique.append(name)
    return [{"name": name} for name in unique]


async def _mapped_lead_id(order: dict[str, Any]) -> str:
    keys = [f"order:{order['order_id']}", f"number:{order['number']}"]
    if _binding_process(order.get("process")) == "created":
        keys.extend(_floating_identity_keys(order))
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


async def _remember_lead(order: dict[str, Any], lead_id: str, replace_lead_orders: bool = False) -> None:
    pairs = [(f"order:{order['order_id']}", lead_id), (f"number:{order['number']}", lead_id)]
    if _binding_process(order.get("process")) == "created":
        pairs.extend((key, lead_id) for key in _floating_identity_keys(order))
    pairs = [(key, value) for key, value in pairs if not key.endswith(":") and value]
    if not pairs:
        return
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        if replace_lead_orders:
            await db.execute("DELETE FROM order_map WHERE lead_id=?", (lead_id,))
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


async def _find_non_autopayment_duplicate_by_phone(
    order: dict[str, Any],
    settings: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    phone = _clean(order.get("phone"), 100)
    if not phone:
        return [], [], ""
    body, error, _ = await _amo_request("GET", f"/api/v4/contacts?query={quote(phone)}&with=leads&limit=50", settings)
    if error:
        return [], [], error
    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    phone_rule = {"field_code": "PHONE"}
    for contact in (((body or {}).get("_embedded") or {}).get("contacts") or []):
        if not any(_compare_value(value, phone) for value in _entity_rule_values(contact, phone_rule)):
            continue
        lead_ids = [_clean(item.get("id"), 64) for item in ((contact.get("_embedded") or {}).get("leads") or [])]
        if not any(lead_ids):
            contact_id = _clean(contact.get("id"), 64)
            links, links_error, _ = await _amo_request(
                "GET",
                f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads",
                settings,
            )
            if links_error:
                return [], [], links_error
            lead_ids = [
                _clean(item.get("to_entity_id"), 64)
                for item in (((links or {}).get("_embedded") or {}).get("links") or [])
            ]
        for lead_id in [item for item in lead_ids if item]:
            lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
            if lead_error:
                return [], [], lead_error
            if isinstance(lead, dict) and lead.get("id"):
                candidates.append((lead, contact))
    seen_candidate_ids = {_clean(pair[0].get("id"), 64) for pair in candidates}
    for lead_id in await _customer_db_deal_ids_for_phone(phone):
        if lead_id in seen_candidate_ids:
            continue
        lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
        if lead_error or not isinstance(lead, dict) or not lead.get("id"):
            continue
        candidates.append((lead, {}))
        seen_candidate_ids.add(lead_id)
    if not candidates:
        return [], [], ""
    candidates.sort(
        key=lambda pair: (-int(pair[0].get("updated_at") or 0), -int(pair[0].get("id") or 0))
    )
    unique_leads: list[dict[str, Any]] = []
    unique_contacts: list[dict[str, Any]] = []
    seen_leads: set[str] = set()
    seen_contacts: set[str] = set()
    for lead, contact in candidates:
        lead_id = _clean(lead.get("id"), 64)
        contact_id = _clean(contact.get("id"), 64)
        if lead_id and lead_id not in seen_leads:
            seen_leads.add(lead_id)
            unique_leads.append(lead)
        if contact_id and contact_id not in seen_contacts:
            seen_contacts.add(contact_id)
            unique_contacts.append(contact)
    return unique_leads, unique_contacts, ""


async def _customer_db_deal_ids_for_phone(phone: Any) -> list[str]:
    """Find every amo deal across duplicated contact cards by exact normalized phone."""
    identity = _phone_identity(phone)
    if not identity:
        return []
    try:
        path = _customer_db_path()
    except RuntimeError:
        return []
    if not path.exists():
        return []

    def scan() -> list[str]:
        result: set[str] = set()
        try:
            with sqlite3.connect(path) as db:
                rows = db.execute(
                    "SELECT platform_id,custom_fields FROM cdb_amo_deals WHERE custom_fields LIKE ?",
                    (f"%{identity[-10:]}%",),
                ).fetchall()
            for platform_id, raw in rows:
                try:
                    fields = json.loads(raw or "{}")
                except (TypeError, json.JSONDecodeError):
                    continue
                phones = {_phone_identity(value) for value in (fields.get("phones") or [])}
                if identity in phones:
                    lead_id = _clean(platform_id, 64)
                    if lead_id:
                        result.add(lead_id)
        except sqlite3.Error as exc:
            _log("warning", "customer-db exact phone lookup failed: %s", exc)
        return sorted(result, key=lambda value: int(value) if value.isdigit() else 0, reverse=True)

    return await asyncio.to_thread(scan)


async def _find_existing_lead(order: dict[str, Any], settings: dict[str, str], binding: dict[str, Any]) -> tuple[str, str]:
    process = _binding_process(order.get("process"))
    mapped = await _mapped_lead_id(order)
    if mapped:
        body, error, _ = await _amo_request("GET", f"/api/v4/leads/{mapped}", settings)
        if body and not error:
            mapped_number = _lead_order_number(body)
            if process == "created" or not mapped_number or _compare_value(mapped_number, order.get("number")):
                return mapped, "local_map"
    entity = "leads" if _is_paid_process(process) else _clean_duplicate_search_entity(binding.get("duplicate_search_entity"))
    rules = list(DEFAULT_DUPLICATE_SEARCH_RULES) if _is_paid_process(process) else []
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
                if str(item.get("pipeline_id") or "") != str(binding.get("pipeline_id") or settings.get("pipeline_id") or ""):
                    continue
                lead_id = _clean(item.get("id"), 64)
            else:
                lead_id = await _contact_linked_lead_id(item, settings)
            if lead_id:
                await _remember_lead(order, lead_id)
                field = rule.get("field_id") or rule.get("field_code") or rule.get("field")
                return lead_id, f"{entity}:{field}"
    if process != "created":
        return "", ""
    phone = _clean(order.get("phone"), 100)
    if not phone:
        return "", ""
    body, error, _ = await _amo_request("GET", f"/api/v4/contacts?query={quote(phone)}&with=leads", settings)
    if error:
        return "", error
    candidates: list[dict[str, Any]] = []
    phone_rule = {"field_code": "PHONE", "source": "phone"}
    target_pipeline = str(binding.get("pipeline_id") or settings.get("pipeline_id") or "")
    for contact in (((body or {}).get("_embedded") or {}).get("contacts") or []):
        if not any(_compare_value(value, phone) for value in _entity_rule_values(contact, phone_rule)):
            continue
        lead_ids = [_clean(item.get("id"), 64) for item in ((contact.get("_embedded") or {}).get("leads") or [])]
        if not any(lead_ids):
            contact_id = _clean(contact.get("id"), 64)
            links, links_error, _ = await _amo_request("GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", settings)
            if not links_error:
                lead_ids = [_clean(item.get("to_entity_id"), 64) for item in (((links or {}).get("_embedded") or {}).get("links") or [])]
        for lead_id in [item for item in lead_ids if item]:
            lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
            if (
                not lead_error
                and isinstance(lead, dict)
                and str(lead.get("pipeline_id") or "") == target_pipeline
                and _existing_payment_rank(lead) == 1
            ):
                candidates.append(lead)
    seen_candidate_ids = {_clean(lead.get("id"), 64) for lead in candidates}
    for lead_id in await _customer_db_deal_ids_for_phone(phone):
        if lead_id in seen_candidate_ids:
            continue
        lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
        if (
            not lead_error
            and isinstance(lead, dict)
            and str(lead.get("pipeline_id") or "") == target_pipeline
            and _existing_payment_rank(lead) == 1
        ):
            candidates.append(lead)
            seen_candidate_ids.add(lead_id)
    if not candidates:
        return "", ""
    candidates.sort(key=lambda lead: (-int(lead.get("updated_at") or 0), -int(lead.get("id") or 0)))
    lead_id = _clean(candidates[0].get("id"), 64)
    await _remember_lead(order, lead_id)
    return lead_id, "contacts:PHONE"


async def _create_lead(order: dict[str, Any], settings: dict[str, str], binding: dict[str, Any], responsible_user_id: str = "") -> tuple[dict[str, Any], str]:
    lead_fields, error = await _amo_fields("leads", settings)
    if error:
        return {}, error
    contact_fields, error = await _amo_fields("contacts", settings)
    if error:
        return {}, error
    existing_contact, contact_error = await _find_contact_for_order(order, settings)
    if contact_error:
        return {}, f"Поиск контакта: {contact_error}"
    if existing_contact:
        _contact_update, contact_error = await _fill_empty_contact_fields(existing_contact, order, contact_fields, settings)
        if contact_error:
            return {}, f"Дополнение контакта: {contact_error}"
        contact = {"id": int(existing_contact["id"])}
    else:
        contact = {"name": order["contact_name"] or order["lead_name"]}
        if order["first_name"]:
            contact["first_name"] = order["first_name"]
        if order["last_name"]:
            contact["last_name"] = order["last_name"]
        contact_custom = _contact_field_values(contact_fields, order)
        if contact_custom:
            contact["custom_fields_values"] = contact_custom
    lead: dict[str, Any] = {
        "name": order["lead_name"],
        "price": _budget_value(order, settings),
        "custom_fields_values": _lead_field_values(lead_fields, order),
        "_embedded": {"tags": _tags(settings, order), "contacts": [contact]},
    }
    for setting_key in ("pipeline_id", "status_id"):
        value = _int_or_none(binding.get(setting_key) or settings.get(setting_key))
        if value:
            lead[setting_key] = value
    selected_responsible = _int_or_none(responsible_user_id)
    if selected_responsible:
        lead["responsible_user_id"] = selected_responsible
    body, error, _ = await _amo_request("POST", "/api/v4/leads/complex", settings, [lead])
    if error:
        return {}, error
    item = body[0] if isinstance(body, list) and body else body
    lead_id = _clean((item or {}).get("id"), 64)
    embedded_contacts = ((item or {}).get("_embedded") or {}).get("contacts") or [{}]
    contact_id = _clean((item or {}).get("contact_id") or embedded_contacts[0].get("id"), 64)
    await _remember_lead(order, lead_id)
    return {"lead_id": lead_id, "contact_id": contact_id, "response": body}, ""


async def _update_lead(
    lead_id: str,
    order: dict[str, Any],
    settings: dict[str, str],
    binding: dict[str, Any],
    existing: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    lead_fields, error = await _amo_fields("leads", settings)
    if error:
        return {}, error
    contact_fields, error = await _amo_fields("contacts", settings)
    if error:
        return {}, error
    existing_contact, error = await _find_contact_for_order(order, settings)
    if error:
        return {}, f"Поиск контакта: {error}"
    if existing_contact:
        _contact_update, error = await _fill_empty_contact_fields(existing_contact, order, contact_fields, settings)
        if error:
            return {}, f"Дополнение контакта: {error}"
    payload: dict[str, Any] = {
        "name": order["lead_name"],
        "price": _budget_value(order, settings),
        "custom_fields_values": _lead_field_values(lead_fields, order),
        "_embedded": {"tags": _tags(settings, order)},
    }
    target_status = _int_or_none(binding.get("status_id") or settings.get("status_id"))
    existing_status = _int_or_none(existing.get("status_id"))
    movable_statuses = set(_status_ids_payload(binding.get("move_from_statuses") or binding.get("move_from_statuses_json")))
    if target_status and (existing_status == target_status or str(existing_status or "") in movable_statuses):
        payload["status_id"] = target_status
    body, error, _ = await _amo_request("PATCH", f"/api/v4/leads/{lead_id}", settings, payload)
    if error:
        return {}, error
    await _remember_lead(order, lead_id, replace_lead_orders=_binding_process(order.get("process")) == "created")
    return {"lead_id": lead_id, "response": body}, ""


def _lead_order_number(lead: dict[str, Any]) -> str:
    for field in lead.get("custom_fields_values") or []:
        if int(field.get("field_id") or 0) == 1006689 or _clean(field.get("field_name"), 300).casefold() == "№ гк":
            values = field.get("values") or []
            return _clean((values[0] if values else {}).get("value"), 100)
    return ""


def _existing_payment_rank(lead: dict[str, Any]) -> int:
    paid = 0.0
    left = 0.0
    for field in lead.get("custom_fields_values") or []:
        field_id = int(field.get("field_id") or 0)
        value = ((field.get("values") or [{}])[0]).get("value")
        if field_id == 1006697:
            paid = _money(value)
        elif field_id == 1006699:
            left = _money(value)
    if paid > 0:
        return 3 if left <= 0 else 2
    if int(lead.get("status_id") or 0) == 142:
        return 3
    return 1


def _duplicate_action(existing: dict[str, Any], order: dict[str, Any]) -> str:
    same_order = bool(_lead_order_number(existing) and _compare_value(_lead_order_number(existing), order.get("number")))
    incoming_rank = _payment_rank(order.get("process"))
    existing_rank = _existing_payment_rank(existing)
    if existing_rank >= 2 and not same_order:
        if incoming_rank >= _payment_rank("partial"):
            return "create_new_paid_order"
        return "create_new_unpaid_attempt"
    if existing_rank >= 2 and same_order:
        if incoming_rank > existing_rank:
            return "update_payment_transition"
        return "note_only_locked_payment"
    return "update"


async def _add_order_note(lead_id: str, order: dict[str, Any], settings: dict[str, str]) -> tuple[dict[str, Any], str]:
    text = _format_order_template(settings.get("note_template") or DEFAULT_NOTE_TEMPLATE, order, 10000)
    existing, existing_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}/notes?limit=250", settings)
    if not existing_error:
        for note in (((existing or {}).get("_embedded") or {}).get("notes") or []):
            if note.get("note_type") == "common" and _clean((note.get("params") or {}).get("text"), 10000) == text:
                return {"skipped": True, "reason": "identical note already exists"}, ""
    body, error, _ = await _amo_request(
        "POST",
        f"/api/v4/leads/{lead_id}/notes",
        settings,
        [{"note_type": "common", "params": {"text": text}}],
    )
    return {"text_length": len(text), "response": body}, error


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


def _task_responsible_id(binding: dict[str, Any], deal_responsible_user_id: str = "") -> int | None:
    return (
        _int_or_none(binding.get("task_responsible_user_id"))
        or _int_or_none(deal_responsible_user_id)
        or _int_or_none(binding.get("responsible_user_id"))
    )


def _tasks_forbidden(process: Any) -> bool:
    return _clean(process, 64) == "minicourse_paid"


async def _create_task_for_lead(
    lead_id: str,
    order: dict[str, Any],
    settings: dict[str, str],
    binding: dict[str, Any],
    responsible_user_id: str = "",
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
    existing, existing_error, _ = await _amo_request("GET", f"/api/v4/tasks?filter[entity_id]={lead_id_int}&limit=250", settings)
    if not existing_error:
        for task in (((existing or {}).get("_embedded") or {}).get("tasks") or []):
            if not task.get("is_completed") and _clean(task.get("text"), 2000) == task_text:
                return {"skipped": True, "reason": "open task already exists", "task_id": _clean(task.get("id"), 64)}, ""
    for related_lead_id in await _customer_db_deal_ids_for_phone(order.get("phone")):
        if related_lead_id == str(lead_id_int):
            continue
        related, related_error, _ = await _amo_request(
            "GET",
            f"/api/v4/tasks?filter[entity_id]={related_lead_id}&limit=250",
            settings,
        )
        if related_error:
            continue
        for task in (((related or {}).get("_embedded") or {}).get("tasks") or []):
            if not task.get("is_completed") and _clean(task.get("text"), 2000) == task_text:
                return {
                    "skipped": True,
                    "reason": "open task already exists for the same phone",
                    "task_id": _clean(task.get("id"), 64),
                    "task_lead_id": related_lead_id,
                }, ""
    task: dict[str, Any] = {
        "entity_id": lead_id_int,
        "entity_type": "leads",
        "task_type_id": int(binding.get("task_type_id") or 1),
        "text": task_text,
        "complete_till": int(time.time()) + due_minutes * 60,
    }
    responsible_id = _task_responsible_id(binding, responsible_user_id)
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
    # Webhooks and Customer DB polling may deliver adjacent orders at once.
    # Serialize the decision+write section so the persistent floating identity
    # map is committed before the next order of the same person is evaluated.
    async with _order_process_lock:
        return await _process_order_payload_unlocked(payload, raw_payload, method, process)


async def _process_order_payload_unlocked(
    payload: dict[str, Any],
    raw_payload: str,
    method: str,
    process: str = "",
) -> dict[str, Any]:
    settings = await _settings_map()
    order = _normalize_order(payload, settings)
    requested_process = process or payload.get("payment_state") or payload.get("status") or ""
    order["process"] = _route_order(order, requested_process, settings)
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
    ignored_special = {
        "ignore_minicourse_unpaid": "мини-курсы выгружаются только после полной оплаты",
        "ignore_surcharge_partial": "для доплаты поддерживаются только создание и полная оплата",
        "ignore_surcharge_title": "в названии заказа нет «Доплата до VIP»",
    }
    if order["process"] in ignored_special:
        reason = ignored_special[order["process"]]
        event_id = await _store_event({
            "method": method,
            "order_id": order["order_id"],
            "number": order["number"],
            "lead_id": "",
            "contact_id": "",
            "action": order["process"],
            "success": 1,
            "ignored": 1,
            "error": "",
            "details": json.dumps({"order": order, "reason": reason}, ensure_ascii=False),
            "raw_payload": raw_payload,
        })
        return {
            "ok": True,
            "stored": False,
            "event_id": event_id,
            "ignored": True,
            "action": order["process"],
            "reason": reason,
            "status_code": 200,
        }
    _apply_attribution(order, settings)
    autopayment_ok, autopayment_source = _autopayment_match(payload, order)
    special_order = order["process"] in {"surcharge_created", "surcharge_paid", "minicourse_paid"}
    if not autopayment_ok and not special_order:
        existing_leads, contacts, lookup_error = await _find_non_autopayment_duplicate_by_phone(order, settings)
        if lookup_error:
            result, process_error, action, ignored = {}, lookup_error, "non_autopayment_lookup_failed", 0
        elif not existing_leads:
            result, process_error, action, ignored = {}, "", "ignored_non_autopayment_no_duplicate", 1
        else:
            note_results: list[dict[str, Any]] = []
            process_error = ""
            for existing in existing_leads:
                lead_id = _clean(existing.get("id"), 64)
                note, note_error = await _add_order_note(lead_id, order, settings)
                note_results.append({"lead_id": lead_id, "note": note, "error": note_error})
                if note_error:
                    process_error = note_error
                    break
            result = {
                "lead_id": _clean(existing_leads[0].get("id"), 64),
                "lead_ids": [_clean(item.get("id"), 64) for item in existing_leads],
                "notes": note_results,
                "preserved_all_fields": True,
            }
            action, ignored = "noted_non_autopayment_contact_deals", 0
        base_event = {
            "method": method,
            "order_id": order["order_id"],
            "number": order["number"],
            "lead_id": _clean(result.get("lead_id"), 64),
            "contact_id": _clean((contacts[0] if contacts else {}).get("id"), 64),
            "action": action,
            "success": 0 if process_error else 1,
            "ignored": ignored,
            "error": process_error,
            "details": json.dumps({"order": order, "autopayment_match": "", "result": result}, ensure_ascii=False),
            "raw_payload": raw_payload,
        }
        event_id = await _store_event(base_event)
        return {
            "ok": not bool(process_error),
            "stored": bool(existing_leads and not process_error),
            "event_id": event_id,
            "ignored": bool(ignored),
            "action": action,
            "lead_id": _clean(result.get("lead_id"), 64),
            "error": process_error,
            "status_code": 200,
        }
    order["autopayment_match"] = autopayment_source if autopayment_ok else order["process"]
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
    existing: dict[str, Any] | None = None
    if existing_id:
        existing_body, existing_error, _ = await _amo_request("GET", f"/api/v4/leads/{existing_id}", settings)
        if existing_error:
            result, error, action = {}, existing_error, "lookup_failed"
        else:
            existing = existing_body if isinstance(existing_body, dict) else None
    if existing_id and duplicate_policy == "skip":
        result, error = {"lead_id": existing_id, "skipped_duplicate": True}, ""
        action = "skipped_duplicate"
        base_event["ignored"] = 1
    elif existing_id and existing:
        duplicate_action = _duplicate_action(existing, order)
        if duplicate_action in {"create_new_paid_order", "create_new_unpaid_attempt"}:
            responsible = await _new_responsible(settings, binding)
            if _selected_responsible_ids(settings, binding) and not responsible:
                result, error, action = {}, "Нет активного выбранного ответственного amoCRM", "create_failed"
            else:
                result, error = await _create_lead(order, settings, binding, responsible)
                action = "created_new_paid_order" if duplicate_action == "create_new_paid_order" else "created_new_unpaid_attempt"
                if not error and result.get("lead_id"):
                    await _advance_responsible_cursor(settings, binding)
        elif duplicate_action.startswith("note_only"):
            result, error = {"lead_id": existing_id, "preserved": {
                "pipeline_id": existing.get("pipeline_id"),
                "status_id": existing.get("status_id"),
                "responsible_user_id": existing.get("responsible_user_id"),
                "name": existing.get("name"),
            }}, ""
            action = duplicate_action
        else:
            result, error = await _update_lead(existing_id, order, settings, binding, existing)
            action = "updated"
    elif existing_id:
        result, error = {}, error or "Не удалось прочитать найденную сделку"
        action = "lookup_failed"
    else:
        responsible = await _new_responsible(settings, binding)
        if _selected_responsible_ids(settings, binding) and not responsible:
            result, error, action = {}, "Нет активного выбранного ответственного amoCRM", "create_failed"
        else:
            result, error = await _create_lead(order, settings, binding, responsible)
            action = "created"
            if not error and result.get("lead_id"):
                await _advance_responsible_cursor(settings, binding)
    lead_id = _clean((result or {}).get("lead_id") or existing_id, 64)
    note_result: dict[str, Any] = {"skipped": True}
    note_error = ""
    if not error and lead_id and action != "skipped_duplicate":
        note_result, note_error = await _add_order_note(lead_id, order, settings)
        if note_error:
            error = f"note: {note_error}"
    task_result: dict[str, Any] = {"skipped": True}
    task_error = ""
    task_forbidden = _tasks_forbidden(order["process"])
    if not error and not task_forbidden and action in {"created", "created_new_paid_order", "created_new_unpaid_attempt"}:
        created_responsible = _clean((result or {}).get("responsible_user_id") or responsible, 64)
        task_result, task_error = await _create_task_for_lead(lead_id, order, settings, binding, created_responsible)
        if task_error:
            error = f"task: {task_error}"
    elif task_forbidden:
        task_result = {"skipped": True, "reason": "tasks are disabled for this order type"}
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
            "note": note_result,
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


async def _customer_db_rows(limit: int = CDB_PAGE_SIZE, offset: int = 0) -> list[dict[str, Any]]:
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
                OFFSET ?
                """,
                (max(1, min(5000, int(limit))), max(0, int(offset))),
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
            source_hash = _customer_db_source_hash(row.get("custom_fields"))
            await db.execute(
                """
                INSERT OR IGNORE INTO cdb_sync(source_record_id,source_updated_at,source_hash,success,error,last_synced_at)
                VALUES(?,?,?,?,?,?)
                """,
                (record_id, _clean(row.get("updated_at"), 80), source_hash, 1, "bootstrapped without amo sync", _now()),
            )
            count += 1
        await db.commit()
    return count


async def _bootstrap_all_customer_db_rows() -> tuple[int, int]:
    offset = 0
    source_rows = 0
    bootstrapped = 0
    while True:
        rows = await _customer_db_rows(limit=CDB_PAGE_SIZE, offset=offset)
        if not rows:
            break
        source_rows += len(rows)
        bootstrapped += await _bootstrap_customer_db_sync(rows)
        offset += len(rows)
    await _set_setting("cdb_sync_bootstrapped", "1")
    if bootstrapped:
        _log("info", "customer-db sync bootstrap marked %s existing GetCourse orders as seen", bootstrapped)
    return source_rows, bootstrapped


async def _refresh_cdb_sync_fingerprint(record_id: int, updated_at: str, source_hash: str) -> None:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            "UPDATE cdb_sync SET source_updated_at=?,source_hash=? WHERE source_record_id=?",
            (updated_at, source_hash, record_id),
        )
        await db.commit()


async def _sync_customer_db_once(backfill: bool = False, limit: int = 200) -> dict[str, Any]:
    async with _sync_lock:
        return await _sync_customer_db_once_unlocked(backfill=backfill, limit=limit)


async def _sync_customer_db_once_unlocked(backfill: bool = False, limit: int = 200) -> dict[str, Any]:
    settings = await _settings_map()
    if _bindings_paused(settings):
        return {"ok": True, "paused": True, "source_rows": 0, "processed": 0, "bootstrapped": 0}
    if settings.get("cdb_sync_bootstrapped") != "1" and not backfill:
        source_rows, bootstrapped = await _bootstrap_all_customer_db_rows()
        return {"ok": True, "source_rows": source_rows, "processed": 0, "bootstrapped": bootstrapped}
    process_limit = max(1, min(1000, int(limit)))
    offset = 0
    source_rows = 0
    processed = 0
    import_skipped = 0
    errors = []
    while processed < process_limit:
        rows = await _customer_db_rows(limit=CDB_PAGE_SIZE, offset=offset)
        if not rows:
            break
        source_rows += len(rows)
        states = await _sync_state_for([int(row["id"]) for row in rows])
        for row in rows:
            if processed >= process_limit:
                break
            record_id = int(row["id"])
            updated_at = _clean(row.get("updated_at"), 80)
            legacy_source_hash = _clean(row.get("custom_fields"), 200000)
            source_hash = _customer_db_source_hash(row.get("custom_fields"))
            state = states.get(record_id)
            if state and int(state.get("success") or 0):
                if state.get("source_hash") == source_hash:
                    if state.get("source_updated_at") != updated_at:
                        await _refresh_cdb_sync_fingerprint(record_id, updated_at, source_hash)
                    continue
                state_source_hash = _clean(state.get("source_hash"), 200000)
                if not state_source_hash.startswith("v2:") and _customer_db_source_hash(state_source_hash) == source_hash:
                    await _refresh_cdb_sync_fingerprint(record_id, updated_at, source_hash)
                    continue
                if state.get("source_updated_at") == updated_at and state.get("source_hash") == legacy_source_hash:
                    await _refresh_cdb_sync_fingerprint(record_id, updated_at, source_hash)
                    continue
            try:
                fields = json.loads(row.get("custom_fields") or "{}")
                if not isinstance(fields, dict):
                    raise ValueError("custom_fields is not an object")
                if _customer_db_file_import(fields):
                    await _mark_cdb_sync(
                        record_id,
                        updated_at,
                        source_hash,
                        {"ok": True, "error": "file import excluded from amo sync"},
                    )
                    processed += 1
                    import_skipped += 1
                    continue
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
        offset += len(rows)
    return {
        "ok": not errors,
        "source_rows": source_rows,
        "processed": processed,
        "import_skipped": import_skipped,
        "errors": errors[:20],
    }


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
    body, error, _ = await _amo_request("GET", "/api/v4/users?limit=250", settings)
    if error:
        return JSONResponse({"error": error, "users": []}, status_code=502)
    users = []
    for user in (((body or {}).get("_embedded") or {}).get("users") or []):
        if not isinstance(user, dict):
            continue
        rights = user.get("rights") if isinstance(user.get("rights"), dict) else {}
        users.append({
            "id": _clean(user.get("id"), 64),
            "name": _clean(user.get("name"), 300),
            "email": _clean(user.get("email"), 300),
            "is_active": bool(rights.get("is_active", user.get("is_active", True))),
        })
    return {"users": users}


@router.post("/amo/preset-from-leads")
async def amo_preset_from_leads(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    lead_ids = data.get("lead_ids") if isinstance(data, dict) else []
    if not isinstance(lead_ids, list) or not 1 <= len(lead_ids) <= 10:
        raise HTTPException(400, "Укажите от 1 до 10 lead_ids")
    settings = await _settings_map()
    samples = []
    for raw_id in lead_ids:
        lead_id = _int_or_none(raw_id)
        if not lead_id:
            continue
        lead, error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}?with=contacts", settings)
        if error:
            samples.append({"lead_id": lead_id, "error": error})
            continue
        samples.append({
            "lead_id": lead_id,
            "pipeline_id": lead.get("pipeline_id"),
            "status_id": lead.get("status_id"),
            "responsible_user_id": lead.get("responsible_user_id"),
            "tag_names": [item.get("name") for item in ((lead.get("_embedded") or {}).get("tags") or [])],
            "custom_field_ids": [item.get("field_id") for item in lead.get("custom_fields_values") or []],
        })
    preset = {"source_lead_ids": lead_ids, "samples": samples, "created_at": _now()}
    await _set_setting("sample_preset_json", json.dumps(preset, ensure_ascii=False))
    return preset


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
        ("date_add", "{date_add}"),
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
        ("pay_field_user_ym_uid", "{pay_field_user_ym_uid}"),
        ("reg_field_user_ym_uid", "{reg_field_user_ym_uid}"),
        ("user_source", "{object.user.source}"),
        ("user_content", "{object.user.content}"),
        ("user_campaign", "{object.user.campaign}"),
        ("user_term", "{object.user.term}"),
        ("user_medium", "{object.user.medium}"),
        ("orderUtmS", "{object.create_session.utm_source}"),
        ("orderUtmM", "{object.create_session.utm_medium}"),
        ("orderUtmCa", "{object.create_session.utm_campaign}"),
        ("orderUtmCo", "{object.create_session.utm_content}"),
        ("orderUtmT", "{object.create_session.utm_term}"),
    ]
    return urlencode(pairs)
