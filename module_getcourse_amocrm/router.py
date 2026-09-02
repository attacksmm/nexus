from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
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
_contact_email_backfill_task: asyncio.Task | None = None
_sync_lock = asyncio.Lock()
_user_email_sync_lock = asyncio.Lock()
_user_email_amo_lock = asyncio.Lock()
_user_email_amo_last_request = 0.0
_user_email_source_lock = asyncio.Lock()
_user_email_source_cache_token: tuple[tuple[int, int], tuple[int, int]] | None = None
_user_email_source_cache: dict[str, set[str]] = {}
_profile_order_note_sync_lock = asyncio.Lock()
_order_process_lock = asyncio.Lock()

MODULE_ID = "getcourse-amocrm"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
GC_PROFILE_LINK_FIELD = "Пользователь в ГК"
GC_ORDER_FIELD_NAMES = (
    "№ ГК",
    "ГК ID Заказа",
    "Дата создания",
    "Пользователь в ГК",
    "Ссылка на оплату",
    "Заказ в ГК",
    "Название тарифа",
    "Оплачено",
    "Осталось оплатить",
    "Стоимость тарифа",
)
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
    "utm_inheritance_enabled": "1",
    "utm_inheritance_days": "45",
    "utm_inheritance_min_fields": "3",
    "cdb_user_email_sync_enabled": "0",
    "cdb_user_email_sync_batch": "10",
    "cdb_user_email_retry_minutes": "60",
    "cdb_user_email_scan_after_id": "0",
    "cdb_profile_order_scan_after_id": "0",
    "cdb_contact_email_backfill_pending": "1",
    "cdb_contact_email_backfill_status": "pending",
    "cdb_contact_email_backfill_result": "{}",
    "cdb_contact_email_backfill_cursor": "0",
}
MAX_MANUAL_SYNC_LIMIT = 10
MAX_USER_EMAIL_SYNC_LIMIT = 100
CDB_AMO_IGNORED_PAYMENT_STATES = {"refunded", "partial_refund", "canceled"}
CDB_AMO_PROCESSABLE_PAYMENT_STATES = {
    "", "created", "unpaid", "new", "partial", "partially_paid", "paid",
}
ORDER_EMAIL_TERMINAL_STATUS_BY_RESULT = {
    "updated": "email_updated",
    "already_present": "email_already_present",
    "email_filled_before_update": "email_filled_before_update",
    "email_conflict": "email_conflict",
    "source_conflict": "email_source_conflict",
    "invalid_source": "email_invalid_source",
}
ORDER_EMAIL_RESULT_STATUS_BY_STATE = {
    state: result for result, state in ORDER_EMAIL_TERMINAL_STATUS_BY_RESULT.items()
}

MINICOURSE_PIPELINE_ID = "8493006"
MINICOURSE_PAID_STATUS_ID = "69046790"
MINICOURSE_RESPONSIBLE_USER_ID = "6269974"
SURCHARGE_PREMIUM_OFFER_IDS = {"5858685", "8623911"}

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


async def setup(ctx):
    global _db_path, _module_dir, _logger, _sync_task, _contact_email_backfill_task
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-amocrm"))
    await _init_db()
    if _sync_task is None or _sync_task.done():
        lifecycle = getattr(ctx, "lifecycle", None)
        if lifecycle is not None:
            _sync_task = lifecycle.create_task(
                _customer_db_sync_loop(), name="getcourse-amocrm-customer-db-sync",
            )
        else:
            _sync_task = asyncio.create_task(
                _customer_db_sync_loop(), name="getcourse-amocrm-customer-db-sync",
            )
    if _contact_email_backfill_task is None or _contact_email_backfill_task.done():
        lifecycle = getattr(ctx, "lifecycle", None)
        if lifecycle is not None:
            _contact_email_backfill_task = lifecycle.create_task(
                _contact_email_backfill_loop(), name="getcourse-amocrm-contact-email-backfill",
            )
        else:
            _contact_email_backfill_task = asyncio.create_task(
                _contact_email_backfill_loop(), name="getcourse-amocrm-contact-email-backfill",
            )


async def shutdown() -> None:
    global _sync_task, _contact_email_backfill_task
    tasks = [task for task in (_sync_task, _contact_email_backfill_task) if task]
    _sync_task = None
    _contact_email_backfill_task = None
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        if not task.done():
            try:
                await task
            except asyncio.CancelledError:
                pass


def _connect() -> aiosqlite.Connection:
    """Open the module database with the same bounded busy policy everywhere."""

    return aiosqlite.connect(_db_path, timeout=30)  # type: ignore[arg-type]


async def _init_db() -> None:
    async with aiosqlite.connect(_db_path, timeout=30) as db:  # type: ignore[arg-type]
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
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
            CREATE TABLE IF NOT EXISTS cdb_user_email_sync (
                source_record_id INTEGER PRIMARY KEY,
                source_updated_at TEXT NOT NULL DEFAULT '',
                source_hash TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                contact_id TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                next_retry_at TEXT NOT NULL DEFAULT '',
                last_synced_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_cdb_user_email_retry
                ON cdb_user_email_sync(success,next_retry_at);
            CREATE TABLE IF NOT EXISTS gc_profile_bindings (
                source_record_id INTEGER PRIMARY KEY,
                gc_user_id TEXT NOT NULL UNIQUE,
                source_hash TEXT NOT NULL DEFAULT '',
                utm_term TEXT NOT NULL DEFAULT '',
                match_kind TEXT NOT NULL DEFAULT '',
                match_value TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                contact_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_gc_profile_bindings_target
                ON gc_profile_bindings(success,lead_id,contact_id);
            CREATE TABLE IF NOT EXISTS gc_profile_order_notes (
                source_record_id INTEGER PRIMARY KEY,
                source_hash TEXT NOT NULL DEFAULT '',
                gc_user_id TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                note_hash TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                fields_synced INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_gc_profile_order_notes_retry
                ON gc_profile_order_notes(success,source_record_id);
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
        await _ensure_gc_profile_binding_columns(db)
        await _ensure_gc_profile_order_note_columns(db)
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


async def _ensure_gc_profile_binding_columns(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(gc_profile_bindings)")
    columns = {str(row[1]) for row in await cur.fetchall()}
    if "match_kind" not in columns:
        await db.execute(
            "ALTER TABLE gc_profile_bindings ADD COLUMN match_kind TEXT NOT NULL DEFAULT ''"
        )
    if "match_value" not in columns:
        await db.execute(
            "ALTER TABLE gc_profile_bindings ADD COLUMN match_value TEXT NOT NULL DEFAULT ''"
        )
    await db.execute(
        """
        UPDATE gc_profile_bindings
        SET match_kind='utm_term',match_value=utm_term
        WHERE success=1 AND match_kind='' AND utm_term!=''
        """
    )


async def _ensure_gc_profile_order_note_columns(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(gc_profile_order_notes)")
    columns = {str(row[1]) for row in await cur.fetchall()}
    if "fields_synced" not in columns:
        await db.execute(
            "ALTER TABLE gc_profile_order_notes ADD COLUMN fields_synced INTEGER NOT NULL DEFAULT 0"
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
    async with _connect() as db:
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
        "utm_inheritance_enabled", "utm_inheritance_days", "utm_inheritance_min_fields",
        "cdb_user_email_sync_enabled", "cdb_user_email_sync_batch", "cdb_user_email_retry_minutes",
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
        elif key in {"cdb_sync_enabled", "utm_inheritance_enabled", "cdb_user_email_sync_enabled"}:
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
        elif key == "utm_inheritance_days":
            value = str(max(1, min(180, int(float(value or 45)))))
        elif key == "utm_inheritance_min_fields":
            value = str(max(2, min(5, int(float(value or 3)))))
        elif key == "cdb_user_email_sync_batch":
            value = str(max(1, min(50, int(float(value or 10)))))
        elif key == "cdb_user_email_retry_minutes":
            value = str(max(5, min(1440, int(float(value or 60)))))
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
    if SURCHARGE_PREMIUM_OFFER_IDS.intersection(
        {_clean(item, 100) for item in (order.get("offer_ids") or [])}
    ):
        return True
    return bool(
        re.search(
            r"(?iu)\bдоплат\w*\s+до(?:\s+тарифа)?\s+(?:premium|премиум|vip|вип)\b",
            _clean(order.get("title"), 4000),
        )
    )


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
    # Business invariant for surcharge orders only: medium describes this
    # exact transition, while the other UTM values stay tied to the user's
    # original profile attribution.  Never erase markers such as
    # ``perevodpismo`` with a later profile refresh.
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


def _lead_utm(lead: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, field_name, code in UTM_SPECS:
        values = _entity_rule_values(lead, {"field_code": code})
        if not values:
            values = _entity_rule_values(lead, {"field": field_name})
        result[key] = _clean(values[0] if values else "", 500)
    return result


def _attribution_anchor(order: dict[str, Any]) -> datetime:
    text = _clean(order.get("date_add"), 100)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=MOSCOW_TZ).astimezone(timezone.utc)
    except ValueError:
        pass
    for pattern in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            parsed = datetime.strptime(text, pattern).replace(tzinfo=MOSCOW_TZ)
            if pattern == "%d.%m.%Y":
                parsed = parsed.replace(hour=23, minute=59, second=59)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


async def _mapped_getcourse_lead_ids() -> set[str]:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        rows = await (await db.execute("SELECT DISTINCT lead_id FROM order_map WHERE lead_id<>''")).fetchall()
    return {_clean(row[0], 64) for row in rows if _clean(row[0], 64)}


async def _attribution_alert(kind: str, message: str) -> None:
    module = sys.modules.get("_nexus_mod_getcourse-onboarding")
    sender = getattr(module, "service_system_alert", None) if module else None
    if sender:
        try:
            await sender(kind=f"utm_{_clean(kind, 50)}", message=message)
            return
        except Exception as exc:
            if _logger:
                _logger.warning("UTM alert delivery failed: %s", exc)
    if _logger:
        _logger.warning("%s", message)


async def _inherit_missing_attribution(order: dict[str, Any], settings: dict[str, str]) -> dict[str, Any]:
    current = dict(order.get("utm") or {})
    missing = [key for key, _name, _code in UTM_SPECS if not _clean(current.get(key), 500)]
    if settings.get("utm_inheritance_enabled", "1") != "1" or not missing:
        return {"status": "not_needed", "filled": [], "source_lead_id": ""}
    contact, error = await _find_contact_for_order(order, settings, with_leads=True)
    if error:
        return {"status": "lookup_error", "error": error, "filled": [], "source_lead_id": ""}
    if not contact:
        return {"status": "no_exact_contact", "filled": [], "source_lead_id": ""}
    contact_id = _clean(contact.get("id"), 64)
    linked_ids = [
        _clean(item.get("id"), 64)
        for item in (((contact.get("_embedded") or {}).get("leads")) or [])
        if isinstance(item, dict) and _clean(item.get("id"), 64)
    ]
    if not linked_ids and contact_id:
        links, links_error, _ = await _amo_request(
            "GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", settings
        )
        if links_error:
            return {"status": "lookup_error", "error": links_error, "filled": [], "source_lead_id": ""}
        linked_ids = [
            _clean(item.get("to_entity_id"), 64)
            for item in (((links or {}).get("_embedded") or {}).get("links")) or []
            if isinstance(item, dict) and _clean(item.get("to_entity_id"), 64)
        ]
    mapped = await _mapped_getcourse_lead_ids()
    anchor = _attribution_anchor(order)
    horizon = timedelta(days=max(1, min(180, int(settings.get("utm_inheritance_days") or 45))))
    minimum = max(2, min(5, int(settings.get("utm_inheritance_min_fields") or 3)))
    gc_pipelines = {str(settings.get("pipeline_id") or ""), MINICOURSE_PIPELINE_ID}
    candidates: list[dict[str, Any]] = []
    for lead_id in list(dict.fromkeys(linked_ids))[:100]:
        if lead_id in mapped:
            continue
        lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
        if lead_error:
            return {"status": "lookup_error", "error": lead_error, "filled": [], "source_lead_id": ""}
        if not isinstance(lead, dict) or _clean(lead.get("pipeline_id"), 64) in gc_pipelines:
            continue
        created_at = int(lead.get("created_at") or 0)
        created = datetime.fromtimestamp(created_at, timezone.utc) if created_at > 0 else anchor
        if created > anchor or anchor - created > horizon:
            continue
        utm = _lead_utm(lead)
        completeness = sum(bool(value) for value in utm.values())
        if completeness >= minimum:
            candidates.append({
                "lead_id": lead_id, "created_at": created_at, "completeness": completeness,
                "utm": utm, "pipeline_id": _clean(lead.get("pipeline_id"), 64),
            })
    if not candidates:
        return {"status": "no_source", "filled": [], "source_lead_id": "", "contact_id": contact_id}
    candidates.sort(key=lambda item: (item["created_at"], item["completeness"], int(item["lead_id"])), reverse=True)
    best = candidates[0]
    best_fingerprint = tuple(best["utm"].get(key, "") for key, _name, _code in UTM_SPECS)
    close_conflicts = [
        item for item in candidates[1:]
        if abs(int(best["created_at"]) - int(item["created_at"])) <= 6 * 60 * 60
        and tuple(item["utm"].get(key, "") for key, _name, _code in UTM_SPECS) != best_fingerprint
    ]
    if close_conflicts:
        message = (
            "⚠️ Nexus не стал наследовать UTM: найдено несколько близких источников\n"
            f"Заказ GetCourse: {order.get('number') or order.get('order_id')}\n"
            f"Контакт amoCRM: {contact_id}\nКандидаты: {best['lead_id']}, {close_conflicts[0]['lead_id']}"
        )
        await _attribution_alert("ambiguous", message)
        return {
            "status": "ambiguous", "filled": [], "source_lead_id": "", "contact_id": contact_id,
            "candidate_lead_ids": [item["lead_id"] for item in candidates[:10]],
        }
    filled: list[str] = []
    for key in missing:
        value = _clean(best["utm"].get(key), 500)
        if value:
            current[key] = value
            filled.append(key)
    order["utm"] = current
    order["vk_dialog"] = (
        f"https://vk.com/gim225075265/convo/{quote(current['utm_term'])}" if current.get("utm_term") else ""
    )
    return {
        "status": "inherited" if filled else "no_missing_values",
        "filled": filled,
        "source_lead_id": best["lead_id"],
        "source_pipeline_id": best["pipeline_id"],
        "contact_id": contact_id,
        "preserved_order_values": [key for key, _name, _code in UTM_SPECS if key not in missing],
    }


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
        "offer_ids": sorted(set(re.findall(r"\b\d{5,}\b", _flatten_text(offers)))),
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


async def _find_contact_for_order(
    order: dict[str, Any], settings: dict[str, str], *, with_leads: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    checks = [
        ("phone", _clean(order.get("phone"), 100), {"field_code": "PHONE"}),
        ("email", _clean(order.get("email"), 500), {"field_code": "EMAIL"}),
    ]
    for _kind, query, rule in checks:
        if not query:
            continue
        suffix = "&with=leads" if with_leads else ""
        body, error, _ = await _amo_request(
            "GET", f"/api/v4/contacts?query={quote(query)}&limit=50{suffix}", settings
        )
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


def _valid_email(value: Any) -> str:
    email = _clean(value, 320).casefold()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        return ""
    return email


def _strict_utm_term(value: Any) -> str:
    """Normalize UTM only; deliberately never apply phone-like digit matching."""
    return _clean(value, 1000)


async def _user_email_amo_request(
    method: str, path: str, settings: dict[str, str], payload: Any = None,
) -> tuple[Any, str, int]:
    """Keep this background integration safely below amoCRM's per-integration rate limit."""
    global _user_email_amo_last_request
    async with _user_email_amo_lock:
        loop = asyncio.get_running_loop()
        delay = 0.25 - (loop.time() - _user_email_amo_last_request)
        if delay > 0:
            await asyncio.sleep(delay)
        result = await _amo_request(method, path, settings, payload)
        _user_email_amo_last_request = loop.time()
        if result[2] == 429:
            await asyncio.sleep(2)
            result = await _amo_request(method, path, settings, payload)
            _user_email_amo_last_request = loop.time()
        return result


def _lead_utm_term_match(lead: dict[str, Any], wanted: str) -> tuple[bool, str]:
    system_values: set[str] = set()
    text_values: set[str] = set()
    for field in lead.get("custom_fields_values") or []:
        if not isinstance(field, dict):
            continue
        values = {
            _strict_utm_term(item.get("value"))
            for item in (field.get("values") or [])
            if isinstance(item, dict) and _strict_utm_term(item.get("value"))
        }
        code = _clean(field.get("field_code") or field.get("code"), 120).upper()
        name = _clean(field.get("field_name") or field.get("name"), 300).casefold()
        if code == "UTM_TERM":
            system_values.update(values)
        elif name == "utm_term":
            text_values.update(values)
    if system_values and text_values and system_values != text_values:
        return False, "conflicting_utm_fields"
    selected = system_values or text_values
    return _strict_utm_term(wanted) in selected, ""


async def _exact_utm_leads(utm_term: str, settings: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    """Use field-scoped filters first, then post-filter every live amoCRM lead."""
    wanted = _strict_utm_term(utm_term)
    if not wanted:
        return [], ""
    fields, error = await _amo_fields("leads", settings)
    if error:
        return [], error
    filterable_types = {"text", "url", "textarea", "streetaddress"}
    field_ids = {
        int(field["id"])
        for field in fields
        if _int_or_none(field.get("id"))
        and _clean(field.get("type"), 80) in filterable_types
        and (
            _clean(field.get("code"), 120).upper() == "UTM_TERM"
            or _clean(field.get("name"), 300).casefold() == "utm_term"
        )
    }
    candidates: dict[str, dict[str, Any]] = {}
    paths: list[tuple[str, bool]] = [
        ((
            "/api/v4/leads?"
            f"filter[custom_fields_values][{field_id}][]={quote(utm_term, safe='')}"
            "&with=contacts&limit=100"
        ), True)
        for field_id in sorted(field_ids)
    ]
    # Always union the broad search: historical deals may contain only the
    # predefined tracking UTM_TERM and not the parallel text field.
    paths.append((
        f"/api/v4/leads?query={quote(utm_term, safe='')}&with=contacts&limit=100", False,
    ))
    for path, optional_field_filter in paths:
        body, search_error, search_status = await _user_email_amo_request("GET", path, settings)
        if search_error:
            if (
                optional_field_filter
                and search_status == 400
                and "filter for current account" in search_error.casefold()
            ):
                continue
            return [], search_error
        found = (((body or {}).get("_embedded") or {}).get("leads") or [])
        if len(found) >= 100 or ((body or {}).get("_links") or {}).get("next"):
            return [], "Поиск amoCRM по utm_term достиг безопасного лимита 100 записей"
        for lead in found:
            lead_id = _clean((lead or {}).get("id"), 64)
            if lead_id:
                candidates[lead_id] = lead
        if len(candidates) > 100:
            return [], "Поиск amoCRM по utm_term дал больше 100 кандидатов"
    exact: list[dict[str, Any]] = []
    for lead_id, candidate in candidates.items():
        live, live_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/leads/{lead_id}?with=contacts", settings,
        )
        if live_error:
            return [], live_error
        if not isinstance(live, dict):
            continue
        matches, conflict = _lead_utm_term_match(live, wanted)
        if conflict:
            return [], f"Сделка {lead_id}: конфликт системного и текстового utm_term"
        if matches:
            exact.append(live)
    exact.sort(key=lambda item: int(item.get("id") or 0))
    return exact, ""


def _main_contact_id(lead: dict[str, Any]) -> tuple[str, str]:
    contacts = [
        item for item in (((lead.get("_embedded") or {}).get("contacts")) or [])
        if isinstance(item, dict) and _int_or_none(item.get("id"))
    ]
    main_ids = {
        _clean(item.get("id"), 64)
        for item in contacts
        if str(item.get("is_main") or "").strip().lower() in {"1", "true"}
    }
    if len(main_ids) == 1:
        return next(iter(main_ids)), ""
    if len(main_ids) > 1:
        return "", "несколько основных контактов"
    unique_ids = {_clean(item.get("id"), 64) for item in contacts}
    if len(unique_ids) == 1:
        return next(iter(unique_ids)), ""
    if not unique_ids:
        return "", "у сделки нет контакта"
    return "", "у сделки несколько контактов и не указан основной"


def _protected_bizon_window_now() -> bool:
    now = datetime.now(MOSCOW_TZ)
    minute = now.hour * 60 + now.minute
    return 11 * 60 + 45 <= minute < 14 * 60 + 46 or 18 * 60 + 45 <= minute < 21 * 60 + 46


def _customer_db_change_token(db_path: Path) -> tuple[tuple[int, int], tuple[int, int]]:
    def token(path: Path) -> tuple[int, int]:
        try:
            stat = path.stat()
            return stat.st_mtime_ns, stat.st_size
        except OSError:
            return 0, 0
    return token(db_path), token(Path(str(db_path) + "-wal"))


async def _customer_db_user_email_map() -> tuple[dict[str, set[str]], str]:
    """Build the compact UTM/phone -> email map and cache it until DB/WAL changes."""
    global _user_email_source_cache_token, _user_email_source_cache
    try:
        db_path = _customer_db_path()
    except RuntimeError as exc:
        return {}, str(exc)
    if not db_path.exists():
        return {}, "Customer DB не найдена"
    async with _user_email_source_lock:
        current_token = _customer_db_change_token(db_path)
        if _user_email_source_cache_token == current_token:
            return _user_email_source_cache, ""
        for _attempt in range(2):
            before = _customer_db_change_token(db_path)
            try:
                async with aiosqlite.connect(db_path) as db:
                    # WAL readers get a transactionally consistent view even
                    # while the importer commits newer rows in parallel.
                    await db.execute("BEGIN")
                    result: dict[str, set[str]] = {}
                    cur = await db.execute(
                        """
                        SELECT
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.email') END,
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.utm_term') END,
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.phone') END
                        FROM cdb_getcourse_users
                        """
                    )
                    while True:
                        user_rows = await cur.fetchmany(500)
                        if not user_rows:
                            break
                        for raw_email, raw_term, raw_phone in user_rows:
                            email = _clean(raw_email, 320).casefold()
                            term = _strict_utm_term(raw_term)
                            phone = _phone_identity(raw_phone)
                            if term and email:
                                result.setdefault(f"utm:{term}", set()).add(email)
                            if phone and email:
                                result.setdefault(f"phone:{phone}", set()).add(email)
                    cur = await db.execute(
                        """
                        SELECT
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.email') END,
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.utm_term') END,
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.user_term') END,
                            CASE WHEN json_valid(custom_fields)
                                THEN json_extract(custom_fields, '$.phone') END
                        FROM cdb_getcourse_orders
                        """
                    )
                    while True:
                        order_rows = await cur.fetchmany(500)
                        if not order_rows:
                            break
                        for raw_email, raw_term, raw_user_term, raw_phone in order_rows:
                            email = _valid_email(raw_email)
                            term = _strict_utm_term(raw_term or raw_user_term)
                            phone = _phone_identity(raw_phone)
                            if term and email:
                                result.setdefault(f"utm:{term}", set()).add(email)
                            if phone and email:
                                result.setdefault(f"phone:{phone}", set()).add(email)
            except Exception as exc:
                return {}, f"Проверка GetCourse source map: {exc}"
            # Mark the cache with the token observed before the snapshot.
            # If an import overlapped this read, the next lookup sees a newer
            # token and refreshes instead of serving the older snapshot again.
            _user_email_source_cache_token = before
            _user_email_source_cache = result
            return result, ""
        return {}, "Построение GetCourse source map не выполнено"


async def _customer_db_exact_email_claims(
    identity_kind: str, identity_value: Any,
) -> tuple[set[str], str]:
    """Read exact email claims from one SQLite snapshot when the shared cache is moving."""
    wanted = (
        _strict_utm_term(identity_value)
        if identity_kind == "utm_term" else _phone_identity(identity_value)
    )
    if not wanted:
        return set(), ""
    claims: set[str] = set()
    try:
        async with aiosqlite.connect(_customer_db_path()) as db:
            await db.execute("BEGIN")
            for table, query in (
                (
                    "users",
                    """
                    SELECT
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.email') END,
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.utm_term') END,
                        NULL,
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.phone') END
                    FROM cdb_getcourse_users
                    """,
                ),
                (
                    "orders",
                    """
                    SELECT
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.email') END,
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.utm_term') END,
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.user_term') END,
                        CASE WHEN json_valid(custom_fields)
                            THEN json_extract(custom_fields, '$.phone') END
                    FROM cdb_getcourse_orders
                    """,
                ),
            ):
                _ = table
                cur = await db.execute(query)
                while True:
                    rows = await cur.fetchmany(500)
                    if not rows:
                        break
                    for raw_email, raw_term, raw_user_term, raw_phone in rows:
                        matches = (
                            _strict_utm_term(raw_term or raw_user_term) == wanted
                            if identity_kind == "utm_term"
                            else _phone_identity(raw_phone) == wanted
                        )
                        email = _valid_email(raw_email)
                        if matches and email:
                            claims.add(email)
    except Exception as exc:
        return set(), f"Проверка GetCourse email snapshot: {exc}"
    return claims, ""


async def _customer_db_emails_for_exact_utm(utm_term: str) -> tuple[set[str], str]:
    wanted = _strict_utm_term(utm_term)
    source_map, error = await _customer_db_user_email_map()
    if not error:
        return set(source_map.get(f"utm:{wanted}", set())) if wanted else set(), ""
    return await _customer_db_exact_email_claims("utm_term", wanted)


async def _customer_db_emails_for_exact_phone(phone: Any) -> tuple[set[str], str]:
    wanted = _phone_identity(phone)
    source_map, error = await _customer_db_user_email_map()
    if not error:
        return set(source_map.get(f"phone:{wanted}", set())) if wanted else set(), ""
    return await _customer_db_exact_email_claims("phone", wanted)


def _lead_utm_term_values(lead: dict[str, Any]) -> tuple[set[str], bool]:
    system_values: set[str] = set()
    text_values: set[str] = set()
    for field in lead.get("custom_fields_values") or []:
        if not isinstance(field, dict):
            continue
        values = {
            _strict_utm_term(item.get("value"))
            for item in (field.get("values") or [])
            if isinstance(item, dict) and _strict_utm_term(item.get("value"))
        }
        code = _clean(field.get("field_code") or field.get("code"), 120).upper()
        name = _clean(field.get("field_name") or field.get("name"), 300).casefold()
        if code == "UTM_TERM":
            system_values.update(values)
        elif name == "utm_term":
            text_values.update(values)
    conflict = bool(system_values and text_values and system_values != text_values)
    return (set() if conflict else system_values or text_values), conflict


def _unique_source_email(source_map: dict[str, set[str]], key: str) -> str:
    claims = {_valid_email(item) for item in source_map.get(key, set()) if _valid_email(item)}
    return next(iter(claims)) if len(claims) == 1 else ""


def _plan_contact_email_backfill(
    leads: list[dict[str, Any]],
    contacts: list[dict[str, Any]],
    source_map: dict[str, set[str]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Plan EMAIL-only contact patches from one consistent amoCRM snapshot."""
    deal_contact_ids: set[str] = set()
    utm_evidence: dict[str, list[tuple[str, str, str]]] = {}
    utm_conflicts = _index_leads_for_contact_email_backfill(
        leads, source_map, deal_contact_ids, utm_evidence,
    )
    return _plan_contact_email_page(contacts, source_map, deal_contact_ids, utm_evidence, {
        "lead_contacts": len(deal_contact_ids),
        "utm_conflicts": utm_conflicts,
    })


def _index_leads_for_contact_email_backfill(
    leads: list[dict[str, Any]],
    source_map: dict[str, set[str]],
    deal_contact_ids: set[str],
    utm_evidence: dict[str, list[tuple[str, str, str]]],
) -> int:
    utm_conflicts = 0
    for lead in leads:
        lead_id = _clean(lead.get("id"), 64)
        links = [
            item for item in (((lead.get("_embedded") or {}).get("contacts")) or [])
            if isinstance(item, dict) and _clean(item.get("id"), 64)
        ]
        deal_contact_ids.update(_clean(item.get("id"), 64) for item in links)
        terms, conflict = _lead_utm_term_values(lead)
        if conflict:
            utm_conflicts += 1
            continue
        contact_id, contact_error = _main_contact_id(lead)
        if contact_error:
            continue
        for term in terms:
            email = _unique_source_email(source_map, f"utm:{term}")
            if email:
                utm_evidence.setdefault(contact_id, []).append((term, lead_id, email))
    return utm_conflicts


def _plan_contact_email_page(
    contacts: list[dict[str, Any]],
    source_map: dict[str, set[str]],
    deal_contact_ids: set[str],
    utm_evidence: dict[str, list[tuple[str, str, str]]],
    base_stats: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    targets: list[dict[str, Any]] = []
    already_present = 0
    ambiguous = 0
    for contact in contacts:
        contact_id = _clean(contact.get("id"), 64)
        if not contact_id or contact_id not in deal_contact_ids:
            continue
        if _contact_raw_emails(contact):
            already_present += 1
            continue
        evidence: list[tuple[str, str, str]] = []
        for term, lead_id, email in utm_evidence.get(contact_id, []):
            evidence.append(("utm_term", f"{lead_id}:{term}", email))
        for value in _entity_rule_values(contact, {"field_code": "PHONE"}):
            phone = _phone_identity(value)
            email = _unique_source_email(source_map, f"phone:{phone}") if phone else ""
            if email:
                evidence.append(("phone", phone, email))
        emails = {item[2] for item in evidence}
        if len(emails) != 1:
            ambiguous += int(bool(evidence))
            continue
        targets.append({
            "contact_id": contact_id,
            "email": next(iter(emails)),
            "evidence": evidence,
        })
    return targets, {
        **(base_stats or {}),
        "already_present": already_present,
        "ambiguous": ambiguous,
    }


async def _amo_entity_page(
    entity: str, page: int, settings: dict[str, str], *, with_value: str = "",
) -> tuple[list[dict[str, Any]], bool, str]:
    suffix = f"&with={quote(with_value, safe='')}" if with_value else ""
    body, error, status = await _user_email_amo_request(
        "GET", f"/api/v4/{entity}?limit=250&page={page}{suffix}", settings,
    )
    if error:
        return [], True, error
    if status == 204 or not body:
        return [], True, ""
    page_items = [
        item for item in ((((body or {}).get("_embedded") or {}).get(entity)) or [])
        if isinstance(item, dict)
    ]
    has_next = bool(((body or {}).get("_links") or {}).get("next"))
    return page_items, (not has_next and len(page_items) < 250), ""


async def _live_backfill_candidates(
    target: dict[str, Any], source_map: dict[str, set[str]], settings: dict[str, str],
) -> tuple[set[str], dict[str, Any] | None, str]:
    contact_id = _clean(target.get("contact_id"), 64)
    contact, error, _ = await _user_email_amo_request(
        "GET", f"/api/v4/contacts/{contact_id}?with=leads", settings,
    )
    if error or not isinstance(contact, dict) or _clean(contact.get("id"), 64) != contact_id:
        return set(), None, error or "Контакт amoCRM изменился или недоступен"
    if _contact_raw_emails(contact):
        return set(), contact, "already_present"
    candidates: set[str] = set()
    for value in _entity_rule_values(contact, {"field_code": "PHONE"}):
        phone = _phone_identity(value)
        email = _unique_source_email(source_map, f"phone:{phone}") if phone else ""
        if email:
            candidates.add(email)
    for kind, raw_evidence, _email in target.get("evidence") or []:
        if kind != "utm_term":
            continue
        lead_id, _, term = _clean(raw_evidence, 1200).partition(":")
        if not lead_id or not term:
            continue
        lead, lead_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/leads/{lead_id}?with=contacts", settings,
        )
        if lead_error or not isinstance(lead, dict):
            return set(), contact, lead_error or "Сделка amoCRM недоступна"
        matches, conflict = _lead_utm_term_match(lead, term)
        current_contact_id, contact_error = _main_contact_id(lead)
        if conflict or not matches or contact_error or current_contact_id != contact_id:
            continue
        email = _unique_source_email(source_map, f"utm:{term}")
        if email:
            candidates.add(email)
    return candidates, contact, ""


async def _patch_backfill_target_email(
    target: dict[str, Any], source_map: dict[str, set[str]], settings: dict[str, str],
) -> dict[str, Any]:
    """Revalidate one planned target and perform the only allowed EMAIL PATCH."""
    expected = _valid_email(target.get("email"))
    contact_id = _clean(target.get("contact_id"), 64)
    if not expected or not contact_id:
        return {"ok": True, "status": "changed_before_update", "updated_count": 0}
    contact_fields, fields_error = await _amo_fields("contacts", settings)
    if fields_error:
        return {"ok": False, "status": "lookup_error", "updated_count": 0}
    email_field = next(
        (field for field in contact_fields if _clean(field.get("code"), 120).upper() == "EMAIL"),
        None,
    )
    if not email_field or not _int_or_none(email_field.get("id")):
        return {"ok": False, "status": "configuration_error", "updated_count": 0}
    final_contact, final_error, _ = await _user_email_amo_request(
        "GET", f"/api/v4/contacts/{contact_id}?with=leads", settings,
    )
    if (
        final_error or not isinstance(final_contact, dict)
        or _clean(final_contact.get("id"), 64) != contact_id
    ):
        return {"ok": False, "status": "lookup_error", "updated_count": 0}
    if _contact_raw_emails(final_contact):
        return {"ok": True, "status": "email_filled_before_update", "updated_count": 0}
    live_lead_ids, links_error = await _contact_lead_ids_for_email_sync(final_contact, settings)
    if links_error:
        return {"ok": False, "status": "lookup_error", "updated_count": 0}
    if not live_lead_ids:
        return {"ok": True, "status": "changed_before_update", "updated_count": 0}
    candidates: set[str] = set()
    valid_identities: list[tuple[str, str]] = []
    for value in _entity_rule_values(final_contact, {"field_code": "PHONE"}):
        phone = _phone_identity(value)
        email = _unique_source_email(source_map, f"phone:{phone}") if phone else ""
        if email:
            candidates.add(email)
            valid_identities.append(("phone", phone))
    for kind, raw_identity, _email in target.get("evidence") or []:
        if kind == "utm_term":
            lead_id, separator, term = _clean(raw_identity, 1200).partition(":")
            if not separator or lead_id not in live_lead_ids:
                continue
            lead, lead_error, _ = await _user_email_amo_request(
                "GET", f"/api/v4/leads/{lead_id}?with=contacts", settings,
            )
            if lead_error or not isinstance(lead, dict):
                return {"ok": False, "status": "live_lookup_error", "updated_count": 0}
            matches, conflict = _lead_utm_term_match(lead, term)
            current_contact_id, contact_error = _main_contact_id(lead)
            if not conflict and matches and not contact_error and current_contact_id == contact_id:
                email = _unique_source_email(source_map, f"utm:{term}")
                if email:
                    candidates.add(email)
                    valid_identities.append(("utm_term", term))
    if candidates != {expected}:
        return {"ok": True, "status": "changed_before_update", "updated_count": 0}
    identity_valid = False
    for kind, value in valid_identities:
        if kind == "utm_term":
            source_emails = set(source_map.get(f"utm:{value}", set()))
        else:
            source_emails = set(source_map.get(f"phone:{value}", set()))
        if source_emails == {expected}:
            identity_valid = True
            break
    if not identity_valid:
        return {"ok": True, "status": "changed_before_update", "updated_count": 0}
    payload = {
        "custom_fields_values": [{
            "field_id": int(email_field["id"]),
            "values": [{"value": expected, "enum_code": "WORK"}],
        }]
    }
    _body, patch_error, _ = await _user_email_amo_request(
        "PATCH", f"/api/v4/contacts/{contact_id}", settings, payload,
    )
    return {
        "ok": not bool(patch_error),
        "status": "update_error" if patch_error else "updated",
        "updated_count": 0 if patch_error else 1,
    }


async def _backfill_all_contact_emails(settings: dict[str, str]) -> dict[str, Any]:
    """Scan amoCRM by bounded pages and patch only live, empty contact EMAIL fields."""
    if _protected_bizon_window_now():
        return {"ok": False, "protected_window": True, "error": "Защищённое окно Bizon"}
    source_map, source_error = await _customer_db_user_email_map()
    if source_error:
        return {"ok": False, "error": source_error}
    previous = _jsonish(settings.get("cdb_contact_email_backfill_result") or "{}")
    historical_updated = int(
        (previous.get("contacts_updated_total") or previous.get("contacts_updated") or 0)
        if isinstance(previous, dict) else 0
    )
    deal_contact_ids: set[str] = set()
    utm_evidence: dict[str, list[tuple[str, str, str]]] = {}
    counters = {
        "leads_scanned": 0,
        "contacts_scanned": 0,
        "lead_contacts": 0,
        "utm_conflicts": 0,
        "planned_empty_contacts": 0,
        "contacts_updated": 0,
        "already_present": 0,
        "already_present_live": 0,
        "ambiguous": 0,
        "changed_or_conflicting": 0,
        "errors": 0,
    }
    error_status_counts: dict[str, int] = {}

    def snapshot(phase: str, **extra: Any) -> dict[str, Any]:
        return {
            "phase": phase,
            "source_keys": len(source_map),
            **counters,
            "contacts_updated_total": historical_updated + counters["contacts_updated"],
            "error_status_counts": error_status_counts,
            **extra,
        }

    async def control_state(phase: str) -> dict[str, Any] | None:
        live_settings = await _settings_map()
        if live_settings.get("cdb_contact_email_backfill_pending") != "1":
            return snapshot(phase, ok=True, halted=True)
        if _protected_bizon_window_now():
            return snapshot(phase, ok=True, paused=True)
        return None

    for page in range(1, 1001):
        stopped = await control_state("leads")
        if stopped:
            await _set_setting(
                "cdb_contact_email_backfill_result", json.dumps(stopped, ensure_ascii=False),
            )
            return stopped
        leads, done, lead_error = await _amo_entity_page(
            "leads", page, settings, with_value="contacts",
        )
        if lead_error:
            return snapshot("leads", ok=False, error=lead_error)
        counters["leads_scanned"] += len(leads)
        counters["utm_conflicts"] += _index_leads_for_contact_email_backfill(
            leads, source_map, deal_contact_ids, utm_evidence,
        )
        counters["lead_contacts"] = len(deal_contact_ids)
        if page % 10 == 0 or done:
            await _set_setting(
                "cdb_contact_email_backfill_result",
                json.dumps(snapshot("leads"), ensure_ascii=False),
            )
        if done:
            break
    else:
        return snapshot("leads", ok=False, error="Выгрузка amoCRM leads достигла лимита 250000")

    for page in range(1, 1001):
        stopped = await control_state("contacts")
        if stopped:
            await _set_setting(
                "cdb_contact_email_backfill_result", json.dumps(stopped, ensure_ascii=False),
            )
            return stopped
        contacts, done, contact_error = await _amo_entity_page(
            "contacts", page, settings, with_value="leads",
        )
        if contact_error:
            return snapshot("contacts", ok=False, error=contact_error)
        counters["contacts_scanned"] += len(contacts)
        targets, page_stats = _plan_contact_email_page(
            contacts, source_map, deal_contact_ids, utm_evidence,
        )
        counters["planned_empty_contacts"] += len(targets)
        counters["already_present"] += page_stats["already_present"]
        counters["ambiguous"] += page_stats["ambiguous"]
        for target in targets:
            result: dict[str, Any] = {}
            for attempt in range(3):
                result = await _patch_backfill_target_email(target, source_map, settings)
                if result.get("ok"):
                    break
                if attempt < 2:
                    await asyncio.sleep(2 + attempt * 3)
            status = _clean(result.get("status"), 80) or "unknown_error"
            counters["contacts_updated"] += int(result.get("updated_count") or 0)
            if status in {"already_present", "email_filled_before_update"}:
                counters["already_present_live"] += 1
            elif status in {
                "source_conflict", "email_conflict", "ambiguous_contact",
                "changed_before_update", "no_match", "invalid_source",
            }:
                counters["changed_or_conflicting"] += 1
            elif not result.get("ok"):
                counters["errors"] += 1
                error_status_counts[status] = error_status_counts.get(status, 0) + 1
        await _set_setting(
            "cdb_contact_email_backfill_result",
            json.dumps(snapshot("contacts"), ensure_ascii=False),
        )
        if done:
            break
    else:
        return snapshot(
            "contacts", ok=False, error="Выгрузка amoCRM contacts достигла лимита 250000",
        )
    return snapshot(
        "completed", ok=counters["errors"] == 0, completed=True,
    )


async def _contact_email_backfill_loop() -> None:
    while True:
        try:
            settings = await _settings_map()
            if settings.get("cdb_contact_email_backfill_pending") == "1":
                if _protected_bizon_window_now():
                    await _set_setting("cdb_contact_email_backfill_status", "waiting_bizon_window")
                elif _env()["amo_base_url"] and _env()["amo_token"]:
                    await _set_setting("cdb_contact_email_backfill_status", "running")
                    result = await _backfill_all_contact_emails(settings)
                    await _set_setting(
                        "cdb_contact_email_backfill_result",
                        json.dumps(result, ensure_ascii=False),
                    )
                    await _set_setting(
                        "cdb_contact_email_backfill_status",
                        (
                            "waiting_bizon_window" if result.get("paused")
                            else "halted" if result.get("halted")
                            else "completed" if result.get("ok") else "retrying"
                        ),
                    )
                    if result.get("completed") and result.get("ok"):
                        await _set_setting("cdb_contact_email_backfill_pending", "0")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "contact EMAIL backfill failed: %s", exc)
            try:
                await _set_setting("cdb_contact_email_backfill_status", "retrying")
            except Exception:
                pass
        await asyncio.sleep(30)


async def _customer_db_users_for_exact_utm(utm_term: Any) -> tuple[set[str], str]:
    return await _customer_db_exact_user_claims("utm_term", utm_term, "gc_user_id")


async def _customer_db_users_for_exact_phone(phone: Any) -> tuple[set[str], str]:
    return await _customer_db_exact_user_claims("phone", phone, "gc_user_id")


async def _customer_db_exact_user_claims(
    identity_kind: str, identity_value: Any, claim_kind: str,
) -> tuple[set[str], str]:
    """Read one authoritative cdb_getcourse_users snapshot without global-WAL noise."""
    wanted = (
        _strict_utm_term(identity_value)
        if identity_kind == "utm_term" else _phone_identity(identity_value)
    )
    if not wanted:
        return set(), ""
    try:
        async with aiosqlite.connect(_customer_db_path()) as db:
            if identity_kind == "utm_term":
                cur = await db.execute(
                    """
                    SELECT platform_id,custom_fields
                    FROM cdb_getcourse_users
                    WHERE json_extract(custom_fields,'$.utm_term')=?
                    """,
                    (wanted,),
                )
            else:
                # Phone formatting is not canonical in historical rows; 5k user
                # rows are intentionally scanned inside one SQLite snapshot and
                # normalized in Python before any external mutation.
                cur = await db.execute(
                    "SELECT platform_id,custom_fields FROM cdb_getcourse_users"
                )
            rows = await cur.fetchall()
    except Exception as exc:
        return set(), f"Проверка GetCourse source claims: {exc}"
    claims: set[str] = set()
    for platform_id, raw in rows:
        try:
            fields = json.loads(raw or "{}")
        except Exception:
            continue
        if not isinstance(fields, dict):
            continue
        if identity_kind == "phone" and _phone_identity(fields.get("phone")) != wanted:
            continue
        if claim_kind == "email":
            claim = _clean(fields.get("email"), 320).casefold()
        else:
            claim = _clean(platform_id or fields.get("gc_user_id"), 120)
        if claim:
            claims.add(claim)
    return claims, ""


def _contact_raw_emails(contact: dict[str, Any]) -> set[str]:
    return {
        _clean(value, 500)
        for value in _entity_rule_values(contact, {"field_code": "EMAIL"})
        if _clean(value, 500)
    }


async def _contact_lead_ids_for_email_sync(
    contact: dict[str, Any], settings: dict[str, str],
) -> tuple[list[str], str]:
    lead_ids = [
        _clean(item.get("id"), 64)
        for item in (((contact.get("_embedded") or {}).get("leads")) or [])
        if isinstance(item, dict) and _clean(item.get("id"), 64)
    ]
    if lead_ids:
        return list(dict.fromkeys(lead_ids)), ""
    contact_id = _clean(contact.get("id"), 64)
    if not contact_id:
        return [], "Контакт amoCRM найден без ID"
    body, error, _ = await _user_email_amo_request(
        "GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", settings,
    )
    if error:
        return [], error
    lead_ids = [
        _clean(item.get("to_entity_id"), 64)
        for item in (((body or {}).get("_embedded") or {}).get("links") or [])
        if isinstance(item, dict)
        and _clean(item.get("to_entity_type"), 80).casefold() in {"", "leads"}
        and _clean(item.get("to_entity_id"), 64)
    ]
    return list(dict.fromkeys(lead_ids)), ""


async def _exact_phone_contacts(
    phone: Any, settings: dict[str, str],
) -> tuple[list[tuple[dict[str, Any], list[str]]], str]:
    """Find only live contacts whose PHONE is exact and which are linked to a deal."""
    wanted = _phone_identity(phone)
    if not wanted:
        return [], ""
    body, error, _ = await _user_email_amo_request(
        "GET", f"/api/v4/contacts?query={quote(wanted, safe='')}&with=leads&limit=100", settings,
    )
    if error:
        return [], error
    found = (((body or {}).get("_embedded") or {}).get("contacts") or [])
    if len(found) >= 100 or ((body or {}).get("_links") or {}).get("next"):
        return [], "Поиск amoCRM по телефону достиг безопасного лимита 100 записей"
    candidate_ids = {
        _clean((contact or {}).get("id"), 64)
        for contact in found
        if isinstance(contact, dict)
        and _clean(contact.get("id"), 64)
        and any(
            _phone_identity(value) == wanted
            for value in _entity_rule_values(contact, {"field_code": "PHONE"})
        )
    }
    exact: list[tuple[dict[str, Any], list[str]]] = []
    for contact_id in sorted(candidate_ids, key=lambda value: int(value)):
        live, live_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/contacts/{contact_id}?with=leads", settings,
        )
        if live_error:
            return [], live_error
        if (
            not isinstance(live, dict)
            or _clean(live.get("id"), 64) != contact_id
            or not any(
                _phone_identity(value) == wanted
                for value in _entity_rule_values(live, {"field_code": "PHONE"})
            )
        ):
            continue
        lead_ids, links_error = await _contact_lead_ids_for_email_sync(live, settings)
        if links_error:
            return [], links_error
        if lead_ids:
            exact.append((live, lead_ids))
    return exact, ""


async def _sync_user_email_by_phone(
    email: Any,
    phone: Any,
    settings: dict[str, str],
    *,
    dry_run: bool = False,
    expected_contact_id: str = "",
) -> dict[str, Any]:
    """Patch only empty EMAIL fields on exact-phone contacts linked to deals."""
    clean_email = _valid_email(email)
    clean_phone = _phone_identity(phone)
    if not clean_email or not clean_phone:
        return {"ok": True, "status": "invalid_source", "lead_id": "", "contact_id": ""}
    source_emails, source_error = await _customer_db_emails_for_exact_phone(clean_phone)
    if source_error:
        return {
            "ok": False, "status": "source_lookup_error", "lead_id": "", "contact_id": "",
            "error": source_error,
        }
    if source_emails != {clean_email}:
        return {
            "ok": True, "status": "source_conflict", "lead_id": "", "contact_id": "",
            "error": "Точный телефон не принадлежит одному email GetCourse",
        }
    contacts, lookup_error = await _exact_phone_contacts(clean_phone, settings)
    if lookup_error:
        return {
            "ok": False, "status": "lookup_error", "lead_id": "", "contact_id": "",
            "error": lookup_error,
        }
    if not contacts:
        return {"ok": True, "status": "no_match", "lead_id": "", "contact_id": ""}
    exact_contact_ids = {_clean(contact.get("id"), 64) for contact, _lead_ids in contacts}
    if expected_contact_id and exact_contact_ids != {_clean(expected_contact_id, 64)}:
        return {
            "ok": True, "status": "changed_before_update", "lead_id": "",
            "contact_id": ",".join(exact_contact_ids),
            "error": "Контакт по телефону изменился после предварительной проверки",
        }
    contact_fields, fields_error = await _amo_fields("contacts", settings)
    if fields_error:
        return {
            "ok": False, "status": "lookup_error", "lead_id": "", "contact_id": "",
            "error": fields_error,
        }
    email_field = next(
        (field for field in contact_fields if _clean(field.get("code"), 120).upper() == "EMAIL"),
        None,
    )
    if not email_field or not _int_or_none(email_field.get("id")):
        return {
            "ok": False, "status": "configuration_error", "lead_id": "", "contact_id": "",
            "error": "Поле EMAIL контакта amoCRM не найдено",
        }
    results: list[dict[str, Any]] = []
    for contact, initial_lead_ids in contacts:
        contact_id = _clean(contact.get("id"), 64)
        existing_raw = _contact_raw_emails(contact)
        existing_valid = {_valid_email(value) for value in existing_raw if _valid_email(value)}
        if existing_raw:
            results.append({
                "ok": True,
                "status": "already_present" if clean_email in existing_valid else "email_conflict",
                "contact_id": contact_id,
                "lead_ids": initial_lead_ids,
            })
            continue
        if dry_run:
            results.append({
                "ok": True, "status": "would_update", "contact_id": contact_id,
                "lead_ids": initial_lead_ids,
            })
            continue
        # Repeat all mutable identity checks immediately before the only PATCH.
        source_emails, source_error = await _customer_db_emails_for_exact_phone(clean_phone)
        if source_error or source_emails != {clean_email}:
            results.append({
                "ok": not bool(source_error),
                "status": "source_lookup_error" if source_error else "source_conflict",
                "contact_id": contact_id, "lead_ids": initial_lead_ids,
                "error": source_error or "Источник GetCourse изменился до записи",
            })
            continue
        live, live_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/contacts/{contact_id}?with=leads", settings,
        )
        if live_error or not isinstance(live, dict) or _clean(live.get("id"), 64) != contact_id:
            results.append({
                "ok": False, "status": "lookup_error", "contact_id": contact_id,
                "lead_ids": initial_lead_ids,
                "error": live_error or "Повторная проверка контакта не пройдена",
            })
            continue
        phone_still_exact = any(
            _phone_identity(value) == clean_phone
            for value in _entity_rule_values(live, {"field_code": "PHONE"})
        )
        live_lead_ids, links_error = await _contact_lead_ids_for_email_sync(live, settings)
        if links_error:
            results.append({
                "ok": False, "status": "lookup_error", "contact_id": contact_id,
                "lead_ids": initial_lead_ids, "error": links_error,
            })
            continue
        if not phone_still_exact or not live_lead_ids:
            results.append({
                "ok": True, "status": "changed_before_update", "contact_id": contact_id,
                "lead_ids": live_lead_ids,
                "error": "Телефон или связь контакта со сделкой изменились до записи",
            })
            continue
        final_contact, final_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/contacts/{contact_id}", settings,
        )
        if (
            final_error
            or not isinstance(final_contact, dict)
            or _clean(final_contact.get("id"), 64) != contact_id
        ):
            results.append({
                "ok": False, "status": "lookup_error", "contact_id": contact_id,
                "lead_ids": live_lead_ids,
                "error": final_error or "Финальная проверка контакта не пройдена",
            })
            continue
        if not any(
            _phone_identity(value) == clean_phone
            for value in _entity_rule_values(final_contact, {"field_code": "PHONE"})
        ):
            results.append({
                "ok": True, "status": "changed_before_update", "contact_id": contact_id,
                "lead_ids": live_lead_ids, "error": "Телефон изменился до записи",
            })
            continue
        if _contact_raw_emails(final_contact):
            results.append({
                "ok": True, "status": "email_filled_before_update", "contact_id": contact_id,
                "lead_ids": live_lead_ids,
            })
            continue
        payload = {
            "custom_fields_values": [{
                "field_id": int(email_field["id"]),
                "values": [{"value": clean_email, "enum_code": "WORK"}],
            }]
        }
        _body, patch_error, _ = await _user_email_amo_request(
            "PATCH", f"/api/v4/contacts/{contact_id}", settings, payload,
        )
        results.append({
            "ok": not bool(patch_error),
            "status": "update_error" if patch_error else "updated",
            "contact_id": contact_id, "lead_ids": live_lead_ids, "error": patch_error,
        })
    statuses = [str(item["status"]) for item in results]
    errors = [str(item.get("error") or "") for item in results if item.get("error")]
    failed = [item for item in results if not item.get("ok")]
    if failed:
        status = str(failed[0]["status"]) if len(results) == 1 else "partial_error"
    else:
        status = next(
            (
                candidate for candidate in (
                    "updated", "would_update", "email_conflict", "already_present",
                    "email_filled_before_update", "source_conflict", "changed_before_update",
                ) if candidate in statuses
            ),
            "no_match",
        )
    lead_ids = list(dict.fromkeys(
        lead_id for item in results for lead_id in item.get("lead_ids", []) if lead_id
    ))
    contact_ids = list(dict.fromkeys(
        str(item.get("contact_id") or "") for item in results if item.get("contact_id")
    ))
    return {
        "ok": not failed, "status": status, "lead_id": ",".join(lead_ids),
        "contact_id": ",".join(contact_ids), "error": "; ".join(errors),
        "updated_count": statuses.count("updated"),
    }


async def _sync_user_email_by_utm(
    email: Any,
    utm_term: Any,
    settings: dict[str, str],
    *,
    dry_run: bool = False,
    expected_contact_id: str = "",
) -> dict[str, Any]:
    """Patch only an empty EMAIL field on one unambiguous main contact."""
    clean_email = _valid_email(email)
    clean_utm = _clean(utm_term, 1000)
    if not clean_email or not _strict_utm_term(clean_utm):
        return {"ok": True, "status": "invalid_source", "lead_id": "", "contact_id": ""}
    leads, error = await _exact_utm_leads(clean_utm, settings)
    if error:
        return {"ok": False, "status": "lookup_error", "lead_id": "", "contact_id": "", "error": error}
    if not leads:
        return {"ok": True, "status": "no_match", "lead_id": "", "contact_id": ""}
    contact_ids: set[str] = set()
    lead_contact_ids: dict[str, str] = {}
    contact_lead_ids: dict[str, list[str]] = {}
    lead_ids: list[str] = []
    for lead in leads:
        lead_id = _clean(lead.get("id"), 64)
        contact_id, contact_error = _main_contact_id(lead)
        if contact_error:
            return {
                "ok": True, "status": "ambiguous_contact", "lead_id": lead_id,
                "contact_id": "", "error": contact_error,
            }
        lead_ids.append(lead_id)
        contact_ids.add(contact_id)
        lead_contact_ids[lead_id] = contact_id
        contact_lead_ids.setdefault(contact_id, []).append(lead_id)
    sorted_contact_ids = sorted(contact_ids, key=lambda value: int(value))
    joined_contact_ids = ",".join(sorted_contact_ids)
    if expected_contact_id and contact_ids != {_clean(expected_contact_id, 64)}:
        return {
            "ok": True, "status": "changed_before_update", "lead_id": ",".join(lead_ids),
            "contact_id": joined_contact_ids,
            "error": "Основной контакт по utm_term изменился после предварительной проверки",
        }
    contacts: dict[str, dict[str, Any]] = {}
    has_empty_contact = False
    for contact_id in sorted_contact_ids:
        contact, contact_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/contacts/{contact_id}", settings,
        )
        if contact_error:
            return {
                "ok": False, "status": "lookup_error", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id, "error": contact_error,
            }
        if not isinstance(contact, dict) or _clean(contact.get("id"), 64) != contact_id:
            return {
                "ok": False, "status": "lookup_error", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id, "error": "amoCRM вернула не тот контакт или ответ без ID",
            }
        existing_raw = _contact_raw_emails(contact)
        existing_valid = {_valid_email(value) for value in existing_raw if _valid_email(value)}
        if existing_raw and clean_email not in existing_valid:
            return {
                "ok": True, "status": "email_conflict", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id,
            }
        has_empty_contact = has_empty_contact or not existing_raw
        contacts[contact_id] = contact
    source_emails, source_error = await _customer_db_emails_for_exact_utm(clean_utm)
    if source_error:
        return {
            "ok": False, "status": "source_lookup_error", "lead_id": ",".join(lead_ids),
            "contact_id": contact_id, "error": source_error,
        }
    if source_emails != {clean_email}:
        return {
            "ok": True, "status": "source_conflict", "lead_id": ",".join(lead_ids),
            "contact_id": joined_contact_ids,
            "error": "Точный utm_term не принадлежит одному email GetCourse",
        }
    contact_fields, fields_error = await _amo_fields("contacts", settings)
    if fields_error:
        return {
            "ok": False, "status": "lookup_error", "lead_id": ",".join(lead_ids),
            "contact_id": joined_contact_ids, "error": fields_error,
        }
    email_field = next(
        (field for field in contact_fields if _clean(field.get("code"), 120).upper() == "EMAIL"),
        None,
    )
    if not email_field or not _int_or_none(email_field.get("id")):
        return {
            "ok": False, "status": "configuration_error", "lead_id": ",".join(lead_ids),
            "contact_id": joined_contact_ids, "error": "Поле EMAIL контакта amoCRM не найдено",
        }
    if dry_run:
        return {
            "ok": True, "status": "would_update" if has_empty_contact else "already_present",
            "lead_id": ",".join(lead_ids), "contact_id": joined_contact_ids,
        }
    # Narrow provider races immediately before the only mutation: the same
    # exact deals must still point to the same contact, the source claim must
    # still be unique, and EMAIL must still be empty.
    for lead_id in lead_ids:
        live_lead, live_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/leads/{lead_id}?with=contacts", settings,
        )
        if live_error or not isinstance(live_lead, dict):
            return {
                "ok": False, "status": "lookup_error", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id, "error": live_error or "Сделка amoCRM недоступна",
            }
        still_matches, utm_conflict = _lead_utm_term_match(live_lead, clean_utm)
        current_contact_id, link_error = _main_contact_id(live_lead)
        if (
            utm_conflict or not still_matches or link_error
            or current_contact_id != lead_contact_ids.get(lead_id)
        ):
            return {
                "ok": True, "status": "changed_before_update", "lead_id": ",".join(lead_ids),
                "contact_id": joined_contact_ids,
                "error": "UTM или основной контакт изменились до записи",
            }
    source_emails, source_error = await _customer_db_emails_for_exact_utm(clean_utm)
    if source_error or source_emails != {clean_email}:
        return {
            "ok": not bool(source_error),
            "status": "source_lookup_error" if source_error else "source_conflict",
            "lead_id": ",".join(lead_ids), "contact_id": joined_contact_ids,
            "error": source_error or "Источник GetCourse изменился до записи",
        }
    payload = {
        "custom_fields_values": [{
            "field_id": int(email_field["id"]),
            "values": [{"value": clean_email, "enum_code": "WORK"}],
        }]
    }
    statuses: list[str] = []
    for contact_id in sorted_contact_ids:
        if _contact_raw_emails(contacts[contact_id]):
            statuses.append("already_present")
            continue
        final_contact, final_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/contacts/{contact_id}", settings,
        )
        if (
            final_error
            or not isinstance(final_contact, dict)
            or _clean(final_contact.get("id"), 64) != contact_id
        ):
            return {
                "ok": False, "status": "lookup_error", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id,
                "error": final_error or "Повторная проверка контакта не пройдена",
            }
        final_raw = _contact_raw_emails(final_contact)
        final_valid = {_valid_email(value) for value in final_raw if _valid_email(value)}
        if final_raw:
            statuses.append("already_present" if clean_email in final_valid else "email_conflict")
            continue
        _body, patch_error, _ = await _user_email_amo_request(
            "PATCH", f"/api/v4/contacts/{contact_id}", settings, payload,
        )
        if patch_error:
            return {
                "ok": False, "status": "update_error", "lead_id": ",".join(lead_ids),
                "contact_id": contact_id, "error": patch_error,
            }
        statuses.append("updated")
    return {
        "ok": True,
        "status": (
            "updated" if "updated" in statuses
            else "email_conflict" if "email_conflict" in statuses
            else "already_present"
        ),
        "lead_id": ",".join(lead_ids), "contact_id": joined_contact_ids,
        "updated_count": statuses.count("updated"),
    }


def _merge_user_email_sync_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"ok": True, "status": "invalid_source", "lead_id": "", "contact_id": ""}
    statuses = [_clean(item.get("status"), 80) for item in results]
    failures = [item for item in results if not item.get("ok")]
    contact_ids = list(dict.fromkeys(
        contact_id
        for item in results
        for contact_id in _clean(item.get("contact_id"), 2000).split(",")
        if contact_id
    ))
    pending = {"source_conflict", "ambiguous_contact", "changed_before_update", "no_match"}
    if failures:
        status = _clean(failures[0].get("status"), 80) if len(results) == 1 else "partial_error"
    elif len(contact_ids) > 1 or "ambiguous_contact" in statuses:
        status = "ambiguous_contact"
    elif "would_update" in statuses:
        status = "would_update_partial" if any(item in pending for item in statuses) else "would_update"
    elif "updated" in statuses:
        status = "partial_pending" if any(item in pending for item in statuses) else "updated"
    else:
        terminal_status = next((
            candidate for candidate in (
                "email_conflict", "already_present", "email_filled_before_update",
            ) if candidate in statuses
        ), "")
        if terminal_status and any(item in pending for item in statuses):
            status = "partial_pending"
        else:
            status = terminal_status or next((
                candidate for candidate in (
                    "source_conflict", "changed_before_update", "no_match",
                ) if candidate in statuses
            ), "invalid_source")
    lead_ids = list(dict.fromkeys(
        lead_id
        for item in results
        for lead_id in _clean(item.get("lead_id"), 2000).split(",")
        if lead_id
    ))
    errors = [
        _clean(item.get("error"), 1000) for item in results if _clean(item.get("error"), 1000)
    ]
    return {
        "ok": not failures,
        "status": status,
        "lead_id": ",".join(lead_ids),
        "contact_id": ",".join(contact_ids),
        "error": "; ".join(errors),
    }


async def _sync_user_email(
    email: Any,
    phone: Any,
    utm_term: Any,
    settings: dict[str, str],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Prefer exact UTM; fall back to exact phone when UTM has no amoCRM match."""
    if not _valid_email(email):
        return {"ok": True, "status": "invalid_source", "lead_id": "", "contact_id": ""}
    if _strict_utm_term(utm_term):
        utm_result = await _sync_user_email_by_utm(
            email, utm_term, settings, dry_run=dry_run,
        )
        if utm_result.get("status") not in {"no_match", "invalid_source"}:
            return utm_result
    if _phone_identity(phone):
        return await _sync_user_email_by_phone(email, phone, settings, dry_run=dry_run)
    return {"ok": True, "status": "no_match", "lead_id": "", "contact_id": ""}


def _gc_profile_url(gc_user_id: Any, settings: dict[str, str]) -> str:
    base = _clean(settings.get("getcourse_base_url"), 500).rstrip("/")
    user_id = _clean(gc_user_id, 120)
    return f"{base}/user/control/user/update/id/{quote(user_id)}" if base and user_id else ""


def _named_amo_field(
    fields: list[dict[str, Any]], name: str, field_type: str = "",
) -> dict[str, Any] | None:
    return next(
        (
            field for field in fields
            if _clean(field.get("name"), 300).casefold() == name.casefold()
            and (not field_type or _clean(field.get("type"), 80) == field_type)
            and _int_or_none(field.get("id"))
        ),
        None,
    )


def _missing_named_field_values(
    entity: dict[str, Any],
    field_catalog: list[dict[str, Any]],
    wanted: list[tuple[str, Any]],
    *,
    conflict_names: set[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    conflict_keys = {name.casefold() for name in (conflict_names or set())}
    values: list[dict[str, Any]] = []
    for name, value in wanted:
        if value is None or value == "":
            continue
        field = _named_amo_field(field_catalog, name)
        if not field:
            return [], f"Поле сделки amoCRM «{name}» не найдено"
        existing = {
            _clean(item, 2000)
            for item in _entity_rule_values(entity, {"field_id": str(field["id"])})
            if _clean(item, 2000)
        }
        clean_value = _clean(value, 2000)
        if existing and clean_value not in existing and name.casefold() in conflict_keys:
            return [], f"Поле сделки amoCRM «{name}» уже содержит другого пользователя"
        if not existing:
            values.append({"field_id": int(field["id"]), "values": [{"value": value}]})
    return values, ""


async def _ensure_gc_binding_fields(
    settings: dict[str, str], *, dry_run: bool = True,
) -> dict[str, Any]:
    fields, error = await _amo_fields("leads", settings)
    if error:
        return {"ok": False, "status": "lookup_error", "error": error, "missing": []}
    missing = [name for name in GC_ORDER_FIELD_NAMES if not _named_amo_field(fields, name)]
    return {
        "ok": not missing,
        "status": "ready" if not missing else "configuration_error",
        "missing": [{"entity": "leads", "name": name} for name in missing],
        "error": "" if not missing else "В amoCRM отсутствуют существующие поля блока ГК",
        "creates_fields": False,
    }


async def _save_gc_profile_binding(
    row: dict[str, Any], source_hash: str, gc_user_id: str, utm_term: str,
    match_kind: str, match_value: str, lead_id: str, contact_id: str,
    status: str, error: str = "",
) -> None:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            """
            INSERT INTO gc_profile_bindings(
                source_record_id,gc_user_id,source_hash,utm_term,match_kind,match_value,
                lead_id,contact_id,status,success,error,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_record_id) DO UPDATE SET
                gc_user_id=excluded.gc_user_id,
                source_hash=excluded.source_hash,
                utm_term=excluded.utm_term,
                match_kind=excluded.match_kind,
                match_value=excluded.match_value,
                lead_id=excluded.lead_id,
                contact_id=excluded.contact_id,
                status=excluded.status,
                success=excluded.success,
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (
                int(row["id"]), gc_user_id, source_hash, utm_term, match_kind, match_value,
                lead_id, contact_id,
                status, 1 if status in {"bound", "already_bound"} else 0,
                _clean(error, 1000), _now(),
            ),
        )
        await db.commit()


def _profile_target_result(
    status: str, *, ok: bool = True, error: str = "", match_kind: str = "",
    match_value: str = "", lead: dict[str, Any] | None = None,
    lead_id: str = "", contact_id: str = "",
) -> dict[str, Any]:
    return {
        "ok": ok, "status": status, "error": error,
        "match_kind": match_kind, "match_value": match_value,
        "lead": lead or {}, "lead_id": lead_id, "contact_id": contact_id,
    }


async def _profile_target_by_utm(
    gc_user_id: str, utm_term: str, settings: dict[str, str],
) -> dict[str, Any]:
    wanted = _strict_utm_term(utm_term)
    if not wanted:
        return _profile_target_result("identifier_missing", match_kind="utm_term")
    users, error = await _customer_db_users_for_exact_utm(wanted)
    if error:
        return _profile_target_result(
            "source_lookup_error", ok=False, error=error,
            match_kind="utm_term", match_value=wanted,
        )
    if users != {gc_user_id}:
        return _profile_target_result(
            "source_conflict", error="utm_term GetCourse не принадлежит одному профилю",
            match_kind="utm_term", match_value=wanted,
        )
    leads, error = await _exact_utm_leads(wanted, settings)
    if error:
        return _profile_target_result(
            "lookup_error", ok=False, error=error,
            match_kind="utm_term", match_value=wanted,
        )
    if len(leads) != 1:
        return _profile_target_result(
            "no_match" if not leads else "ambiguous_deal",
            error="По utm_term нужна ровно одна сделка",
            match_kind="utm_term", match_value=wanted,
            lead_id=",".join(_clean(item.get("id"), 64) for item in leads),
        )
    lead = leads[0]
    lead_id = _clean(lead.get("id"), 64)
    contact_id, link_error = _main_contact_id(lead)
    if link_error:
        return _profile_target_result(
            "ambiguous_contact", error=link_error,
            match_kind="utm_term", match_value=wanted, lead_id=lead_id,
        )
    return _profile_target_result(
        "matched", match_kind="utm_term", match_value=wanted,
        lead=lead, lead_id=lead_id, contact_id=contact_id,
    )


async def _profile_target_by_phone(
    gc_user_id: str, phone: str, settings: dict[str, str],
) -> dict[str, Any]:
    wanted = _phone_identity(phone)
    if not wanted:
        return _profile_target_result("identifier_missing", match_kind="phone")
    users, error = await _customer_db_users_for_exact_phone(wanted)
    if error:
        return _profile_target_result(
            "source_lookup_error", ok=False, error=error,
            match_kind="phone", match_value=wanted,
        )
    if users != {gc_user_id}:
        return _profile_target_result(
            "source_conflict", error="Телефон GetCourse не принадлежит одному профилю",
            match_kind="phone", match_value=wanted,
        )
    contacts, error = await _exact_phone_contacts(wanted, settings)
    if error:
        return _profile_target_result(
            "lookup_error", ok=False, error=error,
            match_kind="phone", match_value=wanted,
        )
    contact_map: dict[str, set[str]] = {}
    for contact, lead_ids in contacts:
        contact_id = _clean(contact.get("id"), 64)
        if contact_id:
            contact_map.setdefault(contact_id, set()).update(
                _clean(lead_id, 64) for lead_id in lead_ids if _clean(lead_id, 64)
            )
    if not contact_map:
        return _profile_target_result(
            "no_match", match_kind="phone", match_value=wanted,
        )
    if len(contact_map) != 1:
        return _profile_target_result(
            "ambiguous_contact", error="Точный телефон найден у нескольких контактов сделок",
            match_kind="phone", match_value=wanted,
            contact_id=",".join(sorted(contact_map, key=lambda value: int(value))),
        )
    contact_id, lead_ids = next(iter(contact_map.items()))
    if len(lead_ids) != 1:
        return _profile_target_result(
            "ambiguous_deal", error="Контакт по телефону связан не с одной сделкой",
            match_kind="phone", match_value=wanted, contact_id=contact_id,
            lead_id=",".join(sorted(lead_ids, key=lambda value: int(value))),
        )
    lead_id = next(iter(lead_ids))
    lead, error, _ = await _user_email_amo_request(
        "GET", f"/api/v4/leads/{lead_id}?with=contacts", settings,
    )
    if error or not isinstance(lead, dict) or _clean(lead.get("id"), 64) != lead_id:
        return _profile_target_result(
            "lookup_error", ok=False, error=error or "Сделка amoCRM недоступна",
            match_kind="phone", match_value=wanted, contact_id=contact_id, lead_id=lead_id,
        )
    main_contact_id, link_error = _main_contact_id(lead)
    if link_error or main_contact_id != contact_id:
        return _profile_target_result(
            "ambiguous_contact", error="Контакт по телефону не является основным контактом сделки",
            match_kind="phone", match_value=wanted, contact_id=contact_id, lead_id=lead_id,
        )
    return _profile_target_result(
        "matched", match_kind="phone", match_value=wanted,
        lead=lead, lead_id=lead_id, contact_id=contact_id,
    )


async def _resolve_gc_profile_target(
    gc_user_id: str, utm_term: str, phone: str, settings: dict[str, str],
    *, only_kind: str = "",
) -> dict[str, Any]:
    if only_kind == "utm_term":
        return await _profile_target_by_utm(gc_user_id, utm_term, settings)
    if only_kind == "phone":
        return await _profile_target_by_phone(gc_user_id, phone, settings)
    utm_result = await _profile_target_by_utm(gc_user_id, utm_term, settings)
    if utm_result.get("status") == "matched" or not utm_result.get("ok"):
        return utm_result
    phone_result = await _profile_target_by_phone(gc_user_id, phone, settings)
    if phone_result.get("status") == "matched" or not phone_result.get("ok"):
        return phone_result
    if phone_result.get("status") != "identifier_missing":
        if utm_result.get("status") != "identifier_missing":
            phone_result["error"] = "; ".join(filter(None, (
                _clean(utm_result.get("error"), 500), _clean(phone_result.get("error"), 500),
            )))
        return phone_result
    return utm_result


async def _sync_gc_profile_binding(
    row: dict[str, Any], fields: dict[str, Any], settings: dict[str, str],
    *, dry_run: bool = False,
) -> dict[str, Any]:
    """Prefer exact UTM; otherwise bind by one exact phone contact and one deal."""
    gc_user_id = _clean(row.get("platform_id") or fields.get("gc_user_id"), 120)
    utm_term = _strict_utm_term(fields.get("utm_term"))
    phone = _phone_identity(fields.get("phone"))
    source_hash = _user_email_source_hash(fields)
    if not gc_user_id or (not utm_term and not phone):
        return {
            "ok": True, "status": "no_identity", "lead_id": "", "contact_id": "",
            "gc_user_id": gc_user_id,
        }
    target = await _resolve_gc_profile_target(gc_user_id, utm_term, phone, settings)
    if target.get("status") != "matched":
        return {
            **{key: target.get(key, "") for key in ("ok", "status", "lead_id", "contact_id", "error")},
            "gc_user_id": gc_user_id,
        }
    lead = target["lead"]
    lead_id = _clean(target.get("lead_id"), 64)
    contact_id = _clean(target.get("contact_id"), 64)
    match_kind = _clean(target.get("match_kind"), 40)
    match_value = _clean(target.get("match_value"), 1000)
    lead_fields, lead_fields_error = await _amo_fields("leads", settings)
    if lead_fields_error:
        return {"ok": False, "status": "lookup_error", "lead_id": lead_id,
                "contact_id": contact_id, "gc_user_id": gc_user_id, "error": lead_fields_error}
    profile_url = _gc_profile_url(gc_user_id, settings)
    lead_values, lead_value_error = _missing_named_field_values(
        lead, lead_fields, [(GC_PROFILE_LINK_FIELD, profile_url)],
        conflict_names={GC_PROFILE_LINK_FIELD},
    )
    if lead_value_error:
        return {
            "ok": False, "status": "configuration_error", "lead_id": lead_id,
            "contact_id": contact_id, "gc_user_id": gc_user_id,
            "error": lead_value_error,
        }
    if dry_run:
        return {
            "ok": True,
            "status": "would_bind" if lead_values else "already_bound",
            "lead_id": lead_id, "contact_id": contact_id, "gc_user_id": gc_user_id,
            "match_kind": match_kind,
        }
    live_target = await _resolve_gc_profile_target(
        gc_user_id, utm_term, phone, settings, only_kind=match_kind,
    )
    if (
        live_target.get("status") != "matched"
        or _clean(live_target.get("match_value"), 1000) != match_value
        or _clean(live_target.get("lead_id"), 64) != lead_id
        or _clean(live_target.get("contact_id"), 64) != contact_id
    ):
        return {
            "ok": bool(live_target.get("ok")), "status": "changed_before_update",
            "lead_id": lead_id, "contact_id": contact_id, "gc_user_id": gc_user_id,
            "error": _clean(live_target.get("error"), 1000) or "Источник или сделка изменились до записи",
        }
    live_lead = live_target["lead"]
    lead_values, lead_value_error = _missing_named_field_values(
        live_lead, lead_fields, [(GC_PROFILE_LINK_FIELD, profile_url)],
        conflict_names={GC_PROFILE_LINK_FIELD},
    )
    if lead_value_error:
        return {
            "ok": False, "status": "configuration_error", "lead_id": lead_id,
            "contact_id": contact_id, "gc_user_id": gc_user_id,
            "error": lead_value_error,
        }
    if lead_values:
        _body, patch_error, _ = await _user_email_amo_request(
            "PATCH", f"/api/v4/leads/{lead_id}", settings,
            {"custom_fields_values": lead_values},
        )
        if patch_error:
            return {
                "ok": False, "status": "update_error", "lead_id": lead_id,
                "contact_id": contact_id, "gc_user_id": gc_user_id, "error": patch_error,
            }
    status = "bound" if lead_values else "already_bound"
    await _save_gc_profile_binding(
        row, source_hash, gc_user_id, utm_term, match_kind, match_value,
        lead_id, contact_id, status,
    )
    return {
        "ok": True, "status": status, "lead_id": lead_id, "contact_id": contact_id,
        "gc_user_id": gc_user_id, "match_kind": match_kind,
    }


async def _sync_gc_profile(
    row: dict[str, Any], fields: dict[str, Any], settings: dict[str, str],
    *, dry_run: bool = False,
) -> dict[str, Any]:
    email_result = await _sync_user_email(
        fields.get("email"), fields.get("phone"), fields.get("utm_term"),
        settings, dry_run=dry_run,
    )
    binding_result = await _sync_gc_profile_binding(row, fields, settings, dry_run=dry_run)
    binding_result["email_status"] = _clean(email_result.get("status"), 80)
    if (
        not dry_run
        and _db_path
        and binding_result.get("status") in {"bound", "already_bound"}
    ):
        binding_result["order_backfill"] = await _sync_profile_orders_for_user(
            binding_result.get("gc_user_id"), settings,
        )
    if binding_result.get("status") == "no_identity":
        return email_result
    if binding_result.get("status") in {"bound", "already_bound", "would_bind"}:
        if not email_result.get("ok"):
            binding_result["ok"] = False
            binding_result["status"] = "partial_error"
            binding_result["error"] = _clean(email_result.get("error"), 1000)
        elif dry_run and email_result.get("status") == "would_update":
            binding_result["status"] = "would_update"
    return binding_result


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


async def service_create_onboarding_support_task(
    *, source_record_id: int = 0, order_id: str = "", text: str = "", due_minutes: int = 60,
    test_lead_id: str = "", phone: str = "", email: str = "", utm_term: str = "",
) -> dict[str, Any]:
    """Create a care task on an already mapped deal; never create a deal."""

    clean_order_id = _clean(order_id, 100)
    source_id = max(0, int(source_record_id or 0))
    explicit_test_lead = _clean(test_lead_id, 64)
    if explicit_test_lead and not clean_order_id.startswith("onboarding-live-test-"):
        return {"ok": False, "status": "invalid", "lead_id": "", "task_id": "", "error": "Явная тестовая сделка запрещена"}
    lead_id = explicit_test_lead
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        if not lead_id and clean_order_id:
            row = await (
                await db.execute(
                    "SELECT lead_id FROM order_map WHERE order_key=? AND lead_id<>'' ORDER BY updated_at DESC LIMIT 1",
                    (f"order:{clean_order_id}",),
                )
            ).fetchone()
            lead_id = _clean(row[0] if row else "", 64)
        if not lead_id and source_id:
            row = await (
                await db.execute(
                    "SELECT lead_id FROM cdb_sync WHERE source_record_id=? AND lead_id<>'' LIMIT 1",
                    (source_id,),
                )
            ).fetchone()
            lead_id = _clean(row[0] if row else "", 64)
    if not lead_id:
        resolved = await service_resolve_onboarding_manager(phone=phone, email=email, utm_term=utm_term)
        if not resolved.get("ok"):
            return {
                "ok": False, "status": "failed", "lead_id": "", "task_id": "",
                "error": _clean(resolved.get("error"), 1000) or "Не удалось найти сделку amoCRM",
            }
        if resolved.get("found") and resolved.get("entity") == "lead":
            lead_id = _clean(resolved.get("entity_id"), 64)
    if not lead_id:
        return {"ok": False, "status": "not_found", "lead_id": "", "task_id": "", "error": "Актуальная сделка amoCRM не найдена"}

    settings = await _settings_map()
    lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
    if lead_error or not isinstance(lead, dict):
        return {"ok": False, "status": "failed", "lead_id": lead_id, "task_id": "", "error": lead_error or "Сделка amoCRM недоступна"}
    task_text = _clean(text, 2000) or f"Нужна помощь с доступом GetCourse, заказ {clean_order_id}"
    binding = {
        "task_enabled": 1,
        "task_text": task_text,
        "task_due_minutes": max(1, min(60 * 24 * 30, int(due_minutes or 60))),
        "task_type_id": 1,
        "task_responsible_user_id": "",
        "responsible_user_id": "",
    }
    result, error = await _create_task_for_lead(
        lead_id,
        {"order_id": clean_order_id, "number": clean_order_id},
        settings,
        binding,
        _clean(lead.get("responsible_user_id"), 64),
    )
    if error:
        return {"ok": False, "status": "failed", "lead_id": lead_id, "task_id": "", "error": error}
    return {
        "ok": True,
        "status": "existing" if result.get("skipped") else "created",
        "lead_id": lead_id,
        "task_id": _clean(result.get("task_id"), 64),
        "responsible_user_id": _clean(lead.get("responsible_user_id"), 64),
    }


async def service_add_onboarding_confirmation_note(
    *, source_record_id: int = 0, order_id: str = "", phone: str = "", email: str = "",
    utm_term: str = "", text: str = "Пользователь подтвердил вход GetCourse",
) -> dict[str, Any]:
    """Add one idempotent confirmation note to the newest mapped customer deal; never create a deal."""

    clean_order_id = _clean(order_id, 100)
    source_id = max(0, int(source_record_id or 0))
    lead_id = ""
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        if clean_order_id:
            row = await (
                await db.execute(
                    "SELECT lead_id FROM order_map WHERE order_key=? AND lead_id<>'' ORDER BY updated_at DESC LIMIT 1",
                    (f"order:{clean_order_id}",),
                )
            ).fetchone()
            lead_id = _clean(row[0] if row else "", 64)
        if not lead_id and source_id:
            row = await (
                await db.execute(
                    "SELECT lead_id FROM cdb_sync WHERE source_record_id=? AND lead_id<>'' LIMIT 1",
                    (source_id,),
                )
            ).fetchone()
            lead_id = _clean(row[0] if row else "", 64)

    if not lead_id:
        resolved = await service_resolve_onboarding_manager(phone=phone, email=email, utm_term=utm_term)
        if not resolved.get("ok"):
            return {
                "ok": False, "status": "failed", "lead_id": "", "note_id": "",
                "error": _clean(resolved.get("error"), 1000) or "Не удалось найти сделку amoCRM",
            }
        if resolved.get("found") and resolved.get("entity") == "lead":
            lead_id = _clean(resolved.get("entity_id"), 64)
    if not lead_id:
        return {
            "ok": False, "status": "not_found", "lead_id": "", "note_id": "",
            "error": "Актуальная сделка amoCRM не найдена",
        }

    settings = await _settings_map()
    note_text = _clean(text, 2000) or "Пользователь подтвердил вход GetCourse"
    existing, existing_error, _ = await _amo_request(
        "GET", f"/api/v4/leads/{lead_id}/notes?limit=250", settings,
    )
    if not existing_error:
        for note in (((existing or {}).get("_embedded") or {}).get("notes") or []):
            if note.get("note_type") == "common" and _clean((note.get("params") or {}).get("text"), 2000) == note_text:
                return {
                    "ok": True, "status": "existing", "lead_id": lead_id,
                    "note_id": _clean(note.get("id"), 64), "error": "",
                }
    body, error, _ = await _amo_request(
        "POST", f"/api/v4/leads/{lead_id}/notes", settings,
        [{"note_type": "common", "params": {"text": note_text}}],
    )
    if error:
        return {"ok": False, "status": "failed", "lead_id": lead_id, "note_id": "", "error": error}
    note_id = ""
    try:
        note_id = _clean((((body or {}).get("_embedded") or {}).get("notes") or [{}])[0].get("id"), 64)
    except Exception:
        note_id = ""
    return {"ok": True, "status": "created", "lead_id": lead_id, "note_id": note_id, "error": ""}


async def service_resolve_onboarding_manager(
    *, phone: str = "", email: str = "", utm_term: str = "",
) -> dict[str, Any]:
    """Resolve an active amoCRM manager by exact customer identity."""

    settings = await _settings_map()
    candidates: list[dict[str, Any]] = []

    async def contact_candidates(kind: str, query: str, rule: dict[str, str]) -> str:
        body, error, _ = await _amo_request(
            "GET", f"/api/v4/contacts?query={quote(query)}&with=leads&limit=50", settings
        )
        if error:
            return error
        for contact in (((body or {}).get("_embedded") or {}).get("contacts") or []):
            if not any(_compare_value(value, query) for value in _entity_rule_values(contact, rule)):
                continue
            lead_ids = [
                _clean(item.get("id"), 64)
                for item in ((contact.get("_embedded") or {}).get("leads") or [])
                if isinstance(item, dict)
            ]
            if not any(lead_ids):
                contact_id = _clean(contact.get("id"), 64)
                links, links_error, _ = await _amo_request(
                    "GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", settings
                )
                if links_error:
                    return links_error
                lead_ids = [
                    _clean(item.get("to_entity_id"), 64)
                    for item in (((links or {}).get("_embedded") or {}).get("links") or [])
                    if isinstance(item, dict)
                ]
            for lead_id in [item for item in lead_ids if item]:
                lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}", settings)
                if lead_error:
                    return lead_error
                if isinstance(lead, dict) and _int_or_none(lead.get("responsible_user_id")):
                    candidates.append({
                        "source": kind, "entity": "lead", "entity_id": _clean(lead.get("id"), 64),
                        "responsible_user_id": _clean(lead.get("responsible_user_id"), 64),
                        "updated_at": int(lead.get("updated_at") or 0),
                    })
            if _int_or_none(contact.get("responsible_user_id")):
                candidates.append({
                    "source": kind, "entity": "contact", "entity_id": _clean(contact.get("id"), 64),
                    "responsible_user_id": _clean(contact.get("responsible_user_id"), 64),
                    "updated_at": int(contact.get("updated_at") or 0),
                })
        return ""

    checks = [
        ("phone", _clean(phone, 100), {"field_code": "PHONE"}),
        ("email", _clean(email, 500), {"field_code": "EMAIL"}),
    ]
    for kind, query, rule in checks:
        if not query:
            continue
        error = await contact_candidates(kind, query, rule)
        if error:
            return {"ok": False, "found": False, "error": error}
        if candidates:
            break

    clean_utm = _clean(utm_term, 500)
    if not candidates and clean_utm:
        body, error, _ = await _amo_request(
            "GET", f"/api/v4/leads?query={quote(clean_utm)}&limit=50", settings
        )
        if error:
            return {"ok": False, "found": False, "error": error}
        for lead in (((body or {}).get("_embedded") or {}).get("leads") or []):
            values = _entity_rule_values(lead, {"field_code": "UTM_TERM"})
            values.extend(_entity_rule_values(lead, {"field": "utm_term"}))
            if not any(_compare_value(value, clean_utm) for value in values):
                continue
            if _int_or_none(lead.get("responsible_user_id")):
                candidates.append({
                    "source": "utm_term", "entity": "lead", "entity_id": _clean(lead.get("id"), 64),
                    "responsible_user_id": _clean(lead.get("responsible_user_id"), 64),
                    "updated_at": int(lead.get("updated_at") or 0),
                })

    if not candidates:
        return {"ok": True, "found": False, "source": "", "manager_user_id": "", "manager_name": ""}
    users, users_error, _ = await _amo_request("GET", "/api/v4/users?limit=250", settings)
    if users_error:
        return {"ok": False, "found": False, "error": users_error}
    active_users = {
        _clean(user.get("id"), 64): _clean(user.get("name"), 300)
        for user in (((users or {}).get("_embedded") or {}).get("users") or [])
        if isinstance(user, dict)
        and _int_or_none(user.get("id"))
        and (user.get("rights") or {}).get("is_active", user.get("is_active", True))
    }
    candidates = [item for item in candidates if item["responsible_user_id"] in active_users]
    if not candidates:
        return {"ok": True, "found": False, "source": "", "manager_user_id": "", "manager_name": ""}
    candidates.sort(
        key=lambda item: (
            1 if item["entity"] == "lead" else 0,
            int(item["updated_at"]),
            int(item["entity_id"]) if item["entity_id"].isdigit() else 0,
        ),
        reverse=True,
    )
    selected = candidates[0]
    deal_id = selected["entity_id"] if selected["entity"] == "lead" else ""
    amo_base_url = _env()["amo_base_url"]
    return {
        "ok": True,
        "found": True,
        "source": selected["source"],
        "entity": selected["entity"],
        "entity_id": selected["entity_id"],
        "deal_id": deal_id,
        "deal_url": f"{amo_base_url}/leads/detail/{deal_id}" if amo_base_url and deal_id else "",
        "manager_user_id": selected["responsible_user_id"],
        "manager_name": active_users[selected["responsible_user_id"]],
    }


async def _store_event(data: dict[str, Any]) -> int:
    keys = ["method", "order_id", "number", "lead_id", "contact_id", "action", "success", "ignored", "error", "details", "raw_payload"]
    async with _connect() as db:
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
        "ignore_surcharge_title": "в названии заказа нет «Доплата до Premium/VIP»",
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
    try:
        order["utm_inheritance"] = await _inherit_missing_attribution(order, settings)
        if order["utm_inheritance"].get("status") == "lookup_error":
            await _attribution_alert(
                "lookup_error",
                "❌ Nexus не смог проверить наследование UTM\n"
                f"Заказ GetCourse: {order.get('number') or order.get('order_id')}\n"
                f"{_clean(order['utm_inheritance'].get('error'), 1000)}",
            )
    except Exception as exc:
        order["utm_inheritance"] = {
            "status": "error", "filled": [], "source_lead_id": "", "error": _clean(exc, 1000),
        }
        await _attribution_alert(
            "exception",
            "❌ Ошибка автоматического наследования UTM\n"
            f"Заказ GetCourse: {order.get('number') or order.get('order_id')}\n{_clean(exc, 1000)}",
        )
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
            if not process_error and result["lead_id"]:
                # This branch deliberately preserves the existing amoCRM deal,
                # but the paid GetCourse order still belongs to that deal.  Keep
                # the same durable order -> lead mapping as the create/update
                # branches so read-only consumers (Streams, onboarding) can
                # resolve the exact deal instead of guessing by phone/email.
                await _remember_lead(order, result["lead_id"])
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
    async with _connect() as db:
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


async def _customer_db_user_rows(after_id: int = 0, limit: int = 10) -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT id,platform_id,custom_fields,updated_at
                FROM cdb_getcourse_users
                WHERE id>?
                ORDER BY id ASC
                LIMIT ?
                """,
                (max(0, int(after_id)), max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit)))),
            )
            return [dict(row) for row in await cur.fetchall()]
    except Exception as exc:
        _log("warning", "customer-db getcourse_users read failed: %s", exc)
        return []


async def _customer_db_profile_order_rows(after_id: int = 0, limit: int = 10) -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT id,platform_id,custom_fields,updated_at
                FROM cdb_getcourse_orders
                WHERE id>?
                ORDER BY id ASC
                LIMIT ?
                """,
                (max(0, int(after_id)), max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit)))),
            )
            return [dict(row) for row in await cur.fetchall()]
    except Exception as exc:
        _log("warning", "customer-db profile order read failed: %s", exc)
        return []


async def _customer_db_profile_order_rows_for_user(
    gc_user_id: Any, limit: int = 100,
) -> list[dict[str, Any]]:
    user_id = _clean(gc_user_id, 120)
    db_path = _customer_db_path()
    if not user_id or not db_path.exists():
        return []
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT id,platform_id,custom_fields,updated_at
                FROM cdb_getcourse_orders
                WHERE json_extract(custom_fields,'$.gc_user_id')=?
                ORDER BY id ASC
                LIMIT ?
                """,
                (user_id, max(1, min(500, int(limit)))),
            )
            return [dict(row) for row in await cur.fetchall()]
    except Exception as exc:
        _log("warning", "customer-db profile orders for user read failed: %s", exc)
        return []


async def _current_gc_profile(gc_user_id: Any) -> tuple[dict[str, Any], str]:
    user_id = _clean(gc_user_id, 120)
    if not user_id:
        return {}, "gc_user_id пустой"
    try:
        async with aiosqlite.connect(_customer_db_path()) as db:
            cur = await db.execute(
                """
                SELECT custom_fields
                FROM cdb_getcourse_users
                WHERE platform_id=?
                ORDER BY id DESC
                LIMIT 2
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
    except Exception as exc:
        return {}, f"Профиль GetCourse недоступен: {exc}"
    if len(rows) != 1:
        return {}, "Профиль GetCourse не найден или продублирован"
    try:
        fields = json.loads(rows[0][0] or "{}")
    except Exception:
        return {}, "Профиль GetCourse содержит некорректный JSON"
    return fields if isinstance(fields, dict) else {}, ""


async def _gc_profile_binding(gc_user_id: Any) -> dict[str, Any]:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM gc_profile_bindings WHERE gc_user_id=? AND success=1 LIMIT 2",
            (_clean(gc_user_id, 120),),
        )
        rows = await cur.fetchall()
    return dict(rows[0]) if len(rows) == 1 else {}


async def _profile_order_note_state(source_record_id: int) -> dict[str, Any]:
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT * FROM gc_profile_order_notes WHERE source_record_id=?",
                (int(source_record_id),),
            )
        ).fetchone()
    return dict(row) if row else {}


async def _save_profile_order_note_state(
    row: dict[str, Any], source_hash: str, gc_user_id: str, lead_id: str,
    note_hash: str, status: str, error: str = "", *, fields_synced: bool = False,
) -> None:
    success = status in {
        "noted", "already_noted", *ORDER_EMAIL_RESULT_STATUS_BY_STATE,
    }
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            """
            INSERT INTO gc_profile_order_notes(
                source_record_id,source_hash,gc_user_id,lead_id,note_hash,
                status,success,fields_synced,error,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_record_id) DO UPDATE SET
                source_hash=excluded.source_hash,
                gc_user_id=excluded.gc_user_id,
                lead_id=excluded.lead_id,
                note_hash=excluded.note_hash,
                status=excluded.status,
                success=excluded.success,
                fields_synced=excluded.fields_synced,
                error=excluded.error,
                updated_at=excluded.updated_at
            """,
            (
                int(row["id"]), source_hash, gc_user_id, lead_id, note_hash,
                status, 1 if success else 0, 1 if fields_synced else 0,
                _clean(error, 1000), _now(),
            ),
        )
        await db.commit()


async def _add_profile_order_note(
    lead_id: str, text: str, settings: dict[str, str], *,
    gc_user_id: str, match_kind: str, match_value: str, contact_id: str,
) -> tuple[str, str]:
    for page in range(1, 5):
        existing, existing_error, _ = await _user_email_amo_request(
            "GET", f"/api/v4/leads/{lead_id}/notes?limit=250&page={page}", settings,
        )
        if existing_error:
            return "", existing_error
        notes = (((existing or {}).get("_embedded") or {}).get("notes") or [])
        for note in notes:
            if (
                note.get("note_type") == "common"
                and _clean((note.get("params") or {}).get("text"), 10000) == text
            ):
                return "already_noted", ""
        has_next = bool(((existing or {}).get("_links") or {}).get("next"))
        if not has_next and len(notes) < 250:
            break
        if page == 4:
            return "", "Проверка примечаний достигла безопасного лимита 1000 записей"
    valid, validation_error = await _validate_live_gc_binding(
        gc_user_id, match_kind, match_value, lead_id, contact_id, settings,
    )
    if not valid:
        return "", validation_error or "Привязка изменилась перед записью примечания"
    _body, error, _ = await _user_email_amo_request(
        "POST", f"/api/v4/leads/{lead_id}/notes", settings,
        [{"note_type": "common", "params": {"text": text}}],
    )
    return ("noted" if not error else ""), error


async def _validate_live_gc_binding(
    gc_user_id: str, match_kind: str, match_value: str,
    lead_id: str, contact_id: str,
    settings: dict[str, str],
) -> tuple[bool, str]:
    profile, profile_error = await _current_gc_profile(gc_user_id)
    if profile_error:
        return False, profile_error
    kind = _clean(match_kind, 40) or "utm_term"
    current_value = (
        _strict_utm_term(profile.get("utm_term"))
        if kind == "utm_term" else _phone_identity(profile.get("phone"))
    )
    if current_value != _clean(match_value, 1000):
        return False, f"{kind} профиля изменился"
    target = await _resolve_gc_profile_target(
        gc_user_id,
        current_value if kind == "utm_term" else "",
        current_value if kind == "phone" else "",
        settings,
        only_kind=kind,
    )
    if target.get("status") != "matched":
        return False, _clean(target.get("error"), 1000) or "Связь профиля со сделкой изменилась"
    if (
        _clean(target.get("lead_id"), 64) != lead_id
        or _clean(target.get("contact_id"), 64) != contact_id
        or _clean(target.get("match_value"), 1000) != _clean(match_value, 1000)
    ):
        return False, "Сделка или основной контакт изменились"
    return True, ""


def _gc_order_lead_field_values(
    lead: dict[str, Any], field_catalog: list[dict[str, Any]], order: dict[str, Any],
) -> list[dict[str, Any]]:
    allowed = {name.casefold() for name in GC_ORDER_FIELD_NAMES}
    allowed_ids = {
        int(field["id"])
        for field in field_catalog
        if _int_or_none(field.get("id"))
        and _clean(field.get("name"), 300).casefold() in allowed
    }
    changes: list[dict[str, Any]] = []
    for item in _lead_field_values(field_catalog, order):
        field_id = int(item.get("field_id") or 0)
        if field_id not in allowed_ids:
            continue
        incoming = [
            value.get("value") for value in (item.get("values") or [])
            if isinstance(value, dict) and value.get("value") not in (None, "")
        ]
        existing = _entity_rule_values(lead, {"field_id": str(field_id)})
        if incoming and not (
            len(existing) == len(incoming)
            and all(any(_compare_value(old, new) for old in existing) for new in incoming)
        ):
            changes.append(item)
    return changes


async def _sync_bound_profile_order_note(
    row: dict[str, Any], settings: dict[str, str], *, dry_run: bool = False,
) -> dict[str, Any]:
    try:
        fields = json.loads(row.get("custom_fields") or "{}")
    except Exception:
        fields = {}
    if not isinstance(fields, dict):
        fields = {}
    source_hash = _customer_db_source_hash(row.get("custom_fields"))
    email_state: dict[str, Any] = {}
    if _db_path and Path(_db_path).exists() and not dry_run:
        email_state = await _profile_order_note_state(int(row["id"]))
    email_result = {"ok": True, "status": "not_checked"}
    if (
        int(email_state.get("success") or 0)
        and email_state.get("source_hash") == source_hash
        and _clean(email_state.get("status"), 100).startswith("email_")
    ):
        email_result = {
            "ok": True,
            "status": ORDER_EMAIL_RESULT_STATUS_BY_STATE.get(
                _clean(email_state.get("status"), 100), "invalid_source",
            ),
            "lead_id": _clean(email_state.get("lead_id"), 2000),
            "contact_id": "",
            "state_skipped": True,
        }
    elif _db_path:
        email_result = await _sync_user_email(
            fields.get("email"), fields.get("phone"),
            fields.get("utm_term") or fields.get("user_term"),
            settings, dry_run=dry_run,
        )
        if not email_result.get("ok"):
            return {
                "ok": False, "status": "lookup_error", "lead_id": "",
                "gc_user_id": _clean(fields.get("gc_user_id"), 120),
                "error": _clean(email_result.get("error"), 1000)
                or f"Email sync: {_clean(email_result.get('status'), 80)}",
            }
    gc_user_id = _clean(fields.get("gc_user_id"), 120)
    email_status = _clean(email_result.get("status"), 80)
    if not gc_user_id:
        result_status = ORDER_EMAIL_TERMINAL_STATUS_BY_RESULT.get(email_status, "invalid_source")
        if (
            not dry_run and _db_path and Path(_db_path).exists()
            and result_status.startswith("email_") and not email_result.get("state_skipped")
        ):
            await _save_profile_order_note_state(
                row, source_hash, "", _clean(email_result.get("lead_id"), 2000),
                "", result_status,
            )
        return {
            "ok": True, "status": result_status,
            "lead_id": _clean(email_result.get("lead_id"), 2000), "gc_user_id": "",
        }
    binding = await _gc_profile_binding(gc_user_id)
    if not binding:
        result_status = ORDER_EMAIL_TERMINAL_STATUS_BY_RESULT.get(email_status, "no_binding")
        if (
            not dry_run and _db_path and Path(_db_path).exists()
            and result_status.startswith("email_") and not email_result.get("state_skipped")
        ):
            await _save_profile_order_note_state(
                row, source_hash, gc_user_id, _clean(email_result.get("lead_id"), 2000),
                "", result_status,
            )
        return {
            "ok": True, "status": result_status,
            "lead_id": _clean(email_result.get("lead_id"), 2000),
            "gc_user_id": gc_user_id,
        }
    lead_id = _clean(binding.get("lead_id"), 64)
    contact_id = _clean(binding.get("contact_id"), 64)
    match_kind = _clean(binding.get("match_kind"), 40) or "utm_term"
    match_value = _clean(
        binding.get("match_value") or (
            binding.get("utm_term") if match_kind == "utm_term" else ""
        ),
        1000,
    )
    profile, profile_error = await _current_gc_profile(gc_user_id)
    current_value = (
        _strict_utm_term(profile.get("utm_term"))
        if match_kind == "utm_term" else _phone_identity(profile.get("phone"))
    )
    if profile_error or not current_value or current_value != match_value:
        return {
            "ok": not bool(profile_error), "status": "binding_changed", "lead_id": lead_id,
            "gc_user_id": gc_user_id,
            "error": profile_error or f"{match_kind} профиля изменился после привязки",
        }
    order = _normalize_order(_payload_from_customer_db(fields), settings)
    requested_process = fields.get("payment_state") or fields.get("status") or ""
    order["process"] = _route_order(order, requested_process, settings)
    _apply_attribution(order, settings)
    note_text = _format_order_template(
        settings.get("note_template") or DEFAULT_NOTE_TEMPLATE, order, 10000,
    )
    note_hash = hashlib.sha256(note_text.encode("utf-8")).hexdigest()
    state = await _profile_order_note_state(int(row["id"]))
    if (
        int(state.get("success") or 0)
        and state.get("source_hash") == source_hash
        and _clean(state.get("lead_id"), 64) == lead_id
        and state.get("note_hash") == note_hash
        and int(state.get("fields_synced") or 0)
    ):
        return {
            "ok": True, "status": "already_noted", "lead_id": lead_id,
            "gc_user_id": gc_user_id, "state_skipped": True,
            "email_status": _clean(email_result.get("status"), 80),
        }
    target = await _resolve_gc_profile_target(
        gc_user_id,
        current_value if match_kind == "utm_term" else "",
        current_value if match_kind == "phone" else "",
        settings,
        only_kind=match_kind,
    )
    if target.get("status") != "matched":
        return {
            "ok": bool(target.get("ok")), "status": "binding_changed", "lead_id": lead_id,
            "gc_user_id": gc_user_id,
            "error": _clean(target.get("error"), 1000) or "Связь профиля со сделкой изменилась",
        }
    if (
        _clean(target.get("lead_id"), 64) != lead_id
        or _clean(target.get("contact_id"), 64) != contact_id
        or _clean(target.get("match_value"), 1000) != match_value
    ):
        return {
            "ok": True, "status": "binding_changed", "lead_id": lead_id,
            "gc_user_id": gc_user_id,
            "error": "Сделка или основной контакт изменились после привязки",
        }
    lead = target["lead"]
    lead_fields, fields_error = await _amo_fields("leads", settings)
    if fields_error:
        return {
            "ok": False, "status": "lookup_error", "lead_id": lead_id,
            "gc_user_id": gc_user_id, "error": fields_error,
        }
    field_values = _gc_order_lead_field_values(lead, lead_fields, order)
    if dry_run:
        return {
            "ok": True,
            "status": "would_note_and_fill_fields" if field_values else "would_note",
            "gc_user_id": gc_user_id,
        }
    if field_values:
        _body, field_error, _ = await _user_email_amo_request(
            "PATCH", f"/api/v4/leads/{lead_id}", settings,
            {"custom_fields_values": field_values},
        )
        if field_error:
            await _save_profile_order_note_state(
                row, source_hash, gc_user_id, lead_id, note_hash,
                "field_update_error", field_error, fields_synced=False,
            )
            return {
                "ok": False, "status": "field_update_error", "lead_id": lead_id,
                "gc_user_id": gc_user_id, "error": field_error,
            }
    status, note_error = await _add_profile_order_note(
        lead_id, note_text, settings,
        gc_user_id=gc_user_id, match_kind=match_kind,
        match_value=match_value, contact_id=contact_id,
    )
    result_status = status or "note_error"
    await _save_profile_order_note_state(
        row, source_hash, gc_user_id, lead_id, note_hash, result_status, note_error,
        fields_synced=True,
    )
    return {
        "ok": not bool(note_error), "status": result_status, "lead_id": lead_id,
        "gc_user_id": gc_user_id, "error": note_error,
        "email_status": _clean(email_result.get("status"), 80),
    }


async def _sync_profile_orders_for_user(
    gc_user_id: Any, settings: dict[str, str], *, limit: int = 100,
) -> dict[str, Any]:
    """Immediately backfill a newly bound profile; the global cursor remains a fallback."""
    async with _profile_order_note_sync_lock:
        rows = await _customer_db_profile_order_rows_for_user(gc_user_id, limit=limit)
        statuses: dict[str, int] = {}
        errors: list[str] = []
        for row in rows:
            result = await _sync_bound_profile_order_note(row, settings)
            status = _clean(result.get("status"), 80) or "unknown"
            statuses[status] = statuses.get(status, 0) + 1
            if not result.get("ok"):
                errors.append(_clean(result.get("error"), 1000) or status)
        return {
            "processed": len(rows), "statuses": statuses,
            "ok": not errors, "errors": errors[:10],
        }


async def _sync_profile_order_notes_once(
    *, limit: int = 10, after_id: int | None = None, dry_run: bool = False,
) -> dict[str, Any]:
    async with _profile_order_note_sync_lock:
        return await _sync_profile_order_notes_once_unlocked(
            limit=limit, after_id=after_id, dry_run=dry_run,
        )


async def _sync_profile_order_notes_once_unlocked(
    *, limit: int = 10, after_id: int | None = None, dry_run: bool = False,
) -> dict[str, Any]:
    if _protected_bizon_window_now():
        return {
            "ok": False, "processed": 0, "next_after_id": 0, "protected_window": True,
            "error": "Примечания заказов отложены до конца защищённого окна Bizon",
        }
    settings = await _settings_map()
    background = after_id is None
    scan_after = max(
        0,
        int(settings.get("cdb_profile_order_scan_after_id") or 0)
        if background else int(after_id or 0),
    )
    batch = max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit)))
    rows = await _customer_db_profile_order_rows(scan_after, batch)
    results: list[dict[str, Any]] = []
    for row in rows:
        result = await _sync_bound_profile_order_note(row, settings, dry_run=dry_run)
        results.append({
            "source_record_id": int(row["id"]),
            "status": result.get("status"),
            "lead_id": result.get("lead_id", ""),
            "error": result.get("error", ""),
        })
    next_after_id = int(rows[-1]["id"]) if rows else 0
    wrapped = bool(background and len(rows) < batch)
    if background and not dry_run:
        await _set_setting(
            "cdb_profile_order_scan_after_id", "0" if wrapped else str(next_after_id),
        )
    error_statuses = {"lookup_error", "field_update_error", "note_error", "error"}
    return {
        "ok": not any(item.get("status") in error_statuses for item in results),
        "dry_run": dry_run, "scanned": len(rows), "processed": len(results),
        "next_after_id": next_after_id, "wrapped": wrapped, "results": results,
    }


def _user_email_source_hash(fields: dict[str, Any]) -> str:
    value = {
        "email": _valid_email(fields.get("email")),
        "phone": _phone_identity(fields.get("phone")),
        "utm_term": _strict_utm_term(fields.get("utm_term")),
    }
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "v4:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _user_email_sync_states(record_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not record_ids:
        return {}
    placeholders = ",".join(["?"] * len(record_ids))
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM cdb_user_email_sync WHERE source_record_id IN ({placeholders})",
            tuple(record_ids),
        )
        return {int(row["source_record_id"]): dict(row) for row in await cur.fetchall()}


async def _mark_user_email_sync(
    row: dict[str, Any], source_hash: str, result: dict[str, Any], retry_minutes: int,
) -> None:
    status = _clean(result.get("status"), 80)
    terminal = status in {
        "updated", "already_present", "email_conflict", "email_filled_before_update",
        "invalid_source", "bound", "already_bound",
    }
    retry_at = ""
    if not terminal:
        retry_at = (
            datetime.now(timezone.utc) + timedelta(minutes=max(5, min(1440, retry_minutes)))
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        await db.execute(
            """
            INSERT INTO cdb_user_email_sync(
                source_record_id,source_updated_at,source_hash,status,lead_id,contact_id,
                success,attempts,error,next_retry_at,last_synced_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_record_id) DO UPDATE SET
                source_updated_at=excluded.source_updated_at,
                source_hash=excluded.source_hash,
                status=excluded.status,
                lead_id=excluded.lead_id,
                contact_id=excluded.contact_id,
                success=excluded.success,
                attempts=CASE
                    WHEN cdb_user_email_sync.source_hash=excluded.source_hash
                    THEN cdb_user_email_sync.attempts+1 ELSE 1 END,
                error=excluded.error,
                next_retry_at=excluded.next_retry_at,
                last_synced_at=excluded.last_synced_at
            """,
            (
                int(row["id"]), _clean(row.get("updated_at"), 80), source_hash, status,
                _clean(result.get("lead_id"), 500), _clean(result.get("contact_id"), 500),
                1 if terminal else 0, 1, _clean(result.get("error"), 1000), retry_at, _now(),
            ),
        )
        await db.commit()


async def _sync_getcourse_user_emails_once(
    *, limit: int = 10, after_id: int | None = None, dry_run: bool = False, force: bool = False,
) -> dict[str, Any]:
    async with _user_email_sync_lock:
        settings = await _settings_map()
        if settings.get("cdb_user_email_sync_enabled") != "1" and not force:
            return {"ok": True, "enabled": False, "processed": 0, "next_after_id": 0}
        if _protected_bizon_window_now():
            return {
                "ok": False, "enabled": settings.get("cdb_user_email_sync_enabled") == "1",
                "processed": 0, "next_after_id": 0,
                "protected_window": True,
                "error": "Email sync отложен до конца защищённого окна Bizon",
            }
        batch = max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit)))
        background = after_id is None
        scan_after = max(
            0,
            int(settings.get("cdb_user_email_scan_after_id") or 0) if background else int(after_id or 0),
        )
        rows = await _customer_db_user_rows(after_id=scan_after, limit=batch)
        states = await _user_email_sync_states([int(row["id"]) for row in rows]) if not dry_run else {}
        retry_minutes = max(5, min(1440, int(settings.get("cdb_user_email_retry_minutes") or 60)))
        now = _now()
        results: list[dict[str, Any]] = []
        state_skipped = 0
        retry_deferred = 0
        for row in rows:
            try:
                fields = json.loads(row.get("custom_fields") or "{}")
                if not isinstance(fields, dict):
                    raise ValueError("custom_fields is not an object")
                source_hash = _user_email_source_hash(fields)
                state = states.get(int(row["id"]))
                if state and state.get("source_hash") == source_hash and not force:
                    if int(state.get("success") or 0):
                        state_skipped += 1
                        continue
                    if _clean(state.get("next_retry_at"), 80) > now:
                        retry_deferred += 1
                        continue
                result = await _sync_gc_profile(row, fields, settings, dry_run=dry_run)
                results.append({
                    "source_record_id": int(row["id"]),
                    "status": result.get("status"),
                    "lead_id": result.get("lead_id", ""),
                    "contact_id": result.get("contact_id", ""),
                    "error": result.get("error", ""),
                })
                if not dry_run:
                    await _mark_user_email_sync(row, source_hash, result, retry_minutes)
                if result.get("status") not in {"invalid_source", "already_present", "email_conflict"}:
                    await asyncio.sleep(0.25)
            except Exception as exc:
                result = {"ok": False, "status": "error", "error": _clean(exc, 1000)}
                results.append({
                    "source_record_id": int(row.get("id") or 0), "status": "error",
                    "lead_id": "", "contact_id": "", "error": result["error"],
                })
                if not dry_run:
                    try:
                        fields = fields if isinstance(fields, dict) else {}
                    except Exception:
                        fields = {}
                    await _mark_user_email_sync(row, _user_email_source_hash(fields), result, retry_minutes)
        next_after_id = int(rows[-1]["id"]) if rows else 0
        wrapped = bool(background and len(rows) < batch)
        if background and not dry_run:
            await _set_setting("cdb_user_email_scan_after_id", "0" if wrapped else str(next_after_id))
        return {
            "ok": not any(item.get("status") in {
                "error", "lookup_error", "source_lookup_error", "update_error", "configuration_error",
                "partial_error",
            } for item in results),
            "enabled": settings.get("cdb_user_email_sync_enabled") == "1",
            "dry_run": dry_run,
            "scanned": len(rows),
            "processed": len(results),
            "state_skipped": state_skipped,
            "retry_deferred": retry_deferred,
            "next_after_id": next_after_id,
            "wrapped": wrapped,
            "results": results,
        }


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
    async with _connect() as db:
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
    async with _connect() as db:
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
    state_skipped = 0
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
                payment_state = _clean(fields.get("payment_state"), 80).casefold()
                if (
                    payment_state in CDB_AMO_IGNORED_PAYMENT_STATES
                    or payment_state not in CDB_AMO_PROCESSABLE_PAYMENT_STATES
                ):
                    await _mark_cdb_sync(
                        record_id,
                        updated_at,
                        source_hash,
                        {
                            "ok": True,
                            "ignored": True,
                            "error": f"customer-db state {payment_state} excluded from amo sync",
                        },
                    )
                    processed += 1
                    state_skipped += 1
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
        "state_skipped": state_skipped,
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
            if settings.get("cdb_user_email_sync_enabled") == "1" and env["amo_base_url"] and env["amo_token"]:
                batch = max(1, min(50, int(settings.get("cdb_user_email_sync_batch") or 10)))
                await _sync_getcourse_user_emails_once(limit=batch)
                await _sync_profile_order_notes_once(limit=batch)
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
        "getcourse_user_email_sync_enabled": settings.get("cdb_user_email_sync_enabled") == "1",
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


@router.post("/amo/ensure-profile-fields")
async def amo_ensure_profile_fields(request: Request, dry_run: int = 1):
    await _require_panel_user(request)
    if _protected_bizon_window_now() and not dry_run:
        return JSONResponse(
            {"ok": False, "protected_window": True, "error": "Создание полей отложено до конца окна Bizon"},
            status_code=409,
        )
    return await _ensure_gc_binding_fields(await _settings_map(), dry_run=bool(dry_run))


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
async def customer_db_sync_run(request: Request, backfill: int = 0, limit: int = MAX_MANUAL_SYNC_LIMIT):
    await _require_panel_user(request)
    return await _sync_customer_db_once(backfill=bool(backfill), limit=max(1, min(MAX_MANUAL_SYNC_LIMIT, int(limit))))


@router.get("/sync/getcourse-users-email/status")
async def getcourse_user_email_sync_status(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    async with aiosqlite.connect(_db_path) as db:  # type: ignore[arg-type]
        db.row_factory = aiosqlite.Row
        totals = dict(await (
            await db.execute(
                """
                SELECT COUNT(*) tracked,
                       SUM(CASE WHEN status='updated' THEN 1 ELSE 0 END) updated,
                       SUM(CASE WHEN status='already_present' THEN 1 ELSE 0 END) already_present,
                       SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) retrying,
                       SUM(CASE WHEN status IN ('email_conflict','ambiguous_contact') THEN 1 ELSE 0 END) conflicts
                FROM cdb_user_email_sync
                """
            )
        ).fetchone() or {})
        binding_totals = dict(await (
            await db.execute(
                """
                SELECT COUNT(*) tracked_bindings,
                       SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) active_bindings
                FROM gc_profile_bindings
                """
            )
        ).fetchone() or {})
        note_totals = dict(await (
            await db.execute(
                """
                SELECT COUNT(*) tracked_order_notes,
                       SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) delivered_order_notes,
                       SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) pending_order_notes
                FROM gc_profile_order_notes
                """
            )
        ).fetchone() or {})
    return {
        "enabled": settings.get("cdb_user_email_sync_enabled") == "1",
        "batch": int(settings.get("cdb_user_email_sync_batch") or 10),
        "scan_after_id": int(settings.get("cdb_user_email_scan_after_id") or 0),
        "order_scan_after_id": int(settings.get("cdb_profile_order_scan_after_id") or 0),
        "contact_backfill_pending": settings.get("cdb_contact_email_backfill_pending") == "1",
        "contact_backfill_status": settings.get("cdb_contact_email_backfill_status") or "",
        "contact_backfill_result": _jsonish(
            settings.get("cdb_contact_email_backfill_result") or "{}",
        ),
        "protected_window": _protected_bizon_window_now(),
        **{key: int(value or 0) for key, value in totals.items()},
        **{key: int(value or 0) for key, value in binding_totals.items()},
        **{key: int(value or 0) for key, value in note_totals.items()},
    }


@router.post("/sync/getcourse-users-email/run")
async def getcourse_user_email_sync_run(
    request: Request, after_id: int = 0, limit: int = 10, dry_run: int = 1,
):
    await _require_panel_user(request)
    return await _sync_getcourse_user_emails_once(
        limit=max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit))),
        after_id=max(0, int(after_id)),
        dry_run=bool(dry_run),
        force=True,
    )


@router.post("/sync/getcourse-profile-orders/run")
async def getcourse_profile_order_sync_run(
    request: Request, after_id: int = 0, limit: int = 10, dry_run: int = 1,
):
    await _require_panel_user(request)
    return await _sync_profile_order_notes_once(
        limit=max(1, min(MAX_USER_EMAIL_SYNC_LIMIT, int(limit))),
        after_id=max(0, int(after_id)),
        dry_run=bool(dry_run),
    )


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
