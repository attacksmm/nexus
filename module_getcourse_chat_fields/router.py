from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import importlib.util
import io
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

try:
    from orchestrator.auth import can_access_module, verify_token_from_request
except Exception:  # pragma: no cover - local smoke tests can run without Nexus
    can_access_module = None
    verify_token_from_request = None

router = APIRouter()

MODULE_ID = "getcourse-chat-fields"
DEFAULT_CURATOR_SPREADSHEET_ID = "1NbKpXgHCJVE1dzpxeDLNGzSfz53vfK0joBRrNtIAaqk"
DEFAULT_CHAT_LINKS_SPREADSHEET_ID = "1zu1__XcKxJH8yC9ForDvibaUnKFCS1pxWHEjLgqlVXA"
CHAT_LINK_SHEETS = {
    "dog": {"telegram": "304757615", "vk": "443062527"},
    "puppy": {"telegram": "1437498106", "vk": "65520414"},
}
_ctx = None
_db_path: Path | None = None
_logger: logging.Logger | None = None
_poll_task: asyncio.Task | None = None
_gc_lookup_task: asyncio.Task | None = None
_gc_write_task: asyncio.Task | None = None
_scan_lock = asyncio.Lock()
_students_cache_lock = asyncio.Lock()
_gc_lookup_lock = asyncio.Lock()
_gc_write_lock = asyncio.Lock()
_registry_write_lock = asyncio.Lock()
_chat_flows_cache: dict[str, Any] = {"key": "", "expires": 0.0, "data": None}
_flow_catalog_fallback_cache: dict[str, Any] = {"key": "", "expires": 0.0, "data": None}
_gc_access_groups_cache: dict[str, Any] = {"expires": 0.0, "items": []}
_gc_pending_exports: dict[str, tuple[str, float]] = {}
_registry_xlsx_export_cache: dict[str, Any] = {"key": "", "expires": 0.0, "rows": {}}

MACHINE_PREFIX = "chat_fields_"
CHAT_ENTITLEMENT_VERSION = 2
DEFAULT_FIELD_NAMES = {
    "field_stream": "Поток",
    "field_vk": "Ссылка на чат ВК",
    "field_tg": "Ссылка на чат ТГ",
    "field_curator": "Номер куратора",
}
DEFAULT_USER_FIELD_IDS = {
    "user_field_stream_id": "10335965",
    "user_field_vk_id": "12513209",
    "user_field_tg_id": "12513210",
    "user_field_curator_id": "13834169",
}
DEFAULT_CURATOR_MAP = "Ирина=Куратор 1;Слава=Куратор 2;Настасья=Куратор 3"
REGISTRY_CURATOR_SYNC_CACHE_KEY = "registry-curator-sync-v1"
ACCESS_SNAPSHOT_CACHE_PREFIX = "getcourse-access-v1:"
DEFAULT_SETTINGS = {
    "enabled": "1",
    "dry_run": "0",
    "poll_seconds": "60",
    "request_timeout": "20",
    "start_date": "",
    "curator_spreadsheet_id": DEFAULT_CURATOR_SPREADSHEET_ID,
    "curator_credentials_path": "",
    "curator_cell": "K2",
    "curator_search_range": "J2:AC2",
    "curator_map": DEFAULT_CURATOR_MAP,
    "chat_links_spreadsheet_id": DEFAULT_CHAT_LINKS_SPREADSHEET_ID,
    "chat_links_credentials_path": "",
    "students_cache_minutes": "30",
    "students_data_range": "A1:AC300",
    "students_order_lookup_limit": "20000",
    "getcourse_web_base_url": "https://club.sobakovod.pro",
    "gc_export_lookup_enabled": "0",
    "gc_export_lookup_max_requests_2h": "80",
    "gc_export_lookup_max_missing_per_refresh": "1",
    "gc_export_lookup_batch_size": "1",
    "gc_export_lookup_poll_attempts": "1",
    "gc_export_lookup_poll_delay_seconds": "2",
    "gc_export_lookup_cache_days": "30",
    "gc_export_lookup_deals_enabled": "1",
    "gc_export_lookup_auto_enqueue_enabled": "1",
    "gc_export_lookup_auto_enqueue_batch_size": "20",
    "gc_export_lookup_worker_interval_seconds": "60",
    "gc_export_lookup_job_timeout_seconds": "12",
    "gc_export_lookup_job_max_attempts": "3",
    "gc_fields_write_enabled": "0",
    "gc_fields_write_worker_interval_seconds": "60",
    "gc_fields_write_job_max_attempts": "3",
    "gc_fields_write_retry_base_seconds": "300",
    "gc_fields_write_retry_max_seconds": "21600",
    "gc_api_new_job_reserve_requests": "10",
    "access_snapshot_minutes": "60",
    **DEFAULT_USER_FIELD_IDS,
    **DEFAULT_FIELD_NAMES,
}


def _db_connect(path):
    return aiosqlite.connect(path, timeout=30)


def setup(ctx):
    global _ctx, _db_path, _logger, _poll_task, _gc_lookup_task, _gc_write_task
    _ctx = ctx
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger(f"nexus.mod.{MODULE_ID}"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        _poll_task = loop.create_task(_poll_loop())
        _gc_lookup_task = loop.create_task(_gc_lookup_loop())
        _gc_write_task = loop.create_task(_gc_write_loop())
    else:
        loop.run_until_complete(_init_db())


async def shutdown():
    global _poll_task, _gc_lookup_task, _gc_write_task
    tasks = [task for task in (_poll_task, _gc_lookup_task, _gc_write_task) if task]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _poll_task = _gc_lookup_task = _gc_write_task = None


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "on", "да"}


async def _require_user(request: Request) -> dict[str, Any]:
    if verify_token_from_request is None:
        return {"role": "admin", "username": "local"}
    user = await verify_token_from_request(request)
    if not user:
        raise HTTPException(status_code=401, detail="unauthorized")
    if can_access_module and not can_access_module(user, MODULE_ID):
        raise HTTPException(status_code=403, detail="forbidden")
    return user


async def _init_db() -> None:
    assert _db_path is not None
    _db_path.parent.mkdir(parents=True, exist_ok=True)
    async with _db_connect(_db_path) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS processed_orders (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                source_record_id    INTEGER NOT NULL UNIQUE,
                platform_id         TEXT NOT NULL DEFAULT '',
                order_id            TEXT NOT NULL DEFAULT '',
                gc_user_id          TEXT NOT NULL DEFAULT '',
                source_hash         TEXT NOT NULL DEFAULT '',
                status              TEXT NOT NULL DEFAULT '',
                course_key          TEXT NOT NULL DEFAULT '',
                tariff              TEXT NOT NULL DEFAULT '',
                stream              TEXT NOT NULL DEFAULT '',
                vk_link             TEXT NOT NULL DEFAULT '',
                tg_link             TEXT NOT NULL DEFAULT '',
                customer_ok         INTEGER NOT NULL DEFAULT 0,
                getcourse_ok        INTEGER NOT NULL DEFAULT 0,
                error               TEXT NOT NULL DEFAULT '',
                details_json        TEXT NOT NULL DEFAULT '{}',
                created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_processed_status ON processed_orders(status);
            CREATE INDEX IF NOT EXISTS idx_processed_updated ON processed_orders(updated_at);
            CREATE TABLE IF NOT EXISTS scan_runs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                finished_at   TEXT NOT NULL DEFAULT '',
                source_rows   INTEGER NOT NULL DEFAULT 0,
                processed     INTEGER NOT NULL DEFAULT 0,
                skipped       INTEGER NOT NULL DEFAULT 0,
                failed        INTEGER NOT NULL DEFAULT 0,
                dry_run       INTEGER NOT NULL DEFAULT 0,
                error         TEXT NOT NULL DEFAULT '',
                details_json  TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS flow_students_cache (
                key          TEXT PRIMARY KEY,
                value_json   TEXT NOT NULL DEFAULT '{}',
                updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS gc_export_api_calls (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                purpose       TEXT NOT NULL DEFAULT '',
                requested_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                details_json  TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_gc_export_api_calls_requested ON gc_export_api_calls(requested_at);
            CREATE TABLE IF NOT EXISTS gc_export_lookup_cache (
                email         TEXT PRIMARY KEY,
                gc_user_id    TEXT NOT NULL DEFAULT '',
                user_url      TEXT NOT NULL DEFAULT '',
                order_id      TEXT NOT NULL DEFAULT '',
                order_url     TEXT NOT NULL DEFAULT '',
                status        TEXT NOT NULL DEFAULT '',
                error         TEXT NOT NULL DEFAULT '',
                source_json   TEXT NOT NULL DEFAULT '{}',
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS gc_export_lookup_jobs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT NOT NULL UNIQUE,
                status        TEXT NOT NULL DEFAULT 'pending',
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_run_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                last_error    TEXT NOT NULL DEFAULT '',
                result_json   TEXT NOT NULL DEFAULT '{}',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_gc_export_lookup_jobs_status ON gc_export_lookup_jobs(status,next_run_at);
            CREATE TABLE IF NOT EXISTS gc_fields_write_jobs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT NOT NULL DEFAULT '',
                gc_user_id    TEXT NOT NULL DEFAULT '',
                order_id      TEXT NOT NULL DEFAULT '',
                deal_number   TEXT NOT NULL DEFAULT '',
                status        TEXT NOT NULL DEFAULT 'pending',
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_run_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                last_error    TEXT NOT NULL DEFAULT '',
                payload_json  TEXT NOT NULL DEFAULT '{}',
                result_json   TEXT NOT NULL DEFAULT '{}',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                UNIQUE(email, order_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gc_fields_write_jobs_status ON gc_fields_write_jobs(status,next_run_at);
            """
        )
        for key, value in DEFAULT_SETTINGS.items():
            if key == "start_date":
                value = _today()
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        await db.execute(
            """
            UPDATE gc_export_lookup_jobs
            SET status='pending', updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE status='running'
            """
        )
        await db.execute(
            """
            UPDATE gc_fields_write_jobs
            SET status='pending', updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE status='running'
            """
        )
        await db.execute(
            """
            UPDATE processed_orders
            SET status='quarantined', updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE status='failed' AND error='course not detected'
            """
        )
        await db.execute(
            """
            UPDATE gc_fields_write_jobs
            SET status='quarantined',
                next_run_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'),
                updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE status IN ('pending','failed','failed_exhausted')
              AND last_error LIKE '%Ошибка обновления заказа%'
            """
        )
        await db.commit()
    _log("info", "getcourse-chat-fields DB initialized")


def _env() -> dict[str, str]:
    return {
        "account_name": os.environ.get("GETCOURSE_ACCOUNT_NAME", "").strip(),
        "api_token": os.environ.get("GETCOURSE_API_TOKEN", "").strip(),
        "customer_db_path": os.environ.get("GETCOURSE_CHAT_FIELDS_CUSTOMER_DB_PATH", "").strip(),
        "course_chat_db_path": os.environ.get("GETCOURSE_CHAT_FIELDS_COURSE_CHAT_DB_PATH", "").strip(),
        "google_credentials_path": (
            os.environ.get("GETCOURSE_CHAT_FIELDS_GOOGLE_CREDENTIALS_FILE")
            or os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE")
            or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            or ""
        ).strip(),
        "curator_spreadsheet_id": (
            os.environ.get("GETCOURSE_CHAT_FIELDS_CURATOR_SPREADSHEET_ID")
            or os.environ.get("GOOGLE_SHEETS_STUDENTS_SPREADSHEET_ID")
            or ""
        ).strip(),
        "chat_links_spreadsheet_id": (
            os.environ.get("GETCOURSE_CHAT_FIELDS_LINKS_SPREADSHEET_ID")
            or os.environ.get("TILDA_CHAT_LINKS_SPREADSHEET_ID")
            or ""
        ).strip(),
    }


async def _settings_map() -> dict[str, str]:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data = DEFAULT_SETTINGS.copy()
    data["start_date"] = _today()
    data.update({str(row[0]): str(row[1] or "") for row in rows})
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, str]:
    allowed = set(DEFAULT_SETTINGS)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        for key in allowed:
            if key not in data:
                continue
            value = _clean(data.get(key), 5000)
            if key in {"enabled", "dry_run"}:
                value = "1" if _truthy(value) else "0"
            if key == "poll_seconds":
                value = str(_bounded_int(value, 10, 3600, 60))
            if key == "request_timeout":
                value = str(_bounded_int(value, 5, 60, 20))
            if key == "students_cache_minutes":
                value = str(_bounded_int(value, 1, 1440, 30))
            if key == "students_order_lookup_limit":
                value = str(_bounded_int(value, 100, 200000, 20000))
            if key == "gc_export_lookup_max_requests_2h":
                value = str(_bounded_int(value, 0, 100, 80))
            if key == "gc_export_lookup_max_missing_per_refresh":
                value = str(_bounded_int(value, 0, 1000, 50))
            if key == "gc_export_lookup_batch_size":
                value = str(_bounded_int(value, 1, 100, 50))
            if key == "gc_export_lookup_poll_attempts":
                value = str(_bounded_int(value, 1, 5, 2))
            if key == "gc_export_lookup_poll_delay_seconds":
                value = str(_bounded_int(value, 0, 20, 2))
            if key == "gc_export_lookup_cache_days":
                value = str(_bounded_int(value, 1, 365, 30))
            if key == "gc_export_lookup_deals_enabled":
                value = "1" if _truthy(value) else "0"
            if key == "gc_export_lookup_auto_enqueue_enabled":
                value = "1" if _truthy(value) else "0"
            if key == "gc_export_lookup_auto_enqueue_batch_size":
                value = str(_bounded_int(value, 1, 100, 20))
            if key == "gc_export_lookup_worker_interval_seconds":
                value = str(_bounded_int(value, 10, 3600, 60))
            if key == "gc_export_lookup_job_timeout_seconds":
                value = str(_bounded_int(value, 3, 60, 12))
            if key == "gc_export_lookup_job_max_attempts":
                value = str(_bounded_int(value, 1, 10, 3))
            if key == "gc_fields_write_enabled":
                value = "1" if _truthy(value) else "0"
            if key == "gc_fields_write_worker_interval_seconds":
                value = str(_bounded_int(value, 10, 3600, 60))
            if key == "gc_fields_write_job_max_attempts":
                value = str(_bounded_int(value, 1, 10, 3))
            if key == "gc_fields_write_retry_base_seconds":
                value = str(_bounded_int(value, 60, 3600, 300))
            if key == "gc_fields_write_retry_max_seconds":
                value = str(_bounded_int(value, 300, 86400, 21600))
            if key == "gc_api_new_job_reserve_requests":
                value = str(_bounded_int(value, 0, 50, 10))
            if key == "start_date" and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                value = _today()
            if key == "curator_cell":
                value = value.upper() if re.fullmatch(r"[A-Z]{1,3}\d{1,5}", value.upper()) else "K2"
            if key == "students_data_range":
                value = value.upper() if re.fullmatch(r"[A-Z]{1,3}\d{1,5}:[A-Z]{1,3}\d{1,5}", value.upper()) else "A1:AC300"
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    return await _settings_map()


def _bounded_int(value: Any, min_value: int, max_value: int, default: int) -> int:
    try:
        return max(min_value, min(max_value, int(float(value))))
    except Exception:
        return default


def _module_dir() -> Path:
    if not _ctx:
        return Path(__file__).parent
    return Path(_ctx.module_dir)


def _customer_db_path() -> Path:
    env_path = _env()["customer_db_path"]
    if env_path:
        return Path(env_path)
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "customer-db" / "data" / "customer-db.db",
        module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


def _course_chat_db_path() -> Path:
    env_path = _env()["course_chat_db_path"]
    if env_path:
        return Path(env_path)
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "course-chat-creator" / "data" / "course-chat-creator.db",
        module_dir.parent.parent / "modules" / "course-chat-creator" / "data" / "course-chat-creator.db",
        module_dir.parent.parent / "module_course_chat_creator" / "data" / "course-chat-creator.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


def _student_transfer_db_path() -> Path:
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "student-transfer" / "data" / "student-transfer.db",
        module_dir.parent.parent / "modules" / "student-transfer" / "data" / "student-transfer.db",
        module_dir.parent.parent / "module_student_transfer" / "data" / "student-transfer.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm(value: Any) -> str:
    return _compact_text(value).replace("ё", "е").casefold()


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value)


def _tag_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, dict):
        result: list[str] = []
        for key, item in value.items():
            if isinstance(item, (bool, int, float)) and bool(item):
                result.append(str(key))
            else:
                result.extend(_tag_names(item))
        return result
    if isinstance(value, (list, tuple, set)):
        result = []
        for item in value:
            result.extend(_tag_names(item))
        return result
    return [item.strip() for item in re.split(r"[|,;\n]+", str(value)) if item.strip()]


def _autopayment_match(fields: dict[str, Any]) -> tuple[bool, str]:
    """Match the GetCourse→amoCRM autopayment contract exactly."""

    keyword = "автооплата"
    if any(
        keyword in _flatten_text(fields.get(key)).casefold()
        for key in ("title", "order_name", "name_order", "positions")
    ):
        return True, "title"
    tag_names: list[str] = []
    for key in ("tags", "order_tags", "tag_names", "deal_tags", "object.tags"):
        tag_names.extend(_tag_names(fields.get(key)))
    if any(name.casefold() == keyword for name in tag_names):
        return True, "tag"
    marker = _clean(fields.get("autopayment") or fields.get("is_autopayment"), 40).casefold()
    if marker in {"1", "true", "yes", "on", "да", keyword}:
        return True, "tag_condition"
    return False, ""


def _valid_email(value: Any) -> bool:
    text = _clean(value, 300)
    if not text or len(text) > 254:
        return False
    return bool(re.fullmatch(r"[^@\s<>]+@[^@\s<>]+\.[^@\s<>]{2,}", text))


def _google_auth_available() -> bool:
    try:
        return bool(
            importlib.util.find_spec("google.oauth2.service_account")
            and importlib.util.find_spec("google.auth.transport.requests")
        )
    except Exception:
        return False


def _source_hash(fields: dict[str, Any], settings: dict[str, str]) -> str:
    ignored = set(DEFAULT_FIELD_NAMES.values())
    ignored.update(settings.get(key, "") for key in DEFAULT_FIELD_NAMES)
    cleaned = {
        key: value
        for key, value in fields.items()
        if key not in ignored and not str(key).startswith(MACHINE_PREFIX)
    }
    raw = json.dumps(cleaned, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _classify_course(fields: dict[str, Any]) -> str:
    text = _norm(
        " ".join(
            str(fields.get(key) or "")
            for key in ("title", "positions", "offer_tags", "offers")
        )
    )
    has_puppy = "первые шаги к воспитанию" in text or "щенок" in text
    has_dog = "послушная собака" in text or "современный собаковод" in text
    if has_puppy:
        return "puppy"
    if has_dog:
        return "dog"
    return ""


def _classify_tariff(fields: dict[str, Any]) -> str:
    text = _norm(
        " ".join(
            str(fields.get(key) or "")
            for key in ("title", "positions", "offer_tags", "offers")
        )
    )
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*стандарт", text):
        return "standard"
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*премиум", text):
        return "premium"
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*(?:vip|вип)", text):
        return "vip"
    tags = _norm(fields.get("offer_tags"))
    tag_items = {item.strip() for item in tags.split("|") if item.strip()}
    if "стандарт" in tag_items:
        return "standard"
    if "премиум" in tag_items:
        return "premium"
    if tag_items & {"vip", "вип"}:
        return "vip"
    return ""


def _is_completed_paid(fields: dict[str, Any]) -> bool:
    status = _norm(fields.get("status"))
    payment = _norm(fields.get("payment_state"))
    return status in {"завершен", "завершён"} and payment == "paid"


def _is_onboarding_paid(fields: dict[str, Any]) -> bool:
    """Accept the first partial/full payment without requiring a later GC lifecycle status."""

    status = _norm(fields.get("status"))
    if any(marker in status for marker in ("возврат", "отмен", "refund", "cancel")):
        return False
    return _norm(fields.get("payment_state")) in {"partial", "paid"}


def _money_value(value: Any) -> float:
    text = _clean(value, 100).replace("\u00a0", "").replace(" ", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except (TypeError, ValueError):
        return 0.0


def _chat_product_entitlement(fields: dict[str, Any]) -> dict[str, Any]:
    """Classify only products that grant an educational chat."""

    text = _norm(
        " ".join(
            str(fields.get(key) or "")
            for key in ("title", "positions", "offer_tags", "offers")
        )
    )
    tariff = _classify_tariff(fields)
    has_puppy = "первые шаги к воспитанию" in text or "курс щенок" in text
    has_dog = "послушная собака" in text or "современный собаковод" in text
    if re.search(r"(?:^|[|\s])щ\s*\+\s*с(?:$|[|\s])", text):
        has_puppy = True
        has_dog = True
    combo = has_puppy and has_dog
    excluded_marker = next(
        (
            marker
            for marker in (
                "тестовый период",
                "тест-драйв",
                "тест драйв",
                "мини-курс",
                "мини курс",
                "15 минут",
                "личное наставничество",
                "доплата",
                "скидка",
            )
            if marker in text
        ),
        "",
    )
    if excluded_marker:
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": False,
            "course_key": "puppy" if combo or has_puppy else ("dog" if has_dog else ""),
            "tariff": tariff,
            "product_kind": "excluded",
            "reason": f"excluded_product:{excluded_marker}",
        }
    if "модуль" in text:
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": False,
            "course_key": "puppy" if has_puppy else ("dog" if has_dog else ""),
            "tariff": tariff,
            "product_kind": "module",
            "reason": "excluded_product:module",
        }
    if tariff == "standard":
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": False,
            "course_key": "puppy" if combo or has_puppy else ("dog" if has_dog else ""),
            "tariff": tariff,
            "product_kind": "combo" if combo else "single",
            "reason": "standard_no_chat",
        }
    if combo:
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": True,
            "course_key": "puppy",
            "tariff": tariff or "combo",
            "product_kind": "combo",
            "reason": "combo_puppy_chat",
        }
    if has_puppy and tariff in {"premium", "vip"}:
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": True,
            "course_key": "puppy",
            "tariff": tariff,
            "product_kind": "single",
            "reason": "eligible_product",
        }
    if has_dog and tariff in {"premium", "vip"}:
        return {
            "version": CHAT_ENTITLEMENT_VERSION,
            "eligible": True,
            "course_key": "dog",
            "tariff": tariff,
            "product_kind": "single",
            "reason": "eligible_product",
        }
    return {
        "version": CHAT_ENTITLEMENT_VERSION,
        "eligible": False,
        "course_key": "puppy" if has_puppy else ("dog" if has_dog else ""),
        "tariff": tariff,
        "product_kind": "unknown",
        "reason": "product_not_entitled",
    }


def _chat_payment_entitlement(fields: dict[str, Any]) -> dict[str, Any]:
    status = _norm(fields.get("status"))
    payment = _norm(fields.get("payment_state"))
    paid_money = _money_value(fields.get("payed_money"))
    if status in {"завершен", "завершён"} and payment == "paid":
        return {"eligible": True, "kind": "paid", "paid_money": paid_money, "reason": "paid"}
    if payment in {"partial", "partial_refund"} and paid_money > 0:
        return {
            "eligible": True,
            "kind": "partial_refund" if payment == "partial_refund" else "partial",
            "paid_money": paid_money,
            "reason": "positive_partial_refund_remainder" if payment == "partial_refund" else "positive_partial_payment",
        }
    return {
        "eligible": False,
        "kind": payment or "unknown",
        "paid_money": paid_money,
        "reason": "payment_not_entitled",
    }


def _chat_entitlement(fields: dict[str, Any]) -> dict[str, Any]:
    product = _chat_product_entitlement(fields)
    payment = _chat_payment_entitlement(fields)
    return {
        **product,
        "eligible": bool(product.get("eligible") and payment.get("eligible")),
        "payment": payment,
        "reason": product.get("reason") if not product.get("eligible") else payment.get("reason"),
    }


def _stream_number(*values: Any) -> str:
    for value in values:
        text = _clean(value, 500)
        if not text:
            continue
        exact = re.fullmatch(r"\D*(\d{1,4})\D*", text)
        if exact:
            return exact.group(1)
        leading = re.search(r"^\s*(\d{1,4})(?=[\s.:-])", text)
        if leading:
            return leading.group(1)
        flow = re.search(r"\b[СCЩ]\s*(\d{1,4})\b", text, flags=re.IGNORECASE)
        if flow:
            return flow.group(1)
    return ""


def _course_sheet_prefix(course_key: str) -> str:
    return "Щ" if course_key == "puppy" else "С"


def _sheet_title_matches(title: Any, course_key: str, stream: str) -> bool:
    prefix = _course_sheet_prefix(course_key).casefold()
    normalized = _norm(title).replace(" ", "")
    return bool(re.match(rf"^{re.escape(prefix)}0*{re.escape(str(stream))}(?!\d)", normalized))


def _date_value(value: Any) -> datetime | None:
    text = _clean(value, 100)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        match = re.search(r"(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})", text)
        if not match:
            return None
        year = int(match.group(3))
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(match.group(2)), int(match.group(1)), tzinfo=timezone.utc)
        except ValueError:
            return None


def _business_order_date_text(fields: dict[str, Any]) -> str:
    for key in ("first_payment_at", "paid_at", "payed_at", "payment_date", "received_at", "date_creation"):
        value = _clean(fields.get(key), 100)
        if value:
            return value
    return ""


def _business_order_date(fields: dict[str, Any]) -> datetime | None:
    return _date_value(_business_order_date_text(fields))


def _add_flow_start_dates(items: list[dict[str, Any]], now: datetime | None = None) -> None:
    today = (now or datetime.now(timezone.utc)).date()
    for course_key in ("puppy", "dog"):
        dated: list[tuple[dict[str, Any], int, int, int | None]] = []
        flows = sorted(
            (item for item in items if _clean(item.get("course_key"), 50) == course_key),
            key=lambda item: _bounded_int(item.get("stream"), 0, 100000, 0),
            reverse=True,
        )
        for item in flows:
            match = re.search(r"(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?", _clean(item.get("curator_sheet"), 300))
            if not match:
                continue
            year = int(match.group(3)) if match.group(3) else None
            if year is not None and year < 100:
                year += 2000
            dated.append((item, int(match.group(1)), int(match.group(2)), year))
        if not dated:
            continue
        first_item, first_day, first_month, first_year = dated[0]
        if first_year is None:
            candidates = [datetime(year, first_month, first_day).date() for year in (today.year - 1, today.year, today.year + 1)]
            first_year = min(candidates, key=lambda value: abs((value - today).days)).year
        first_item["date_start"] = f"{first_year:04d}-{first_month:02d}-{first_day:02d}"
        previous = (first_month, first_day)
        year = first_year
        for item, day, month, explicit_year in dated[1:]:
            if explicit_year is not None:
                year = explicit_year
            elif (month, day) > previous:
                year -= 1
            item["date_start"] = f"{year:04d}-{month:02d}-{day:02d}"
            previous = (month, day)


def _dated_flow_for_order(data: dict[str, Any], course_key: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    order_date = _business_order_date(fields)
    if not order_date:
        return None
    candidates = []
    for flow in data.get("items") or []:
        start = _date_value(flow.get("date_start"))
        if (
            _clean(flow.get("course_key"), 50) != course_key
            or flow.get("activation_pending")
            or not start
            or start.date() > order_date.date()
        ):
            continue
        activated_at = _date_value(flow.get("activated_at"))
        if activated_at and order_date < activated_at:
            continue
        if not re.match(r"^https?://", _clean(flow.get("vk_link"), 2000), flags=re.IGNORECASE):
            continue
        if not re.match(r"^https?://", _clean(flow.get("tg_link"), 2000), flags=re.IGNORECASE):
            continue
        candidates.append((start, _bounded_int(flow.get("stream"), 0, 100000, 0), flow))
    return max(candidates, key=lambda item: (item[0], item[1]))[2] if candidates else None


def _assigned_flow_for_order(data: dict[str, Any], course_key: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    """Prefer the exact chat assignment already written for this order."""

    assigned_course = _clean(fields.get("chat_fields_course_key"), 50)
    stream = _clean(fields.get(DEFAULT_FIELD_NAMES["field_stream"]), 100)
    if assigned_course != course_key or not stream:
        return None
    catalog_flow = next(
        (
            item for item in data.get("items") or []
            if _clean(item.get("course_key"), 50) == course_key
            and _clean(item.get("stream"), 100) == stream
        ),
        {},
    )
    return {
        "course_key": course_key,
        "stream": stream,
        "date_start": _clean(catalog_flow.get("date_start"), 50),
        "vk_link": _clean(
            fields.get(DEFAULT_FIELD_NAMES["field_vk"]) or catalog_flow.get("vk_link"), 2000
        ),
        "tg_link": _clean(
            fields.get(DEFAULT_FIELD_NAMES["field_tg"]) or catalog_flow.get("tg_link"), 2000
        ),
    }


def _curator_name_map(settings: dict[str, str] | None = None) -> tuple[tuple[str, str], ...]:
    raw_map = _clean((settings or {}).get("curator_map") or DEFAULT_CURATOR_MAP, 5000)
    items: list[tuple[str, str]] = []
    for part in re.split(r"[;\n]+", raw_map):
        part = part.strip()
        if not part:
            continue
        if "=>" in part:
            marker, result = part.split("=>", 1)
        elif "=" in part:
            marker, result = part.split("=", 1)
        elif ":" in part:
            marker, result = part.split(":", 1)
        else:
            continue
        marker = _norm(marker)
        result = _clean(result, 100)
        if marker and result:
            items.append((marker, result))
    return tuple(items) or (("ирина", "Куратор 1"), ("слава", "Куратор 2"), ("настас", "Куратор 3"))


def _map_curator(raw_value: Any, curator_map: tuple[tuple[str, str], ...] | dict[str, str] | None = None) -> str:
    pairs = _curator_name_map(curator_map if isinstance(curator_map, dict) else None) if curator_map is None or isinstance(curator_map, dict) else curator_map
    normalized = _norm(raw_value)
    for marker, result in pairs:
        if marker in normalized:
            return result
    return ""


def _a1_range(title: str, cell: str) -> str:
    escaped = str(title or "").replace("'", "''")
    return f"'{escaped}'!{cell}"


def _curator_spreadsheet_id(settings: dict[str, str]) -> str:
    return _clean(
        _env()["curator_spreadsheet_id"]
        or settings.get("curator_spreadsheet_id")
        or DEFAULT_CURATOR_SPREADSHEET_ID,
        200,
    )


def _curator_credentials_path(settings: dict[str, str]) -> Path | None:
    raw = _clean(settings.get("curator_credentials_path") or _env()["google_credentials_path"], 2000)
    return Path(raw) if raw else None


def _chat_links_spreadsheet_id(settings: dict[str, str]) -> str:
    return _clean(
        _env()["chat_links_spreadsheet_id"]
        or settings.get("chat_links_spreadsheet_id")
        or DEFAULT_CHAT_LINKS_SPREADSHEET_ID,
        200,
    )


def _chat_links_credentials_path(settings: dict[str, str]) -> Path | None:
    raw = _clean(
        settings.get("chat_links_credentials_path")
        or settings.get("curator_credentials_path")
        or _env()["google_credentials_path"],
        2000,
    )
    return Path(raw) if raw else None


def _getcourse_web_base_url(settings: dict[str, str]) -> str:
    raw = _clean(settings.get("getcourse_web_base_url") or "https://club.sobakovod.pro", 300)
    if not raw:
        return "https://club.sobakovod.pro"
    if not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        raw = "https://" + raw
    return raw.rstrip("/")


def _gc_account_base_url() -> str:
    account = _env()["account_name"]
    if not account:
        return ""
    if "." in account:
        return f"https://{account}"
    return f"https://{account}.getcourse.ru"


def _flow_students_cache_key(settings: dict[str, str]) -> str:
    raw = json.dumps(
        {
            "curator_spreadsheet_id": _curator_spreadsheet_id(settings),
            "curator_credentials_path": str(_curator_credentials_path(settings) or ""),
            "chat_links_spreadsheet_id": _chat_links_spreadsheet_id(settings),
            "chat_links_credentials_path": str(_chat_links_credentials_path(settings) or ""),
            "students_data_range": _students_sheet_range(settings),
            "curator_cell": settings.get("curator_cell") or "K2",
            "curator_search_range": settings.get("curator_search_range") or "J2:AC2",
            "curator_map": settings.get("curator_map") or DEFAULT_CURATOR_MAP,
            "customer_db_path": str(_customer_db_path()),
            "getcourse_web_base_url": _getcourse_web_base_url(settings),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _iso_epoch(value: Any) -> float:
    text = _clean(value, 40)
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _sheet_student_header(rows: list[list[Any]]) -> tuple[int, dict[str, int]]:
    aliases: dict[str, tuple[str, ...]] = {
        "name": ("фио", "имя"),
        "date": ("дата",),
        "course": ("курс",),
        "tariff": ("тариф",),
        "enrollment": ("оформлен",),
        "manager": ("менеджер",),
        "responsible_curator": ("ответственный куратор", "ответсвенный куратор", "отвественный куратор", "куратор"),
        "tg_account": ("tg аккаунт", "tg/vk аккаунт", "tg/вк аккаунт", "вк аккаунт", "telegram", "телеграм"),
        "email": ("почта", "email", "e-mail"),
        "buyers": ("доб. в купивших", "добавлен в купивших"),
    }
    best_idx = 6 if len(rows) > 6 else 0
    best_map: dict[str, int] = {"name": 0, "date": 1, "course": 2, "tariff": 3, "responsible_curator": 4, "tg_account": 5, "email": 6}
    for idx, row in enumerate(rows[:30]):
        normalized = [_norm(cell) for cell in row]
        has_name = any("фио" in cell for cell in normalized)
        has_email = any("почта" in cell or "email" in cell or "e-mail" in cell for cell in normalized)
        if not (has_name and has_email):
            continue
        mapping: dict[str, int] = {}
        for key, names in aliases.items():
            for col_idx, cell in enumerate(normalized):
                if any(name in cell for name in names):
                    mapping[key] = col_idx
                    break
        # A real header is authoritative.  Keeping positional fallback fields
        # here is dangerous: current staff sheets have no separate "Курс"
        # column, so the old fallback made both course and tariff point at C.
        return idx, mapping
    return best_idx, best_map


def _row_value(row: list[Any], idx: int | None, limit: int = 1000) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return _clean(row[idx], limit)


def _column_number(value: str) -> int:
    result = 0
    for char in value.upper():
        if not ("A" <= char <= "Z"):
            continue
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result


def _students_sheet_range(settings: dict[str, str]) -> str:
    configured = _clean(settings.get("students_data_range") or "A1:AC300", 50).upper()
    match = re.fullmatch(r"([A-Z]{1,3})(\d{1,5}):([A-Z]{1,3})(\d{1,5})", configured)
    if not match:
        return "A1:AC300"
    start_col, start_row, end_col, end_row = match.groups()
    if _column_number(start_col) > _column_number("A"):
        start_col = "A"
    if _column_number(end_col) < _column_number("AC"):
        end_col = "AC"
    return f"{start_col}{start_row}:{end_col}{end_row}"


def _student_items_from_rows(
    rows: list[list[Any]],
    order_index: dict[str, dict[str, Any]],
    curator_map: tuple[tuple[str, str], ...] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    header_idx, cols = _sheet_student_header(rows)
    items: list[dict[str, Any]] = []
    for offset, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2):
        email = _row_value(row, cols.get("email"), 300)
        name = _row_value(row, cols.get("name"), 300)
        if not email and not name:
            continue
        if not _valid_email(email):
            continue
        order = order_index.get(_norm(email)) or {}
        raw_responsible_curator = _row_value(row, cols.get("responsible_curator"), 200)
        mapped_responsible_curator = _map_curator(raw_responsible_curator, curator_map or _curator_name_map())
        items.append(
            {
                "row": offset,
                "name": name,
                "date": _row_value(row, cols.get("date"), 80),
                "course": _row_value(row, cols.get("course"), 100),
                "tariff": _row_value(row, cols.get("tariff"), 100),
                "responsible_curator": mapped_responsible_curator,
                "responsible_curator_raw": raw_responsible_curator,
                "tg_account": _row_value(row, cols.get("tg_account"), 500),
                "email": email,
                "gc_user_id": order.get("gc_user_id", ""),
                "user_url": order.get("user_url", ""),
                "order_id": order.get("order_id", ""),
                "deal_number": order.get("deal_number", ""),
                "order_url": order.get("order_url", ""),
                "order_status": order.get("status", ""),
                "payment_state": order.get("payment_state", ""),
                "order_title": order.get("title", ""),
                "order_updated_at": order.get("updated_at", ""),
                "source_record_id": order.get("source_record_id", ""),
            }
        )
    return items


async def _customer_order_index(settings: dict[str, str]) -> dict[str, dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return {}
    limit = _bounded_int(settings.get("students_order_lookup_limit"), 100, 200000, 20000)
    web_base = _getcourse_web_base_url(settings)
    rows: list[dict[str, Any]]
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, platform_id, custom_fields, created_at, updated_at
            FROM cdb_getcourse_orders
            ORDER BY datetime(COALESCE(updated_at, created_at)) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        fields = _json_dict(row.get("custom_fields"))
        email = _clean(fields.get("email") or fields.get("user_email"), 300)
        if not _valid_email(email):
            continue
        order_id = _clean(fields.get("order_id") or row.get("platform_id"), 100)
        gc_user_id = _clean(fields.get("gc_user_id"), 100)
        item = {
            "source_record_id": int(row.get("id") or 0),
            "platform_id": _clean(row.get("platform_id"), 100),
            "order_id": order_id,
            "deal_number": _clean(fields.get("number") or fields.get("deal_number") or fields.get("order_number") or order_id, 100),
            "gc_user_id": gc_user_id,
            "user_url": f"{web_base}/user/control/user/update/id/{urllib.parse.quote(gc_user_id)}" if gc_user_id else "",
            "order_url": f"{web_base}/sales/control/deal/update/id/{urllib.parse.quote(order_id)}" if order_id else "",
            "status": _clean(fields.get("status"), 100),
            "payment_state": _clean(fields.get("payment_state"), 100),
            "title": _clean(fields.get("title") or fields.get("positions") or fields.get("offers"), 1000),
            "created_at": _clean(row.get("created_at"), 100),
            "updated_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
            "_paid": _is_completed_paid(fields),
            "_updated_ts": _iso_epoch(row.get("updated_at") or row.get("created_at")),
        }
        grouped.setdefault(_norm(email), []).append(item)
    result: dict[str, dict[str, Any]] = {}
    for email_key, items in grouped.items():
        best = sorted(items, key=lambda item: (1 if item.get("_paid") else 0, item.get("_updated_ts") or 0), reverse=True)[0]
        best.pop("_paid", None)
        best.pop("_updated_ts", None)
        result[email_key] = best
    return result


def _dict_walk(value: Any):
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _dict_walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _dict_walk(child)


def _extract_export_id(data: Any) -> str:
    for value in _dict_walk(data):
        if isinstance(value, dict):
            for key in ("export_id", "exportId", "id"):
                found = _clean(value.get(key), 100)
                if found and re.fullmatch(r"\d+", found):
                    return found
    return ""


def _getcourse_response_error(data: dict[str, Any]) -> str:
    if data.get("success") is not False and data.get("error") in (None, False, "", 0):
        return ""
    info = data.get("info") if isinstance(data.get("info"), dict) else {}
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    return _clean(
        data.get("error_message") or info.get("error_message") or result.get("error_message")
        or (data.get("error") if isinstance(data.get("error"), str) else "")
        or "GetCourse временно не принял запрос",
        500,
    )


def _extract_export_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        info = data.get("info") if isinstance(data.get("info"), dict) else {}
        fields = info.get("fields") or data.get("fields")
        items = info.get("items") or data.get("items")
        if isinstance(fields, list) and isinstance(items, list):
            headers = [_clean(field, 300) for field in fields]
            rows: list[dict[str, Any]] = []
            for item in items:
                if isinstance(item, dict):
                    rows.append(dict(item))
                elif isinstance(item, list):
                    rows.append({headers[idx] if idx < len(headers) else str(idx): cell for idx, cell in enumerate(item)})
            if rows:
                return rows
    candidates: list[list[Any]] = []
    for value in _dict_walk(data):
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            candidates.append(value)
    if not candidates:
        return []
    candidates.sort(key=len, reverse=True)
    return [dict(item) for item in candidates[0]]


def _flat_lookup(row: dict[str, Any], names: tuple[str, ...]) -> str:
    wanted = {_norm(name) for name in names}
    for value in _dict_walk(row):
        if not isinstance(value, dict):
            continue
        for key, cell in value.items():
            normalized_key = _norm(key)
            if normalized_key in wanted or any(name in normalized_key for name in wanted):
                result = _clean(cell, 1000)
                if result:
                    return result
    return ""


def _user_id_from_export_row(row: dict[str, Any]) -> str:
    return _clean(
        _flat_lookup(row, ("id", "user_id", "gc_user_id", "Пользователь ID", "ID пользователя"))
        or row.get("id")
        or row.get("user_id"),
        100,
    )


def _email_from_export_row(row: dict[str, Any]) -> str:
    for value in _dict_walk(row):
        if isinstance(value, str) and _valid_email(value):
            return _clean(value, 300)
    email = _clean(_flat_lookup(row, ("email", "e-mail", "Почта", "Эл. адрес")), 300)
    return email if _valid_email(email) else ""


def _deal_id_from_export_row(row: dict[str, Any]) -> str:
    return _clean(
        _flat_lookup(row, ("id", "deal_id", "order_id", "Заказ ID", "ID заказа"))
        or row.get("id")
        or row.get("deal_id")
        or row.get("order_id"),
        100,
    )


def _deal_user_id_from_export_row(row: dict[str, Any]) -> str:
    return _clean(
        _flat_lookup(row, ("user_id", "gc_user_id", "Пользователь ID", "ID пользователя"))
        or row.get("user_id")
        or row.get("gc_user_id"),
        100,
    )


def _deal_updated_from_export_row(row: dict[str, Any]) -> str:
    return _clean(_flat_lookup(row, ("updated_at", "created_at", "Дата создания", "Дата обновления")) or row.get("updated_at") or row.get("created_at"), 100)


async def _gc_export_calls_used() -> int:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*) FROM gc_export_api_calls
            WHERE datetime(requested_at) >= datetime('now','-2 hours')
              AND purpose NOT IN ('getcourse-import','students-fields:user','students-fields:deal','student-transfer:curator-order')
            """
        )
        row = await cur.fetchone()
    return int((row or [0])[0] or 0)


async def _gc_export_budget_left(settings: dict[str, str]) -> int:
    limit = _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    used = await _gc_export_calls_used()
    return max(0, limit - used)


def _gc_new_job_reserve(settings: dict[str, str]) -> int:
    limit = _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    configured = _bounded_int(settings.get("gc_api_new_job_reserve_requests"), 0, 50, 10)
    return min(configured, max(0, limit - 2))


def _gc_error_classification(error: Any) -> str:
    """Classify an import failure without depending on GetCourse response shape."""

    text = _norm(error)
    if not text:
        return "transient"
    if "лимит getcourse api" in text or "api budget" in text or "quota" in text:
        return "quota"
    if re.search(r"\bhttp\s+(408|425|429|5\d\d)\b", text):
        return "transient"
    if any(
        marker in text
        for marker in (
            "timed out",
            "timeout",
            "bad gateway",
            "connection reset",
            "connection refused",
            "connection aborted",
            "network is unreachable",
            "temporarily unavailable",
        )
    ):
        return "transient"
    if re.search(r"\bhttp\s+(400|401|403|404|405|409|410|422)\b", text):
        return "terminal"
    if any(
        marker in text
        for marker in (
            "ошибка обновления заказа",
            "course not detected",
            "gc_user_id отсутствует",
            "deal_number отсутствует",
            "не настроены",
            "required non-empty fields are missing",
        )
    ):
        return "terminal"
    return "transient"


def _gc_retry_delay_seconds(settings: dict[str, str], attempt: int, classification: str) -> int:
    if classification == "quota":
        return 900
    base = _bounded_int(settings.get("gc_fields_write_retry_base_seconds"), 60, 3600, 300)
    maximum = _bounded_int(settings.get("gc_fields_write_retry_max_seconds"), 300, 86400, 21600)
    return min(maximum, base * (2 ** max(0, int(attempt or 1) - 1)))


def _gc_retry_metadata(error: str, state: dict[str, Any] | None, settings: dict[str, str]) -> dict[str, Any]:
    previous = _json_dict(_json_dict((state or {}).get("details_json", "{}")).get("retry"))
    attempts = int(previous.get("attempts") or 0) + 1
    classification = _gc_error_classification(error)
    delay = 0 if classification == "terminal" else _gc_retry_delay_seconds(settings, attempts, classification)
    next_retry_at = ""
    if delay:
        next_retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "classification": classification,
        "attempts": attempts,
        "delay_seconds": delay,
        "next_retry_at": next_retry_at,
        "last_error": _clean(error, 2000),
    }


async def _gc_export_next_budget_at(settings: dict[str, str], needed: int = 4) -> str:
    limit = _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    used = await _gc_export_calls_used()
    if used <= max(0, limit - needed):
        return ""
    to_expire = used - max(0, limit - needed)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT datetime(requested_at,'+2 hours')
            FROM gc_export_api_calls
            WHERE datetime(requested_at) >= datetime('now','-2 hours')
              AND purpose NOT IN ('getcourse-import','students-fields:user','students-fields:deal','student-transfer:curator-order')
            ORDER BY datetime(requested_at) ASC, id ASC
            LIMIT 1 OFFSET ?
            """,
            (max(0, to_expire - 1),),
        )
        row = await cur.fetchone()
    return _clean((row or [""])[0], 40)


async def _record_gc_export_call(purpose: str, details: dict[str, Any]) -> None:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            "INSERT INTO gc_export_api_calls(purpose,details_json) VALUES(?,?)",
            (_clean(purpose, 100), json.dumps(details, ensure_ascii=False)),
        )
        await db.commit()


async def _getcourse_export_get(path: str, params: dict[str, Any], settings: dict[str, str], purpose: str) -> tuple[bool, dict[str, Any], str]:
    env = _env()
    base = _gc_account_base_url()
    if not base or not env["api_token"]:
        return False, {}, "GETCOURSE_ACCOUNT_NAME/GETCOURSE_API_TOKEN не настроены"
    if await _gc_export_budget_left(settings) <= 0:
        return False, {}, "лимит GetCourse Export API для модуля исчерпан"
    query = {"key": env["api_token"], **{key: value for key, value in params.items() if value not in (None, "")}}
    await _record_gc_export_call(purpose, {"path": path, "params": {key: ("***" if key == "key" else value) for key, value in query.items()}})
    timeout = _bounded_int(settings.get("request_timeout"), 5, 60, 20)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(base.rstrip("/") + path, params=query)
        try:
            body = resp.json()
        except Exception:
            body = {"text": resp.text[:2000]}
        if resp.status_code >= 400:
            return False, body if isinstance(body, dict) else {"response": body}, f"HTTP {resp.status_code}"
        parsed = body if isinstance(body, dict) else {"response": body}
        api_error = _getcourse_response_error(parsed)
        if api_error:
            return False, parsed, api_error
        return True, parsed, ""
    except Exception as exc:
        return False, {}, str(exc)


async def _getcourse_export_rows(path: str, params: dict[str, Any], settings: dict[str, str], purpose: str) -> tuple[list[dict[str, Any]], str]:
    pending_key = json.dumps([path, params], ensure_ascii=False, sort_keys=True, default=str)
    pending = _gc_pending_exports.get(pending_key)
    export_id = pending[0] if pending and pending[1] > time.monotonic() else ""
    if not export_id:
        _gc_pending_exports.pop(pending_key, None)
        ok, data, error = await _getcourse_export_get(path, params, settings, f"{purpose}:start")
        if not ok:
            return [], error
        export_id = _extract_export_id(data)
        direct_rows = _extract_export_rows(data)
        if direct_rows:
            return direct_rows, ""
        if not export_id:
            return [], "GetCourse ещё формирует выгрузку"
        _gc_pending_exports[pending_key] = (export_id, time.monotonic() + 2 * 3600)
    attempts = _bounded_int(settings.get("gc_export_lookup_poll_attempts"), 1, 5, 2)
    delay = _bounded_int(settings.get("gc_export_lookup_poll_delay_seconds"), 0, 20, 2)
    last_error = ""
    for attempt in range(attempts):
        if delay and attempt:
            await asyncio.sleep(delay)
        ok, export_data, error = await _getcourse_export_get(f"/pl/api/account/exports/{urllib.parse.quote(export_id)}", {}, settings, f"{purpose}:poll")
        if not ok:
            last_error = error
            if "404" in error or "не найден" in _norm(error):
                _gc_pending_exports.pop(pending_key, None)
            continue
        rows = _extract_export_rows(export_data)
        if rows:
            _gc_pending_exports.pop(pending_key, None)
            return rows, ""
        last_error = _clean(export_data.get("status") or export_data.get("state") or "export is not ready", 300)
    return [], last_error or "export is not ready"


async def _load_gc_lookup_cache(emails: list[str], settings: dict[str, str]) -> dict[str, dict[str, Any]]:
    if not emails:
        return {}
    cache_days = _bounded_int(settings.get("gc_export_lookup_cache_days"), 1, 365, 30)
    placeholders = ",".join("?" for _ in emails)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT *
            FROM gc_export_lookup_cache
            WHERE email IN ({placeholders})
              AND COALESCE(gc_user_id,'') <> ''
              AND datetime(updated_at) >= datetime('now', ?)
            """,
            (*emails, f"-{cache_days} days"),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        result[_norm(row.get("email"))] = {
            "gc_user_id": _clean(row.get("gc_user_id"), 100),
            "user_url": _clean(row.get("user_url"), 1000),
            "order_id": _clean(row.get("order_id"), 100),
            "order_url": _clean(row.get("order_url"), 1000),
            "source_record_id": "",
            "status": "gc_export_cache",
            "payment_state": "",
            "title": "",
            "updated_at": _clean(row.get("updated_at"), 100),
        }
    return result


async def _save_gc_lookup_cache(email_key: str, item: dict[str, Any], status: str = "ok", error: str = "") -> None:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO gc_export_lookup_cache(email,gc_user_id,user_url,order_id,order_url,status,error,source_json,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(email) DO UPDATE SET
                gc_user_id=excluded.gc_user_id,
                user_url=excluded.user_url,
                order_id=excluded.order_id,
                order_url=excluded.order_url,
                status=excluded.status,
                error=excluded.error,
                source_json=excluded.source_json,
                updated_at=excluded.updated_at
            """,
            (
                email_key,
                _clean(item.get("gc_user_id"), 100),
                _clean(item.get("user_url"), 1000),
                _clean(item.get("order_id"), 100),
                _clean(item.get("order_url"), 1000),
                _clean(status, 100),
                _clean(error, 1000),
                json.dumps(item.get("source") or {}, ensure_ascii=False),
                _now(),
            ),
        )
        await db.commit()


async def _patch_flow_students_cache_email(email_key: str, item: dict[str, Any]) -> int:
    if not item.get("gc_user_id"):
        return 0
    normalized = _norm(email_key)
    if not _valid_email(normalized):
        return 0
    assert _db_path is not None
    patched_rows = 0
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT key,value_json FROM flow_students_cache")
        rows = await cur.fetchall()
        for cache_key, value_json in rows:
            data = _json_dict(value_json)
            changed = False
            for flow in data.get("items") or []:
                for student in flow.get("students") or []:
                    if _norm(student.get("email")) != normalized:
                        continue
                    for key in ("gc_user_id", "user_url", "order_id", "order_url"):
                        if not student.get(key) and item.get(key):
                            student[key] = item[key]
                            changed = True
                    if changed:
                        student["lookup_source"] = "getcourse_export"
            if changed:
                data["matched_orders"] = sum(
                    1
                    for flow in data.get("items") or []
                    for student in flow.get("students") or []
                    if student.get("order_url") or student.get("user_url")
                )
                data["lookup_cache_patched_at"] = _now()
                await db.execute(
                    "UPDATE flow_students_cache SET value_json=? WHERE key=?",
                    (json.dumps(data, ensure_ascii=False), cache_key),
                )
                patched_rows += 1
        await db.commit()
    return patched_rows


async def _getcourse_export_lookup_missing(emails: list[str], settings: dict[str, str]) -> dict[str, dict[str, Any]]:
    if not _truthy(settings.get("gc_export_lookup_enabled")):
        return {}
    normalized = []
    seen: set[str] = set()
    for email in emails:
        email_key = _norm(email)
        if _valid_email(email_key) and email_key not in seen:
            normalized.append(email_key)
            seen.add(email_key)
    if not normalized:
        return {}
    cached = await _load_gc_lookup_cache(normalized, settings)
    missing = [email for email in normalized if email not in cached]
    max_missing = _bounded_int(settings.get("gc_export_lookup_max_missing_per_refresh"), 0, 1000, 50)
    if max_missing <= 0:
        return cached
    missing = missing[:max_missing]
    batch_size = _bounded_int(settings.get("gc_export_lookup_batch_size"), 1, 100, 50)
    web_base = _getcourse_web_base_url(settings)
    result = dict(cached)
    for offset in range(0, len(missing), batch_size):
        if await _gc_export_budget_left(settings) < 1:
            break
        batch = missing[offset : offset + batch_size]
        user_rows, user_error = await _getcourse_export_rows("/pl/api/account/users", {"email": ",".join(batch)}, settings, "students-users")
        if user_error:
            raise RuntimeError(f"GetCourse Export users lookup failed: {user_error}")
        users_by_email: dict[str, dict[str, Any]] = {}
        users_by_id: dict[str, str] = {}
        for row in user_rows:
            email = _norm(_email_from_export_row(row))
            user_id = _user_id_from_export_row(row)
            if not email or not user_id:
                continue
            item = {
                "gc_user_id": user_id,
                "user_url": f"{web_base}/user/control/user/update/id/{urllib.parse.quote(user_id)}",
                "order_id": "",
                "order_url": "",
                "status": "gc_export_user",
                "payment_state": "",
                "title": "",
                "updated_at": _now(),
                "source": {"user": row},
            }
            users_by_email[email] = item
            users_by_id[user_id] = email
        for email in batch:
            item = users_by_email.get(email)
            if item:
                result[email] = item
                await _save_gc_lookup_cache(email, item, "ok")
            else:
                await _save_gc_lookup_cache(email, {}, "not_found", user_error)
    return result


def _missing_student_emails(snapshot: dict[str, Any]) -> list[str]:
    emails: list[str] = []
    seen: set[str] = set()
    for flow in snapshot.get("items") or []:
        for student in flow.get("students") or []:
            if student.get("gc_user_id") and student.get("order_id"):
                continue
            email = _norm(student.get("email"))
            if _valid_email(email) and email not in seen:
                emails.append(email)
                seen.add(email)
    return emails


def _apply_gc_export_lookup(snapshot: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> int:
    applied = 0
    for flow in snapshot.get("items") or []:
        for student in flow.get("students") or []:
            item = lookup.get(_norm(student.get("email"))) or {}
            if not item:
                continue
            changed = False
            for key in ("gc_user_id", "user_url", "order_id", "order_url"):
                if not student.get(key) and item.get(key):
                    student[key] = item[key]
                    changed = True
            if changed:
                student["lookup_source"] = "getcourse_export"
                applied += 1
    return applied


async def _enqueue_gc_lookup_emails(emails: list[str], reason: str = "manual") -> dict[str, Any]:
    normalized: list[str] = []
    seen: set[str] = set()
    for email in emails:
        email_key = _norm(email)
        if _valid_email(email_key) and email_key not in seen:
            normalized.append(email_key)
            seen.add(email_key)
    if not normalized:
        return {"queued": 0, "emails": []}
    assert _db_path is not None
    queued = 0
    async with _db_connect(_db_path) as db:
        for email in normalized:
            cur = await db.execute(
                """
                INSERT INTO gc_export_lookup_jobs(email,status,last_error,result_json,updated_at)
                VALUES(?,'pending','',?,?)
                ON CONFLICT(email) DO UPDATE SET
                    status=CASE
                        WHEN gc_export_lookup_jobs.status IN ('completed','running') THEN gc_export_lookup_jobs.status
                        ELSE 'pending'
                    END,
                    next_run_at=CASE
                        WHEN gc_export_lookup_jobs.status IN ('completed','running') THEN gc_export_lookup_jobs.next_run_at
                        ELSE strftime('%Y-%m-%dT%H:%M:%SZ','now')
                    END,
                    last_error=CASE
                        WHEN gc_export_lookup_jobs.status IN ('completed','running') THEN gc_export_lookup_jobs.last_error
                        ELSE ''
                    END,
                    updated_at=excluded.updated_at
                """,
                (email, json.dumps({"reason": reason}, ensure_ascii=False), _now()),
            )
            queued += max(0, int(cur.rowcount or 0))
        await db.commit()
    return {"queued": len(normalized), "emails": normalized}


async def _existing_gc_lookup_emails() -> set[str]:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT email FROM gc_export_lookup_jobs")
        return {_norm(row[0]) for row in await cur.fetchall() if row and row[0]}


async def _enqueue_missing_from_students_cache(settings: dict[str, str], limit: int = 50, *, skip_existing: bool = False) -> dict[str, Any]:
    ttl = _bounded_int(settings.get("students_cache_minutes"), 1, 1440, 30)
    cache = await _load_flow_students_cache(_flow_students_cache_key(settings), ttl, allow_stale=True)
    if not cache:
        return {"queued": 0, "emails": [], "error": "students cache is empty"}
    emails = _missing_student_emails(cache)
    if skip_existing:
        existing = await _existing_gc_lookup_emails()
        emails = [email for email in emails if email not in existing]
    max_limit = max(1, min(1000, int(limit or 50)))
    return await _enqueue_gc_lookup_emails(emails[:max_limit], reason="missing_from_students_cache")


async def _open_gc_lookup_jobs_count(settings: dict[str, str]) -> int:
    max_attempts = _bounded_int(settings.get("gc_export_lookup_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM gc_export_lookup_jobs
            WHERE status IN ('pending','running','failed')
              AND attempts < ?
            """,
            (max_attempts,),
        )
        row = await cur.fetchone()
    return int((row or [0])[0] or 0)


async def _open_gc_write_jobs_count(settings: dict[str, str] | None = None) -> int:
    active_settings = settings or await _settings_map()
    max_attempts = _bounded_int(active_settings.get("gc_fields_write_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM gc_fields_write_jobs
            WHERE status IN ('pending','running','failed')
              AND attempts < ?
            """,
            (max_attempts,),
        )
        row = await cur.fetchone()
    return int((row or [0])[0] or 0)


async def _auto_enqueue_gc_lookup_jobs(settings: dict[str, str]) -> dict[str, Any]:
    if not _truthy(settings.get("gc_export_lookup_auto_enqueue_enabled", "1")):
        return {"queued": 0, "emails": [], "disabled": True}
    if await _open_gc_write_jobs_count(settings) > 0:
        return {"queued": 0, "emails": [], "reason": "write_queue_not_empty"}
    if await _open_gc_lookup_jobs_count(settings) > 0:
        return {"queued": 0, "emails": [], "reason": "queue_not_empty"}
    budget_left = await _gc_export_budget_left(settings)
    reserve = _gc_new_job_reserve(settings)
    usable_budget = max(0, budget_left - reserve)
    if usable_budget < 4:
        return {
            "queued": 0,
            "emails": [],
            "reason": "budget_reserved_for_new_jobs",
            "requests_left_2h": budget_left,
            "reserved_requests": reserve,
        }
    configured_batch = _bounded_int(settings.get("gc_export_lookup_auto_enqueue_batch_size"), 1, 100, 20)
    batch = max(1, min(configured_batch, usable_budget // 4))
    result = await _enqueue_missing_from_students_cache(settings, limit=batch, skip_existing=True)
    result["requests_left_2h"] = budget_left
    result["reserved_requests"] = reserve
    result["auto"] = True
    return result


def _deal_number_from_source(source: dict[str, Any]) -> str:
    deal = source.get("deal") if isinstance(source.get("deal"), dict) else {}
    return _clean(
        deal.get("Номер")
        or deal.get("number")
        or deal.get("deal_number")
        or deal.get("Номер заказа"),
        100,
    )


async def _deal_number_from_customer_source(source_record_id: Any) -> str:
    try:
        record_id = int(source_record_id or 0)
    except Exception:
        return ""
    if record_id <= 0:
        return ""
    db_path = _customer_db_path()
    if not db_path.exists():
        return ""
    async with _db_connect(db_path) as db:
        cur = await db.execute("SELECT custom_fields FROM cdb_getcourse_orders WHERE id=?", (record_id,))
        row = await cur.fetchone()
    fields = _json_dict((row or [""])[0])
    return _clean(fields.get("number") or fields.get("deal_number") or fields.get("order_number"), 100)


async def _lookup_cache_by_email(emails: list[str]) -> dict[str, dict[str, Any]]:
    if not emails:
        return {}
    placeholders = ",".join("?" for _ in emails)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM gc_export_lookup_cache WHERE email IN ({placeholders})",
            tuple(emails),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    return {_norm(row.get("email")): row for row in rows}


async def _existing_gc_write_job_keys() -> set[tuple[str, str]]:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT email, order_id
            FROM gc_fields_write_jobs
            """
        )
        rows = await cur.fetchall()
    return {(_norm(row[0]), _clean(row[1], 100)) for row in rows if _valid_email(_norm(row[0])) and _clean(row[1], 100)}


async def _fields_write_candidates_from_cache(settings: dict[str, str], limit: int = 500) -> list[dict[str, Any]]:
    ttl = _bounded_int(settings.get("students_cache_minutes"), 1, 1440, 30)
    cache = await _load_flow_students_cache(_flow_students_cache_key(settings), ttl, allow_stale=True)
    if not cache:
        return []
    curator_values = {value for _, value in _curator_name_map(settings)}
    emails = sorted(
        {
            _norm(student.get("email"))
            for flow in cache.get("items") or []
            for student in flow.get("students") or []
            if _valid_email(_norm(student.get("email")))
        }
    )
    lookup_cache = await _lookup_cache_by_email(emails)
    existing_jobs = await _existing_gc_write_job_keys()
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for flow in cache.get("items") or []:
        stream = _clean(flow.get("stream"), 100)
        vk_link = _clean(flow.get("vk_link"), 2000)
        tg_link = _clean(flow.get("tg_link"), 2000)
        flow_curator = _clean(flow.get("curator_value"), 100)
        if not stream or not vk_link or not tg_link:
            continue
        for student in flow.get("students") or []:
            email = _norm(student.get("email"))
            if not _valid_email(email):
                continue
            gc_user_id = _clean(student.get("gc_user_id"), 100)
            order_id = _clean(student.get("order_id"), 100)
            if not gc_user_id:
                cached = lookup_cache.get(email) or {}
                gc_user_id = _clean(cached.get("gc_user_id"), 100)
            if not order_id:
                cached = lookup_cache.get(email) or {}
                order_id = _clean(cached.get("order_id"), 100)
            curator = _clean(student.get("responsible_curator") or flow_curator, 100)
            if curator not in curator_values:
                continue
            if not gc_user_id or not order_id:
                continue
            cached = lookup_cache.get(email) or {}
            source = _json_dict(cached.get("source_json"))
            deal_number = _deal_number_from_source(source)
            if not deal_number:
                deal_number = await _deal_number_from_customer_source(student.get("source_record_id"))
            if not deal_number:
                continue
            key = (email, order_id)
            if key in seen or key in existing_jobs:
                continue
            seen.add(key)
            output_fields = {
                settings["field_stream"]: stream,
                settings["field_vk"]: vk_link,
                settings["field_tg"]: tg_link,
                settings["field_curator"]: curator,
            }
            output_fields = {key: value for key, value in output_fields.items() if _clean(value)}
            if len(output_fields) != 4:
                continue
            result.append(
                {
                    "email": email,
                    "gc_user_id": gc_user_id,
                    "order_id": order_id,
                    "deal_number": deal_number,
                    "fields": output_fields,
                    "user_fields": _getcourse_user_addfields(output_fields, settings),
                    "flow": {
                        "course": flow.get("course"),
                        "course_key": flow.get("course_key"),
                        "stream": stream,
                        "sheet_title": flow.get("sheet_title"),
                    },
                }
            )
            if len(result) >= max(1, int(limit or 500)):
                return result
    return result


def _flow_students_effective_curators(snapshot: dict[str, Any] | None) -> tuple[dict[tuple[str, str], str], dict[tuple[str, str, str], str]]:
    flow_curators: dict[tuple[str, str], str] = {}
    student_curators: dict[tuple[str, str, str], str] = {}
    if not snapshot:
        return flow_curators, student_curators
    for flow in snapshot.get("items") or []:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 50)
        if not course_key or not stream:
            continue
        flow_key = (course_key, stream)
        flow_curator = _clean(flow.get("curator_value"), 100)
        if flow_curator:
            flow_curators[flow_key] = flow_curator
        for student in flow.get("students") or []:
            email = _norm(student.get("email"))
            if not _valid_email(email):
                continue
            effective = _clean(student.get("responsible_curator") or flow_curator, 100)
            if effective:
                student_curators[(course_key, stream, email)] = effective
    return flow_curators, student_curators


async def _curator_change_write_candidates(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    settings: dict[str, str],
    limit: int = 500,
) -> list[dict[str, Any]]:
    old_flow_curators, old_student_curators = _flow_students_effective_curators(previous)
    if not old_flow_curators and not old_student_curators:
        return []
    curator_values = {value for _, value in _curator_name_map(settings)}
    emails = sorted(
        {
            _norm(student.get("email"))
            for flow in current.get("items") or []
            for student in flow.get("students") or []
            if _valid_email(_norm(student.get("email")))
        }
    )
    lookup_cache = await _lookup_cache_by_email(emails)
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for flow in current.get("items") or []:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 100)
        vk_link = _clean(flow.get("vk_link"), 2000)
        tg_link = _clean(flow.get("tg_link"), 2000)
        flow_curator = _clean(flow.get("curator_value"), 100)
        flow_key = (course_key, stream)
        if not course_key or not stream or not vk_link or not tg_link:
            continue
        for student in flow.get("students") or []:
            email = _norm(student.get("email"))
            if not _valid_email(email):
                continue
            curator = _clean(student.get("responsible_curator") or flow_curator, 100)
            previous_curator = _clean(old_student_curators.get((course_key, stream, email)) or old_flow_curators.get(flow_key), 100)
            if not previous_curator or previous_curator == curator or curator not in curator_values:
                continue
            gc_user_id = _clean(student.get("gc_user_id"), 100)
            order_id = _clean(student.get("order_id"), 100)
            cached = lookup_cache.get(email) or {}
            if not gc_user_id:
                gc_user_id = _clean(cached.get("gc_user_id"), 100)
            if not order_id:
                order_id = _clean(cached.get("order_id"), 100)
            if not gc_user_id or not order_id:
                continue
            source = _json_dict(cached.get("source_json"))
            deal_number = _deal_number_from_source(source)
            if not deal_number:
                deal_number = await _deal_number_from_customer_source(student.get("source_record_id"))
            if not deal_number:
                continue
            key = (email, order_id)
            if key in seen:
                continue
            seen.add(key)
            output_fields = {
                settings["field_stream"]: stream,
                settings["field_vk"]: vk_link,
                settings["field_tg"]: tg_link,
                settings["field_curator"]: curator,
            }
            output_fields = {key: value for key, value in output_fields.items() if _clean(value)}
            if len(output_fields) != 4:
                continue
            result.append(
                {
                    "email": email,
                    "gc_user_id": gc_user_id,
                    "order_id": order_id,
                    "deal_number": deal_number,
                    "fields": output_fields,
                    "user_fields": _getcourse_user_addfields(output_fields, settings),
                    "flow": {
                        "course": flow.get("course"),
                        "course_key": course_key,
                        "stream": stream,
                        "sheet_title": flow.get("sheet_title"),
                        "change_reason": "curator_changed_from_sheets",
                        "previous_curator": previous_curator,
                        "new_curator": curator,
                    },
                }
            )
            if len(result) >= max(1, int(limit or 500)):
                return result
    return result


async def _fields_write_reconciliation_candidates(settings: dict[str, str], limit: int = 500) -> list[dict[str, Any]]:
    if await _load_flow_students_cache(REGISTRY_CURATOR_SYNC_CACHE_KEY, 15):
        return []
    ttl = _bounded_int(settings.get("students_cache_minutes"), 1, 1440, 30)
    cache = await _load_flow_students_cache(_flow_students_cache_key(settings), ttl, allow_stale=True)
    if not cache:
        return []
    expected: dict[tuple[str, str], dict[str, Any]] = {}
    for flow in cache.get("items") or []:
        stream = _clean(flow.get("stream"), 100)
        vk_link = _clean(flow.get("vk_link"), 2000)
        tg_link = _clean(flow.get("tg_link"), 2000)
        flow_curator = _clean(flow.get("curator_value"), 100)
        if not stream or not vk_link or not tg_link or not flow_curator:
            continue
        for student in flow.get("students") or []:
            email = _norm(student.get("email"))
            order_id = _clean(student.get("order_id"), 100)
            if not _valid_email(email) or not order_id:
                continue
            curator = _clean(student.get("responsible_curator") or flow_curator, 100)
            if not curator:
                continue
            output_fields = {
                settings["field_stream"]: stream,
                settings["field_vk"]: vk_link,
                settings["field_tg"]: tg_link,
                settings["field_curator"]: curator,
            }
            output_fields = {key: value for key, value in output_fields.items() if _clean(value)}
            if len(output_fields) != 4:
                continue
            # The same paid order can remain in older flow sheets after a student
            # is moved. Cache items are ordered newest first, so keep the first
            # occurrence instead of letting an older duplicate win.
            expected.setdefault((email, order_id), {
                "fields": output_fields,
                "flow": {
                    "course": flow.get("course"),
                    "course_key": flow.get("course_key"),
                    "stream": stream,
                    "sheet_title": flow.get("sheet_title"),
                    "change_reason": "field_write_reconciliation",
                },
            })
    if not expected:
        return []
    assert _db_path is not None
    result: list[dict[str, Any]] = []
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT email,gc_user_id,order_id,deal_number,status,payload_json,result_json
            FROM gc_fields_write_jobs
            WHERE status <> 'running'
            """
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        email = _norm(row.get("email"))
        order_id = _clean(row.get("order_id"), 100)
        wanted = expected.get((email, order_id))
        if not wanted:
            continue
        payload = _json_dict(row.get("payload_json"))
        result_payload = _json_dict(row.get("result_json"))
        current_fields = _json_dict(payload.get("fields")) or _json_dict(result_payload.get("fields"))
        wanted_fields = wanted["fields"]
        if all(_clean(current_fields.get(key)) == _clean(value) for key, value in wanted_fields.items()):
            continue
        gc_user_id = _clean(row.get("gc_user_id"), 100)
        deal_number = _clean(row.get("deal_number"), 100)
        if not gc_user_id or not deal_number:
            continue
        result.append(
            {
                "email": email,
                "gc_user_id": gc_user_id,
                "order_id": order_id,
                "deal_number": deal_number,
                "fields": wanted_fields,
                "user_fields": _getcourse_user_addfields(wanted_fields, settings),
                "flow": {
                    **wanted["flow"],
                    "previous_fields": current_fields,
                },
            }
        )
        if len(result) >= max(1, int(limit or 500)):
            return result
    return result


async def _gc_lookup_status(settings: dict[str, str] | None = None) -> dict[str, Any]:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT status,COUNT(*) FROM gc_export_lookup_jobs GROUP BY status")
        counts = {str(row[0]): int(row[1] or 0) for row in await cur.fetchall()}
        cur = await db.execute(
            """
            SELECT id,email,status,attempts,next_run_at,last_error,updated_at
            FROM gc_export_lookup_jobs
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT 20
            """
        )
        recent = [
            {
                "id": int(row[0]),
                "email": row[1],
                "status": row[2],
                "attempts": int(row[3] or 0),
                "next_run_at": row[4],
                "last_error": row[5],
                "updated_at": row[6],
            }
            for row in await cur.fetchall()
        ]
        cur = await db.execute("SELECT COUNT(*) FROM gc_export_lookup_cache WHERE COALESCE(gc_user_id,'')<>''")
        cached_users = int((await cur.fetchone())[0] or 0)
    active_settings = settings or await _settings_map()
    requests_used = await _gc_export_calls_used()
    requests_left = await _gc_export_budget_left(active_settings)
    limit_2h = _bounded_int(active_settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    open_jobs = sum(counts.get(status, 0) for status in ("pending", "running", "failed"))
    budget_needed = 4
    paused_reason = ""
    next_budget_at = ""
    if _truthy(active_settings.get("gc_export_lookup_enabled")) and open_jobs == 0 and requests_left < budget_needed:
        paused_reason = "budget_low"
        next_budget_at = await _gc_export_next_budget_at(active_settings, needed=budget_needed)
    return {
        "enabled": _truthy(active_settings.get("gc_export_lookup_enabled")),
        "counts": counts,
        "recent": recent,
        "cached_users": cached_users,
        "requests_used_2h": requests_used,
        "requests_left_2h": requests_left,
        "limit_2h": limit_2h,
        "budget_needed": budget_needed,
        "paused_reason": paused_reason,
        "next_budget_at": next_budget_at,
    }


async def _claim_gc_lookup_job(settings: dict[str, str]) -> dict[str, Any] | None:
    max_attempts = _bounded_int(settings.get("gc_export_lookup_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM gc_export_lookup_jobs
            WHERE status IN ('pending','failed')
              AND attempts < ?
              AND result_json LIKE '%"export_id"%'
            """,
            (max_attempts,),
        )
        has_open_export = int((await cur.fetchone())[0] or 0) > 0
        export_clause = "AND result_json LIKE '%\"export_id\"%'" if has_open_export else "AND result_json NOT LIKE '%\"export_id\"%'"
        cur = await db.execute(
            f"""
            SELECT *
            FROM gc_export_lookup_jobs
            WHERE status IN ('pending','failed')
              AND attempts < ?
              AND datetime(next_run_at) <= datetime('now')
              {export_clause}
            ORDER BY CASE WHEN result_json LIKE '%"export_id"%' THEN 0 ELSE 1 END,
                     datetime(next_run_at) ASC,
                     id ASC
            LIMIT 1
            """,
            (max_attempts,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        await db.execute(
            """
            UPDATE gc_export_lookup_jobs
            SET status='running', attempts=attempts+1, updated_at=?
            WHERE id=?
            """,
            (_now(), int(row["id"])),
        )
        await db.commit()
        return dict(row)


async def _finish_gc_lookup_job(job_id: int, status: str, error: str = "", result: dict[str, Any] | None = None) -> None:
    delay_seconds = 0
    if status == "failed":
        attempts = 1
        assert _db_path is not None
        async with _db_connect(_db_path) as db_read:
            cur = await db_read.execute("SELECT attempts FROM gc_export_lookup_jobs WHERE id=?", (int(job_id),))
            row = await cur.fetchone()
            attempts = int((row or [1])[0] or 1)
        delay_seconds = min(3600, 60 * attempts * attempts)
    next_run_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"
    if delay_seconds:
        next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{int(delay_seconds)} seconds')"
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            f"""
            UPDATE gc_export_lookup_jobs
            SET status=?, last_error=?, result_json=?, next_run_at={next_run_expr}, updated_at=?
            WHERE id=?
            """,
            (
                _clean(status, 50),
                _clean(error, 2000),
                json.dumps(result or {}, ensure_ascii=False),
                _now(),
                int(job_id),
            ),
        )
        await db.commit()


async def _defer_gc_lookup_job(job_id: int, error: str = "", delay_seconds: int = 600, result: dict[str, Any] | None = None) -> None:
    delay_seconds = max(60, min(7200, int(delay_seconds or 600)))
    next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{delay_seconds} seconds')"
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            f"""
            UPDATE gc_export_lookup_jobs
            SET status='pending',
                attempts=CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                last_error=?,
                result_json=?,
                next_run_at={next_run_expr},
                updated_at=?
            WHERE id=?
            """,
            (_clean(error, 2000), json.dumps(result or {}, ensure_ascii=False), _now(), int(job_id)),
        )
        await db.commit()


def _user_item_from_export_rows(email: str, rows: list[dict[str, Any]], settings: dict[str, str]) -> dict[str, Any]:
    users_by_email: dict[str, dict[str, Any]] = {}
    web_base = _getcourse_web_base_url(settings)
    for row in rows:
        row_email = _norm(_email_from_export_row(row))
        user_id = _user_id_from_export_row(row)
        if not _valid_email(row_email) or not user_id:
            continue
        users_by_email[row_email] = {
            "gc_user_id": user_id,
            "user_url": f"{web_base}/user/control/user/update/id/{urllib.parse.quote(user_id)}",
            "order_id": "",
            "order_url": "",
            "status": "gc_export_user",
            "payment_state": "",
            "title": "",
            "updated_at": _now(),
            "source": {"user": row},
        }
    return users_by_email.get(email) or {}


def _deal_item_from_export_rows(rows: list[dict[str, Any]], settings: dict[str, str]) -> dict[str, Any]:
    web_base = _getcourse_web_base_url(settings)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        deal_id = _deal_id_from_export_row(row)
        if not deal_id:
            continue
        candidates.append(
            {
                "order_id": deal_id,
                "order_url": f"{web_base}/sales/control/deal/update/id/{urllib.parse.quote(deal_id)}",
                "order_updated_at": _deal_updated_from_export_row(row),
                "_updated_ts": _iso_epoch(_deal_updated_from_export_row(row)),
                "source": {"deal": row},
            }
        )
    if not candidates:
        return {}
    candidates.sort(key=lambda item: item.get("_updated_ts") or 0, reverse=True)
    item = dict(candidates[0])
    item.pop("_updated_ts", None)
    return item


async def _finish_gc_lookup_item(job_id: int, email: str, item: dict[str, Any]) -> None:
    if item.get("gc_user_id"):
        await _save_gc_lookup_cache(email, item, "ok")
        await _patch_flow_students_cache_email(email, item)
        await _finish_gc_lookup_job(int(job_id), "completed", "", item)
        return
    await _save_gc_lookup_cache(email, {}, "not_found", "")
    await _finish_gc_lookup_job(int(job_id), "not_found", "user not found by GetCourse Export API", {})


async def _process_gc_lookup_job(job: dict[str, Any], settings: dict[str, str]) -> None:
    email = _norm(job.get("email"))
    if not email:
        await _finish_gc_lookup_job(int(job["id"]), "failed", "empty email")
        return
    if await _gc_export_budget_left(settings) < 1:
        await _defer_gc_lookup_job(int(job["id"]), "GetCourse Export API budget exhausted; deferred")
        return
    state = _json_dict(job.get("result_json"))
    export_id = _clean(state.get("export_id"), 100)
    phase = _clean(state.get("phase") or "users", 50)
    timeout = _bounded_int(settings.get("gc_export_lookup_job_timeout_seconds"), 3, 60, 12)
    try:
        async with asyncio.timeout(timeout):
            if export_id:
                ok, data, error = await _getcourse_export_get(
                    f"/pl/api/account/exports/{urllib.parse.quote(export_id)}",
                    {},
                    settings,
                    "students-deals:poll" if phase == "deals" else "students-users:poll",
                )
                if not ok:
                    raise RuntimeError(error or "GetCourse export poll failed")
                rows = _extract_export_rows(data)
                if rows:
                    if phase == "deals":
                        item = _json_dict(state.get("item"))
                        deal_item = _deal_item_from_export_rows(rows, settings)
                        if deal_item:
                            item.update({key: value for key, value in deal_item.items() if key != "source"})
                            source = _json_dict(item.get("source"))
                            source.update(deal_item.get("source") or {})
                            item["source"] = source
                        await _finish_gc_lookup_item(int(job["id"]), email, item)
                        return
                    item = _user_item_from_export_rows(email, rows, settings)
                    if item.get("gc_user_id") and _truthy(settings.get("gc_export_lookup_deals_enabled")) and await _gc_export_budget_left(settings) >= 1:
                        ok, deal_data, deal_error = await _getcourse_export_get(
                            "/pl/api/account/deals",
                            {"user_id": item["gc_user_id"]},
                            settings,
                            "students-deals:start",
                        )
                        if ok:
                            deal_rows = _extract_export_rows(deal_data)
                            if deal_rows:
                                deal_item = _deal_item_from_export_rows(deal_rows, settings)
                                if deal_item:
                                    item.update({key: value for key, value in deal_item.items() if key != "source"})
                                    source = _json_dict(item.get("source"))
                                    source.update(deal_item.get("source") or {})
                                    item["source"] = source
                                await _finish_gc_lookup_item(int(job["id"]), email, item)
                                return
                            deal_export_id = _extract_export_id(deal_data)
                            if deal_export_id:
                                await _defer_gc_lookup_job(
                                    int(job["id"]),
                                    f"waiting GetCourse deals export {deal_export_id}",
                                    delay_seconds=60,
                                    result={"phase": "deals", "export_id": deal_export_id, "email": email, "item": item, "started_at": _now()},
                                )
                                return
                        else:
                            item["deal_lookup_error"] = deal_error
                    await _finish_gc_lookup_item(int(job["id"]), email, item)
                    return
                wait_state = _clean(data.get("status") or data.get("state") or data.get("message") or "export is not ready", 300)
                if any(marker in _norm(wait_state) for marker in ("complete", "finish", "done", "success", "ready", "заверш", "готов")):
                    if phase == "deals":
                        await _finish_gc_lookup_item(int(job["id"]), email, _json_dict(state.get("item")))
                    else:
                        await _save_gc_lookup_cache(email, {}, "not_found", "")
                        await _finish_gc_lookup_job(int(job["id"]), "not_found", "user not found by GetCourse Export API", {})
                    return
                await _defer_gc_lookup_job(
                    int(job["id"]),
                    f"waiting GetCourse export {export_id}: {wait_state}",
                    delay_seconds=60,
                    result={**state, "phase": phase, "export_id": export_id, "email": email, "last_poll_at": _now(), "last_state": wait_state},
                )
                return
            if phase == "deals":
                item = _json_dict(state.get("item"))
                if not item.get("gc_user_id"):
                    await _finish_gc_lookup_item(int(job["id"]), email, item)
                    return
                ok, deal_data, deal_error = await _getcourse_export_get(
                    "/pl/api/account/deals",
                    {"user_id": item["gc_user_id"]},
                    settings,
                    "students-deals:start",
                )
                if not ok:
                    item["deal_lookup_error"] = deal_error
                    await _finish_gc_lookup_item(int(job["id"]), email, item)
                    return
                deal_rows = _extract_export_rows(deal_data)
                if deal_rows:
                    deal_item = _deal_item_from_export_rows(deal_rows, settings)
                    if deal_item:
                        item.update({key: value for key, value in deal_item.items() if key != "source"})
                        source = _json_dict(item.get("source"))
                        source.update(deal_item.get("source") or {})
                        item["source"] = source
                    await _finish_gc_lookup_item(int(job["id"]), email, item)
                    return
                deal_export_id = _extract_export_id(deal_data)
                if deal_export_id:
                    await _defer_gc_lookup_job(
                        int(job["id"]),
                        f"waiting GetCourse deals export {deal_export_id}",
                        delay_seconds=60,
                        result={**state, "phase": "deals", "export_id": deal_export_id, "email": email, "item": item, "started_at": _now()},
                    )
                    return
                await _finish_gc_lookup_item(int(job["id"]), email, item)
                return
            ok, data, error = await _getcourse_export_get(
                "/pl/api/account/users",
                {"email": email},
                settings,
                "students-users:start",
            )
            if not ok:
                raise RuntimeError(error or "GetCourse export start failed")
            rows = _extract_export_rows(data)
            if rows:
                item = _user_item_from_export_rows(email, rows, settings)
                await _finish_gc_lookup_item(int(job["id"]), email, item)
                return
            export_id = _extract_export_id(data)
            if export_id:
                await _defer_gc_lookup_job(
                    int(job["id"]),
                    f"waiting GetCourse export {export_id}",
                    delay_seconds=60,
                    result={"export_id": export_id, "email": email, "started_at": _now()},
                )
                return
            await _finish_gc_lookup_job(int(job["id"]), "not_found", "user not found by GetCourse Export API", {})
    except Exception as exc:
        await _finish_gc_lookup_job(int(job["id"]), "failed", str(exc))


async def _gc_lookup_loop() -> None:
    await asyncio.sleep(20)
    while True:
        sleep_seconds = 60
        try:
            settings = await _settings_map()
            sleep_seconds = _bounded_int(settings.get("gc_export_lookup_worker_interval_seconds"), 10, 3600, 60)
            if not _truthy(settings.get("gc_export_lookup_enabled")):
                await asyncio.sleep(sleep_seconds)
                continue
            async with _gc_lookup_lock:
                job = await _claim_gc_lookup_job(settings)
                if job:
                    await _process_gc_lookup_job(job, settings)
                else:
                    enqueue_result = await _auto_enqueue_gc_lookup_jobs(settings)
                    if int(enqueue_result.get("queued") or 0) > 0:
                        _log("info", "gc lookup auto-enqueued %s emails", enqueue_result.get("queued"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "gc lookup worker failed: %s", exc)
        await asyncio.sleep(sleep_seconds)


def _flow_students_snapshot_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    flows: list[dict[str, Any]],
    settings: dict[str, str],
    order_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    session = AuthorizedSession(credentials)
    metadata_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_resp.raise_for_status()
    metadata = metadata_resp.json() or {}
    spreadsheet_title = _clean((metadata.get("properties") or {}).get("title"), 300)
    sheets = [sheet.get("properties") or {} for sheet in metadata.get("sheets") or []]
    data_range = _students_sheet_range(settings)
    curator_map = _curator_name_map(settings)
    matched: list[tuple[dict[str, Any], dict[str, Any], list[str]]] = []
    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for flow in flows:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 50)
        if not course_key or not stream or (course_key, stream) in seen:
            continue
        seen.add((course_key, stream))
        sheet_props = next((props for props in sheets if _sheet_title_matches(props.get("title"), course_key, stream)), None)
        if not sheet_props:
            errors.append({"course_key": course_key, "stream": stream, "status": "sheet_not_found"})
            items.append(
                {
                    **flow,
                    "sheet_title": "",
                    "sheet_id": "",
                    "sheet_url": "",
                    "students_count": 0,
                    "students": [],
                    "error": f"worksheet for {_course_sheet_prefix(course_key)}{stream} not found",
                }
            )
            continue
        title = _clean(sheet_props.get("title"), 300)
        ranges = [_a1_range(title, data_range)]
        matched.append((flow, sheet_props, ranges))

    all_value_ranges: list[dict[str, Any]] = []
    range_counts: list[int] = [len(ranges) for _, _, ranges in matched]
    range_requests: list[str] = [range_name for _, _, ranges in matched for range_name in ranges]
    for offset in range(0, len(range_requests), 10):
        chunk = range_requests[offset : offset + 10]
        params: list[tuple[str, str]] = [("majorDimension", "ROWS")]
        params.extend(("ranges", range_name) for range_name in chunk)
        try:
            values_resp = session.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
                params=params,
                timeout=45,
            )
            values_resp.raise_for_status()
            all_value_ranges.extend((values_resp.json() or {}).get("valueRanges") or [])
        except Exception as exc:
            status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            if status_code == 429:
                errors.append({"status": "google_range_error", "error": str(exc), "ranges": chunk})
                all_value_ranges.extend({"values": [], "_error": str(exc), "_range": range_name} for range_name in chunk)
                continue
            for range_name in chunk:
                try:
                    one_resp = session.get(
                        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
                        params=[("majorDimension", "ROWS"), ("ranges", range_name)],
                        timeout=30,
                    )
                    one_resp.raise_for_status()
                    value_ranges = (one_resp.json() or {}).get("valueRanges") or []
                    all_value_ranges.append(value_ranges[0] if value_ranges else {"values": []})
                except Exception as one_exc:
                    errors.append({"status": "google_range_error", "error": str(one_exc), "range": range_name})
                    all_value_ranges.append({"values": [], "_error": str(one_exc), "_range": range_name})

    cursor = 0
    for idx, (flow, sheet_props, _) in enumerate(matched):
        value_ranges = all_value_ranges[cursor : cursor + range_counts[idx]]
        cursor += range_counts[idx]
        data_rows = (value_ranges[0] or {}).get("values") if value_ranges else []
        curator_values = [value for row in (data_rows or [])[:6] for value in row if str(value or "").strip()]
        raw_curator = next((_clean(value, 300) for value in curator_values if "куратор" in _norm(value)), _clean(curator_values[0], 300) if curator_values else "")
        mapped_curator = _map_curator(raw_curator, curator_map)
        sheet_id = sheet_props.get("sheetId")
        students = _student_items_from_rows(data_rows or [], order_index, curator_map)
        items.append(
            {
                **flow,
                "curator_value": mapped_curator or flow.get("curator_value") or "",
                "curator_raw": raw_curator or flow.get("curator_raw") or "",
                "sheet_title": _clean(sheet_props.get("title"), 300),
                "sheet_id": sheet_id,
                "sheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
                "students_count": len(students),
                "students": students,
            }
        )
    items.sort(key=lambda item: (0 if item.get("course_key") == "puppy" else 1, -_bounded_int(item.get("stream"), 0, 100000, 0)))
    google_errors = [
        error
        for error in errors
        if str(error.get("status") or "").startswith("google_") and "429" in str(error.get("error") or "")
    ]
    return {
        "ok": not google_errors,
        "updated_at": _now(),
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": spreadsheet_title,
        "students_data_range": data_range,
        "customer_db_path": str(_customer_db_path()),
        "items": items,
        "errors": errors,
    }


async def _load_flow_students_cache(cache_key: str, max_age_minutes: int, allow_stale: bool = False) -> dict[str, Any] | None:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT value_json,updated_at FROM flow_students_cache WHERE key=?", (cache_key,))
        row = await cur.fetchone()
    if not row:
        return None
    updated_at = _clean(row["updated_at"], 40)
    age_seconds = max(0, int(time.time() - _iso_epoch(updated_at)))
    data = _json_dict(row["value_json"])
    if not data or not data.get("ok", True):
        return None
    data["cached"] = True
    data["cache_age_seconds"] = age_seconds
    data["cache_updated_at"] = updated_at
    if allow_stale or age_seconds <= max_age_minutes * 60:
        return data
    return None


async def _save_flow_students_cache(cache_key: str, data: dict[str, Any]) -> None:
    assert _db_path is not None
    updated_at = _clean(data.get("updated_at") or _now(), 40)
    async with _db_connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO flow_students_cache(key,value_json,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (cache_key, json.dumps(data, ensure_ascii=False), updated_at),
        )
        await db.commit()


async def _save_flow_students_fallback_cache(
    cache_key: str, data: dict[str, Any], *, reason: str
) -> dict[str, Any]:
    """Keep the last good snapshot fresh enough to avoid retry storms after Google errors."""

    assert _db_path is not None
    payload = dict(data)
    refreshed_at = _now()
    payload["cached"] = True
    payload["stale_cache"] = True
    payload["refresh_failed_at"] = refreshed_at
    payload["stale_due_error"] = _clean(reason, 1000)
    async with _db_connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO flow_students_cache(key,value_json,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (cache_key, json.dumps(payload, ensure_ascii=False), refreshed_at),
        )
        await db.commit()
    return payload


async def _flow_students(settings: dict[str, str], refresh: bool = False) -> dict[str, Any]:
    cache_key = _flow_students_cache_key(settings)
    ttl = _bounded_int(settings.get("students_cache_minutes"), 1, 1440, 30)
    if not refresh:
        cached = await _load_flow_students_cache(cache_key, ttl)
        if cached:
            return cached
        stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
        if stale:
            stale["stale_cache"] = True
            stale["needs_refresh"] = False
            stale["cache_minutes"] = ttl
            stale["getcourse_export_lookup"] = {
                "enabled": _truthy(settings.get("gc_export_lookup_enabled")),
                "requests_left_2h": await _gc_export_budget_left(settings),
                "limit_2h": _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80),
                "queue": await _gc_lookup_status(settings),
            }
            return stale
        return {
            "ok": True,
            "cached": False,
            "needs_refresh": True,
            "items": [],
            "errors": [],
            "message": "flow students cache is empty or expired; use refresh=1 to rebuild it",
            "cache_minutes": ttl,
            "getcourse_export_lookup": {
                "enabled": _truthy(settings.get("gc_export_lookup_enabled")),
                "requests_left_2h": await _gc_export_budget_left(settings),
                "limit_2h": _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80),
            },
        }
    async with _students_cache_lock:
        previous_cache = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
        spreadsheet_id = _curator_spreadsheet_id(settings)
        credentials_path = _curator_credentials_path(settings)
        if not spreadsheet_id:
            return {"ok": False, "items": [], "errors": [{"status": "missing_spreadsheet", "error": "students spreadsheet id is empty"}]}
        if not credentials_path or not credentials_path.exists():
            return {
                "ok": False,
                "items": [],
                "errors": [{"status": "missing_google_credentials", "error": "Google Sheets credentials file is not configured or not found"}],
                "credentials_path": str(credentials_path or ""),
            }
        if not _google_auth_available():
            return {"ok": False, "items": [], "errors": [{"status": "missing_google_auth", "error": "google-auth is not installed"}]}
        flow_catalog = await _chat_flows_base(settings)
        flows = flow_catalog.get("items") or []
        flow_catalog_stale = False
        if not flows and previous_cache and previous_cache.get("items"):
            flows = previous_cache["items"]
            flow_catalog_stale = True
        order_index = await _customer_order_index(settings)
        try:
            data = await asyncio.to_thread(_flow_students_snapshot_sync, spreadsheet_id, credentials_path, flows, settings, order_index)
        except Exception as exc:
            stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
            if stale:
                return await _save_flow_students_fallback_cache(cache_key, stale, reason=str(exc))
            return {"ok": False, "items": [], "errors": [{"status": "google_error", "error": str(exc)}]}
        data["cached"] = False
        data["cache_age_seconds"] = 0
        data["cache_minutes"] = ttl
        if flow_catalog_stale:
            data["flow_catalog_stale"] = True
            data["flow_catalog_error"] = "Каталог ссылок вернул пустой список; использован последний рабочий снимок"
        if data.get("ok") and not data.get("items") and previous_cache and previous_cache.get("items"):
            stale = dict(previous_cache)
            stale["empty_refresh_rejected"] = True
            return await _save_flow_students_fallback_cache(
                cache_key,
                stale,
                reason="Google вернул пустой реестр; сохранён последний рабочий снимок",
            )
        missing_emails = _missing_student_emails(data)
        lookup = await _load_gc_lookup_cache(missing_emails, settings)
        export_applied = _apply_gc_export_lookup(data, lookup)
        data["matched_orders"] = sum(1 for flow in data.get("items") or [] for student in flow.get("students") or [] if student.get("order_url") or student.get("user_url"))
        data["getcourse_export_lookup"] = {
            "enabled": _truthy(settings.get("gc_export_lookup_enabled")),
            "missing_emails": len(missing_emails),
            "applied": export_applied,
            "requests_used_this_refresh": 0,
            "requests_left_2h": await _gc_export_budget_left(settings),
            "limit_2h": _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80),
            "queue": await _gc_lookup_status(settings),
        }
        if data.get("ok"):
            await _save_flow_students_cache(cache_key, data)
            _chat_flows_cache.clear()
            try:
                changed_candidates = await _curator_change_write_candidates(previous_cache, data, settings, limit=500)
                if changed_candidates:
                    data["curator_change_jobs"] = await _enqueue_gc_fields_write_items(changed_candidates, force=True)
                else:
                    data["curator_change_jobs"] = {"queued": 0, "candidates": 0, "items": []}
            except Exception as exc:
                data["curator_change_jobs"] = {"queued": 0, "candidates": 0, "items": [], "error": _clean(str(exc), 1000)}
                _log("warning", "curator-change enqueue failed: %s", exc)
        else:
            stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
            if stale:
                return await _save_flow_students_fallback_cache(
                    cache_key,
                    stale,
                    reason="; ".join(_clean(error.get("error"), 300) for error in data.get("errors") or [])[:1000],
                )
        return data


async def _flow_students_for_processing(settings: dict[str, str]) -> dict[str, Any] | None:
    cache_key = _flow_students_cache_key(settings)
    ttl = _bounded_int(settings.get("students_cache_minutes"), 1, 1440, 30)
    cached = await _load_flow_students_cache(cache_key, ttl)
    if cached:
        return cached
    refreshed = await _flow_students(settings, refresh=True)
    if refreshed and refreshed.get("ok"):
        return refreshed
    stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
    return stale


def _student_order_date_matches(student_date: Any, fields: dict[str, Any], row: dict[str, Any]) -> bool:
    text = _clean(student_date, 50)
    match = re.search(r"(\d{1,2})[./-](\d{1,2})", text)
    if not match:
        return False
    day = int(match.group(1))
    month = int(match.group(2))
    for value in (fields.get("received_at"), row.get("updated_at"), row.get("created_at")):
        raw = _clean(value, 80)
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            continue
        if parsed.day == day and parsed.month == month:
            return True
    return False


def _student_flow_match(snapshot: dict[str, Any] | None, row: dict[str, Any], fields: dict[str, Any], course_key: str) -> dict[str, Any] | None:
    if not snapshot:
        return None
    email = _norm(fields.get("email") or fields.get("user_email"))
    gc_user_id = _clean(fields.get("gc_user_id"), 100)
    order_id = _clean(fields.get("order_id") or row.get("platform_id"), 100)
    matches: list[tuple[int, int, dict[str, Any]]] = []
    for flow in snapshot.get("items") or []:
        if _clean(flow.get("course_key"), 50) != course_key:
            continue
        stream = _clean(flow.get("stream"), 100)
        if not stream:
            continue
        for student in flow.get("students") or []:
            score = 0
            reasons: list[str] = []
            if order_id and _clean(student.get("order_id"), 100) == order_id:
                score += 100
                reasons.append("order_id")
            if gc_user_id and _clean(student.get("gc_user_id"), 100) == gc_user_id:
                score += 50
                reasons.append("gc_user_id")
            if email and _norm(student.get("email")) == email:
                score += 25
                reasons.append("email")
            if score <= 0:
                continue
            if _student_order_date_matches(student.get("date"), fields, row):
                score += 30
                reasons.append("date")
            matches.append(
                (
                    score,
                    _bounded_int(stream, 0, 100000, 0),
                    {
                        "flow": flow,
                        "student": student,
                        "stream": stream,
                        "match_reasons": reasons,
                        "score": score,
                    },
                )
            )
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    # The same editable email can remain on an older worksheet after a new
    # purchase.  An email-only tie is ambiguous and must fall back to the
    # payment/activation dates instead of silently choosing the higher stream.
    top_score = matches[0][0]
    if top_score <= 25 and sum(1 for item in matches if item[0] == top_score) > 1:
        return None
    return matches[0][2]


def _parse_chat_link_rows(rows: list[list[Any]], course_key: str, platform: str, wanted_stream: str = "") -> dict[str, Any]:
    best: dict[str, Any] | None = None
    for row in rows:
        if len(row) < 2:
            continue
        title = _clean(row[0], 500)
        link = _clean(row[1], 2000)
        if not link.startswith("http"):
            continue
        stream = _stream_number(title)
        if not stream:
            continue
        if wanted_stream and stream != str(wanted_stream):
            continue
        item = {
            "course_key": course_key,
            "platform": platform,
            "title": title,
            "stream_number": stream,
            "link": link,
            "source": "chat_links_sheet",
        }
        if wanted_stream:
            return item
        if best is None or int(stream) > int(best["stream_number"]):
            best = item
    if best:
        return best
    raise RuntimeError(f"chat link row not found for {course_key}/{platform}/{wanted_stream or 'latest'}")


def _chat_link_items_from_rows(rows: list[list[Any]], course_key: str, platform: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 2:
            continue
        title = _clean(row[0], 500)
        link = _clean(row[1], 2000)
        if not title or not link.startswith("http"):
            continue
        stream = _stream_number(title)
        if not stream:
            continue
        items.append(
            {
                "course_key": course_key,
                "platform": platform,
                "title": title,
                "stream_number": stream,
                "link": link,
                "source": "chat_links_sheet",
            }
        )
    return items


async def _fetch_chat_link_rows_public(spreadsheet_id: str, gid: str) -> list[list[str]]:
    encoded_id = urllib.parse.quote(spreadsheet_id, safe="")
    encoded_gid = urllib.parse.quote(str(gid), safe="")
    url = f"https://docs.google.com/spreadsheets/d/{encoded_id}/gviz/tq?tqx=out:csv&gid={encoded_gid}"

    def load_once() -> list[list[str]]:
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 Nexus GetCourse Chat Fields"})
        with urllib.request.urlopen(request, timeout=25) as response:
            text = response.read().decode("utf-8-sig", errors="replace")
        return [[_clean(cell, 3000) for cell in row] for row in csv.reader(io.StringIO(text))]

    return await asyncio.to_thread(load_once)


async def _fetch_chat_link_rows_private(spreadsheet_id: str, gid: str, credentials_path: Path) -> list[list[str]]:
    if not _google_auth_available():
        raise RuntimeError("google-auth is not installed")
    return await asyncio.to_thread(_fetch_chat_link_rows_private_sync, spreadsheet_id, gid, credentials_path)


def _fetch_chat_link_rows_private_sync(spreadsheet_id: str, gid: str, credentials_path: Path) -> list[list[str]]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    session = AuthorizedSession(credentials)
    metadata_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_resp.raise_for_status()
    metadata = metadata_resp.json() or {}
    title = ""
    for sheet in metadata.get("sheets") or []:
        props = sheet.get("properties") or {}
        if str(props.get("sheetId")) == str(gid):
            title = _clean(props.get("title"), 300)
            break
    if not title:
        raise RuntimeError(f"worksheet gid={gid} not found")
    values_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params=[("ranges", _a1_range(title, "A:B")), ("majorDimension", "ROWS")],
        timeout=30,
    )
    values_resp.raise_for_status()
    value_ranges = (values_resp.json() or {}).get("valueRanges") or []
    rows = (value_ranges[0] or {}).get("values") if value_ranges else []
    return [[_clean(cell, 3000) for cell in row] for row in rows]


async def _chat_link_from_sheet(course_key: str, platform: str, stream: str, settings: dict[str, str]) -> dict[str, Any]:
    gid = (CHAT_LINK_SHEETS.get(course_key) or {}).get(platform)
    spreadsheet_id = _chat_links_spreadsheet_id(settings)
    if not gid:
        return {"ok": False, "status": "sheet_gid_missing", "error": f"chat link sheet gid missing for {course_key}/{platform}"}
    if not spreadsheet_id:
        return {"ok": False, "status": "spreadsheet_missing", "error": "chat links spreadsheet id is empty"}
    credentials_path = _chat_links_credentials_path(settings)
    try:
        if credentials_path and credentials_path.exists():
            rows = await _fetch_chat_link_rows_private(spreadsheet_id, gid, credentials_path)
            source_auth = "service_account"
        else:
            rows = await _fetch_chat_link_rows_public(spreadsheet_id, gid)
            source_auth = "public_csv"
        item = _parse_chat_link_rows(rows, course_key, platform, stream)
        item.update(
            {
                "ok": True,
                "status": "ok",
                "gid": gid,
                "spreadsheet_id": spreadsheet_id,
                "source_auth": source_auth,
                "sheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={gid}",
            }
        )
        return item
    except Exception as exc:
        return {
            "ok": False,
            "status": "chat_link_error",
            "error": str(exc),
            "course_key": course_key,
            "platform": platform,
            "stream": stream,
            "gid": gid,
            "spreadsheet_id": spreadsheet_id,
            "credentials_path": str(credentials_path or ""),
        }


async def _resolve_chat_links(course_key: str, stream: str, settings: dict[str, str]) -> dict[str, Any]:
    vk, tg = await asyncio.gather(
        _chat_link_from_sheet(course_key, "vk", stream, settings),
        _chat_link_from_sheet(course_key, "telegram", stream, settings),
    )
    ok = bool(vk.get("ok") and tg.get("ok"))
    error = ""
    if not ok:
        errors = []
        if not vk.get("ok"):
            errors.append(f"VK: {vk.get('error') or vk.get('status')}")
        if not tg.get("ok"):
            errors.append(f"TG: {tg.get('error') or tg.get('status')}")
        error = "; ".join(errors)
    return {"ok": ok, "status": "ok" if ok else "chat_links_pending", "vk": vk, "telegram": tg, "error": error}


async def _chat_link_items(course_key: str, platform: str, settings: dict[str, str]) -> dict[str, Any]:
    gid = (CHAT_LINK_SHEETS.get(course_key) or {}).get(platform)
    spreadsheet_id = _chat_links_spreadsheet_id(settings)
    if not gid:
        return {"ok": False, "items": [], "error": f"chat link sheet gid missing for {course_key}/{platform}"}
    credentials_path = _chat_links_credentials_path(settings)
    try:
        if credentials_path and credentials_path.exists():
            rows = await _fetch_chat_link_rows_private(spreadsheet_id, gid, credentials_path)
            source_auth = "service_account"
        else:
            rows = await _fetch_chat_link_rows_public(spreadsheet_id, gid)
            source_auth = "public_csv"
        return {
            "ok": True,
            "items": _chat_link_items_from_rows(rows, course_key, platform),
            "gid": gid,
            "spreadsheet_id": spreadsheet_id,
            "source_auth": source_auth,
        }
    except Exception as exc:
        return {
            "ok": False,
            "items": [],
            "error": str(exc),
            "gid": gid,
            "spreadsheet_id": spreadsheet_id,
        }


async def _chat_flows_base(settings: dict[str, str]) -> dict[str, Any]:
    tasks: list[tuple[str, str, Any]] = []
    for course_key in ("puppy", "dog"):
        for platform in ("vk", "telegram"):
            tasks.append((course_key, platform, _chat_link_items(course_key, platform, settings)))
    results = await asyncio.gather(*(task for _, _, task in tasks))
    by_flow: dict[tuple[str, str], dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    for (course_key, platform, _), result in zip(tasks, results):
        if not result.get("ok"):
            errors.append({"course_key": course_key, "platform": platform, "error": result.get("error"), "gid": result.get("gid")})
            continue
        for item in result.get("items") or []:
            key = (course_key, str(item.get("stream_number") or ""))
            if not key[1]:
                continue
            flow = by_flow.setdefault(
                key,
                {
                    "course_key": course_key,
                    "course": "Щенок" if course_key == "puppy" else "Собака",
                    "stream": key[1],
                    "vk_title": "",
                    "vk_link": "",
                    "tg_title": "",
                    "tg_link": "",
                },
            )
            if platform == "vk":
                flow["vk_title"] = item.get("title") or ""
                flow["vk_link"] = item.get("link") or ""
            else:
                flow["tg_title"] = item.get("title") or ""
                flow["tg_link"] = item.get("link") or ""
    items = sorted(
        by_flow.values(),
        key=lambda item: (0 if item["course_key"] == "puppy" else 1, -_bounded_int(item.get("stream"), 0, 100000, 0)),
    )
    return {"items": items, "errors": errors, "ok": not errors}


async def _flow_activation_times() -> dict[tuple[str, str], str]:
    """Return the moment a Streams-created flow became fully usable."""

    result: dict[tuple[str, str], str] = {}
    streams_db = _student_transfer_db_path()
    if streams_db.exists():
        try:
            async with _db_connect(streams_db) as db:
                rows = await (
                    await db.execute(
                        """SELECT course_key,stream,status,updated_at FROM flow_jobs
                           ORDER BY created_at,id"""
                    )
                ).fetchall()
            for course_key, stream, status, updated_at in rows:
                key = (_clean(course_key, 50), _clean(stream, 100))
                if key[0] in {"puppy", "dog"} and key[1]:
                    result[key] = _clean(updated_at, 100) if status == "completed" else "pending"
        except Exception as exc:
            _log("warning", "Streams flow activation lookup failed: %s", exc)

    # Legacy flows were created before Streams had a completion journal.  Their
    # chat creation time is a conservative fallback for the activation cutoff.
    chat_db = _course_chat_db_path()
    if chat_db.exists():
        try:
            async with _db_connect(chat_db) as db:
                rows = await (
                    await db.execute(
                        """SELECT course_key,stream_number,platform,created_at FROM runs
                           WHERE test_mode=0 AND platform IN ('vk','telegram')
                             AND COALESCE(link,'')<>'' AND status<>'error'"""
                    )
                ).fetchall()
            grouped: dict[tuple[str, str], dict[str, int]] = {}
            for course_key, stream, platform, created_at in rows:
                key = (_clean(course_key, 50), _clean(stream, 100))
                if key[0] not in {"puppy", "dog"} or not key[1]:
                    continue
                grouped.setdefault(key, {})[_clean(platform, 20)] = int(created_at or 0)
            for key, platforms in grouped.items():
                if key in result or not platforms.get("vk") or not platforms.get("telegram"):
                    continue
                timestamp = max(platforms["vk"], platforms["telegram"])
                if timestamp > 0:
                    result[key] = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as exc:
            _log("warning", "Course chat activation lookup failed: %s", exc)
    return result


async def _apply_flow_activation_times(data: dict[str, Any]) -> dict[str, Any]:
    activation_times = await _flow_activation_times()
    copied = {**data, "items": [dict(item) for item in data.get("items") or []]}
    for item in copied["items"]:
        key = (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 100))
        activation = activation_times.get(key, "")
        item["activation_pending"] = activation == "pending"
        item["activated_at"] = "" if activation == "pending" else activation
    return copied


async def _chat_flows(settings: dict[str, str]) -> dict[str, Any]:
    cache_key = json.dumps(
        {
            "chat_links_spreadsheet_id": _chat_links_spreadsheet_id(settings),
            "chat_links_credentials_path": str(_chat_links_credentials_path(settings) or ""),
            "curator_spreadsheet_id": _curator_spreadsheet_id(settings),
            "curator_credentials_path": str(_curator_credentials_path(settings) or ""),
            "curator_cell": settings.get("curator_cell") or "K2",
            "curator_search_range": settings.get("curator_search_range") or "J2:AC2",
            "curator_map": settings.get("curator_map") or DEFAULT_CURATOR_MAP,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    now_monotonic = time.monotonic()
    cached = _chat_flows_cache.get("data")
    if cached and _chat_flows_cache.get("key") == cache_key and float(_chat_flows_cache.get("expires") or 0) > now_monotonic:
        return await _apply_flow_activation_times(cached)
    base = await _chat_flows_base(settings)
    items = base.get("items") or []
    errors = base.get("errors") or []
    curator_results = await _curators_for_flows(settings, items)
    for item in items:
        curator = curator_results.get((str(item.get("course_key") or ""), str(item.get("stream") or ""))) or {}
        item["curator_value"] = curator.get("value") or ""
        item["curator_raw"] = curator.get("raw_value") or ""
        item["curator_status"] = curator.get("status") or ""
        item["curator_sheet"] = curator.get("worksheet_title") or ""
        item["curator_url"] = curator.get("url") or ""
    _add_flow_start_dates(items)
    data = {"items": items, "errors": errors, "ok": not errors}
    if items:
        _chat_flows_cache.update({"key": cache_key, "expires": time.monotonic() + 600, "data": data})
    return await _apply_flow_activation_times(data)


async def _resolve_curator(course_key: str, stream: str, settings: dict[str, str]) -> dict[str, Any]:
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    cell = _clean(settings.get("curator_cell") or "K2", 20).upper()
    search_range = _clean(settings.get("curator_search_range") or "J2:AC2", 50).upper()
    if not stream:
        return {"ok": False, "status": "missing_stream", "error": "stream not detected"}
    if not spreadsheet_id:
        return {"ok": False, "status": "missing_spreadsheet", "error": "curator spreadsheet id is empty"}
    if not credentials_path or not credentials_path.exists():
        return {
            "ok": False,
            "status": "missing_google_credentials",
            "error": "Google Sheets credentials file is not configured or not found",
            "credentials_path": str(credentials_path or ""),
            "spreadsheet_id": spreadsheet_id,
            "cell": cell,
        }
    if not _google_auth_available():
        return {
            "ok": False,
            "status": "missing_google_auth",
            "error": "google-auth is not installed",
            "credentials_path": str(credentials_path),
            "spreadsheet_id": spreadsheet_id,
            "cell": cell,
        }
    try:
        return await asyncio.to_thread(
            _resolve_curator_sync,
            course_key,
            stream,
            spreadsheet_id,
            credentials_path,
            cell,
            _clean(settings.get("curator_search_range") or "J2:AC2", 50).upper(),
            _curator_name_map(settings),
        )
    except Exception as exc:
        return {
            "ok": False,
            "status": "google_error",
            "error": str(exc),
            "spreadsheet_id": spreadsheet_id,
            "cell": cell,
        }


def _resolve_curator_sync(
    course_key: str,
    stream: str,
    spreadsheet_id: str,
    credentials_path: Path,
    cell: str,
    search_range: str,
    curator_map: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    session = AuthorizedSession(credentials)
    metadata_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_resp.raise_for_status()
    metadata = metadata_resp.json() or {}
    spreadsheet_title = _clean((metadata.get("properties") or {}).get("title"), 300)
    matched_sheet: dict[str, Any] | None = None
    for sheet in metadata.get("sheets") or []:
        props = sheet.get("properties") or {}
        if _sheet_title_matches(props.get("title"), course_key, stream):
            matched_sheet = props
            break
    if not matched_sheet:
        return {
            "ok": False,
            "status": "sheet_not_found",
            "error": f"worksheet for {_course_sheet_prefix(course_key)}{stream} not found",
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_title": spreadsheet_title,
            "cell": cell,
        }

    title = _clean(matched_sheet.get("title"), 300)
    range_name = _a1_range(title, cell)
    ranges = [range_name]
    if search_range and search_range != cell:
        ranges.append(_a1_range(title, search_range))
    values_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params=[*(("ranges", value) for value in ranges), ("majorDimension", "ROWS")],
        timeout=30,
    )
    values_resp.raise_for_status()
    value_ranges = (values_resp.json() or {}).get("valueRanges") or []
    raw_value = ""
    for value_range in value_ranges:
        rows = (value_range or {}).get("values") or []
        values = [str(value) for row in rows for value in row if str(value or "").strip()]
        raw_value = next((_clean(value, 300) for value in values if "куратор" in _norm(value)), values[0] if values else "")
        if raw_value:
            break
    curator = _map_curator(raw_value, curator_map)
    sheet_id = matched_sheet.get("sheetId")
    result = {
        "ok": bool(curator),
        "status": "ok" if curator else "unknown_curator",
        "value": curator,
        "raw_value": _clean(raw_value, 300),
        "worksheet_title": title,
        "sheet_id": sheet_id,
        "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": spreadsheet_title,
        "cell": cell,
    }
    if not curator:
        result["error"] = f"curator value is empty or unknown in {title}!{cell}"
    return result


async def _curators_for_flows(settings: dict[str, str], flows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    cell = _clean(settings.get("curator_cell") or "K2", 20).upper()
    search_range = _clean(settings.get("curator_search_range") or "J2:AC2", 50).upper()
    keys = [(str(flow.get("course_key") or ""), str(flow.get("stream") or "")) for flow in flows]
    keys = [(course_key, stream) for course_key, stream in keys if course_key and stream]
    if not keys:
        return {}
    if not spreadsheet_id:
        return {key: {"ok": False, "status": "missing_spreadsheet", "error": "curator spreadsheet id is empty"} for key in keys}
    if not credentials_path or not credentials_path.exists():
        return {
            key: {
                "ok": False,
                "status": "missing_google_credentials",
                "error": "Google Sheets credentials file is not configured or not found",
                "credentials_path": str(credentials_path or ""),
            }
            for key in keys
        }
    if not _google_auth_available():
        return {key: {"ok": False, "status": "missing_google_auth", "error": "google-auth is not installed"} for key in keys}
    try:
        return await asyncio.to_thread(
            _curators_for_flows_sync,
            spreadsheet_id,
            credentials_path,
            cell,
            search_range,
            _curator_name_map(settings),
            tuple(dict.fromkeys(keys)),
        )
    except Exception as exc:
        return {key: {"ok": False, "status": "google_error", "error": str(exc)} for key in keys}


def _curators_for_flows_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    cell: str,
    search_range: str,
    curator_map: tuple[tuple[str, str], ...],
    keys: tuple[tuple[str, str], ...],
) -> dict[tuple[str, str], dict[str, Any]]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    session = AuthorizedSession(credentials)
    metadata_resp = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_resp.raise_for_status()
    metadata = metadata_resp.json() or {}
    spreadsheet_title = _clean((metadata.get("properties") or {}).get("title"), 300)
    sheets = [sheet.get("properties") or {} for sheet in metadata.get("sheets") or []]
    matched: list[tuple[tuple[str, str], dict[str, Any], list[str]]] = []
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for course_key, stream in keys:
        sheet_props = next((props for props in sheets if _sheet_title_matches(props.get("title"), course_key, stream)), None)
        if not sheet_props:
            result[(course_key, stream)] = {
                "ok": False,
                "status": "sheet_not_found",
                "error": f"worksheet for {_course_sheet_prefix(course_key)}{stream} not found",
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_title": spreadsheet_title,
                "cell": cell,
            }
            continue
        title = _clean(sheet_props.get("title"), 300)
        ranges = [_a1_range(title, cell)]
        if search_range and search_range != cell:
            ranges.append(_a1_range(title, search_range))
        matched.append(((course_key, stream), sheet_props, ranges))
    if matched:
        all_value_ranges: list[Any] = []
        range_counts: list[int] = [len(ranges) for _, _, ranges in matched]
        range_requests: list[tuple[int, str]] = []
        for matched_idx, (_, _, ranges) in enumerate(matched):
            range_requests.extend((matched_idx, range_name) for range_name in ranges)
        for offset in range(0, len(range_requests), 10):
            chunk = range_requests[offset : offset + 10]
            params: list[tuple[str, str]] = [("majorDimension", "ROWS")]
            params.extend(("ranges", range_name) for _, range_name in chunk)
            try:
                values_resp = session.get(
                    f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
                    params=params,
                    timeout=30,
                )
                values_resp.raise_for_status()
                all_value_ranges.extend((values_resp.json() or {}).get("valueRanges") or [])
            except Exception:
                for _, range_name in chunk:
                    try:
                        one_resp = session.get(
                            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
                            params=[("majorDimension", "ROWS"), ("ranges", range_name)],
                            timeout=30,
                        )
                        one_resp.raise_for_status()
                        value_ranges = (one_resp.json() or {}).get("valueRanges") or []
                        all_value_ranges.append(value_ranges[0] if value_ranges else {})
                    except Exception as exc:
                        all_value_ranges.append({"values": [], "_error": str(exc)})
        cursor = 0
        for idx, (key, sheet_props, _) in enumerate(matched):
            value_ranges = all_value_ranges[cursor : cursor + range_counts[idx]]
            cursor += range_counts[idx]
            error_range = next((value_range for value_range in value_ranges if value_range.get("_error")), None)
            if error_range and len(value_ranges) == 1:
                result[key] = {
                    "ok": False,
                    "status": "google_error",
                    "error": error_range.get("_error"),
                    "spreadsheet_id": spreadsheet_id,
                    "cell": cell,
                }
                continue
            raw_value = ""
            for value_range in value_ranges:
                if value_range.get("_error"):
                    continue
                rows = (value_range or {}).get("values") if value_range else []
                values = [str(value) for row in (rows or []) for value in row if str(value or "").strip()]
                raw_value = next((_clean(value, 300) for value in values if "куратор" in _norm(value)), values[0] if values else "")
                if raw_value:
                    break
            curator = _map_curator(raw_value, curator_map)
            title = _clean(sheet_props.get("title"), 300)
            sheet_id = sheet_props.get("sheetId")
            item = {
                "ok": bool(curator),
                "status": "ok" if curator else "unknown_curator",
                "value": curator,
                "raw_value": _clean(raw_value, 300),
                "worksheet_title": title,
                "sheet_id": sheet_id,
                "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_title": spreadsheet_title,
                "cell": cell,
            }
            if not curator:
                item["error"] = f"curator value is empty or unknown in {title}!{cell}"
            result[key] = item
    return result


async def _latest_chats() -> dict[str, dict[str, dict[str, Any] | None]]:
    db_path = _course_chat_db_path()
    result: dict[str, dict[str, dict[str, Any] | None]] = {
        "puppy": {"vk": None, "telegram": None},
        "dog": {"vk": None, "telegram": None},
    }
    if not db_path.exists():
        return result
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        for course_key in result:
            cur = await db.execute(
                """
                SELECT id,platform,title,stream_number,date_start,course_key,status,link,chat_id,created_at
                FROM runs
                WHERE course_key=? AND platform IN ('vk','telegram') AND test_mode=0
                  AND COALESCE(link,'')<>'' AND status<>'error'
                ORDER BY created_at, id
                """,
                (course_key,),
            )
            rows = await cur.fetchall()
            by_stream: dict[str, dict[str, dict[str, Any]]] = {}
            for row in rows:
                item = dict(row)
                stream_number = _clean(item.get("stream_number"), 100)
                if stream_number:
                    by_stream.setdefault(stream_number, {})[item["platform"]] = item
            complete = [
                pair for pair in by_stream.values() if pair.get("vk") and pair.get("telegram")
            ]
            if not complete:
                continue
            pair = max(
                complete,
                key=lambda item: max(
                    (int(item["vk"].get("created_at") or 0), int(item["vk"].get("id") or 0)),
                    (int(item["telegram"].get("created_at") or 0), int(item["telegram"].get("id") or 0)),
                ),
            )
            result[course_key] = {"vk": pair["vk"], "telegram": pair["telegram"]}
    return result


def _active_chats_from_flows(data: dict[str, Any]) -> dict[str, dict[str, dict[str, Any] | None]]:
    """Return the highest stream whose manual VK and Telegram links are both ready."""

    result: dict[str, dict[str, dict[str, Any] | None]] = {
        "puppy": {"vk": None, "telegram": None},
        "dog": {"vk": None, "telegram": None},
    }
    ready: dict[str, list[dict[str, Any]]] = {"puppy": [], "dog": []}
    for flow in data.get("items") or []:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 100)
        vk_link = _clean(flow.get("vk_link"), 2000)
        tg_link = _clean(flow.get("tg_link"), 2000)
        if course_key not in ready or not stream:
            continue
        if not re.match(r"^https?://", vk_link, flags=re.IGNORECASE):
            continue
        if not re.match(r"^https?://", tg_link, flags=re.IGNORECASE):
            continue
        ready[course_key].append(flow)
    for course_key, items in ready.items():
        if not items:
            continue
        flow = max(items, key=lambda item: (_bounded_int(item.get("stream"), 0, 100000, 0), _clean(item.get("stream"), 100)))
        stream = _clean(flow.get("stream"), 100)
        result[course_key] = {
            "vk": {
                "platform": "vk",
                "title": _clean(flow.get("vk_title"), 300),
                "stream_number": stream,
                "link": _clean(flow.get("vk_link"), 2000),
                "course_key": course_key,
                "source": "chat_links_sheet_active",
            },
            "telegram": {
                "platform": "telegram",
                "title": _clean(flow.get("tg_title"), 300),
                "stream_number": stream,
                "link": _clean(flow.get("tg_link"), 2000),
                "course_key": course_key,
                "source": "chat_links_sheet_active",
            },
        }
    return result


def _prefer_sheet_chat(
    sheet_chat: dict[str, Any], created_chat: dict[str, Any] | None, stream: str
) -> dict[str, Any]:
    del created_chat, stream
    return sheet_chat


async def _customer_rows(settings: dict[str, str], limit: int) -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    assert _db_path is not None
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("ATTACH DATABASE ? AS mod", (str(_db_path),))
        cur = await db.execute(
            """
            SELECT c.id, c.platform_id, c.custom_fields, c.created_at, c.updated_at
            FROM cdb_getcourse_orders c
            LEFT JOIN mod.processed_orders p ON p.source_record_id = c.id
            WHERE datetime(COALESCE(c.updated_at, c.created_at)) >= datetime(?)
              AND (
                p.source_record_id IS NULL
                OR p.status NOT IN ('processed','skipped')
                OR datetime(COALESCE(c.updated_at, c.created_at)) > datetime(COALESCE(p.updated_at, '1970-01-01T00:00:00Z'))
              )
            ORDER BY CASE
                       WHEN p.source_record_id IS NULL THEN 0
                       WHEN datetime(COALESCE(c.updated_at, c.created_at)) > datetime(COALESCE(p.updated_at, '1970-01-01T00:00:00Z')) THEN 1
                       WHEN p.status NOT IN ('processed','skipped') THEN 2
                       ELSE 3
                     END,
                     datetime(COALESCE(c.updated_at, c.created_at)) DESC,
                     c.id DESC
            LIMIT ?
            """,
            (settings.get("start_date") or _today(), max(1, min(5000, int(limit)))),
        )
        rows = [dict(row) for row in await cur.fetchall()]
        await db.execute("DETACH DATABASE mod")
        return rows


def _source_order_summary(row: dict[str, Any], state: dict[str, Any] | None) -> dict[str, Any]:
    fields = _json_dict(row.get("custom_fields"))
    entitlement = _chat_entitlement(fields)
    course_key = _clean(entitlement.get("course_key"), 50)
    tariff = _clean(entitlement.get("tariff"), 50)
    state_details = _json_dict((state or {}).get("details_json", "{}"))
    output_fields = state_details.get("output_fields") if isinstance(state_details.get("output_fields"), dict) else {}
    title = _clean(fields.get("title") or fields.get("positions") or fields.get("offers"), 1000)
    return {
        "id": int(row.get("id") or 0),
        "platform_id": _clean(row.get("platform_id"), 100),
        "order_id": _clean(fields.get("order_id") or row.get("platform_id"), 100),
        "gc_user_id": _clean(fields.get("gc_user_id"), 100),
        "created_at": _clean(row.get("created_at"), 100),
        "updated_at": _clean(row.get("updated_at"), 100),
        "status": _clean(fields.get("status"), 100),
        "payment_state": _clean(fields.get("payment_state"), 100),
        "title": title,
        "course_key": course_key,
        "course": "Щенок" if course_key == "puppy" else ("Собака" if course_key == "dog" else ""),
        "tariff": tariff,
        "eligible": bool(entitlement.get("eligible")),
        "entitlement_reason": _clean(entitlement.get("reason"), 200),
        "processed_status": _clean((state or {}).get("status"), 100),
        "processed_error": _clean((state or {}).get("error"), 1000),
        "processed_stream": _clean((state or {}).get("stream") or output_fields.get("Поток"), 100),
        "processed_updated_at": _clean((state or {}).get("updated_at"), 100),
    }


async def _source_orders(settings: dict[str, str], query: str = "", date_from: str = "", limit: int = 100) -> dict[str, Any]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return {"items": [], "path": str(db_path), "error": "customer-db not found"}
    query = _clean(query, 300)
    date_from = _clean(date_from, 30)
    max_limit = max(1, min(500, int(limit)))
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        if query:
            like = f"%{query}%"
            cur = await db.execute(
                """
                SELECT id, platform_id, custom_fields, created_at, updated_at
                FROM cdb_getcourse_orders
                WHERE CAST(id AS TEXT)=? OR COALESCE(platform_id,'') LIKE ? OR COALESCE(custom_fields,'') LIKE ?
                ORDER BY datetime(COALESCE(updated_at, created_at)) DESC, id DESC
                LIMIT ?
                """,
                (query, like, like, max_limit),
            )
        else:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from):
                date_from = settings.get("start_date") or _today()
            cur = await db.execute(
                """
                SELECT id, platform_id, custom_fields, created_at, updated_at
                FROM cdb_getcourse_orders
                WHERE datetime(COALESCE(updated_at, created_at)) >= datetime(?)
                ORDER BY datetime(COALESCE(updated_at, created_at)) DESC, id DESC
                LIMIT ?
                """,
                (date_from, max_limit),
            )
        rows = [dict(row) for row in await cur.fetchall()]
    states = await _processed_state([int(row["id"]) for row in rows])
    return {
        "items": [_source_order_summary(row, states.get(int(row["id"]))) for row in rows],
        "path": str(db_path),
        "start_date": settings.get("start_date") or _today(),
        "query": query,
    }


async def _processed_state(record_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not record_ids:
        return {}
    placeholders = ",".join("?" for _ in record_ids)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM processed_orders WHERE source_record_id IN ({placeholders})",
            tuple(record_ids),
        )
        return {int(row["source_record_id"]): dict(row) for row in await cur.fetchall()}


def _should_skip_state(state: dict[str, Any] | None, source_hash: str, settings: dict[str, str], gc_ready: bool) -> bool:
    if not state or state.get("source_hash") != source_hash:
        return False
    status = str(state.get("status") or "")
    if status == "processed":
        details = _json_dict(state.get("details_json", "{}"))
        if gc_ready and (not details.get("getcourse_deal") or not details.get("getcourse_user_field_ids")):
            return False
        return True
    if _truthy(settings.get("dry_run")) and status == "dry_run":
        return True
    if not gc_ready and status in {"customer_only", "dry_run"}:
        return True
    if status in {"skipped", "quarantined"}:
        return True
    error = _clean(state.get("error"), 2000)
    if status == "failed" and _gc_error_classification(error) == "terminal":
        return True
    if status == "customer_only":
        details = _json_dict(state.get("details_json", "{}"))
        retry = _json_dict(details.get("retry"))
        classification = _clean(retry.get("classification"), 50) or _gc_error_classification(error)
        if classification == "terminal":
            return True
        next_retry_at = _iso_epoch(retry.get("next_retry_at"))
        if next_retry_at:
            return next_retry_at > time.time()
        legacy_updated_at = _iso_epoch(state.get("updated_at"))
        if legacy_updated_at:
            delay = _gc_retry_delay_seconds(settings, 1, classification)
            return legacy_updated_at + delay > time.time()
    return False


async def _update_customer_fields(record_id: int, fields: dict[str, Any], patch: dict[str, Any]) -> None:
    db_path = _customer_db_path()
    merged = dict(fields)
    merged.update(patch)
    async with _db_connect(db_path) as db:
        await db.execute(
            """
            UPDATE cdb_getcourse_orders
            SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE id=?
            """,
            (json.dumps(merged, ensure_ascii=False), record_id),
        )
        await db.commit()


async def _sync_gc_fields_write_customer_state(
    job: dict[str, Any],
    output_fields: dict[str, Any],
    flow: dict[str, Any],
    *,
    getcourse_ok: bool,
    error: str = "",
) -> dict[str, Any]:
    order_id = _clean(job.get("order_id"), 100)
    gc_user_id = _clean(job.get("gc_user_id"), 100)
    email = _norm(job.get("email"))
    if not output_fields or not (order_id or gc_user_id or email):
        return {"synced": False, "reason": "missing identity"}

    record_id = 0
    fields: dict[str, Any] = {}
    db_path = _customer_db_path()
    if db_path.exists():
        async with _db_connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            clauses: list[str] = []
            args: list[Any] = []
            if order_id:
                clauses.extend(["platform_id=?", "custom_fields LIKE ?"])
                args.extend([order_id, f"%{order_id}%"])
            if gc_user_id:
                clauses.append("custom_fields LIKE ?")
                args.append(f"%{gc_user_id}%")
            if email:
                clauses.append("LOWER(custom_fields) LIKE ?")
                args.append(f"%{email}%")
            cur = await db.execute(
                f"""
                SELECT id, custom_fields
                FROM cdb_getcourse_orders
                WHERE {' OR '.join(clauses)}
                ORDER BY datetime(COALESCE(updated_at, created_at)) DESC, id DESC
                LIMIT 30
                """,
                tuple(args),
            )
            rows = [dict(row) for row in await cur.fetchall()]
        best_score = -1
        for row in rows:
            candidate = _json_dict(row.get("custom_fields"))
            score = 0
            if order_id and _clean(candidate.get("order_id") or row.get("platform_id"), 100) == order_id:
                score += 100
            if gc_user_id and _clean(candidate.get("gc_user_id"), 100) == gc_user_id:
                score += 40
            if email and _norm(candidate.get("email") or candidate.get("user_email")) == email:
                score += 20
            if score > best_score:
                best_score = score
                record_id = int(row.get("id") or 0)
                fields = candidate
        if record_id > 0 and best_score > 0:
            patch = {
                **output_fields,
                f"{MACHINE_PREFIX}course_key": _clean(flow.get("course_key"), 50),
                f"{MACHINE_PREFIX}tariff": _clean(fields.get(f"{MACHINE_PREFIX}tariff") or _classify_tariff(fields), 50),
                f"{MACHINE_PREFIX}curator_raw": _clean(flow.get("curator_raw"), 300),
                f"{MACHINE_PREFIX}curator_sheet": _clean(flow.get("sheet_title"), 300),
                f"{MACHINE_PREFIX}links_source": _clean(flow.get("change_reason") or "field_write_reconciliation", 100),
                f"{MACHINE_PREFIX}vk_link_title": _clean(flow.get("vk_title"), 300),
                f"{MACHINE_PREFIX}tg_link_title": _clean(flow.get("tg_title"), 300),
                f"{MACHINE_PREFIX}source_record_id": record_id,
                f"{MACHINE_PREFIX}updated_at": _now(),
            }
            await _update_customer_fields(record_id, fields, patch)

    assert _db_path is not None
    updated_processed = 0
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        where_parts: list[str] = []
        args: list[Any] = []
        if record_id > 0:
            where_parts.append("source_record_id=?")
            args.append(record_id)
        if order_id:
            where_parts.append("order_id=?")
            args.append(order_id)
        if gc_user_id:
            where_parts.append("gc_user_id=?")
            args.append(gc_user_id)
        if where_parts:
            cur = await db.execute(
                f"SELECT id, details_json FROM processed_orders WHERE {' OR '.join(where_parts)}",
                tuple(args),
            )
            rows = [dict(row) for row in await cur.fetchall()]
            for row in rows:
                details = _json_dict(row.get("details_json"))
                details["output_fields"] = dict(output_fields)
                details["field_write_sync"] = {
                    "job_id": int(job.get("id") or 0),
                    "email": email,
                    "flow": flow,
                    "synced_at": _now(),
                }
                await db.execute(
                    """
                    UPDATE processed_orders
                    SET stream=?,
                        vk_link=?,
                        tg_link=?,
                        customer_ok=CASE WHEN ? > 0 THEN 1 ELSE customer_ok END,
                        getcourse_ok=?,
                        status=CASE WHEN ? THEN 'processed' ELSE status END,
                        error=?,
                        details_json=?,
                        updated_at=?
                    WHERE id=?
                    """,
                    (
                        _clean(output_fields.get(DEFAULT_FIELD_NAMES["field_stream"]) or output_fields.get("Поток"), 100),
                        _clean(output_fields.get(DEFAULT_FIELD_NAMES["field_vk"]) or output_fields.get("Ссылка на чат ВК"), 2000),
                        _clean(output_fields.get(DEFAULT_FIELD_NAMES["field_tg"]) or output_fields.get("Ссылка на чат ТГ"), 2000),
                        record_id,
                        1 if getcourse_ok else 0,
                        1 if getcourse_ok else 0,
                        _clean(error, 2000),
                        json.dumps(details, ensure_ascii=False),
                        _now(),
                        int(row["id"]),
                    ),
                )
                updated_processed += 1
            await db.commit()

    return {
        "synced": bool(record_id or updated_processed),
        "customer_record_id": record_id,
        "processed_rows": updated_processed,
    }


def _getcourse_user_payload(gc_user_id: str, fields: dict[str, Any], email: str = "", phone: str = "") -> dict[str, Any]:
    user = {"id": str(gc_user_id), "addfields": dict(fields)}
    # GetCourse Import API requires email or phone even when an id is present.
    # The caller is responsible for resolving a conflicting source email to
    # the address that actually belongs to gc_user_id before building payload.
    if _valid_email(email):
        user["email"] = _clean(email, 300)
    elif _clean(phone, 100):
        user["phone"] = _clean(phone, 100)
    return {
        "user": user,
        "system": {"refresh_if_exists": 1},
    }


def _getcourse_user_addfields(output_fields: dict[str, Any], settings: dict[str, str]) -> dict[str, Any]:
    pairs = (
        ("user_field_stream_id", "field_stream"),
        ("user_field_vk_id", "field_vk"),
        ("user_field_tg_id", "field_tg"),
        ("user_field_curator_id", "field_curator"),
    )
    result: dict[str, Any] = dict(output_fields)
    for id_key, name_key in pairs:
        field_id = _clean(settings.get(id_key), 100)
        field_name = settings.get(name_key, "")
        value = output_fields.get(field_name, "")
        result[field_id or field_name] = value
    return result


def _getcourse_deal_payload(gc_user_id: str, deal_number: str, fields: dict[str, Any], email: str = "", phone: str = "") -> dict[str, Any]:
    user = {"id": str(gc_user_id)}
    if _valid_email(email):
        user["email"] = _clean(email, 300)
    elif _clean(phone, 100):
        user["phone"] = _clean(phone, 100)
    return {
        "user": user,
        "system": {
            "refresh_if_exists": 1,
            "return_deal_number": 1,
        },
        "deal": {
            "deal_number": str(deal_number),
            "addfields": dict(fields),
        },
    }


async def _post_getcourse_import(path: str, action: str, payload: dict[str, Any], settings: dict[str, str], purpose: str = "getcourse-import") -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    if not env["account_name"] or not env["api_token"]:
        return False, "GETCOURSE_ACCOUNT_NAME/GETCOURSE_API_TOKEN не настроены", {}
    encoded = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("ascii")
    url = f"https://{env['account_name']}.getcourse.ru{path}"
    form = {"action": action, "key": env["api_token"], "params": encoded}
    timeout = _bounded_int(settings.get("request_timeout"), 5, 60, 20)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, data=form)
    body: Any
    try:
        body = resp.json()
    except Exception:
        body = {"text": resp.text[:1000]}
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}", {"response": body}
    top_success = bool(isinstance(body, dict) and body.get("success", False))
    result_payload = body.get("result") if isinstance(body, dict) and isinstance(body.get("result"), dict) else {}
    result_success = bool(result_payload.get("success", True))
    result_error = bool(result_payload.get("error", False))
    if not top_success or not result_success or result_error:
        error = ""
        if isinstance(result_payload, dict):
            error = _clean(result_payload.get("error_message") or result_payload.get("error"), 1000)
        if not error and isinstance(body, dict):
            error = _clean(body.get("error_message") or body.get("error"), 1000)
        return False, error or "GetCourse update error", {"response": body}
    return True, "", {"response": body}


async def _write_getcourse_user(gc_user_id: str, fields: dict[str, Any], settings: dict[str, str], email: str = "", phone: str = "") -> tuple[bool, str, dict[str, Any]]:
    if not gc_user_id:
        return False, "gc_user_id отсутствует в заказе", {}
    return await _post_getcourse_import("/pl/api/users", "add", _getcourse_user_payload(gc_user_id, fields, email, phone), settings, "students-fields:user")


async def _write_getcourse_deal(gc_user_id: str, deal_number: str, fields: dict[str, Any], settings: dict[str, str], email: str = "", phone: str = "") -> tuple[bool, str, dict[str, Any]]:
    if not gc_user_id:
        return False, "gc_user_id отсутствует в заказе", {}
    if not deal_number:
        return False, "deal_number отсутствует в заказе", {}
    return await _post_getcourse_import("/pl/api/deals", "add", _getcourse_deal_payload(gc_user_id, deal_number, fields, email, phone), settings, "students-fields:deal")


async def service_trigger_onboarding_email(
    *,
    gc_user_id: str,
    email: str = "",
    phone: str = "",
    group_name: str,
    fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically write one Nexus mail package and append its trigger group.

    This intentionally exposes only the narrow GetCourse Import API surface
    required by getcourse-onboarding.  It cannot remove groups, change an
    order, or target an arbitrary group/field namespace.
    """

    gc_user_id = _clean(gc_user_id, 100)
    email = _clean(email, 300)
    phone = _clean(phone, 100)
    group_name = _clean(group_name, 300)
    if not gc_user_id or not (_valid_email(email) or phone):
        return {"ok": False, "error": "Для email-пакета нужны gc_user_id и точный email или phone"}
    if not group_name.startswith("Nexus email "):
        return {"ok": False, "error": "Разрешены только группы с префиксом «Nexus email »"}
    safe_fields: dict[str, str] = {}
    for key, value in list((fields or {}).items())[:20]:
        field_name = _clean(key, 200)
        if not field_name.startswith("Nexus email "):
            return {"ok": False, "error": "Разрешены только поля с префиксом «Nexus email »"}
        safe_fields[field_name] = _clean(value, 10000)
    user: dict[str, Any] = {
        "id": gc_user_id,
        "group_name": [group_name],
        "addfields": safe_fields,
    }
    if _valid_email(email):
        user["email"] = email
    else:
        user["phone"] = phone
    settings = await _settings_map()
    ok, error, details = await _post_getcourse_import(
        "/pl/api/users",
        "add",
        {"user": user, "system": {"refresh_if_exists": 1}},
        settings,
        "onboarding-email:user",
    )
    return {
        "ok": bool(ok),
        "error": _clean(error, 1000),
        "gc_user_id": gc_user_id,
        "group_name": group_name,
        "field_names": sorted(safe_fields),
        "details": details,
    }


async def service_update_upgrade_order(
    *,
    gc_user_id: str,
    deal_number: str,
    email: str = "",
    phone: str = "",
    deal_status: str = "",
    offer_id: str = "",
    deal_cost: float | int | str | None = None,
    addfields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply one narrow, auditable Standard→Premium order mutation.

    Deliberately forbids ``payed``: GetCourse documents that setting it via
    Import API may create a payment.  Final completion belongs to a native
    GetCourse process after the coordinator writes its marker field.
    """

    gc_user_id = _clean(gc_user_id, 100)
    deal_number = _clean(deal_number, 100)
    email = _clean(email, 300)
    phone = _clean(phone, 100)
    deal_status = _clean(deal_status, 40).casefold()
    offer_id = _clean(offer_id, 30)
    if not deal_number or not gc_user_id:
        return {"ok": False, "error": "Для изменения нужны gc_user_id и deal_number"}
    if deal_status and deal_status not in {"new", "in_work"}:
        return {"ok": False, "error": "Автоматизации доплат разрешены только статусы new и in_work"}
    if offer_id and not offer_id.isdigit():
        return {"ok": False, "error": "offer_id должен быть числовым"}
    if deal_cost is not None:
        try:
            normalized_cost = round(float(deal_cost), 2)
        except (TypeError, ValueError):
            return {"ok": False, "error": "Некорректная стоимость заказа"}
        if normalized_cost < 0 or normalized_cost > 100_000_000:
            return {"ok": False, "error": "Стоимость заказа вне допустимого диапазона"}
    else:
        normalized_cost = None
    safe_fields: dict[str, str] = {}
    for key, value in list((addfields or {}).items())[:20]:
        field_key = _clean(key, 200)
        if field_key:
            safe_fields[field_key] = _clean(value, 2000)
    deal: dict[str, Any] = {"deal_number": deal_number}
    if deal_status:
        deal["deal_status"] = deal_status
    if offer_id:
        deal["offer_id"] = offer_id
        deal["quantity"] = 1
    if normalized_cost is not None:
        deal["deal_cost"] = f"{normalized_cost:.2f}"
    if safe_fields:
        deal["addfields"] = safe_fields
    if len(deal) == 1:
        return {"ok": False, "error": "Нет изменений для заказа"}
    user: dict[str, str] = {"id": gc_user_id}
    if _valid_email(email):
        user["email"] = email
    elif phone:
        user["phone"] = phone
    payload = {
        "user": user,
        "system": {"refresh_if_exists": 1, "return_deal_number": 1},
        "deal": deal,
    }
    settings = await _settings_map()
    ok, error, details = await _post_getcourse_import(
        "/pl/api/deals", "add", payload, settings, "onboarding-upgrade:deal"
    )
    return {
        "ok": bool(ok),
        "error": _clean(error, 1000),
        "deal_number": deal_number,
        "changed": {
            "deal_status": deal_status,
            "offer_id": offer_id,
            "deal_cost": normalized_cost,
            "field_names": sorted(safe_fields),
        },
        "details": details,
    }


async def _resolve_getcourse_email_for_user(
    gc_user_id: str,
    email: str,
    phone: str,
    settings: dict[str, str],
) -> tuple[str, dict[str, Any], str]:
    """Resolve an Import-API identity without trusting a conflicting order email."""
    wanted_id = _clean(gc_user_id, 100)
    filters: list[tuple[str, str]] = []
    clean_phone = _clean(phone, 100)
    clean_email = _clean(email, 300)
    if clean_phone:
        filters.append(("phone", clean_phone))
    if _valid_email(clean_email):
        filters.append(("email", clean_email))
    checked: list[dict[str, Any]] = []
    last_error = ""
    for key, value in filters:
        rows, error = await _getcourse_export_rows(
            "/pl/api/account/users",
            {key: value},
            settings,
            f"students-identity-{key}",
        )
        last_error = error or last_error
        for row in rows:
            row_id = _user_id_from_export_row(row)
            row_email = _email_from_export_row(row)
            checked.append({"gc_user_id": row_id, "email": row_email})
            if row_id == wanted_id and _valid_email(row_email):
                return row_email, {"filter": key, "matched": True, "candidates": checked}, ""
    return "", {"matched": False, "candidates": checked}, last_error or "GetCourse user identity not found"


async def _gc_fields_source_order(order_id: str) -> dict[str, Any]:
    clean_order_id = _clean(order_id, 100)
    if not clean_order_id:
        return {}
    db_path = _customer_db_path()
    if not db_path.exists():
        return {}
    async with _db_connect(db_path) as db:
        cur = await db.execute(
            """
            SELECT custom_fields
            FROM cdb_getcourse_orders
            WHERE platform_id=?
            ORDER BY datetime(COALESCE(updated_at,created_at)) DESC, id DESC
            LIMIT 1
            """,
            (clean_order_id,),
        )
        row = await cur.fetchone()
    return _json_dict((row or [""])[0])


async def _enqueue_gc_fields_write_items(candidates: list[dict[str, Any]], force: bool = False) -> dict[str, Any]:
    if not candidates:
        return {"queued": 0, "candidates": 0, "items": []}
    queued = 0
    conflict_where = (
        "gc_fields_write_jobs.status <> 'running' AND gc_fields_write_jobs.payload_json <> excluded.payload_json"
        if force
        else "gc_fields_write_jobs.status NOT IN ('completed','pending','running') AND gc_fields_write_jobs.payload_json <> excluded.payload_json"
    )
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        for item in candidates:
            payload = {
                "fields": item["fields"],
                "user_fields": item["user_fields"],
                "flow": item["flow"],
            }
            cur = await db.execute(
                f"""
                INSERT INTO gc_fields_write_jobs(email,gc_user_id,order_id,deal_number,status,last_error,payload_json,result_json,updated_at)
                VALUES(?,?,?,?, 'pending', '', ?, '{{}}', ?)
                ON CONFLICT(email, order_id) DO UPDATE SET
                    gc_user_id=excluded.gc_user_id,
                    deal_number=excluded.deal_number,
                    status='pending',
                    attempts=0,
                    next_run_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'),
                    last_error='',
                    payload_json=excluded.payload_json,
                    result_json='{{}}',
                    updated_at=excluded.updated_at
                WHERE {conflict_where}
                """,
                (
                    item["email"],
                    item["gc_user_id"],
                    item["order_id"],
                    item["deal_number"],
                    json.dumps(payload, ensure_ascii=False),
                    _now(),
                ),
            )
            queued += max(0, int(cur.rowcount or 0))
        await db.commit()
    return {"queued": queued, "candidates": len(candidates), "items": candidates[:20]}


async def _enqueue_gc_fields_write_jobs(settings: dict[str, str], limit: int = 50) -> dict[str, Any]:
    candidates = await _fields_write_candidates_from_cache(settings, limit=limit)
    return await _enqueue_gc_fields_write_items(candidates, force=False)


async def _enqueue_gc_fields_reconciliation_jobs(settings: dict[str, str], limit: int = 50) -> dict[str, Any]:
    candidates = await _fields_write_reconciliation_candidates(settings, limit=limit)
    return await _enqueue_gc_fields_write_items(candidates, force=True)


async def _gc_fields_write_status(settings: dict[str, str] | None = None) -> dict[str, Any]:
    active_settings = settings or await _settings_map()
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT status,COUNT(*) FROM gc_fields_write_jobs GROUP BY status")
        counts = {str(row[0]): int(row[1] or 0) for row in await cur.fetchall()}
        cur = await db.execute(
            """
            SELECT id,email,order_id,deal_number,status,attempts,next_run_at,last_error,updated_at
            FROM gc_fields_write_jobs
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT 20
            """
        )
        recent = [
            {
                "id": int(row[0]),
                "email": row[1],
                "order_id": row[2],
                "deal_number": row[3],
                "status": row[4],
                "attempts": int(row[5] or 0),
                "next_run_at": row[6],
                "last_error": row[7],
                "updated_at": row[8],
            }
            for row in await cur.fetchall()
        ]
    return {
        "enabled": _truthy(active_settings.get("gc_fields_write_enabled")),
        "counts": counts,
        "recent": recent,
        "requests_used_2h": await _gc_export_calls_used(),
        "requests_left_2h": await _gc_export_budget_left(active_settings),
        "limit_2h": _bounded_int(active_settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80),
        "reserved_requests": _gc_new_job_reserve(active_settings),
        "next_budget_at": await _gc_export_next_budget_at(active_settings, needed=2),
    }


async def _claim_gc_fields_write_job(settings: dict[str, str]) -> dict[str, Any] | None:
    max_attempts = _bounded_int(settings.get("gc_fields_write_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT *
            FROM gc_fields_write_jobs
            WHERE status IN ('pending','failed')
              AND attempts < ?
              AND datetime(next_run_at) <= datetime('now')
            ORDER BY CASE
                       WHEN attempts=0 THEN 0
                       ELSE 1
                     END,
                     CASE
                       WHEN payload_json LIKE '%field_write_reconciliation%' THEN 0
                       WHEN payload_json LIKE '%curator_changed_from_sheets%' THEN 0
                       ELSE 1
                     END,
                     datetime(next_run_at) ASC,
                     id ASC
            LIMIT 1
            """,
            (max_attempts,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        await db.execute(
            "UPDATE gc_fields_write_jobs SET status='running', attempts=attempts+1, updated_at=? WHERE id=?",
            (_now(), int(row["id"])),
        )
        await db.commit()
        return dict(row)


async def _mark_exhausted_gc_fields_write_jobs(settings: dict[str, str]) -> int:
    max_attempts = _bounded_int(settings.get("gc_fields_write_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute(
            """
            UPDATE gc_fields_write_jobs
            SET status='failed_exhausted',
                last_error=CASE WHEN COALESCE(last_error,'')='' THEN 'max attempts exhausted' ELSE last_error END,
                updated_at=?
            WHERE status IN ('pending','failed')
              AND attempts >= ?
            """,
            (_now(), max_attempts),
        )
        await db.commit()
        return max(0, int(cur.rowcount or 0))


async def _finish_gc_fields_write_job(job_id: int, status: str, error: str = "", result: dict[str, Any] | None = None) -> None:
    delay_seconds = 0
    if status == "failed":
        assert _db_path is not None
        async with _db_connect(_db_path) as db_read:
            cur = await db_read.execute("SELECT attempts FROM gc_fields_write_jobs WHERE id=?", (int(job_id),))
            row = await cur.fetchone()
        attempts = int((row or [1])[0] or 1)
        settings = await _settings_map()
        delay_seconds = _gc_retry_delay_seconds(settings, attempts, "transient")
    next_run_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"
    if delay_seconds:
        next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{int(delay_seconds)} seconds')"
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            f"""
            UPDATE gc_fields_write_jobs
            SET status=?, last_error=?, result_json=?, next_run_at={next_run_expr}, updated_at=?
            WHERE id=?
            """,
            (_clean(status, 50), _clean(error, 2000), json.dumps(result or {}, ensure_ascii=False), _now(), int(job_id)),
        )
        await db.commit()


async def _defer_gc_fields_write_job(job_id: int, error: str = "", delay_seconds: int = 600, result: dict[str, Any] | None = None) -> None:
    delay_seconds = max(60, min(7200, int(delay_seconds or 600)))
    next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{delay_seconds} seconds')"
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            f"""
            UPDATE gc_fields_write_jobs
            SET status='pending',
                attempts=CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
                last_error=?,
                result_json=?,
                next_run_at={next_run_expr},
                updated_at=?
            WHERE id=?
            """,
            (_clean(error, 2000), json.dumps(result or {}, ensure_ascii=False), _now(), int(job_id)),
        )
        await db.commit()


async def _process_gc_fields_write_job(job: dict[str, Any], settings: dict[str, str]) -> None:
    budget_left = await _gc_export_budget_left(settings)
    reserve = _gc_new_job_reserve(settings)
    if budget_left < 2 + reserve:
        await _defer_gc_fields_write_job(
            int(job["id"]),
            "GetCourse API budget reserved for new jobs; deferred" if reserve else "GetCourse API budget low; deferred",
            delay_seconds=900,
        )
        return
    payload = _json_dict(job.get("payload_json"))
    fields = {key: value for key, value in _json_dict(payload.get("fields")).items() if _clean(value)}
    user_fields = {key: value for key, value in _json_dict(payload.get("user_fields")).items() if _clean(value)}
    required_names = {settings["field_stream"], settings["field_vk"], settings["field_tg"], settings["field_curator"]}
    if not required_names.issubset(set(fields)):
        await _finish_gc_fields_write_job(int(job["id"]), "skipped", "required non-empty fields are missing", {"fields": fields})
        return
    source_order = await _gc_fields_source_order(_clean(job.get("order_id"), 100))
    source_entitlement = _chat_entitlement(source_order) if source_order else {}
    if source_order and not source_entitlement.get("eligible"):
        await _finish_gc_fields_write_job(
            int(job["id"]),
            "skipped",
            "source order is not entitled to chat",
            {
                "status": _clean(source_order.get("status"), 100),
                "payment_state": _clean(source_order.get("payment_state"), 100),
                "order_id": _clean(job.get("order_id"), 100),
                "entitlement": source_entitlement,
            },
        )
        return
    gc_user_id = _clean(source_order.get("gc_user_id") or job.get("gc_user_id"), 100)
    deal_number = _clean(
        source_order.get("number")
        or source_order.get("deal_number")
        or source_order.get("order_number")
        or job.get("deal_number"),
        100,
    )
    email = _clean(source_order.get("email") or job.get("email"), 300)
    phone = _clean(source_order.get("phone") or source_order.get("user_phone"), 100)
    previous_error = _norm(job.get("last_error"))
    previous_result = _json_dict(job.get("result_json"))
    previous_user_id = _clean(
        _json_dict(_json_dict(_json_dict(previous_result.get("user")).get("response")).get("result")).get("user_id"),
        100,
    )
    identity_conflict = bool(
        (previous_user_id and previous_user_id != gc_user_id)
        or "обязательные поля email или phone" in previous_error
        or "ни эл. адрес, ни телефон" in previous_error
    )
    identity_resolution: dict[str, Any] = {}
    if identity_conflict:
        if await _gc_export_budget_left(settings) < 3 + reserve:
            await _defer_gc_fields_write_job(
                int(job["id"]),
                "GetCourse API budget low; identity resolution deferred",
                delay_seconds=900,
            )
            return
        resolved_email, identity_resolution, identity_error = await _resolve_getcourse_email_for_user(
            gc_user_id, email, phone, settings
        )
        if not resolved_email:
            await _defer_gc_fields_write_job(
                int(job["id"]),
                f"GetCourse identity resolution failed: {identity_error}",
                delay_seconds=900,
                result={"identity_resolution": identity_resolution},
            )
            return
        email = resolved_email
    user_ok, user_error, user_details = await _write_getcourse_user(gc_user_id, user_fields, settings, email=email, phone=phone)
    if user_error and "лимит GetCourse API" in user_error:
        await _defer_gc_fields_write_job(int(job["id"]), user_error, delay_seconds=600, result={"user": user_details})
        return
    deal_ok, deal_error, deal_details = await _write_getcourse_deal(gc_user_id, deal_number, fields, settings, email=email, phone=phone)
    if deal_error and "лимит GetCourse API" in deal_error:
        await _defer_gc_fields_write_job(int(job["id"]), deal_error, delay_seconds=600, result={"user": user_details, "deal": deal_details})
        return
    ok = bool(user_ok and deal_ok)
    error = "; ".join(part for part in [user_error and f"user: {user_error}", deal_error and f"deal: {deal_error}"] if part)
    classification = _gc_error_classification(error)
    result = {
        "user": user_details,
        "deal": deal_details,
        "fields": fields,
        "user_fields": user_fields,
        "identity_resolution": identity_resolution,
        "retry": {
            "classification": classification,
            "reserved_requests": _gc_new_job_reserve(settings),
        },
    }
    result["customer_sync"] = await _sync_gc_fields_write_customer_state(
        job,
        fields,
        _json_dict(payload.get("flow")),
        getcourse_ok=ok,
        error=error,
    )
    await _finish_gc_fields_write_job(
        int(job["id"]),
        "completed" if ok else ("quarantined" if classification == "terminal" else "failed"),
        error,
        result,
    )


async def _gc_write_loop() -> None:
    await asyncio.sleep(25)
    while True:
        sleep_seconds = 60
        try:
            settings = await _settings_map()
            sleep_seconds = _bounded_int(settings.get("gc_fields_write_worker_interval_seconds"), 10, 3600, 60)
            if not _truthy(settings.get("gc_fields_write_enabled")):
                await asyncio.sleep(sleep_seconds)
                continue
            async with _gc_write_lock, _gc_lookup_lock:
                exhausted = await _mark_exhausted_gc_fields_write_jobs(settings)
                if exhausted:
                    _log("warning", "gc fields write marked %s exhausted jobs", exhausted)
                budget_left = await _gc_export_budget_left(settings)
                if budget_left < 2:
                    await asyncio.sleep(sleep_seconds)
                    continue
                job = await _claim_gc_fields_write_job(settings)
                if job:
                    await _process_gc_fields_write_job(job, settings)
                else:
                    result = await _enqueue_gc_fields_reconciliation_jobs(settings, limit=20)
                    if int(result.get("queued") or 0) > 0:
                        _log("info", "gc fields reconciliation queued %s jobs", result.get("queued"))
                    else:
                        result = await _enqueue_gc_fields_write_jobs(settings, limit=20)
                    if int(result.get("queued") or 0) > 0:
                        _log("info", "gc fields write auto-enqueued %s jobs", result.get("queued"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "gc fields write worker failed: %s", exc)
        await asyncio.sleep(sleep_seconds)


async def _mark_processed(data: dict[str, Any]) -> None:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO processed_orders(
                source_record_id,platform_id,order_id,gc_user_id,source_hash,status,course_key,tariff,
                stream,vk_link,tg_link,customer_ok,getcourse_ok,error,details_json,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_record_id) DO UPDATE SET
                platform_id=excluded.platform_id,
                order_id=excluded.order_id,
                gc_user_id=excluded.gc_user_id,
                source_hash=excluded.source_hash,
                status=excluded.status,
                course_key=excluded.course_key,
                tariff=excluded.tariff,
                stream=excluded.stream,
                vk_link=excluded.vk_link,
                tg_link=excluded.tg_link,
                customer_ok=excluded.customer_ok,
                getcourse_ok=excluded.getcourse_ok,
                error=excluded.error,
                details_json=excluded.details_json,
                updated_at=excluded.updated_at
            """,
            (
                int(data["source_record_id"]),
                _clean(data.get("platform_id"), 100),
                _clean(data.get("order_id"), 100),
                _clean(data.get("gc_user_id"), 100),
                _clean(data.get("source_hash"), 100),
                _clean(data.get("status"), 50),
                _clean(data.get("course_key"), 50),
                _clean(data.get("tariff"), 50),
                _clean(data.get("stream"), 100),
                _clean(data.get("vk_link"), 2000),
                _clean(data.get("tg_link"), 2000),
                1 if data.get("customer_ok") else 0,
                1 if data.get("getcourse_ok") else 0,
                _clean(data.get("error"), 2000),
                json.dumps(data.get("details") or {}, ensure_ascii=False),
                _now(),
            ),
        )
        await db.commit()


async def _process_row(
    row: dict[str, Any],
    chats: dict[str, dict[str, dict[str, Any] | None]],
    settings: dict[str, str],
    state: dict[str, Any] | None,
    force: bool = False,
    student_snapshot: dict[str, Any] | None = None,
    flow_catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fields = _json_dict(row.get("custom_fields"))
    source_hash = _source_hash(fields, settings)
    gc_ready = bool(_env()["account_name"] and _env()["api_token"])
    if not force and _should_skip_state(state, source_hash, settings, gc_ready):
        return {"action": "skipped_state"}
    base = {
        "source_record_id": int(row["id"]),
        "platform_id": _clean(row.get("platform_id"), 100),
        "order_id": _clean(fields.get("order_id") or row.get("platform_id"), 100),
        "gc_user_id": _clean(fields.get("gc_user_id"), 100),
        "source_hash": source_hash,
    }
    deal_number = _clean(fields.get("number") or fields.get("deal_number") or fields.get("order_number") or base["order_id"], 100)
    entitlement = _chat_entitlement(fields)
    course_key = _clean(entitlement.get("course_key"), 50)
    tariff = _clean(entitlement.get("tariff"), 50)
    if not entitlement.get("eligible"):
        await _mark_processed({
            **base,
            "status": "skipped",
            "course_key": course_key,
            "tariff": tariff,
            "error": _clean(entitlement.get("reason") or "order is not entitled to chat", 2000),
            "details": {
                "entitlement": entitlement,
                "status": fields.get("status"),
                "payment_state": fields.get("payment_state"),
            },
        })
        return {"action": "skipped"}
    if not course_key:
        await _mark_processed({
            **base,
            "status": "quarantined",
            "error": "course not detected",
            "details": {
                "title": fields.get("title"),
                "entitlement": entitlement,
                "retry": {
                    "classification": "terminal",
                    "attempts": 1,
                    "delay_seconds": 0,
                    "next_retry_at": "",
                    "last_error": "course not detected",
                    "retry_when": "source_hash_changed",
                },
            },
        })
        return {"action": "quarantined"}
    student_match = _student_flow_match(student_snapshot, row, fields, course_key)
    student_flow: dict[str, Any] | None = student_match.get("flow") if student_match else None
    student_item: dict[str, Any] = student_match.get("student") if student_match else {}
    course_chats = chats.get(course_key) or {}
    if student_flow:
        stream = _clean(student_flow.get("stream"), 100)
        sheet_vk_chat = {
            "platform": "vk",
            "title": _clean(student_flow.get("vk_title"), 300),
            "stream_number": stream,
            "link": _clean(student_flow.get("vk_link"), 2000),
            "course_key": course_key,
            "source": "flow_students_sheet",
        }
        sheet_tg_chat = {
            "platform": "telegram",
            "title": _clean(student_flow.get("tg_title"), 300),
            "stream_number": stream,
            "link": _clean(student_flow.get("tg_link"), 2000),
            "course_key": course_key,
            "source": "flow_students_sheet",
        }
        vk_chat = _prefer_sheet_chat(sheet_vk_chat, course_chats.get("vk"), stream)
        tg_chat = _prefer_sheet_chat(sheet_tg_chat, course_chats.get("telegram"), stream)
    else:
        dated_flow = _dated_flow_for_order(flow_catalog or {}, course_key, fields)
        if not dated_flow:
            await _mark_processed({
                **base,
                "status": "pending_flow_date",
                "course_key": course_key,
                "tariff": tariff,
                "error": "flow not found for payment date",
                "details": {"payment_date": _business_order_date_text(fields), "entitlement": entitlement},
            })
            return {"action": "pending_flow_date"}
        stream = _clean(dated_flow.get("stream"), 100)
        vk_chat = {"platform": "vk", "title": _clean(dated_flow.get("vk_title"), 300), "stream_number": stream, "link": _clean(dated_flow.get("vk_link"), 2000), "course_key": course_key, "source": "chat_links_sheet_dated"}
        tg_chat = {"platform": "telegram", "title": _clean(dated_flow.get("tg_title"), 300), "stream_number": stream, "link": _clean(dated_flow.get("tg_link"), 2000), "course_key": course_key, "source": "chat_links_sheet_dated"}
    curator_value = _clean(student_item.get("responsible_curator") or (student_flow or {}).get("curator_value"), 100)
    if curator_value:
        curator = {
            "ok": True,
            "status": "ok",
            "value": curator_value,
            "raw_value": _clean(student_item.get("responsible_curator_raw") or (student_flow or {}).get("curator_raw"), 300),
            "worksheet_title": _clean((student_flow or {}).get("sheet_title"), 300),
            "sheet_id": (student_flow or {}).get("sheet_id") or "",
            "url": _clean((student_flow or {}).get("sheet_url"), 1000),
            "source": "flow_students_sheet",
        }
    else:
        curator = await _resolve_curator(course_key, stream, settings)
    if not curator.get("ok"):
        await _mark_processed({
            **base,
            "status": "pending_curator",
            "course_key": course_key,
            "tariff": tariff,
            "stream": stream,
            "error": _clean(curator.get("error") or curator.get("status"), 2000),
            "details": {"latest_chats": {"vk": vk_chat, "telegram": tg_chat}, "curator": curator, "student_flow_match": student_match or {}, "entitlement": entitlement},
        })
        return {"action": "pending_curator", "error": curator.get("error")}
    if _clean((vk_chat or {}).get("link"), 2000) and _clean((tg_chat or {}).get("link"), 2000):
        link_result: dict[str, Any] = {
            "ok": True,
            "status": "ok",
            "vk": {
                "course_key": course_key,
                "platform": "vk",
                "title": _clean((vk_chat or {}).get("title"), 300),
                "stream_number": stream,
                "link": _clean((vk_chat or {}).get("link"), 2000),
                "source": _clean((vk_chat or {}).get("source"), 100),
            },
            "telegram": {
                "course_key": course_key,
                "platform": "telegram",
                "title": _clean((tg_chat or {}).get("title"), 300),
                "stream_number": stream,
                "link": _clean((tg_chat or {}).get("link"), 2000),
                "source": _clean((tg_chat or {}).get("source"), 100),
            },
            "error": "",
        }
    else:
        link_result = await _resolve_chat_links(course_key, stream, settings)
    if not link_result.get("ok"):
        await _mark_processed({
            **base,
            "status": "pending_chat_links",
            "course_key": course_key,
            "tariff": tariff,
            "stream": stream,
            "error": _clean(link_result.get("error") or link_result.get("status"), 2000),
            "details": {
                "latest_chats": {"vk": vk_chat, "telegram": tg_chat},
                "curator": curator,
                "chat_links": link_result,
                "student_flow_match": student_match or {},
                "entitlement": entitlement,
            },
        })
        return {"action": "pending_chat_links", "error": link_result.get("error")}
    output_fields = {
        settings["field_stream"]: stream,
        settings["field_vk"]: _clean((link_result.get("vk") or {}).get("link"), 2000),
        settings["field_tg"]: _clean((link_result.get("telegram") or {}).get("link"), 2000),
        settings["field_curator"]: curator["value"],
    }
    patch = {
        **output_fields,
        f"{MACHINE_PREFIX}course_key": course_key,
        f"{MACHINE_PREFIX}tariff": tariff,
        f"{MACHINE_PREFIX}curator_raw": _clean(curator.get("raw_value"), 300),
        f"{MACHINE_PREFIX}curator_sheet": _clean(curator.get("worksheet_title"), 300),
        f"{MACHINE_PREFIX}links_source": "+".join(
            sorted(
                {
                    _clean((link_result.get(platform) or {}).get("source"), 100)
                    for platform in ("vk", "telegram")
                    if _clean((link_result.get(platform) or {}).get("source"), 100)
                }
            )
        ),
        f"{MACHINE_PREFIX}vk_link_title": _clean((link_result.get("vk") or {}).get("title"), 300),
        f"{MACHINE_PREFIX}tg_link_title": _clean((link_result.get("telegram") or {}).get("title"), 300),
        f"{MACHINE_PREFIX}standard_no_links": False,
        f"{MACHINE_PREFIX}source_record_id": int(row["id"]),
        f"{MACHINE_PREFIX}updated_at": _now(),
    }
    details: dict[str, Any] = {
        "output_fields": output_fields,
        "getcourse_user_field_ids": {
            "stream": _clean(settings.get("user_field_stream_id"), 100),
            "vk": _clean(settings.get("user_field_vk_id"), 100),
            "tg": _clean(settings.get("user_field_tg_id"), 100),
            "curator": _clean(settings.get("user_field_curator_id"), 100),
        },
        "latest_chats": {"vk": vk_chat, "telegram": tg_chat},
        "student_flow_match": student_match or {},
        "curator": curator,
        "chat_links": link_result,
        "entitlement": entitlement,
    }
    await _update_customer_fields(int(row["id"]), fields, patch)
    dry_run = _truthy(settings.get("dry_run"))
    getcourse_ok = False
    error = ""
    status = "processed"
    if dry_run:
        status = "dry_run"
        details["getcourse_user_payload"] = _getcourse_user_payload(
            base["gc_user_id"],
            _getcourse_user_addfields(output_fields, settings),
            email=_clean(fields.get("email") or fields.get("user_email"), 300),
            phone=_clean(fields.get("phone") or fields.get("user_phone"), 100),
        )
        details["getcourse_deal_payload"] = _getcourse_deal_payload(
            base["gc_user_id"],
            deal_number,
            output_fields,
            email=_clean(fields.get("email") or fields.get("user_email"), 300),
            phone=_clean(fields.get("phone") or fields.get("user_phone"), 100),
        )
        details["dry_run"] = True
    else:
        is_retry = bool(
            state
            and state.get("source_hash") == source_hash
            and state.get("status") == "customer_only"
        )
        reserve = _gc_new_job_reserve(settings) if is_retry else 0
        budget_left = await _gc_export_budget_left(settings)
        if budget_left < 2 + reserve:
            budget_error = (
                "GetCourse API budget reserved for new jobs; deferred"
                if reserve
                else "лимит GetCourse API для модуля исчерпан"
            )
            user_ok, user_error, user_details = False, budget_error, {}
            deal_ok, deal_error, deal_details = False, budget_error, {}
        else:
            user_ok, user_error, user_details = await _write_getcourse_user(
                base["gc_user_id"],
                _getcourse_user_addfields(output_fields, settings),
                settings,
                email=_clean(fields.get("email") or fields.get("user_email"), 300),
                phone=_clean(fields.get("phone") or fields.get("user_phone"), 100),
            )
            deal_ok, deal_error, deal_details = await _write_getcourse_deal(
                base["gc_user_id"],
                deal_number,
                output_fields,
                settings,
                email=_clean(fields.get("email") or fields.get("user_email"), 300),
                phone=_clean(fields.get("phone") or fields.get("user_phone"), 100),
            )
        getcourse_ok = bool(user_ok and deal_ok)
        error = "; ".join(part for part in [user_error and f"user: {user_error}", deal_error and f"deal: {deal_error}"] if part)
        details["getcourse_user"] = user_details
        details["getcourse_deal"] = deal_details
        details["deal_number"] = deal_number
        if not getcourse_ok:
            retry_state = state if state and state.get("source_hash") == source_hash else None
            details["retry"] = _gc_retry_metadata(error, retry_state, settings)
            status = "quarantined" if details["retry"]["classification"] == "terminal" else "customer_only"
    await _mark_processed({
        **base,
        "status": status,
        "course_key": course_key,
        "tariff": tariff,
        "stream": stream,
        "vk_link": output_fields[settings["field_vk"]],
        "tg_link": output_fields[settings["field_tg"]],
        "customer_ok": True,
        "getcourse_ok": getcourse_ok,
        "error": error,
        "details": details,
    })
    return {"action": status, "error": error}


async def _scan_once(*, force_failed: bool = False, limit: int = 200) -> dict[str, Any]:
    async with _scan_lock:
        settings = await _settings_map()
        dry_run = _truthy(settings.get("dry_run"))
        run_id = await _create_scan_run(dry_run)
        summary = {"ok": True, "source_rows": 0, "processed": 0, "skipped": 0, "failed": 0, "dry_run": dry_run}
        try:
            rows = await _customer_rows(settings, limit)
            summary["source_rows"] = len(rows)
            states = await _processed_state([int(row["id"]) for row in rows])
            flow_catalog = await _chat_flows(settings) if rows else {"items": [], "errors": [], "ok": True}
            chats = _active_chats_from_flows(flow_catalog)
            summary["active_streams"] = {
                course_key: _clean(((pair or {}).get("vk") or {}).get("stream_number"), 100)
                for course_key, pair in chats.items()
            }
            if flow_catalog.get("errors"):
                summary["chat_link_errors"] = flow_catalog.get("errors")
            student_snapshot = await _flow_students_for_processing(settings) if rows else None
            if student_snapshot:
                summary["students_cache_updated_at"] = _clean(student_snapshot.get("cache_updated_at") or student_snapshot.get("updated_at"), 40)
                summary["students_cache_age_seconds"] = int(student_snapshot.get("cache_age_seconds") or 0)
            for row in rows:
                result = await _process_row(row, chats, settings, states.get(int(row["id"])), force=force_failed, student_snapshot=student_snapshot, flow_catalog=flow_catalog)
                action = result.get("action")
                if action in {"processed", "dry_run", "customer_only"}:
                    summary["processed"] += 1
                elif action in {"skipped", "skipped_state", "pending_curator", "pending_chat_links", "pending_flow_date"}:
                    summary["skipped"] += 1
                elif action in {"failed", "quarantined"}:
                    summary["failed"] += 1
            await _finish_scan_run(run_id, summary)
            return summary
        except Exception as exc:
            summary["ok"] = False
            summary["error"] = str(exc)
            await _finish_scan_run(run_id, summary)
            _log("error", "scan failed: %s", exc, exc_info=True)
            return summary


async def _create_scan_run(dry_run: bool) -> int:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        cur = await db.execute("INSERT INTO scan_runs(dry_run) VALUES(?)", (1 if dry_run else 0,))
        await db.commit()
        return int(cur.lastrowid)


async def _finish_scan_run(run_id: int, summary: dict[str, Any]) -> None:
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        await db.execute(
            """
            UPDATE scan_runs
            SET finished_at=?, source_rows=?, processed=?, skipped=?, failed=?, error=?, details_json=?
            WHERE id=?
            """,
            (
                _now(),
                int(summary.get("source_rows") or 0),
                int(summary.get("processed") or 0),
                int(summary.get("skipped") or 0),
                int(summary.get("failed") or 0),
                _clean(summary.get("error"), 2000),
                json.dumps(summary, ensure_ascii=False),
                run_id,
            ),
        )
        await db.commit()


async def _poll_loop() -> None:
    await asyncio.sleep(8)
    while True:
        sleep_seconds = 60
        try:
            settings = await _settings_map()
            sleep_seconds = _bounded_int(settings.get("poll_seconds"), 10, 3600, 60)
            if _truthy(settings.get("enabled")):
                await _scan_once(limit=200)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "poll loop failed: %s", exc)
        await asyncio.sleep(sleep_seconds)


async def service_transfer_snapshot(*, refresh: bool = False) -> dict[str, Any]:
    settings = await _settings_map()
    data = await _flow_students(settings, refresh=refresh)
    if data.get("needs_refresh") and not refresh:
        data = await _flow_students(settings, refresh=True)
    return data


async def service_flow_catalog() -> dict[str, Any]:
    """Return chat-link flows without reading the legacy student registry."""

    settings = await _settings_map()
    cache_key = _flow_students_cache_key(settings)
    now_monotonic = time.monotonic()
    fallback = _flow_catalog_fallback_cache.get("data")
    if (
        fallback
        and _flow_catalog_fallback_cache.get("key") == cache_key
        and float(_flow_catalog_fallback_cache.get("expires") or 0) > now_monotonic
    ):
        return dict(fallback)
    data = await _chat_flows(settings)
    if data.get("ok") and data.get("items"):
        _flow_catalog_fallback_cache.update({"key": "", "expires": 0.0, "data": None})
        return {**data, "stale": bool(data.get("stale")), "source": data.get("source") or "google_sheets"}
    cached = await _load_flow_students_cache(cache_key, 1, allow_stale=True)
    if not cached or not cached.get("items"):
        return {**data, "stale": False, "source": "unavailable"}
    items = [dict(item) for item in cached["items"] if isinstance(item, dict)]
    for item in items:
        if not _clean(item.get("curator_sheet"), 300):
            item["curator_sheet"] = _clean(item.get("sheet_title"), 300)
    _add_flow_start_dates(items)
    result = {
        "ok": True,
        "stale": True,
        "source": "flow_students_cache",
        "items": items,
        "errors": data.get("errors") or [],
    }
    _flow_catalog_fallback_cache.update({
        "key": cache_key,
        "expires": now_monotonic + 300,
        "data": result,
    })
    return dict(result)


async def service_resolve_onboarding_flow(*, course_key: str, paid_at: str) -> dict[str, Any]:
    """Resolve one dated onboarding flow with a persistent-cache fallback."""

    normalized_course = _clean(course_key, 50)
    normalized_paid_at = _clean(paid_at, 100)
    if normalized_course not in {"puppy", "dog"} or not _business_order_date({"paid_at": normalized_paid_at}):
        return {
            "ok": False,
            "status": "invalid",
            "flow": {},
            "stale": False,
            "source": "",
            "errors": [],
        }
    catalog = await service_flow_catalog()
    flow = _dated_flow_for_order(catalog, normalized_course, {"paid_at": normalized_paid_at}) or {}
    return {
        "ok": bool(flow),
        "status": "resolved" if flow else "not_found",
        "flow": {
            "stream": _clean(flow.get("stream"), 50),
            "date_start": _clean(flow.get("date_start"), 50),
            "vk_link": _clean(flow.get("vk_link"), 2000),
            "tg_link": _clean(flow.get("tg_link"), 2000),
        } if flow else {},
        "stale": bool(catalog.get("stale")),
        "source": _clean(catalog.get("source"), 100),
        "errors": catalog.get("errors") or [],
    }


async def service_entitled_orders(
    *, after_source_record_id: int = 0, after_updated_at: str = "", limit: int = 1000
) -> dict[str, Any]:
    """Return new customer-db orders using the module's existing entitlement rules."""

    db_path = _customer_db_path()
    if not db_path.exists():
        return {"ok": False, "items": [], "cursor": int(after_source_record_id), "max_source_record_id": 0, "error": "customer-db not found"}
    bounded_limit = max(1, min(5000, int(limit)))
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        max_row = await (
            await db.execute(
                "SELECT id,COALESCE(updated_at,created_at) FROM cdb_getcourse_orders ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC LIMIT 1"
            )
        ).fetchone()
        max_source_record_id = int((await (await db.execute("SELECT COALESCE(MAX(id),0) FROM cdb_getcourse_orders")).fetchone())[0])
        if after_updated_at:
            where = "datetime(COALESCE(updated_at,created_at))>datetime(?) OR (COALESCE(updated_at,created_at)=? AND id>?)"
            params = (_clean(after_updated_at, 100), _clean(after_updated_at, 100), max(0, int(after_source_record_id)), bounded_limit)
            order_by = "datetime(COALESCE(updated_at,created_at)),id"
        else:
            where = "id>?"
            params = (max(0, int(after_source_record_id)), bounded_limit)
            order_by = "id"
        rows = [
            dict(row)
            for row in await (
                await db.execute(
                    """
                    SELECT id,platform_id,custom_fields,created_at,updated_at
                    FROM cdb_getcourse_orders
                    WHERE {where}
                    ORDER BY {order_by}
                    LIMIT ?
                    """.format(where=where, order_by=order_by),
                    params,
                )
            ).fetchall()
        ]
    items: list[dict[str, Any]] = []
    for row in rows:
        fields = _json_dict(row.get("custom_fields"))
        entitlement = _chat_entitlement(fields)
        if not entitlement.get("eligible"):
            continue
        email = _clean(fields.get("email") or fields.get("user_email"), 300)
        if not _valid_email(email):
            continue
        course_key = _clean(entitlement.get("course_key"), 50)
        tariff = _clean(entitlement.get("tariff"), 100)
        order_id = _clean(fields.get("order_id") or row.get("platform_id"), 100)
        autopayment, autopayment_source = _autopayment_match(fields)
        items.append(
            {
                "source_record_id": int(row.get("id") or 0),
                "order_id": order_id,
                "deal_number": _clean(fields.get("number") or fields.get("deal_number") or fields.get("order_number") or order_id, 100),
                "gc_user_id": _clean(fields.get("gc_user_id"), 100),
                "name": _clean(fields.get("name") or fields.get("user_name") or fields.get("full_name") or fields.get("fio"), 300),
                "email": email,
                "phone": _clean(fields.get("phone") or fields.get("user_phone"), 100),
                "tg_account": _clean(fields.get("tg_account") or fields.get("telegram") or fields.get("user_telegram"), 500),
                "date": _business_order_date_text(fields),
                "course_key": course_key,
                "course": "Щенок" if course_key == "puppy" else "Собака",
                "tariff": tariff.upper() if tariff == "vip" else tariff.capitalize(),
                "status": _clean(fields.get("status"), 100),
                "payment_state": _clean(fields.get("payment_state"), 100),
                "total_amount": _money_value(fields.get("cost_money") or fields.get("costMoney")),
                "remaining_amount": _money_value(fields.get("payed_money") or fields.get("payedMoney")),
                "refund_amount": max(
                    0.0,
                    _money_value(fields.get("cost_money") or fields.get("costMoney"))
                    - _money_value(fields.get("payed_money") or fields.get("payedMoney")),
                ),
                "created_at": _clean(row.get("created_at"), 100),
                "updated_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
                "entitlement": entitlement,
            }
        )
    return {
        "ok": True,
        "items": items,
        "cursor": int(rows[-1]["id"]) if rows else int(after_source_record_id),
        "cursor_updated_at": _clean((rows[-1] if rows else {}).get("updated_at") or (rows[-1] if rows else {}).get("created_at") or after_updated_at, 100),
        "max_source_record_id": max_source_record_id,
        "max_updated_id": int((max_row or [0, ""])[0] or 0),
        "max_updated_at": _clean((max_row or [0, ""])[1], 100),
        "has_more": bool(rows and (
            _clean(rows[-1].get("updated_at") or rows[-1].get("created_at"), 100), int(rows[-1]["id"])
        ) < (_clean((max_row or [0, ""])[1], 100), int((max_row or [0, ""])[0] or 0))),
    }


async def service_paid_course_orders(
    *, after_source_record_id: int = 0, after_updated_at: str = "", limit: int = 1000
) -> dict[str, Any]:
    """Return paid or partially paid core-course orders for onboarding.

    Standard packages are included. A positive partial payment is enough to
    start onboarding; refunds and cancelled orders remain excluded.
    """

    db_path = _customer_db_path()
    if not db_path.exists():
        return {
            "ok": False,
            "items": [],
            "cursor": int(after_source_record_id),
            "cursor_updated_at": _clean(after_updated_at, 100),
            "max_source_record_id": 0,
            "error": "customer-db not found",
        }
    bounded_limit = max(1, min(5000, int(limit)))
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        max_row = await (
            await db.execute(
                "SELECT id,COALESCE(updated_at,created_at) FROM cdb_getcourse_orders "
                "ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC LIMIT 1"
            )
        ).fetchone()
        max_source_record_id = int(
            (await (await db.execute("SELECT COALESCE(MAX(id),0) FROM cdb_getcourse_orders")).fetchone())[0]
        )
        if after_updated_at:
            where = (
                "datetime(COALESCE(updated_at,created_at))>datetime(?) OR "
                "(COALESCE(updated_at,created_at)=? AND id>?)"
            )
            params = (
                _clean(after_updated_at, 100),
                _clean(after_updated_at, 100),
                max(0, int(after_source_record_id)),
                bounded_limit,
            )
            order_by = "datetime(COALESCE(updated_at,created_at)),id"
        else:
            where = "id>?"
            params = (max(0, int(after_source_record_id)), bounded_limit)
            order_by = "id"
        rows = [
            dict(row)
            for row in await (
                await db.execute(
                    """
                    SELECT id,platform_id,custom_fields,created_at,updated_at
                    FROM cdb_getcourse_orders
                    WHERE {where}
                    ORDER BY {order_by}
                    LIMIT ?
                    """.format(where=where, order_by=order_by),
                    params,
                )
            ).fetchall()
        ]

    flow_catalog = await service_flow_catalog()
    items: list[dict[str, Any]] = []
    for row in rows:
        fields = _json_dict(row.get("custom_fields"))
        if not _is_onboarding_paid(fields):
            continue
        product = _chat_product_entitlement(fields)
        tariff = _classify_tariff(fields)
        course_key = _clean(product.get("course_key"), 50)
        product_kind = _clean(product.get("product_kind"), 50)
        if tariff not in {"standard", "premium", "vip"}:
            continue
        if course_key not in {"puppy", "dog"} or product_kind in {"excluded", "module", "unknown"}:
            continue
        email = _clean(fields.get("email") or fields.get("user_email"), 300)
        paid_at = _business_order_date_text(fields) or _clean(row.get("updated_at") or row.get("created_at"), 100)
        exact_flow = _assigned_flow_for_order(flow_catalog, course_key, fields)
        flow = exact_flow or _dated_flow_for_order(flow_catalog, course_key, {"paid_at": paid_at}) or {}
        order_id = _clean(fields.get("order_id") or row.get("platform_id"), 100)
        autopayment, autopayment_source = _autopayment_match(fields)
        items.append(
            {
                "source_record_id": int(row.get("id") or 0),
                "order_id": order_id,
                "deal_number": _clean(
                    fields.get("number") or fields.get("deal_number") or fields.get("order_number") or order_id,
                    100,
                ),
                "gc_user_id": _clean(fields.get("gc_user_id"), 100),
                "name": _clean(
                    fields.get("name") or fields.get("user_name") or fields.get("full_name") or fields.get("fio"),
                    300,
                ),
                "email": email,
                "phone": _clean(fields.get("phone") or fields.get("user_phone"), 100),
                "paid_at": paid_at,
                "course_key": course_key,
                "course": "Щенок" if course_key == "puppy" else "Собака",
                "tariff": tariff,
                "product_kind": product_kind,
                "payment_state": _clean(fields.get("payment_state"), 40),
                "autopayment": autopayment,
                "autopayment_source": autopayment_source,
                "manager_name": _clean(fields.get("manager_name") or fields.get("manager"), 300),
                "utm_term": _clean(fields.get("utm_term") or fields.get("order_utm_term"), 1000),
                "flow": {
                    "stream": _clean(flow.get("stream"), 50),
                    "date_start": _clean(flow.get("date_start"), 50),
                    "vk_link": _clean(flow.get("vk_link"), 2000),
                    "tg_link": _clean(flow.get("tg_link"), 2000),
                },
                "flow_source": "exact_order_assignment" if exact_flow else "payment_date",
                "created_at": _clean(row.get("created_at"), 100),
                "updated_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
            }
        )
    tail = rows[-1] if rows else {}
    return {
        "ok": True,
        "items": items,
        "cursor": int(tail.get("id") or after_source_record_id),
        "cursor_updated_at": _clean(
            tail.get("updated_at") or tail.get("created_at") or after_updated_at,
            100,
        ),
        "max_source_record_id": max_source_record_id,
        "max_updated_id": int((max_row or [0, ""])[0] or 0),
        "max_updated_at": _clean((max_row or [0, ""])[1], 100),
        "has_more": bool(
            rows
            and (
                _clean(rows[-1].get("updated_at") or rows[-1].get("created_at"), 100),
                int(rows[-1]["id"]),
            )
            < (_clean((max_row or [0, ""])[1], 100), int((max_row or [0, ""])[0] or 0))
        ),
        "flow_errors": flow_catalog.get("errors") or [],
        "flow_stale": bool(flow_catalog.get("stale")),
        "flow_source": _clean(flow_catalog.get("source"), 100),
        "flow_items": len(flow_catalog.get("items") or []),
    }


def _upgrade_course_key(fields: dict[str, Any]) -> str:
    """Classify the three separately sold upgrade families."""

    text = _norm(
        " ".join(
            _flatten_text(fields.get(key))
            for key in ("title", "positions", "offer_tags", "offers", "product_title")
        )
    )
    has_puppy = "первые шаги к воспитанию" in text or "щенок" in text
    has_dog = "послушная собака" in text or "современный собаковод" in text
    combo = bool(
        (has_puppy and has_dog)
        or re.search(r"(?:^|[|\s])щ\s*\+\s*с(?:$|[|\s])", text)
        or "щенок + собака" in text
    )
    if combo:
        return "combo"
    if has_puppy:
        return "puppy"
    if has_dog:
        return "dog"
    return ""


def _upgrade_offer_id(fields: dict[str, Any]) -> str:
    for key in ("offer_id", "position_offer_id", "deal_offer_id", "offerId"):
        value = fields.get(key)
        if isinstance(value, dict):
            value = value.get("id")
        text = _clean(value, 100)
        match = re.search(r"\d+", text)
        if match:
            return match.group(0)
    offers = fields.get("offers")
    if isinstance(offers, list) and len(offers) == 1 and isinstance(offers[0], dict):
        return _clean(offers[0].get("id") or offers[0].get("offer_id"), 30)
    offers_text = _clean(offers, 100)
    if offers_text.isdigit():
        return offers_text
    return ""


def _upgrade_order_view(row: dict[str, Any], fields: dict[str, Any]) -> dict[str, Any]:
    order_id = _clean(fields.get("order_id") or row.get("platform_id"), 100)
    deal_number = _clean(
        fields.get("number") or fields.get("deal_number") or fields.get("order_number") or order_id,
        100,
    )
    autopayment, autopayment_source = _autopayment_match(fields)
    return {
        "source_record_id": int(row.get("id") or 0),
        "order_id": order_id,
        "deal_number": deal_number,
        "gc_user_id": _clean(fields.get("gc_user_id") or fields.get("user_id"), 100),
        "name": _clean(fields.get("name") or fields.get("user_name") or fields.get("full_name") or fields.get("fio"), 300),
        "email": _clean(fields.get("email") or fields.get("user_email"), 300),
        "phone": _clean(fields.get("phone") or fields.get("user_phone"), 100),
        "title": _clean(fields.get("title") or fields.get("order_name") or fields.get("positions"), 1000),
        "offer_id": _upgrade_offer_id(fields),
        "course_key": _upgrade_course_key(fields),
        "tariff": _classify_tariff(fields),
        "status": _clean(fields.get("status"), 100),
        "payment_state": _clean(fields.get("payment_state"), 40),
        "cost_money": _money_value(
            fields.get("cost_money") or fields.get("deal_cost") or fields.get("cost")
        ),
        "payed_money": _money_value(fields.get("payed_money") or fields.get("paid_money")),
        "left_cost_money": _money_value(fields.get("left_cost_money") or fields.get("left_cost")),
        "paid_at": _business_order_date_text(fields) or _clean(row.get("updated_at") or row.get("created_at"), 100),
        "autopayment": bool(autopayment),
        "autopayment_source": autopayment_source,
        "updated_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
    }


def _upgrade_order_active(fields: dict[str, Any]) -> bool:
    status = _norm(fields.get("status"))
    return not any(
        marker in status
        for marker in ("возврат", "отмен", "ложн", "refund", "cancel", "false")
    )


def _upgrade_order_fully_paid(fields: dict[str, Any]) -> bool:
    if _norm(fields.get("payment_state")) != "paid":
        return False
    cost = _money_value(fields.get("cost_money") or fields.get("deal_cost") or fields.get("cost"))
    paid = _money_value(fields.get("payed_money") or fields.get("paid_money"))
    left = _money_value(fields.get("left_cost_money") or fields.get("left_cost"))
    return cost > 0 and paid + 0.01 >= cost and left <= 0.01


def _upgrade_identity_matches(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_id = _clean(left.get("gc_user_id"), 100)
    right_id = _clean(right.get("gc_user_id"), 100)
    if left_id and right_id:
        return left_id == right_id
    left_email = _norm(left.get("email"))
    right_email = _norm(right.get("email"))
    return bool(left_email and right_email and left_email == right_email)


async def service_upgrade_candidates(
    *, after_source_record_id: int = 0, limit: int = 250
) -> dict[str, Any]:
    """Return paid surcharge orders and their possible earlier Standard order.

    This is a local customer-db read. Text matching only discovers candidates;
    the coordinator decides whether an exact configured surcharge offer may be
    applied automatically.
    """

    db_path = _customer_db_path()
    if not db_path.exists():
        return {"ok": False, "items": [], "cursor": int(after_source_record_id), "error": "customer-db not found"}
    bounded_limit = max(1, min(1000, int(limit or 250)))
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        max_source_record_id = int(
            (await (await db.execute("SELECT COALESCE(MAX(id),0) FROM cdb_getcourse_orders")).fetchone())[0]
        )
        candidate_rows = [
            dict(row)
            for row in await (
                await db.execute(
                    """SELECT id,platform_id,custom_fields,created_at,updated_at
                       FROM cdb_getcourse_orders WHERE id>? ORDER BY id LIMIT ?""",
                    (max(0, int(after_source_record_id)), bounded_limit),
                )
            ).fetchall()
        ]
        surcharge_views: list[dict[str, Any]] = []
        for row in candidate_rows:
            fields = _json_dict(row.get("custom_fields"))
            text = _norm(" ".join(_flatten_text(fields.get(key)) for key in ("title", "positions", "offers")))
            if (
                "доплата" in text
                and _upgrade_order_fully_paid(fields)
                and _upgrade_order_active(fields)
            ):
                surcharge_views.append(_upgrade_order_view(row, fields))
        identities = [item for item in surcharge_views if item.get("gc_user_id") or item.get("email")]
        all_rows: list[dict[str, Any]] = []
        if identities:
            clauses: list[str] = []
            params: list[Any] = []
            user_ids = sorted({_clean(item.get("gc_user_id"), 100) for item in identities if item.get("gc_user_id")})
            emails = sorted({_norm(item.get("email")) for item in identities if item.get("email")})
            if user_ids:
                clauses.append(
                    f"CAST(json_extract(custom_fields,'$.gc_user_id') AS TEXT) IN ({','.join('?' for _ in user_ids)})"
                )
                params.extend(user_ids)
            if emails:
                clauses.append(
                    f"lower(COALESCE(json_extract(custom_fields,'$.email'),json_extract(custom_fields,'$.user_email'))) IN ({','.join('?' for _ in emails)})"
                )
                params.extend(emails)
            if clauses:
                all_rows = [
                    dict(row)
                    for row in await (
                        await db.execute(
                            f"""SELECT id,platform_id,custom_fields,created_at,updated_at
                                FROM cdb_getcourse_orders
                                WHERE json_valid(custom_fields) AND ({' OR '.join(clauses)})
                                ORDER BY id""",
                            params,
                        )
                    ).fetchall()
                ]
    origins: list[dict[str, Any]] = []
    for row in all_rows:
        fields = _json_dict(row.get("custom_fields"))
        if (
            _classify_tariff(fields) == "standard"
            and _upgrade_order_fully_paid(fields)
            and _upgrade_order_active(fields)
        ):
            origins.append(_upgrade_order_view(row, fields))
    items: list[dict[str, Any]] = []
    for surcharge in surcharge_views:
        paid_at = _date_value(surcharge.get("paid_at"))
        matches = []
        for origin in origins:
            origin_paid_at = _date_value(origin.get("paid_at"))
            if not _upgrade_identity_matches(surcharge, origin):
                continue
            if surcharge.get("course_key") and origin.get("course_key") != surcharge.get("course_key"):
                continue
            if paid_at and origin_paid_at and origin_paid_at > paid_at:
                continue
            if origin.get("order_id") == surcharge.get("order_id"):
                continue
            matches.append(origin)
        matches.sort(key=lambda item: (item.get("paid_at") or "", int(item.get("source_record_id") or 0)), reverse=True)
        items.append({**surcharge, "origins": matches, "origin_count": len(matches)})
    tail = candidate_rows[-1] if candidate_rows else {}
    cursor = int(tail.get("id") or after_source_record_id)
    return {
        "ok": True,
        "items": items,
        "cursor": cursor,
        "max_source_record_id": max_source_record_id,
        "has_more": bool(candidate_rows and cursor < max_source_record_id),
    }


def _upgrade_export_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": _deal_id_from_export_row(row),
        "number": _flat_lookup(row, ("number", "deal_number", "Номер заказа", "Заказ")),
        "gc_user_id": _deal_user_id_from_export_row(row),
        "email": _email_from_export_row(row),
        "offer_id": _flat_lookup(row, ("offer_id", "ID предложения", "Предложение ID")),
        "title": _flat_lookup(row, ("title", "offer_title", "Предложение", "Состав заказа")),
        "status": _flat_lookup(row, ("status", "deal_status", "Статус")),
        "payment_state": _flat_lookup(row, ("payment_state", "Статус оплаты")),
        "cost_money": _flat_lookup(row, ("cost_money", "deal_cost", "Стоимость", "Сумма заказа")),
        "payed_money": _flat_lookup(row, ("payed_money", "paid_money", "Оплачено")),
        "left_cost_money": _flat_lookup(row, ("left_cost_money", "Осталось", "Остаток")),
        "paid_at": _flat_lookup(row, ("payed_at", "paid_at", "Дата оплаты", "Дата завершения")),
    }


async def service_upgrade_order_snapshots(
    *, order_ids: list[str], gc_user_id: str = "", live: bool = False
) -> dict[str, Any]:
    wanted = {_clean(value, 100) for value in order_ids[:50] if _clean(value, 100)}
    if not wanted:
        return {"ok": True, "items": []}
    db_path = _customer_db_path()
    if not db_path.exists():
        return {"ok": False, "items": [], "error": "customer-db not found"}
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                f"""SELECT id,platform_id,custom_fields,created_at,updated_at
                    FROM cdb_getcourse_orders
                    WHERE platform_id IN ({','.join('?' for _ in wanted)})
                    ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC""",
                sorted(wanted),
            )
        ).fetchall()
    found: dict[str, dict[str, Any]] = {}
    for row in rows:
        fields = _json_dict(row["custom_fields"])
        view = _upgrade_order_view(dict(row), fields)
        found.setdefault(view["order_id"], view)
    source = "cache"
    warning = ""
    related_items: list[dict[str, Any]] = []
    if live and _clean(gc_user_id, 100):
        settings = await _settings_map()
        if await _gc_export_budget_left(settings) >= 3:
            rows, error = await _getcourse_export_rows(
                "/pl/api/account/deals",
                {"user_id": _clean(gc_user_id, 100)},
                settings,
                "onboarding-upgrade-verify",
            )
            if not error:
                for export_row in rows:
                    fields = _upgrade_export_fields(export_row)
                    related_items.append(
                        _upgrade_order_view(
                            {
                                "id": 0,
                                "platform_id": _clean(fields.get("order_id"), 100),
                                "updated_at": _deal_updated_from_export_row(export_row),
                                "created_at": "",
                            },
                            fields,
                        )
                    )
                    order_id = _clean(fields.get("order_id"), 100)
                    if order_id not in wanted:
                        continue
                    found[order_id] = _upgrade_order_view(
                        {
                            "id": 0,
                            "platform_id": order_id,
                            "updated_at": _deal_updated_from_export_row(export_row),
                            "created_at": "",
                        },
                        fields,
                    )
                source = "live"
            else:
                warning = _clean(error, 1000)
        else:
            warning = "Лимит GetCourse Export API: live-проверка отложена"
    return {
        "ok": True,
        "items": [found[value] for value in sorted(wanted) if value in found],
        "source": source,
        "warning": warning,
        "related_items": related_items,
        "requests_left_2h": await _gc_export_budget_left(await _settings_map()),
    }


async def service_order_identities(*, identities: list[dict[str, Any]]) -> dict[str, Any]:
    """Return phones and tariffs from existing GetCourse orders without using the GetCourse API."""

    requested: dict[str, set[str]] = {}
    source_ids: list[int] = []
    order_ids: list[str] = []
    gc_user_ids: list[str] = []
    emails: list[str] = []
    for item in identities[:250]:
        key = _clean(item.get("key"), 100)
        if not key:
            continue
        values: set[str] = set()
        try:
            source_id = int(item.get("source_record_id") or 0)
        except (TypeError, ValueError):
            source_id = 0
        order_id = _clean(item.get("order_id"), 100)
        gc_user_id = _clean(item.get("gc_user_id"), 100)
        email = _clean(item.get("email"), 300).casefold()
        if source_id > 0:
            values.add(f"source:{source_id}")
            source_ids.append(source_id)
        if order_id:
            values.add(f"order:{order_id}")
            order_ids.append(order_id)
        if gc_user_id:
            values.add(f"gc:{gc_user_id}")
            gc_user_ids.append(gc_user_id)
        if email:
            values.add(f"email:{email}")
            emails.append(email)
        requested[key] = values
    if not requested:
        return {"ok": True, "items": []}
    where: list[str] = []
    params: list[Any] = []
    for column, values in (
        ("id", sorted(set(source_ids))),
        ("platform_id", sorted(set(order_ids))),
        ("CAST(json_extract(custom_fields,'$.gc_user_id') AS TEXT)", sorted(set(gc_user_ids))),
        ("lower(json_extract(custom_fields,'$.email'))", sorted(set(emails))),
    ):
        if values:
            where.append(f"{column} IN ({','.join('?' for _ in values)})")
            params.extend(values)
    db_path = _customer_db_path()
    if not db_path.exists() or not where:
        return {"ok": False, "items": [], "error": "customer-db not found"}
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                f"""
                SELECT id,platform_id,custom_fields,updated_at,created_at
                FROM cdb_getcourse_orders
                WHERE json_valid(custom_fields) AND ({' OR '.join(where)})
                ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC
                """,
                params,
            )
        ).fetchall()
    found: dict[str, tuple[int, dict[str, Any]]] = {}
    for row in rows:
        fields = _json_dict(row["custom_fields"])
        row_keys = {
            f"source:{int(row['id'])}",
            f"order:{_clean(fields.get('order_id') or row['platform_id'], 100)}",
            f"gc:{_clean(fields.get('gc_user_id'), 100)}",
            f"email:{_clean(fields.get('email') or fields.get('user_email'), 300).casefold()}",
        }
        entitlement = _chat_entitlement(fields)
        tariff = _clean(entitlement.get("tariff"), 100)
        value = {
            "source_record_id": int(row["id"]),
            "order_id": _clean(fields.get("order_id") or row["platform_id"], 100),
            "deal_number": _clean(
                fields.get("number") or fields.get("deal_number") or fields.get("order_number")
                or fields.get("order_id") or row["platform_id"], 100,
            ),
            "gc_user_id": _clean(fields.get("gc_user_id"), 100),
            "name": _clean(fields.get("name") or fields.get("user_name") or fields.get("full_name") or fields.get("fio"), 300),
            "email": _clean(fields.get("email") or fields.get("user_email"), 300),
            "phone": _clean(fields.get("phone") or fields.get("user_phone"), 100),
            "date": _business_order_date_text(fields) or _clean(row["updated_at"] or row["created_at"], 100),
            "tariff": tariff.upper() if tariff == "vip" else tariff.capitalize(),
            "utm_term": _clean(fields.get("utm_term"), 1000),
            "product_kind": _clean(entitlement.get("product_kind"), 50),
            "assignment": {
                "course_key": _clean(fields.get("chat_fields_course_key"), 50),
                "stream": _clean(fields.get(DEFAULT_FIELD_NAMES["field_stream"]), 100),
                "vk_link": _clean(fields.get(DEFAULT_FIELD_NAMES["field_vk"]), 2000),
                "tg_link": _clean(fields.get(DEFAULT_FIELD_NAMES["field_tg"]), 2000),
                "curator": _clean(fields.get(DEFAULT_FIELD_NAMES["field_curator"]), 100),
            },
        }
        for key, identity_keys in requested.items():
            matches = identity_keys & row_keys
            score = max(
                (4 if item.startswith("source:") else 3 if item.startswith("order:") else 2 if item.startswith("gc:") else 1)
                for item in matches
            ) if matches else 0
            if score > (found.get(key) or (0, {}))[0]:
                found[key] = (score, {"key": key, **value})
    return {"ok": True, "items": [item for _, item in found.values()]}


async def service_order_financials(*, source_record_ids: list[int]) -> dict[str, Any]:
    """Return money fields by the indexed local row ID; never call GetCourse."""
    ids = sorted({int(value) for value in source_record_ids[:250] if int(value or 0) > 0})
    db_path = _customer_db_path()
    if not ids or not db_path.exists():
        return {"ok": bool(not ids), "items": []}
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            f"SELECT id,custom_fields FROM cdb_getcourse_orders WHERE id IN ({','.join('?' for _ in ids)})",
            ids,
        )).fetchall()
    items = []
    for row in rows:
        fields = _json_dict(row["custom_fields"])
        total = _money_value(fields.get("cost_money") or fields.get("costMoney"))
        remaining = _money_value(fields.get("payed_money") or fields.get("payedMoney"))
        items.append({
            "source_record_id": int(row["id"]), "total_amount": total,
            "remaining_amount": remaining, "refund_amount": max(0.0, total - remaining),
            "payment_state": _clean(fields.get("payment_state"), 50),
        })
    return {"ok": True, "items": items}


def _access_group_pairs(value: Any) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for chunk in _clean(value, 20000).split(","):
        group_id, _, added_at = chunk.strip().partition(":")
        group_id = _clean(group_id, 30)
        if group_id.isdigit() and group_id not in seen:
            seen.add(group_id)
            result.append({"group_id": group_id, "added_at": _clean(added_at, 40)})
    return result


def _access_groups_from_row(row: dict[str, Any]) -> list[dict[str, str]]:
    for key, value in row.items():
        normalized = _norm(key)
        if "idgrouplist" in normalized or "id групп" in normalized or "group list" in normalized:
            groups = _access_group_pairs(value)
            if groups:
                return groups
    return []


async def _cached_user_access(gc_user_id: str, email: str) -> dict[str, Any]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return {"ok": False, "groups": [], "source": "cache", "error": "customer-db not found"}
    clauses: list[str] = []
    params: list[Any] = []
    if gc_user_id:
        clauses.append("platform_id=?")
        params.append(gc_user_id)
    if email:
        clauses.append("lower(json_extract(custom_fields,'$.email'))=lower(?)")
        params.append(email)
    if not clauses:
        return {"ok": False, "groups": [], "source": "cache", "error": "GetCourse ID или email не найден"}
    async with _db_connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                f"""
                SELECT platform_id,custom_fields,updated_at,created_at
                FROM cdb_getcourse_users
                WHERE json_valid(custom_fields) AND ({' OR '.join(clauses)})
                ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC
                LIMIT 1
                """,
                params,
            )
        ).fetchone()
    if not row:
        return {"ok": False, "groups": [], "source": "cache", "error": "Снимок доступов не найден"}
    fields = _json_dict(row["custom_fields"])
    return {
        "ok": True,
        "gc_user_id": _clean(row["platform_id"], 100),
        "email": _clean(fields.get("email"), 300),
        "groups": _access_group_pairs(fields.get("getcourse_group_membership")),
        "source": "cache",
        "updated_at": _clean(row["updated_at"] or row["created_at"], 40),
    }


def _access_snapshot_cache_key(gc_user_id: str, email: str) -> str:
    identity = f"id:{_clean(gc_user_id, 100)}" if _clean(gc_user_id, 100) else f"email:{_norm(email)}"
    return ACCESS_SNAPSHOT_CACHE_PREFIX + hashlib.sha256(identity.encode("utf-8")).hexdigest()


async def _load_access_snapshot(
    gc_user_id: str, email: str, *, max_age_minutes: int, allow_stale: bool = False
) -> dict[str, Any] | None:
    cached = await _load_flow_students_cache(
        _access_snapshot_cache_key(gc_user_id, email), max_age_minutes, allow_stale=allow_stale
    )
    if not cached:
        return None
    cached["source"] = "cache"
    cached["refresh_due"] = int(cached.get("cache_age_seconds") or 0) > max_age_minutes * 60
    return cached


async def _save_access_snapshots(items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    assert _db_path is not None
    values = []
    for item in items:
        updated_at = _clean(item.get("updated_at") or _now(), 40)
        values.append(
            (
                _access_snapshot_cache_key(_clean(item.get("gc_user_id"), 100), _clean(item.get("email"), 300)),
                json.dumps(item, ensure_ascii=False),
                updated_at,
            )
        )
    async with _db_connect(_db_path) as db:
        await db.executemany(
            """
            INSERT INTO flow_students_cache(key,value_json,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at
            """,
            values,
        )
        await db.commit()
    return len(values)


async def _live_access_group_catalog(settings: dict[str, str]) -> list[dict[str, str]]:
    now = time.monotonic()
    if now < float(_gc_access_groups_cache.get("expires") or 0) and _gc_access_groups_cache.get("items"):
        return list(_gc_access_groups_cache["items"])
    ok, data, error = await _getcourse_export_get("/pl/api/account/groups", {}, settings, "access-groups")
    if not ok:
        raise RuntimeError(error or "Не удалось получить группы GetCourse")
    info = data.get("info") if isinstance(data, dict) else None
    rows = info if isinstance(info, list) else _extract_export_rows(data)
    items: list[dict[str, str]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        group_id = _clean(row.get("id") or row.get("group_id"), 30)
        name = _clean(row.get("name") or row.get("title"), 500)
        if group_id.isdigit() and name:
            items.append({"group_id": group_id, "name": name})
    if not items:
        raise RuntimeError("GetCourse вернул пустой каталог групп")
    _gc_access_groups_cache.update({"expires": now + 3600, "items": items})
    return list(items)


async def service_getcourse_access_snapshot(
    *, gc_user_id: str = "", email: str = "", live: bool = False, force: bool = False
) -> dict[str, Any]:
    """Read exact GetCourse group IDs; live reads share the module's 2-hour API budget."""

    gc_user_id = _clean(gc_user_id, 100)
    email = _clean(email, 300)
    settings = await _settings_map()
    ttl = _bounded_int(settings.get("access_snapshot_minutes"), 15, 240, 60)
    fresh = await _load_access_snapshot(gc_user_id, email, max_age_minutes=ttl)
    if fresh and (not live or not force):
        fresh["requests_left_2h"] = await _gc_export_budget_left(settings)
        return fresh
    stale = await _load_access_snapshot(gc_user_id, email, max_age_minutes=ttl, allow_stale=True)
    legacy = await _cached_user_access(gc_user_id, email)
    fallback = stale or (legacy if legacy.get("ok") else None)
    if not live:
        if fallback:
            return {**fallback, "stale": True, "refresh_due": True, "requests_left_2h": await _gc_export_budget_left(settings)}
        return {
            "ok": False,
            "groups": [],
            "source": "cache",
            "refresh_due": True,
            "error": "Снимок доступов не найден",
            "requests_left_2h": await _gc_export_budget_left(settings),
        }
    needed = 6 + (0 if _gc_access_groups_cache.get("items") else 1)
    if await _gc_export_budget_left(settings) < needed:
        if fallback:
            return {
                **fallback,
                "stale": True,
                "refresh_due": True,
                "warning": "Лимит GetCourse API: показан последний снимок",
                "requests_left_2h": await _gc_export_budget_left(settings),
                "next_at": await _gc_export_next_budget_at(settings, needed=needed),
            }
        return {
            "ok": False,
            "groups": [],
            "source": "live",
            "error": "Лимит GetCourse API: недостаточно запросов для проверки",
            "requests_left_2h": await _gc_export_budget_left(settings),
            "next_at": await _gc_export_next_budget_at(settings, needed=needed),
        }
    async with _gc_lookup_lock:
        try:
            if not force:
                fresh = await _load_access_snapshot(gc_user_id, email, max_age_minutes=ttl)
                if fresh:
                    fresh["requests_left_2h"] = await _gc_export_budget_left(settings)
                    return fresh
            catalog = await _live_access_group_catalog(settings)
            lookup_settings = dict(settings)
            lookup_settings["gc_export_lookup_poll_attempts"] = "5"
            lookup_settings["gc_export_lookup_poll_delay_seconds"] = "5"
            params: dict[str, Any] = {"idgrouplist": "id_date"}
            if email:
                params["email"] = email
            elif gc_user_id:
                params["id"] = gc_user_id
            else:
                raise RuntimeError("GetCourse ID или email не найден")
            rows, error = await _getcourse_export_rows("/pl/api/account/users", params, lookup_settings, "access-user")
            if error:
                raise RuntimeError(error)
            chosen = next(
                (
                    row
                    for row in rows
                    if (gc_user_id and _user_id_from_export_row(row) == gc_user_id)
                    or (email and _norm(_email_from_export_row(row)) == _norm(email))
                ),
                rows[0] if len(rows) == 1 else None,
            )
            if not chosen:
                raise RuntimeError("Пользователь GetCourse не найден")
            names = {item["group_id"]: item["name"] for item in catalog}
            groups = _access_groups_from_row(chosen)
            for group in groups:
                group["name"] = names.get(group["group_id"], "")
            if any(not item.get("name") for item in groups):
                raise RuntimeError("В GetCourse найдена группа без имени; изменение отменено")
            result = {
                "ok": True,
                "gc_user_id": _user_id_from_export_row(chosen) or gc_user_id,
                "email": _email_from_export_row(chosen) or email,
                "groups": groups,
                "catalog": catalog,
                "source": "live",
                "updated_at": _now(),
                "requests_left_2h": await _gc_export_budget_left(settings),
            }
            await _save_access_snapshots([result])
            return result
        except Exception as exc:
            message = _clean(exc, 1000)
            if "export is not ready" in _norm(message):
                message = "GetCourse ещё формирует выгрузку"
            export_busy = "уже запущен один экспорт" in _norm(message)
            next_at = (
                (datetime.now(timezone.utc) + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
                if export_busy else ""
            )
            if export_busy:
                message = "Обновление отложено"
            if fallback:
                return {
                    **fallback,
                    "stale": True,
                    "refresh_due": True,
                    "warning": message,
                    "next_at": next_at,
                    "requests_left_2h": await _gc_export_budget_left(settings),
                }
            return {
                "ok": False,
                "groups": [],
                "source": "live",
                "error": message,
                "refresh_due": export_busy,
                "next_at": next_at,
                "requests_left_2h": await _gc_export_budget_left(settings),
            }


async def service_sync_getcourse_access_snapshots(
    *, identities: list[dict[str, Any]], catalog: list[dict[str, Any]], root_group_ids: list[str]
) -> dict[str, Any]:
    """Refresh many enrolled users through course-group exports without blocking the UI."""

    requested_by_id: dict[str, dict[str, str]] = {}
    requested_by_email: dict[str, dict[str, str]] = {}
    for raw in identities[:5000]:
        item = {"gc_user_id": _clean(raw.get("gc_user_id"), 100), "email": _clean(raw.get("email"), 300)}
        if item["gc_user_id"]:
            requested_by_id[item["gc_user_id"]] = item
        if _valid_email(item["email"]):
            requested_by_email[_norm(item["email"])] = item
    roots = list(dict.fromkeys(_clean(value, 30) for value in root_group_ids if _clean(value, 30).isdigit()))[:4]
    if not roots or not (requested_by_id or requested_by_email):
        return {"ok": True, "updated": 0, "matched": 0, "groups": 0}
    names = {
        _clean(item.get("group_id"), 30): _clean(item.get("name"), 500)
        for item in catalog
        if _clean(item.get("group_id"), 30).isdigit() and _clean(item.get("name"), 500)
    }
    settings = await _settings_map()
    needed = len(roots) * 6
    reserve = _gc_new_job_reserve(settings)
    budget = await _gc_export_budget_left(settings)
    if budget < needed + reserve:
        return {
            "ok": False,
            "updated": 0,
            "error": "Лимит GetCourse API: пакетная синхронизация отложена",
            "requests_left_2h": budget,
            "next_at": await _gc_export_next_budget_at(settings, needed=needed + reserve),
        }
    snapshots: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    lookup_settings = dict(settings)
    lookup_settings["gc_export_lookup_poll_attempts"] = "5"
    lookup_settings["gc_export_lookup_poll_delay_seconds"] = "5"
    async with _gc_lookup_lock:
        for group_id in roots:
            rows, error = await _getcourse_export_rows(
                f"/pl/api/account/groups/{group_id}/users",
                {"added_at[from]": "2000-01-01", "idgrouplist": "id_date"},
                lookup_settings,
                f"access-batch-{group_id}",
            )
            if error:
                errors.append(f"{group_id}: {_clean(error, 300)}")
                continue
            for row in rows:
                gc_id = _user_id_from_export_row(row)
                email_value = _email_from_export_row(row)
                identity = requested_by_id.get(gc_id) or requested_by_email.get(_norm(email_value))
                if not identity:
                    continue
                groups = _access_groups_from_row(row)
                for group in groups:
                    group["name"] = names.get(group["group_id"], "")
                if any(not group.get("name") for group in groups):
                    continue
                key = _access_snapshot_cache_key(identity["gc_user_id"] or gc_id, identity["email"] or email_value)
                snapshots[key] = {
                    "ok": True,
                    "gc_user_id": gc_id or identity["gc_user_id"],
                    "email": email_value or identity["email"],
                    "groups": groups,
                    "catalog": catalog,
                    "source": "batch",
                    "updated_at": _now(),
                }
    updated = await _save_access_snapshots(list(snapshots.values()))
    return {
        "ok": bool(updated) or not errors,
        "updated": updated,
        "matched": len(snapshots),
        "groups": len(roots),
        "errors": errors,
        "requests_left_2h": await _gc_export_budget_left(settings),
    }


async def service_getcourse_access_budget() -> dict[str, Any]:
    settings = await _settings_map()
    return {
        "requests_left_2h": await _gc_export_budget_left(settings),
        "needed_for_verification": 6,
        "next_at": await _gc_export_next_budget_at(settings, needed=6),
    }


def _registry_lesson_columns(rows: list[list[Any]], header_idx: int) -> list[dict[str, Any]]:
    width = max((len(row) for row in rows), default=0)
    result: list[dict[str, Any]] = []
    for col_idx in range(7, min(width, _column_number("AC"))):
        values = [row[col_idx] for row in rows[header_idx + 1 :] if col_idx < len(row)]
        has_checkbox = any(isinstance(value, bool) or _norm(value) in {"true", "false"} for value in values)
        labels: list[str] = []
        for row in rows[: header_idx + 1]:
            if col_idx >= len(row):
                continue
            raw_label = row[col_idx]
            label_text = _clean(
                str(raw_label) if isinstance(raw_label, (int, float)) else raw_label,
                200,
            )
            if label_text:
                labels.append(label_text)
        label = labels[-1] if labels else ""
        normalized_label = _norm(label)
        looks_like_checkbox = bool(
            re.search(r"(?:урок|модул|занят|недел|вип|vip|отзыв|купивш|чат)", normalized_label)
            or bool(re.fullmatch(r"\d+(?:[.,]\d+)?", normalized_label))
        )
        # A newly created flow can contain no boolean values yet. Header-based
        # detection keeps its homework columns visible and writable from day one.
        if not has_checkbox and not looks_like_checkbox:
            continue
        letters = ""
        number = col_idx + 1
        while number:
            number, remainder = divmod(number - 1, 26)
            letters = chr(65 + remainder) + letters
        result.append({"key": letters, "label": label or letters, "column": col_idx})
    return result


def _registry_checkbox_validation_requests(
    sheet_id: int,
    row_numbers: list[int],
    columns: list[int],
) -> list[dict[str, Any]]:
    """Build compact Boolean-validation requests for 1-based rows and 0-based columns."""

    rows = sorted({int(value) for value in row_numbers if int(value) > 0})
    cols = sorted({int(value) for value in columns if int(value) >= 0})
    if not rows or not cols:
        return []

    def runs(values: list[int]) -> list[tuple[int, int]]:
        result: list[tuple[int, int]] = []
        start = end = values[0]
        for value in values[1:]:
            if value == end + 1:
                end = value
                continue
            result.append((start, end))
            start = end = value
        result.append((start, end))
        return result

    rule = {"condition": {"type": "BOOLEAN"}, "strict": True, "showCustomUi": True}
    return [
        {"setDataValidation": {
            "range": {
                "sheetId": int(sheet_id),
                "startRowIndex": first_row - 1,
                "endRowIndex": last_row,
                "startColumnIndex": first_column,
                "endColumnIndex": last_column + 1,
            },
            "rule": rule,
        }}
        for first_row, last_row in runs(rows)
        for first_column, last_column in runs(cols)
    ]


def _registry_lesson_values(row: list[Any], lesson_columns: list[dict[str, Any]]) -> dict[str, bool]:
    return {
        lesson["key"]: (
            row[lesson["column"]]
            if lesson["column"] < len(row) and isinstance(row[lesson["column"]], bool)
            else lesson["column"] < len(row) and _norm(row[lesson["column"]]) == "true"
        )
        for lesson in lesson_columns
    }


def _registry_service_checkbox(label: Any) -> bool:
    """Return True for operational checkboxes that are not learning progress."""
    return bool(re.search(r"(?:купивш|чат)", _norm(label)))


def _registry_batch_rows(session: Any, spreadsheet_id: str, titles: list[str]) -> list[list[list[Any]]]:
    result: list[list[list[Any]]] = []
    for offset in range(0, len(titles), 3):
        chunk = titles[offset : offset + 3]
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
            params=[("ranges", _a1_range(title, "A1:AC300")) for title in chunk]
            + [("majorDimension", "ROWS"), ("valueRenderOption", "UNFORMATTED_VALUE")],
            timeout=60,
        )
        response.raise_for_status()
        value_ranges = (response.json() or {}).get("valueRanges") or []
        if len(value_ranges) != len(chunk):
            raise RuntimeError("Google Sheets returned incomplete registry ranges")
        result.extend((value_range or {}).get("values") or [] for value_range in value_ranges)
    return result


def _registry_xlsx_rows(content: bytes) -> dict[str, list[list[Any]]]:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            shared = ["".join(node.text or "" for node in item.findall(".//x:t", ns)) for item in root.findall("x:si", ns)]
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        targets = {item.attrib["Id"]: item.attrib["Target"] for item in relationships.findall("r:Relationship", rel_ns)}
        result: dict[str, list[list[Any]]] = {}
        for sheet in workbook.findall(".//x:sheet", ns):
            relation_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
            target = targets.get(relation_id, "").lstrip("/")
            path = target if target.startswith("xl/") else f"xl/{target}"
            if path not in archive.namelist():
                continue
            rows: list[list[Any]] = []
            root = ET.fromstring(archive.read(path))
            for row_node in root.findall(".//x:sheetData/x:row", ns):
                row: list[Any] = []
                for cell in row_node.findall("x:c", ns):
                    match = re.match(r"([A-Z]+)", cell.attrib.get("r", ""))
                    if not match:
                        continue
                    column = _column_number(match.group(1)) - 1
                    if column < 0 or column >= _column_number("AC"):
                        continue
                    value_node = cell.find("x:v", ns)
                    raw = value_node.text if value_node is not None else ""
                    kind = cell.attrib.get("t", "")
                    if kind == "s" and raw.isdigit() and int(raw) < len(shared):
                        value: Any = shared[int(raw)]
                    elif kind == "b":
                        value = raw == "1"
                    elif kind == "inlineStr":
                        value = "".join(node.text or "" for node in cell.findall(".//x:t", ns))
                    else:
                        try:
                            value = float(raw) if "." in raw else int(raw)
                        except (TypeError, ValueError):
                            value = raw
                    if len(row) <= column:
                        row.extend([""] * (column + 1 - len(row)))
                    row[column] = value
                rows.append(row)
                if len(rows) >= 300:
                    break
            result[str(sheet.attrib.get("name") or "")[:300]] = rows
    return result


def _registry_repair_homework_checkboxes_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Restore homework checkboxes in populated rows whose progress cells are not Boolean."""

    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    ))
    metadata_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title,index)"},
        timeout=30,
    )
    metadata_response.raise_for_status()
    metadata = metadata_response.json() or {}
    sheets = [
        sheet.get("properties") or {}
        for sheet in metadata.get("sheets") or []
        if re.match(r"^[СCЩ]\s*\d+", _clean((sheet.get("properties") or {}).get("title"), 300), flags=re.IGNORECASE)
    ]
    export_response = session.get(
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export",
        params={"format": "xlsx"},
        timeout=120,
    )
    export_response.raise_for_status()
    rows_by_title = _registry_xlsx_rows(export_response.content)

    prepared: list[dict[str, Any]] = []
    signature_max_boolean: dict[tuple[str, ...], int] = {}
    for props in sheets:
        title = str(props.get("title") or "")[:300]
        rows = rows_by_title.get(title) or []
        if not rows:
            continue
        header_idx, columns = _sheet_student_header(rows)
        homework_columns = [
            item for item in _registry_lesson_columns(rows, header_idx)
            if not _registry_service_checkbox(item.get("label"))
        ]
        if not homework_columns:
            continue
        name_column = columns.get("name", 0)
        email_column = columns.get("email", 7)
        student_rows: list[int] = []
        for row_number, row in enumerate(rows[header_idx + 1 : 300], start=header_idx + 2):
            name = _row_value(row, name_column, 300)
            email = _row_value(row, email_column, 300)
            other_identity = any(
                _clean(row[index], 300)
                for index in range(min(len(row), max(8, email_column + 1)))
                if index != name_column
            )
            if _valid_email(email) or bool(name and other_identity):
                student_rows.append(row_number)
        signature = tuple(_norm(item.get("label")) for item in homework_columns)
        boolean_counts: dict[int, int] = {}
        for row_number in student_rows:
            row = rows[row_number - 1] if row_number <= len(rows) else []
            boolean_counts[row_number] = sum(
                1
                for lesson in homework_columns
                for value in [row[int(lesson["column"])] if int(lesson["column"]) < len(row) else None]
                if isinstance(value, bool) or _norm(value) in {"true", "false"}
            )
        signature_max_boolean[signature] = max(
            signature_max_boolean.get(signature, 0),
            max(boolean_counts.values(), default=0),
        )
        prepared.append({
            "props": props,
            "title": title,
            "homework_columns": homework_columns,
            "student_rows": student_rows,
            "boolean_counts": boolean_counts,
            "signature": signature,
        })

    requests: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    for prepared_sheet in prepared:
        props = prepared_sheet["props"]
        title = prepared_sheet["title"]
        homework_columns = prepared_sheet["homework_columns"]
        student_rows = prepared_sheet["student_rows"]
        boolean_counts = prepared_sheet["boolean_counts"]
        signature = prepared_sheet["signature"]
        # Repair only completely inactive homework rows, and only when an
        # identical header layout has a fully working row elsewhere. Partial
        # blanks in legacy/tariff-specific layouts are intentional.
        if signature_max_boolean.get(signature, 0) < len(homework_columns):
            continue
        missing_groups: dict[tuple[int, ...], list[int]] = {}
        for row_number in student_rows:
            if int(boolean_counts.get(row_number, 0)) == 0:
                missing_groups.setdefault(
                    tuple(int(item["column"]) for item in homework_columns), []
                ).append(row_number)
        sheet_requests: list[dict[str, Any]] = []
        for missing_columns, row_numbers in missing_groups.items():
            sheet_requests.extend(_registry_checkbox_validation_requests(
                int(props.get("sheetId") or 0), row_numbers, list(missing_columns)
            ))
        requests.extend(sheet_requests)
        missing_cells = sum(len(columns_) * len(rows_) for columns_, rows_ in missing_groups.items())
        if missing_cells:
            items.append({
                "sheet_id": int(props.get("sheetId") or 0),
                "sheet_title": _clean(title, 300),
                "student_rows": student_rows,
                "rows_to_repair": sorted({row for rows_ in missing_groups.values() for row in rows_}),
                "missing_cells": missing_cells,
            })
    if apply:
        for offset in range(0, len(requests), 500):
            update_response = session.post(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
                json={"requests": requests[offset : offset + 500]},
                timeout=120,
            )
            update_response.raise_for_status()
    return {
        "ok": True,
        "applied": bool(apply),
        "spreadsheet_title": _clean((metadata.get("properties") or {}).get("title"), 300),
        "sheets_checked": len(sheets),
        "sheets_to_repair": len([item for item in items if item["missing_cells"]]),
        "rows_to_repair": sum(len(item["rows_to_repair"]) for item in items),
        "missing_cells": sum(int(item["missing_cells"]) for item in items),
        "requests": len(requests),
        "repair_rule": "completely_inactive_homework_rows",
        "items": items,
    }


async def service_repair_registry_homework_checkboxes(*, apply: bool = False) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Google Sheets не настроен")
    async with _registry_write_lock:
        return await asyncio.to_thread(
            _registry_repair_homework_checkboxes_sync,
            spreadsheet_id,
            credentials_path,
            apply=bool(apply),
        )


def _registry_sheet_snapshot_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    flows: list[dict[str, Any]],
    known_layouts: list[dict[str, Any]],
    curator_cell: str,
    curator_map: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/drive.readonly"],
    )
    session = AuthorizedSession(credentials)
    metadata: dict[str, Any] = {}
    exported_rows: dict[str, list[list[Any]]] | None = None

    def export_rows() -> dict[str, list[list[Any]]]:
        response = session.get(
            f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export",
            params={"format": "xlsx"}, timeout=120,
        )
        response.raise_for_status()
        return _registry_xlsx_rows(response.content)

    try:
        metadata_response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
            params={"fields": "properties.title,sheets.properties(sheetId,title,index)"},
            timeout=30,
        )
        metadata_response.raise_for_status()
        metadata = metadata_response.json() or {}
        sheets = [sheet.get("properties") or {} for sheet in metadata.get("sheets") or []]
    except Exception as exc:
        if not any(code in str(exc) for code in ("400", "429")):
            raise
        exported_rows = export_rows()
        known = {
            (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50)): int(item.get("sheet_id") or 0)
            for item in known_layouts
        }
        sheets = [
            {"title": title, "sheetId": known.get((course_key, stream), 0), "index": index}
            for index, flow in enumerate(flows)
            for course_key, stream in [(_clean(flow.get("course_key"), 50), _clean(flow.get("stream"), 50))]
            for title in [next((name for name in exported_rows if _sheet_title_matches(name, course_key, stream)), "")]
            if title
        ]
    prepared: list[dict[str, Any]] = []
    for flow in flows:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 50)
        props = next((item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)), None)
        if not props:
            prepared.append({"course_key": course_key, "stream": stream, "props": None, "rows": []})
            continue
        title = str(props.get("title") or "")[:300]
        prepared.append({"course_key": course_key, "stream": stream, "props": props, "title": title, "rows": (exported_rows or {}).get(title, [])})
    readable = [item for item in prepared if item["props"]]
    if readable and exported_rows is None:
        try:
            rows_by_item = _registry_batch_rows(session, spreadsheet_id, [item["title"] for item in readable])
        except Exception as exc:
            if not any(code in str(exc) for code in ("400", "429")):
                raise
            exported_rows = export_rows()
            rows_by_item = [(exported_rows or {}).get(item["title"], []) for item in readable]
        for item, rows in zip(readable, rows_by_item):
            item["rows"] = rows
    items: list[dict[str, Any]] = []
    curator_match = re.fullmatch(r"([A-Z]+)(\d+)", curator_cell.upper())
    curator_column = _column_number(curator_match.group(1)) - 1 if curator_match else 10
    curator_row = int(curator_match.group(2)) - 1 if curator_match else 1
    for item in prepared:
        course_key, stream, props = item["course_key"], item["stream"], item["props"]
        if not props:
            items.append({"course_key": course_key, "stream": stream, "status": "sheet_not_found", "students": [], "lesson_columns": []})
            continue
        title, rows = item["title"], item["rows"]
        header_idx, columns = _sheet_student_header(rows)
        lesson_columns = _registry_lesson_columns(rows, header_idx)
        raw_curator = _row_value(rows[curator_row] if curator_row < len(rows) else [], curator_column, 300)
        students: list[dict[str, Any]] = []
        for row_number, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2):
            email = _row_value(row, columns.get("email"), 300)
            if not _valid_email(email):
                continue
            lessons = _registry_lesson_values(row, lesson_columns)
            raw_responsible_curator = _row_value(row, columns.get("responsible_curator"), 200)
            students.append({
                "row": row_number,
                "name": _row_value(row, columns.get("name"), 300),
                "date": _row_value(row, columns.get("date"), 100),
                "course": _row_value(row, columns.get("course"), 100),
                "tariff": _row_value(row, columns.get("tariff"), 100),
                "enrollment": _row_value(row, columns.get("enrollment"), 100),
                "manager_name": _row_value(row, columns.get("manager"), 200),
                "tg_account": _row_value(row, columns.get("tg_account"), 500),
                "email": email,
                "buyers": _row_value(row, columns.get("buyers"), 100),
                "responsible_curator": _map_curator(raw_responsible_curator, curator_map),
                "responsible_curator_raw": raw_responsible_curator,
                "lessons": lessons,
            })
        items.append(
            {
                "course_key": course_key,
                "stream": stream,
                "status": "ok",
                "sheet_id": int(props.get("sheetId") or 0),
                "sheet_title": title,
                "curator_value": _map_curator(raw_curator, curator_map),
                "curator_raw": raw_curator,
                "header_row": header_idx + 1,
                "lesson_columns": [{"key": item["key"], "label": item["label"]} for item in lesson_columns],
                "students": students,
            }
        )
    return {"ok": True, "spreadsheet_id": spreadsheet_id, "spreadsheet_title": _clean((metadata.get("properties") or {}).get("title"), 300), "items": items}


async def service_registry_sheet_snapshot(
    *, flows: list[dict[str, Any]], known_layouts: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        return {"ok": False, "items": [], "error": "Google Sheets is not configured"}
    return await asyncio.to_thread(
        _registry_sheet_snapshot_sync,
        spreadsheet_id,
        credentials_path,
        flows,
        known_layouts or [],
        _clean(settings.get("curator_cell") or "K2", 20).upper(),
        _curator_name_map(settings),
    )


def _set_registry_flow_curator_sync(
    spreadsheet_id: str, credentials_path: Path, sheet_title: str, cell: str, curator_raw: str
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(
        Credentials.from_service_account_file(
            str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    )
    response = session.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(sheet_title, cell)}",
        params={"valueInputOption": "RAW"},
        json={"majorDimension": "ROWS", "values": [[curator_raw]]},
        timeout=30,
    )
    response.raise_for_status()
    return {"ok": True, "sheet_title": sheet_title, "cell": cell}


async def service_set_registry_flow_curator(*, course_key: str, stream: str, curator: str) -> dict[str, Any]:
    settings = await _settings_map()
    curator_raw = _transfer_curator_raw(settings, curator)
    if not curator_raw:
        raise ValueError("Куратор не поддерживается")
    flow = next(
        (
            item for item in (await _chat_flows(settings)).get("items") or []
            if _clean(item.get("course_key"), 50) == _clean(course_key, 50)
            and _clean(item.get("stream"), 50) == _clean(stream, 50)
        ),
        None,
    )
    sheet_title = _clean((flow or {}).get("curator_sheet"), 300)
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not sheet_title or not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Лист потока не найден")
    result = await asyncio.to_thread(
        _set_registry_flow_curator_sync,
        spreadsheet_id,
        credentials_path,
        sheet_title,
        _clean(settings.get("curator_cell") or "K2", 20).upper(),
        curator_raw,
    )
    _chat_flows_cache.clear()
    return {**result, "curator": curator}


async def service_reconcile_registry_curators(*, flows: list[dict[str, Any]], limit: int = 500) -> dict[str, Any]:
    settings = await _settings_map()
    await _save_flow_students_cache(
        REGISTRY_CURATOR_SYNC_CACHE_KEY,
        {"ok": True, "updated_at": _now(), "items": [], "source": "student-transfer"},
    )
    allowed = {value for _, value in _curator_name_map(settings)}
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = [dict(row) for row in await (await db.execute(
            "SELECT source_record_id,platform_id,order_id,gc_user_id,details_json FROM processed_orders ORDER BY datetime(updated_at) DESC,id DESC"
        )).fetchall()]
    by_identity: dict[str, dict[str, Any]] = {}
    for row in rows:
        for value in (row.get("source_record_id"), row.get("platform_id"), row.get("order_id")):
            if _clean(value, 100):
                by_identity.setdefault(_clean(value, 100), row)

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    maximum = max(1, min(2000, int(limit or 500)))
    for flow in flows:
        stream = _clean(flow.get("stream"), 100)
        vk_link = _clean(flow.get("vk_link"), 2000)
        tg_link = _clean(flow.get("tg_link"), 2000)
        if not stream or not vk_link or not tg_link:
            continue
        for student in flow.get("students") or []:
            curator = _clean(student.get("teacher_code") or student.get("responsible_curator") or flow.get("teacher_code"), 100)
            if curator not in allowed:
                continue
            processed = next(
                (by_identity.get(_clean(student.get(key), 100)) for key in ("source_record_id", "order_id") if _clean(student.get(key), 100)),
                None,
            )
            # New students can reach the registry after their order has fallen
            # outside the scanner window. Registry identities are sufficient.
            processed = processed or {}
            current = _json_dict(_json_dict(processed.get("details_json")).get("output_fields"))
            if _clean(current.get(settings["field_curator"]), 100) == curator:
                continue
            email = _norm(student.get("email"))
            order_id = _clean(student.get("order_id") or processed.get("platform_id") or processed.get("order_id"), 100)
            gc_user_id = _clean(student.get("gc_user_id") or processed.get("gc_user_id"), 100)
            deal_number = _clean(student.get("deal_number") or order_id, 100)
            key = (email, order_id)
            if not _valid_email(email) or not order_id or not gc_user_id or key in seen:
                continue
            seen.add(key)
            output_fields = {
                settings["field_stream"]: stream,
                settings["field_vk"]: vk_link,
                settings["field_tg"]: tg_link,
                settings["field_curator"]: curator,
            }
            candidates.append({
                "email": email,
                "gc_user_id": gc_user_id,
                "order_id": order_id,
                "deal_number": deal_number,
                "fields": output_fields,
                "user_fields": _getcourse_user_addfields(output_fields, settings),
                "flow": {
                    "course": flow.get("course"),
                    "course_key": flow.get("course_key"),
                    "stream": stream,
                    "sheet_title": flow.get("sheet_title"),
                    "change_reason": "registry_curator_reconciliation",
                    "previous_curator": _clean(current.get(settings["field_curator"]), 100),
                    "new_curator": curator,
                },
            })
            if len(candidates) >= maximum:
                break
        if len(candidates) >= maximum:
            break
    result = await _enqueue_gc_fields_write_items(candidates, force=True)
    result["enabled"] = _truthy(settings.get("gc_fields_write_enabled"))
    result["requests_left_2h"] = await _gc_export_budget_left(settings)
    result["reserved_requests"] = _gc_new_job_reserve(settings)
    return result


def _registry_sheet_mirror_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    flows: list[dict[str, Any]],
    curator_cell: str,
    layouts: list[dict[str, Any]],
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(
        Credentials.from_service_account_file(
            str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    )

    def metadata() -> list[dict[str, Any]]:
        response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
            params={"fields": "sheets.properties(sheetId,title,index)"}, timeout=30,
        )
        response.raise_for_status()
        return [sheet.get("properties") or {} for sheet in (response.json() or {}).get("sheets") or []]

    sheets = [
        {"sheetId": int(item.get("sheet_id") or 0), "title": str(item.get("sheet_title") or "")[:300], "index": index}
        for index, item in enumerate(layouts)
        if int(item.get("sheet_id") or 0) and _clean(item.get("sheet_title"), 300)
    ]
    if any(
        not next((item for item in sheets if _sheet_title_matches(item.get("title"), flow.get("course_key"), flow.get("stream"))), None)
        for flow in flows
    ):
        sheets = metadata()
    created: list[str] = []
    for flow in flows:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 50)
        if next((item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)), None):
            continue
        templates = [item for item in sheets if _sheet_title_matches(item.get("title"), course_key, _stream_number(item.get("title")))]
        templates = [item for item in templates if _stream_number(item.get("title"))]
        if not templates:
            continue
        source = max(templates, key=lambda item: int(_stream_number(item.get("title")) or 0))
        new_title = f"{_course_sheet_prefix(course_key)}{stream}"
        duplicate_response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": [{"duplicateSheet": {"sourceSheetId": int(source["sheetId"]), "newSheetName": new_title}}]},
            timeout=30,
        )
        duplicate_response.raise_for_status()
        created.append(new_title)
        sheets = metadata()

    layout_map = {
        (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50)): item
        for item in layouts
        if item.get("status") == "ok"
    }
    prepared: list[dict[str, Any]] = []
    for flow in flows:
        course_key = _clean(flow.get("course_key"), 50)
        stream = _clean(flow.get("stream"), 50)
        props = next((item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)), None)
        if props:
            prepared.append({
                "flow": flow,
                "props": props,
                "title": str(props.get("title") or "")[:300],
                "layout": layout_map.get((course_key, stream)),
                "rows": [],
            })
    unresolved = [item for item in prepared if not item["layout"]]
    if unresolved:
        for item, rows in zip(unresolved, _registry_batch_rows(session, spreadsheet_id, [item["title"] for item in unresolved])):
            item["rows"] = rows

    writes: list[dict[str, Any]] = []
    tail_clears: list[str] = []
    mirrored = 0
    for item in prepared:
        flow, title, rows = item["flow"], item["title"], item["rows"]
        layout = item["layout"]
        if layout:
            header_idx = max(0, int(layout.get("header_row") or 1) - 1)
            lesson_columns = layout.get("lesson_columns") or []
        else:
            header_idx, _columns = _sheet_student_header(rows)
            lesson_columns = _registry_lesson_columns(rows, header_idx)
        start_row = header_idx + 2
        escaped = title.replace("'", "''")
        teacher = _clean(flow.get("teacher"), 200)
        if teacher:
            writes.append({"range": _a1_range(title, curator_cell), "majorDimension": "ROWS", "values": [[teacher]]})
        students = flow.get("students") or []
        if students:
            writes.append({
                "range": f"'{escaped}'!A{start_row}:G{start_row + len(students) - 1}",
                "majorDimension": "ROWS",
                "values": [[
                _clean(student.get("name"), 300), _clean(student.get("date"), 100),
                _clean(student.get("course"), 100), _clean(student.get("tariff"), 100),
                _clean(student.get("teacher"), 200), _clean(student.get("tg_account"), 500),
                _clean(student.get("email"), 300),
                ] for student in students],
            })
            for lesson in lesson_columns:
                writes.append({
                    "range": f"'{escaped}'!{lesson['key']}{start_row}:{lesson['key']}{start_row + len(students) - 1}",
                    "majorDimension": "ROWS",
                    "values": [[bool((student.get("lessons") or {}).get(lesson["key"], False))] for student in students],
                })
        tail_start = start_row + len(students)
        if tail_start <= 300:
            tail_clears.append(f"'{escaped}'!A{tail_start}:G300")
            tail_clears.extend(f"'{escaped}'!{item['key']}{tail_start}:{item['key']}300" for item in lesson_columns)
        mirrored += len(students)
    for offset in range(0, len(writes), 500):
        response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
            json={"valueInputOption": "RAW", "data": writes[offset : offset + 500]}, timeout=60,
        )
        response.raise_for_status()
    for offset in range(0, len(tail_clears), 500):
        clear_response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchClear",
            json={"ranges": tail_clears[offset : offset + 500]}, timeout=45,
        )
        if not clear_response.ok:
            raise RuntimeError(
                f"Google batchClear {clear_response.status_code}: {_clean(clear_response.text, 1000)}; "
                f"ranges={tail_clears[offset : offset + 3]}"
            )
    return {"ok": True, "created_sheets": created, "mirrored_students": mirrored, "writes": len(writes)}


def _latest_stream_sheets(sheets: list[dict[str, Any]], per_course: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for course_key in ("dog", "puppy"):
        matches: list[tuple[int, dict[str, Any]]] = []
        for sheet in sheets:
            props = sheet.get("properties") if isinstance(sheet.get("properties"), dict) else {}
            stream = _stream_number(props.get("title"))
            if stream and _sheet_title_matches(props.get("title"), course_key, stream):
                matches.append((int(stream), sheet))
        selected.extend(sheet for _, sheet in sorted(matches, key=lambda item: item[0])[-per_course:])
    return selected


def _is_tariff_conditional_rule(rule: dict[str, Any]) -> bool:
    boolean_rule = rule.get("booleanRule") if isinstance(rule.get("booleanRule"), dict) else {}
    condition = boolean_rule.get("condition") if isinstance(boolean_rule.get("condition"), dict) else {}
    values = condition.get("values") if isinstance(condition.get("values"), list) else []
    text = " ".join(_clean(item.get("userEnteredValue"), 500) for item in values if isinstance(item, dict)).casefold()
    return any(marker in text for marker in (
        "стандарт", "standard", "премиум", "premium", "вип", '"vip"', "щенок+собака", "щенок + собака",
    ))


def _sheet_column_name(index: int) -> str:
    value = max(0, int(index)) + 1
    result = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _registry_tariff_format_plan(
    sheets: list[dict[str, Any]], rows_by_title: dict[str, list[list[Any]]], per_course: int,
) -> dict[str, Any]:
    requests: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    colors = {
        "standard": {"red": 0.95686275, "green": 0.78039217, "blue": 0.7647059},
        "vip": {"red": 0.8509804, "green": 0.91764706, "blue": 0.827451},
        "combo": {"red": 0.7882353, "green": 0.854902, "blue": 0.972549},
    }
    for sheet in _latest_stream_sheets(sheets, max(1, min(int(per_course), 20))):
        props = sheet.get("properties") if isinstance(sheet.get("properties"), dict) else {}
        title = _clean(props.get("title"), 300)
        sheet_id = int(props.get("sheetId") or 0)
        grid = props.get("gridProperties") if isinstance(props.get("gridProperties"), dict) else {}
        rows = rows_by_title.get(title) or []
        header_idx, columns = _sheet_student_header(rows)
        tariff_column = int(columns.get("tariff", 3))
        course_column = columns.get("course")
        first_data_row = header_idx + 2
        row_count = max(first_data_row, int(grid.get("rowCount") or len(rows) or first_data_row))
        # Tariff colours are an identity hint, not a progress marker. Keeping
        # them out of lesson/checkbox columns prevents an entire VIP/Standard
        # row from looking as if every module had been completed.
        identity_column_count = tariff_column + 1
        rules = sheet.get("conditionalFormats") if isinstance(sheet.get("conditionalFormats"), list) else []
        removed = [index for index, rule in enumerate(rules) if isinstance(rule, dict) and _is_tariff_conditional_rule(rule)]
        for index in reversed(removed):
            requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": index}})
        cell = f"${_sheet_column_name(tariff_column)}{first_data_row}"
        range_ = {
            "sheetId": sheet_id,
            "startRowIndex": header_idx + 1,
            "endRowIndex": row_count,
            "startColumnIndex": 0,
            "endColumnIndex": identity_column_count,
        }
        requests.append({"repeatCell": {
            "range": range_,
            "cell": {"userEnteredFormat": {}},
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.backgroundColorStyle",
        }})
        formulas = []
        if course_column is not None:
            course_cell = f"${_sheet_column_name(int(course_column))}{first_data_row}"
            formulas.extend((
                (f'={course_cell}="Щенок+Собака"', colors["combo"]),
                (f'={course_cell}="Щенок + Собака"', colors["combo"]),
            ))
        formulas.extend((
            (f'={cell}="ВИП"', colors["vip"]),
            (f'={cell}="Стандарт"', colors["standard"]),
        ))
        for index, (formula, color) in enumerate(formulas):
            requests.append({"addConditionalFormatRule": {
                "index": index,
                "rule": {
                    "ranges": [range_],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                        "format": {"backgroundColor": color},
                    },
                },
            }})
        items.append({
            "sheet_id": sheet_id,
            "sheet_title": title,
            "header_row": header_idx + 1,
            "tariff_column": tariff_column + 1,
            "course_column": int(course_column) + 1 if course_column is not None else None,
            "removed_rules": len(removed),
            "added_rules": len(formulas),
            "backgrounds_cleared": True,
        })
    return {"requests": requests, "items": items}


def _registry_format_tariffs_sync(
    spreadsheet_id: str, credentials_path: Path, *, per_course: int = 10, apply: bool = False,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    ))
    response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "sheets(properties(sheetId,title,index,gridProperties(rowCount,columnCount)),conditionalFormats)"},
        timeout=30,
    )
    response.raise_for_status()
    sheets = (response.json() or {}).get("sheets") or []
    selected = _latest_stream_sheets(sheets, max(1, min(int(per_course), 20)))
    titles = [_clean((sheet.get("properties") or {}).get("title"), 300) for sheet in selected]
    rows_by_title = {title: rows for title, rows in zip(titles, _registry_batch_rows(session, spreadsheet_id, titles))}
    plan = _registry_tariff_format_plan(sheets, rows_by_title, per_course)
    if apply and plan["requests"]:
        update = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": plan["requests"]}, timeout=60,
        )
        if not update.ok:
            raise RuntimeError(f"Google tariff formatting {update.status_code}: {_clean(update.text, 2000)}")
    return {
        "ok": True,
        "applied": bool(apply),
        "sheets": len(plan["items"]),
        "removed_rules": sum(int(item["removed_rules"]) for item in plan["items"]),
        "added_rules": sum(int(item["added_rules"]) for item in plan["items"]),
        "items": plan["items"],
    }


async def service_registry_sheet_mirror(
    *, flows: list[dict[str, Any]], layouts: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    return {"ok": True, "paused": True, "reason": "legacy sheet recovery"}


def _registry_flow_sheet_title(course_key: str, stream: str, date_start: str) -> str:
    course_key = _clean(course_key, 50)
    stream = _clean(stream, 50)
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise ValueError("Неверно указан курс или номер потока")
    try:
        start = datetime.strptime(_clean(date_start, 100), "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Дата старта должна быть в формате ГГГГ-ММ-ДД") from exc
    return f"{_course_sheet_prefix(course_key)}{stream} ({start:%d.%m})"


def _registry_flow_sheet_context(
    session: Any, spreadsheet_id: str, course_key: str, stream: str, date_start: str,
) -> dict[str, Any]:
    target_title = _registry_flow_sheet_title(course_key, stream, date_start)
    response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title,index,hidden,gridProperties(rowCount,columnCount))"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json() or {}
    sheets = [sheet.get("properties") or {} for sheet in payload.get("sheets") or []]
    existing = next(
        (item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)),
        None,
    )
    target_stream_number = int(stream)
    templates = [
        item for item in sheets
        if _sheet_title_matches(item.get("title"), course_key, _stream_number(item.get("title")))
        and _stream_number(item.get("title"))
        and int(_stream_number(item.get("title")) or 0) < target_stream_number
    ]
    template = max(templates, key=lambda item: int(_stream_number(item.get("title")) or 0)) if templates else None
    return {
        "spreadsheet_title": _clean((payload.get("properties") or {}).get("title"), 300),
        "target_title": target_title,
        "existing": existing,
        "template": template,
    }


def _registry_flow_sheet_preflight_sync(
    spreadsheet_id: str, credentials_path: Path, course_key: str, stream: str, date_start: str,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    ))
    context = _registry_flow_sheet_context(session, spreadsheet_id, course_key, stream, date_start)
    existing = context["existing"]
    template = context["template"]
    if existing:
        return {
            "ok": False,
            "status": "already_exists",
            "error": f"Лист потока уже существует: {_clean(existing.get('title'), 300)}",
            "sheet_title": _clean(existing.get("title"), 300),
            "sheet_id": int(existing.get("sheetId") or 0),
        }
    if not template:
        return {
            "ok": False,
            "status": "template_not_found",
            "error": "Не найден предыдущий лист этого курса, который можно взять за шаблон",
        }
    return {
        "ok": True,
        "status": "ready",
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": context["spreadsheet_title"],
        "sheet_title": context["target_title"],
        "template_title": _clean(template.get("title"), 300),
        "template_sheet_id": int(template.get("sheetId") or 0),
    }


async def service_registry_flow_sheet_preflight(
    *, course_key: str, stream: str, date_start: str,
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        return {"ok": False, "status": "not_configured", "error": "Таблица учеников Google не настроена"}
    try:
        return await asyncio.to_thread(
            _registry_flow_sheet_preflight_sync,
            spreadsheet_id, credentials_path, course_key, stream, date_start,
        )
    except ValueError as exc:
        return {"ok": False, "status": "invalid", "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "status": "unavailable", "error": f"Google таблица недоступна: {_clean(exc, 500)}"}


def _registry_flow_sheet_status_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    course_key: str,
    stream: str,
    date_start: str,
    expected_sheet_id: int = 0,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    ))
    expected_title = _registry_flow_sheet_title(course_key, stream, date_start)
    response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "properties.title,sheets.properties(sheetId,title,index,hidden)"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json() or {}
    sheets = [sheet.get("properties") or {} for sheet in payload.get("sheets") or []]
    target = next(
        (item for item in sheets if expected_sheet_id and int(item.get("sheetId") or 0) == int(expected_sheet_id)),
        None,
    )
    if target is None:
        target = next(
            (item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)),
            None,
        )
    if not target:
        return {
            "ok": False,
            "status": "missing",
            "error": f"Лист потока не найден: {expected_title}",
            "spreadsheet_id": spreadsheet_id,
            "sheet_title": expected_title,
            "sheet_id": int(expected_sheet_id or 0),
        }
    sheet_id = int(target.get("sheetId") or 0)
    title = _clean(target.get("title"), 300)
    hidden = bool(target.get("hidden"))
    exact_id = not expected_sheet_id or sheet_id == int(expected_sheet_id)
    exact_title = title == expected_title
    ok = bool(sheet_id and exact_id and exact_title and not hidden)
    error = ""
    if not exact_id:
        error = "Google вернул другой идентификатор листа потока"
    elif not exact_title:
        error = f"Лист потока переименован: ожидался {expected_title}, найден {title}"
    elif hidden:
        error = f"Лист потока скрыт: {title}"
    return {
        "ok": ok,
        "status": "ready" if ok else "invalid",
        "error": error,
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": _clean((payload.get("properties") or {}).get("title"), 300),
        "sheet_title": title,
        "sheet_id": sheet_id,
        "sheet_index": int(target.get("index") or 0),
        "hidden": hidden,
        "sheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}",
    }


async def service_registry_flow_sheet_status(
    *, course_key: str, stream: str, date_start: str, expected_sheet_id: int = 0,
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        return {"ok": False, "status": "not_configured", "error": "Таблица учеников Google не настроена"}
    try:
        return await asyncio.to_thread(
            _registry_flow_sheet_status_sync,
            spreadsheet_id,
            credentials_path,
            course_key,
            stream,
            date_start,
            int(expected_sheet_id or 0),
        )
    except ValueError as exc:
        return {"ok": False, "status": "invalid", "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "status": "unavailable", "error": f"Google таблица недоступна: {_clean(exc, 500)}"}


def _create_registry_flow_sheet_sync(
    spreadsheet_id: str,
    credentials_path: Path,
    course_key: str,
    stream: str,
    date_start: str,
    curator_raw: str,
    curator_cell: str,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    ))
    context = _registry_flow_sheet_context(session, spreadsheet_id, course_key, stream, date_start)
    existing = context["existing"]
    template = context["template"]
    if existing:
        # This service is called only by an already accepted, idempotent flow job.
        # Reusing the conventional target title lets a job continue after a process
        # restart between duplicateSheet and the cleanup request.
        target = existing
        created = False
    else:
        if not template:
            raise RuntimeError("Не найден лист-шаблон этого курса")
        duplicate = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": [{"duplicateSheet": {
                "sourceSheetId": int(template.get("sheetId") or 0),
                "newSheetName": context["target_title"],
                "insertSheetIndex": int(template.get("index") or 0) + 1,
            }}]},
            timeout=45,
        )
        duplicate.raise_for_status()
        replies = (duplicate.json() or {}).get("replies") or []
        target = ((replies[0] or {}).get("duplicateSheet") or {}).get("properties") if replies else None
        if not target or not int(target.get("sheetId") or 0):
            raise RuntimeError("Google не вернул данные созданного листа")
        created = True

    visible = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
        json={"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": int(target.get("sheetId") or 0), "hidden": False},
            "fields": "hidden",
        }}]},
        timeout=30,
    )
    visible.raise_for_status()

    title = _clean(target.get("title") or context["target_title"], 300)
    values = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, 'A1:AC300')}",
        params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"},
        timeout=30,
    )
    values.raise_for_status()
    rows = (values.json() or {}).get("values") or []
    header_idx, _columns = _sheet_student_header(rows)
    target_grid = target.get("gridProperties") if isinstance(target.get("gridProperties"), dict) else {}
    template_grid = (
        template.get("gridProperties")
        if isinstance((template or {}).get("gridProperties"), dict)
        else {}
    )
    grid = target_grid or template_grid
    end_row = max(300, int(grid.get("rowCount") or 300))
    end_column = _sheet_column_name(max(_column_number("AC"), int(grid.get("columnCount") or 0)) - 1)
    clear_range = f"'{title.replace(chr(39), chr(39) * 2)}'!A{header_idx + 2}:{end_column}{end_row}"
    cleared = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchClear",
        json={"ranges": [clear_range]},
        timeout=45,
    )
    cleared.raise_for_status()
    validation_end_row = min(end_row, 300)
    if template and validation_end_row >= header_idx + 2:
        validation_copy = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": [{"copyPaste": {
                "source": {
                    "sheetId": int(template.get("sheetId") or 0),
                    "startRowIndex": header_idx + 1,
                    "endRowIndex": validation_end_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": max(_column_number("AC"), int(grid.get("columnCount") or 0)),
                },
                "destination": {
                    "sheetId": int(target.get("sheetId") or 0),
                    "startRowIndex": header_idx + 1,
                    "endRowIndex": validation_end_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": max(_column_number("AC"), int(grid.get("columnCount") or 0)),
                },
                "pasteType": "PASTE_DATA_VALIDATION",
            }}]},
            timeout=45,
        )
        validation_copy.raise_for_status()
    curator_write = session.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, curator_cell)}",
        params={"valueInputOption": "RAW"},
        json={"majorDimension": "ROWS", "values": [[curator_raw]]},
        timeout=30,
    )
    curator_write.raise_for_status()
    verified = _registry_flow_sheet_status_sync(
        spreadsheet_id,
        credentials_path,
        course_key,
        stream,
        date_start,
        int(target.get("sheetId") or 0),
    )
    if not verified.get("ok"):
        raise RuntimeError(verified.get("error") or "Созданный лист потока не прошёл проверку")
    return {
        "ok": True,
        "status": "created" if created else "continued",
        "spreadsheet_id": spreadsheet_id,
        "sheet_id": int(target.get("sheetId") or 0),
        "sheet_title": title,
        "sheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={int(target.get('sheetId') or 0)}",
        "template_title": _clean((template or {}).get("title"), 300),
        "header_row": header_idx + 1,
        "students_cleared_from_row": header_idx + 2,
        "students_cleared_through_column": end_column,
        "checkboxes_cleared": True,
        "data_validation_restored": bool(template),
        "curator": curator_raw,
        "verified": True,
        "hidden": False,
        "sheet_index": int(verified.get("sheet_index") or 0),
    }


async def service_create_registry_flow_sheet(
    *, course_key: str, stream: str, date_start: str, curator: str,
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    curator_raw = _transfer_curator_raw(settings, curator)
    if not curator_raw:
        raise ValueError("Куратор не поддерживается")
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Таблица учеников Google не настроена")
    result = await asyncio.to_thread(
        _create_registry_flow_sheet_sync,
        spreadsheet_id,
        credentials_path,
        course_key,
        stream,
        date_start,
        curator_raw,
        _clean(settings.get("curator_cell") or "K2", 20).upper(),
    )
    _chat_flows_cache.clear()
    return result


def _registry_date(value: Any) -> str:
    text = _clean(value, 100)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%d.%m.%Y")
    except ValueError:
        return text


def _registry_tariff(value: Any) -> str:
    text = _clean(value, 100)
    return {
        "premium": "Премиум",
        "vip": "ВИП",
        "вип": "ВИП",
        "standard": "Стандарт",
        "mentorship": "Наставничество",
        "module_standard": "Помодульно",
    }.get(_norm(text), text)


def _registry_manager(value: Any) -> str:
    return re.sub(r"\s*\((?:auto|авто)\)\s*$", "", _clean(value, 300), flags=re.IGNORECASE).strip()


def _registry_sheet_context(
    session: Any, spreadsheet_id: str, course_key: str, stream: str, sheet_title: str = "",
) -> tuple[dict[str, Any], str, list[list[Any]], int, dict[str, int], list[dict[str, Any]]]:
    title = _clean(sheet_title, 300)
    if title:
        # Streams already resolved the exact sheet while building its local
        # snapshot. Reusing it removes one metadata request from every lesson
        # click and materially reduces Google Sheets quota pressure.
        props = {"sheetId": 0, "title": title}
    else:
        metadata_response = session.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
            params={"fields": "sheets.properties(sheetId,title,index)"},
            timeout=30,
        )
        metadata_response.raise_for_status()
        sheets = [sheet.get("properties") or {} for sheet in (metadata_response.json() or {}).get("sheets") or []]
        props = next((item for item in sheets if _sheet_title_matches(item.get("title"), course_key, stream)), None)
        if not props:
            raise RuntimeError("Лист потока не найден")
        title = _clean(props.get("title"), 300)
    values_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, 'A1:AC300')}",
        params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"},
        timeout=30,
    )
    values_response.raise_for_status()
    rows = (values_response.json() or {}).get("values") or []
    header_idx, columns = _sheet_student_header(rows)
    if "email" not in columns or "name" not in columns:
        raise RuntimeError("В листе не найдены колонки ФИО и почты")
    return props, title, rows, header_idx, columns, _registry_lesson_columns(rows, header_idx)


def _registry_ensure_student_sync(
    *, spreadsheet_id: str, credentials_path: Path, course_key: str, stream: str,
    student: dict[str, Any],
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    email = _clean(student.get("email"), 300)
    if not _valid_email(email):
        raise RuntimeError("У ученика не найдена почта")
    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    ))
    props, title, rows, header_idx, columns, lesson_columns = _registry_sheet_context(
        session, spreadsheet_id, course_key, stream
    )
    values = {
        "name": _clean(student.get("name"), 300),
        "date": _registry_date(student.get("date")),
        "course": _clean(student.get("course"), 100),
        "tariff": _registry_tariff(student.get("tariff")),
        "enrollment": _clean(student.get("enrollment") or "Геткурс", 100),
        "manager": _registry_manager(student.get("manager_name")),
        "tg_account": _clean(student.get("tg_account"), 500),
        "email": email,
    }
    matches = [
        row_number
        for row_number, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2)
        if _norm(_row_value(row, columns["email"], 300)) == _norm(email)
    ]
    previous_email = _clean(student.get("sheet_email"), 300)
    if not matches and _valid_email(previous_email):
        matches = [
            row_number
            for row_number, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2)
            if _norm(_row_value(row, columns["email"], 300)) == _norm(previous_email)
        ]
    if len(matches) > 1:
        raise RuntimeError("В листе найдено несколько строк ученика")
    if matches:
        target_row = matches[0]
        current_row = rows[target_row - 1] if target_row <= len(rows) else []
        writes_by_range = {
            _a1_range(title, f"{_column_letters(columns[key] + 1)}{target_row}"): value
            for key, value in values.items()
            if key in columns and value and _norm(_row_value(current_row, columns[key], 1000)) != _norm(value)
        }
        if writes_by_range:
            write_response = session.post(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
                json={
                    "valueInputOption": "USER_ENTERED",
                    "data": [{"range": cell, "values": [[value]]} for cell, value in writes_by_range.items()],
                },
                timeout=45,
            )
            write_response.raise_for_status()
            verify_response = session.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, f'A{target_row}:AC{target_row}')}",
                params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"}, timeout=30,
            )
            verify_response.raise_for_status()
            verify_row = ((verify_response.json() or {}).get("values") or [[]])[0]
            if _norm(_row_value(verify_row, columns["email"], 300)) != _norm(email):
                raise RuntimeError("Google не подтвердил обновление строки")
        return {
            "ok": True, "status": "updated_existing" if writes_by_range else "already_exists", "row": target_row,
            "sheet_id": int(props.get("sheetId") or 0), "sheet_title": title,
            "lesson_columns": [{"key": item["key"], "label": item["label"]} for item in lesson_columns],
        }
    identity_columns = set(columns.values())
    target_row = next(
        (
            row_number
            for row_number in range(header_idx + 2, 301)
            if not any(
                _row_value(rows[row_number - 1] if row_number <= len(rows) else [], column, 1000)
                for column in identity_columns
            )
        ),
        0,
    )
    if not target_row:
        raise RuntimeError("В листе потока нет свободной строки")
    template_row = next(
        (
            row_number
            for row_number in range(target_row - 1, header_idx + 1, -1)
            if _valid_email(_row_value(rows[row_number - 1] if row_number <= len(rows) else [], columns["email"], 300))
        ),
        0,
    )
    if not template_row:
        template_row = next(
            (
                row_number
                for row_number, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2)
                if _valid_email(_row_value(row, columns["email"], 300))
            ),
            0,
        )
    formatting_requests: list[dict[str, Any]] = []
    if template_row:
        # Empty staff rows are deliberately preformatted.  Copying a filled
        # student's FORMAT also copies one-off red/blue manual fills.  Preserve
        # the target row exactly and restore only controls/validation.
        formatting_requests.append({"copyPaste": {
            "source": {"sheetId": int(props["sheetId"]), "startRowIndex": template_row - 1, "endRowIndex": template_row, "startColumnIndex": 0, "endColumnIndex": _column_number("AC")},
            "destination": {"sheetId": int(props["sheetId"]), "startRowIndex": target_row - 1, "endRowIndex": target_row, "startColumnIndex": 0, "endColumnIndex": _column_number("AC")},
            "pasteType": "PASTE_DATA_VALIDATION",
        }})
    formatting_requests.extend(_registry_checkbox_validation_requests(
        int(props["sheetId"]),
        [target_row],
        [int(item["column"]) for item in lesson_columns],
    ))
    if formatting_requests:
        copy_response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": formatting_requests},
            timeout=45,
        )
        copy_response.raise_for_status()
    writes_by_range = {
        _a1_range(title, f"{_column_letters(columns[key] + 1)}{target_row}"): value
        for key, value in values.items()
        if key in columns and value
    }
    writes_by_range.update({
        _a1_range(title, f"{item['key']}{target_row}"): False
        for item in lesson_columns
    })
    writes = [{"range": cell, "values": [[value]]} for cell, value in writes_by_range.items()]
    write_response = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
        json={"valueInputOption": "USER_ENTERED", "data": writes},
        timeout=45,
    )
    write_response.raise_for_status()
    verify_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, f'A{target_row}:AC{target_row}')}",
        params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"}, timeout=30,
    )
    verify_response.raise_for_status()
    verify_row = ((verify_response.json() or {}).get("values") or [[]])[0]
    if _norm(_row_value(verify_row, columns["email"], 300)) != _norm(email):
        raise RuntimeError("Google не подтвердил созданную строку")
    if any(_registry_lesson_values(verify_row, lesson_columns).values()):
        raise RuntimeError("Google не очистил отметки новой строки")
    return {
        "ok": True, "status": "created", "row": target_row,
        "sheet_id": int(props.get("sheetId") or 0), "sheet_title": title,
        "lesson_columns": [{"key": item["key"], "label": item["label"]} for item in lesson_columns],
    }


def _column_letters(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


async def service_registry_ensure_student(
    *, course_key: str, stream: str, student: dict[str, Any]
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Google Sheets не настроен")
    async with _registry_write_lock:
        return await asyncio.to_thread(
            _registry_ensure_student_sync,
            spreadsheet_id=spreadsheet_id, credentials_path=credentials_path,
            course_key=_clean(course_key, 50), stream=_clean(stream, 50), student=student,
        )


def _registry_write_lesson_sync(
    *, spreadsheet_id: str, credentials_path: Path, course_key: str, stream: str,
    email: str, source_row: int, lesson_key: str, value: bool, expected_value: bool,
    sheet_title: str = "",
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    session = AuthorizedSession(Credentials.from_service_account_file(
        str(credentials_path), scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
    ))
    try:
        _props, title, rows, header_idx, columns, lesson_columns = _registry_sheet_context(
            session, spreadsheet_id, course_key, stream, sheet_title
        )
    except Exception as exc:
        title = _clean(sheet_title, 300)
        if not title or not re.search(r"(?:\b429\b|too many requests|resource_exhausted|quota)", str(exc), re.I):
            raise
        # The Sheets read quota is shared with background synchronization.
        # The authenticated XLSX export uses the Drive path, so it can verify
        # the exact email/row before a write even while Sheets reads are being
        # throttled. Never fall back to an unchecked direct cell write.
        now_monotonic = time.monotonic()
        cached_rows = (
            _registry_xlsx_export_cache.get("rows")
            if _registry_xlsx_export_cache.get("key") == spreadsheet_id
            and float(_registry_xlsx_export_cache.get("expires") or 0) > now_monotonic
            else {}
        )
        if not cached_rows:
            export_response = session.get(
                f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export",
                params={"format": "xlsx"}, timeout=120,
            )
            export_response.raise_for_status()
            cached_rows = _registry_xlsx_rows(export_response.content)
            _registry_xlsx_export_cache.update({
                "key": spreadsheet_id,
                "expires": now_monotonic + 60,
                "rows": cached_rows,
            })
        rows = cached_rows.get(title) or []
        if not rows:
            raise RuntimeError("Лист потока не найден в резервной копии таблицы") from exc
        header_idx, columns = _sheet_student_header(rows)
        if "email" not in columns or "name" not in columns:
            raise RuntimeError("В листе не найдены колонки ФИО и почты") from exc
        lesson_columns = _registry_lesson_columns(rows, header_idx)
    lesson = next((item for item in lesson_columns if item["key"] == _clean(lesson_key, 5).upper()), None)
    if not lesson:
        raise RuntimeError("Колонка прогресса не найдена")
    matches = [
        row_number
        for row_number, row in enumerate(rows[header_idx + 1 :], start=header_idx + 2)
        if _norm(_row_value(row, columns["email"], 300)) == _norm(email)
    ]
    if len(matches) != 1:
        raise RuntimeError("Строка ученика изменилась; обновите данные")
    row_number = matches[0]
    if source_row and row_number != int(source_row):
        raise RuntimeError("Строка ученика изменилась; обновите данные")
    row = rows[row_number - 1] if row_number <= len(rows) else []
    current = bool(_registry_lesson_values(row, [lesson])[lesson["key"]])
    if current == bool(value):
        return {"ok": True, "status": "already_updated", "row": row_number, "value": current}
    if current != bool(expected_value):
        raise RuntimeError("Таблица уже изменена; обновите данные")
    cell = f"{lesson['key']}{row_number}"
    response = session.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(title, cell)}",
        params={
            "valueInputOption": "RAW", "includeValuesInResponse": "true",
            "responseValueRenderOption": "UNFORMATTED_VALUE",
        },
        json={"majorDimension": "ROWS", "values": [[bool(value)]]}, timeout=30,
    )
    response.raise_for_status()
    updated = (((response.json() or {}).get("updatedData") or {}).get("values") or [[None]])[0][0]
    confirmed = _registry_lesson_values([updated], [{"key": "value", "column": 0}])["value"]
    if confirmed != bool(value):
        raise RuntimeError("Google не подтвердил изменение")
    return {"ok": True, "status": "updated", "row": row_number, "value": bool(value)}


async def service_registry_write_lesson(
    *, course_key: str, stream: str, email: str, source_row: int,
    lesson_key: str, value: bool, expected_value: bool, sheet_title: str = "",
) -> dict[str, Any]:
    settings = await _settings_map()
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Google Sheets не настроен")
    async with _registry_write_lock:
        return await asyncio.to_thread(
            _registry_write_lesson_sync,
            spreadsheet_id=spreadsheet_id, credentials_path=credentials_path,
            course_key=_clean(course_key, 50), stream=_clean(stream, 50),
            email=_clean(email, 300), source_row=int(source_row or 0),
            lesson_key=_clean(lesson_key, 5).upper(), value=bool(value), expected_value=bool(expected_value),
            sheet_title=_clean(sheet_title, 300),
        )


def _transfer_student_matches(snapshot: dict[str, Any], email: str) -> list[dict[str, Any]]:
    email_key = _norm(email)
    matches: list[dict[str, Any]] = []
    for flow in snapshot.get("items") or []:
        for student in flow.get("students") or []:
            if _norm(student.get("email")) == email_key:
                matches.append({"flow": flow, "student": student})
    return matches


def _transfer_sheet_move_sync(
    *,
    spreadsheet_id: str,
    credentials_path: Path,
    source_sheet_id: int,
    source_sheet_title: str,
    source_row: int,
    target_sheet_id: int,
    target_sheet_title: str,
    students_range: str,
    email: str,
    target_course: str,
    target_curator: str,
    student: dict[str, Any] | None = None,
    move: bool = True,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    session = AuthorizedSession(credentials)
    end_row_match = re.search(r":?[A-Z]{1,3}(\d+)$", students_range)
    end_row = int(end_row_match.group(1)) if end_row_match else 300
    end_column = _column_number("AC")
    source_range = _a1_range(source_sheet_title, students_range)
    target_range = _a1_range(target_sheet_title, students_range)
    values_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params=[("majorDimension", "ROWS"), ("ranges", source_range), ("ranges", target_range)],
        timeout=30,
    )
    values_response.raise_for_status()
    value_ranges = (values_response.json() or {}).get("valueRanges") or []
    source_rows = (value_ranges[0] or {}).get("values") or [] if value_ranges else []
    target_rows = (value_ranges[1] or {}).get("values") or [] if len(value_ranges) > 1 else []
    source_header_idx, source_columns = _sheet_student_header(source_rows)
    header_idx, columns = _sheet_student_header(target_rows)
    source_email_col = source_columns.get("email", 6)
    email_col = columns.get("email", 6)
    existing_target_rows = [
        index
        for index, row in enumerate(target_rows[header_idx + 1 :], start=header_idx + 2)
        if _norm(_row_value(row, email_col, 300)) == _norm(email)
    ]
    source_values = source_rows[source_row - 1] if 0 < source_row <= len(source_rows) else []
    source_has_email = _norm(_row_value(source_values, source_email_col, 300)) == _norm(email)
    delete_source_row = {
        "deleteDimension": {
            "range": {
                "sheetId": int(source_sheet_id),
                "dimension": "ROWS",
                "startIndex": int(source_row) - 1,
                "endIndex": int(source_row),
            }
        }
    }
    if existing_target_rows:
        if len(existing_target_rows) > 1:
            raise RuntimeError("В целевом потоке несколько строк ученика")
        if move and source_has_email and not (
            source_sheet_id == target_sheet_id and source_row in existing_target_rows
        ):
            response = session.post(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
                json={"requests": [delete_source_row]}, timeout=30,
            )
            response.raise_for_status()
            return {
                "ok": True, "status": "duplicate_removed", "target_row": existing_target_rows[0],
                "source_row_deleted": True,
            }
        return {
            "ok": True,
            "status": "already_moved" if move else "already_copied",
            "target_row": existing_target_rows[0],
            "source_row_deleted": False,
        }
    if not source_has_email:
        raise RuntimeError("Исходная строка изменилась; обновите данные ученика")
    target_row = next(
        (
            row_number
            for row_number in range(header_idx + 2, end_row + 1)
            if not any(
                _row_value(
                    target_rows[row_number - 1] if row_number <= len(target_rows) else [],
                    column,
                    1000,
                )
                for column in set(columns.values())
                if column not in {columns.get("buyers")}
            )
        ),
        0,
    )
    if not target_row:
        raise RuntimeError("В целевом потоке нет свободной строки")
    template_row = next(
        (
            row_number for row_number in range(target_row - 1, header_idx + 1, -1)
            if _valid_email(_row_value(target_rows[row_number - 1], email_col, 300))
        ),
        0,
    )
    if template_row:
        response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": [
                {"copyPaste": {
                    "source": {
                        "sheetId": int(target_sheet_id), "startRowIndex": template_row - 1,
                        "endRowIndex": template_row, "startColumnIndex": 0, "endColumnIndex": end_column,
                    },
                    "destination": {
                        "sheetId": int(target_sheet_id), "startRowIndex": target_row - 1,
                        "endRowIndex": target_row, "startColumnIndex": 0, "endColumnIndex": end_column,
                    },
                    "pasteType": paste_type,
                }}
                for paste_type in ("PASTE_FORMAT", "PASTE_DATA_VALIDATION")
            ]},
            timeout=45,
        )
        response.raise_for_status()

    source_lessons = {
        _norm(item.get("label")): bool(_registry_lesson_values(source_values, [item])[item["key"]])
        for item in _registry_lesson_columns(source_rows, source_header_idx)
        if _norm(item.get("label"))
    }
    supplied = student if isinstance(student, dict) else {}
    semantic_values: dict[str, Any] = {
        "name": _row_value(source_values, source_columns.get("name"), 300),
        "date": source_values[source_columns["date"]] if source_columns.get("date") is not None and source_columns["date"] < len(source_values) else "",
        "course": target_course,
        "tariff": _registry_tariff(_row_value(source_values, source_columns.get("tariff"), 100)),
        "enrollment": _row_value(source_values, source_columns.get("enrollment"), 100) or "Геткурс",
        "manager": _registry_manager(supplied.get("manager_name") or _row_value(source_values, source_columns.get("manager"), 300)),
        "tg_account": _clean(supplied.get("tg_account") or _row_value(source_values, source_columns.get("tg_account"), 500), 500),
        "email": email,
    }
    writes = [
        {
            "range": _a1_range(target_sheet_title, f"{_column_letters(columns[key] + 1)}{target_row}"),
            "values": [[value]],
        }
        for key, value in semantic_values.items()
        if key in columns and value != ""
    ]
    if columns.get("buyers") is not None:
        source_buyers = source_columns.get("buyers")
        value = bool(source_values[source_buyers]) if source_buyers is not None and source_buyers < len(source_values) else False
        writes.append({
            "range": _a1_range(target_sheet_title, f"{_column_letters(columns['buyers'] + 1)}{target_row}"),
            "values": [[value]],
        })
    target_lesson_columns = _registry_lesson_columns(target_rows, header_idx)
    for item in target_lesson_columns:
        # Copying a student without deleting the old row means granting access to
        # another flow, not transferring completed lessons. Keep operational
        # marks such as "Чат", but start every learning-progress checkbox empty.
        lesson_value = source_lessons.get(_norm(item.get("label")), False)
        if not move and not _registry_service_checkbox(item.get("label")):
            lesson_value = False
        writes.append({
            "range": _a1_range(target_sheet_title, f"{item['key']}{target_row}"),
            "values": [[lesson_value]],
        })
    response = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
        json={"valueInputOption": "USER_ENTERED", "data": writes}, timeout=45,
    )
    response.raise_for_status()
    verify = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(target_sheet_title, f'A{target_row}:AC{target_row}')}",
        params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"}, timeout=30,
    )
    verify.raise_for_status()
    verified = ((verify.json() or {}).get("values") or [[]])[0]
    if _norm(_row_value(verified, email_col, 300)) != _norm(email):
        raise RuntimeError("Google не подтвердил строку в целевом потоке")
    if not move:
        verified_lessons = _registry_lesson_values(verified, target_lesson_columns)
        uncleared = [
            item["label"]
            for item in target_lesson_columns
            if not _registry_service_checkbox(item.get("label")) and verified_lessons.get(item["key"])
        ]
        if uncleared:
            raise RuntimeError("Google не очистил прогресс в новом потоке")
    if move:
        response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
            json={"requests": [delete_source_row]}, timeout=30,
        )
        response.raise_for_status()
    return {
        "ok": True,
        "status": "moved" if move else "copied",
        "target_row": target_row,
        "source_row_deleted": move,
        "progress_reset": not move,
    }


async def service_transfer_move_student(
    *, email: str, source_course_key: str, source_stream: str, source_row: int,
    target_course_key: str, target_stream: str, student: dict[str, Any] | None = None,
    move: bool = True,
) -> dict[str, Any]:
    settings = await _settings_map()
    snapshot = await _flow_students(settings, refresh=False)
    if not snapshot.get("ok"):
        raise RuntimeError("Не удалось обновить таблицу потоков")
    source_flow = next(
        (
            flow for flow in snapshot.get("items") or []
            if _clean(flow.get("course_key")) == _clean(source_course_key)
            and _clean(flow.get("stream")) == _clean(source_stream)
        ),
        None,
    )
    target = next(
        (
            flow for flow in snapshot.get("items") or []
            if _clean(flow.get("course_key")) == _clean(target_course_key)
            and _clean(flow.get("stream")) == _clean(target_stream)
        ),
        None,
    )
    if not target:
        raise RuntimeError("Целевой поток не найден")
    if not source_flow:
        raise RuntimeError("Исходный поток не найден")
    if source_course_key == target_course_key and str(source_stream) == str(target_stream):
        return {"ok": True, "status": "already_in_target", "target": target, "student": student or {}}
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists():
        raise RuntimeError("Google Sheets не настроен")
    async with _registry_write_lock:
        result = await asyncio.to_thread(
            _transfer_sheet_move_sync,
            spreadsheet_id=spreadsheet_id,
            credentials_path=credentials_path,
            source_sheet_id=int(source_flow.get("sheet_id") or 0),
            source_sheet_title=_clean(source_flow.get("sheet_title"), 300),
            source_row=int(source_row),
            target_sheet_id=int(target.get("sheet_id") or 0),
            target_sheet_title=_clean(target.get("sheet_title"), 300),
            students_range=_students_sheet_range(settings),
            email=_clean(email, 300),
            target_course=_clean(target.get("course"), 100),
            target_curator=_clean(target.get("curator_raw") or target.get("curator_value"), 200),
            student=student,
            move=move,
        )
    return {
        **result,
        "target": target,
        "student": {**(student or {}), "row": int(result.get("target_row") or 0)},
    }


def _transfer_curator_raw(settings: dict[str, str], curator: str) -> str:
    raw_map = _clean(settings.get("curator_map") or DEFAULT_CURATOR_MAP, 5000)
    for part in re.split(r"[;\n]+", raw_map):
        separator = "=>" if "=>" in part else ("=" if "=" in part else (":" if ":" in part else ""))
        if not separator:
            continue
        marker, value = part.split(separator, 1)
        if _clean(value, 100) == _clean(curator, 100):
            return _clean(marker, 200)
    return ""


def _transfer_sheet_curator_sync(
    *, spreadsheet_id: str, credentials_path: Path, sheet_id: int, sheet_title: str,
    students_range: str, source_row: int, email: str, curator_raw: str,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    credentials = Credentials.from_service_account_file(
        str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    session = AuthorizedSession(credentials)
    values_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{_a1_range(sheet_title, students_range)}",
        params={"majorDimension": "ROWS"}, timeout=30,
    )
    values_response.raise_for_status()
    rows = (values_response.json() or {}).get("values") or []
    _, columns = _sheet_student_header(rows)
    start_match = re.match(r"[A-Z]{1,3}(\d+)", students_range)
    start_row = int(start_match.group(1)) if start_match else 1
    row_index = int(source_row) - start_row
    if row_index < 0 or row_index >= len(rows):
        raise RuntimeError("Исходная строка изменилась; обновите таблицу")
    email_col = columns.get("email", 6)
    curator_col = columns.get("responsible_curator")
    if curator_col is None:
        raise RuntimeError("В таблице не найдена колонка куратора")
    row = rows[row_index]
    if _norm(_row_value(row, email_col, 300)) != _norm(email):
        raise RuntimeError("Исходная строка изменилась; обновите таблицу")
    if _norm(_row_value(row, curator_col, 200)) == _norm(curator_raw):
        return {"ok": True, "status": "already_updated", "row": int(source_row), "curator": curator_raw}
    response = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
        json={"requests": [{"updateCells": {
            "range": {
                "sheetId": int(sheet_id),
                "startRowIndex": int(source_row) - 1,
                "endRowIndex": int(source_row),
                "startColumnIndex": int(curator_col),
                "endColumnIndex": int(curator_col) + 1,
            },
            "rows": [{"values": [{"userEnteredValue": {"stringValue": curator_raw}}]}],
            "fields": "userEnteredValue",
        }}]}, timeout=45,
    )
    response.raise_for_status()
    return {"ok": True, "status": "updated", "row": int(source_row), "curator": curator_raw}


async def service_transfer_update_student_curator(
    *, email: str, source_course_key: str, source_stream: str, source_row: int, curator: str,
) -> dict[str, Any]:
    settings = await _settings_map()
    allowed = {value for _, value in _curator_name_map(settings)}
    if curator not in allowed:
        raise RuntimeError("Куратор не поддерживается")
    snapshot = await _flow_students(settings, refresh=False)
    source_flow = next(
        (
            flow for flow in snapshot.get("items") or []
            if _clean(flow.get("course_key")) == _clean(source_course_key)
            and _clean(flow.get("stream")) == _clean(source_stream)
        ),
        None,
    )
    if not source_flow:
        raise RuntimeError("Исходный поток не найден")
    source_student = next(
        (
            item for item in source_flow.get("students") or []
            if _norm(item.get("email")) == _norm(email)
            and int(item.get("row") or 0) == int(source_row)
        ),
        None,
    )
    if source_student and _clean(source_student.get("responsible_curator"), 100) == curator:
        return {"ok": True, "status": "already_updated", "row": int(source_row), "curator": curator}
    spreadsheet_id = _curator_spreadsheet_id(settings)
    credentials_path = _curator_credentials_path(settings)
    curator_raw = _transfer_curator_raw(settings, curator)
    if not spreadsheet_id or not credentials_path or not credentials_path.exists() or not curator_raw:
        raise RuntimeError("Google Sheets или соответствие куратора не настроено")
    result = await asyncio.to_thread(
        _transfer_sheet_curator_sync,
        spreadsheet_id=spreadsheet_id,
        credentials_path=credentials_path,
        sheet_id=int(source_flow.get("sheet_id") or 0),
        sheet_title=_clean(source_flow.get("sheet_title"), 300),
        students_range=_students_sheet_range(settings),
        source_row=int(source_row),
        email=_clean(email, 300),
        curator_raw=curator_raw,
    )
    return {**result, "curator_value": curator}


async def service_transfer_write_getcourse(
    *, email: str, gc_user_id: str, order_id: str, deal_number: str,
    target_course_key: str, target_stream: str, target_flow: dict[str, Any] | None = None,
    source_stream: str = "",
) -> dict[str, Any]:
    settings = await _settings_map()
    snapshot = await _flow_students(settings, refresh=False)
    cached_target = next(
        (
            flow for flow in snapshot.get("items") or []
            if _clean(flow.get("course_key")) == _clean(target_course_key)
            and _clean(flow.get("stream")) == _clean(target_stream)
        ),
        None,
    )
    provided = target_flow if isinstance(target_flow, dict) else {}
    provided_matches = (
        _clean(provided.get("course_key")) == _clean(target_course_key)
        and _clean(provided.get("stream")) == _clean(target_stream)
    )
    target = dict(cached_target or {})
    if provided_matches:
        target.update({
            "course_key": _clean(target_course_key, 50),
            "course": _clean(provided.get("course") or target.get("course"), 100),
            "stream": _clean(target_stream, 50),
            "vk_link": _clean(provided.get("vk_link") or target.get("vk_link"), 2000),
            "tg_link": _clean(provided.get("tg_link") or target.get("tg_link"), 2000),
            "curator_value": _clean(
                provided.get("curator_value") or provided.get("curator") or target.get("curator_value"), 100
            ),
        })
    if not target:
        raise RuntimeError("Целевой поток не найден")
    output_fields = {
        settings["field_stream"]: _clean(target_stream, 100),
        settings["field_vk"]: _clean(target.get("vk_link"), 2000),
        settings["field_tg"]: _clean(target.get("tg_link"), 2000),
        settings["field_curator"]: _clean(target.get("curator_value"), 100),
    }
    if not output_fields[settings["field_curator"]] or not (
        output_fields[settings["field_vk"]] or output_fields[settings["field_tg"]]
    ):
        raise RuntimeError("В целевом потоке не заполнены куратор или ссылки")
    user_ok, user_error, user_details = await _write_getcourse_user(
        _clean(gc_user_id, 100),
        _getcourse_user_addfields(output_fields, settings),
        settings,
        email=_clean(email, 300),
    )
    deal_ok, deal_error, deal_details = await _write_getcourse_deal(
        _clean(gc_user_id, 100),
        _clean(deal_number or order_id, 100),
        output_fields,
        settings,
        email=_clean(email, 300),
    )
    error = "; ".join(part for part in (user_error and f"user: {user_error}", deal_error and f"deal: {deal_error}") if part)
    synced = await _sync_gc_fields_write_customer_state(
        {"email": email, "gc_user_id": gc_user_id, "order_id": order_id},
        output_fields,
        {**target, "change_reason": "student_transfer"},
        getcourse_ok=bool(user_ok and deal_ok),
        error=error,
    )
    onboarding_email: dict[str, Any] = {"ok": False, "status": "not_queued"}
    if user_ok and deal_ok:
        onboarding = sys.modules.get("_nexus_mod_getcourse-onboarding")
        queue_email = getattr(onboarding, "service_queue_flow_email", None) if onboarding else None
        if queue_email:
            try:
                onboarding_email = await queue_email(
                    gc_user_id=_clean(gc_user_id, 100),
                    email=_clean(email, 300),
                    order_id=_clean(order_id, 100),
                    course_key=_clean(target_course_key, 50),
                    course=_clean(target.get("course"), 100),
                    source_stream=_clean(source_stream, 50),
                    stream=_clean(target_stream, 50),
                    vk_link=_clean(target.get("vk_link"), 2000),
                    tg_link=_clean(target.get("tg_link"), 2000),
                )
            except Exception as exc:
                onboarding_email = {"ok": False, "status": "failed", "error": _clean(exc, 1000)}
                if _logger:
                    _logger.exception("flow transition email queue failed")
        else:
            onboarding_email = {"ok": False, "status": "module_unavailable"}
            if _logger:
                _logger.error("getcourse-onboarding unavailable after successful flow transfer")
    return {
        "ok": bool(user_ok and deal_ok),
        "error": error,
        "fields": output_fields,
        "user": user_details,
        "deal": deal_details,
        "customer_sync": synced,
        "onboarding_email": onboarding_email,
        "target": target,
    }


async def service_transfer_write_curator(
    *, email: str, gc_user_id: str, order_id: str, deal_number: str, curator: str,
) -> dict[str, Any]:
    settings = await _settings_map()
    if curator not in {value for _, value in _curator_name_map(settings)}:
        raise RuntimeError("Куратор не поддерживается")
    output_fields = {settings["field_curator"]: _clean(curator, 100)}
    user_ok, user_error, user_details = await _write_getcourse_user(
        _clean(gc_user_id, 100),
        _getcourse_user_addfields(output_fields, settings),
        settings,
        email=_clean(email, 300),
    )
    deal_ok, deal_error, deal_details = await _write_getcourse_deal(
        _clean(gc_user_id, 100),
        _clean(deal_number or order_id, 100),
        output_fields,
        settings,
        email=_clean(email, 300),
    )
    error = "; ".join(part for part in (user_error and f"user: {user_error}", deal_error and f"deal: {deal_error}") if part)
    synced = await _sync_gc_fields_write_customer_state(
        {"email": email, "gc_user_id": gc_user_id, "order_id": order_id},
        output_fields,
        {"curator_value": curator, "change_reason": "curator_change"},
        getcourse_ok=bool(user_ok and deal_ok),
        error=error,
    )
    return {
        "ok": bool(user_ok and deal_ok),
        "error": error,
        "fields": output_fields,
        "user": user_details,
        "deal": deal_details,
        "customer_sync": synced,
        "target": {"curator_value": curator},
    }


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID}


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    settings = await _settings_map()
    return {
        **settings,
        "paths": {"customer_db": str(_customer_db_path()), "course_chat_db": str(_course_chat_db_path())},
        "env": {
            "account_name": bool(_env()["account_name"]),
            "api_token": bool(_env()["api_token"]),
            "google_credentials": bool(_curator_credentials_path(settings) and _curator_credentials_path(settings).exists()),
            "google_auth": _google_auth_available(),
            "chat_links_credentials": bool(_chat_links_credentials_path(settings) and _chat_links_credentials_path(settings).exists()),
        },
    }


@router.post("/settings")
async def post_settings(request: Request):
    await _require_user(request)
    data = await request.json()
    if not isinstance(data, dict):
        return JSONResponse({"error": "JSON object required"}, status_code=400)
    return await get_settings_from_map(await _save_settings(data))


async def get_settings_from_map(settings: dict[str, str]):
    return {
        **settings,
        "paths": {"customer_db": str(_customer_db_path()), "course_chat_db": str(_course_chat_db_path())},
        "env": {
            "account_name": bool(_env()["account_name"]),
            "api_token": bool(_env()["api_token"]),
            "google_credentials": bool(_curator_credentials_path(settings) and _curator_credentials_path(settings).exists()),
            "google_auth": _google_auth_available(),
            "chat_links_credentials": bool(_chat_links_credentials_path(settings) and _chat_links_credentials_path(settings).exists()),
        },
    }


@router.get("/latest-chats")
async def latest_chats(request: Request):
    await _require_user(request)
    return {"items": await _latest_chats(), "path": str(_course_chat_db_path())}


@router.post("/registry/homework-checkboxes/repair")
async def repair_registry_homework_checkboxes(request: Request):
    await _require_user(request)
    data = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    if not isinstance(data, dict):
        data = {}
    try:
        return await service_repair_registry_homework_checkboxes(apply=_truthy(data.get("apply")))
    except Exception as exc:
        raise HTTPException(502, _clean(exc, 1000)) from exc


@router.get("/chat-flows")
async def chat_flows(request: Request):
    await _require_user(request)
    settings = await _settings_map()
    return await _chat_flows(settings)


@router.get("/flow-students")
async def flow_students(request: Request, refresh: str = "0"):
    await _require_user(request)
    settings = await _settings_map()
    return await _flow_students(settings, refresh=_truthy(refresh))


@router.post("/flow-students/refresh")
async def flow_students_refresh(request: Request):
    await _require_user(request)
    settings = await _settings_map()
    return await _flow_students(settings, refresh=True)


@router.get("/gc-lookup/status")
async def gc_lookup_status(request: Request):
    await _require_user(request)
    settings = await _settings_map()
    return await _gc_lookup_status(settings)


@router.post("/gc-lookup/enqueue")
async def gc_lookup_enqueue(request: Request):
    await _require_user(request)
    data = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    if not isinstance(data, dict):
        data = {}
    settings = await _settings_map()
    emails: list[str] = []
    if data.get("email"):
        emails.append(_clean(data.get("email"), 300))
    raw_emails = data.get("emails")
    if isinstance(raw_emails, list):
        emails.extend(_clean(email, 300) for email in raw_emails)
    if _truthy(data.get("missing_from_cache")):
        limit = _bounded_int(data.get("limit"), 1, 1000, 50)
        result = await _enqueue_missing_from_students_cache(settings, limit=limit, skip_existing=True)
    else:
        result = await _enqueue_gc_lookup_emails(emails, reason="manual_api")
    result["status"] = await _gc_lookup_status(settings)
    return result


@router.get("/field-write/status")
async def field_write_status(request: Request):
    await _require_user(request)
    settings = await _settings_map()
    return await _gc_fields_write_status(settings)


@router.post("/field-write/enqueue")
async def field_write_enqueue(request: Request):
    await _require_user(request)
    data = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    if not isinstance(data, dict):
        data = {}
    settings = await _settings_map()
    limit = _bounded_int(data.get("limit"), 1, 500, 50)
    result = await _enqueue_gc_fields_write_jobs(settings, limit=limit)
    result["status"] = await _gc_fields_write_status(settings)
    return result


@router.post("/scan")
async def scan_now(request: Request):
    await _require_user(request)
    data = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    result = await _scan_once(force_failed=_truthy((data or {}).get("force_failed")), limit=_bounded_int((data or {}).get("limit"), 1, 1000, 200))
    return result


@router.get("/orders")
async def orders(request: Request, status: str = "all", limit: int = 100):
    await _require_user(request)
    where = ""
    args: list[Any] = []
    if status != "all":
        where = "WHERE status=?"
        args.append(status)
    args.append(max(1, min(500, int(limit))))
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT *
            FROM processed_orders
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(args),
        )
        items = [dict(row) for row in await cur.fetchall()]
        for item in items:
            item["details"] = _json_dict(item.pop("details_json", "{}"))
    return {"items": items}


@router.get("/source-orders")
async def source_orders(request: Request, query: str = "", date_from: str = "", limit: int = 100):
    await _require_user(request)
    settings = await _settings_map()
    return await _source_orders(settings, query=query, date_from=date_from, limit=limit)


@router.get("/runs")
async def runs(request: Request, limit: int = 30):
    await _require_user(request)
    assert _db_path is not None
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM scan_runs ORDER BY id DESC LIMIT ?",
            (max(1, min(200, int(limit))),),
        )
        items = [dict(row) for row in await cur.fetchall()]
        for item in items:
            item["details"] = _json_dict(item.pop("details_json", "{}"))
    return {"items": items}
