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
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
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
_chat_flows_cache: dict[str, Any] = {"key": "", "expires": 0.0, "data": None}

MACHINE_PREFIX = "chat_fields_"
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
    **DEFAULT_USER_FIELD_IDS,
    **DEFAULT_FIELD_NAMES,
}


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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data = DEFAULT_SETTINGS.copy()
    data["start_date"] = _today()
    data.update({str(row[0]): str(row[1] or "") for row in rows})
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, str]:
    allowed = set(DEFAULT_SETTINGS)
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    text = _norm(" ".join(str(fields.get(key) or "") for key in ("title", "positions", "offers")))
    has_puppy = "первые шаги к воспитанию" in text or "щенок" in text
    has_dog = "послушная собака" in text or "современный собаковод" in text
    if has_puppy:
        return "puppy"
    if has_dog:
        return "dog"
    return ""


def _classify_tariff(fields: dict[str, Any]) -> str:
    text = _norm(" ".join(str(fields.get(key) or "") for key in ("title", "positions", "offers")))
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*стандарт", text):
        return "standard"
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*премиум", text):
        return "premium"
    if re.search(r"(?:тариф|пакет)\s*[«\"]?\s*vip", text):
        return "vip"
    return ""


def _is_completed_paid(fields: dict[str, Any]) -> bool:
    status = _norm(fields.get("status"))
    payment = _norm(fields.get("payment_state"))
    return status in {"завершен", "завершён"} and payment == "paid"


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
        "responsible_curator": ("ответственный куратор", "куратор"),
        "tg_account": ("tg аккаунт", "telegram", "телеграм"),
        "email": ("почта", "email", "e-mail"),
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
        best_map.update(mapping)
        return idx, best_map
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
    async with aiosqlite.connect(db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM gc_export_api_calls WHERE datetime(requested_at) >= datetime('now','-2 hours')"
        )
        row = await cur.fetchone()
    return int((row or [0])[0] or 0)


async def _gc_export_budget_left(settings: dict[str, str]) -> int:
    limit = _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    used = await _gc_export_calls_used()
    return max(0, limit - used)


async def _gc_export_next_budget_at(settings: dict[str, str], needed: int = 4) -> str:
    limit = _bounded_int(settings.get("gc_export_lookup_max_requests_2h"), 0, 100, 80)
    used = await _gc_export_calls_used()
    if used <= max(0, limit - needed):
        return ""
    to_expire = used - max(0, limit - needed)
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT datetime(requested_at,'+2 hours')
            FROM gc_export_api_calls
            WHERE datetime(requested_at) >= datetime('now','-2 hours')
            ORDER BY datetime(requested_at) ASC, id ASC
            LIMIT 1 OFFSET ?
            """,
            (max(0, to_expire - 1),),
        )
        row = await cur.fetchone()
    return _clean((row or [""])[0], 40)


async def _record_gc_export_call(purpose: str, details: dict[str, Any]) -> None:
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
        return True, body if isinstance(body, dict) else {"response": body}, ""
    except Exception as exc:
        return False, {}, str(exc)


async def _getcourse_export_rows(path: str, params: dict[str, Any], settings: dict[str, str], purpose: str) -> tuple[list[dict[str, Any]], str]:
    ok, data, error = await _getcourse_export_get(path, params, settings, f"{purpose}:start")
    if not ok:
        return [], error
    export_id = _extract_export_id(data)
    direct_rows = _extract_export_rows(data)
    if direct_rows:
        return direct_rows, ""
    if not export_id:
        return [], "export_id not found in GetCourse response"
    attempts = _bounded_int(settings.get("gc_export_lookup_poll_attempts"), 1, 5, 2)
    delay = _bounded_int(settings.get("gc_export_lookup_poll_delay_seconds"), 0, 20, 2)
    last_error = ""
    for attempt in range(attempts):
        if delay and attempt:
            await asyncio.sleep(delay)
        ok, export_data, error = await _getcourse_export_get(f"/pl/api/account/exports/{urllib.parse.quote(export_id)}", {}, settings, f"{purpose}:poll")
        if not ok:
            last_error = error
            continue
        rows = _extract_export_rows(export_data)
        if rows:
            return rows, ""
        last_error = _clean(export_data.get("status") or export_data.get("state") or "export is not ready", 300)
    return [], last_error or "export is not ready"


async def _load_gc_lookup_cache(emails: list[str], settings: dict[str, str]) -> dict[str, dict[str, Any]]:
    if not emails:
        return {}
    cache_days = _bounded_int(settings.get("gc_export_lookup_cache_days"), 1, 365, 30)
    placeholders = ",".join("?" for _ in emails)
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    if budget_left < 4:
        return {"queued": 0, "emails": [], "reason": "budget_low", "requests_left_2h": budget_left}
    configured_batch = _bounded_int(settings.get("gc_export_lookup_auto_enqueue_batch_size"), 1, 100, 20)
    batch = max(1, min(configured_batch, budget_left // 4))
    result = await _enqueue_missing_from_students_cache(settings, limit=batch, skip_existing=True)
    result["requests_left_2h"] = budget_left
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
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT custom_fields FROM cdb_getcourse_orders WHERE id=?", (record_id,))
        row = await cur.fetchone()
    fields = _json_dict((row or [""])[0])
    return _clean(fields.get("number") or fields.get("deal_number") or fields.get("order_number"), 100)


async def _lookup_cache_by_email(emails: list[str]) -> dict[str, dict[str, Any]]:
    if not emails:
        return {}
    placeholders = ",".join("?" for _ in emails)
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM gc_export_lookup_cache WHERE email IN ({placeholders})",
            tuple(emails),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    return {_norm(row.get("email")): row for row in rows}


async def _existing_gc_write_job_keys() -> set[tuple[str, str]]:
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT email, order_id
            FROM gc_fields_write_jobs
            WHERE status IN ('completed','pending','running')
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


async def _gc_lookup_status(settings: dict[str, str] | None = None) -> dict[str, Any]:
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
        async with aiosqlite.connect(_db_path) as db_read:
            cur = await db_read.execute("SELECT attempts FROM gc_export_lookup_jobs WHERE id=?", (int(job_id),))
            row = await cur.fetchone()
            attempts = int((row or [1])[0] or 1)
        delay_seconds = min(3600, 60 * attempts * attempts)
    next_run_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"
    if delay_seconds:
        next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{int(delay_seconds)} seconds')"
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO flow_students_cache(key,value_json,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (cache_key, json.dumps(data, ensure_ascii=False), updated_at),
        )
        await db.commit()


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
        flows = (await _chat_flows_base(settings)).get("items") or []
        order_index = await _customer_order_index(settings)
        try:
            data = await asyncio.to_thread(_flow_students_snapshot_sync, spreadsheet_id, credentials_path, flows, settings, order_index)
        except Exception as exc:
            stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
            if stale:
                stale["stale_due_error"] = _clean(str(exc), 1000)
                return stale
            return {"ok": False, "items": [], "errors": [{"status": "google_error", "error": str(exc)}]}
        data["cached"] = False
        data["cache_age_seconds"] = 0
        data["cache_minutes"] = ttl
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
        else:
            stale = await _load_flow_students_cache(cache_key, ttl, allow_stale=True)
            if stale:
                stale["stale_due_error"] = "; ".join(_clean(error.get("error"), 300) for error in data.get("errors") or [])[:1000]
                return stale
        return data


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
        return cached
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
    data = {"items": items, "errors": errors, "ok": not errors}
    if items:
        _chat_flows_cache.update({"key": cache_key, "expires": time.monotonic() + 600, "data": data})
    return data


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
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        for course_key in result:
            for platform in ("vk", "telegram"):
                cur = await db.execute(
                    """
                    SELECT id,platform,title,stream_number,date_start,course_key,status,link,chat_id,created_at
                    FROM runs
                    WHERE course_key=? AND platform=? AND test_mode=0 AND COALESCE(link,'')<>'' AND status<>'error'
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (course_key, platform),
                )
                row = await cur.fetchone()
                result[course_key][platform] = dict(row) if row else None
    return result


async def _customer_rows(settings: dict[str, str], limit: int) -> list[dict[str, Any]]:
    db_path = _customer_db_path()
    if not db_path.exists():
        return []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, platform_id, custom_fields, created_at, updated_at
            FROM cdb_getcourse_orders
            WHERE datetime(COALESCE(updated_at, created_at)) >= datetime(?)
            ORDER BY datetime(COALESCE(updated_at, created_at)) ASC, id ASC
            LIMIT ?
            """,
            (settings.get("start_date") or _today(), max(1, min(5000, int(limit)))),
        )
        return [dict(row) for row in await cur.fetchall()]


def _source_order_summary(row: dict[str, Any], state: dict[str, Any] | None) -> dict[str, Any]:
    fields = _json_dict(row.get("custom_fields"))
    course_key = _classify_course(fields)
    tariff = _classify_tariff(fields)
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
        "eligible": bool(_is_completed_paid(fields) and course_key),
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
    async with aiosqlite.connect(db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    if status == "skipped":
        return True
    return False


async def _update_customer_fields(record_id: int, fields: dict[str, Any], patch: dict[str, Any]) -> None:
    db_path = _customer_db_path()
    merged = dict(fields)
    merged.update(patch)
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            UPDATE cdb_getcourse_orders
            SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE id=?
            """,
            (json.dumps(merged, ensure_ascii=False), record_id),
        )
        await db.commit()


def _getcourse_user_payload(gc_user_id: str, fields: dict[str, Any], email: str = "", phone: str = "") -> dict[str, Any]:
    user = {
        "id": str(gc_user_id),
        "addfields": dict(fields),
    }
    if email:
        user["email"] = email
    if phone:
        user["phone"] = phone
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
    if email:
        user["email"] = email
    if phone:
        user["phone"] = phone
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
    if await _gc_export_budget_left(settings) <= 0:
        return False, "лимит GetCourse API для модуля исчерпан", {}
    encoded = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("ascii")
    url = f"https://{env['account_name']}.getcourse.ru{path}"
    form = {"action": action, "key": env["api_token"], "params": encoded}
    timeout = _bounded_int(settings.get("request_timeout"), 5, 60, 20)
    await _record_gc_export_call(purpose, {"path": path, "action": action})
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


async def _enqueue_gc_fields_write_jobs(settings: dict[str, str], limit: int = 50) -> dict[str, Any]:
    candidates = await _fields_write_candidates_from_cache(settings, limit=limit)
    if not candidates:
        return {"queued": 0, "candidates": 0, "items": []}
    queued = 0
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
        for item in candidates:
            payload = {
                "fields": item["fields"],
                "user_fields": item["user_fields"],
                "flow": item["flow"],
            }
            cur = await db.execute(
                """
                INSERT INTO gc_fields_write_jobs(email,gc_user_id,order_id,deal_number,status,last_error,payload_json,result_json,updated_at)
                VALUES(?,?,?,?, 'pending', '', ?, '{}', ?)
                ON CONFLICT(email, order_id) DO UPDATE SET
                    status='pending',
                    next_run_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'),
                    last_error='',
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                WHERE gc_fields_write_jobs.status NOT IN ('completed','pending','running')
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


async def _gc_fields_write_status(settings: dict[str, str] | None = None) -> dict[str, Any]:
    active_settings = settings or await _settings_map()
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
        "next_budget_at": await _gc_export_next_budget_at(active_settings, needed=2),
    }


async def _claim_gc_fields_write_job(settings: dict[str, str]) -> dict[str, Any] | None:
    max_attempts = _bounded_int(settings.get("gc_fields_write_job_max_attempts"), 1, 10, 3)
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT *
            FROM gc_fields_write_jobs
            WHERE status IN ('pending','failed')
              AND attempts < ?
              AND datetime(next_run_at) <= datetime('now')
            ORDER BY datetime(next_run_at) ASC, id ASC
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


async def _finish_gc_fields_write_job(job_id: int, status: str, error: str = "", result: dict[str, Any] | None = None) -> None:
    delay_seconds = 0
    if status == "failed":
        assert _db_path is not None
        async with aiosqlite.connect(_db_path) as db_read:
            cur = await db_read.execute("SELECT attempts FROM gc_fields_write_jobs WHERE id=?", (int(job_id),))
            row = await cur.fetchone()
        attempts = int((row or [1])[0] or 1)
        delay_seconds = min(3600, 60 * attempts * attempts)
    next_run_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"
    if delay_seconds:
        next_run_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ','now','+{int(delay_seconds)} seconds')"
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    if await _gc_export_budget_left(settings) < 2:
        await _defer_gc_fields_write_job(int(job["id"]), "GetCourse API budget low; deferred", delay_seconds=600)
        return
    payload = _json_dict(job.get("payload_json"))
    fields = {key: value for key, value in _json_dict(payload.get("fields")).items() if _clean(value)}
    user_fields = {key: value for key, value in _json_dict(payload.get("user_fields")).items() if _clean(value)}
    required_names = {settings["field_stream"], settings["field_vk"], settings["field_tg"], settings["field_curator"]}
    if not required_names.issubset(set(fields)):
        await _finish_gc_fields_write_job(int(job["id"]), "skipped", "required non-empty fields are missing", {"fields": fields})
        return
    gc_user_id = _clean(job.get("gc_user_id"), 100)
    deal_number = _clean(job.get("deal_number"), 100)
    email = _clean(job.get("email"), 300)
    user_ok, user_error, user_details = await _write_getcourse_user(gc_user_id, user_fields, settings, email=email)
    if user_error and "лимит GetCourse API" in user_error:
        await _defer_gc_fields_write_job(int(job["id"]), user_error, delay_seconds=600, result={"user": user_details})
        return
    deal_ok, deal_error, deal_details = await _write_getcourse_deal(gc_user_id, deal_number, fields, settings, email=email)
    if deal_error and "лимит GetCourse API" in deal_error:
        await _defer_gc_fields_write_job(int(job["id"]), deal_error, delay_seconds=600, result={"user": user_details, "deal": deal_details})
        return
    ok = bool(user_ok and deal_ok)
    error = "; ".join(part for part in [user_error and f"user: {user_error}", deal_error and f"deal: {deal_error}"] if part)
    await _finish_gc_fields_write_job(
        int(job["id"]),
        "completed" if ok else "failed",
        error,
        {"user": user_details, "deal": deal_details, "fields": fields, "user_fields": user_fields},
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
            async with _gc_write_lock:
                job = await _claim_gc_fields_write_job(settings)
                if job:
                    await _process_gc_fields_write_job(job, settings)
                elif await _gc_export_budget_left(settings) >= 2:
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
    async with aiosqlite.connect(_db_path) as db:
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


async def _process_row(row: dict[str, Any], chats: dict[str, dict[str, dict[str, Any] | None]], settings: dict[str, str], state: dict[str, Any] | None, force: bool = False) -> dict[str, Any]:
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
    if not _is_completed_paid(fields):
        await _mark_processed({**base, "status": "skipped", "error": "order is not completed paid", "details": {"status": fields.get("status"), "payment_state": fields.get("payment_state")}})
        return {"action": "skipped"}
    course_key = _classify_course(fields)
    tariff = _classify_tariff(fields)
    if not course_key:
        await _mark_processed({**base, "status": "failed", "error": "course not detected", "details": {"title": fields.get("title")}})
        return {"action": "failed"}
    course_chats = chats.get(course_key) or {}
    vk_chat = course_chats.get("vk")
    tg_chat = course_chats.get("telegram")
    if not vk_chat or not tg_chat:
        await _mark_processed({**base, "status": "failed", "course_key": course_key, "tariff": tariff, "error": "latest VK/TG chat not found", "details": {"latest_chats": course_chats}})
        return {"action": "failed"}
    stream = _stream_number(
        vk_chat.get("stream_number") if vk_chat else "",
        tg_chat.get("stream_number") if tg_chat else "",
        vk_chat.get("title") if vk_chat else "",
        tg_chat.get("title") if tg_chat else "",
    )
    curator = await _resolve_curator(course_key, stream, settings)
    if not curator.get("ok"):
        await _mark_processed({
            **base,
            "status": "pending_curator",
            "course_key": course_key,
            "tariff": tariff,
            "stream": stream,
            "error": _clean(curator.get("error") or curator.get("status"), 2000),
            "details": {"latest_chats": {"vk": vk_chat, "telegram": tg_chat}, "curator": curator},
        })
        return {"action": "pending_curator", "error": curator.get("error")}
    is_standard = tariff == "standard"
    link_result: dict[str, Any] = {"ok": True, "vk": {}, "telegram": {}, "standard_no_links": True}
    if not is_standard:
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
                },
            })
            return {"action": "pending_chat_links", "error": link_result.get("error")}
    output_fields = {
        settings["field_stream"]: stream,
        settings["field_vk"]: "" if is_standard else _clean((link_result.get("vk") or {}).get("link"), 2000),
        settings["field_tg"]: "" if is_standard else _clean((link_result.get("telegram") or {}).get("link"), 2000),
        settings["field_curator"]: curator["value"],
    }
    patch = {
        **output_fields,
        f"{MACHINE_PREFIX}course_key": course_key,
        f"{MACHINE_PREFIX}tariff": tariff,
        f"{MACHINE_PREFIX}curator_raw": _clean(curator.get("raw_value"), 300),
        f"{MACHINE_PREFIX}curator_sheet": _clean(curator.get("worksheet_title"), 300),
        f"{MACHINE_PREFIX}links_source": "none_standard" if is_standard else "chat_links_sheet",
        f"{MACHINE_PREFIX}vk_link_title": "" if is_standard else _clean((link_result.get("vk") or {}).get("title"), 300),
        f"{MACHINE_PREFIX}tg_link_title": "" if is_standard else _clean((link_result.get("telegram") or {}).get("title"), 300),
        f"{MACHINE_PREFIX}standard_no_links": is_standard,
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
        "curator": curator,
        "chat_links": link_result,
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
            status = "customer_only"
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
            chats = await _latest_chats()
            for row in rows:
                result = await _process_row(row, chats, settings, states.get(int(row["id"])), force=force_failed)
                action = result.get("action")
                if action in {"processed", "dry_run", "customer_only"}:
                    summary["processed"] += 1
                elif action in {"skipped", "skipped_state", "pending_curator", "pending_chat_links"}:
                    summary["skipped"] += 1
                elif action == "failed":
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
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute("INSERT INTO scan_runs(dry_run) VALUES(?)", (1 if dry_run else 0,))
        await db.commit()
        return int(cur.lastrowid)


async def _finish_scan_run(run_id: int, summary: dict[str, Any]) -> None:
    assert _db_path is not None
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM scan_runs ORDER BY id DESC LIMIT ?",
            (max(1, min(200, int(limit))),),
        )
        items = [dict(row) for row in await cur.fetchall()]
        for item in items:
            item["details"] = _json_dict(item.pop("details_json", "{}"))
    return {"items": items}
