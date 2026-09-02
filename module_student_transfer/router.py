from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import sys
import time
import unicodedata
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.auth import enforce_rate_limit, require_admin, verify_token_from_request

router = APIRouter()

MODULE_ID = "student-transfer"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SESSION_COOKIE = "student_transfer_session"
SESSION_TTL_DAYS = 30
PASSWORD_MIN_LENGTH = 8
ACCESS_VERIFY_DELAY_SECONDS = 20
TEST_PERIOD_MAX_DAYS = 90
PACKAGE_CHANGE_ADMIN_URL = "https://vk.me/tehpod_sobakovodpro"
PROTECTED_PACKAGE_KEYS = {"standard", "premium", "vip"}
STUDENT_ENRICHMENT_TTL_SECONDS = 300
STUDENT_ENRICHMENT_NEGATIVE_TTL_SECONDS = 30
STUDENT_ENRICHMENT_STALE_SECONDS = 3600
STUDENT_ENRICHMENT_WAIT_SECONDS = 1.25
STUDENT_ENRICHMENT_CACHE_LIMIT = 4096
STUDENT_CARD_IDENTITY_TIMEOUT_SECONDS = 8.0
STUDENT_CARD_EXTERNAL_TIMEOUT_SECONDS = 8.0
STUDENT_CARD_MANAGER_FALLBACK_TIMEOUT_SECONDS = 2.0
CURATOR_OFFERS = {
    "Куратор 1": 8593080,
    "Куратор 2": 8593081,
    "Куратор 3": 8593084,
}
CURATOR_NAMES = {
    "Куратор 1": "Ирина",
    "Куратор 2": "Слава",
    "Куратор 3": "Настасья",
}
DEFAULT_OPERATORS = [
    "Никита Попов",
    "Кристина Рыжкова",
    "Дима Кошурин",
    "Татьяна Воробьева",
    "Евгений Норкин",
    "Артем Зайцев",
    "Наталья Абрамова",
    "Андрей Карачкиев",
    "Татьяна Истратова",
]

_db_path: Path | None = None
_module_dir: Path | None = None
_logger = None
_worker_task: asyncio.Task | None = None
_sheet_worker_task: asyncio.Task | None = None
_refund_sync_task: asyncio.Task | None = None
_access_sync_task: asyncio.Task | None = None
_access_queue_task: asyncio.Task | None = None
_access_browser_cache_task: asyncio.Task | None = None
_transfer_lock = asyncio.Lock()
_operation_queue_lock = asyncio.Lock()
_chat_delivery_lock = asyncio.Lock()
_registry_lock = asyncio.Lock()
_snapshot_lock = asyncio.Lock()
_flow_creation_lock = asyncio.Lock()
_refund_sync_lock = asyncio.Lock()
_access_apply_locks: dict[str, asyncio.Lock] = {}
_snapshot_refresh_task: asyncio.Task | None = None
_registry_sync_task: asyncio.Task | None = None
_last_registry_sync = 0.0
_last_roster_sync = 0.0
_registry_retry_at = 0.0
_snapshot_cache: dict[str, Any] = {"expires_at": 0.0, "data": None}
_student_list_index_cache: dict[str, Any] = {
    "source": None, "all": [], "by_course": {}, "by_stream": {}, "by_flow": {},
}
_student_enrichment_caches: dict[str, dict[str, dict[str, Any]]] = {
    "identity": {},
    "manager": {},
}
_student_enrichment_locks: dict[str, asyncio.Lock] = {
    "identity": asyncio.Lock(),
    "manager": asyncio.Lock(),
}
_student_enrichment_tasks: dict[str, asyncio.Task] = {}
_session_revoke_tasks: set[asyncio.Task] = set()
_revoked_session_tokens: set[str] = set()
_password_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
_dummy_password_hash = _password_ctx.hash("streams-dummy-password")


@asynccontextmanager
async def _connect(timeout: float = 30):
    db = await aiosqlite.connect(_must_db(), timeout=timeout)
    try:
        await db.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
        await db.execute("PRAGMA foreign_keys=ON")
        db.row_factory = aiosqlite.Row
        yield db
    finally:
        await db.close()


class StrictInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LoginIn(StrictInput):
    login: str = Field(max_length=200)
    password: str = Field(default="", max_length=200)


class OperatorIn(StrictInput):
    login: str = Field(max_length=200)
    display_name: str = Field(default="", max_length=200)
    password: str = Field(default="", max_length=200)
    clear_password: bool = False
    active: bool = True


class TransferRef(StrictInput):
    preview_id: str = Field(default="", max_length=64)
    enrollment_id: str = Field(default="", max_length=100)
    email: str = Field(max_length=320)
    source_course_key: str = Field(max_length=50)
    source_stream: str = Field(max_length=50)
    source_row: int
    target_course_key: str = Field(max_length=50)
    target_stream: str = Field(max_length=50)
    move_sheet_row: bool = True
    chat_only: bool = False
    chat_source_stream: str = Field(default="", max_length=50)
    vk_target: str = Field(default="", max_length=500)
    delivery_already_sent: bool = False


class CuratorChangeRef(StrictInput):
    enrollment_id: str = Field(default="", max_length=100)
    email: str = Field(max_length=320)
    source_course_key: str = Field(max_length=50)
    source_stream: str = Field(max_length=50)
    source_row: int
    curator: str = Field(max_length=100)


class FlowCreateIn(StrictInput):
    course_key: str = Field(max_length=50)
    stream: str = Field(max_length=50)
    date_start: str = Field(max_length=100)
    teacher_id: int


class VkLinkIn(StrictInput):
    link: str = Field(max_length=2000)


class FlowManualCompleteIn(StrictInput):
    invite_link: str = Field(max_length=2000)
    opened_as_community: bool
    history_250_enabled: bool
    members_admin_only: bool
    system_notifications_disabled: bool = False


class FlowCuratorIn(StrictInput):
    curator: str = Field(max_length=100)


class AccessChangeIn(StrictInput):
    group_id: str = Field(max_length=30)
    enabled: bool


class AccessPreviewIn(StrictInput):
    changes: list[AccessChangeIn] = Field(min_length=1, max_length=40)


class AccessApplyIn(StrictInput):
    request_id: str = Field(min_length=8, max_length=40)


class TestPeriodIn(StrictInput):
    days: int = Field(ge=1, le=TEST_PERIOD_MAX_DAYS)
    courses: list[str] = Field(min_length=1, max_length=2)


class TestDriveCheckIn(StrictInput):
    email: str = Field(default="", max_length=320)
    phone: str = Field(default="", max_length=100)
    browser_id: str = Field(default="", max_length=128)


class TestDriveConfirmIn(StrictInput):
    token: str = Field(min_length=20, max_length=256)
    gc_user_id: str = Field(min_length=1, max_length=100)
    browser_id: str = Field(default="", max_length=128)


class LessonUpdateIn(StrictInput):
    value: bool
    expected_value: bool


class StudentNoteIn(StrictInput):
    note: str = Field(default="", max_length=2000)


class ChatRemovalIn(StrictInput):
    preview_id: str = Field(min_length=20, max_length=100)


class RefundStatusIn(StrictInput):
    refunded: bool
    expected_refunded: bool
    reason: str = Field(default="", max_length=500)


class MessengerSendIn(StrictInput):
    request_id: str = Field(default="", max_length=64)
    channel_id: str = Field(max_length=200)
    transport: str = Field(max_length=40)
    provider: str = Field(max_length=40)
    chat_id: str = Field(default="", max_length=250)
    text: str = Field(default="", max_length=4000)
    subject: str = Field(default="", max_length=300)
    attachment_url: str = Field(default="", max_length=4000)
    attachment_type: str = Field(default="", max_length=100)


class MessengerTemplatePreviewIn(StrictInput):
    template_id: int = Field(default=0, ge=0)
    body: str = Field(default="", max_length=20_000)


class MessengerTemplateFavoriteIn(StrictInput):
    template_id: int = Field(gt=0)
    favorite: bool


async def setup(ctx):
    global _db_path, _module_dir, _logger, _worker_task, _sheet_worker_task, _refund_sync_task, _access_sync_task, _access_queue_task, _access_browser_cache_task
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    lifecycle = getattr(ctx, "lifecycle", None)
    create_task = lifecycle.create_task if lifecycle is not None else asyncio.create_task
    _worker_task = create_task(_worker_loop(), name="student-transfer-worker")
    _sheet_worker_task = create_task(_sheet_operation_loop(), name="student-transfer-sheet-operations")
    _refund_sync_task = create_task(_refund_sync_loop(), name="student-transfer-refund-sync")
    _access_sync_task = create_task(_access_sync_loop(), name="student-transfer-access-sync")
    _access_queue_task = create_task(_access_queue_loop(), name="student-transfer-access-queue")
    _access_browser_cache_task = create_task(
        _access_browser_cache_loop(), name="student-transfer-access-browser-cache"
    )


async def shutdown():
    global _worker_task, _sheet_worker_task, _refund_sync_task, _access_sync_task, _access_queue_task, _access_browser_cache_task, _snapshot_refresh_task, _registry_sync_task
    tasks = [task for task in (_worker_task, _sheet_worker_task, _refund_sync_task, _access_sync_task, _access_queue_task, _access_browser_cache_task, _snapshot_refresh_task, _registry_sync_task) if task and not task.done()]
    tasks.extend(task for task in _student_enrichment_tasks.values() if not task.done())
    tasks.extend(task for task in _session_revoke_tasks if not task.done())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _worker_task = None
    _sheet_worker_task = None
    _refund_sync_task = None
    _access_sync_task = None
    _access_queue_task = None
    _access_browser_cache_task = None
    _snapshot_refresh_task = None
    _registry_sync_task = None
    _student_enrichment_tasks.clear()
    _session_revoke_tasks.clear()


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("student-transfer module is not initialized")
    return _db_path


def _must_module_dir() -> Path:
    if _module_dir is None:
        raise RuntimeError("student-transfer module is not initialized")
    return _module_dir


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _session_expires() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _norm(value: Any) -> str:
    raw = unicodedata.normalize("NFKC", _clean(value, 1000)).casefold().replace("ё", "е")
    return " ".join(raw.split())


def _sheet_date_value(value: Any) -> str:
    text = _clean(value, 100)
    try:
        serial = float(text.replace(",", "."))
    except ValueError:
        return text
    if not (1 <= serial <= 100000):
        return text
    return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime("%Y-%m-%d")


def _tariff_key(value: Any) -> str:
    return {
        "standard": "standard",
        "стандарт": "standard",
        "premium": "premium",
        "премиум": "premium",
        "vip": "vip",
        "вип": "vip",
    }.get(_norm(value), "other")


def _phone_search_key(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value, 100))
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits


def _is_phone_search(value: Any) -> bool:
    raw = _clean(value, 1000).strip()
    return bool(raw and re.fullmatch(r"[+\d\s().-]+", raw) and len(_phone_search_key(raw)) >= 10)


def _vk_target_id(value: Any) -> str:
    """Normalize an explicit VK profile ID without accepting chat URLs."""

    raw = _clean(value, 500)
    if re.fullmatch(r"\d{3,20}", raw):
        return raw
    try:
        parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    except ValueError:
        return ""
    if (parsed.hostname or "").casefold() not in {"vk.com", "www.vk.com", "vk.ru", "www.vk.ru"}:
        return ""
    match = re.fullmatch(r"/id(\d{3,20})/?", parsed.path)
    return match.group(1) if match else ""


def _cookie_path(request: Request) -> str:
    if request.headers.get("x-forwarded-prefix", "").rstrip("/") == "/streams":
        return "/streams"
    root_path = request.scope.get("root_path", "") or ""
    if root_path.rstrip("/") == "/streams":
        return "/streams"
    return f"{root_path}/{MODULE_ID}".replace("//", "/")


def _module(module_id: str, service: str):
    module = sys.modules.get(f"_nexus_mod_{module_id}")
    if module is None or not hasattr(module, service):
        raise HTTPException(503, f"Модуль {module_id} недоступен")
    return module


def _backup_stream_repair_db() -> None:
    if _module_dir is None:
        return
    db_path = _must_db()
    if not db_path.exists():
        return
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30) as source:
            exists = source.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='enrollments'"
            ).fetchone()
            affected = exists and source.execute(
                "SELECT 1 FROM enrollments WHERE lower(email)=? LIMIT 1",
                ("artimida444@yandex.ru",),
            ).fetchone()
            if not affected:
                return
            backup = (
                _must_module_dir().parents[1]
                / "backups/student-transfer-pre-stream-repair-20260808T2245MSK/student-transfer.db"
            )
            if backup.exists():
                return
            backup.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(backup) as target:
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("student-transfer backup quick_check failed")
            backup.chmod(0o600)
            if _logger:
                _logger.info("Streams repair backup created: %s", backup)
    except sqlite3.Error as exc:
        raise RuntimeError(f"student-transfer backup failed: {exc}") from exc


def _backup_auth_migration_db() -> None:
    if _module_dir is None:
        return
    db_path = _must_db()
    if not db_path.exists():
        return
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30) as source:
            table = source.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='registry_meta'"
            ).fetchone()
            version = source.execute(
                "SELECT value FROM registry_meta WHERE key='auth_version'"
            ).fetchone() if table else None
            if version and version[0] in {"nexus-sso-v1", "streams-password-v1"}:
                return
            backup = (
                _must_module_dir().parents[1]
                / "backups/student-transfer-pre-nexus-sso-20260811T2130MSK/student-transfer.db"
            )
            if backup.exists():
                return
            backup.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(backup) as target:
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("student-transfer auth migration backup quick_check failed")
            backup.chmod(0o600)
    except Exception as exc:
        raise RuntimeError(f"student-transfer auth migration backup failed: {exc}") from exc


def _backup_refund_migration_db() -> None:
    """Create one recoverable copy before the first refund status migration."""
    db_path = _must_db()
    if not db_path.exists():
        return
    backup = (
        _must_module_dir().parents[1]
        / "backups/student-transfer-pre-refunds-1.4.88/student-transfer.db"
    )
    if backup.exists():
        return
    backup.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60) as source:
            with sqlite3.connect(backup) as target:
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("student-transfer refund backup quick_check failed")
        backup.chmod(0o600)
    except Exception:
        backup.unlink(missing_ok=True)
        raise


def _backup_password_migration_db() -> None:
    """Create a verified backup immediately before adding Streams passwords."""
    if _module_dir is None:
        return
    db_path = _must_db()
    if not db_path.exists():
        return
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30) as source:
            has_operators = source.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='operators'"
            ).fetchone()
            if not has_operators:
                return
            columns = {row[1] for row in source.execute("PRAGMA table_info(operators)").fetchall()}
            if "password_hash" in columns:
                return
            backup = (
                _must_module_dir().parents[1]
                / "backups/student-transfer-pre-password-auth-v1/student-transfer.db"
            )
            if backup.exists():
                return
            backup.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(backup) as target:
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("student-transfer password migration backup quick_check failed")
            backup.chmod(0o600)
            if _logger:
                _logger.info("Streams password migration backup created: %s", backup)
    except Exception as exc:
        raise RuntimeError(f"student-transfer password migration backup failed: {exc}") from exc


def _backup_curator_source_migration_db() -> None:
    """Back up persistent Streams state before adding curator ownership."""
    if _module_dir is None:
        return
    db_path = _must_db()
    if not db_path.exists():
        return
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30) as source:
            has_registry = source.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='flow_registry'"
            ).fetchone()
            if not has_registry:
                return
            columns = {row[1] for row in source.execute("PRAGMA table_info(flow_registry)").fetchall()}
            if "curator_source" in columns:
                return
            backup = (
                _must_module_dir().parents[1]
                / "backups/student-transfer-pre-curator-source-v1/student-transfer.db"
            )
            if backup.exists():
                return
            backup.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(backup) as target:
                source.backup(target)
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("student-transfer curator source backup quick_check failed")
            backup.chmod(0o600)
            if _logger:
                _logger.info("Streams curator source migration backup created: %s", backup)
    except Exception as exc:
        raise RuntimeError(f"student-transfer curator source migration backup failed: {exc}") from exc


async def _init_db() -> None:
    await asyncio.to_thread(_backup_stream_repair_db)
    await asyncio.to_thread(_backup_auth_migration_db)
    await asyncio.to_thread(_backup_password_migration_db)
    await asyncio.to_thread(_backup_curator_source_migration_db)
    async with _connect() as db:
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=5000;
            CREATE TABLE IF NOT EXISTS operators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                login_key TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                password_hash TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                operator_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                enrollment_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                email TEXT NOT NULL,
                gc_user_id TEXT NOT NULL DEFAULT '',
                student_name TEXT NOT NULL DEFAULT '',
                source_course_key TEXT NOT NULL,
                source_stream TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                target_course_key TEXT NOT NULL,
                target_stream TEXT NOT NULL,
                curator TEXT NOT NULL,
                offer_id INTEGER NOT NULL,
                operator_id INTEGER NOT NULL,
                operator_name TEXT NOT NULL,
                student_json TEXT NOT NULL DEFAULT '{}',
                steps_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_transfers_created ON transfers(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_transfers_email ON transfers(email, created_at DESC);
            CREATE TABLE IF NOT EXISTS registry_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS flow_registry (
                course_key TEXT NOT NULL,
                stream TEXT NOT NULL,
                course TEXT NOT NULL,
                date_start TEXT NOT NULL DEFAULT '',
                teacher_id INTEGER NOT NULL DEFAULT 0,
                teacher TEXT NOT NULL DEFAULT '',
                teacher_code TEXT NOT NULL DEFAULT '',
                curator_source TEXT NOT NULL DEFAULT '',
                offer_id INTEGER NOT NULL DEFAULT 0,
                vk_link TEXT NOT NULL DEFAULT '',
                tg_link TEXT NOT NULL DEFAULT '',
                vk_admin_url TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'draft',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(course_key,stream)
            );
            CREATE TABLE IF NOT EXISTS enrollments (
                id TEXT PRIMARY KEY,
                source_record_id INTEGER NOT NULL DEFAULT 0,
                order_id TEXT NOT NULL DEFAULT '',
                deal_number TEXT NOT NULL DEFAULT '',
                gc_user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL,
                tg_account TEXT NOT NULL DEFAULT '',
                date TEXT NOT NULL DEFAULT '',
                course_key TEXT NOT NULL,
                course TEXT NOT NULL,
                stream TEXT NOT NULL DEFAULT '',
                tariff TEXT NOT NULL DEFAULT '',
                teacher TEXT NOT NULL DEFAULT '',
                teacher_code TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'assigned',
                source_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_enrollments_source
                ON enrollments(source_record_id) WHERE source_record_id>0;
            CREATE INDEX IF NOT EXISTS idx_enrollments_flow ON enrollments(course_key,stream,status);
            CREATE INDEX IF NOT EXISTS idx_enrollments_email ON enrollments(email,course_key);
            CREATE TABLE IF NOT EXISTS lesson_progress (
                enrollment_id TEXT NOT NULL,
                lesson_key TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                value INTEGER NOT NULL DEFAULT 0,
                sheet_value INTEGER NOT NULL DEFAULT 0,
                dirty INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(enrollment_id,lesson_key),
                FOREIGN KEY(enrollment_id) REFERENCES enrollments(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS student_notes (
                enrollment_id TEXT PRIMARY KEY,
                note TEXT NOT NULL DEFAULT '',
                updated_by TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_student_notes_updated_at ON student_notes(updated_at DESC);
            CREATE TABLE IF NOT EXISTS enrollment_status_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enrollment_id TEXT NOT NULL,
                target_status TEXT NOT NULL CHECK(target_status IN ('active','refunded')),
                previous_status TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                operator_id INTEGER NOT NULL,
                operator_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_enrollment_status_overrides_latest
                ON enrollment_status_overrides(enrollment_id,id DESC);
            CREATE TABLE IF NOT EXISTS registry_sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS flow_jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                course_key TEXT NOT NULL,
                stream TEXT NOT NULL,
                date_start TEXT NOT NULL,
                teacher_id INTEGER NOT NULL,
                operator_id INTEGER NOT NULL,
                operator_name TEXT NOT NULL,
                result_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS test_periods (
                id TEXT PRIMARY KEY,
                enrollment_id TEXT NOT NULL,
                gc_user_id TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                phone_key TEXT NOT NULL DEFAULT '',
                student_name TEXT NOT NULL DEFAULT '',
                courses_json TEXT NOT NULL DEFAULT '[]',
                group_ids_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL,
                starts_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,
                grant_request_id TEXT NOT NULL DEFAULT '',
                revoke_request_id TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                operator_id INTEGER NOT NULL,
                operator_name TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT '',
                allow_repeat INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_test_periods_due
                ON test_periods(status,next_attempt_at,expires_at);
            CREATE INDEX IF NOT EXISTS idx_test_periods_enrollment
                ON test_periods(enrollment_id,created_at DESC);
            CREATE TABLE IF NOT EXISTS test_period_identities (
                identity_type TEXT NOT NULL,
                identity_value TEXT NOT NULL,
                test_period_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(identity_type,identity_value),
                FOREIGN KEY(test_period_id) REFERENCES test_periods(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS access_browser_snapshots (
                gc_user_id TEXT PRIMARY KEY,
                email TEXT NOT NULL DEFAULT '',
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT '',
                last_attempt_at TEXT NOT NULL DEFAULT '',
                next_attempt_at TEXT NOT NULL DEFAULT '',
                failures INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_access_browser_snapshots_due
                ON access_browser_snapshots(next_attempt_at,last_attempt_at);
            CREATE TABLE IF NOT EXISTS testdrive_pending (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL DEFAULT '',
                phone_key TEXT NOT NULL DEFAULT '',
                browser_hash TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_testdrive_pending_identity
                ON testdrive_pending(email,phone_key,expires_at);
            """
        )
        transfer_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(transfers)")).fetchall()}
        if "enrollment_id" not in transfer_columns:
            await db.execute("ALTER TABLE transfers ADD COLUMN enrollment_id TEXT NOT NULL DEFAULT ''")
        flow_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(flow_registry)")).fetchall()}
        if "vk_admin_url" not in flow_columns:
            await db.execute("ALTER TABLE flow_registry ADD COLUMN vk_admin_url TEXT NOT NULL DEFAULT ''")
        if "curator_source" not in flow_columns:
            await db.execute("ALTER TABLE flow_registry ADD COLUMN curator_source TEXT NOT NULL DEFAULT ''")
        operator_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(operators)")).fetchall()}
        if "password_hash" not in operator_columns:
            await db.execute("ALTER TABLE operators ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''")
        test_period_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(test_periods)")).fetchall()}
        if "allow_repeat" not in test_period_columns:
            await db.execute("ALTER TABLE test_periods ADD COLUMN allow_repeat INTEGER NOT NULL DEFAULT 0")
        now = _now()
        for login in DEFAULT_OPERATORS:
            await db.execute(
                """
                INSERT OR IGNORE INTO operators(login,login_key,display_name,active,created_at,updated_at)
                VALUES(?,?,?,?,?,?)
                """,
                (login, _norm(login), login, 1, now, now),
            )
        await db.execute(
            """
            INSERT INTO registry_meta(key,value) VALUES('auth_version','streams-password-v1')
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """
        )
        await db.execute(
            "INSERT OR IGNORE INTO registry_meta(key,value) VALUES('testdrive_callback_token',?)",
            (secrets.token_urlsafe(36),),
        )
        await db.execute(
            "INSERT OR IGNORE INTO registry_meta(key,value) VALUES('testdrive_hash_key',?)",
            (secrets.token_hex(32),),
        )
        await db.execute("DELETE FROM sessions WHERE expires_at<=?", (now,))
        await db.execute("DELETE FROM testdrive_pending WHERE expires_at<=?", (now,))
        await db.execute("UPDATE transfers SET status='queued',updated_at=? WHERE status='running'", (now,))
        await db.execute("UPDATE flow_jobs SET status='queued',updated_at=? WHERE status='running'", (now,))
        await db.execute(
            """UPDATE registry_sync_runs
               SET status='failed',
                   error='Синхронизация была прервана перезапуском модуля; следующий запуск выполняется автоматически.',
                   finished_at=?
               WHERE status='running'""",
            (now,),
        )
        await db.execute(
            "UPDATE test_periods SET status='queued_grant',next_attempt_at=?,updated_at=? "
            "WHERE status='preparing'",
            (now, now),
        )
        await db.commit()


async def _require_operator(request: Request) -> dict[str, Any]:
    _require_same_origin(request)
    token = request.cookies.get(SESSION_COOKIE)
    if token and token not in _revoked_session_tokens:
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            row = await (
                await db.execute(
                    """
                    SELECT o.* FROM sessions s JOIN operators o ON o.id=s.operator_id
                    WHERE s.token=? AND s.expires_at>? AND o.active=1
                    """,
                    (token, _now()),
                )
            ).fetchone()
        if row:
            return dict(row)
    raise HTTPException(401, "Войдите в управление потоками")


async def _create_session(token: str, operator_id: str) -> None:
    for attempt in range(6):
        try:
            async with _connect(timeout=0.5) as db:
                await db.execute("DELETE FROM sessions WHERE expires_at<=?", (_now(),))
                await db.execute(
                    "INSERT INTO sessions(token,operator_id,expires_at,created_at) VALUES(?,?,?,?)",
                    (token, operator_id, _session_expires(), _now()),
                )
                await db.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).casefold() or attempt == 5:
                raise
            await asyncio.sleep(0.1 * (attempt + 1))


async def _revoke_session(token: str) -> None:
    try:
        for attempt in range(30):
            try:
                async with _connect(timeout=0.25) as db:
                    await db.execute("DELETE FROM sessions WHERE token=?", (token,))
                    await db.commit()
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).casefold():
                    raise
                await asyncio.sleep(min(2.0, 0.1 * (attempt + 1)))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if _logger:
            _logger.warning("Streams session revocation deferred until restart cleanup: %s", exc)


def _schedule_session_revocation(token: str) -> None:
    _revoked_session_tokens.add(token)
    task = asyncio.create_task(_revoke_session(token), name="student-transfer-session-revoke")
    _session_revoke_tasks.add(task)
    task.add_done_callback(_session_revoke_tasks.discard)


def _require_same_origin(request: Request) -> None:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    if request.headers.get("sec-fetch-site", "").casefold() == "cross-site":
        raise HTTPException(403, "Запрос с другого сайта заблокирован")
    origin = request.headers.get("origin", "").strip()
    if not origin:
        return
    expected_host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").casefold()
    parsed = urlsplit(origin)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.casefold() != expected_host:
        raise HTTPException(403, "Запрос с другого сайта заблокирован")


def _password_matches(password: str, password_hash: str) -> bool:
    try:
        return _password_ctx.verify(password, password_hash)
    except (TypeError, ValueError):
        return False


async def _require_admin(request: Request) -> dict[str, Any]:
    _require_same_origin(request)
    user = await verify_token_from_request(request)
    if not require_admin(user):
        raise HTTPException(403, "admin required")
    return user


async def _refresh_snapshot_cache(*, refresh: bool = False) -> dict[str, Any]:
    async with _snapshot_lock:
        if not refresh and time.monotonic() < float(_snapshot_cache["expires_at"]):
            return _snapshot_cache["data"]
        data = await _registry_snapshot(refresh=refresh)
        if not data.get("ok", True):
            raise HTTPException(503, "Не удалось загрузить потоки")
        _snapshot_cache.update(data=data, expires_at=time.monotonic() + 360)
    return data


async def _refresh_snapshot_in_background() -> None:
    try:
        await _refresh_snapshot_cache()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if _logger:
            _logger.warning("flow registry snapshot refresh deferred: %s", exc)


def _schedule_snapshot_refresh() -> None:
    global _snapshot_refresh_task
    if _snapshot_refresh_task and not _snapshot_refresh_task.done():
        return
    _snapshot_refresh_task = asyncio.create_task(
        _refresh_snapshot_in_background(), name="student-transfer-snapshot-refresh"
    )


async def _sync_registry_in_background() -> None:
    try:
        await _sync_registry(force=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if _logger:
            _logger.warning("flow registry background sync deferred: %s", exc)


def _schedule_registry_sync() -> None:
    global _registry_sync_task
    if _registry_sync_task and not _registry_sync_task.done():
        return
    _registry_sync_task = asyncio.create_task(
        _sync_registry_in_background(), name="student-transfer-registry-sync"
    )


async def _snapshot(refresh: bool = False) -> dict[str, Any]:
    cached = _snapshot_cache.get("data")
    if not refresh and cached is not None:
        if time.monotonic() >= float(_snapshot_cache["expires_at"]):
            _schedule_snapshot_refresh()
        return cached
    return await _refresh_snapshot_cache(refresh=refresh)


def _clear_snapshot_cache() -> None:
    _snapshot_cache.update(data=None, expires_at=0.0)
    _student_list_index_cache.update(
        source=None, all=[], by_course={}, by_stream={}, by_flow={},
    )


def _teacher_code(name: Any) -> str:
    normalized = _norm(name)
    for code, teacher in CURATOR_NAMES.items():
        if _norm(teacher) == normalized:
            return code
    return ""


def _enrollment_id(source_record_id: Any, course_key: Any, stream: Any, email: Any) -> str:
    try:
        source_id = int(source_record_id or 0)
    except (TypeError, ValueError):
        source_id = 0
    if source_id > 0:
        return f"order:{source_id}"
    raw = "|".join((_norm(course_key), _norm(stream), _norm(email)))
    return "legacy:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


async def _meta_get(key: str, default: str = "") -> str:
    async with _connect() as db:
        row = await (await db.execute("SELECT value FROM registry_meta WHERE key=?", (key,))).fetchone()
    return str((row or [default])[0] or default)


async def _meta_set(key: str, value: Any) -> None:
    async with _connect() as db:
        await db.execute(
            "INSERT INTO registry_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        await db.commit()


async def _sync_refunds() -> dict[str, Any]:
    async with _refund_sync_lock:
        return await _sync_refunds_unlocked()


async def _sync_refunds_unlocked() -> dict[str, Any]:
    """Move refunded orders out of the active roster using the webhook ledger."""
    # v4 deliberately replays the immutable ledger once after the business
    # rule was clarified: more than 15,000 RUB remaining keeps the student;
    # 15,000 or less moves the student to Refunds.
    cursor = int(await _meta_get("refund_event_cursor_v4", "0") or 0)
    try:
        pending_raw = json.loads(await _meta_get("refund_pending_json_v4", "[]") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        pending_raw = []
    pending = [item for item in pending_raw if isinstance(item, dict)]
    try:
        orders = _module("getcourse-orders", "service_refund_events")
        result = await orders.service_refund_events(after_event_id=cursor, limit=1000)
    except Exception as exc:
        if _logger:
            _logger.warning("Refund synchronization deferred: %s", exc)
        return {"moved": 0, "pending": len(pending), "deferred": True}
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "Возвраты GetCourse недоступны")
    if result.get("items") or pending:
        await asyncio.to_thread(_backup_refund_migration_db)
    by_event: dict[int, dict[str, Any]] = {}
    for event in [*pending, *(result.get("items") or [])]:
        event_id = int(event.get("event_id") or 0)
        if event_id:
            by_event[event_id] = event
    unmatched: list[dict[str, Any]] = []
    moved = 0
    now = _now()
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        for event_index, event_id in enumerate(sorted(by_event), start=1):
            event = by_event[event_id]
            keys = list(dict.fromkeys(
                value for value in (
                    _clean(event.get("order_id"), 100),
                    _clean(event.get("platform_id"), 100),
                ) if value
            ))
            if not keys:
                continue
            placeholders = ",".join("?" for _ in keys)
            row = await (
                await db.execute(
                    f"""SELECT id,source_json,status,stream FROM enrollments
                        WHERE order_id IN ({placeholders}) OR deal_number IN ({placeholders})
                           OR CAST(source_record_id AS TEXT) IN ({placeholders})
                        ORDER BY updated_at DESC LIMIT 1""",
                    (*keys, *keys, *keys),
                )
            ).fetchone()
            if not row:
                unmatched.append(event)
                continue
            try:
                source = json.loads(row["source_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                source = {}
            if not isinstance(source, dict):
                source = {}
            source["refund"] = {
                "event_id": event_id,
                "received_at": _clean(event.get("received_at"), 100),
                "status": _clean(event.get("status"), 300),
                "payment_state": _clean(event.get("payment_state"), 40),
                "kind": _clean(event.get("refund_kind"), 20) or "full",
                "total_amount": float(event.get("total_amount") or 0),
                "remaining_amount": float(event.get("remaining_amount") or 0),
                "refund_amount": float(event.get("refund_amount") or 0),
            }
            is_partial = source["refund"]["kind"] == "partial"
            target_status = ("assigned" if _clean(source.get("stream") or "", 50) else "pending") if is_partial else "refunded"
            # The persisted columns are authoritative for assignment; source
            # snapshots from older releases may not carry stream.
            if is_partial:
                target_status = "assigned" if _clean(row["stream"], 50) else "pending"
            override = await (
                await db.execute(
                    """SELECT target_status FROM enrollment_status_overrides
                       WHERE enrollment_id=? ORDER BY id DESC LIMIT 1""",
                    (row["id"],),
                )
            ).fetchone()
            is_canceled = source["refund"]["payment_state"] == "canceled"
            if override and not is_canceled:
                target_status = (
                    "refunded" if override[0] == "refunded"
                    else ("assigned" if _clean(row["stream"], 50) else "pending")
                )
            if row["status"] != target_status:
                moved += 1
            await db.execute(
                "UPDATE enrollments SET status=?,source_json=?,updated_at=? WHERE id=?",
                (target_status, json.dumps(source, ensure_ascii=False), now, row["id"]),
            )
            if event_index % 50 == 0:
                await db.commit()
        await db.commit()
    await _meta_set("refund_event_cursor_v4", int(result.get("cursor") or cursor))
    await _meta_set("refund_pending_json_v4", json.dumps(unmatched[-1000:], ensure_ascii=False))
    return {"moved": moved, "pending": len(unmatched), "cursor": int(result.get("cursor") or cursor)}


async def _flow_rows() -> list[dict[str, Any]]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM flow_registry ORDER BY CASE course_key WHEN 'puppy' THEN 0 ELSE 1 END, CAST(stream AS INTEGER) DESC,stream DESC"
            )
        ).fetchall()
    return [dict(row) for row in rows]


async def _enrollment_rows() -> list[dict[str, Any]]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM enrollments WHERE status NOT IN ('removed','refunded') ORDER BY CASE course_key WHEN 'puppy' THEN 0 ELSE 1 END,CAST(stream AS INTEGER) DESC,name,email"
            )
        ).fetchall()
        lesson_rows = await (
            await db.execute("SELECT * FROM lesson_progress ORDER BY length(lesson_key),lesson_key")
        ).fetchall()
    lessons: dict[str, list[dict[str, Any]]] = {}
    for row in lesson_rows:
        item = dict(row)
        lessons.setdefault(item["enrollment_id"], []).append(
            {"key": item["lesson_key"], "label": item["label"], "value": bool(item["value"])}
        )
    result = []
    for row in rows:
        item = dict(row)
        item["lessons"] = lessons.get(item["id"], [])
        result.append(item)
    return result


async def _refund_enrollment_rows() -> list[dict[str, Any]]:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM enrollments WHERE status='refunded' ORDER BY updated_at DESC,name,email"
            )
        ).fetchall()
        lesson_rows = await (
            await db.execute(
                """SELECT p.* FROM lesson_progress p
                   JOIN enrollments e ON e.id=p.enrollment_id
                   WHERE e.status='refunded' ORDER BY length(p.lesson_key),p.lesson_key"""
            )
        ).fetchall()
    lessons: dict[str, list[dict[str, Any]]] = {}
    for row in lesson_rows:
        item = dict(row)
        lessons.setdefault(item["enrollment_id"], []).append(
            {"key": item["lesson_key"], "label": item["label"], "value": bool(item["value"])}
        )
    result = []
    for row in rows:
        item = dict(row)
        item["lessons"] = lessons.get(item["id"], [])
        result.append(item)
    return result


async def _upsert_flows(link_catalog: dict[str, Any], creator_catalog: dict[str, Any]) -> list[dict[str, Any]]:
    creator_map = {
        (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50)): item
        for item in creator_catalog.get("items") or []
    }
    now = _now()
    async with _connect() as db:
        manual_vk_links: dict[tuple[str, str], str] = {}
        created_tg_links: dict[tuple[str, str], str] = {}
        manual_rows = await (
            await db.execute(
                """SELECT course_key,stream,result_json FROM flow_jobs
                   ORDER BY created_at DESC,id DESC"""
            )
        ).fetchall()
        for manual_row in manual_rows:
            key = (_clean(manual_row["course_key"], 50), _clean(manual_row["stream"], 50))
            result = _load_steps(manual_row["result_json"])
            manual = result.get("manual_link") if isinstance(result.get("manual_link"), dict) else {}
            manual_link = _clean(manual.get("link"), 2000)
            if key not in manual_vk_links and manual.get("ok") and manual_link.startswith("https://vk.me/join/"):
                manual_vk_links[key] = manual_link
            chats = result.get("chats") if isinstance(result.get("chats"), dict) else {}
            if not chats and isinstance(result.get("create"), dict):
                chats = result["create"]
            telegram = chats.get("telegram") if isinstance(chats.get("telegram"), dict) else {}
            tg_link = _clean(telegram.get("group_link"), 2000)
            if key not in created_tg_links and tg_link.startswith("https://t.me/"):
                created_tg_links[key] = tg_link
        for link_flow in link_catalog.get("items") or []:
            course_key = _clean(link_flow.get("course_key"), 50)
            stream = _clean(link_flow.get("stream"), 50)
            if not course_key or not stream:
                continue
            created = creator_map.get((course_key, stream)) or {}
            date_start = _clean(link_flow.get("date_start") or created.get("date_start"), 100)
            teacher = _clean(created.get("teacher"), 200)
            vk_link = manual_vk_links.get((course_key, stream)) or _clean(link_flow.get("vk_link"), 2000)
            tg_link = created_tg_links.get((course_key, stream)) or _clean(link_flow.get("tg_link"), 2000)
            status = "ready" if vk_link.startswith("http") and tg_link.startswith("http") else "draft"
            await db.execute(
                """
                INSERT INTO flow_registry(
                    course_key,stream,course,date_start,teacher_id,teacher,teacher_code,curator_source,offer_id,
                    vk_link,tg_link,vk_admin_url,status,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(course_key,stream) DO UPDATE SET
                    course=excluded.course,
                    date_start=CASE WHEN excluded.date_start<>'' THEN excluded.date_start ELSE flow_registry.date_start END,
                    teacher_id=CASE WHEN flow_registry.curator_source='streams' THEN flow_registry.teacher_id WHEN excluded.teacher<>'' THEN excluded.teacher_id ELSE flow_registry.teacher_id END,
                    teacher=CASE WHEN flow_registry.curator_source='streams' THEN flow_registry.teacher WHEN excluded.teacher<>'' THEN excluded.teacher ELSE flow_registry.teacher END,
                    teacher_code=CASE WHEN flow_registry.curator_source='streams' THEN flow_registry.teacher_code WHEN excluded.teacher_code<>'' THEN excluded.teacher_code ELSE flow_registry.teacher_code END,
                    curator_source=CASE WHEN flow_registry.curator_source='streams' THEN flow_registry.curator_source WHEN excluded.curator_source<>'' THEN excluded.curator_source ELSE flow_registry.curator_source END,
                    offer_id=CASE WHEN flow_registry.curator_source='streams' THEN flow_registry.offer_id WHEN excluded.teacher<>'' THEN excluded.offer_id ELSE flow_registry.offer_id END,
                    vk_link=excluded.vk_link,tg_link=excluded.tg_link,vk_admin_url=excluded.vk_admin_url,
                    status=excluded.status,updated_at=excluded.updated_at
                """,
                (
                    course_key,
                    stream,
                    _clean(link_flow.get("course"), 100),
                    date_start,
                    int(created.get("teacher_id") or 0),
                    teacher,
                    _teacher_code(teacher),
                    "streams" if teacher else "",
                    int(created.get("offer_id") or 0),
                    vk_link,
                    tg_link,
                    _clean(created.get("vk_admin_url"), 2000),
                    status,
                    now,
                ),
            )
        await db.commit()
    return await _flow_rows()


def _created_flow_links(create_result: dict[str, Any], course_key: str, stream: str) -> dict[str, str]:
    """Extract the just-created chat links without waiting for a Google catalog refresh."""
    telegram = create_result.get("telegram") if isinstance(create_result.get("telegram"), dict) else {}
    vk = create_result.get("vk") if isinstance(create_result.get("vk"), dict) else {}
    catalog = create_result.get("catalog") if isinstance(create_result.get("catalog"), dict) else {}
    catalog_flow = next(
        (
            item for item in catalog.get("items") or []
            if _clean(item.get("course_key"), 50) == course_key
            and _clean(item.get("stream"), 50) == stream
        ),
        {},
    )
    owner_group_id = int(vk.get("owner_group_id") or 0)
    chat_id = _clean(vk.get("chat_id"), 100)
    admin_url = _clean(catalog_flow.get("vk_admin_url"), 2000)
    if not admin_url and owner_group_id and chat_id:
        admin_url = f"https://vk.ru/gim{owner_group_id}?sel=c{chat_id}"
    return {
        "vk_link": _clean(vk.get("group_link") or (catalog_flow.get("vk") or {}).get("link"), 2000),
        "tg_link": _clean(telegram.get("group_link") or (catalog_flow.get("telegram") or {}).get("link"), 2000),
        "vk_admin_url": admin_url,
    }


async def _persist_created_flow(
    job: dict[str, Any], teacher: dict[str, Any], create_result: dict[str, Any],
    *, final_vk_link: str = "", ready: bool = False,
) -> dict[str, Any]:
    """Persist a created flow immediately; Google Sheets may still be rate-limited."""
    course_key = _clean(job.get("course_key"), 50)
    stream = _clean(job.get("stream"), 50)
    links = _created_flow_links(create_result, course_key, stream)
    if final_vk_link:
        links["vk_link"] = _clean(final_vk_link, 2000)
    status = "ready" if ready and links["vk_link"].startswith("http") and links["tg_link"].startswith("http") else "draft"
    now = _now()
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO flow_registry(
                course_key,stream,course,date_start,teacher_id,teacher,teacher_code,curator_source,offer_id,
                vk_link,tg_link,vk_admin_url,status,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(course_key,stream) DO UPDATE SET
                course=excluded.course,date_start=excluded.date_start,teacher_id=excluded.teacher_id,
                teacher=excluded.teacher,teacher_code=excluded.teacher_code,curator_source='streams',
                offer_id=excluded.offer_id,
                vk_link=CASE WHEN excluded.vk_link<>'' THEN excluded.vk_link ELSE flow_registry.vk_link END,
                tg_link=CASE WHEN excluded.tg_link<>'' THEN excluded.tg_link ELSE flow_registry.tg_link END,
                vk_admin_url=CASE WHEN excluded.vk_admin_url<>'' THEN excluded.vk_admin_url ELSE flow_registry.vk_admin_url END,
                status=excluded.status,updated_at=excluded.updated_at
            """,
            (
                course_key, stream, "Собака" if course_key == "dog" else "Щенок",
                _clean(job.get("date_start"), 100), int(teacher.get("id") or job.get("teacher_id") or 0),
                _clean(teacher.get("name"), 200), _teacher_code(teacher.get("name")), "streams",
                int(teacher.get("offer_id") or 0), links["vk_link"], links["tg_link"],
                links["vk_admin_url"], status, now,
            ),
        )
        await db.commit()
    return {**links, "status": status}


async def _propagate_flow_curator_changes(
    previous: dict[tuple[str, str], dict[str, Any]],
    current: list[dict[str, Any]],
    source_keys: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    changed: set[tuple[str, str]] = set()
    async with _connect() as db:
        for flow in current:
            key = (flow["course_key"], flow["stream"])
            old_code = _clean((previous.get(key) or {}).get("teacher_code"), 100)
            new_code = _clean(flow.get("teacher_code"), 100)
            if key not in source_keys or not new_code or old_code == new_code:
                continue
            await db.execute(
                """
                UPDATE enrollments SET teacher=?,teacher_code=?,updated_at=?
                WHERE course_key=? AND stream=? AND status<>'removed'
                  AND (COALESCE(teacher_code,'')='' OR teacher_code=?)
                """,
                (flow["teacher"], new_code, _now(), key[0], key[1], old_code),
            )
            changed.add(key)
        await db.commit()
    return changed


async def _apply_sheet_curators(
    snapshot: dict[str, Any],
    creator_keys: set[tuple[str, str]],
    skip_student_keys: set[tuple[str, str]],
) -> dict[str, Any]:
    flow_changes = 0
    student_changes = 0
    changed_flow_keys: set[tuple[str, str]] = set()
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        for flow in snapshot.get("items") or []:
            key = (_clean(flow.get("course_key"), 50), _clean(flow.get("stream"), 50))
            curator = _clean(flow.get("curator_value"), 100)
            if key in creator_keys or curator not in CURATOR_NAMES:
                continue
            current = await (await db.execute(
                "SELECT teacher,teacher_code,curator_source FROM flow_registry WHERE course_key=? AND stream=?", key
            )).fetchone()
            if not current or _clean(current["curator_source"], 30) == "streams" or _clean(current["teacher_code"], 100) == curator:
                continue
            old_code = _clean(current["teacher_code"], 100)
            teacher = CURATOR_NAMES[curator]
            await db.execute(
                "UPDATE flow_registry SET teacher=?,teacher_code=?,updated_at=? WHERE course_key=? AND stream=?",
                (teacher, curator, _now(), *key),
            )
            await db.execute(
                """
                UPDATE enrollments SET teacher=?,teacher_code=?,updated_at=?
                WHERE course_key=? AND stream=? AND status<>'removed'
                  AND (COALESCE(teacher_code,'')='' OR teacher_code=?)
                """,
                (teacher, curator, _now(), *key, old_code),
            )
            changed_flow_keys.add(key)
            flow_changes += 1

        # Do not keep the writer lock while walking every student in the sheet.
        await db.commit()

        scanned_students = 0
        for flow in snapshot.get("items") or []:
            key = (_clean(flow.get("course_key"), 50), _clean(flow.get("stream"), 50))
            if key in skip_student_keys or key in changed_flow_keys:
                continue
            for student in flow.get("students") or []:
                scanned_students += 1
                if scanned_students % 10 == 0:
                    await db.commit()
                curator = _clean(student.get("responsible_curator"), 100)
                if curator not in CURATOR_NAMES:
                    continue
                rows = await (await db.execute(
                    "SELECT id,teacher_code FROM enrollments WHERE course_key=? AND stream=? AND lower(email)=lower(?) AND status<>'removed'",
                    (*key, _clean(student.get("email"), 300)),
                )).fetchall()
                if len(rows) != 1 or _clean(rows[0]["teacher_code"], 100) == curator:
                    continue
                await db.execute(
                    "UPDATE enrollments SET teacher=?,teacher_code=?,updated_at=? WHERE id=?",
                    (CURATOR_NAMES[curator], curator, _now(), rows[0]["id"]),
                )
                student_changes += 1
        await db.commit()
    return {"flows": flow_changes, "students": student_changes}


async def _seed_from_legacy(snapshot: dict[str, Any], flows: list[dict[str, Any]]) -> int:
    flow_map = {(item["course_key"], item["stream"]): item for item in flows}
    now = _now()
    inserted = 0
    async with _connect() as db:
        scanned_students = 0
        for flow in snapshot.get("items") or []:
            course_key = _clean(flow.get("course_key"), 50)
            stream = _clean(flow.get("stream"), 50)
            registry_flow = flow_map.get((course_key, stream)) or {}
            for student in flow.get("students") or []:
                scanned_students += 1
                if scanned_students % 10 == 0:
                    await db.commit()
                email = _clean(student.get("email"), 300)
                if not email:
                    continue
                source_record_id = int(student.get("source_record_id") or 0)
                enrollment_id = _enrollment_id(source_record_id, course_key, stream, email)
                cur = await db.execute(
                    """
                    INSERT OR IGNORE INTO enrollments(
                        id,source_record_id,order_id,deal_number,gc_user_id,name,email,tg_account,date,
                        course_key,course,stream,tariff,teacher,teacher_code,status,source_json,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        enrollment_id,
                        source_record_id,
                        _clean(student.get("order_id"), 100),
                        _clean(student.get("deal_number") or student.get("order_id"), 100),
                        _clean(student.get("gc_user_id"), 100),
                        _clean(student.get("name"), 300),
                        email,
                        _clean(student.get("tg_account"), 500),
                        _clean(student.get("date"), 100),
                        course_key,
                        _clean(flow.get("course"), 100),
                        stream,
                        _clean(student.get("tariff"), 100),
                        _clean(registry_flow.get("teacher"), 200),
                        _clean(registry_flow.get("teacher_code"), 100),
                        "assigned",
                        json.dumps(student, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                inserted += max(0, int(cur.rowcount or 0))
        await db.commit()
    return inserted


async def _reconcile_sheet_assignments(snapshot: dict[str, Any], flows: list[dict[str, Any]]) -> int:
    """Adopt new sheet students and keep known enrollments on their actual sheet."""
    flow_map = {(item["course_key"], item["stream"]): item for item in flows}
    candidates: dict[str, list[tuple[tuple[int, str, int], dict[str, Any], dict[str, Any]]]] = {}
    for flow in snapshot.get("items") or []:
        stream = _clean(flow.get("stream"), 50)
        try:
            stream_number = int(stream)
        except ValueError:
            stream_number = 0
        for student in flow.get("students") or []:
            email = _norm(student.get("email"))
            if not email:
                continue
            priority = (stream_number, _clean(flow.get("date_start"), 100), int(student.get("row") or 0))
            candidates.setdefault(email, []).append((priority, flow, student))

    async with _connect() as db:
        existing_emails = {
            _norm(row[0])
            for row in await (await db.execute("SELECT email FROM enrollments WHERE status NOT IN ('removed','refunded')")).fetchall()
            if row and _norm(row[0])
        }
        refunded_emails = {
            _norm(row[0])
            for row in await (await db.execute("SELECT email FROM enrollments WHERE status='refunded'")).fetchall()
            if row and _norm(row[0])
        }
    missing_emails = [email for email in candidates if email not in existing_emails]
    identities: dict[str, dict[str, Any]] = {}
    if missing_emails:
        try:
            fields = _module("getcourse-chat-fields", "service_order_identities")
            for offset in range(0, len(missing_emails), 250):
                result = await fields.service_order_identities(
                    identities=[{"key": email, "email": email} for email in missing_emails[offset : offset + 250]]
                )
                identities.update({_norm(item.get("key")): item for item in result.get("items") or []})
        except Exception as exc:
            if _logger:
                _logger.warning("New sheet student identity lookup skipped: %s", exc)

    changed = 0
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        for index, (email, matches) in enumerate(candidates.items(), start=1):
            if email in refunded_emails:
                continue
            if index % 10 == 0:
                await db.commit()
            rows = await (await db.execute(
                "SELECT id,course_key,course,stream,name,email,tg_account,date,tariff,teacher,teacher_code,source_json "
                "FROM enrollments WHERE lower(email)=? AND status NOT IN ('removed','refunded')",
                (email,),
            )).fetchall()
            if not rows:
                # Email is editable by staff.  A unique existing binding to the
                # same flow + row is the stable identity for importing that edit.
                bound_rows: dict[str, aiosqlite.Row] = {}
                for _priority, match_flow, match_student in matches:
                    bound = await (
                        await db.execute(
                            """
                            SELECT id,course_key,course,stream,name,email,tg_account,date,tariff,teacher,teacher_code,source_json
                            FROM enrollments
                            WHERE course_key=? AND stream=? AND status NOT IN ('removed','refunded')
                              AND CAST(json_extract(source_json,'$.row') AS INTEGER)=?
                            """,
                            (
                                _clean(match_flow.get("course_key"), 50),
                                _clean(match_flow.get("stream"), 50),
                                int(match_student.get("row") or 0),
                            ),
                        )
                    ).fetchone()
                    if bound:
                        bound_rows[str(bound["id"])] = bound
                if len(bound_rows) == 1:
                    rows = list(bound_rows.values())
            if not rows:
                identity = identities.get(email) or {}
                if not int(identity.get("source_record_id") or 0) or not _clean(identity.get("gc_user_id"), 100):
                    continue
                assignment = identity.get("assignment") if isinstance(identity.get("assignment"), dict) else {}
                exact = next((
                    item for item in matches
                    if _clean(item[1].get("course_key"), 50) == _clean(assignment.get("course_key"), 50)
                    and _clean(item[1].get("stream"), 50) == _clean(assignment.get("stream"), 50)
                ), None)
                _, flow, student = exact or max(matches, key=lambda item: item[0])
                course_key = _clean(flow.get("course_key"), 50)
                stream = _clean(flow.get("stream"), 50)
                registry_flow = flow_map.get((course_key, stream)) or {}
                student_curator = _clean(student.get("responsible_curator"), 100)
                teacher_code = student_curator if student_curator in CURATOR_NAMES else _clean(registry_flow.get("teacher_code"), 100)
                source = {**identity, **student}
                source["sheet_title"] = _clean(flow.get("sheet_title"), 300)
                source["sheet_id"] = int(flow.get("sheet_id") or 0)
                source["course_assignments"] = sorted(
                    [{
                        "course_key": _clean(match_flow.get("course_key"), 50),
                        "course": _clean(match_flow.get("course"), 100),
                        "stream": _clean(match_flow.get("stream"), 50),
                        "row": int(match_student.get("row") or 0),
                        "sheet_title": _clean(match_flow.get("sheet_title"), 300),
                        "sheet_id": int(match_flow.get("sheet_id") or 0),
                    } for _, match_flow, match_student in matches],
                    key=lambda item: (0 if item["course_key"] == "puppy" else 1, int(item["stream"] or 0)),
                )
                now = _now()
                cur = await db.execute(
                    """
                    INSERT OR IGNORE INTO enrollments(
                        id,source_record_id,order_id,deal_number,gc_user_id,name,email,tg_account,date,
                        course_key,course,stream,tariff,teacher,teacher_code,status,source_json,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _enrollment_id(identity.get("source_record_id"), course_key, stream, email),
                        int(identity.get("source_record_id") or 0), _clean(identity.get("order_id"), 100),
                        _clean(identity.get("deal_number") or identity.get("order_id"), 100),
                        _clean(identity.get("gc_user_id"), 100),
                        _clean(student.get("name") or identity.get("name"), 300), email,
                        _clean(student.get("tg_account"), 500), _clean(student.get("date") or identity.get("date"), 100),
                        course_key, _clean(flow.get("course") or registry_flow.get("course"), 100), stream,
                        _clean(student.get("tariff") or identity.get("tariff"), 100),
                        CURATOR_NAMES.get(teacher_code, _clean(registry_flow.get("teacher"), 200)), teacher_code,
                        "assigned", json.dumps(source, ensure_ascii=False), now, now,
                    ),
                )
                changed += max(0, int(cur.rowcount or 0))
                continue
            if len(rows) != 1:
                continue
            row = rows[0]
            enrollment_id = row["id"]
            same_course = [item for item in matches if _clean(item[1].get("course_key"), 50) == row["course_key"]]
            if not same_course:
                continue
            exact = next((item for item in same_course if _clean(item[1].get("stream"), 50) == row["stream"]), None)
            _, flow, student = exact or max(same_course, key=lambda item: item[0])
            course_key = _clean(flow.get("course_key"), 50)
            stream = _clean(flow.get("stream"), 50)
            registry_flow = flow_map.get((course_key, stream)) or {}
            try:
                source = json.loads(row["source_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                source = {}
            if not isinstance(source, dict):
                source = {}
            source.update(student)
            source["sheet_title"] = _clean(flow.get("sheet_title"), 300)
            source["sheet_id"] = int(flow.get("sheet_id") or 0)
            source["course_assignments"] = sorted(
                [
                    {
                        "course_key": _clean(match_flow.get("course_key"), 50),
                        "course": _clean(match_flow.get("course"), 100),
                        "stream": _clean(match_flow.get("stream"), 50),
                        "row": int(match_student.get("row") or 0),
                        "sheet_title": _clean(match_flow.get("sheet_title"), 300),
                        "sheet_id": int(match_flow.get("sheet_id") or 0),
                    }
                    for _, match_flow, match_student in matches
                ],
                key=lambda item: (0 if item["course_key"] == "puppy" else 1, int(item["stream"] or 0)),
            )
            assignment_changed = (row["course_key"], row["stream"]) != (course_key, stream)
            await db.execute(
                """
                UPDATE enrollments SET course_key=?,course=?,stream=?,name=?,email=?,tg_account=?,date=?,tariff=?,teacher=?,teacher_code=?,
                    source_json=?,status='assigned',updated_at=? WHERE id=?
                """,
                (
                    course_key, _clean(flow.get("course") or registry_flow.get("course") or row["course"], 100), stream,
                    _clean(student.get("name") if "name" in student else row["name"], 300),
                    _clean(student.get("email") if "email" in student else row["email"], 320),
                    _clean(student.get("tg_account") if "tg_account" in student else row["tg_account"], 500),
                    _sheet_date_value(student.get("date") if "date" in student else row["date"]),
                    _clean(student.get("tariff") if "tariff" in student else row["tariff"], 100),
                    _clean(registry_flow.get("teacher") or row["teacher"], 200),
                    _clean(registry_flow.get("teacher_code") or row["teacher_code"], 100),
                    json.dumps(source, ensure_ascii=False),
                    _now(), enrollment_id,
                ),
            )
            if assignment_changed:
                await db.execute("DELETE FROM lesson_progress WHERE enrollment_id=?", (enrollment_id,))
                changed += 1
        await db.commit()
    return changed


async def _import_sheet_lessons(snapshot: dict[str, Any]) -> int:
    changed = 0
    pending_students = 0
    scanned_students = 0
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        for flow in snapshot.get("items") or []:
            course_key = _clean(flow.get("course_key"), 50)
            stream = _clean(flow.get("stream"), 50)
            labels = {item["key"]: item.get("label") or item["key"] for item in flow.get("lesson_columns") or []}
            students_by_email: dict[str, list[dict[str, Any]]] = {}
            for student in flow.get("students") or []:
                students_by_email.setdefault(_norm(student.get("email")), []).append(student)
            for email, students in students_by_email.items():
                scanned_students += 1
                changed_before_student = changed
                rows = await (
                    await db.execute(
                        "SELECT id,source_json FROM enrollments WHERE course_key=? AND stream=? AND lower(email)=lower(?) AND status<>'removed'",
                        (course_key, stream, email),
                    )
                ).fetchall()
                if len(rows) != 1:
                    continue
                student = students[0]
                if len(students) > 1:
                    try:
                        source_row = int(json.loads(rows[0]["source_json"] or "{}").get("row") or 0)
                    except (TypeError, ValueError, json.JSONDecodeError):
                        source_row = 0
                    exact = [item for item in students if int(item.get("row") or 0) == source_row]
                    if len(exact) != 1:
                        continue
                    student = exact[0]
                enrollment_id = rows[0]["id"]
                try:
                    source = json.loads(rows[0]["source_json"] or "{}")
                except (TypeError, json.JSONDecodeError):
                    source = {}
                if not isinstance(source, dict):
                    source = {}
                sheet_row = int(student.get("row") or 0)
                if sheet_row and int(source.get("row") or 0) != sheet_row:
                    source["row"] = sheet_row
                    await db.execute(
                        "UPDATE enrollments SET source_json=?,updated_at=? WHERE id=?",
                        (json.dumps(source, ensure_ascii=False), _now(), enrollment_id),
                    )
                    changed += 1
                for key, value in (student.get("lessons") or {}).items():
                    current = await (
                        await db.execute(
                            "SELECT value,sheet_value,dirty FROM lesson_progress WHERE enrollment_id=? AND lesson_key=?",
                            (enrollment_id, key),
                        )
                    ).fetchone()
                    numeric = 1 if value else 0
                    if not current:
                        await db.execute(
                            "INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at) VALUES(?,?,?,?,?,0,?)",
                            (enrollment_id, key, labels.get(key, key), numeric, numeric, _now()),
                        )
                        changed += 1
                    elif (
                        int(current[0]) != numeric
                        or int(current[1]) != numeric
                        or int(current[2])
                    ):
                        await db.execute(
                            "UPDATE lesson_progress SET value=?,sheet_value=?,dirty=0,label=?,updated_at=? WHERE enrollment_id=? AND lesson_key=?",
                            (numeric, numeric, labels.get(key, key), _now(), enrollment_id, key),
                        )
                        changed += 1
                if changed > changed_before_student:
                    pending_students += 1
                if pending_students and (pending_students >= 10 or scanned_students % 25 == 0):
                    await db.commit()
                    pending_students = 0
        await db.commit()
    return changed


async def _student_by_id(enrollment_id: str, *, resolve_external: bool = False) -> dict[str, Any]:
    key = _clean(enrollment_id, 100)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                """SELECT e.*,f.date_start AS flow_date_start,f.vk_link,f.tg_link,
                          f.teacher AS flow_teacher,f.teacher_code AS flow_teacher_code
                   FROM enrollments e
                   LEFT JOIN flow_registry f ON f.course_key=e.course_key AND f.stream=e.stream
                   WHERE e.id=? AND e.status<>'removed' LIMIT 1""",
                (key,),
            )
        ).fetchone()
        lesson_rows = await (
            await db.execute(
                """SELECT lesson_key,label,value FROM lesson_progress
                   WHERE enrollment_id=? ORDER BY length(lesson_key),lesson_key""",
                (key,),
            )
        ).fetchall() if row else []
    if not row:
        raise HTTPException(404, "Ученик не найден")
    stored = dict(row)
    try:
        source = json.loads(stored.get("source_json") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        source = {}
    if not isinstance(source, dict):
        source = {}
    student = {
        **source,
        **stored,
        "enrollment_id": stored["id"],
        "phone": _clean(source.get("phone") or source.get("user_phone"), 100),
        "course_assignments": source.get("course_assignments") or [],
        "lessons": [
            {"key": lesson["lesson_key"], "label": lesson["label"], "value": bool(lesson["value"])}
            for lesson in lesson_rows
        ],
    }
    item = _student_result(
        {
            "course_key": stored["course_key"],
            "course": stored["course"],
            "stream": stored["stream"],
            "curator_value": stored.get("teacher_code") or stored.get("flow_teacher_code"),
            "teacher": stored.get("teacher") or stored.get("flow_teacher"),
            "sheet_title": stored.get("sheet_title"),
        },
        student,
    )
    item["refunded"] = stored.get("status") == "refunded"
    refund = source.get("refund") if isinstance(source.get("refund"), dict) else {}
    item["refunded_at"] = _clean(refund.get("received_at"), 100)
    await asyncio.gather(_enrich_student_notes([item]), _enrich_student_financials([item]))
    if not resolve_external:
        return item
    try:
        await asyncio.wait_for(
            _enrich_order_identities([item]),
            timeout=STUDENT_CARD_IDENTITY_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        if _logger:
            _logger.warning("Opened student identity lookup timed out enrollment_id=%s", key)
    await _enrich_opened_student_external(item)
    return item


async def _widget_student_base(enrollment_id: str) -> dict[str, Any]:
    """Return immediately usable card facts without rebuilding Streams."""
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                """SELECT e.*,f.teacher AS flow_teacher,f.teacher_code AS flow_teacher_code
                   FROM enrollments e
                   LEFT JOIN flow_registry f ON f.course_key=e.course_key AND f.stream=e.stream
                   WHERE e.id=? AND e.status<>'removed' LIMIT 1""",
                (_clean(enrollment_id, 100),),
            )
        ).fetchone()
    if not row:
        raise HTTPException(404, "Ученик не найден")
    stored = dict(row)
    try:
        source = json.loads(stored.get("source_json") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        source = {}
    student = {
        **source,
        **stored,
        "enrollment_id": _clean(stored.get("id"), 100),
        "phone": _clean(source.get("phone") or source.get("user_phone"), 100),
        "course_assignments": source.get("course_assignments") or [],
        "lessons": [],
    }
    teacher_code = _clean(stored.get("teacher_code") or stored.get("flow_teacher_code"), 100)
    teacher_name = _clean(stored.get("teacher") or stored.get("flow_teacher"), 200)
    item = _student_result(
        {
            "course_key": _clean(stored.get("course_key"), 50),
            "course": _clean(stored.get("course"), 100),
            "stream": _clean(stored.get("stream"), 50),
            "curator_value": teacher_code,
        },
        student,
    )
    if teacher_name:
        item["curator_name"] = teacher_name
    profile_id = _clean(item.get("gc_user_id"), 100)
    if profile_id:
        item["user_url"] = f"https://club.sobakovod.pro/user/control/user/update/id/{quote(profile_id)}"
    try:
        await asyncio.wait_for(_enrich_successful_managers([item]), timeout=1.5)
    except TimeoutError:
        item.setdefault("manager_name", "")
        item.setdefault("manager_id", "")
    return item


async def service_widget_lessons(*, enrollment_id: str) -> dict[str, Any]:
    """Load only lesson and call progress for the mini card."""
    key = _clean(enrollment_id, 100)
    if key.startswith("gc:"):
        return {"ok": True, "enrollment_id": key, "lessons": []}
    async with _connect() as db:
        exists = await (
            await db.execute(
                "SELECT 1 FROM enrollments WHERE id=? AND status<>'removed' LIMIT 1", (key,),
            )
        ).fetchone()
        if not exists:
            raise HTTPException(404, "Ученик не найден")
        rows = await (
            await db.execute(
                """SELECT lesson_key,label,value FROM lesson_progress
                   WHERE enrollment_id=? ORDER BY length(lesson_key),lesson_key""",
                (key,),
            )
        ).fetchall()
    return {
        "ok": True,
        "enrollment_id": key,
        "lessons": [
            {"key": _clean(row[0], 20), "label": _clean(row[1], 200), "value": bool(row[2])}
            for row in rows
        ],
    }


async def service_widget_student(
    *, gc_user_id: str = "", email: str = "", phone: str = "", name: str = "",
    include_access: bool = True, summary_only: bool = False,
) -> dict[str, Any]:
    """Compact Streams card for other Nexus modules; matching stays exact."""
    gc_id = _clean(gc_user_id, 100)
    email_key = _norm(email)
    phone_key = _phone_search_key(phone)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = None
        if gc_id:
            row = await (await db.execute(
                """SELECT id,gc_user_id FROM enrollments WHERE gc_user_id=? AND status<>'removed'
                   ORDER BY date DESC,updated_at DESC LIMIT 1""", (gc_id,),
            )).fetchone()
        if not row and email_key:
            row = await (await db.execute(
                """SELECT id,gc_user_id FROM enrollments WHERE lower(email)=? AND status<>'removed'
                   ORDER BY date DESC,updated_at DESC LIMIT 1""", (email_key,),
            )).fetchone()
        if not row and len(phone_key) >= 10:
            await db.create_function("nexus_phone_key", 1, _phone_search_key, deterministic=True)
            rows = await (await db.execute(
                """SELECT id,gc_user_id,source_json FROM enrollments
                   WHERE status<>'removed' AND nexus_phone_key(
                     COALESCE(json_extract(source_json,'$.phone'),json_extract(source_json,'$.user_phone'),'')
                   )=?
                   ORDER BY date DESC,updated_at DESC LIMIT 2""", (phone_key,),
            )).fetchall()
            exact = []
            for candidate in rows:
                try:
                    source = json.loads(candidate["source_json"] or "{}")
                except (TypeError, ValueError):
                    source = {}
                if _phone_search_key(source.get("phone") or source.get("user_phone")) == phone_key:
                    exact.append(candidate)
            row = exact[0] if len(exact) == 1 else None
    if not row:
        try:
            resolver = _module("chat-moderators", "service_resolve_access_user")
            prospect = await asyncio.to_thread(
                resolver.service_resolve_access_user,
                gc_user_id=gc_id, email=email, phone=phone,
            )
        except Exception as exc:
            if _logger:
                _logger.warning("GetCourse prospect lookup skipped: %s", exc)
            prospect = {"found": False}
        if not prospect.get("found") and gc_id.isdigit():
            # The messenger identity graph already resolved this exact
            # GetCourse ID from the current card's phone/email.  Access
            # snapshots are intentionally sparse, so their absence must not
            # hide a real GetCourse profile from the operator.
            prospect = {
                "found": True, "gc_user_id": gc_id,
                "email": email, "phone": phone, "full_name": name,
            }
        if not prospect.get("found"):
            return {"ok": True, "found": False, "paid_access": False}
        matched_gc_id = _clean(prospect.get("gc_user_id"), 100)
        if not matched_gc_id:
            return {"ok": True, "found": False, "paid_access": False}
        item = {
            "enrollment_id": f"gc:{matched_gc_id}",
            "gc_user_id": matched_gc_id,
            "name": _clean(prospect.get("full_name"), 300),
            "email": _clean(prospect.get("email") or email, 320),
            "phone": _clean(prospect.get("phone") or phone, 100),
            "course_key": "",
            "course": "Доступ ещё не куплен",
            "course_display": "Доступ ещё не куплен",
            "stream": "",
            "stream_display": "",
            "tariff": "",
            "lessons": [],
            "prospect": True,
        }
        await _remember_widget_prospect(item)
        return {
            "ok": True, "found": True, "paid_access": False,
            "gc_user_id": matched_gc_id,
            "profile_url": f"https://club.sobakovod.pro/user/control/user/update/id/{quote(matched_gc_id)}",
            "item": item,
        }
    matched_gc_id = _clean(row["gc_user_id"], 100)
    profile_url = (
        f"https://club.sobakovod.pro/user/control/user/update/id/{quote(matched_gc_id)}"
        if matched_gc_id else ""
    )
    if summary_only:
        item = await _widget_student_base(_clean(row["id"], 100))
        return {
            "ok": True, "found": True, "paid_access": True,
            "gc_user_id": matched_gc_id,
            "profile_url": item.get("user_url") or profile_url,
            "item": item,
        }
    item = await _student_by_id(_clean(row["id"], 100))
    result = {
        "ok": True, "found": True, "paid_access": True, "item": item,
        "profile_url": item.get("user_url") or profile_url,
    }
    if include_access:
        result["access"] = await _student_access_view(item["enrollment_id"], live=False)
    return result


async def _remember_widget_prospect(item: dict[str, Any]) -> None:
    gc_user_id = _clean(item.get("gc_user_id"), 100)
    if not gc_user_id.isdigit():
        return
    value = json.dumps({
        "gc_user_id": gc_user_id,
        "email": _clean(item.get("email"), 320),
        "phone": _clean(item.get("phone"), 100),
        "name": _clean(item.get("name"), 300),
        "updated_at": _now(),
    }, ensure_ascii=False, separators=(",", ":"))
    await _meta_set(f"widget_prospect:{gc_user_id}", value)


async def _widget_prospect_identity(gc_user_id: str) -> dict[str, Any]:
    raw = await _meta_get(f"widget_prospect:{_clean(gc_user_id, 100)}")
    try:
        value = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        value = {}
    return value if isinstance(value, dict) else {}


async def _student_access_view(enrollment_id: str, *, live: bool = False) -> dict[str, Any]:
    identity = await _access_identity(enrollment_id)
    current = await _get_access_view(identity, live=live, force=live, allow_stale=not live)
    if not current.get("ok") or current.get("refresh_due") or current.get("stale"):
        current = await _queue_access_refresh(enrollment_id, current)
    pending = await _pending_access(identity)
    if not pending.get("pending"):
        if live and (
            not current.get("ok")
            or current.get("stale")
            or current.get("refresh_due")
            or current.get("source") not in {"live", "browser"}
        ):
            return {
                **current,
                "ok": False,
                "items": [],
                "current_groups": [],
                "error": current.get("error") or current.get("warning") or "GetCourse ещё не вернул доступы",
            }
        return current
    if live and current.get("ok") and not current.get("stale") and current.get("source") in {"live", "browser"}:
        verifier = _module("chat-moderators", "service_record_access_verification")
        verification = await asyncio.to_thread(
            verifier.service_record_access_verification,
            request_id=pending["request_id"], actual_groups=current.get("current_groups") or [], defer_on_mismatch=True,
        )
        if verification.get("verified"):
            return current
    return _access_target_view(
        current,
        pending.get("target_groups") or [],
        pending.get("next_check_at") or "",
        ready_by=pending.get("ready_by") or "",
        stage=pending.get("stage") or "verifying",
    )


async def _preview_access_change(
    *, enrollment_id: str, changes: list[dict[str, Any]], requester_user_id: str,
) -> dict[str, Any]:
    identity = await _access_identity(enrollment_id)
    current = await _get_access_view(identity, live=False, allow_stale=True)
    if not current.get("ok"):
        raise RuntimeError("Не удалось проверить текущие доступы. Попробуйте ещё раз через минуту")
    _guard_protected_package_changes(current, changes)
    fields = _module("getcourse-chat-fields", "service_getcourse_access_budget")
    budget = await fields.service_getcourse_access_budget()
    verification_delayed = (
        int(budget.get("requests_left_2h") or 0) < int(budget.get("needed_for_verification") or 6)
    )
    verification_delayed = True
    access = _module("chat-moderators", "service_prepare_access_change")
    prepared = await asyncio.to_thread(
        access.service_prepare_access_change,
        gc_user_id=_clean(identity.get("gc_user_id"), 100),
        email=_clean(identity.get("email"), 300),
        current_groups=current.get("current_groups") or [],
        changes=changes,
        requester_user_id=_clean(requester_user_id, 200),
    )
    return {
        "ok": True,
        "request_id": prepared["request_id"],
        "added": prepared.get("added") or [],
        "removed": prepared.get("removed") or [],
        "expires_in": prepared.get("expires_in") or 60,
        "access": current,
        "verification_delayed": verification_delayed,
        "next_check_at": budget.get("next_at") or "",
    }


def _guard_protected_package_changes(
    current: dict[str, Any], changes: list[dict[str, Any]],
) -> None:
    items = [item for item in current.get("items") or [] if isinstance(item, dict)]
    by_id = {str(item.get("group_id") or ""): item for item in items}
    current_packages: dict[str, set[str]] = {}
    for item in items:
        package_key = _clean(item.get("package_key"), 50)
        course_key = _clean(item.get("course_key"), 50)
        if (
            item.get("group_kind") == "package"
            and package_key in PROTECTED_PACKAGE_KEYS
            and bool(item.get("enabled"))
            and course_key
        ):
            current_packages.setdefault(course_key, set()).add(package_key)
    if not current_packages:
        return
    for change in changes:
        group = by_id.get(_clean(change.get("group_id"), 30))
        if not group or group.get("group_kind") != "package":
            continue
        course_key = _clean(group.get("course_key"), 50)
        selected = current_packages.get(course_key)
        if not selected:
            continue
        package_key = _clean(group.get("package_key"), 50)
        if bool(change.get("enabled")) and package_key in selected:
            continue
        raise RuntimeError(
            "Пакет ученика нельзя менять через обычное сохранение доступов. "
            f"Обратитесь к администратору: {PACKAGE_CHANGE_ADMIN_URL}"
        )


async def _apply_access_change(
    *, enrollment_id: str, request_id: str, requester_user_id: str,
) -> dict[str, Any]:
    lock = _access_apply_locks.setdefault(_clean(enrollment_id, 100), asyncio.Lock())
    if lock.locked():
        raise RuntimeError("Другой сотрудник уже меняет доступы этого ученика. Подождите немного.")
    async with lock:
        identity = await _access_identity(enrollment_id)
        access = _module("chat-moderators", "service_apply_access_change")
        requester = _clean(requester_user_id, 200)
        prepared = await asyncio.to_thread(
            access.service_access_change_request,
            request_id=request_id,
            requester_user_id=requester,
        )
        expected = {
            value.casefold()
            for value in (
                _clean(identity.get("gc_user_id"), 100),
                _clean(identity.get("email"), 320),
            )
            if value
        }
        actual = {
            value.casefold()
            for value in (
                _clean(prepared.get("gc_user_id"), 100),
                _clean(prepared.get("identifier"), 320),
            )
            if value
        }
        if not expected or expected.isdisjoint(actual):
            raise RuntimeError("Эта проверка была создана для другого ученика. Откройте ученика заново.")
        scheduler = _module("chat-moderators", "service_schedule_access_apply")
        scheduled = await asyncio.to_thread(
            scheduler.service_schedule_access_apply,
            request_id=request_id,
            requester_user_id=requester,
            delay_seconds=2,
        )
        promotion_queued = False
        if identity.get("prospect"):
            promotion_queued = await _queue_manual_promotion(
                request_id=request_id,
                identity=identity,
                target_groups=scheduled.get("target_groups") or prepared.get("target_groups") or [],
            )
        current = await _get_access_view(identity, live=False, allow_stale=True)
        target_groups = scheduled.get("target_groups") or []
        return {
            "ok": True,
            "queued": True,
            "applied": False,
            "verified": False,
            "verification_pending": True,
            "operation_id": request_id,
            "verification_delayed": True,
            "next_check_at": scheduled.get("next_check_at") or "",
            "ready_by": scheduled.get("ready_by") or "",
            "promotion_queued": promotion_queued,
            "access": _access_target_view(
                current,
                target_groups,
                scheduled.get("next_check_at") or "",
                ready_by=scheduled.get("ready_by") or "",
                stage="queued",
            ),
        }


def _manual_package_selections(groups: list[dict[str, Any]]) -> dict[str, str]:
    selected: dict[str, str] = {}
    for group in groups:
        course_key = _clean(group.get("course_key"), 50)
        package_key = _clean(group.get("package_key"), 50)
        if course_key in {"puppy", "dog"} and group.get("group_kind") == "package" and package_key:
            selected[course_key] = package_key
    return selected


async def _queue_manual_promotion(
    *, request_id: str, identity: dict[str, Any], target_groups: list[dict[str, Any]],
) -> bool:
    packages = _manual_package_selections(target_groups)
    if not packages:
        return False
    await _meta_set(
        f"manual_promotion:{_clean(request_id, 64)}",
        json.dumps({
            "request_id": _clean(request_id, 64),
            "gc_user_id": _clean(identity.get("gc_user_id"), 100),
            "email": _clean(identity.get("email"), 320),
            "phone": _clean(identity.get("phone"), 100),
            "name": _clean(identity.get("name"), 300),
            "packages": packages,
            "attempts": 0,
            "next_at": _now(),
        }, ensure_ascii=False),
    )
    return True


def _manual_tariff_label(package_key: str) -> str:
    return {
        "standard": "Стандарт", "premium": "Премиум", "vip": "ВИП",
        "mentorship": "Наставничество", "module_standard": "Помодульно",
    }.get(_clean(package_key, 50), _clean(package_key, 100))


async def _latest_ready_flow(course_key: str) -> dict[str, Any]:
    async with _connect() as db:
        row = await (
            await db.execute(
                "SELECT * FROM flow_registry WHERE course_key=? AND status='ready' "
                "ORDER BY date_start DESC,CAST(stream AS INTEGER) DESC LIMIT 1",
                (_clean(course_key, 50),),
            )
        ).fetchone()
    return dict(row) if row else {}


async def _promote_manual_student(job: dict[str, Any]) -> None:
    gc_user_id = _clean(job.get("gc_user_id"), 100)
    if not gc_user_id.isdigit():
        raise RuntimeError("Для добавления в Streams не найден GetCourse ID")
    packages = job.get("packages") if isinstance(job.get("packages"), dict) else {}
    now = _now()
    for course_key, package_key in packages.items():
        if course_key not in {"puppy", "dog"}:
            continue
        flow = await _latest_ready_flow(course_key)
        stream = _clean(flow.get("stream"), 50)
        course = _clean(flow.get("course"), 100) or ("Щенок" if course_key == "puppy" else "Собака")
        enrollment_id = f"manual:{gc_user_id}:{course_key}"
        source = {
            "source": "messenger-access",
            "phone": _clean(job.get("phone"), 100),
            "manual_access_request_id": _clean(job.get("request_id"), 64),
            "course_assignments": ([{
                "course_key": course_key, "course": course, "stream": stream,
                "sheet_title": _clean(flow.get("sheet_title"), 300),
            }] if stream else []),
        }
        async with _connect() as db:
            await db.execute(
                """INSERT INTO enrollments(
                    id,source_record_id,order_id,deal_number,gc_user_id,name,email,tg_account,date,
                    course_key,course,stream,tariff,teacher,teacher_code,status,source_json,created_at,updated_at
                ) VALUES(?,0,'','',?,?,?,'',?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,email=excluded.email,stream=excluded.stream,tariff=excluded.tariff,
                    teacher=excluded.teacher,teacher_code=excluded.teacher_code,status=excluded.status,
                    source_json=excluded.source_json,updated_at=excluded.updated_at""",
                (
                    enrollment_id, gc_user_id, _clean(job.get("name"), 300),
                    _clean(job.get("email"), 320), now, course_key, course, stream,
                    _manual_tariff_label(package_key), _clean(flow.get("teacher"), 200),
                    _clean(flow.get("teacher_code"), 100), "assigned" if stream else "pending",
                    json.dumps(source, ensure_ascii=False), now, now,
                ),
            )
            await db.commit()
        if stream:
            student = await _widget_student_base(enrollment_id)
            fields = _module("getcourse-chat-fields", "service_registry_ensure_student")
            result = await fields.service_registry_ensure_student(
                course_key=course_key, stream=stream, student=student,
            )
            await _bind_sheet_row(
                enrollment_id, int(result.get("row") or 0), result.get("lesson_columns") or [],
            )
    _clear_snapshot_cache()


async def _process_manual_promotions() -> None:
    async with _connect() as db:
        row = await (
            await db.execute(
                "SELECT key,value FROM registry_meta WHERE key LIKE 'manual_promotion:%' "
                "AND COALESCE(json_extract(value,'$.next_at'),'')<=? "
                "ORDER BY json_extract(value,'$.next_at'),key LIMIT 1",
                (_now(),),
            )
        ).fetchone()
    if not row:
        return
    key, raw = str(row[0]), str(row[1] or "{}")
    try:
        job = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        job = {}
    if _clean(job.get("next_at"), 40) > _now():
        return
    request_id = _clean(job.get("request_id"), 64)
    state = await _test_period_request_state(request_id)
    if state == "pending":
        return
    if state != "verified":
        attempts = int(job.get("attempts") or 0) + 1
        job.update(attempts=attempts, next_at=_test_period_retry_at(attempts), error="Выдача тарифа ещё не подтверждена")
        await _meta_set(key, json.dumps(job, ensure_ascii=False))
        return
    try:
        await _promote_manual_student(job)
    except Exception as exc:
        attempts = int(job.get("attempts") or 0) + 1
        job.update(attempts=attempts, next_at=_test_period_retry_at(attempts), error=_clean(exc, 1000))
        await _meta_set(key, json.dumps(job, ensure_ascii=False))
        if _logger:
            _logger.warning("Manual Streams promotion %s deferred: %s", request_id, exc)
        return
    async with _connect() as db:
        await db.execute("DELETE FROM registry_meta WHERE key=?", (key,))
        await db.commit()


async def service_widget_access(*, enrollment_id: str, live: bool = False) -> dict[str, Any]:
    return await _student_access_view(enrollment_id, live=live)


async def service_widget_access_preview(
    *, enrollment_id: str, changes: list[dict[str, Any]], requester_user_id: str,
) -> dict[str, Any]:
    return await _preview_access_change(
        enrollment_id=enrollment_id,
        changes=changes,
        requester_user_id=requester_user_id,
    )


async def service_widget_access_apply(
    *, enrollment_id: str, request_id: str, requester_user_id: str,
) -> dict[str, Any]:
    return await _apply_access_change(
        enrollment_id=enrollment_id,
        request_id=request_id,
        requester_user_id=requester_user_id,
    )


async def service_widget_access_operation(*, request_id: str) -> dict[str, Any]:
    """Return the durable verification state for a widget access command."""
    state = await _test_period_request_state(request_id)
    return {
        "request_id": _clean(request_id, 64),
        "status": state,
        "operation_pending": state in {"pending", "missing"},
    }


async def _bind_sheet_row(enrollment_id: str, row: int, lesson_columns: list[dict[str, Any]]) -> None:
    now = _now()
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        current = await (await db.execute(
            "SELECT source_json FROM enrollments WHERE id=? AND status<>'removed'", (enrollment_id,)
        )).fetchone()
        if not current:
            raise HTTPException(404, "Ученик не найден")
        try:
            source = json.loads(current["source_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        source["row"] = int(row)
        await db.execute(
            "UPDATE enrollments SET source_json=?,updated_at=? WHERE id=?",
            (json.dumps(source, ensure_ascii=False), now, enrollment_id),
        )
        for lesson in lesson_columns:
            key = _clean(lesson.get("key"), 5).upper()
            if not key:
                continue
            await db.execute(
                """
                INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at)
                VALUES(?,?,?,?,?,0,?)
                ON CONFLICT(enrollment_id,lesson_key) DO UPDATE SET label=excluded.label
                """,
                (enrollment_id, key, _clean(lesson.get("label") or key, 200), 0, 0, now),
            )
        await db.commit()
    _clear_snapshot_cache()


def _sheet_operation_time(seconds: int = 120) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0, seconds))).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _insert_sheet_operation(
    db: aiosqlite.Connection,
    *,
    student: dict[str, Any],
    action: str,
    operator_id: int = 0,
    operator_name: str = "Streams",
    lesson: dict[str, Any] | None = None,
    value: bool | None = None,
    expected_value: bool | None = None,
) -> dict[str, Any]:
    """Insert an idempotent Google write into the existing durable operation journal."""
    enrollment_id = _clean(student.get("enrollment_id") or student.get("id"), 100)
    lesson_key = _clean((lesson or {}).get("key"), 5).upper()
    active = await (
        await db.execute(
            """
            SELECT id,status,steps_json FROM transfers
            WHERE enrollment_id=? AND status IN ('queued','running','waiting')
              AND json_extract(steps_json,'$.preview.action')=?
              AND (?='' OR json_extract(steps_json,'$.preview.lesson.key')=?)
            ORDER BY created_at DESC,id DESC LIMIT 1
            """,
            (enrollment_id, action, lesson_key, lesson_key),
        )
    ).fetchone()
    if active:
        active_steps = _load_steps(active["steps_json"])
        requested = ((active_steps.get("preview") or {}).get("lesson") or {}).get("value")
        if action == "lesson_update" and bool(requested) != bool(value):
            raise HTTPException(409, "Предыдущая отметка ещё выполняется. Дождитесь её завершения")
        preview = active_steps.get("preview") or {}
        return {
            "ok": True,
            "accepted": True,
            "existing": True,
            "id": active["id"],
            "status": active["status"],
            "scheduled_at": preview.get("scheduled_at") or _sheet_operation_time(),
            "value": value,
        }
    operation_id = uuid.uuid4().hex
    now = _now()
    # User-facing completion deadline. A Google request can itself take tens
    # of seconds, so the former "+2 seconds" was a false promise.
    scheduled_at = _sheet_operation_time(120 if action == "lesson_update" else 300)
    preview: dict[str, Any] = {
        "action": action,
        "scheduled_at": scheduled_at,
        "source": {
            "course_key": _clean(student.get("course_key"), 50),
            "stream": _clean(student.get("stream"), 50),
            "row": int(student.get("row") or 0),
            "email": _clean(student.get("email"), 320),
        },
        "target": {},
    }
    if action == "lesson_update":
        preview["lesson"] = {
            "key": lesson_key,
            "label": _clean((lesson or {}).get("label") or lesson_key, 200),
            "value": bool(value),
            "expected_value": bool(expected_value),
        }
    await db.execute(
        """
        INSERT INTO transfers(
            id,enrollment_id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
            target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
            student_json,steps_json,error,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            operation_id, enrollment_id, "queued", _clean(student.get("email"), 320),
            _clean(student.get("gc_user_id"), 100), _clean(student.get("name"), 300),
            _clean(student.get("course_key"), 50), _clean(student.get("stream"), 50), int(student.get("row") or 0),
            _clean(student.get("course_key"), 50), _clean(student.get("stream"), 50), "", 0,
            int(operator_id or 0), _clean(operator_name, 200) or "Streams",
            json.dumps(student, ensure_ascii=False), json.dumps({"preview": preview}, ensure_ascii=False),
            "", now, now,
        ),
    )
    return {
        "ok": True, "accepted": True, "id": operation_id, "status": "queued",
        "scheduled_at": scheduled_at, "value": value,
    }


async def _queue_sheet_operation(**kwargs: Any) -> dict[str, Any]:
    async with _operation_queue_lock, _connect() as db:
        result = await _insert_sheet_operation(db, **kwargs)
        await db.commit()
        return result


async def _queue_lesson_update(
    *, enrollment_id: str, lesson_key: str, data: LessonUpdateIn,
    operator_id: int, operator_name: str,
) -> dict[str, Any]:
    """Accept a lesson command from local durable state without provider calls."""

    clean_enrollment_id = _clean(enrollment_id, 100)
    clean_lesson_key = _clean(lesson_key, 5).upper()
    async with _operation_queue_lock, _connect(timeout=2) as db:
        row = await (
            await db.execute(
                """SELECT e.*,p.lesson_key,p.label AS lesson_label,p.value AS lesson_value
                   FROM enrollments e
                   LEFT JOIN lesson_progress p
                     ON p.enrollment_id=e.id AND upper(p.lesson_key)=?
                   WHERE e.id=? AND e.status<>'removed' LIMIT 1""",
                (clean_lesson_key, clean_enrollment_id),
            )
        ).fetchone()
        if not row:
            raise HTTPException(404, "Ученик не найден")
        stored = dict(row)
        if not stored.get("lesson_key"):
            raise HTTPException(404, "Отметка не найдена")
        try:
            source = json.loads(stored.get("source_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        student = {
            **source,
            "enrollment_id": clean_enrollment_id,
            "email": _clean(stored.get("email"), 320),
            "gc_user_id": _clean(stored.get("gc_user_id"), 100),
            "name": _clean(stored.get("name"), 300),
            "course_key": _clean(stored.get("course_key"), 50),
            "course": _clean(stored.get("course"), 100),
            "stream": _clean(stored.get("stream"), 50),
            "row": int(source.get("row") or 0),
        }
        if not student["row"]:
            raise HTTPException(409, "Ученик ещё не добавлен в таблицу")
        current_value = bool(stored.get("lesson_value"))
        if current_value != bool(data.expected_value):
            raise HTTPException(409, "Отметка уже изменилась. Обновите данные")
        result = await _insert_sheet_operation(
            db,
            student=student,
            action="lesson_update",
            operator_id=operator_id,
            operator_name=operator_name,
            lesson={
                "key": _clean(stored.get("lesson_key"), 5).upper(),
                "label": _clean(stored.get("lesson_label") or clean_lesson_key, 200),
                "value": current_value,
            },
            value=bool(data.value),
            expected_value=bool(data.expected_value),
        )
        await db.commit()
        return result


async def _queue_card_command(
    *, student: dict[str, Any], action: str, operator: dict[str, Any],
    payload: dict[str, Any] | None = None, request_id: str = "",
) -> dict[str, Any]:
    """Persist a rate-limited card command before touching its provider."""

    if action not in {"chat_delivery", "messenger_send"}:
        raise RuntimeError("Неизвестная команда карточки")
    clean_request_id = _clean(request_id, 64)
    async with _operation_queue_lock, _connect() as db:
        if clean_request_id:
            active = await (
                await db.execute(
                    """
                    SELECT id,status,steps_json FROM transfers
                    WHERE enrollment_id=?
                      AND json_extract(steps_json,'$.preview.action')=?
                      AND json_extract(steps_json,'$.preview.request_id')=?
                    ORDER BY created_at DESC,id DESC LIMIT 1
                    """,
                    (student["enrollment_id"], action, clean_request_id),
                )
            ).fetchone()
            if active:
                preview = (_load_steps(active["steps_json"]).get("preview") or {})
                return {
                    "ok": True, "accepted": True, "existing": True,
                    "id": active["id"], "status": active["status"],
                    "scheduled_at": preview.get("scheduled_at") or _sheet_operation_time(120),
                }
        operation_id = uuid.uuid4().hex
        now = _now()
        scheduled_at = _sheet_operation_time(120)
        preview = {
            "action": action,
            "request_id": clean_request_id,
            "scheduled_at": scheduled_at,
            "payload": payload or {},
            "source": {
                "course_key": _clean(student.get("course_key"), 50),
                "stream": _clean(student.get("stream"), 50),
                "row": int(student.get("row") or 0),
                "email": _clean(student.get("email"), 320),
            },
            "target": {},
        }
        await db.execute(
            """
            INSERT INTO transfers(
                id,enrollment_id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                student_json,steps_json,error,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                operation_id, student["enrollment_id"], "queued", _clean(student.get("email"), 320),
                _clean(student.get("gc_user_id"), 100), _clean(student.get("name"), 300),
                _clean(student.get("course_key"), 50), _clean(student.get("stream"), 50), int(student.get("row") or 0),
                _clean(student.get("course_key"), 50), _clean(student.get("stream"), 50), "", 0,
                int(operator.get("id") or 0), _clean(operator.get("display_name") or operator.get("login"), 200),
                json.dumps(student, ensure_ascii=False), json.dumps({"preview": preview}, ensure_ascii=False),
                "", now, now,
            ),
        )
        await db.commit()
    return {
        "ok": True, "accepted": True, "id": operation_id, "status": "queued",
        "scheduled_at": scheduled_at,
    }


async def _mirror_payload() -> list[dict[str, Any]]:
    flows = await _flow_rows()
    enrollments = await _enrollment_rows()
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in enrollments:
        if not item.get("stream") or item.get("status") == "pending":
            continue
        grouped.setdefault((item["course_key"], item["stream"]), []).append(
            {
                **item,
                "lessons": {lesson["key"]: lesson["value"] for lesson in item.get("lessons") or []},
            }
        )
    return [{**flow, "students": grouped.get((flow["course_key"], flow["stream"]), [])} for flow in flows]


def _order_event_at(order: dict[str, Any]) -> datetime | None:
    for key in (
        "first_payment_at", "paid_at", "payed_at", "payment_date", "received_at",
        "date_creation", "created_at", "updated_at", "date",
    ):
        value = _clean(order.get(key), 100)
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            sheet_date = _sheet_date_value(value)
            try:
                parsed = datetime.fromisoformat(sheet_date)
            except ValueError:
                continue
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


async def _assign_new_orders(orders: dict[str, Any], flows: list[dict[str, Any]]) -> int:
    ready: dict[str, list[tuple[datetime, dict[str, Any]]]] = {}
    for flow in flows:
        if flow.get("status") != "ready" or not str(flow.get("stream") or "").isdigit():
            continue
        try:
            start = datetime.fromisoformat(_clean(flow.get("date_start"), 100).replace("Z", "+00:00"))
        except ValueError:
            continue
        ready.setdefault(flow["course_key"], []).append((start, flow))
    activation_times: dict[tuple[str, str], datetime] = {}
    managed_flows: set[tuple[str, str]] = set()
    async with _connect() as db:
        rows = await (
            await db.execute(
                """SELECT course_key,stream,status,updated_at FROM flow_jobs
                   ORDER BY created_at,id"""
            )
        ).fetchall()
    for course_key, stream, status, updated_at in rows:
        key = (_clean(course_key, 50), _clean(stream, 50))
        managed_flows.add(key)
        if status != "completed" or not _clean(updated_at, 100):
            activation_times.pop(key, None)
            continue
        try:
            activated = datetime.fromisoformat(_clean(updated_at, 100).replace("Z", "+00:00"))
        except ValueError:
            continue
        activation_times[key] = activated if activated.tzinfo else activated.replace(tzinfo=timezone.utc)
    now = _now()
    inserted = 0
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        for order in orders.get("items") or []:
            source_record_id = int(order.get("source_record_id") or 0)
            order_date = _order_event_at(order)
            course_key = _clean(order.get("course_key"), 50)
            candidates = [
                item for item in ready.get(course_key, [])
                if order_date
                and item[0].date() <= order_date.date()
                and (
                    (course_key, _clean(item[1].get("stream"), 50)) not in managed_flows
                    or (
                        activation_times.get((course_key, _clean(item[1].get("stream"), 50)))
                        and order_date >= activation_times[(course_key, _clean(item[1].get("stream"), 50))]
                    )
                )
            ]
            flow = max(candidates, key=lambda item: (item[0], int(item[1]["stream"])))[1] if candidates else None
            gc_user_id = _clean(order.get("gc_user_id"), 100)
            email = _clean(order.get("email"), 300)
            logical_matches: list[aiosqlite.Row] = []
            if gc_user_id:
                logical_matches = await (
                    await db.execute(
                        "SELECT * FROM enrollments WHERE course_key=? AND gc_user_id=? AND status<>'removed' "
                        "ORDER BY created_at,id LIMIT 3",
                        (course_key, gc_user_id),
                    )
                ).fetchall()
            if not logical_matches and email:
                logical_matches = await (
                    await db.execute(
                        "SELECT * FROM enrollments WHERE course_key=? AND lower(email)=lower(?) AND status<>'removed' "
                        "ORDER BY created_at,id LIMIT 3",
                        (course_key, email),
                    )
                ).fetchall()
            existing = logical_matches[0] if len(logical_matches) == 1 else None
            if existing and int(existing["source_record_id"] or 0) != source_record_id:
                try:
                    previous_source = json.loads(existing["source_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    previous_source = {}
                if not isinstance(previous_source, dict):
                    previous_source = {}
                preserved_sheet = {
                    key: previous_source[key]
                    for key in ("row", "sheet_id", "sheet_title", "course_assignments")
                    if key in previous_source
                }
                merged_source = {**previous_source, **order, **preserved_sheet}
                previous_sheet_email = _clean(
                    previous_source.get("sheet_email") or existing["email"], 320
                )
                if int(merged_source.get("row") or 0) and previous_sheet_email:
                    merged_source["sheet_email"] = previous_sheet_email
                existing_stream = _clean(existing["stream"], 50)
                existing_flow = next((
                    item for item in flows
                    if _clean(item.get("course_key"), 50) == course_key
                    and _clean(item.get("stream"), 50) == existing_stream
                ), None)
                target_flow = existing_flow or flow
                stream = existing_stream or _clean((target_flow or {}).get("stream"), 50)
                status = "assigned" if stream else "pending"
                tg_account = _clean(order.get("tg_account") or existing["tg_account"], 500)
                await db.execute(
                    """
                    UPDATE enrollments SET
                        source_record_id=?,order_id=?,deal_number=?,gc_user_id=?,name=?,email=?,tg_account=?,date=?,
                        course_key=?,course=?,stream=?,tariff=?,teacher=?,teacher_code=?,status=?,source_json=?,updated_at=?
                    WHERE id=?
                    """,
                    (
                        source_record_id, _clean(order.get("order_id"), 100),
                        _clean(order.get("deal_number") or order.get("order_id"), 100), gc_user_id,
                        _clean(order.get("name") or existing["name"], 300), email or _clean(existing["email"], 300),
                        tg_account, _clean(order.get("date") or existing["date"], 100), course_key,
                        _clean(order.get("course") or existing["course"], 100), stream,
                        _clean(order.get("tariff") or existing["tariff"], 100),
                        _clean((target_flow or {}).get("teacher") or existing["teacher"], 200),
                        _clean((target_flow or {}).get("teacher_code") or existing["teacher_code"], 100),
                        status, json.dumps(merged_source, ensure_ascii=False), now, existing["id"],
                    ),
                )
                if status == "assigned":
                    await _insert_sheet_operation(
                        db,
                        student={
                            **merged_source,
                            "enrollment_id": existing["id"],
                            "course_key": course_key,
                            "course": _clean(order.get("course") or existing["course"], 100),
                            "stream": stream,
                            "email": email or _clean(existing["email"], 300),
                            "name": _clean(order.get("name") or existing["name"], 300),
                            "tg_account": tg_account,
                            "teacher": _clean((target_flow or {}).get("teacher") or existing["teacher"], 200),
                            "teacher_code": _clean((target_flow or {}).get("teacher_code") or existing["teacher_code"], 100),
                        },
                        action="sheet_row",
                    )
                inserted += 1
                continue
            status = "assigned" if flow else "pending"
            stream = _clean((flow or {}).get("stream"), 50)
            enrollment_id = _enrollment_id(source_record_id, order.get("course_key"), stream, order.get("email"))
            cur = await db.execute(
                """
                INSERT OR IGNORE INTO enrollments(
                    id,source_record_id,order_id,deal_number,gc_user_id,name,email,tg_account,date,
                    course_key,course,stream,tariff,teacher,teacher_code,status,source_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    enrollment_id, source_record_id, _clean(order.get("order_id"), 100),
                    _clean(order.get("deal_number"), 100), _clean(order.get("gc_user_id"), 100),
                    _clean(order.get("name"), 300), _clean(order.get("email"), 300),
                    _clean(order.get("tg_account"), 500), _clean(order.get("date"), 100),
                    _clean(order.get("course_key"), 50), _clean(order.get("course"), 100), stream,
                    _clean(order.get("tariff"), 100), _clean((flow or {}).get("teacher"), 200),
                    _clean((flow or {}).get("teacher_code"), 100), status,
                    json.dumps(order, ensure_ascii=False), now, now,
                ),
            )
            was_inserted = max(0, int(cur.rowcount or 0))
            inserted += was_inserted
            if was_inserted and flow:
                await _insert_sheet_operation(
                    db,
                    student={
                        **order,
                        "enrollment_id": enrollment_id,
                        "course_key": _clean(order.get("course_key"), 50),
                        "course": _clean(order.get("course"), 100),
                        "stream": stream,
                        "teacher": _clean(flow.get("teacher"), 200),
                        "teacher_code": _clean(flow.get("teacher_code"), 100),
                        "row": 0,
                    },
                    action="sheet_row",
                )
        await db.commit()
    return inserted


async def _queue_missing_sheet_rows(limit: int = 5) -> int:
    """Gradually repair assigned students that predate automatic row creation."""
    queued = 0
    async with _operation_queue_lock, _connect() as db:
        rows = await (
            await db.execute(
                """
                SELECT * FROM enrollments e
                WHERE e.status='assigned' AND e.stream<>''
                  AND COALESCE(CAST(json_extract(e.source_json,'$.row') AS INTEGER),0)=0
                  AND NOT EXISTS (
                    SELECT 1 FROM transfers t
                    WHERE t.enrollment_id=e.id AND t.status IN ('queued','running','waiting')
                      AND json_extract(t.steps_json,'$.preview.action')='sheet_row'
                  )
                ORDER BY e.created_at,e.id LIMIT ?
                """,
                (max(1, min(100, int(limit))),),
            )
        ).fetchall()
        for row in rows:
            student = dict(row)
            student["enrollment_id"] = student["id"]
            try:
                source = json.loads(student.get("source_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                source = {}
            if isinstance(source, dict):
                student = {**source, **student}
            await _insert_sheet_operation(db, student=student, action="sheet_row")
            queued += 1
        await db.commit()
    return queued


async def _sync_roster_delta() -> dict[str, Any]:
    """Refresh new orders and refunds without the expensive Google sheet scan."""
    global _last_roster_sync
    now_monotonic = time.monotonic()
    if now_monotonic - _last_roster_sync < 115:
        return {"ok": True, "status": "recent"}
    if _registry_lock.locked():
        return {"ok": True, "status": "full_sync_running"}
    async with _registry_lock:
        if time.monotonic() - _last_roster_sync < 115:
            return {"ok": True, "status": "recent"}
        if await _meta_get("initialized") != "1":
            return {"ok": True, "status": "awaiting_initial_sync"}
        cursor = int(await _meta_get("orders_cursor", "0") or 0)
        cursor_updated_at = await _meta_get("orders_cursor_updated_at")
        if not cursor_updated_at:
            return {"ok": True, "status": "awaiting_initial_sync"}
        fields = _module("getcourse-chat-fields", "service_entitled_orders")
        orders = await fields.service_entitled_orders(
            after_source_record_id=cursor,
            after_updated_at=cursor_updated_at,
            limit=1000,
        )
        if not orders.get("ok"):
            raise RuntimeError(orders.get("error") or "Заказы GetCourse недоступны")
        flows = await _flow_rows()
        added = await _assign_new_orders(orders, flows)
        await _meta_set("orders_cursor", int(orders.get("cursor") or cursor))
        await _meta_set("orders_cursor_updated_at", orders.get("cursor_updated_at") or cursor_updated_at)
        refunds = await _sync_refunds()
        managers_warmed = await _warm_recent_manager_cache(limit=1) if added else 0
        _last_roster_sync = time.monotonic()
        if added or int(refunds.get("moved") or 0):
            _clear_snapshot_cache()
        await _meta_set("last_roster_sync_at", _now())
        return {
            "ok": True,
            "status": "completed",
            "new_enrollments": added,
            "refunds": refunds,
            "managers_warmed": managers_warmed,
        }


async def _sync_registry(*, force: bool = False) -> dict[str, Any]:
    global _last_registry_sync, _registry_retry_at
    now_monotonic = time.monotonic()
    if now_monotonic < _registry_retry_at:
        return {"ok": False, "status": "deferred", "error": "Обновление отложено", "retry_after": max(1, int(_registry_retry_at - now_monotonic))}
    if now_monotonic - _last_registry_sync < (30 if force else 295):
        return {"ok": True, "status": "recent", "updated_at": await _meta_get("last_sync_at")}
    async with _registry_lock:
        now_monotonic = time.monotonic()
        if now_monotonic < _registry_retry_at:
            return {"ok": False, "status": "deferred", "error": "Обновление отложено", "retry_after": max(1, int(_registry_retry_at - now_monotonic))}
        if now_monotonic - _last_registry_sync < (30 if force else 295):
            return {"ok": True, "status": "recent", "updated_at": await _meta_get("last_sync_at")}
        started = _now()
        async with _connect() as db:
            cur = await db.execute(
                "INSERT INTO registry_sync_runs(status,started_at) VALUES('running',?)", (started,)
            )
            await db.commit()
            run_id = int(cur.lastrowid)
        details: dict[str, Any] = {}
        try:
            fields = _module("getcourse-chat-fields", "service_flow_catalog")
            creator = _module("course-chat-creator", "service_flow_catalog")
            link_catalog, creator_catalog = await asyncio.gather(
                fields.service_flow_catalog(), asyncio.to_thread(creator.service_flow_catalog)
            )
            if not link_catalog.get("ok"):
                raise RuntimeError("Таблица ссылок недоступна")
            if not creator_catalog.get("ok"):
                raise RuntimeError("Каталог чатов недоступен")
            previous_flows = {(item["course_key"], item["stream"]): item for item in await _flow_rows()}
            creator_keys = {
                (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50))
                for item in creator_catalog.get("items") or []
                if _teacher_code(item.get("teacher"))
            }
            flows = await _upsert_flows(link_catalog, creator_catalog)
            creator_changed = await _propagate_flow_curator_changes(previous_flows, flows, creator_keys)
            details["flows"] = len(flows)
            initialized = await _meta_get("initialized") == "1"
            mirror_initialized = await _meta_get("mirror_initialized") == "1"
            legacy_layouts: list[dict[str, Any]] = []
            if not initialized or not mirror_initialized:
                legacy = await fields.service_transfer_snapshot(refresh=not initialized)
                legacy_layouts = legacy.get("items") or []
                legacy_count = sum(len(flow.get("students") or []) for flow in legacy.get("items") or [])
                if not legacy.get("ok") or legacy_count <= 0:
                    raise RuntimeError("Исходная таблица клиентов не загрузилась; реестр не изменён")
                details["seeded"] = await _seed_from_legacy(legacy, flows)
                details["legacy_students"] = legacy_count
                if not initialized:
                    cursor_info = await fields.service_entitled_orders(after_source_record_id=0, limit=1)
                    if not cursor_info.get("ok"):
                        raise RuntimeError(cursor_info.get("error") or "Заказы GetCourse недоступны")
                    await _meta_set("orders_cursor", int(cursor_info.get("max_updated_id") or cursor_info.get("max_source_record_id") or 0))
                    await _meta_set("orders_cursor_updated_at", cursor_info.get("max_updated_at") or "")
                    await _meta_set("initialized", "1")
            cursor = int(await _meta_get("orders_cursor", "0") or 0)
            cursor_updated_at = await _meta_get("orders_cursor_updated_at")
            if not cursor_updated_at:
                baseline = await fields.service_entitled_orders(after_source_record_id=0, limit=1)
                if not baseline.get("ok"):
                    raise RuntimeError(baseline.get("error") or "Заказы GetCourse недоступны")
                cursor = int(baseline.get("max_updated_id") or baseline.get("max_source_record_id") or cursor)
                cursor_updated_at = _clean(baseline.get("max_updated_at"), 100)
                await _meta_set("orders_cursor", cursor)
                await _meta_set("orders_cursor_updated_at", cursor_updated_at)
                orders = {"ok": True, "items": [], "cursor": cursor, "cursor_updated_at": cursor_updated_at}
            else:
                orders = await fields.service_entitled_orders(
                    after_source_record_id=cursor, after_updated_at=cursor_updated_at, limit=1000
                )
            if not orders.get("ok"):
                raise RuntimeError(orders.get("error") or "Заказы GetCourse недоступны")
            details["new_enrollments"] = await _assign_new_orders(orders, flows)
            await _meta_set("orders_cursor", int(orders.get("cursor") or cursor))
            await _meta_set("orders_cursor_updated_at", orders.get("cursor_updated_at") or cursor_updated_at)
            sheet_snapshot = await fields.service_registry_sheet_snapshot(flows=flows, known_layouts=legacy_layouts)
            if not sheet_snapshot.get("ok"):
                raise RuntimeError(sheet_snapshot.get("error") or "Исходная таблица уроков недоступна")
            details["sheet_assignments"] = await _reconcile_sheet_assignments(sheet_snapshot, flows)
            details["curator_changes"] = await _apply_sheet_curators(sheet_snapshot, creator_keys, creator_changed)
            details["lesson_changes"] = await _import_sheet_lessons(sheet_snapshot)
            details["refunds"] = await _sync_refunds()
            # Keep legacy repair deliberately bounded: new enrollments are queued
            # immediately above, while older rowless records are healed gradually.
            details["sheet_rows_queued"] = await _queue_missing_sheet_rows(limit=5)
            mirror_payload = await _mirror_payload()
            details["mirror"] = {"ok": True, "paused": True, "reason": "google_is_source"}
            details["getcourse_curators"] = await fields.service_reconcile_registry_curators(flows=mirror_payload)
            await _meta_set("mirror_initialized", "1")
            await _meta_set("last_sync_at", _now())
            _last_registry_sync = time.monotonic()
            _registry_retry_at = 0.0
            _clear_snapshot_cache()
            async with _connect() as db:
                await db.execute(
                    "UPDATE registry_sync_runs SET status='completed',details_json=?,finished_at=? WHERE id=?",
                    (json.dumps(details, ensure_ascii=False), _now(), run_id),
                )
                await db.commit()
            return {"ok": True, "status": "completed", "updated_at": await _meta_get("last_sync_at"), **details}
        except asyncio.CancelledError:
            async with _connect() as db:
                await db.execute(
                    """UPDATE registry_sync_runs
                       SET status='failed',
                           error='Синхронизация была прервана перезапуском модуля; следующий запуск выполняется автоматически.',
                           finished_at=?
                       WHERE id=? AND status='running'""",
                    (_now(), run_id),
                )
                await db.commit()
            raise
        except Exception as exc:
            _registry_retry_at = time.monotonic() + (90 if "429" in str(exc) or "Too Many Requests" in str(exc) else 20)
            async with _connect() as db:
                await db.execute(
                    "UPDATE registry_sync_runs SET status='failed',details_json=?,error=?,finished_at=? WHERE id=?",
                    (json.dumps(details, ensure_ascii=False), _clean(exc, 2000), _now(), run_id),
                )
                await db.commit()
            if _logger:
                _logger.exception("flow registry sync failed")
            return {"ok": False, "status": "failed", "error": _clean(exc, 2000), **details}


async def _registry_snapshot(*, refresh: bool = False) -> dict[str, Any]:
    if refresh or not await _meta_get("initialized"):
        await _sync_registry(force=True)
    flows = await _flow_rows()
    enrollments = await _enrollment_rows()
    by_flow: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in enrollments:
        try:
            source = json.loads(item.get("source_json") or "{}")
        except Exception:
            source = {}
        amo_manager = source.get("amo_manager") if isinstance(source.get("amo_manager"), dict) else {}
        by_flow.setdefault((item["course_key"], item["stream"]), []).append(
            {
                "row": int(source.get("row") or 0),
                "name": item["name"],
                "date": item["date"],
                "course": item["course"],
                "tariff": item["tariff"],
                "responsible_curator": item["teacher_code"],
                "responsible_curator_raw": item["teacher"],
                "tg_account": item["tg_account"],
                "email": item["email"],
                "gc_user_id": item["gc_user_id"],
                "order_id": item["order_id"],
                "deal_number": item["deal_number"],
                "source_record_id": item["source_record_id"],
                "enrollment_id": item["id"],
                "phone": _clean(source.get("phone") or source.get("user_phone"), 100),
                "utm_term": _clean(source.get("utm_term"), 1000),
                "manager_name": _clean(amo_manager.get("name") or source.get("manager_name"), 300),
                "manager_id": _clean(amo_manager.get("id") or source.get("manager_id"), 64),
                "amo_deal_id": _clean(amo_manager.get("deal_id") or source.get("amo_deal_id"), 64),
                "amo_deal_url": _clean(amo_manager.get("deal_url") or source.get("amo_deal_url"), 1000),
                "product_kind": _clean(
                    source.get("product_kind")
                    or (
                        (source.get("entitlement") or {}).get("product_kind")
                        if isinstance(source.get("entitlement"), dict)
                        else ""
                    ),
                    50,
                ),
                "course_assignments": source.get("course_assignments") if isinstance(source.get("course_assignments"), list) else [],
                "lessons": item.get("lessons") or [],
            }
        )
    items = [
            {
                **flow,
                "curator_value": flow["teacher_code"],
                "curator_raw": flow["teacher"],
                "students": by_flow.get((flow["course_key"], flow["stream"]), []),
                "students_count": len(by_flow.get((flow["course_key"], flow["stream"]), [])),
            }
            for flow in flows
        ]
    for course_key, course in (("puppy", "Щенок"), ("dog", "Собака")):
        students = by_flow.get((course_key, ""), [])
        if students:
            items.append({"course_key": course_key, "course": course, "stream": "", "status": "pending", "students": students, "students_count": len(students)})
    return {
        "ok": True,
        "updated_at": await _meta_get("last_sync_at"),
        "items": items,
    }


def _flow_key(flow: dict[str, Any]) -> tuple[str, str]:
    return _clean(flow.get("course_key"), 50), _clean(flow.get("stream"), 50)


def _course_stream_label(course_key: Any, stream: Any) -> str:
    value = re.sub(r"^[ЩщСс]\s*", "", _clean(stream, 50))
    prefix = {"puppy": "Щ", "dog": "С"}.get(_clean(course_key, 50), "")
    return f"{prefix}{value}" if prefix and value else value


def _student_result(flow: dict[str, Any], student: dict[str, Any]) -> dict[str, Any]:
    curator = _clean(student.get("responsible_curator") or flow.get("curator_value"), 100)
    lessons = student.get("lessons") if isinstance(student.get("lessons"), list) else []
    assignments = student.get("course_assignments") if isinstance(student.get("course_assignments"), list) else []
    assignments = [item for item in assignments if isinstance(item, dict) and _clean(item.get("stream"), 50)]
    primary_course = _clean(flow.get("course_key"), 50)
    display_by_course = {
        primary_course: {"course_key": primary_course, "stream": _clean(flow.get("stream"), 50)}
    }
    for item in assignments:
        course_key = _clean(item.get("course_key"), 50)
        if not course_key or course_key in display_by_course:
            continue
        display_by_course[course_key] = item
    display_assignments = sorted(
        display_by_course.values(),
        key=lambda item: ({"puppy": 0, "dog": 1}.get(_clean(item.get("course_key"), 50), 2)),
    )
    stream_display = " / ".join(
        _course_stream_label(item.get("course_key"), item.get("stream"))
        for item in display_assignments if _clean(item.get("stream"), 50)
    )
    assigned_courses = {_clean(item.get("course_key"), 50) for item in assignments}
    product_kind = _clean(student.get("product_kind"), 50)
    refund = student.get("refund") if isinstance(student.get("refund"), dict) else {}
    amo_manager = student.get("amo_manager") if isinstance(student.get("amo_manager"), dict) else {}

    def lesson_order(item: dict[str, Any]) -> int:
        value = 0
        for char in _clean(item.get("key"), 10).upper():
            if "A" <= char <= "Z":
                value = value * 26 + ord(char) - 64
        return value or 1000

    return {
        "enrollment_id": _clean(student.get("enrollment_id"), 100),
        "email": _clean(student.get("email"), 320),
        "name": _clean(student.get("name"), 300),
        "gc_user_id": _clean(student.get("gc_user_id"), 100),
        "order_id": _clean(student.get("order_id"), 100),
        "source_record_id": int(student.get("source_record_id") or 0),
        "deal_number": _clean(student.get("deal_number") or student.get("order_id"), 100),
        "tg_account": _clean(student.get("tg_account"), 300),
        "date": _clean(student.get("date"), 100),
        "row": int(student.get("row") or 0),
        "course_key": _clean(flow.get("course_key"), 50),
        "course": _clean(flow.get("course"), 100),
        "stream": _clean(flow.get("stream"), 50),
        "course_display": "Щенок + Собака" if product_kind == "combo" or assigned_courses == {"puppy", "dog"} else _clean(flow.get("course"), 100),
        "stream_display": stream_display or _course_stream_label(flow.get("course_key"), flow.get("stream")),
        "course_assignments": assignments,
        "tariff": _clean(student.get("tariff"), 100),
        "manager_name": _clean(amo_manager.get("name") or student.get("manager_name"), 300),
        "manager_id": _clean(amo_manager.get("id") or student.get("manager_id"), 64),
        "amo_deal_id": _clean(amo_manager.get("deal_id") or student.get("amo_deal_id"), 64),
        "amo_deal_url": _clean(amo_manager.get("deal_url") or student.get("amo_deal_url"), 1000),
        "phone": _clean(student.get("phone"), 100),
        "utm_term": _clean(student.get("utm_term"), 1000),
        "product_kind": product_kind,
        "total_amount": float(student.get("total_amount") or refund.get("total_amount") or 0),
        "remaining_amount": float(student.get("remaining_amount") or refund.get("remaining_amount") or 0),
        "refund_amount": float(student.get("refund_amount") or refund.get("refund_amount") or 0),
        "curator": curator,
        "curator_name": CURATOR_NAMES.get(curator, ""),
        "sheet_title": _clean(flow.get("sheet_title") or student.get("sheet_title"), 300),
        "user_url": _clean(student.get("user_url"), 1000),
        "order_url": _clean(student.get("order_url"), 1000),
        "student_note": "",
        "student_note_updated_by": "",
        "student_note_updated_at": "",
        "lessons": sorted(lessons, key=lesson_order),
    }


async def _enrich_successful_managers(items: list[dict[str, Any]]) -> bool:
    if not items:
        return True
    try:
        amo = _module("amocrm-db", "service_successful_managers")
        result = await amo.service_successful_managers(
            identities=[
                {
                    "key": item["enrollment_id"],
                    "email": item.get("email", ""),
                    "phone": item.get("phone", ""),
                    "order_id": item.get("order_id", ""),
                    "deal_number": item.get("deal_number", ""),
                }
                for item in items
            ]
        )
    except Exception as exc:
        if _logger:
            _logger.warning("amoCRM manager lookup skipped: %s", exc)
        return False
    matches = {str(item.get("key") or ""): item for item in result.get("items") or []}
    for item in items:
        match = matches.get(item["enrollment_id"]) or {}
        item["manager_name"] = _clean(match.get("manager_name") or item.get("manager_name"), 300)
        item["manager_id"] = _clean(match.get("manager_id") or item.get("manager_id"), 64)
        item["amo_deal_id"] = _clean(match.get("deal_id") or item.get("amo_deal_id"), 64)
        item["amo_deal_url"] = _clean(match.get("deal_url") or item.get("amo_deal_url"), 1000)
    return True


async def _enrich_live_amo_manager(item: dict[str, Any]) -> bool:
    """Resolve the current amoCRM lead only for an explicitly opened card."""
    if item.get("amo_deal_id"):
        return True
    try:
        amo = _module("getcourse-amocrm", "service_resolve_onboarding_manager")
        utm_term = _clean(item.get("utm_term"), 1000)
        if utm_term:
            # The order UTM is the strongest and cheapest exact key.  Looking
            # up phone and email first can consume several full amoCRM request
            # timeouts before the known deal is checked.
            result = await amo.service_resolve_onboarding_manager(
                phone="", email="", utm_term=utm_term,
            )
        else:
            result = {"ok": True, "found": False}
        if result.get("ok") and not result.get("found"):
            result = await amo.service_resolve_onboarding_manager(
                phone=_clean(item.get("phone"), 100),
                email=_clean(item.get("email"), 320),
                utm_term="",
            )
    except Exception as exc:
        if _logger:
            _logger.warning("Live amoCRM manager lookup skipped: %s", exc)
        return False
    if not result.get("ok") or not result.get("found"):
        return bool(result.get("ok"))
    item["manager_name"] = _clean(result.get("manager_name"), 300)
    item["manager_id"] = _clean(result.get("manager_user_id"), 64)
    item["amo_deal_id"] = _clean(result.get("deal_id"), 64)
    item["amo_deal_url"] = _clean(result.get("deal_url"), 1000)
    item["amo_match_source"] = _clean(result.get("source"), 50)
    return True


async def _persist_manager_enrichment(item: dict[str, Any], *, checked: bool = True) -> bool:
    """Keep a verified manager/deal in the local roster for subsequent fast list reads."""
    enrollment_id = _clean(item.get("enrollment_id"), 100)
    if not enrollment_id:
        return False
    fields = {
        "name": _clean(item.get("manager_name"), 300),
        "id": _clean(item.get("manager_id"), 64),
        "deal_id": _clean(item.get("amo_deal_id"), 64),
        "deal_url": _clean(item.get("amo_deal_url"), 1000),
        "match_source": _clean(item.get("amo_match_source"), 50),
    }
    changed = False
    async with _connect() as db:
        row = await (
            await db.execute("SELECT source_json FROM enrollments WHERE id=?", (enrollment_id,))
        ).fetchone()
        if not row:
            return False
        try:
            source = json.loads(row[0] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        manager = source.get("amo_manager") if isinstance(source.get("amo_manager"), dict) else {}
        for key, value in fields.items():
            if value and _clean(manager.get(key), 1000) != value:
                manager[key] = value
                changed = True
        if fields["name"] or fields["deal_id"]:
            resolved_at = _now()
            manager["resolved_at"] = resolved_at
            source["amo_manager"] = manager
            changed = True
        if checked:
            checked_at = _now()
            lookup = source.get("amo_manager_lookup") if isinstance(source.get("amo_manager_lookup"), dict) else {}
            if lookup.get("checked_at") != checked_at:
                lookup["checked_at"] = checked_at
                source["amo_manager_lookup"] = lookup
                changed = True
        if changed:
            await db.execute(
                "UPDATE enrollments SET source_json=? WHERE id=?",
                (json.dumps(source, ensure_ascii=False), enrollment_id),
            )
            await db.commit()
    if changed:
        _student_enrichment_caches["manager"].clear()
        _clear_snapshot_cache()
    return changed


async def _warm_recent_manager_cache(limit: int = 1) -> int:
    """Resolve a few recent orders in the background without slowing list requests."""
    count = 1
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                """
                SELECT * FROM enrollments
                WHERE status IN ('assigned','pending')
                  AND julianday(created_at)>=julianday('now','-7 days')
                  AND COALESCE(json_extract(source_json,'$.amo_manager.name'),json_extract(source_json,'$.manager_name'),'')=''
                  AND (
                    julianday(json_extract(source_json,'$.amo_manager_lookup.checked_at')) IS NULL
                    OR julianday(json_extract(source_json,'$.amo_manager_lookup.checked_at'))<julianday('now','-15 minutes')
                  )
                ORDER BY created_at DESC,id DESC LIMIT ?
                """,
                (count,),
            )
        ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        stored = dict(row)
        try:
            source = json.loads(stored.get("source_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        items.append(_student_result(
            {
                "course_key": stored.get("course_key"),
                "course": stored.get("course"),
                "stream": stored.get("stream"),
                "curator_value": stored.get("teacher_code"),
                "teacher": stored.get("teacher"),
            },
            {
                **source,
                **stored,
                "enrollment_id": stored.get("id"),
                "phone": _clean(source.get("phone") or source.get("user_phone"), 100),
            },
        ))
    if not items:
        return 0
    await _enrich_order_identities(items)

    async def resolve(item: dict[str, Any]) -> bool:
        try:
            await asyncio.wait_for(
                _enrich_live_amo_manager(item),
                timeout=STUDENT_CARD_EXTERNAL_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            if _logger:
                _logger.warning(
                    "Background amoCRM lookup timed out enrollment_id=%s",
                    _clean(item.get("enrollment_id"), 100),
                )
        await _persist_manager_enrichment(item, checked=True)
        return bool(item.get("manager_name") or item.get("amo_deal_id"))

    results = await asyncio.gather(*(resolve(item) for item in items), return_exceptions=True)
    return sum(result is True for result in results)


async def _enrich_opened_student_external(item: dict[str, Any]) -> None:
    """Load independent card links in parallel without blocking the whole UI."""

    async def live_manager() -> bool:
        try:
            return await asyncio.wait_for(
                _enrich_live_amo_manager(item),
                timeout=STUDENT_CARD_EXTERNAL_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            if _logger:
                _logger.warning(
                    "Opened student amoCRM lookup timed out enrollment_id=%s",
                    _clean(item.get("enrollment_id"), 100),
                )
            return False

    async def profile_link() -> str:
        try:
            return await asyncio.wait_for(
                _resolve_student_profile_link(item),
                timeout=STUDENT_CARD_EXTERNAL_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            if _logger:
                _logger.warning(
                    "Opened student profile lookup timed out enrollment_id=%s",
                    _clean(item.get("enrollment_id"), 100),
                )
            return ""

    manager_ok, profile_url, _ = await asyncio.gather(
        live_manager(),
        profile_link(),
        _enrich_student_notes([item]),
    )
    item["social_profile_url"] = profile_url
    if manager_ok and item.get("amo_deal_id"):
        await _persist_manager_enrichment(item)
        return
    try:
        await asyncio.wait_for(
            _enrich_successful_managers([item]),
            timeout=STUDENT_CARD_MANAGER_FALLBACK_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        if _logger:
            _logger.warning(
                "Opened student local manager fallback timed out enrollment_id=%s",
                _clean(item.get("enrollment_id"), 100),
            )
    if item.get("manager_name") or item.get("amo_deal_id"):
        await _persist_manager_enrichment(item)


async def _enrich_student_notes(items: list[dict[str, Any]]) -> None:
    ids = [_clean(item.get("enrollment_id"), 100) for item in items]
    ids = [value for value in dict.fromkeys(ids) if value]
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    async with _connect() as db:
        rows = await (
            await db.execute(
                f"SELECT enrollment_id,note,updated_by,updated_at FROM student_notes WHERE enrollment_id IN ({placeholders})",
                ids,
            )
        ).fetchall()
    notes = {str(row[0]): row for row in rows}
    for item in items:
        row = notes.get(_clean(item.get("enrollment_id"), 100))
        item["student_note"] = _clean(row[1], 2000) if row else ""
        item["student_note_updated_by"] = _clean(row[2], 200) if row else ""
        item["student_note_updated_at"] = _clean(row[3], 100) if row else ""


async def _enrich_student_financials(items: list[dict[str, Any]]) -> None:
    source_ids = [int(item.get("source_record_id") or 0) for item in items]
    source_ids = [value for value in dict.fromkeys(source_ids) if value > 0]
    if not source_ids:
        return
    try:
        fields = _module("getcourse-chat-fields", "service_order_financials")
        result = await fields.service_order_financials(source_record_ids=source_ids)
    except Exception as exc:
        if _logger:
            _logger.warning("GetCourse financial lookup skipped: %s", exc)
        return
    by_id = {int(row.get("source_record_id") or 0): row for row in result.get("items") or []}
    for item in items:
        # Refund rows already carry the ledger-classified remainder. Some
        # legacy GetCourse callbacks report payed_money above cost_money;
        # never let that current snapshot overwrite the verified refund event.
        if item.get("refunded") and float(item.get("total_amount") or 0) > 0:
            continue
        match = by_id.get(int(item.get("source_record_id") or 0)) or {}
        for key in ("total_amount", "remaining_amount", "refund_amount"):
            if match.get(key) is not None:
                item[key] = float(match.get(key) or 0)
        item["payment_state"] = _clean(match.get("payment_state") or item.get("payment_state"), 50)


async def _enrich_order_identities(items: list[dict[str, Any]]) -> bool:
    if not items:
        return True
    try:
        fields = _module("getcourse-chat-fields", "service_order_identities")
        matches: dict[str, dict[str, Any]] = {}
        for offset in range(0, len(items), 250):
            result = await fields.service_order_identities(
                identities=[
                    {
                        "key": item["enrollment_id"],
                        "source_record_id": item.get("source_record_id", 0),
                        "order_id": item.get("order_id", ""),
                        "gc_user_id": item.get("gc_user_id", ""),
                        "email": item.get("email", ""),
                    }
                    for item in items[offset : offset + 250]
                ]
            )
            matches.update({str(item.get("key") or ""): item for item in result.get("items") or []})
    except Exception as exc:
        if _logger:
            _logger.warning("GetCourse identity lookup skipped: %s", exc)
        return False
    for item in items:
        match = matches.get(item["enrollment_id"]) or {}
        item["phone"] = _clean(item.get("phone") or match.get("phone"), 100)
        item["tariff"] = _clean(item.get("tariff") or match.get("tariff"), 100)
        item["utm_term"] = _clean(item.get("utm_term") or match.get("utm_term"), 1000)
        item["product_kind"] = _clean(item.get("product_kind") or match.get("product_kind"), 50)
        if item["product_kind"] == "combo":
            item["course_display"] = "Щенок + Собака"
        assignment = match.get("assignment")
        if isinstance(assignment, dict):
            item["getcourse_assignment"] = assignment
    return True


def _student_enrichment_fingerprint(provider: str, item: dict[str, Any]) -> str:
    common = [
        _clean(item.get("enrollment_id"), 100),
        _clean(item.get("email"), 320).casefold(),
        _clean(item.get("order_id"), 100),
    ]
    if provider == "identity":
        common.extend([
            _clean(item.get("phone"), 100),
            int(item.get("source_record_id") or 0),
            _clean(item.get("gc_user_id"), 100),
            _clean(item.get("tariff"), 100),
        ])
    else:
        common.append(_clean(item.get("deal_number"), 100))
    raw = json.dumps(common, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _student_enrichment_overlay(provider: str, item: dict[str, Any]) -> dict[str, Any]:
    fields = (
        ("phone", "tariff", "utm_term", "product_kind", "getcourse_assignment")
        if provider == "identity"
        else ("manager_name", "manager_id", "amo_deal_id", "amo_deal_url")
    )
    return {
        field: (dict(item[field]) if field == "getcourse_assignment" and isinstance(item.get(field), dict) else item.get(field, ""))
        for field in fields
    }


def _student_enrichment_has_signal(provider: str, overlay: dict[str, Any]) -> bool:
    if provider == "manager":
        return bool(overlay.get("manager_name") or overlay.get("manager_id") or overlay.get("amo_deal_id"))
    return bool(
        overlay.get("phone")
        or overlay.get("utm_term")
        or overlay.get("product_kind")
        or overlay.get("getcourse_assignment")
    )


def _prune_student_enrichment_cache(provider: str) -> None:
    cache = _student_enrichment_caches[provider]
    if len(cache) <= STUDENT_ENRICHMENT_CACHE_LIMIT:
        return
    excess = len(cache) - STUDENT_ENRICHMENT_CACHE_LIMIT
    oldest = sorted(cache, key=lambda key: float(cache[key].get("stored_at") or 0.0))[:excess]
    for key in oldest:
        cache.pop(key, None)


def _apply_student_enrichment_cache(
    provider: str, items: list[dict[str, Any]], fingerprints: list[str] | None = None,
) -> tuple[bool, int]:
    now = time.monotonic()
    fresh = True
    available = 0
    cache = _student_enrichment_caches[provider]
    keys = fingerprints or [_student_enrichment_fingerprint(provider, item) for item in items]
    for fingerprint, item in zip(keys, items):
        entry = cache.get(fingerprint)
        if not entry or now >= float(entry.get("stale_until") or 0.0):
            fresh = False
            continue
        overlay = entry.get("data") if isinstance(entry.get("data"), dict) else {}
        for field, value in overlay.items():
            if provider == "manager" and not value and item.get(field):
                # A short-lived negative cache entry must never erase a manager
                # that an exact card lookup has already persisted.
                continue
            item[field] = dict(value) if field == "getcourse_assignment" and isinstance(value, dict) else value
        available += 1
        if now >= float(entry.get("expires_at") or 0.0):
            fresh = False
    return fresh, available


async def _refresh_student_enrichment(
    provider: str, items: list[dict[str, Any]], fingerprints: list[str],
) -> bool:
    async with _student_enrichment_locks[provider]:
        now = time.monotonic()
        cache = _student_enrichment_caches[provider]
        missing = [
            (fingerprint, dict(item))
            for fingerprint, item in zip(fingerprints, items)
            if now >= float((cache.get(fingerprint) or {}).get("expires_at") or 0.0)
        ]
        if not missing:
            return True
        stored_at = time.monotonic()
        unresolved: list[tuple[str, dict[str, Any]]] = []
        for fingerprint, item in missing:
            overlay = _student_enrichment_overlay(provider, item)
            if _student_enrichment_has_signal(provider, overlay):
                cache[fingerprint] = {
                    "data": overlay,
                    "stored_at": stored_at,
                    "expires_at": stored_at + STUDENT_ENRICHMENT_TTL_SECONDS,
                    "stale_until": stored_at + STUDENT_ENRICHMENT_STALE_SECONDS,
                }
            else:
                unresolved.append((fingerprint, item))
        if not unresolved:
            _prune_student_enrichment_cache(provider)
            return True
        working = [item for _fingerprint, item in unresolved]
        ok = (
            await _enrich_order_identities(working)
            if provider == "identity"
            else await _enrich_successful_managers(working)
        )
        if not ok:
            return False
        stored_at = time.monotonic()
        for fingerprint, item in unresolved:
            overlay = _student_enrichment_overlay(provider, item)
            ttl = (
                STUDENT_ENRICHMENT_TTL_SECONDS
                if _student_enrichment_has_signal(provider, overlay)
                else STUDENT_ENRICHMENT_NEGATIVE_TTL_SECONDS
            )
            cache[fingerprint] = {
                "data": overlay,
                "stored_at": stored_at,
                "expires_at": stored_at + ttl,
                "stale_until": stored_at + STUDENT_ENRICHMENT_STALE_SECONDS,
            }
        _prune_student_enrichment_cache(provider)
        return True


def _schedule_student_enrichment(
    provider: str, items: list[dict[str, Any]], fingerprints: list[str],
) -> asyncio.Task:
    job_key = provider + ":" + hashlib.sha256("|".join(sorted(fingerprints)).encode("ascii")).hexdigest()
    task = _student_enrichment_tasks.get(job_key)
    if task and not task.done():
        return task
    task = asyncio.create_task(
        _refresh_student_enrichment(provider, [dict(item) for item in items], list(fingerprints)),
        name=f"student-transfer-{provider}-enrichment",
    )
    _student_enrichment_tasks[job_key] = task

    def release(completed: asyncio.Task) -> None:
        if _student_enrichment_tasks.get(job_key) is completed:
            _student_enrichment_tasks.pop(job_key, None)

    task.add_done_callback(release)
    return task


async def _enrich_student_page(items: list[dict[str, Any]]) -> dict[str, bool]:
    if not items:
        return {"pending": False, "incomplete": False}
    jobs: list[asyncio.Task] = []
    wait_for_cold = False
    fingerprints = {
        provider: [_student_enrichment_fingerprint(provider, item) for item in items]
        for provider in ("identity", "manager")
    }
    for provider in ("identity", "manager"):
        fresh, available = _apply_student_enrichment_cache(provider, items, fingerprints[provider])
        if fresh:
            continue
        jobs.append(_schedule_student_enrichment(provider, items, fingerprints[provider]))
        wait_for_cold = wait_for_cold or available == 0
    if jobs and wait_for_cold:
        combined = asyncio.gather(*jobs, return_exceptions=True)
        try:
            await asyncio.wait_for(asyncio.shield(combined), timeout=STUDENT_ENRICHMENT_WAIT_SECONDS)
        except asyncio.TimeoutError:
            pass
    states = [
        _apply_student_enrichment_cache(provider, items, fingerprints[provider])[0]
        for provider in ("identity", "manager")
    ]
    pending = any(not task.done() for task in jobs)
    return {"pending": pending, "incomplete": not all(states) and not pending}


def _clear_student_enrichment_cache() -> None:
    for cache in _student_enrichment_caches.values():
        cache.clear()


def _direct_profile_url(value: Any) -> str:
    """Keep only public VK or Telegram profile URLs suitable for the sheet."""
    raw = _clean(value, 500)
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return ""
    host = (parsed.hostname or "").casefold()
    if parsed.scheme != "https" or not parsed.path:
        return ""
    path = parsed.path
    if host in {"t.me", "www.t.me"}:
        # ``t.me/+...`` is an invite/phone deep link, not a stable public
        # profile page.  It must not outrank an identity verified by UTM.
        return raw if re.fullmatch(r"/[A-Za-z0-9_]{5,32}/?", path) else ""
    if host in {"vk.com", "www.vk.com", "m.vk.com", "vk.ru", "www.vk.ru", "m.vk.ru"}:
        return raw if re.fullmatch(r"/(?:id\d{3,20}|[A-Za-z][A-Za-z0-9_.]{2,63})/?", path) else ""
    return ""


def _telegram_profile_url(username: Any) -> str:
    handle = _clean(username, 200).lstrip("@")
    return f"https://t.me/{handle}" if re.fullmatch(r"[A-Za-z0-9_]{5,32}", handle) else ""


def _vk_profile_url(platform_id: Any) -> str:
    value = _clean(platform_id, 100)
    return f"https://vk.com/id{value}" if re.fullmatch(r"\d{3,20}", value) else ""


def _salebot_profile_url(platform_id: Any) -> str:
    value = _clean(platform_id, 100)
    return f"https://salebot.pro/projects/397724/clients/{value}" if re.fullmatch(r"\d{3,24}", value) else ""


async def _resolve_student_profile_link(student: dict[str, Any]) -> str:
    """Resolve one trustworthy public account URL for the combined TG/VK sheet column."""
    if current := _direct_profile_url(student.get("tg_account")):
        return current
    if utm_term := _clean(student.get("utm_term"), 1000):
        try:
            messenger = _module("messenger-widget", "service_transfer_delivery_target")
            target = await messenger.service_transfer_delivery_target(
                email=_clean(student.get("email"), 320),
                gc_user_id=_clean(student.get("gc_user_id"), 100),
                phone=_clean(student.get("phone"), 100),
                utm_term=utm_term,
            )
            if target.get("provider") == "vk":
                if profile := _vk_profile_url(target.get("recipient_id")):
                    return profile
            if target.get("provider") == "salebot":
                if profile := _salebot_profile_url(target.get("recipient_id")):
                    return profile
        except Exception as exc:
            if _logger:
                _logger.warning("Student delivery profile resolution skipped: %s", exc)
    try:
        messenger = _module("messenger-widget", "service_transfer_recipients")
        recipient = await messenger.service_transfer_recipients(
            email=_clean(student.get("email"), 320),
            gc_user_id=_clean(student.get("gc_user_id"), 100),
            name=_clean(student.get("name"), 300),
            phone=_clean(student.get("phone"), 100),
        )
    except Exception as exc:
        if _logger:
            _logger.warning("Student profile resolution skipped: %s", exc)
        return ""
    return (
        _telegram_profile_url(recipient.get("telegram_username"))
        or _vk_profile_url(recipient.get("vk"))
        or _salebot_profile_url(recipient.get("salebot"))
    )


async def _chat_delivery_view(item: dict[str, Any], *, resolve_target: bool = True) -> dict[str, Any]:
    snapshot = await _snapshot()
    catalog = {_flow_key(value): value for value in snapshot.get("items") or []}
    assignments = [value for value in item.get("course_assignments") or [] if isinstance(value, dict)]
    if not assignments:
        assignments = [{"course_key": item.get("course_key"), "stream": item.get("stream")}]
    flow_links: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for assignment in assignments:
        key = (_clean(assignment.get("course_key"), 50), _clean(assignment.get("stream"), 50))
        if not all(key) or key in seen:
            continue
        seen.add(key)
        flow = catalog.get(key) or {}
        flow_links.append({
            "course_key": key[0],
            "course": _clean(flow.get("course") or ("Щенок" if key[0] == "puppy" else "Собака"), 100),
            "stream": key[1],
            "vk": _clean(flow.get("vk_link"), 2000),
            "telegram": _clean(flow.get("tg_link"), 2000),
        })
    links = {
        "vk": _clean((flow_links[-1] if flow_links else {}).get("vk"), 2000),
        "telegram": _clean((flow_links[-1] if flow_links else {}).get("telegram"), 2000),
    }
    tariff = _tariff_key(item.get("tariff"))
    reason = ""
    if tariff not in {"premium", "vip"}:
        reason = "Чаты доступны только тарифам Премиум и ВИП"
    elif not flow_links or any(
        not all(re.match(r"^https?://", flow.get(key, ""), re.I) for key in ("vk", "telegram"))
        for flow in flow_links
    ):
        reason = "Для потока не сохранена пара ссылок"
    target: dict[str, Any] = {}
    if resolve_target and not reason:
        messenger = _module("messenger-widget", "service_transfer_delivery_target")
        target = await messenger.service_transfer_delivery_target(
            email=_clean(item.get("email"), 320), gc_user_id=_clean(item.get("gc_user_id"), 100),
            phone=_clean(item.get("phone"), 100), utm_term=_clean(item.get("utm_term"), 1000),
        )
        if not target.get("ok"):
            reason = _clean(target.get("reason") or "Доставка недоступна", 500)
    course = "Щ+С" if item.get("product_kind") == "combo" else _clean(item.get("course"), 100)
    channel = (
        "VK" if target.get("provider") == "vk"
        else "TG" if target.get("provider") == "salebot"
        else "подходящий канал" if not resolve_target and not reason else ""
    )
    content_lines = [f"Ссылки на учебные чаты курса «{course}»:"]
    for flow in flow_links:
        content_lines.extend((
            "",
            f"{flow['course']} · поток {flow['stream']}",
            f"ВКонтакте: {flow['vk']}",
            f"Telegram: {flow['telegram']}",
        ))
    content = "\n".join(content_lines)
    return {
        "ok": not reason, "can_send": not reason, "reason": reason,
        "email": _clean(item.get("email"), 320), "phone": _clean(item.get("phone"), 100),
        "course": course, "stream": _clean(item.get("stream"), 50),
        "tariff": "ВИП" if tariff == "vip" else "Премиум" if tariff == "premium" else _clean(item.get("tariff"), 100),
        "channel": channel, "provider": _clean(target.get("provider"), 40),
        "recipient_id": _clean(target.get("recipient_id"), 200), "links": links,
        "flow_links": flow_links, "content": content,
    }


async def _access_identity(enrollment_id: str) -> dict[str, Any]:
    key = _clean(enrollment_id, 100)
    if key.startswith("gc:"):
        gc_user_id = _clean(key.split(":", 1)[1], 100)
        if not gc_user_id.isdigit():
            raise HTTPException(400, "Некорректный GetCourse ID")
        resolver = _module("chat-moderators", "service_resolve_access_user")
        prospect = await asyncio.to_thread(
            resolver.service_resolve_access_user, gc_user_id=gc_user_id,
        )
        if not prospect.get("found"):
            remembered = await _widget_prospect_identity(gc_user_id)
            if remembered:
                prospect = {
                    "found": True, "gc_user_id": gc_user_id,
                    "email": remembered.get("email"), "phone": remembered.get("phone"),
                    "full_name": remembered.get("name"),
                }
        if not prospect.get("found"):
            raise HTTPException(404, "Пользователь GetCourse не найден")
        return {
            "id": key,
            "gc_user_id": gc_user_id,
            "email": _clean(prospect.get("email"), 320),
            "phone": _clean(prospect.get("phone"), 100),
            "name": _clean(prospect.get("full_name"), 300),
            "course_key": "",
            "tariff": "",
            "payment_state": "prospect",
            "prospect": True,
        }
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT id,name,email,gc_user_id,course_key,tariff,source_json FROM enrollments "
                "WHERE id=? AND status<>'removed' LIMIT 1",
                (key,),
            )
        ).fetchone()
    if not row:
        raise HTTPException(404, "Ученик не найден")
    item = dict(row)
    try:
        source = json.loads(item.get("source_json") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        source = {}
    item["payment_state"] = _clean(source.get("payment_state"), 40) if isinstance(source, dict) else ""
    item["phone"] = _clean(source.get("phone") or source.get("user_phone"), 100) if isinstance(source, dict) else ""
    if not _clean(item.get("gc_user_id"), 100) and not _clean(item.get("email"), 300):
        raise HTTPException(400, "GetCourse ID и email не найдены")
    return item


def _access_view(
    snapshot: dict[str, Any], catalog: list[dict[str, Any]], identity: dict[str, Any] | None = None
) -> dict[str, Any]:
    by_id = {str(item.get("group_id") or ""): dict(item) for item in catalog}
    current: list[dict[str, Any]] = []
    for raw in snapshot.get("groups") or []:
        group_id = str(raw.get("group_id") or "")
        current.append({**by_id.get(group_id, {}), **dict(raw), "group_id": group_id})
    current_ids = {item["group_id"] for item in current}
    managed = [
        {**item, "group_id": str(item.get("group_id") or ""), "enabled": str(item.get("group_id") or "") in current_ids}
        for item in catalog
        if item.get("managed") and item.get("course_key") in {"puppy", "dog", "mini_muzzle", "mini_leash", "mini_obedience", "mini_15"} and item.get("group_kind") != "bridge"
        and not (item.get("course_key") == "puppy" and item.get("package_key") == "module_standard")
    ]
    identity = identity or {}
    tariff_key = _tariff_key(identity.get("tariff"))
    course_key = _clean(identity.get("course_key"), 50)
    has_partial_marker = any(
        item.get("course_key") == course_key
        and "частичн" in _norm(item.get("name"))
        and "оплат" in _norm(item.get("name"))
        for item in current
    )
    actual_package = any(
        item.get("course_key") == course_key and item.get("group_kind") == "package"
        for item in current
    )
    if has_partial_marker and not actual_package and tariff_key in {"standard", "premium", "vip"}:
        for item in managed:
            if (
                item.get("course_key") == course_key
                and item.get("group_kind") == "package"
                and item.get("package_key") == tariff_key
            ):
                item["enabled"] = True
                item["inferred"] = True
                item["inferred_reason"] = "partial_payment"
                break
    return {
        "ok": bool(snapshot.get("ok")),
        "source": snapshot.get("source") or "cache",
        "updated_at": snapshot.get("updated_at") or "",
        "items": managed,
        "current_groups": current,
        "other_count": sum(1 for item in current if not item.get("managed")),
        "requests_left_2h": snapshot.get("requests_left_2h"),
        "next_at": snapshot.get("next_at") or "",
        "error": snapshot.get("error") or "",
        "warning": snapshot.get("warning") or "",
        "stale": bool(snapshot.get("stale")),
        "refresh_due": bool(snapshot.get("refresh_due")),
    }


async def _load_browser_access_snapshot(
    gc_user_id: str, email: str = "", *, allow_stale: bool = True
) -> dict[str, Any] | None:
    user_id = _clean(gc_user_id, 100)
    if not user_id.isdigit():
        return None
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT snapshot_json,updated_at FROM access_browser_snapshots WHERE gc_user_id=?",
                (user_id,),
            )
        ).fetchone()
    if not row or not _clean(row["snapshot_json"], 100_000):
        return None
    try:
        snapshot = json.loads(row["snapshot_json"])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(snapshot, dict) or not snapshot.get("ok"):
        return None
    age = _iso_age_seconds(row["updated_at"])
    if age > 72 * 3600 and not allow_stale:
        return None
    snapshot["source"] = "browser-cache"
    snapshot["updated_at"] = _clean(row["updated_at"], 40)
    snapshot["stale"] = age > 6 * 3600
    snapshot["refresh_due"] = age > 6 * 3600
    if email and not snapshot.get("email"):
        snapshot["email"] = _clean(email, 320)
    return snapshot


async def _save_browser_access_snapshot(identity: dict[str, Any], snapshot: dict[str, Any]) -> None:
    user_id = _clean(snapshot.get("gc_user_id") or identity.get("gc_user_id"), 100)
    if not user_id.isdigit() or not snapshot.get("ok"):
        return
    now = _clean(snapshot.get("updated_at"), 40) or _now()
    clean_snapshot = {
        "ok": True,
        "gc_user_id": user_id,
        "email": _clean(snapshot.get("email") or identity.get("email"), 320),
        "groups": [
            {
                "group_id": _clean(item.get("group_id"), 30),
                "name": _clean(item.get("name"), 500),
            }
            for item in snapshot.get("groups") or []
            if _clean(item.get("group_id"), 30).isdigit()
        ],
        "source": "browser-cache",
        "updated_at": now,
    }
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO access_browser_snapshots(
                gc_user_id,email,snapshot_json,updated_at,last_attempt_at,next_attempt_at,failures,last_error
            ) VALUES(?,?,?,?,?,'',0,'')
            ON CONFLICT(gc_user_id) DO UPDATE SET
                email=excluded.email,snapshot_json=excluded.snapshot_json,updated_at=excluded.updated_at,
                last_attempt_at=excluded.last_attempt_at,next_attempt_at='',failures=0,last_error=''
            """,
            (user_id, clean_snapshot["email"], json.dumps(clean_snapshot, ensure_ascii=False), now, _now()),
        )
        await db.commit()


async def _get_access_view(
    identity: dict[str, Any], *, live: bool, force: bool = False, allow_stale: bool = False
) -> dict[str, Any]:
    access = _module("chat-moderators", "service_access_catalog")
    catalog_result = await asyncio.to_thread(access.service_access_catalog)
    catalog = catalog_result.get("items") or []
    snapshot: dict[str, Any] = {}
    if live and _clean(identity.get("gc_user_id"), 100).isdigit():
        try:
            browser = _module("getcourse-onboarding", "service_getcourse_browser_access_snapshot")
            snapshot = await browser.service_getcourse_browser_access_snapshot(
                gc_user_id=_clean(identity.get("gc_user_id"), 100),
            )
            if snapshot.get("ok"):
                await _save_browser_access_snapshot(identity, snapshot)
        except Exception as exc:
            snapshot = {"ok": False, "error": _clean(exc, 1000), "groups": []}
    if not snapshot.get("ok"):
        snapshot = await _load_browser_access_snapshot(
            _clean(identity.get("gc_user_id"), 100),
            _clean(identity.get("email"), 320),
        ) or snapshot
    fields = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
    if not snapshot.get("ok"):
        snapshot = await fields.service_getcourse_access_snapshot(
            gc_user_id=_clean(identity.get("gc_user_id"), 100),
            email=_clean(identity.get("email"), 300),
            live=live,
            force=force,
        )
    if (
        not live
        and not allow_stale
        and snapshot.get("source") != "browser-cache"
        and (not snapshot.get("ok") or snapshot.get("refresh_due"))
    ):
        snapshot = await fields.service_getcourse_access_snapshot(
            gc_user_id=_clean(identity.get("gc_user_id"), 100),
            email=_clean(identity.get("email"), 300),
            live=True,
        )
    live_names = {str(item.get("group_id") or ""): _clean(item.get("name"), 500) for item in snapshot.get("catalog") or []}
    for item in catalog:
        if live_names.get(str(item.get("group_id") or "")):
            item["name"] = live_names[str(item.get("group_id") or "")]
    for item in snapshot.get("groups") or []:
        if not item.get("name"):
            known = next((value for value in catalog if str(value.get("group_id")) == str(item.get("group_id"))), {})
            item["name"] = _clean(known.get("name"), 500)
    return _access_view(snapshot, catalog, identity)


async def _get_access_after_write(identity: dict[str, Any]) -> dict[str, Any]:
    return await _get_access_view(identity, live=True, force=True)


def _access_verification_delay(budget: dict[str, Any]) -> int:
    if int(budget.get("requests_left_2h") or 0) >= int(budget.get("needed_for_verification") or 6):
        return ACCESS_VERIFY_DELAY_SECONDS
    raw = _clean(budget.get("next_at"), 40)
    if raw:
        try:
            due = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if due.tzinfo is None:
                due = due.replace(tzinfo=timezone.utc)
            return max(60, min(6 * 3600, int(due.timestamp() - time.time()) + 30))
        except ValueError:
            pass
    return 15 * 60


def _access_target_view(
    current: dict[str, Any],
    target_groups: list[dict[str, Any]],
    next_check_at: str,
    *,
    ready_by: str = "",
    stage: str = "verifying",
) -> dict[str, Any]:
    target_ids = {str(item.get("group_id") or "") for item in target_groups}
    target_packages = {
        _clean(item.get("course_key"), 50)
        for item in target_groups
        if item.get("group_kind") == "package"
    }
    return {
        **current,
        "ok": True,
        "source": "pending",
        "items": [
            {
                **item,
                "enabled": (
                    str(item.get("group_id") or "") in target_ids
                    or bool(item.get("inferred") and item.get("course_key") not in target_packages)
                ),
            }
            for item in current.get("items") or []
        ],
        "current_groups": target_groups,
        "pending": True,
        "pending_stage": stage,
        "next_check_at": next_check_at,
        "ready_by": ready_by,
        "warning": "",
        "stale": False,
    }


def _access_refresh_key(enrollment_id: str) -> str:
    return f"access_refresh:{_clean(enrollment_id, 100)}"


def _access_due_epoch(value: Any, default_delay: int = 5) -> float:
    raw = _clean(value, 60)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(time.time() + 3, parsed.timestamp())
        except ValueError:
            pass
    return time.time() + default_delay


async def _queue_access_refresh(enrollment_id: str, current: dict[str, Any]) -> dict[str, Any]:
    due = _access_due_epoch(current.get("next_at") or current.get("next_check_at"), 5)
    key = _access_refresh_key(enrollment_id)
    previous_raw = await _meta_get(key)
    try:
        previous = json.loads(previous_raw) if previous_raw else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        previous = {}
    if float(previous.get("next_at") or 0) > time.time():
        due = float(previous["next_at"])
    await _meta_set(key, json.dumps({
        "enrollment_id": _clean(enrollment_id, 100),
        "next_at": due,
        "attempts": int(previous.get("attempts") or 0),
        "error": _clean(current.get("error") or current.get("warning"), 500),
    }, ensure_ascii=False))
    ready_at = due + 2 * 60
    return {
        **current,
        "refresh_due": True,
        "refresh_queued": True,
        "next_check_at": datetime.fromtimestamp(due, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ready_by": datetime.fromtimestamp(ready_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


async def _process_pending_access_refresh() -> None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT key,value FROM registry_meta WHERE key LIKE 'access_refresh:%' ORDER BY key LIMIT 20"
            )
        ).fetchall()
    now = time.time()
    for row in rows:
        try:
            job = json.loads(row["value"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            job = {}
        if float(job.get("next_at") or 0) > now:
            continue
        enrollment_id = _clean(job.get("enrollment_id") or str(row["key"]).split(":", 1)[-1], 100)
        try:
            identity = await _access_identity(enrollment_id)
            result = await _get_access_view(identity, live=True, force=True, allow_stale=True)
            if result.get("ok") and not result.get("stale") and result.get("source") in {"live", "browser"}:
                async with _connect() as db:
                    await db.execute("DELETE FROM registry_meta WHERE key=?", (row["key"],))
                    await db.commit()
                return
            raise RuntimeError(result.get("warning") or result.get("error") or "GetCourse формирует выгрузку")
        except Exception as exc:
            attempts = int(job.get("attempts") or 0) + 1
            delay = _retry_delay(attempts - 1)
            await _meta_set(row["key"], json.dumps({
                "enrollment_id": enrollment_id,
                "next_at": time.time() + delay,
                "attempts": attempts,
                "error": _clean(exc, 500),
            }, ensure_ascii=False))
            return


async def _pending_access(identity: dict[str, Any]) -> dict[str, Any]:
    service = _module("chat-moderators", "service_latest_access_verification")
    return await asyncio.to_thread(
        service.service_latest_access_verification,
        gc_user_id=_clean(identity.get("gc_user_id"), 100),
        email=_clean(identity.get("email"), 300),
    )


def _epoch_iso(value: Any) -> str:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return ""


def _access_operation_view(
    item: dict[str, Any],
    *,
    student: dict[str, Any] | None = None,
    operator_name: str = "",
    compact: bool = False,
) -> dict[str, Any]:
    current = item.get("current_groups") or []
    target = item.get("target_groups") or []
    current_names = {_clean(group.get("name"), 500) for group in current if _clean(group.get("name"), 500)}
    target_names = {_clean(group.get("name"), 500) for group in target if _clean(group.get("name"), 500)}
    added = sorted(target_names - current_names)
    removed = sorted(current_names - target_names)
    result = item.get("apply_result") if isinstance(item.get("apply_result"), dict) else {}
    verification = result.get("verification") if isinstance(result.get("verification"), dict) else {}
    apply_queued = str(item.get("status") or "") == "pending" and bool(result.get("apply_queued"))
    pending = bool(result.get("verification_pending"))
    if str(item.get("status") or "") == "failed" or (verification and not verification.get("verified")):
        status = "failed"
    elif apply_queued:
        status = "queued"
    elif pending:
        status = "verifying"
    else:
        status = "completed"
    action = "access_change"
    if added and not removed:
        action = "access_grant"
    elif removed and not added:
        action = "access_remove"
    created_at = _epoch_iso(item.get("created_at"))
    updated_at = _epoch_iso(item.get("applied_at")) or created_at
    view = {
        "id": _clean(item.get("request_id"), 64),
        "status": status,
        "action": action,
        "student_name": _clean((student or {}).get("name"), 300),
        "email": _clean((student or {}).get("email") or item.get("identifier"), 320),
        "gc_user_id": _clean(item.get("gc_user_id"), 100),
        "operator_name": _clean(operator_name, 200),
        "created_at": created_at,
        "updated_at": updated_at,
        "added_count": len(added),
        "removed_count": len(removed),
        "verification_pending": bool(pending or apply_queued),
        "verification_attempts": int((result.get("apply_attempts") if apply_queued else result.get("verification_attempts")) or 0),
        "next_check_at": _epoch_iso(result.get("apply_next_at") if apply_queued else result.get("verification_next_at")),
        "ready_by": _epoch_iso(result.get("apply_ready_by")),
        "error": _clean(result.get("apply_error") if apply_queued else result.get("verification_error"), 1000),
    }
    if not compact:
        view.update({
            "added": added,
            "removed": removed,
            "missing": verification.get("missing") or [],
            "unexpected": verification.get("unexpected") or [],
            "verified": bool(verification.get("verified")),
        })
    return view


async def _access_operations(*, limit: int, request_id: str = "", compact: bool = False) -> list[dict[str, Any]]:
    access = _module("chat-moderators", "service_access_verifications")
    journal = await asyncio.to_thread(
        access.service_access_verifications,
        limit=max(1, min(int(limit), 200)),
        request_id=_clean(request_id, 64),
    )
    async with _connect() as db:
        students = [
            dict(row)
            for row in await (
                await db.execute(
                    "SELECT name,email,gc_user_id FROM enrollments WHERE status<>'removed' ORDER BY updated_at DESC"
                )
            ).fetchall()
        ]
        operators = [dict(row) for row in await (await db.execute("SELECT id,login,display_name FROM operators")).fetchall()]
    by_gc: dict[str, dict[str, Any]] = {}
    by_email: dict[str, dict[str, Any]] = {}
    for student in students:
        gc_user_id = _clean(student.get("gc_user_id"), 100)
        email = _clean(student.get("email"), 320).casefold()
        if gc_user_id:
            by_gc.setdefault(gc_user_id, student)
        if email:
            by_email.setdefault(email, student)
    operator_names = {
        str(item["id"]): _clean(item.get("display_name") or item.get("login"), 200)
        for item in operators
    }
    views = []
    for item in journal.get("items") or []:
        student = by_gc.get(_clean(item.get("gc_user_id"), 100)) or by_email.get(
            _clean(item.get("identifier"), 320).casefold()
        )
        views.append(
            _access_operation_view(
                item,
                student=student,
                operator_name=operator_names.get(str(item.get("requester_user_id") or ""), ""),
                compact=compact,
            )
        )
    return views


def _retry_delay(attempts: int) -> int:
    return (120, 300, 900, 3600)[min(max(0, int(attempts)), 3)]


def _is_google_rate_limit(value: Any) -> bool:
    return bool(re.search(r"(?:\b429\b|too many requests|resource_exhausted|quota)", str(value or ""), re.I))


def _is_transient_access_error(value: Any) -> bool:
    return bool(re.search(
        r"(?:\b429\b|too many requests|слишком много запросов|timeout|timed out|временно|temporar|connection|502|503|504)",
        str(value or ""),
        re.I,
    ))


def _retry_transfer_steps(steps: dict[str, Any]) -> tuple[dict[str, Any], str]:
    previous = steps.get("retry") if isinstance(steps.get("retry"), dict) else {}
    attempts = int(previous.get("attempts") or 0) + 1
    next_at = datetime.now(timezone.utc) + timedelta(seconds=_retry_delay(attempts - 1))
    steps["retry"] = {"attempts": attempts, "next_retry_at": next_at.strftime("%Y-%m-%dT%H:%M:%SZ"), "reason": "google_429"}
    return steps, f"Google Sheets: запрос поставлен в очередь. Повтор в {next_at.astimezone(MOSCOW_TZ).strftime('%H:%M')} МСК"


def _schedule_target_join_check(
    steps: dict[str, Any], *, delivery_already_sent: bool = False
) -> tuple[dict[str, Any], str]:
    previous = steps.get("join_wait") if isinstance(steps.get("join_wait"), dict) else {}
    attempts = int(previous.get("attempts") or 0) + 1
    delay = (60, 120, 300, 600, 900)[min(attempts - 1, 4)]
    next_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    steps["join_wait"] = {
        "attempts": attempts,
        "next_retry_at": next_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reason": "target_vk_join",
    }
    delivery_note = (
        "Повторная отправка отключена: ссылки уже были доставлены. "
        if delivery_already_sent
        else "Ссылка на новый VK-чат отправлена от сообщества. "
    )
    return steps, delivery_note + (
        f"Старый чат сохранён; вступление проверим в "
        f"{next_at.astimezone(MOSCOW_TZ).strftime('%H:%M')} МСК"
    )


async def _verify_pending_access() -> None:
    access = _module("chat-moderators", "service_pending_access_verifications")
    pending = await asyncio.to_thread(access.service_pending_access_verifications, limit=1)
    for request in pending.get("items") or []:
        scheduler = _module("chat-moderators", "service_schedule_access_verification")
        identity = {"gc_user_id": request.get("gc_user_id"), "email": request.get("identifier")}
        actual = await _get_access_after_write(identity)
        if not actual.get("ok") or actual.get("stale") or actual.get("source") not in {"live", "browser"}:
            await asyncio.to_thread(
                scheduler.service_schedule_access_verification,
                request_id=request["request_id"], delay_seconds=_retry_delay(int((request.get("apply_result") or {}).get("verification_attempts") or 0)),
                error=actual.get("warning") or actual.get("error") or "GetCourse формирует выгрузку",
            )
            continue
        verifier = _module("chat-moderators", "service_record_access_verification")
        verification = await asyncio.to_thread(
            verifier.service_record_access_verification,
            request_id=request["request_id"], actual_groups=actual.get("current_groups") or [], defer_on_mismatch=True,
        )
        if not verification.get("verified"):
            attempts = int((request.get("apply_result") or {}).get("verification_attempts") or 0)
            await asyncio.to_thread(
                scheduler.service_schedule_access_verification,
                request_id=request["request_id"], delay_seconds=_retry_delay(attempts),
                error="GetCourse обновляет список групп",
            )


async def _apply_pending_access() -> None:
    access = _module("chat-moderators", "service_pending_access_applies")
    pending = await asyncio.to_thread(access.service_pending_access_applies, limit=1)
    for request in pending.get("items") or []:
        request_id = _clean(request.get("request_id"), 64)
        requester = _clean(request.get("requester_user_id"), 200)
        try:
            applier = _module("chat-moderators", "service_apply_access_change")
            await asyncio.to_thread(
                applier.service_apply_access_change,
                request_id=request_id,
                requester_user_id=requester,
            )
            scheduler = _module("chat-moderators", "service_schedule_access_verification")
            await asyncio.to_thread(
                scheduler.service_schedule_access_verification,
                request_id=request_id, delay_seconds=ACCESS_VERIFY_DELAY_SECONDS,
            )
        except Exception as exc:
            attempts = int((request.get("apply_result") or {}).get("apply_attempts") or 0)
            if _is_transient_access_error(exc) and attempts < 8:
                retry = _module("chat-moderators", "service_retry_access_apply")
                await asyncio.to_thread(
                    retry.service_retry_access_apply,
                    request_id=request_id,
                    delay_seconds=_retry_delay(attempts),
                    error=_clean(exc, 500),
                )
            else:
                fail = _module("chat-moderators", "service_fail_access_apply")
                await asyncio.to_thread(
                    fail.service_fail_access_apply,
                    request_id=request_id,
                    error=_clean(exc, 500),
                )


def _iso_age_seconds(value: Any) -> float:
    try:
        updated_at = datetime.fromisoformat(_clean(value, 40).replace("Z", "+00:00"))
        return max(0.0, time.time() - updated_at.timestamp())
    except (TypeError, ValueError):
        return float("inf")


async def _sync_access_snapshots() -> dict[str, Any]:
    last_sync = await _meta_get("last_access_sync_at")
    if _iso_age_seconds(last_sync) < 6 * 3600:
        return {"ok": True, "status": "recent", "updated_at": last_sync}
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        identities = [
            dict(row)
            for row in await (
                await db.execute(
                    "SELECT gc_user_id,email FROM enrollments WHERE status<>'removed' AND (gc_user_id<>'' OR email<>'')"
                )
            ).fetchall()
        ]
    access = _module("chat-moderators", "service_access_catalog")
    catalog_result = await asyncio.to_thread(access.service_access_catalog)
    catalog = catalog_result.get("items") or []
    roots = [
        _clean(item.get("group_id"), 30)
        for item in catalog
        if item.get("group_kind") == "root" and item.get("course_key") in {"puppy", "dog"}
    ]
    fields = _module("getcourse-chat-fields", "service_sync_getcourse_access_snapshots")
    result = await fields.service_sync_getcourse_access_snapshots(
        identities=identities,
        catalog=catalog,
        root_group_ids=roots,
    )
    if result.get("ok"):
        await _meta_set("last_access_sync_at", _now())
    return result


async def _access_sync_loop() -> None:
    await asyncio.sleep(15)
    batch_at = 0.0
    while True:
        delay = 30
        try:
            if time.monotonic() >= batch_at:
                result = await _sync_access_snapshots()
                if result.get("status") == "recent":
                    batch_at = time.monotonic() + max(5 * 60, int(6 * 3600 - _iso_age_seconds(result.get("updated_at"))))
                elif not result.get("ok"):
                    batch_at = time.monotonic() + 30 * 60
                    if _logger:
                        _logger.warning("GetCourse access batch sync deferred: %s", result.get("error") or result)
                else:
                    batch_at = time.monotonic() + 6 * 3600
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("GetCourse access batch sync failed: %s", exc)
        await asyncio.sleep(delay)


def _future_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, seconds))).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _next_browser_access_identity() -> dict[str, str] | None:
    now = _now()
    stale_before = (datetime.now(timezone.utc) - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                """
                SELECT e.gc_user_id,MAX(e.email) AS email,COALESCE(s.last_attempt_at,'') AS last_attempt_at
                FROM enrollments e
                LEFT JOIN access_browser_snapshots s ON s.gc_user_id=e.gc_user_id
                WHERE e.status<>'removed' AND e.gc_user_id<>''
                  AND e.gc_user_id NOT GLOB '*[^0-9]*'
                  AND (s.next_attempt_at IS NULL OR s.next_attempt_at='' OR s.next_attempt_at<=?)
                  AND (s.updated_at IS NULL OR s.updated_at='' OR s.updated_at<=?)
                GROUP BY e.gc_user_id
                ORDER BY COALESCE(s.last_attempt_at,'') ASC,e.gc_user_id ASC
                LIMIT 1
                """,
                (now, stale_before),
            )
        ).fetchone()
    return {"gc_user_id": row["gc_user_id"], "email": row["email"] or ""} if row else None


async def _save_browser_access_failure(identity: dict[str, Any], error: str) -> int:
    user_id = _clean(identity.get("gc_user_id"), 100)
    if not user_id.isdigit():
        return 0
    async with _connect() as db:
        row = await (
            await db.execute(
                "SELECT failures FROM access_browser_snapshots WHERE gc_user_id=?", (user_id,)
            )
        ).fetchone()
        failures = int(row[0] or 0) + 1 if row else 1
        delay = (60, 300, 900, 1800, 3600, 7200)[min(failures - 1, 5)]
        await db.execute(
            """
            INSERT INTO access_browser_snapshots(
                gc_user_id,email,snapshot_json,updated_at,last_attempt_at,next_attempt_at,failures,last_error
            ) VALUES(?,?,'{}','',?,?,?,?)
            ON CONFLICT(gc_user_id) DO UPDATE SET
                email=excluded.email,last_attempt_at=excluded.last_attempt_at,
                next_attempt_at=excluded.next_attempt_at,failures=excluded.failures,last_error=excluded.last_error
            """,
            (
                user_id,
                _clean(identity.get("email"), 320),
                _now(),
                _future_iso(delay),
                failures,
                _clean(error, 1000),
            ),
        )
        await db.commit()
    return failures


def _browser_access_global_pause(error: str) -> int:
    normalized = _norm(error)
    if any(marker in normalized for marker in ("сессия", "авторизац", "войти", "login", "captcha", "капч", "403")):
        return 6 * 3600
    if "429" in normalized or "too many" in normalized:
        return 30 * 60
    return 0


async def _access_browser_cache_loop() -> None:
    """Warm Streams and messenger access cards through one throttled GC browser."""

    await asyncio.sleep(45)
    while True:
        delay = 5.0
        try:
            pause_until = await _meta_get("access_browser_pause_until")
            if pause_until and pause_until > _now():
                delay = min(300.0, max(30.0, _iso_age_seconds(_now()) + 60.0))
            else:
                identity = await _next_browser_access_identity()
                if not identity:
                    delay = 60.0
                else:
                    browser = _module("getcourse-onboarding", "service_getcourse_browser_access_snapshot")
                    snapshot = await browser.service_getcourse_browser_access_snapshot(
                        gc_user_id=identity["gc_user_id"]
                    )
                    if snapshot.get("ok"):
                        await _save_browser_access_snapshot(identity, snapshot)
                        delay = 1.0 + secrets.randbelow(801) / 1000
                    else:
                        error = _clean(snapshot.get("error"), 1000) or "GetCourse browser snapshot failed"
                        await _save_browser_access_failure(identity, error)
                        pause = _browser_access_global_pause(error)
                        if pause:
                            await _meta_set("access_browser_pause_until", _future_iso(pause))
                            if _logger:
                                _logger.warning("GetCourse browser access cache paused: %s", error)
                        delay = 30.0 if pause else 2.0 + secrets.randbelow(1001) / 1000
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            pause = _browser_access_global_pause(str(exc))
            if pause:
                await _meta_set("access_browser_pause_until", _future_iso(pause))
            if _logger:
                _logger.warning("GetCourse browser access cache iteration failed: %s", exc)
            delay = 60.0
        await asyncio.sleep(delay)


async def _access_queue_loop() -> None:
    """Keep employee access changes responsive even while the bulk GC sync is busy."""
    await asyncio.sleep(5)
    while True:
        testdrive_worked = False
        try:
            await _apply_pending_access()
            await _verify_pending_access()
            await _process_pending_access_refresh()
            await _process_test_periods()
            await _process_manual_promotions()
            testdrive_worked = await _process_testdrive_confirms()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("GetCourse access queue iteration failed: %s", exc)
        await asyncio.sleep(1.0 + secrets.randbelow(801) / 1000 if testdrive_worked else 10)


def _test_period_retry_at(attempts: int) -> str:
    delay = (15, 30, 60, 120, 300, 900, 1800, 3600)[min(max(0, attempts), 7)]
    return (datetime.now(timezone.utc) + timedelta(seconds=delay)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _test_period_aliases(identity: dict[str, Any]) -> list[tuple[str, str]]:
    aliases: list[tuple[str, str]] = []
    gc_user_id = _clean(identity.get("gc_user_id"), 100)
    email = _clean(identity.get("email"), 320).casefold()
    phone = _phone_search_key(identity.get("phone"))
    if gc_user_id:
        aliases.append(("gc_user_id", gc_user_id))
    if email:
        aliases.append(("email", email))
    if len(phone) >= 10:
        aliases.append(("phone", phone))
    return aliases


async def _testdrive_browser_alias(value: Any) -> tuple[str, str] | None:
    browser_id = _clean(value, 128)
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", browser_id):
        return None
    key = await _meta_get("testdrive_hash_key")
    if not key:
        return None
    digest = hmac.new(key.encode("utf-8"), browser_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return "browser", digest


async def _testdrive_aliases(email: Any = "", phone: Any = "", browser_id: Any = "") -> list[tuple[str, str]]:
    aliases = _test_period_aliases({"email": email, "phone": phone})
    browser = await _testdrive_browser_alias(browser_id)
    if browser:
        aliases.append(browser)
    return aliases


async def _remember_testdrive_pending(email: Any, phone: Any, browser_alias: tuple[str, str] | None) -> None:
    if not browser_alias:
        return
    email_value = _clean(email, 320).casefold()
    phone_key = _phone_search_key(phone)
    if not email_value and len(phone_key) < 10:
        return
    now = _now()
    async with _connect() as db:
        await db.execute("DELETE FROM testdrive_pending WHERE expires_at<=?", (now,))
        await db.execute(
            """INSERT INTO testdrive_pending(email,phone_key,browser_hash,created_at,expires_at)
               VALUES(?,?,?,?,?)""",
            (email_value, phone_key, browser_alias[1], now, _future_iso(2 * 3600)),
        )
        await db.commit()


async def _pending_testdrive_browser_alias(identity: dict[str, Any]) -> tuple[str, str] | None:
    email = _clean(identity.get("email"), 320).casefold()
    phone_key = _phone_search_key(identity.get("phone"))
    if not email and len(phone_key) < 10:
        return None
    clauses: list[str] = []
    params: list[str] = []
    if email:
        clauses.append("email=?")
        params.append(email)
    if len(phone_key) >= 10:
        clauses.append("phone_key=?")
        params.append(phone_key)
    async with _connect() as db:
        row = await (
            await db.execute(
                f"SELECT browser_hash FROM testdrive_pending WHERE expires_at>? AND ({' OR '.join(clauses)}) "
                "ORDER BY created_at DESC LIMIT 1",
                (_now(), *params),
            )
        ).fetchone()
    return ("browser", _clean(row[0], 64)) if row and _clean(row[0], 64) else None


async def _test_period_for_aliases(aliases: list[tuple[str, str]]) -> dict[str, Any] | None:
    if not aliases:
        return None
    clauses = " OR ".join("(i.identity_type=? AND i.identity_value=?)" for _ in aliases)
    params = [value for pair in aliases for value in pair]
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                f"SELECT p.* FROM test_period_identities i JOIN test_periods p ON p.id=i.test_period_id "
                f"WHERE {clauses} ORDER BY p.created_at DESC LIMIT 1",
                params,
            )
        ).fetchone()
    return dict(row) if row else None


async def _test_period_catalog() -> dict[str, dict[str, Any]]:
    service = _module("chat-moderators", "service_test_period_catalog")
    result = await asyncio.to_thread(service.service_test_period_catalog)
    if not result.get("ok"):
        raise RuntimeError("Не найдены группы тестового периода: " + ", ".join(result.get("missing") or []))
    return result.get("items") or {}


def _test_period_group_ids(catalog: dict[str, dict[str, Any]], courses: list[str]) -> list[str]:
    keys = [f"module_{index}" for index in range(1, 9)] + list(courses)
    return [str(catalog[key]["group_id"]) for key in keys]


def _test_period_row_view(row: dict[str, Any] | aiosqlite.Row | None) -> dict[str, Any]:
    if not row:
        return {
            "exists": False, "status": "available", "can_issue": True,
            "can_repeat": False, "operation_pending": False, "reason": "",
        }
    item = dict(row)
    try:
        courses = json.loads(item.get("courses_json") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        courses = []
    status = _clean(item.get("status"), 40)
    pending = status in {"queued_grant", "granting", "queued_revoke", "revoking"}
    reasons = {
        "queued_grant": "Команда принята. Тестовый период будет выдан в фоне.",
        "granting": "Тестовый период выдаётся в GetCourse…",
        "active": "Тестовый период выдан. Nexus автоматически закроет его в срок.",
        "queued_revoke": "Срок закончился. Доступы закрываются в фоне…",
        "revoking": "Доступы тестового периода закрываются в GetCourse…",
        "completed": "Тестовый период завершён, доступы закрыты.",
        "blocked_used": "Ученик уже использовал тестовый период.",
    }
    return {
        "exists": True,
        "id": _clean(item.get("id"), 64),
        "status": status,
        "courses": courses,
        "starts_at": _clean(item.get("starts_at"), 40),
        "expires_at": _clean(item.get("expires_at"), 40),
        "completed_at": _clean(item.get("completed_at"), 40),
        "last_error": _clean(item.get("last_error"), 1000),
        "attempts": int(item.get("attempts") or 0),
        "can_issue": False,
        "can_repeat": status in {"completed", "blocked_used"},
        "operation_pending": pending,
        "reason": reasons.get(status, "Тестовый период уже выдавался"),
    }


def _test_period_active_window(period: dict[str, Any]) -> tuple[str, str]:
    """Start the promised duration only after GetCourse confirms the grant."""

    try:
        planned_start = datetime.fromisoformat(_clean(period.get("starts_at"), 40).replace("Z", "+00:00"))
        planned_end = datetime.fromisoformat(_clean(period.get("expires_at"), 40).replace("Z", "+00:00"))
        duration = max(timedelta(minutes=1), planned_end - planned_start)
    except (TypeError, ValueError):
        duration = timedelta(days=1)
    starts = datetime.now(timezone.utc).replace(microsecond=0)
    return (
        starts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        (starts + duration).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


async def _test_period_row(period_id: str = "", enrollment_id: str = "") -> dict[str, Any] | None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if period_id:
            row = await (await db.execute("SELECT * FROM test_periods WHERE id=?", (_clean(period_id, 64),))).fetchone()
        else:
            row = await (
                await db.execute(
                    "SELECT * FROM test_periods WHERE enrollment_id=? ORDER BY created_at DESC LIMIT 1",
                    (_clean(enrollment_id, 100),),
                )
            ).fetchone()
    return dict(row) if row else None


async def _test_period_request_state(request_id: str) -> str:
    service = _module("chat-moderators", "service_access_verifications")
    result = await asyncio.to_thread(
        service.service_access_verifications, limit=1, request_id=_clean(request_id, 64)
    )
    item = next(iter(result.get("items") or []), None)
    if not item:
        return "missing"
    status = _clean(item.get("status"), 30)
    apply_result = item.get("apply_result") if isinstance(item.get("apply_result"), dict) else {}
    verification = apply_result.get("verification") if isinstance(apply_result.get("verification"), dict) else {}
    if status in {"failed", "cancelled"}:
        return "failed"
    if status == "applied" and verification.get("verified") and not apply_result.get("verification_pending"):
        return "verified"
    return "pending"


async def _update_test_period(period_id: str, **values: Any) -> None:
    if not values:
        return
    values["updated_at"] = _now()
    columns = ",".join(f"{key}=?" for key in values)
    async with _connect() as db:
        await db.execute(
            f"UPDATE test_periods SET {columns} WHERE id=?",
            (*values.values(), _clean(period_id, 64)),
        )
        await db.commit()


async def _prepare_test_period_access(period: dict[str, Any], *, revoke: bool) -> str:
    identity = {
        "gc_user_id": period.get("gc_user_id"),
        "email": period.get("email"),
        "phone": period.get("phone_key"),
    }
    current = await _get_access_view(identity, live=True, force=True, allow_stale=True)
    if not current.get("ok") or current.get("stale"):
        raise RuntimeError(current.get("error") or current.get("warning") or "GetCourse ещё не вернул актуальные доступы")
    catalog = await _test_period_catalog()
    current_ids = {str(item.get("group_id") or "") for item in current.get("current_groups") or []}
    all_trial_ids = {
        str(item["group_id"])
        for key, item in catalog.items()
        if key != "used"
    }
    used_id = str(catalog["used"]["group_id"])
    allow_repeat = bool(int(period.get("allow_repeat") or 0))
    if not revoke and used_id in current_ids and not allow_repeat:
        return "used"
    if revoke:
        changes = [{"group_id": group_id, "enabled": False} for group_id in sorted(all_trial_ids)]
        changes.append({"group_id": used_id, "enabled": True})
        already_done = not (current_ids & all_trial_ids) and used_id in current_ids
    else:
        courses = json.loads(period.get("courses_json") or "[]")
        grant_ids = set(_test_period_group_ids(catalog, courses))
        changes = [{"group_id": group_id, "enabled": True} for group_id in sorted(grant_ids)]
        if allow_repeat and used_id in current_ids:
            changes.insert(0, {"group_id": used_id, "enabled": False})
        already_done = grant_ids.issubset(current_ids) and (not allow_repeat or used_id not in current_ids)
    if already_done:
        return "already_done"
    preparer = _module("chat-moderators", "service_prepare_test_period_change")
    prepared = await asyncio.to_thread(
        preparer.service_prepare_test_period_change,
        gc_user_id=_clean(period.get("gc_user_id"), 100),
        email=_clean(period.get("email"), 320),
        current_groups=current.get("current_groups") or [],
        changes=changes,
        requester_user_id=str(period.get("operator_id") or "test-period"),
    )
    scheduler = _module("chat-moderators", "service_schedule_access_apply")
    await asyncio.to_thread(
        scheduler.service_schedule_access_apply,
        request_id=prepared["request_id"],
        requester_user_id=str(period.get("operator_id") or "test-period"),
        delay_seconds=2,
    )
    return _clean(prepared.get("request_id"), 64)


async def _advance_test_period(period: dict[str, Any]) -> None:
    period_id = _clean(period.get("id"), 64)
    status = _clean(period.get("status"), 40)
    expired = _clean(period.get("expires_at"), 40) <= _now()
    try:
        if expired and status not in {"queued_revoke", "revoking", "completed", "blocked_used"}:
            grant_request_id = _clean(period.get("grant_request_id"), 64)
            if grant_request_id:
                try:
                    cancel = _module("chat-moderators", "service_cancel_access_change")
                    await asyncio.to_thread(cancel.service_cancel_access_change, request_id=grant_request_id)
                except Exception:
                    pass
            status = "queued_revoke"
            await _update_test_period(period_id, status=status, next_attempt_at=_now(), last_error="")
            period["status"] = status
        if status == "queued_grant":
            request_id = await _prepare_test_period_access(period, revoke=False)
            if request_id == "used":
                await _update_test_period(
                    period_id, status="queued_revoke", next_attempt_at=_now(),
                    last_error="Тестовый период уже использован; проверяем снятие тестовых групп",
                )
                return
            if request_id == "already_done":
                starts_at, expires_at = _test_period_active_window(period)
                await _update_test_period(
                    period_id, status="active", starts_at=starts_at, expires_at=expires_at,
                    next_attempt_at=expires_at, attempts=0, last_error="",
                )
            else:
                await _update_test_period(period_id, status="granting", grant_request_id=request_id, next_attempt_at=_test_period_retry_at(0), last_error="")
            return
        if status == "granting":
            request_state = await _test_period_request_state(period.get("grant_request_id") or "")
            if request_state == "verified":
                starts_at, expires_at = _test_period_active_window(period)
                await _update_test_period(
                    period_id, status="active", starts_at=starts_at, expires_at=expires_at,
                    next_attempt_at=expires_at, attempts=0, last_error="",
                )
            elif request_state == "failed" or request_state == "missing":
                attempts = int(period.get("attempts") or 0) + 1
                await _update_test_period(period_id, status="queued_grant", grant_request_id="", attempts=attempts, next_attempt_at=_test_period_retry_at(attempts), last_error="Изменение не подтвердилось; Nexus повторит")
            else:
                await _update_test_period(period_id, next_attempt_at=_test_period_retry_at(0))
            return
        if status == "active":
            await _update_test_period(period_id, next_attempt_at=period["expires_at"])
            return
        if status == "queued_revoke":
            request_id = await _prepare_test_period_access(period, revoke=True)
            if request_id == "already_done":
                await _update_test_period(period_id, status="completed", completed_at=_now(), next_attempt_at="", attempts=0, last_error="")
            else:
                await _update_test_period(period_id, status="revoking", revoke_request_id=request_id, next_attempt_at=_test_period_retry_at(0), last_error="")
            return
        if status == "revoking":
            request_state = await _test_period_request_state(period.get("revoke_request_id") or "")
            if request_state == "verified":
                await _update_test_period(period_id, status="completed", completed_at=_now(), next_attempt_at="", attempts=0, last_error="")
            elif request_state == "failed" or request_state == "missing":
                attempts = int(period.get("attempts") or 0) + 1
                await _update_test_period(period_id, status="queued_revoke", revoke_request_id="", attempts=attempts, next_attempt_at=_test_period_retry_at(attempts), last_error="Снятие не подтвердилось; Nexus повторит")
            else:
                await _update_test_period(period_id, next_attempt_at=_test_period_retry_at(0))
    except Exception as exc:
        attempts = int(period.get("attempts") or 0) + 1
        retry_status = "queued_revoke" if status in {"queued_revoke", "revoking"} or expired else "queued_grant"
        await _update_test_period(
            period_id,
            status=retry_status,
            attempts=attempts,
            next_attempt_at=_test_period_retry_at(attempts),
            last_error=_clean(exc, 1000),
            **({"revoke_request_id": ""} if retry_status == "queued_revoke" else {"grant_request_id": ""}),
        )
        if _logger:
            _logger.warning("Test period %s deferred: %s", period_id, exc)


async def _process_test_periods(period_id: str = "") -> None:
    now = _now()
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if period_id:
            rows = await (await db.execute("SELECT * FROM test_periods WHERE id=? LIMIT 1", (period_id,))).fetchall()
        else:
            rows = await (
                await db.execute(
                    "SELECT * FROM test_periods WHERE status NOT IN ('completed','blocked_used') "
                    "AND (next_attempt_at='' OR next_attempt_at<=? OR expires_at<=?) "
                    "ORDER BY CASE WHEN expires_at<=? THEN 0 ELSE 1 END,next_attempt_at,created_at LIMIT 1",
                    (now, now, now),
                )
            ).fetchall()
    for row in rows:
        await _advance_test_period(dict(row))


def _find_source(snapshot: dict[str, Any], ref: TransferRef | CuratorChangeRef) -> tuple[dict[str, Any], dict[str, Any]]:
    for flow in snapshot.get("items") or []:
        if _flow_key(flow) != (_clean(ref.source_course_key, 50), _clean(ref.source_stream, 50)):
            continue
        for student in flow.get("students") or []:
            if ref.enrollment_id and _clean(student.get("enrollment_id"), 100) == _clean(ref.enrollment_id, 100):
                return flow, student
            if int(student.get("row") or 0) == ref.source_row and _norm(student.get("email")) == _norm(ref.email):
                return flow, student
    raise HTTPException(409, "Исходное назначение ученика изменилось")


def _find_target(snapshot: dict[str, Any], ref: TransferRef) -> dict[str, Any]:
    for flow in snapshot.get("items") or []:
        if _flow_key(flow) == (_clean(ref.target_course_key, 50), _clean(ref.target_stream, 50)):
            return flow
    raise HTTPException(404, "Целевой поток не найден")


async def _preview(ref: TransferRef, *, refresh: bool = False) -> dict[str, Any]:
    same_flow = (
        _clean(ref.source_course_key, 50) == _clean(ref.target_course_key, 50)
        and _clean(ref.source_stream, 50) == _clean(ref.target_stream, 50)
    )
    chat_source_stream = _clean(ref.chat_source_stream, 50)
    vk_target = _vk_target_id(ref.vk_target)
    if same_flow and not ref.chat_only:
        raise HTTPException(400, "Выберите другой поток")
    if ref.chat_only and (not same_flow or not chat_source_stream or chat_source_stream == _clean(ref.target_stream, 50)):
        raise HTTPException(400, "Для исправления чатов укажите другой ошибочный поток")
    snapshot = await _snapshot(refresh=refresh)
    source_flow, student = _find_source(snapshot, ref)
    target = _find_target(snapshot, ref)
    curator = _clean(target.get("curator_value"), 100)
    offer_id = int(target.get("offer_id") or CURATOR_OFFERS.get(curator, 0))
    source = _student_result(source_flow, student)
    await _enrich_order_identities([source])
    await _enrich_successful_managers([source])
    assignment = source.get("getcourse_assignment") or {}
    registry_repair = not ref.chat_only and all((
        _clean(assignment.get("course_key"), 50) == _clean(target.get("course_key"), 50),
        _clean(assignment.get("stream"), 50) == _clean(target.get("stream"), 50),
        _clean(assignment.get("vk_link"), 2000) == _clean(target.get("vk_link"), 2000),
        _clean(assignment.get("tg_link"), 2000) == _clean(target.get("tg_link"), 2000),
        _clean(assignment.get("curator"), 100) == curator,
    ))
    chat_readiness: dict[str, Any] = {}
    if ref.chat_only:
        chat_service = _module("course-chat-creator", "service_transfer_chat_readiness")
        chat_readiness = chat_service.service_transfer_chat_readiness(
            _clean(source_flow.get("course_key"), 50), chat_source_stream
        )
    elif not registry_repair:
        chat_service = _module("course-chat-creator", "service_transfer_chat_readiness")
        chat_readiness = chat_service.service_transfer_chat_readiness(
            _clean(source_flow.get("course_key"), 50), _clean(source_flow.get("stream"), 50)
        )
    warnings: list[str] = []
    if ref.chat_only:
        warnings.append(
            f"Данные ученика не меняются: ссылки будут отправлены повторно, удаление выполняется из чата потока {chat_source_stream}"
        )
        if ref.delivery_already_sent:
            warnings[-1] = (
                f"Сообщения повторно не отправляются: Nexus ждёт вступления в новый VK-чат и только затем "
                f"удаляет ученика из чата потока {chat_source_stream}"
            )
    elif registry_repair:
        warnings.append("GetCourse и чаты не изменяются")
    elif source_flow.get("course_key") != target.get("course_key"):
        warnings.append("Меняется курс; доступ к обучению должен уже существовать в GetCourse")
    for platform, label in (("vk", "VK"), ("telegram", "Telegram")):
        if chat_readiness.get(platform, {}).get("status") == "legacy_inaccessible":
            warnings.append(f"Из старого чата {label} потребуется удалить вручную")
    blockers: list[str] = []
    if not _clean(student.get("gc_user_id"), 100):
        blockers.append("У ученика не найден ID GetCourse")
    if not offer_id and not registry_repair and not ref.chat_only:
        blockers.append("Для куратора целевого потока не задано предложение")
    if ref.chat_only and chat_readiness.get("vk", {}).get("status") == "not_recorded":
        blockers.append("Ошибочный VK-чат не найден")
    if ref.vk_target and not vk_target:
        blockers.append("Укажите числовой VK ID или ссылку вида https://vk.com/id123")
    if ref.delivery_already_sent and (not ref.chat_only or not vk_target):
        blockers.append("Для контроля без повторной отправки укажите VK ID ученика")
    return {
        "action": "chat_repair" if ref.chat_only else ("registry_repair" if registry_repair else "transfer"),
        "chat_source_stream": chat_source_stream,
        "vk_target": vk_target,
        "delivery_already_sent": bool(ref.delivery_already_sent),
        "can_transfer": not blockers,
        "source": source,
        "target": {
            "course_key": _clean(target.get("course_key"), 50),
            "course": _clean(target.get("course"), 100),
            "stream": _clean(target.get("stream"), 50),
            "curator": curator,
            "curator_name": CURATOR_NAMES.get(curator, ""),
            "offer_id": offer_id,
            "vk_link": _clean(target.get("vk_link"), 2000),
            "tg_link": _clean(target.get("tg_link"), 2000),
        },
        "sheet": {
            "found": int(source.get("row") or 0) > 0,
            "title": source.get("sheet_title") or "",
            "row": int(source.get("row") or 0),
            "move": False if ref.chat_only else bool(ref.move_sheet_row),
        },
        "chat_readiness": chat_readiness,
        "warnings": warnings,
        "blockers": blockers,
    }


def _transfer_ref_payload(ref: TransferRef) -> dict[str, Any]:
    # The operator may intentionally toggle "delete old row" after seeing the
    # preview; every identity/flow field must still match exactly.
    return ref.model_dump(exclude={"preview_id", "move_sheet_row"})


async def _remember_transfer_preview(ref: TransferRef, preview_data: dict[str, Any]) -> dict[str, Any]:
    preview_id = uuid.uuid4().hex
    await _meta_set(f"transfer_preview:{preview_id}", json.dumps({
        "expires_at": time.time() + 10 * 60,
        "request": _transfer_ref_payload(ref),
        "preview": preview_data,
    }, ensure_ascii=False))
    return {**preview_data, "preview_id": preview_id}


async def _saved_transfer_preview(ref: TransferRef) -> dict[str, Any]:
    preview_id = _clean(ref.preview_id, 64)
    if not preview_id:
        raise HTTPException(409, "Сначала проверьте перенос")
    raw = await _meta_get(f"transfer_preview:{preview_id}")
    try:
        saved = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        saved = {}
    if float(saved.get("expires_at") or 0) < time.time():
        raise HTTPException(409, "Проверка переноса устарела. Проверьте ещё раз")
    if saved.get("request") != _transfer_ref_payload(ref):
        raise HTTPException(409, "Параметры переноса изменились. Проверьте ещё раз")
    preview_data = saved.get("preview")
    if not isinstance(preview_data, dict):
        raise HTTPException(409, "Проверка переноса не найдена. Проверьте ещё раз")
    preview_data = json.loads(json.dumps(preview_data, ensure_ascii=False))
    sheet = preview_data.get("sheet") if isinstance(preview_data.get("sheet"), dict) else {}
    sheet["move"] = False if ref.chat_only else bool(ref.move_sheet_row)
    preview_data["sheet"] = sheet
    return preview_data


async def _preview_curator_change(ref: CuratorChangeRef, *, refresh: bool = False) -> dict[str, Any]:
    snapshot = await _snapshot(refresh=refresh)
    source_flow, student = _find_source(snapshot, ref)
    curator = _clean(ref.curator, 100)
    offer_id = CURATOR_OFFERS.get(curator, 0)
    source = _student_result(source_flow, student)
    blockers: list[str] = []
    if not _clean(student.get("gc_user_id"), 100):
        blockers.append("У ученика не найден ID GetCourse")
    if not offer_id:
        blockers.append("Куратор не поддерживается")
    if source.get("curator") == curator:
        blockers.append("Этот куратор уже назначен")
    return {
        "action": "curator_change",
        "can_change": not blockers,
        "source": source,
        "target": {
            "curator": curator,
            "curator_name": CURATOR_NAMES.get(curator, ""),
            "offer_id": offer_id,
        },
        "blockers": blockers,
    }


def _load_steps(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}") if not isinstance(value, dict) else value
    except Exception:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def _flow_job_ui_result(result: dict[str, Any], course_key: str, stream: str) -> dict[str, Any]:
    """Keep operation details small; sync diagnostics can contain thousands of students."""
    create = result.get("create") if isinstance(result.get("create"), dict) else {}
    vk = create.get("vk") if isinstance(create.get("vk"), dict) else {}
    telegram = create.get("telegram") if isinstance(create.get("telegram"), dict) else {}
    catalog = create.get("catalog") if isinstance(create.get("catalog"), dict) else {}
    sheet = result.get("sheet") if isinstance(result.get("sheet"), dict) else {}
    catalog_flow = next(
        (
            item for item in catalog.get("items") or []
            if _clean(item.get("course_key"), 50) == course_key
            and _clean(item.get("stream"), 50) == stream
        ),
        {},
    )
    return {
        "stages": result.get("stages") if isinstance(result.get("stages"), dict) else {},
        "manual": result.get("manual") if isinstance(result.get("manual"), dict) else {},
        "sheet": {
            key: sheet.get(key)
            for key in (
                "ok", "status", "sheet_id", "sheet_title", "sheet_url",
                "sheet_index", "verified", "hidden", "error",
            )
            if sheet.get(key) not in (None, "")
        },
        "create": {
            "vk": {
                key: vk.get(key)
                for key in ("group_link", "chat_id", "peer_id", "owner_group_id", "status", "followup_status")
                if vk.get(key) not in (None, "")
            },
            "telegram": {
                key: telegram.get(key)
                for key in ("group_link", "chat_id", "status")
                if telegram.get(key) not in (None, "")
            },
            "catalog": {"items": [catalog_flow] if catalog_flow else []},
        },
    }


async def _save_transfer(transfer_id: str, *, status: str, steps: dict[str, Any], error: str = "") -> None:
    async with _connect() as db:
        await db.execute(
            "UPDATE transfers SET status=?,steps_json=?,error=?,updated_at=? WHERE id=?",
            (status, json.dumps(steps, ensure_ascii=False), _clean(error, 2000), _now(), transfer_id),
        )
        await db.commit()


async def _commit_registry_transfer(
    transfer: dict[str, Any], *, curator_only: bool = False, target_row: int = 0,
    source_row_deleted: bool = False,
) -> dict[str, Any]:
    enrollment_id = _clean(transfer.get("enrollment_id"), 100)
    where = "id=?" if enrollment_id else "lower(email)=lower(?) AND course_key=? AND stream=?"
    args: tuple[Any, ...] = (
        (enrollment_id,)
        if enrollment_id
        else (
            _clean(transfer.get("email"), 320),
            _clean(transfer.get("source_course_key"), 50),
            _clean(transfer.get("source_stream"), 50),
        )
    )
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        if curator_only:
            cur = await db.execute(
                f"UPDATE enrollments SET teacher=?,teacher_code=?,updated_at=? WHERE {where}",
                (
                    CURATOR_NAMES.get(_clean(transfer.get("curator"), 100), _clean(transfer.get("curator"), 100)),
                    _clean(transfer.get("curator"), 100),
                    _now(),
                    *args,
                ),
            )
        else:
            flow = await (
                await db.execute(
                    "SELECT * FROM flow_registry WHERE course_key=? AND stream=?",
                    (_clean(transfer.get("target_course_key"), 50), _clean(transfer.get("target_stream"), 50)),
                )
            ).fetchone()
            if not flow:
                raise RuntimeError("Целевой поток исчез из реестра")
            current = await (await db.execute(f"SELECT source_json FROM enrollments WHERE {where}", args)).fetchone()
            try:
                source = json.loads(current["source_json"] or "{}") if current else {}
            except (TypeError, json.JSONDecodeError):
                source = {}
            if not isinstance(source, dict):
                source = {}
            if target_row:
                source["row"] = int(target_row)
            assignments = [item for item in source.get("course_assignments", []) if isinstance(item, dict)]
            source_key = (_clean(transfer.get("source_course_key"), 50), _clean(transfer.get("source_stream"), 50))
            target_key = (_clean(transfer.get("target_course_key"), 50), _clean(transfer.get("target_stream"), 50))
            if not assignments:
                assignments.append({"course_key": source_key[0], "stream": source_key[1], "row": int(transfer.get("source_row") or 0)})
            assignments = [
                item for item in assignments
                if (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50)) != target_key
                and (
                    not source_row_deleted
                    or (_clean(item.get("course_key"), 50), _clean(item.get("stream"), 50)) != source_key
                )
            ]
            assignments.append({
                "course_key": target_key[0],
                "course": "Щенок" if target_key[0] == "puppy" else "Собака",
                "stream": target_key[1],
                "row": int(target_row or 0),
            })
            source["course_assignments"] = sorted(
                assignments,
                key=lambda item: (0 if _clean(item.get("course_key"), 50) == "puppy" else 1, int(item.get("stream") or 0)),
            )
            cur = await db.execute(
                f"""
                UPDATE enrollments SET course_key=?,course=?,stream=?,teacher=?,teacher_code=?,status='assigned',source_json=?,updated_at=?
                WHERE {where}
                """,
                (
                    _clean(transfer.get("target_course_key"), 50),
                    "Щенок" if _clean(transfer.get("target_course_key"), 50) == "puppy" else "Собака",
                    _clean(transfer.get("target_stream"), 50),
                    _clean(flow["teacher"], 200),
                    _clean(flow["teacher_code"], 100),
                    json.dumps(source, ensure_ascii=False),
                    _now(),
                    *args,
                ),
            )
            if source_row_deleted:
                shifted = await (
                    await db.execute(
                        "SELECT id,source_json FROM enrollments WHERE course_key=? AND stream=? AND status<>'removed'",
                        (
                            _clean(transfer.get("source_course_key"), 50),
                            _clean(transfer.get("source_stream"), 50),
                        ),
                    )
                ).fetchall()
                for item in shifted:
                    try:
                        shifted_source = json.loads(item["source_json"] or "{}")
                    except (TypeError, json.JSONDecodeError):
                        continue
                    shifted_row = int(shifted_source.get("row") or 0)
                    if shifted_row <= int(transfer.get("source_row") or 0):
                        continue
                    shifted_source["row"] = shifted_row - 1
                    await db.execute(
                        "UPDATE enrollments SET source_json=?,updated_at=? WHERE id=?",
                        (json.dumps(shifted_source, ensure_ascii=False), _now(), item["id"]),
                    )
        await db.commit()
    _clear_snapshot_cache()
    if int(cur.rowcount or 0) != 1:
        raise RuntimeError("Назначение ученика изменилось; обновите реестр")
    if not curator_only:
        return {"ok": True, "status": "updated", "row": int(target_row or 0)}
    sync_result = await _sync_registry(force=True)
    if not sync_result.get("ok"):
        raise RuntimeError(sync_result.get("error") or "Не удалось обновить Google-зеркало")
    return {"ok": True, "status": "mirrored", "sync": sync_result}


async def _assert_transfer_source_current(transfer: dict[str, Any]) -> None:
    enrollment_id = _clean(transfer.get("enrollment_id"), 100)
    if not enrollment_id:
        return
    async with _connect() as db:
        row = await (
            await db.execute(
                "SELECT course_key,stream,status,source_json FROM enrollments WHERE id=?",
                (enrollment_id,),
            )
        ).fetchone()
    if not row or row[2] == "removed":
        raise RuntimeError("Назначение ученика изменилось; обновите список")
    try:
        source = json.loads(row[3] or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        source = {}
    current_row = int(source.get("row") or 0) if isinstance(source, dict) else 0
    if (
        _clean(row[0], 50) != _clean(transfer.get("source_course_key"), 50)
        or _clean(row[1], 50) != _clean(transfer.get("source_stream"), 50)
        or (int(transfer.get("source_row") or 0) and current_row != int(transfer.get("source_row") or 0))
    ):
        raise RuntimeError("Назначение ученика изменилось; обновите список")


async def _run_transfer(transfer_id: str) -> None:
    async with _transfer_lock:
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            row = await (await db.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,))).fetchone()
        if not row:
            return
        transfer = dict(row)
        steps = _load_steps(transfer.get("steps_json"))
        await _save_transfer(transfer_id, status="running", steps=steps)
        student = _load_steps(transfer.get("student_json"))
        retry_state: dict[str, Any] | None = None
        action = _clean((steps.get("preview") or {}).get("action"), 50) or "transfer"
        try:
            fields_service = _module("getcourse-chat-fields", "service_transfer_write_getcourse")
            chat_repair = action == "chat_repair"
            chat_source_stream = _clean(
                (steps.get("preview") or {}).get("chat_source_stream") or transfer["source_stream"], 50
            )
            move_sheet_row = bool(((steps.get("preview") or {}).get("sheet") or {}).get("move", True))
            steps.pop("curator_order", None)
            steps.pop("delivery", None)
            retry_state = steps.pop("retry", None)
            if action in {"transfer", "registry_repair", "chat_repair", "curator_change"}:
                await _assert_transfer_source_current(transfer)
            if action == "chat_delivery":
                delivery = await _chat_delivery_view(student)
                if not delivery.get("can_send"):
                    raise RuntimeError(delivery.get("reason") or "Доставка чатов недоступна")
                messenger = _module("messenger-widget", "service_send_transfer_message")
                result = await messenger.service_send_transfer_message(
                    provider=delivery["provider"], recipient_id=delivery["recipient_id"],
                    content=delivery["content"], operation_id=f"streams:{transfer_id}:chats",
                )
                if not result.get("ok"):
                    raise RuntimeError(result.get("error") or "Ссылки на чаты не отправлены")
                steps["delivery"] = result
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            if action == "messenger_send":
                payload = (steps.get("preview") or {}).get("payload") or {}
                messenger = _module("messenger-widget", "service_streams_send")
                result = await messenger.service_streams_send(
                    channel_id=_clean(payload.get("channel_id"), 200),
                    transport=_clean(payload.get("transport"), 40),
                    provider=_clean(payload.get("provider"), 40),
                    chat_id=_clean(payload.get("chat_id"), 250),
                    phone=_clean(student.get("phone"), 100),
                    text=_clean(payload.get("text"), 4000),
                    subject=_clean(payload.get("subject"), 300),
                    operator_name=_clean(transfer.get("operator_name"), 200),
                    email=_clean(student.get("email"), 320),
                    gc_user_id=_clean(student.get("gc_user_id"), 100),
                    name=_clean(student.get("name"), 300),
                    attachment_url=_clean(payload.get("attachment_url"), 4000),
                    attachment_type=_clean(payload.get("attachment_type"), 100),
                    idempotency_key=f"streams:{transfer_id}",
                )
                if not result.get("ok"):
                    raise RuntimeError(result.get("error") or "Сообщение не отправлено")
                steps["delivery"] = {
                    "ok": True, "status": "sent",
                    "channel": _clean((result.get("channel") or {}).get("label"), 200),
                }
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            if action == "curator_change":
                if not (steps.get("getcourse") or {}).get("ok"):
                    result = await fields_service.service_transfer_write_curator(
                        email=transfer["email"],
                        gc_user_id=transfer["gc_user_id"],
                        order_id=_clean(student.get("order_id"), 100),
                        deal_number=_clean(student.get("deal_number") or student.get("order_id"), 100),
                        curator=transfer["curator"],
                    )
                    steps["getcourse"] = result
                    await _save_transfer(transfer_id, status="running", steps=steps, error=_clean(result.get("error"), 2000))
                    if not result.get("ok"):
                        raise RuntimeError(result.get("error") or "GetCourse не обновил куратора")
                if not steps.get("sheet"):
                    if int(transfer.get("source_row") or 0) > 0:
                        steps["sheet"] = await fields_service.service_transfer_update_student_curator(
                            email=transfer["email"],
                            source_course_key=transfer["source_course_key"],
                            source_stream=transfer["source_stream"],
                            source_row=int(transfer["source_row"]),
                            curator=transfer["curator"],
                        )
                    else:
                        steps["sheet"] = {"ok": True, "status": "not_linked"}
                    await _save_transfer(transfer_id, status="running", steps=steps)
                if not (steps.get("registry") or {}).get("ok"):
                    steps["registry"] = await _commit_registry_transfer(transfer, curator_only=True)
                    await _save_transfer(transfer_id, status="running", steps=steps)
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            if action == "registry_repair":
                if not (steps.get("sheet") or {}).get("ok"):
                    result = await fields_service.service_transfer_move_student(
                        email=transfer["email"],
                        source_course_key=transfer["source_course_key"],
                        source_stream=transfer["source_stream"],
                        source_row=int(transfer["source_row"]),
                        target_course_key=transfer["target_course_key"],
                        target_stream=transfer["target_stream"],
                        student=student,
                        move=move_sheet_row,
                    )
                    steps["sheet"] = result
                    await _save_transfer(transfer_id, status="running", steps=steps, error=_clean(result.get("error"), 2000))
                    if not result.get("ok"):
                        raise RuntimeError(result.get("error") or "Таблица не обновлена")
                if not (steps.get("registry") or {}).get("ok"):
                    steps["registry"] = await _commit_registry_transfer(
                        transfer,
                        target_row=int((steps.get("sheet") or {}).get("target_row") or 0),
                        source_row_deleted=bool((steps.get("sheet") or {}).get("source_row_deleted")),
                    )
                    await _save_transfer(transfer_id, status="running", steps=steps)
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            if chat_repair:
                target_flow = (steps.get("preview") or {}).get("target") or {}
                if not (steps.get("email") or {}).get("ok"):
                    if (steps.get("preview") or {}).get("delivery_already_sent"):
                        steps["email"] = {"ok": True, "status": "preserved_already_sent"}
                    else:
                        try:
                            onboarding = _module("getcourse-onboarding", "service_queue_flow_email")
                            steps["email"] = await onboarding.service_queue_flow_email(
                                gc_user_id=transfer["gc_user_id"],
                                email=transfer["email"],
                                order_id=_clean(student.get("order_id"), 100),
                                course_key=transfer["target_course_key"],
                                course=_clean(target_flow.get("course"), 100),
                                source_stream=chat_source_stream,
                                stream=transfer["target_stream"],
                                vk_link=_clean(target_flow.get("vk_link"), 2000),
                                tg_link=_clean(target_flow.get("tg_link"), 2000),
                            )
                        except Exception as exc:
                            steps["email"] = {"ok": False, "status": "failed", "error": _clean(exc, 1000)}
                steps["getcourse"] = {"ok": True, "status": "preserved"}
                steps["sheet"] = {"ok": True, "status": "preserved"}
                steps["registry"] = {"ok": True, "status": "preserved"}
                await _save_transfer(transfer_id, status="running", steps=steps)
            if not (steps.get("getcourse") or {}).get("ok"):
                result = await fields_service.service_transfer_write_getcourse(
                    email=transfer["email"],
                    gc_user_id=transfer["gc_user_id"],
                    order_id=_clean(student.get("order_id"), 100),
                    deal_number=_clean(student.get("deal_number") or student.get("order_id"), 100),
                    source_stream=transfer["source_stream"],
                    target_course_key=transfer["target_course_key"],
                    target_stream=transfer["target_stream"],
                    target_flow=(steps.get("preview") or {}).get("target") or {},
                )
                steps["getcourse"] = result
                await _save_transfer(transfer_id, status="running", steps=steps, error=_clean(result.get("error"), 2000))
                if not result.get("ok"):
                    raise RuntimeError(result.get("error") or "GetCourse не обновил поля")
            if not (steps.get("registry") or {}).get("ok"):
                if not (steps.get("sheet") or {}).get("ok"):
                    result = await fields_service.service_transfer_move_student(
                        email=transfer["email"],
                        source_course_key=transfer["source_course_key"],
                        source_stream=transfer["source_stream"],
                        source_row=int(transfer["source_row"]),
                        target_course_key=transfer["target_course_key"],
                        target_stream=transfer["target_stream"],
                        student=student,
                        move=move_sheet_row,
                    )
                    steps["sheet"] = result
                    await _save_transfer(transfer_id, status="running", steps=steps, error=_clean(result.get("error"), 2000))
                    if not result.get("ok"):
                        raise RuntimeError(result.get("error") or "Таблица не обновлена")
                steps["registry"] = await _commit_registry_transfer(
                    transfer,
                    target_row=int((steps.get("sheet") or {}).get("target_row") or 0),
                    source_row_deleted=bool((steps.get("sheet") or {}).get("source_row_deleted")),
                )
                await _save_transfer(transfer_id, status="running", steps=steps)
            if not move_sheet_row and not chat_repair:
                steps["chat_removal"] = {"ok": True, "status": "preserved", "items": {}}
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            preview_data = steps.get("preview") or {}
            vk_target = _clean(preview_data.get("vk_target"), 200)
            if not vk_target:
                identity_service = _module("messenger-widget", "service_transfer_recipients")
                recipients = await identity_service.service_transfer_recipients(
                    email=transfer["email"], gc_user_id=transfer["gc_user_id"], name=transfer["student_name"]
                )
                vk_target = _clean(recipients.get("vk"), 200)
            if not vk_target:
                steps["target_delivery"] = {"ok": False, "status": "no_vk_identity"}
                steps["chat_removal"] = {
                    "ok": False, "status": "preserved", "items": {
                        "vk": {"ok": False, "status": "preserved_no_identity"},
                        "telegram": {"ok": True, "status": "preserved"},
                    },
                }
                await _save_transfer(
                    transfer_id, status="warning", steps=steps,
                    error="VK-профиль не найден. Ученик оставлен в старом чате; ссылки отправляются по email.",
                )
                return
            chat_service = _module("course-chat-creator", "service_prepare_transfer_vk_member")
            target_delivery = steps.get("target_delivery") if isinstance(steps.get("target_delivery"), dict) else {}
            if not target_delivery:
                target_flow = (steps.get("preview") or {}).get("target") or {}
                if preview_data.get("delivery_already_sent"):
                    target_delivery = {
                        "ok": True,
                        "status": "invite_sent",
                        "delivery": "preserved_already_sent",
                        "target": vk_target,
                    }
                else:
                    target_delivery = await chat_service.service_prepare_transfer_vk_member(
                        target=vk_target,
                        student_name=transfer["student_name"],
                        source_stream=chat_source_stream,
                        target_course_key=transfer["target_course_key"],
                        target_stream=transfer["target_stream"],
                        idempotency_key=f"student-transfer:{transfer_id}:vk-target",
                        vk_link=_clean(target_flow.get("vk_link"), 2000),
                        tg_link=_clean(target_flow.get("tg_link"), 2000),
                    )
                steps["target_delivery"] = target_delivery
                await _save_transfer(transfer_id, status="running", steps=steps)
            joined = target_delivery.get("status") == "joined"
            if not joined and target_delivery.get("status") == "invite_sent":
                membership_service = _module("course-chat-creator", "service_transfer_target_membership")
                membership = await membership_service.service_transfer_target_membership(
                    platform="vk", target=vk_target,
                    course_key=transfer["target_course_key"], stream_number=transfer["target_stream"],
                )
                steps["target_membership"] = membership
                joined = bool(membership.get("ok") and membership.get("present"))
                if joined:
                    steps["target_delivery"] = {**target_delivery, "status": "joined", "joined_at": _now()}
            if not joined:
                if target_delivery.get("status") == "invite_sent":
                    steps, message = _schedule_target_join_check(
                        steps, delivery_already_sent=bool(preview_data.get("delivery_already_sent"))
                    )
                    steps["chat_removal"] = {
                        "ok": False, "status": "waiting_target_join", "items": {
                            "vk": {"ok": False, "status": "preserved_waiting_target_join"},
                            "telegram": {"ok": True, "status": "preserved"},
                        },
                    }
                    await _save_transfer(transfer_id, status="waiting", steps=steps, error=message)
                else:
                    steps["chat_removal"] = {
                        "ok": False, "status": "preserved", "items": {
                            "vk": {"ok": False, "status": "preserved_delivery_failed"},
                            "telegram": {"ok": True, "status": "preserved"},
                        },
                    }
                    await _save_transfer(
                        transfer_id, status="warning", steps=steps,
                        error="Не удалось добавить в новый VK-чат или доставить ссылку. Ученик оставлен в старом чате.",
                    )
                return
            steps.pop("join_wait", None)
            removal_service = _module("course-chat-creator", "service_remove_transfer_member")
            vk_removal = await removal_service.service_remove_transfer_member(
                platform="vk", target=vk_target,
                course_key=transfer["source_course_key"], stream_number=chat_source_stream,
                dry_run=False,
            )
            steps["chat_removal"] = {
                "ok": bool(vk_removal.get("ok")),
                "status": "completed" if vk_removal.get("ok") else "preserved",
                "items": {
                    "vk": vk_removal,
                    "telegram": {"ok": True, "status": "preserved"},
                },
            }
            await _save_transfer(
                transfer_id,
                status="completed" if vk_removal.get("ok") else "warning",
                steps=steps,
                error="" if vk_removal.get("ok") else "Вступление в новый VK-чат подтверждено, но старый чат удалить не удалось.",
            )
        except Exception as exc:
            if _is_google_rate_limit(exc) or (
                action in {"chat_delivery", "messenger_send"} and _is_transient_sheet_error(exc)
            ):
                if retry_state:
                    steps["retry"] = retry_state
                steps, message = _retry_transfer_steps(steps)
                await _save_transfer(transfer_id, status="waiting", steps=steps, error=message)
                if _logger:
                    _logger.warning("student transfer %s deferred after Google 429", transfer_id)
                return
            await _save_transfer(transfer_id, status="failed", steps=steps, error=str(exc))
            if _logger:
                _logger.exception("student transfer %s failed", transfer_id)


async def _run_flow_job(job_id: str) -> None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM flow_jobs WHERE id=?", (job_id,))).fetchone()
        if not row:
            return
        await db.execute("UPDATE flow_jobs SET status='running',updated_at=? WHERE id=?", (_now(), job_id))
        await db.commit()
    job = dict(row)
    result = _load_steps(job.get("result_json") or "{}")
    stages = result.get("stages") if isinstance(result.get("stages"), dict) else {}
    result["stages"] = stages

    async def save_stage(name: str, status: str, payload: Any | None = None) -> None:
        stages[name] = status
        if payload is not None:
            result[name] = payload
        async with _connect() as db:
            await db.execute(
                "UPDATE flow_jobs SET result_json=?,error='',updated_at=? WHERE id=?",
                (json.dumps(result, ensure_ascii=False), _now(), job_id),
            )
            await db.commit()

    try:
        setup = _module("course-chat-creator", "service_flow_setup")
        setup_data = await asyncio.to_thread(setup.service_flow_setup)
        teacher = next(
            (item for item in setup_data.get("teachers") or [] if int(item.get("id") or 0) == int(job["teacher_id"])),
            None,
        )
        if not teacher:
            raise RuntimeError("Выбранный куратор больше недоступен")
        curator_code = _teacher_code(teacher.get("name"))
        if not curator_code:
            raise RuntimeError("Для куратора не найден код GetCourse")

        fields = _module("getcourse-chat-fields", "service_create_registry_flow_sheet")
        sheet_result = result.get("sheet") if isinstance(result.get("sheet"), dict) else {}
        if stages.get("sheet") != "completed":
            await save_stage("sheet", "running")
            sheet_result = await fields.service_create_registry_flow_sheet(
                course_key=job["course_key"],
                stream=job["stream"],
                date_start=job["date_start"],
                curator=curator_code,
            )
        sheet_status = await fields.service_registry_flow_sheet_status(
            course_key=job["course_key"],
            stream=job["stream"],
            date_start=job["date_start"],
            expected_sheet_id=int(sheet_result.get("sheet_id") or 0),
        )
        if not sheet_status.get("ok"):
            await save_stage("sheet", "failed", {**sheet_result, **sheet_status, "verified": False})
            raise RuntimeError(sheet_status.get("error") or "Лист потока не прошёл проверку")
        sheet_result = {**sheet_result, **sheet_status, "verified": True}
        await save_stage("sheet", "completed", sheet_result)

        await save_stage("chats", "running")
        creator = _module("course-chat-creator", "service_create_flow_pair")
        create_result = await creator.service_create_flow_pair(
            course_key=job["course_key"],
            stream_number=job["stream"],
            date_start=job["date_start"],
            teacher_id=int(job["teacher_id"]),
        )
        await save_stage("chats", "completed", create_result)
        await _persist_created_flow(job, teacher, create_result)
        _clear_snapshot_cache()
        await save_stage("links", "completed", create_result.get("catalog") or {})
        await save_stage("sync", "running")
        sync_result = await _sync_registry(force=True)
        if not sync_result.get("ok"):
            raise RuntimeError(sync_result.get("error") or "Не удалось обновить список потоков")
        async with _connect() as db:
            await db.execute(
                "UPDATE flow_registry SET curator_source='streams',updated_at=? WHERE course_key=? AND stream=?",
                (_now(), job["course_key"], job["stream"]),
            )
            await db.commit()
        await save_stage("sync", "completed", sync_result)
        manual = {
            "required": True,
            "opened_as_community": False,
            "history_250_enabled": False,
            "members_admin_only": False,
            "system_notifications_disabled": False,
            "invite_link_saved": False,
        }
        status = "attention"
        async with _connect() as db:
            await db.execute(
                "UPDATE flow_jobs SET status=?,result_json=?,error=?,updated_at=? WHERE id=?",
                (
                    status,
                    json.dumps({**result, "create": create_result, "sync": sync_result, "manual": manual}, ensure_ascii=False),
                    "Создание ещё не закончено: нужно вручную настроить VK-чат.",
                    _now(),
                    job_id,
                ),
            )
            await db.commit()
    except Exception as exc:
        async with _connect() as db:
            await db.execute(
                "UPDATE flow_jobs SET status='failed',result_json=?,error=?,updated_at=? WHERE id=?",
                (json.dumps(result, ensure_ascii=False), _clean(exc, 2000), _now(), job_id),
            )
            await db.commit()
        if _logger:
            _logger.exception("flow creation %s failed", job_id)


async def _complete_ready_manual_flow_jobs() -> int:
    flows = {(item["course_key"], item["stream"]): item for item in await _flow_rows()}
    changed = 0
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM flow_jobs WHERE status='attention'")).fetchall()
        for row in rows:
            result = _load_steps(row["result_json"] or "{}")
            manual = result.get("manual") if isinstance(result.get("manual"), dict) else {}
            flow = flows.get((row["course_key"], row["stream"])) or {}
            if manual.get("required", True) or flow.get("status") != "ready":
                continue
            await db.execute(
                "UPDATE flow_jobs SET status='completed',error='',updated_at=? WHERE id=?",
                (_now(), row["id"]),
            )
            changed += 1
        await db.commit()
    return changed


def _is_transient_sheet_error(value: Any) -> bool:
    return bool(re.search(
        r"(?:\b429\b|too many requests|resource_exhausted|quota|timeout|timed out|temporar|временно|connection|502|503|504)",
        str(value or ""),
        re.I,
    ))


async def _claim_sheet_operation() -> str:
    async with _connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute(
                """
                SELECT id FROM transfers
                WHERE json_extract(steps_json,'$.preview.action') IN ('lesson_update','sheet_row')
                  AND (status='queued' OR (
                    status='waiting' AND COALESCE(
                      NULLIF(json_extract(steps_json,'$.retry.next_retry_at'),''),'')<=?
                  ))
                ORDER BY
                  CASE json_extract(steps_json,'$.preview.action') WHEN 'lesson_update' THEN 0 ELSE 1 END,
                  CASE status WHEN 'queued' THEN 0 ELSE 1 END,created_at,id LIMIT 1
                """,
                (_now(),),
            )
        ).fetchone()
        if not row:
            await db.commit()
            return ""
        operation_id = str(row["id"])
        cur = await db.execute(
            "UPDATE transfers SET status='running',error='',updated_at=? WHERE id=? AND status IN ('queued','waiting')",
            (_now(), operation_id),
        )
        await db.commit()
        return operation_id if int(cur.rowcount or 0) == 1 else ""


async def _run_sheet_operation(operation_id: str) -> None:
    async with _connect() as db:
        row = await (await db.execute("SELECT * FROM transfers WHERE id=?", (operation_id,))).fetchone()
    if not row or row["status"] != "running":
        return
    operation = dict(row)
    steps = _load_steps(operation.get("steps_json"))
    preview = steps.get("preview") if isinstance(steps.get("preview"), dict) else {}
    action = _clean(preview.get("action"), 50)
    retry_state = steps.pop("retry", None)
    try:
        if action == "sheet_row":
            student = await _widget_student_base(operation["enrollment_id"])
            operation_student = _load_steps(operation.get("student_json"))
            sheet_email = _clean(operation_student.get("sheet_email"), 320)
            if sheet_email:
                student["sheet_email"] = sheet_email
            try:
                student["tg_account"] = await _resolve_student_profile_link(student)
            except Exception as exc:
                if _logger:
                    _logger.info("Streams profile link enrichment skipped for %s: %s", operation_id, exc)
            fields = _module("getcourse-chat-fields", "service_registry_ensure_student")
            result = await fields.service_registry_ensure_student(
                course_key=student["course_key"], stream=student["stream"], student=student,
            )
            await _bind_sheet_row(
                operation["enrollment_id"], int(result.get("row") or 0), result.get("lesson_columns") or [],
            )
            steps["sheet"] = {
                "ok": True,
                "status": result.get("status") or "created",
                "row": int(result.get("row") or 0),
                "sheet_title": _clean(result.get("sheet_title"), 300),
            }
            steps["streams"] = {"ok": True, "status": "linked"}
        elif action == "lesson_update":
            lesson = preview.get("lesson") if isinstance(preview.get("lesson"), dict) else {}
            student_context = _load_steps(operation.get("student_json"))
            fields = _module("getcourse-chat-fields", "service_registry_write_lesson")
            result = await fields.service_registry_write_lesson(
                course_key=operation["source_course_key"], stream=operation["source_stream"],
                email=operation["email"], source_row=int(operation["source_row"]),
                lesson_key=_clean(lesson.get("key"), 5).upper(), value=bool(lesson.get("value")),
                expected_value=bool(lesson.get("expected_value")),
                sheet_title=_clean(student_context.get("sheet_title"), 300),
            )
            steps["sheet"] = {
                "ok": True, "status": result.get("status") or "updated",
                "row": int(result.get("row") or operation["source_row"]),
            }
            numeric = 1 if result.get("value") else 0
            async with _connect() as db:
                await db.execute(
                    """
                    INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at)
                    VALUES(?,?,?,?,?,0,?)
                    ON CONFLICT(enrollment_id,lesson_key) DO UPDATE SET
                      value=excluded.value,sheet_value=excluded.sheet_value,dirty=0,label=excluded.label,updated_at=excluded.updated_at
                    """,
                    (
                        operation["enrollment_id"], _clean(lesson.get("key"), 5).upper(),
                        _clean(lesson.get("label"), 200), numeric, numeric, _now(),
                    ),
                )
                await db.commit()
            if int(result.get("row") or 0) != int(operation["source_row"]):
                await _bind_sheet_row(operation["enrollment_id"], int(result.get("row") or 0), [])
            steps["streams"] = {"ok": True, "status": "updated", "value": bool(numeric)}
            _clear_snapshot_cache()
        else:
            raise RuntimeError("Неизвестная операция Google-таблицы")
        await _save_transfer(operation_id, status="completed", steps=steps)
    except Exception as exc:
        message = _clean(exc, 2000)
        if _is_transient_sheet_error(exc):
            if retry_state:
                steps["retry"] = retry_state
            steps, retry_message = _retry_transfer_steps(steps)
            steps["sheet"] = {"ok": False, "status": "waiting", "error": message}
            await _save_transfer(operation_id, status="waiting", steps=steps, error=retry_message)
            return
        conflict = bool(re.search(r"(?:таблица уже изменена|строка ученика изменилась|обновите данные)", message, re.I))
        steps["sheet"] = {"ok": False, "status": "conflict" if conflict else "failed", "error": message}
        await _save_transfer(operation_id, status="warning" if conflict else "failed", steps=steps, error=message)
        if conflict:
            _schedule_registry_sync()
        if _logger:
            _logger.warning("Streams sheet operation %s failed: %s", operation_id, message)


async def _sheet_operation_loop() -> None:
    await asyncio.sleep(1)
    while True:
        try:
            operation_id = await _claim_sheet_operation()
            if operation_id:
                await _run_sheet_operation(operation_id)
                continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("Streams sheet operation worker failed: %s", exc)
        await asyncio.sleep(1)


async def _refund_sync_loop() -> None:
    """Consume the refund ledger independently from slow sheet synchronization."""
    await asyncio.sleep(4)
    while True:
        try:
            result = await _sync_refunds()
            if int(result.get("moved") or 0):
                _clear_snapshot_cache()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("Refund synchronization worker deferred: %s", exc)
        await asyncio.sleep(10)


async def _worker_loop() -> None:
    await asyncio.sleep(2)
    while True:
        delay = 2
        try:
            async with _connect() as db:
                flow_job = await (
                    await db.execute("SELECT id FROM flow_jobs WHERE status='queued' ORDER BY created_at,id LIMIT 1")
                ).fetchone()
                row = await (
                    await db.execute(
                        """
                        SELECT id FROM transfers
                        WHERE COALESCE(json_extract(steps_json,'$.preview.action'),'transfer') NOT IN ('lesson_update','sheet_row')
                          AND (status='queued' OR (
                            status='waiting' AND COALESCE(
                                NULLIF(json_extract(steps_json,'$.join_wait.next_retry_at'),''),
                                NULLIF(json_extract(steps_json,'$.retry.next_retry_at'),''),
                                ''
                            )<=?
                        ))
                        ORDER BY CASE status WHEN 'queued' THEN 0 ELSE 1 END,created_at,id LIMIT 1
                        """,
                        (_now(),),
                    )
                ).fetchone()
            if flow_job:
                await _run_flow_job(str(flow_job[0]))
                continue
            if row:
                await _run_transfer(str(row[0]))
                continue
            await _sync_roster_delta()
            sync_result = await _sync_registry()
            if sync_result.get("status") == "completed":
                await _snapshot()
            if sync_result.get("ok"):
                await _complete_ready_manual_flow_jobs()
            if not sync_result.get("ok"):
                delay = 300 if "429" in str(sync_result.get("error") or "") else 60
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("student-transfer worker failed: %s", exc)
        await asyncio.sleep(delay)


def _transfer_view(row: dict[str, Any], *, compact: bool = False) -> dict[str, Any]:
    item = dict(row)
    item["student"] = _load_steps(item.pop("student_json", "{}"))
    item["steps"] = _load_steps(item.pop("steps_json", "{}"))
    preview = item["steps"].get("preview") if isinstance(item["steps"].get("preview"), dict) else {}
    item["action"] = _clean(preview.get("action"), 50) or "transfer"
    item["scheduled_at"] = _clean(preview.get("scheduled_at"), 100)
    lesson = preview.get("lesson") if isinstance(preview.get("lesson"), dict) else {}
    item["lesson_key"] = _clean(lesson.get("key"), 5)
    item["lesson_label"] = _clean(lesson.get("label"), 200)
    item["requested_value"] = bool(lesson.get("value")) if lesson else None
    if compact:
        item.pop("student", None)
        item["steps"] = {
            key: {
                field: value.get(field)
                for field in ("status", "ok", "error", "next_retry_at")
                if field in value
            }
            for key, value in item["steps"].items()
            if key != "preview" and isinstance(value, dict)
        }
    return item


async def _queue_operation(preview_data: dict[str, Any], operator: dict[str, Any]) -> dict[str, Any]:
    preview_data = {**preview_data}
    preview_data.setdefault("scheduled_at", _sheet_operation_time(300))
    source = preview_data["source"]
    target = preview_data["target"]
    action = _clean(preview_data.get("action"), 50) or "transfer"
    same_flow = action == "curator_change"
    target_course_key = source["course_key"] if same_flow else target["course_key"]
    target_stream = source["stream"] if same_flow else target["stream"]
    # ponytail: one process-wide lock is enough at current operator volume; use a DB uniqueness key if workers are split.
    async with _operation_queue_lock:
        async with _connect() as db:
            db.row_factory = aiosqlite.Row
            active = await (
                await db.execute(
                    """
                    SELECT id,status,steps_json FROM transfers
                    WHERE status IN ('queued','running','waiting') AND lower(email)=lower(?)
                      AND source_course_key=? AND source_stream=?
                      AND target_course_key=? AND target_stream=? AND curator=?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (
                        source["email"], source["course_key"], source["stream"],
                        target_course_key, target_stream, target["curator"],
                    ),
                )
            ).fetchone()
            if active:
                existing_preview = (_load_steps(active["steps_json"]).get("preview") or {})
                return {
                    "ok": True, "id": active["id"], "status": active["status"],
                    "action": action, "existing": True,
                    "scheduled_at": existing_preview.get("scheduled_at") or preview_data["scheduled_at"],
                }
            transfer_id = uuid.uuid4().hex
            now = _now()
            await db.execute(
                """
                INSERT INTO transfers(
                    id,enrollment_id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                    target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                    student_json,steps_json,error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    transfer_id,
                    source.get("enrollment_id") or "",
                    "queued",
                    source["email"],
                    source["gc_user_id"],
                    source["name"],
                    source["course_key"],
                    source["stream"],
                    source["row"],
                    target_course_key,
                    target_stream,
                    target["curator"],
                    target["offer_id"],
                    operator["id"],
                    operator["display_name"] or operator["login"],
                    json.dumps(source, ensure_ascii=False),
                    json.dumps({"preview": preview_data}, ensure_ascii=False),
                    "",
                    now,
                    now,
                ),
            )
            await db.commit()
    return {
        "ok": True, "id": transfer_id, "status": "queued", "action": action,
        "scheduled_at": preview_data["scheduled_at"],
    }


@router.get("/app")
@router.get("/app/")
@router.get("/")
async def fullscreen_app():
    html = (_must_module_dir() / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    script_hashes = []
    for script in re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE):
        digest = base64.b64encode(hashlib.sha256(script.encode("utf-8")).digest()).decode("ascii")
        script_hashes.append(f"'sha256-{digest}'")
    csp = "; ".join((
        "default-src 'self'",
        "base-uri 'none'",
        "object-src 'none'",
        "frame-ancestors 'self'",
        f"script-src 'self' {' '.join(script_hashes)}",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: https://*.userapi.com https://*.vkuserphoto.ru",
        "connect-src 'self'",
        "form-action 'self'",
    ))
    return HTMLResponse(
        html,
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": csp,
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
        },
    )


@router.post("/login")
async def login(data: LoginIn, request: Request):
    _require_same_origin(request)
    enforce_rate_limit(request, "student-transfer-login", limit=20, window_seconds=300)
    login_key = _norm(data.login)
    password = str(data.password or "")
    if not login_key:
        raise HTTPException(400, "Введите имя и фамилию")
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute("SELECT * FROM operators WHERE login_key=? AND active=1", (login_key,))
        ).fetchone()
    operator = dict(row) if row else None
    password_hash = str(operator.get("password_hash") or "") if operator else ""
    verified = await asyncio.to_thread(
        _password_matches, password, password_hash or _dummy_password_hash,
    )
    if not operator or (password_hash and not verified) or (not password_hash and password):
        raise HTTPException(401, "Неверное имя или пароль")
    token = secrets.token_urlsafe(40)
    try:
        await _create_session(token, operator["id"])
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).casefold():
            raise HTTPException(503, "Streams синхронизирует данные. Повторите вход через несколько секунд") from exc
        raise
    response = JSONResponse({"ok": True, "display_name": operator["display_name"] or operator["login"]})
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=SESSION_TTL_DAYS * 86400,
        path=_cookie_path(request),
    )
    return response


@router.post("/logout")
async def logout(request: Request):
    _require_same_origin(request)
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        _schedule_session_revocation(token)
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE, path=_cookie_path(request))
    return response


@router.get("/me")
async def me(request: Request):
    operator = None
    try:
        operator = await _require_operator(request)
    except HTTPException:
        pass
    return {
        "authenticated": bool(operator),
        "operator": operator and {
            "id": operator["id"],
            "login": operator["login"],
            "display_name": operator["display_name"] or operator["login"],
        },
        "nexus_admin": False,
    }


@router.get("/catalog")
async def catalog(request: Request, refresh: str = "0"):
    await _require_operator(request)
    if refresh == "1":
        _schedule_registry_sync()
    snapshot = await _snapshot(refresh=False)
    items = []
    for flow in snapshot.get("items") or []:
        course_key, stream = _flow_key(flow)
        if not stream:
            continue
        items.append(
            {
                "course_key": course_key,
                "course": _clean(flow.get("course"), 100),
                "stream": stream,
                "date_start": _clean(flow.get("date_start"), 100),
                "curator": _clean(flow.get("curator_value"), 100),
                "teacher": _clean(flow.get("teacher"), 200),
                "teacher_id": int(flow.get("teacher_id") or 0),
                "offer_id": int(flow.get("offer_id") or CURATOR_OFFERS.get(_clean(flow.get("curator_value"), 100), 0)),
                "status": _clean(flow.get("status"), 50),
                "students_count": int(flow.get("students_count") or 0),
                "vk_link": _clean(flow.get("vk_link"), 2000),
                "tg_link": _clean(flow.get("tg_link"), 2000),
                "vk_admin_url": _clean(flow.get("vk_admin_url"), 2000),
            }
        )
    return {
        "items": items,
        "updated_at": snapshot.get("updated_at") or snapshot.get("cache_updated_at") or "",
        "refresh_queued": refresh == "1",
        "scheduled_at": _sheet_operation_time(300) if refresh == "1" else "",
    }


def _student_list_index(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Flatten the cached registry once instead of rebuilding it per filter keystroke."""
    if _student_list_index_cache.get("source") is snapshot:
        return _student_list_index_cache
    all_items: list[dict[str, Any]] = []
    by_course: dict[str, list[dict[str, Any]]] = {}
    by_stream: dict[str, list[dict[str, Any]]] = {}
    by_flow: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for flow in snapshot.get("items") or []:
        flow_course = _clean(flow.get("course_key"), 50)
        flow_stream = _clean(flow.get("stream"), 50)
        for student in flow.get("students") or []:
            item = _student_result(flow, student)
            all_items.append(item)
            by_course.setdefault(flow_course, []).append(item)
            by_stream.setdefault(flow_stream, []).append(item)
            by_flow.setdefault((flow_course, flow_stream), []).append(item)
    _student_list_index_cache.update(
        source=snapshot, all=all_items, by_course=by_course,
        by_stream=by_stream, by_flow=by_flow,
    )
    return _student_list_index_cache


def _student_list_sort_value(item: dict[str, Any], key: str) -> Any:
    if key == "remaining_amount":
        return float(item.get("remaining_amount") or 0)
    if key == "stream":
        value = _clean(item.get("stream_display") or item.get("stream"), 100)
        match = re.search(r"\d+", value)
        return (int(match.group()) if match else -1, _norm(value))
    values = {
        "name": item.get("name"),
        "email": item.get("email"),
        "course": item.get("course_display") or item.get("course"),
        "curator": item.get("curator_name") or item.get("curator"),
        "manager": item.get("manager_name"),
    }
    return _norm(values.get(key))


@router.get("/students")
async def students(
    request: Request,
    q: str = "",
    course_key: str = "",
    stream: str = "",
    tariff: str = "",
    curator: str = "",
    offset: int = 0,
    limit: int = 100,
    refresh: str = "0",
    sort: str = "",
    direction: str = "",
):
    await _require_operator(request)
    query = _norm(q)
    query_phone = _phone_search_key(q)
    phone_lookup = _is_phone_search(q)
    numeric_queries = set(re.findall(r"\d{5,20}", _clean(q, 1000)))
    if refresh == "1":
        _clear_student_enrichment_cache()
        _schedule_registry_sync()
    snapshot = await _snapshot(refresh=False)
    index = _student_list_index(snapshot)
    selected_course = _clean(course_key, 50)
    selected_stream = _clean(stream, 50)
    if selected_course and selected_stream:
        candidates = index["by_flow"].get((selected_course, selected_stream), [])
    elif selected_course:
        candidates = index["by_course"].get(selected_course, [])
    elif selected_stream:
        candidates = index["by_stream"].get(selected_stream, [])
    else:
        candidates = index["all"]
    # Exact phone search may need the local order identity index because older
    # sheet rows do not contain a phone. Normal list loading never pays this cost.
    if query and phone_lookup:
        await _enrich_order_identities(candidates)
    base: list[dict[str, Any]] = []
    for item in candidates:
        haystack = _norm(" ".join(
            _clean(item.get(key), 500)
            for key in ("name", "email", "phone", "gc_user_id", "order_id", "deal_number", "enrollment_id")
        ))
        phone_match = bool(phone_lookup and query_phone in _phone_search_key(item.get("phone")))
        numeric_match = bool(numeric_queries and any(value in haystack for value in numeric_queries))
        if query and query not in haystack and not phone_match and not numeric_match:
            continue
        base.append(item)
    tariffs = {key: 0 for key in ("standard", "premium", "vip", "other")}
    vip_by_curator = {key: 0 for key in CURATOR_NAMES}
    for item in base:
        tariff_key = _tariff_key(item.get("tariff"))
        tariffs[tariff_key] += 1
        if tariff_key == "vip" and item.get("curator") in vip_by_curator:
            vip_by_curator[item["curator"]] += 1
    selected_tariff = _clean(tariff, 30).lower()
    selected_curator = _clean(curator, 100)
    found = [
        item for item in base
        if (not selected_tariff or _tariff_key(item.get("tariff")) == selected_tariff)
        and (not selected_curator or item.get("curator") == selected_curator)
    ]
    sort_key = _clean(sort, 40)
    sort_direction = _clean(direction, 10).lower()
    if sort_key in {"name", "email", "course", "stream", "curator", "manager", "remaining_amount"}:
        found.sort(
            key=lambda item: _student_list_sort_value(item, sort_key),
            reverse=sort_direction == "desc",
        )
    start = max(0, min(100000, int(offset)))
    page_size = max(1, min(250, int(limit)))
    page = [dict(item) for item in found[start : start + page_size]]
    enrichment, _, _ = await asyncio.gather(
        _enrich_student_page(page),
        _enrich_student_notes(page),
        _enrich_student_financials(page),
    )
    return {
        "items": page,
        "total": len(found),
        "offset": start,
        "limit": page_size,
        "curators": [{"value": key, "name": CURATOR_NAMES[key], "offer_id": value} for key, value in CURATOR_OFFERS.items()],
        "summary": {
            "total": len(base),
            "tariffs": tariffs,
            "vip_by_curator": [
                {"value": key, "name": CURATOR_NAMES[key], "count": vip_by_curator[key]}
                for key in CURATOR_NAMES
            ],
        },
        "enrichment_pending": enrichment["pending"],
        "enrichment_incomplete": enrichment["incomplete"],
        "updated_at": snapshot.get("updated_at") or snapshot.get("cache_updated_at") or "",
        "refresh_queued": refresh == "1",
        "scheduled_at": _sheet_operation_time(300) if refresh == "1" else "",
    }


@router.get("/students/{enrollment_id}")
async def student(enrollment_id: str, request: Request):
    await _require_operator(request)
    return {
        "item": await _student_by_id(enrollment_id),
        "curators": [{"value": key, "name": CURATOR_NAMES[key], "offer_id": value} for key, value in CURATOR_OFFERS.items()],
    }


@router.get("/students/{enrollment_id}/external")
async def student_external(enrollment_id: str, request: Request):
    await _require_operator(request)
    return {"item": await _student_by_id(enrollment_id, resolve_external=True)}


@router.get("/refunds")
async def refunds(request: Request, q: str = "", offset: int = 0, limit: int = 100):
    await _require_operator(request)
    query = _norm(q)
    flows = {(row["course_key"], row["stream"]): row for row in await _flow_rows()}
    items: list[dict[str, Any]] = []
    for stored in await _refund_enrollment_rows():
        try:
            source = json.loads(stored.get("source_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        flow = flows.get((stored["course_key"], stored["stream"])) or {
            "course_key": stored["course_key"], "course": stored["course"], "stream": stored["stream"],
            "teacher": stored["teacher"], "teacher_code": stored["teacher_code"],
        }
        item = _student_result(
            {**flow, "curator_value": stored["teacher_code"] or flow.get("teacher_code")},
            {
                **source, **stored, "enrollment_id": stored["id"],
                "phone": _clean(source.get("phone") or source.get("user_phone"), 100),
                "course_assignments": source.get("course_assignments") or [],
            },
        )
        refund = source.get("refund") if isinstance(source.get("refund"), dict) else {}
        item["refunded"] = True
        item["refunded_at"] = _clean(refund.get("received_at"), 100)
        item["total_amount"] = float(refund.get("total_amount") or item.get("total_amount") or 0)
        item["remaining_amount"] = float(refund.get("remaining_amount") or item.get("remaining_amount") or 0)
        item["refund_amount"] = float(refund.get("refund_amount") or item.get("refund_amount") or 0)
        haystack = _norm(" ".join(_clean(item.get(key), 500) for key in (
            "name", "email", "phone", "gc_user_id", "order_id", "deal_number",
        )))
        if not query or query in haystack:
            items.append(item)
    start = max(0, min(100000, int(offset)))
    page_size = max(1, min(250, int(limit)))
    page = items[start:start + page_size]
    enrichment, _, _ = await asyncio.gather(
        _enrich_student_page(page),
        _enrich_student_notes(page),
        _enrich_student_financials(page),
    )
    return {
        "items": page,
        "total": len(items),
        "offset": start,
        "limit": page_size,
        "enrichment_pending": enrichment["pending"],
        "enrichment_incomplete": enrichment["incomplete"],
    }


async def _refund_chat_identities(item: dict[str, Any]) -> dict[str, str]:
    await _enrich_order_identities([item])
    messenger = _module("messenger-widget", "service_transfer_recipients")
    resolved = await messenger.service_transfer_recipients(
        email=_clean(item.get("email"), 320),
        gc_user_id=_clean(item.get("gc_user_id"), 100),
        phone=_clean(item.get("phone"), 100),
        name=_clean(item.get("name"), 300),
    )
    identities = {
        "vk": _clean(resolved.get("vk"), 30),
        "telegram": _clean(resolved.get("telegram"), 30),
    }
    return {
        platform: value for platform, value in identities.items()
        if value.isdigit() and int(value) > 0
    }


@router.get("/students/{enrollment_id}/chat-removal")
async def preview_refund_chat_removal(enrollment_id: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-chat-removal-preview", limit=10, window_seconds=120)
    item = await _student_by_id(enrollment_id)
    if not item.get("refunded"):
        raise HTTPException(409, "Кнопка доступна только для полного возврата")
    identities = await _refund_chat_identities(item)
    if not identities:
        raise HTTPException(409, "Не найден точный числовой ID VK или Telegram. Удаление не запущено")
    moderator = _module("chat-moderators", "service_refund_chat_membership")
    platforms = await asyncio.gather(*(
        moderator.service_refund_chat_membership(platform=platform, user_id=user_id, dry_run=True)
        for platform, user_id in identities.items()
    ), return_exceptions=True)
    results: dict[str, Any] = {}
    for (platform, _user_id), result in zip(identities.items(), platforms):
        if isinstance(result, Exception):
            results[platform] = {"ok": False, "error": str(result), "found": [], "admin_skipped": []}
        else:
            results[platform] = result
    preview_id = secrets.token_urlsafe(32)
    await _meta_set(f"refund_removal_preview:{preview_id}", json.dumps({
        "enrollment_id": item["enrollment_id"], "created_at": _now(),
        "identities": identities, "results": results,
    }, ensure_ascii=False))
    return {
        "ok": True, "preview_id": preview_id, "identities": identities,
        "platforms": results,
        "found": sum(len(value.get("found") or []) for value in results.values()),
        "admin_skipped": sum(len(value.get("admin_skipped") or []) for value in results.values()),
    }


@router.post("/students/{enrollment_id}/chat-removal")
async def apply_refund_chat_removal(enrollment_id: str, data: ChatRemovalIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-chat-removal-apply", limit=5, window_seconds=300)
    raw = await _meta_get(f"refund_removal_preview:{data.preview_id}")
    try:
        preview = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        preview = {}
    if _clean(preview.get("enrollment_id"), 100) != _clean(enrollment_id, 100):
        raise HTTPException(409, "Проверка устарела. Нажмите кнопку ещё раз")
    try:
        created_at = datetime.fromisoformat(_clean(preview.get("created_at"), 100).replace("Z", "+00:00"))
        age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    except (TypeError, ValueError):
        age_seconds = 9999
    if age_seconds > 600:
        raise HTTPException(409, "Проверка старше 10 минут. Нажмите кнопку ещё раз")
    item = await _student_by_id(enrollment_id)
    if not item.get("refunded"):
        raise HTTPException(409, "Возврат уже не полный. Удаление отменено")
    identities = await _refund_chat_identities(item)
    expected = {str(key): str(value) for key, value in (preview.get("identities") or {}).items()}
    if identities != expected:
        raise HTTPException(409, "Связанные ID клиента изменились. Удаление отменено; проверьте карточку")
    moderator = _module("chat-moderators", "service_refund_chat_membership")
    applied = await asyncio.gather(*(
        moderator.service_refund_chat_membership(platform=platform, user_id=user_id, dry_run=False)
        for platform, user_id in identities.items()
    ), return_exceptions=True)
    results: dict[str, Any] = {}
    for (platform, _user_id), result in zip(identities.items(), applied):
        results[platform] = ({"ok": False, "error": str(result), "removed": []}
                             if isinstance(result, Exception) else result)
    await _meta_set(f"refund_removal_preview:{data.preview_id}", "")
    removed = sum(len(value.get("removed") or []) for value in results.values())
    errors = [value.get("error") for value in results.values() if value.get("error")]
    if _logger:
        _logger.info(
            "refund chat removal enrollment_id=%s operator=%s identities=%s removed=%s",
            enrollment_id, _clean(operator.get("display_name") or operator.get("login"), 200),
            identities, removed,
        )
    return {"ok": not errors, "removed": removed, "platforms": results, "errors": errors}


@router.post("/students/{enrollment_id}/refund-status")
async def change_refund_status(
    enrollment_id: str, data: RefundStatusIn, request: Request,
):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-refund-status", limit=20, window_seconds=120)
    clean_id = _clean(enrollment_id, 100)
    now = _now()
    operator_id = int(operator.get("id") or 0)
    operator_name = _clean(operator.get("display_name") or operator.get("login"), 200) or "Streams"
    reason = data.reason.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "").strip()
    async with _connect(timeout=5) as db:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute(
                "SELECT * FROM enrollments WHERE id=? AND status<>'removed' LIMIT 1",
                (clean_id,),
            )
        ).fetchone()
        if not row:
            raise HTTPException(404, "Ученик не найден")
        stored = dict(row)
        current_refunded = stored.get("status") == "refunded"
        if current_refunded != bool(data.expected_refunded):
            raise HTTPException(409, "Статус уже изменился. Обновите карточку")
        if current_refunded == bool(data.refunded):
            raise HTTPException(409, "Такой статус уже установлен")
        target_override = "refunded" if data.refunded else "active"
        target_status = "refunded" if data.refunded else (
            "assigned" if _clean(stored.get("stream"), 50) else "pending"
        )
        await db.execute(
            """INSERT INTO enrollment_status_overrides(
                   enrollment_id,target_status,previous_status,reason,
                   operator_id,operator_name,created_at
               ) VALUES(?,?,?,?,?,?,?)""",
            (
                clean_id, target_override, _clean(stored.get("status"), 30), reason,
                operator_id, operator_name, now,
            ),
        )
        await db.execute(
            "UPDATE enrollments SET status=?,updated_at=? WHERE id=?",
            (target_status, now, clean_id),
        )
        try:
            source = json.loads(stored.get("source_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            source = {}
        if not isinstance(source, dict):
            source = {}
        operation_id = uuid.uuid4().hex
        preview = {
            "action": "refund_status_change",
            "source": {
                "course_key": _clean(stored.get("course_key"), 50),
                "stream": _clean(stored.get("stream"), 50),
                "row": int(source.get("row") or 0),
                "email": _clean(stored.get("email"), 320),
                "refunded": current_refunded,
            },
            "target": {"refunded": bool(data.refunded)},
            "reason": reason,
        }
        await db.execute(
            """INSERT INTO transfers(
                   id,enrollment_id,status,email,gc_user_id,student_name,
                   source_course_key,source_stream,source_row,target_course_key,target_stream,
                   curator,offer_id,operator_id,operator_name,student_json,steps_json,
                   error,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                operation_id, clean_id, "completed", _clean(stored.get("email"), 320),
                _clean(stored.get("gc_user_id"), 100), _clean(stored.get("name"), 300),
                _clean(stored.get("course_key"), 50), _clean(stored.get("stream"), 50),
                int(source.get("row") or 0), _clean(stored.get("course_key"), 50),
                _clean(stored.get("stream"), 50), _clean(stored.get("teacher_code"), 100),
                0, operator_id, operator_name, json.dumps({**source, **stored}, ensure_ascii=False),
                json.dumps({"preview": preview, "refund_status": {"ok": True}}, ensure_ascii=False),
                "", now, now,
            ),
        )
        await db.commit()
    _clear_snapshot_cache()
    _clear_student_enrichment_cache()
    if _logger:
        _logger.info(
            "manual refund status enrollment_id=%s refunded=%s operator=%s",
            clean_id, bool(data.refunded), operator_name,
        )
    return {"ok": True, "operation_id": operation_id, "item": await _student_by_id(clean_id)}


@router.put("/students/{enrollment_id}/note")
async def save_student_note(enrollment_id: str, data: StudentNoteIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-note-save", limit=30, window_seconds=120)
    item = await _student_by_id(enrollment_id)
    note = data.note.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "").strip()
    now = _now()
    updated_by = _clean(operator.get("display_name") or operator.get("login"), 200)
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO student_notes(enrollment_id,note,updated_by,updated_at) VALUES(?,?,?,?)
            ON CONFLICT(enrollment_id) DO UPDATE SET
                note=excluded.note,updated_by=excluded.updated_by,updated_at=excluded.updated_at
            """,
            (item["enrollment_id"], note, updated_by, now),
        )
        await db.commit()
    return {"ok": True, "note": note, "updated_by": updated_by, "updated_at": now}


@router.get("/students/{enrollment_id}/messenger")
async def student_messenger(
    enrollment_id: str, request: Request, history: str = "1", channel_id: str = "",
):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-messenger-read", limit=60, window_seconds=120)
    item = await _student_by_id(enrollment_id)
    messenger = _module("messenger-widget", "service_streams_conversations")
    return await messenger.service_streams_conversations(
        email=item.get("email") or "",
        gc_user_id=item.get("gc_user_id") or "",
        name=item.get("name") or "",
        phone=item.get("phone") or "",
        operator_name=operator.get("display_name") or operator.get("login") or "",
        include_history=history != "0",
        history_channel_id=_clean(channel_id, 200),
    )


@router.post("/students/{enrollment_id}/messenger/send")
async def student_messenger_send(enrollment_id: str, data: MessengerSendIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-messenger-send", limit=60, window_seconds=300)
    item = await _student_by_id(enrollment_id)
    if not data.text.strip() and not data.attachment_url.strip():
        raise HTTPException(400, "Введите сообщение или добавьте вложение")
    if data.attachment_url and not data.attachment_url.startswith("https://"):
        raise HTTPException(400, "Вложение должно иметь HTTPS-ссылку")
    return await _queue_card_command(
        student=item, action="messenger_send", operator=operator,
        request_id=data.request_id or uuid.uuid4().hex,
        payload=data.model_dump(exclude={"request_id"}),
    )


@router.post("/students/{enrollment_id}/messenger/template-preview")
async def student_messenger_template_preview(
    enrollment_id: str, data: MessengerTemplatePreviewIn, request: Request,
):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-messenger-template-preview", limit=120, window_seconds=300)
    item = await _student_by_id(enrollment_id)
    messenger = _module("messenger-widget", "service_streams_template_preview")
    try:
        return await messenger.service_streams_template_preview(
            template_id=data.template_id, body=data.body,
            email=item.get("email") or "", gc_user_id=item.get("gc_user_id") or "",
            name=item.get("name") or "", phone=item.get("phone") or "",
            operator_name=operator.get("display_name") or operator.get("login") or "",
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/students/{enrollment_id}/messenger/template-favorite")
async def student_messenger_template_favorite(
    enrollment_id: str, data: MessengerTemplateFavoriteIn, request: Request,
):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-messenger-template-favorite", limit=60, window_seconds=300)
    await _student_by_id(enrollment_id)
    messenger = _module("messenger-widget", "service_streams_template_favorite")
    try:
        return await messenger.service_streams_template_favorite(
            template_id=data.template_id, favorite=data.favorite,
            operator_name=operator.get("display_name") or operator.get("login") or "",
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/students/{enrollment_id}/chat-delivery")
async def student_chat_delivery(enrollment_id: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-chat-delivery-read", limit=30, window_seconds=120)
    result = await _chat_delivery_view(await _student_by_id(enrollment_id), resolve_target=False)
    result.pop("recipient_id", None)
    result.pop("content", None)
    return result


@router.post("/students/{enrollment_id}/chat-delivery")
async def send_student_chats(enrollment_id: str, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-chat-delivery-send", limit=4, window_seconds=300)
    item = await _student_by_id(enrollment_id)
    return await _queue_card_command(
        student=item, action="chat_delivery", operator=operator,
        request_id=f"chats:{item['enrollment_id']}:{int(time.time() // 30)}",
    )


@router.get("/students/{enrollment_id}/access")
async def student_access(enrollment_id: str, request: Request, live: str = "0"):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-read", limit=40, window_seconds=120)
    # A UI refresh is a command to the background worker, never a browser-held
    # GetCourse request. Return the usable cached view immediately with ETA.
    current = await _student_access_view(enrollment_id, live=False)
    if live == "1":
        current = await _queue_access_refresh(enrollment_id, current)
    return current


TESTDRIVE_PAGE_ORIGIN = "https://club.sobakovod.pro"
TESTDRIVE_CHECK_URL = "https://junior.sobakovod.pro/streams/testdrive/check"
TESTDRIVE_CONFIRM_URL = "https://junior.sobakovod.pro/streams/testdrive/confirm"


def _testdrive_cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": TESTDRIVE_PAGE_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "600",
        "Vary": "Origin",
    }


def _require_testdrive_origin(request: Request) -> None:
    if _clean(request.headers.get("origin"), 300) != TESTDRIVE_PAGE_ORIGIN:
        raise HTTPException(403, "Источник запроса не разрешён")


@router.options("/testdrive/check")
async def testdrive_check_options(request: Request):
    _require_testdrive_origin(request)
    return Response(status_code=204, headers=_testdrive_cors_headers())


@router.post("/testdrive/check")
async def testdrive_check(data: TestDriveCheckIn, request: Request):
    """Public preflight; its short-lived browser link never reserves an identity."""

    _require_testdrive_origin(request)
    enforce_rate_limit(request, "student-transfer-testdrive-check", limit=30, window_seconds=300)
    aliases = await _testdrive_aliases(data.email, data.phone, data.browser_id)
    if not aliases:
        raise HTTPException(400, "Укажите корректную почту или телефон")
    existing = await _test_period_for_aliases(aliases)
    if not existing:
        await _remember_testdrive_pending(
            data.email,
            data.phone,
            next((alias for alias in aliases if alias[0] == "browser"), None),
        )
    payload = {
        "ok": True,
        "eligible": not bool(existing),
        "reason": "Тестовый период уже использован" if existing else "",
    }
    return JSONResponse(payload, headers=_testdrive_cors_headers())


@router.get("/testdrive/client.js")
async def testdrive_client_script():
    script = r'''(()=>{"use strict";
const endpoint="https://junior.sobakovod.pro/streams/testdrive/check",key="sobakovod-testdrive-browser-v1";
const browserId=()=>{try{let value=localStorage.getItem(key);if(!/^[A-Za-z0-9_-]{16,128}$/.test(value||"")){value=(crypto.randomUUID?crypto.randomUUID():Array.from(crypto.getRandomValues(new Uint8Array(24)),v=>v.toString(16).padStart(2,"0")).join(""));localStorage.setItem(key,value)}return value}catch{return""}};
const message=(form,text)=>{let node=form.querySelector(".form-result-block");if(!node){node=document.createElement("div");form.prepend(node)}node.textContent=text;node.style.display="block";node.style.color="#b42318";node.style.margin="12px 0"};
const loading=form=>{let node=form.querySelector(".nexus-testdrive-loading");if(!node){node=document.createElement("div");node.className="nexus-testdrive-loading";node.innerHTML='<span aria-hidden="true"></span>Проверяем возможность тестового периода…';form.prepend(node)}node.style.cssText="display:flex;align-items:center;gap:8px;margin:12px 0;color:#475467";const spinner=node.firstElementChild;spinner.style.cssText="display:inline-block;width:16px;height:16px;flex:0 0 16px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:nexusTrialSpin .7s linear infinite";if(!document.getElementById("nexus-testdrive-style")){const style=document.createElement("style");style.id="nexus-testdrive-style";style.textContent="@keyframes nexusTrialSpin{to{transform:rotate(360deg)}}";document.head.append(style)}return()=>node.remove()};
for(const form of document.querySelectorAll('form[action*="/pl/lite/block-public/process"]'))form.addEventListener("submit",async event=>{if(form.dataset.nexusTrialApproved==="1"){delete form.dataset.nexusTrialApproved;return}event.preventDefault();event.stopImmediatePropagation();const submitter=event.submitter||form.querySelector('[type="submit"]'),email=form.querySelector('[name="formParams[email]"]')?.value||"",phone=form.querySelector('[name="formParams[phone]"]')?.value||"",stopLoading=loading(form);if(submitter){submitter.disabled=true;submitter.setAttribute("aria-busy","true")}try{const response=await fetch(endpoint,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email,phone,browser_id:browserId()})}),data=await response.json();if(!response.ok)throw new Error(data.detail||"Проверка временно недоступна");if(!data.eligible){message(form,data.reason||"Тестовый период уже использован");return}form.dataset.nexusTrialApproved="1";stopLoading();if(submitter){submitter.disabled=false;submitter.removeAttribute("aria-busy")}form.requestSubmit(submitter||undefined)}catch(error){message(form,error.message||"Проверка временно недоступна")}finally{stopLoading();if(submitter&&form.dataset.nexusTrialApproved!=="1"){submitter.disabled=false;submitter.removeAttribute("aria-busy")}}},true);
})();'''
    return Response(
        script,
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "public, max-age=300", "X-Content-Type-Options": "nosniff"},
    )


async def _confirm_testdrive_job(job: dict[str, Any]) -> dict[str, Any]:
    user_id = _clean(job.get("gc_user_id"), 100)
    browser = _module("getcourse-onboarding", "service_getcourse_browser_access_snapshot")
    snapshot = await browser.service_getcourse_browser_access_snapshot(gc_user_id=user_id)
    if not snapshot.get("ok"):
        raise HTTPException(409, snapshot.get("error") or "GetCourse не подтвердил тестовые группы")
    identity = {
        "gc_user_id": user_id,
        "email": _clean(snapshot.get("email"), 320),
        "phone": _clean(snapshot.get("phone"), 100),
        "name": _clean(snapshot.get("name"), 300),
    }
    await _save_browser_access_snapshot(identity, snapshot)
    aliases = _test_period_aliases(identity)
    browser_alias = await _testdrive_browser_alias(job.get("browser_id"))
    if browser_alias:
        aliases.append(browser_alias)
    pending_browser_alias = await _pending_testdrive_browser_alias(identity)
    if pending_browser_alias and pending_browser_alias not in aliases:
        aliases.append(pending_browser_alias)
    catalog = await _test_period_catalog()
    current_ids = {str(item.get("group_id") or "") for item in snapshot.get("groups") or []}
    courses = [course for course in ("puppy", "dog") if str(catalog[course]["group_id"]) in current_ids]
    existing = await _test_period_for_aliases(aliases)
    if existing:
        existing_status = _clean(existing.get("status"), 40)
        if courses and existing_status in {"completed", "blocked_used", "queued_revoke", "revoking"}:
            now = _now()
            await _update_test_period(
                existing["id"], status="queued_revoke", expires_at=now, next_attempt_at=now,
                courses_json=json.dumps(courses, ensure_ascii=False),
                group_ids_json=json.dumps(_test_period_group_ids(catalog, courses)),
                last_error="Повторная выдача обнаружена GetCourse; Nexus снимает доступ",
            )
        async with _connect() as db:
            await db.executemany(
                "INSERT OR IGNORE INTO test_period_identities(identity_type,identity_value,test_period_id,created_at) VALUES(?,?,?,?)",
                [(kind, value, existing["id"], _now()) for kind, value in aliases],
            )
            await db.commit()
        return {"ok": True, "duplicate": True, "cleanup_queued": bool(courses and existing_status in {"completed", "blocked_used", "queued_revoke", "revoking"}), "period": _test_period_row_view(await _test_period_row(period_id=existing["id"]))}
    if not courses:
        raise HTTPException(409, "У пользователя нет активных групп тест-драйва")
    period_id = uuid.uuid4().hex
    try:
        starts = datetime.fromisoformat(_clean(job.get("received_at"), 40).replace("Z", "+00:00")).astimezone(timezone.utc).replace(microsecond=0)
    except ValueError:
        starts = datetime.now(timezone.utc).replace(microsecond=0)
    starts_at = starts.strftime("%Y-%m-%dT%H:%M:%SZ")
    expires_at = (starts + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    group_ids = _test_period_group_ids(catalog, courses)
    async with _connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        for identity_type, identity_value in aliases:
            claimed = await (
                await db.execute(
                    "SELECT test_period_id FROM test_period_identities WHERE identity_type=? AND identity_value=?",
                    (identity_type, identity_value),
                )
            ).fetchone()
            if claimed:
                await db.rollback()
                existing = await _test_period_row(period_id=claimed[0])
                return {"ok": True, "duplicate": True, "period": _test_period_row_view(existing)}
        await db.execute(
            """INSERT INTO test_periods(
                id,enrollment_id,gc_user_id,email,phone_key,student_name,courses_json,group_ids_json,
                status,starts_at,expires_at,next_attempt_at,operator_id,operator_name,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                period_id, f"testdrive:{user_id}", user_id, identity["email"],
                _phone_search_key(identity["phone"]), identity["name"],
                json.dumps(courses, ensure_ascii=False), json.dumps(group_ids), "active",
                starts_at, expires_at, expires_at, 0, "GetCourse testdrive", starts_at, starts_at,
            ),
        )
        await db.executemany(
            "INSERT INTO test_period_identities(identity_type,identity_value,test_period_id,created_at) VALUES(?,?,?,?)",
            [(kind, value, period_id, starts_at) for kind, value in aliases],
        )
        await db.commit()
    return {"ok": True, "duplicate": False, "period": _test_period_row_view(await _test_period_row(period_id=period_id))}


async def _queue_testdrive_confirm(data: TestDriveConfirmIn) -> dict[str, Any]:
    expected = await _meta_get("testdrive_callback_token")
    if not expected or not hmac.compare_digest(expected, data.token):
        raise HTTPException(401, "Неверный ключ подтверждения")
    user_id = _clean(data.gc_user_id, 100)
    if not user_id.isdigit():
        raise HTTPException(400, "GetCourse ID должен быть числом")
    key = f"testdrive_confirm:{user_id}"
    now = _now()
    async with _connect() as db:
        row = await (await db.execute("SELECT value FROM registry_meta WHERE key=?", (key,))).fetchone()
        try:
            previous = json.loads(str(row[0] or "{}")) if row else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            previous = {}
        payload = {
            "gc_user_id": user_id,
            "browser_id": _clean(data.browser_id, 128) or _clean(previous.get("browser_id"), 128),
            "received_at": _clean(previous.get("received_at"), 40) or now,
            "next_at": now,
            "attempts": int(previous.get("attempts") or 0),
            "last_error": "",
        }
        await db.execute(
            "INSERT INTO registry_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(payload, ensure_ascii=False)),
        )
        await db.commit()
    return {"ok": True, "accepted": True, "queued": True, "gc_user_id": user_id}


async def _process_testdrive_confirms() -> bool:
    async with _connect() as db:
        row = await (
            await db.execute(
                "SELECT key,value FROM registry_meta WHERE key LIKE 'testdrive_confirm:%' "
                "AND COALESCE(json_extract(value,'$.next_at'),'')<=? "
                "ORDER BY json_extract(value,'$.next_at'),key LIMIT 1",
                (_now(),),
            )
        ).fetchone()
    if not row:
        return False
    key, raw = str(row[0]), str(row[1] or "{}")
    try:
        job = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        job = {}
    if _clean(job.get("next_at"), 40) > _now():
        return False
    try:
        await _confirm_testdrive_job(job)
    except Exception as exc:
        attempts = int(job.get("attempts") or 0) + 1
        job.update(
            attempts=attempts, next_at=_test_period_retry_at(attempts),
            last_error=_clean(getattr(exc, "detail", None) or exc, 1000),
        )
        await _meta_set(key, json.dumps(job, ensure_ascii=False))
        if _logger:
            _logger.warning("GetCourse test-drive confirmation %s deferred: %s", job.get("gc_user_id"), exc)
        return True
    async with _connect() as db:
        await db.execute("DELETE FROM registry_meta WHERE key=?", (key,))
        await db.commit()
    return True


@router.post("/testdrive/confirm")
async def testdrive_confirm(data: TestDriveConfirmIn, request: Request):
    enforce_rate_limit(request, "student-transfer-testdrive-confirm", limit=300, window_seconds=3600)
    return await _queue_testdrive_confirm(data)


@router.get("/testdrive/config")
async def testdrive_config(request: Request):
    await _require_operator(request)
    return {
        "check_url": TESTDRIVE_CHECK_URL,
        "confirm_url": TESTDRIVE_CONFIRM_URL,
        "client_script": '<script src="https://junior.sobakovod.pro/streams/testdrive/client.js" defer></script>',
        "callback_token": await _meta_get("testdrive_callback_token"),
    }


@router.post("/testdrive/config/rotate")
async def rotate_testdrive_config(request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-testdrive-rotate", limit=3, window_seconds=3600)
    token = secrets.token_urlsafe(36)
    await _meta_set("testdrive_callback_token", token)
    return {"ok": True, "callback_token": token}


async def _test_period_for_identity(identity: dict[str, Any]) -> dict[str, Any] | None:
    return await _test_period_for_aliases(_test_period_aliases(identity))


async def _test_period_status_for_identity(identity: dict[str, Any], *, live: bool) -> dict[str, Any]:
    existing = await _test_period_for_identity(identity)
    result = _test_period_row_view(existing)
    if live and not existing:
        access = await _get_access_view(identity, live=True, force=True, allow_stale=True)
        if not access.get("ok") or access.get("stale"):
            raise RuntimeError(access.get("error") or access.get("warning") or "Не удалось проверить доступы GetCourse")
        catalog = await _test_period_catalog()
        current_ids = {str(item.get("group_id") or "") for item in access.get("current_groups") or []}
        used_id = str(catalog["used"]["group_id"])
        trial_ids = {str(item["group_id"]) for key, item in catalog.items() if key != "used"}
        if used_id in current_ids:
            result.update(status="used", can_issue=False, reason="Ученик уже использовал тестовый период")
        elif current_ids & trial_ids:
            result.update(status="active_external", can_issue=False, reason="Тестовый период уже активен в GetCourse")
    return result


async def _test_period_status(enrollment_id: str, *, live: bool) -> dict[str, Any]:
    return await _test_period_status_for_identity(await _access_identity(enrollment_id), live=live)


async def _create_test_period(
    enrollment_id: str, *, days: int, courses: list[str], operator: dict[str, Any]
) -> dict[str, Any]:
    return await _create_test_period_for_identity(
        await _access_identity(enrollment_id), enrollment_id=enrollment_id,
        days=days, courses=courses, operator=operator,
    )


async def _create_test_period_for_identity(
    identity: dict[str, Any], *, enrollment_id: str, days: int,
    courses: list[str], operator: dict[str, Any], allow_repeat: bool = False,
) -> dict[str, Any]:
    normalized_courses = sorted({_clean(item, 20) for item in courses})
    if not normalized_courses or any(item not in {"puppy", "dog"} for item in normalized_courses):
        raise HTTPException(400, "Выберите Щенок, Собака или оба курса")
    aliases = _test_period_aliases(identity)
    if not aliases:
        raise HTTPException(409, "У ученика нет GetCourse ID, email или телефона")
    period_id = uuid.uuid4().hex
    starts = datetime.now(timezone.utc).replace(microsecond=0)
    starts_at = starts.strftime("%Y-%m-%dT%H:%M:%SZ")
    expires_at = (starts + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    operator_name = _clean(operator.get("display_name") or operator.get("login"), 200)
    async with _connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        claimed_period_ids: set[str] = set()
        for identity_type, identity_value in aliases:
            claimed = await (
                await db.execute(
                    "SELECT test_period_id FROM test_period_identities WHERE identity_type=? AND identity_value=?",
                    (identity_type, identity_value),
                )
            ).fetchone()
            if claimed:
                claimed_period_ids.add(_clean(claimed[0], 64))
        if claimed_period_ids and not allow_repeat:
            await db.rollback()
            raise HTTPException(409, "Тестовый период уже выдавался этому ученику")
        if allow_repeat and claimed_period_ids:
            placeholders = ",".join("?" for _ in claimed_period_ids)
            rows = await (await db.execute(
                f"SELECT id,status FROM test_periods WHERE id IN ({placeholders})",
                sorted(claimed_period_ids),
            )).fetchall()
            unfinished = [row for row in rows if _clean(row[1], 40) not in {"completed", "blocked_used"}]
            if unfinished:
                await db.rollback()
                raise HTTPException(409, "Предыдущий тестовый период ещё выполняется или активен")
        await db.execute(
            """INSERT INTO test_periods(
                id,enrollment_id,gc_user_id,email,phone_key,student_name,courses_json,group_ids_json,
                status,starts_at,expires_at,next_attempt_at,operator_id,operator_name,created_at,updated_at,allow_repeat
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                period_id, _clean(enrollment_id, 100), _clean(identity.get("gc_user_id"), 100),
                _clean(identity.get("email"), 320), _phone_search_key(identity.get("phone")),
                _clean(identity.get("name"), 300), json.dumps(normalized_courses, ensure_ascii=False),
                "[]", "queued_grant", starts_at, expires_at, starts_at,
                int(operator.get("id") or 0), operator_name, starts_at, starts_at, int(allow_repeat),
            ),
        )
        await db.executemany(
            """INSERT INTO test_period_identities(identity_type,identity_value,test_period_id,created_at)
               VALUES(?,?,?,?) ON CONFLICT(identity_type,identity_value) DO UPDATE SET
               test_period_id=excluded.test_period_id,created_at=excluded.created_at""",
            [(identity_type, identity_value, period_id, starts_at) for identity_type, identity_value in aliases],
        )
        await db.commit()
    return _test_period_row_view(await _test_period_row(period_id=period_id))


async def service_widget_test_period(
    *, enrollment_id: str, action: str = "status", days: int = 1,
    courses: list[str] | None = None, requester_user_id: str = "",
) -> dict[str, Any]:
    identity = await _access_identity(enrollment_id)
    normalized_action = _clean(action, 20).lower()
    if normalized_action == "status":
        # Widget polling must stay cheap and must never block the employee on
        # a live browser session. The durable worker performs the authoritative
        # GetCourse/"already used" check before it grants anything.
        return await _test_period_status_for_identity(identity, live=False)
    if normalized_action == "revoke":
        row = await _test_period_row(enrollment_id=enrollment_id)
        if not row:
            raise HTTPException(404, "Активный тестовый период не найден")
        status = _clean(row.get("status"), 40)
        if status in {"completed", "blocked_used"}:
            return _test_period_row_view(row)
        grant_request_id = _clean(row.get("grant_request_id"), 64)
        if grant_request_id:
            try:
                cancel = _module("chat-moderators", "service_cancel_access_change")
                await asyncio.to_thread(cancel.service_cancel_access_change, request_id=grant_request_id)
            except Exception:
                # Revocation remains durable even when cancellation races the grant.
                pass
        now = _now()
        await _update_test_period(
            row["id"], status="queued_revoke", expires_at=now,
            next_attempt_at=now, last_error="Досрочное закрытие запрошено из виджета",
        )
        return _test_period_row_view(await _test_period_row(period_id=row["id"]))
    if normalized_action not in {"create", "repeat"}:
        raise HTTPException(400, "Неизвестное действие тестового периода")
    if not 1 <= int(days) <= TEST_PERIOD_MAX_DAYS:
        raise HTTPException(400, "Количество дней должно быть от 1 до 90")
    requester = _clean(requester_user_id, 200) or "messenger"
    return await _create_test_period_for_identity(
        identity, enrollment_id=enrollment_id, days=int(days), courses=courses or [],
        operator={"id": 0, "display_name": requester}, allow_repeat=normalized_action == "repeat",
    )


@router.get("/students/{enrollment_id}/test-period")
async def student_test_period(enrollment_id: str, request: Request, live: str = "0"):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-test-period-read", limit=20, window_seconds=120)
    try:
        return await _test_period_status(enrollment_id, live=live == "1")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc


@router.post("/students/{enrollment_id}/test-period")
async def create_student_test_period(enrollment_id: str, data: TestPeriodIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-test-period-create", limit=6, window_seconds=300)
    return await _create_test_period(
        enrollment_id, days=data.days, courses=data.courses, operator=operator
    )


@router.post("/students/{enrollment_id}/access/preview")
async def preview_student_access(enrollment_id: str, data: AccessPreviewIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-preview", limit=12, window_seconds=120)
    try:
        return await _preview_access_change(
            enrollment_id=enrollment_id,
            changes=[item.model_dump() for item in data.changes],
            requester_user_id=str(operator["id"]),
        )
    except Exception as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc


@router.post("/students/{enrollment_id}/access/apply")
async def apply_student_access(enrollment_id: str, data: AccessApplyIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-apply", limit=8, window_seconds=120)
    try:
        return await _apply_access_change(
            enrollment_id=enrollment_id,
            request_id=data.request_id,
            requester_user_id=str(operator["id"]),
        )
    except Exception as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc


@router.post("/students/{enrollment_id}/sheet-row")
async def ensure_student_sheet_row(enrollment_id: str, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-sheet-row", limit=20, window_seconds=120)
    student = await _widget_student_base(enrollment_id)
    result = await _queue_sheet_operation(
        student=student,
        action="sheet_row",
        operator_id=int(operator["id"]),
        operator_name=operator["display_name"] or operator["login"],
    )
    return {**result, "row": int(student.get("row") or 0), "message": "Добавление в таблицу принято"}


@router.put("/students/{enrollment_id}/lessons/{lesson_key}")
async def update_lesson(enrollment_id: str, lesson_key: str, data: LessonUpdateIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-lesson", limit=120, window_seconds=120)
    return await _queue_lesson_update(
        enrollment_id=enrollment_id,
        lesson_key=lesson_key,
        data=data,
        operator_id=int(operator["id"]),
        operator_name=operator["display_name"] or operator["login"],
    )


@router.get("/flow-setup")
async def flow_setup(request: Request):
    await _require_operator(request)
    creator = _module("course-chat-creator", "service_flow_setup")
    return await asyncio.to_thread(creator.service_flow_setup)


@router.post("/flows/preflight")
async def preflight_flow(data: FlowCreateIn, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-preflight", limit=20, window_seconds=300)
    course_key = _clean(data.course_key, 50)
    stream = _clean(data.stream, 50)
    blockers: list[str] = []
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise HTTPException(400, "Некорректный курс или номер потока")
    try:
        start = datetime.strptime(_clean(data.date_start, 100), "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(400, "Некорректная дата") from exc

    setup_module = _module("course-chat-creator", "service_flow_setup")
    setup_data = await asyncio.to_thread(setup_module.service_flow_setup)
    teacher = next(
        (item for item in setup_data.get("teachers") or [] if int(item.get("id") or 0) == int(data.teacher_id)),
        None,
    )
    if not teacher or not int((teacher or {}).get("offer_id") or 0):
        blockers.append("У куратора не настроен GetCourse offer_id")

    async with _connect() as db:
        duplicate_job = await (await db.execute(
            "SELECT status FROM flow_jobs WHERE course_key=? AND stream=? ORDER BY created_at DESC LIMIT 1",
            (course_key, stream),
        )).fetchone()
        duplicate_flow = await (await db.execute(
            "SELECT 1 FROM flow_registry WHERE course_key=? AND stream=?", (course_key, stream)
        )).fetchone()
    if duplicate_flow:
        blockers.append("Такой поток уже есть в Streams")
    elif duplicate_job:
        blockers.append("Операция для этого потока уже есть. Откройте её во вкладке «Операции»")

    fields = _module("getcourse-chat-fields", "service_registry_flow_sheet_preflight")
    creator_status_module = _module("course-chat-creator", "status")
    sheet_check, services_check = await asyncio.gather(
        fields.service_registry_flow_sheet_preflight(
            course_key=course_key, stream=stream, date_start=data.date_start,
        ),
        creator_status_module.status(),
    )
    if not sheet_check.get("ok"):
        blockers.append(_clean(sheet_check.get("error"), 500) or "Не готова таблица учеников")
    required = services_check.get("required_env") or services_check.get("env") or {}
    if not required.get("vk_group_token") or not required.get("vk_group_id"):
        blockers.append("Не настроено создание VK-бесед")
    if not required.get("telegram_api") or not required.get("telegram_session"):
        blockers.append("Нет рабочей авторизованной Telegram-сессии")
    links_check = services_check.get("chat_links_sync") or {}
    if not links_check.get("configured"):
        blockers.append("Не настроена запись в таблицу ссылок на чаты")
    return {
        "ok": not blockers,
        "can_create": not blockers,
        "blockers": blockers,
        "course_key": course_key,
        "course": "Послушная собака" if course_key == "dog" else "Щенок",
        "stream": stream,
        "date_start": start.strftime("%d.%m.%Y"),
        "teacher_id": int(data.teacher_id),
        "teacher": _clean((teacher or {}).get("name"), 200),
        "sheet": sheet_check,
        "checks": {
            "google_sheet": bool(sheet_check.get("ok")),
            "telegram": bool(required.get("telegram_api") and required.get("telegram_session")),
            "vk": bool(required.get("vk_group_token") and required.get("vk_group_id")),
            "chat_links": bool(links_check.get("configured")),
        },
    }


@router.post("/flows")
async def create_flow(data: FlowCreateIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-create", limit=10, window_seconds=300)
    course_key = _clean(data.course_key, 50)
    stream = _clean(data.stream, 50)
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise HTTPException(400, "Некорректный курс или номер потока")
    try:
        datetime.strptime(_clean(data.date_start, 100), "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Некорректная дата")
    setup = _module("course-chat-creator", "service_flow_setup")
    setup_data = await asyncio.to_thread(setup.service_flow_setup)
    teacher = next(
        (item for item in setup_data.get("teachers") or [] if int(item.get("id") or 0) == int(data.teacher_id)),
        None,
    )
    if not teacher or not int(teacher.get("offer_id") or 0):
        raise HTTPException(409, "Куратор не настроен для создания потока")
    async with _flow_creation_lock, _connect() as db:
        duplicate = await (
            await db.execute(
                "SELECT 1 FROM flow_jobs WHERE course_key=? AND stream=?",
                (course_key, stream),
            )
        ).fetchone()
        duplicate = duplicate or await (
            await db.execute("SELECT 1 FROM flow_registry WHERE course_key=? AND stream=?", (course_key, stream))
        ).fetchone()
        if duplicate:
            raise HTTPException(409, "Поток уже существует или создаётся")
        job_id = uuid.uuid4().hex
        await db.execute(
            """
            INSERT INTO flow_jobs(id,status,course_key,stream,date_start,teacher_id,operator_id,operator_name,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job_id, "queued", course_key, stream, _clean(data.date_start, 100), int(data.teacher_id),
                int(operator["id"]), operator["display_name"] or operator["login"], _now(), _now(),
            ),
        )
        await db.commit()
    return {"ok": True, "id": job_id, "status": "queued"}


@router.post("/flows/jobs/{job_id}/complete")
async def complete_flow_job(job_id: str, data: FlowManualCompleteIn, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-complete", limit=12, window_seconds=300)
    if not all((
        data.opened_as_community,
        data.history_250_enabled,
        data.members_admin_only,
        data.system_notifications_disabled,
    )):
        raise HTTPException(400, "Отметьте все четыре шага настройки VK-чата")
    link = _clean(data.invite_link, 2000)
    if not re.match(r"^https://vk\.me/join/", link, re.I):
        raise HTTPException(400, "Вставьте ссылку-приглашение VK")
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM flow_jobs WHERE id=?", (_clean(job_id, 64),))).fetchone()
    if not row:
        raise HTTPException(404, "Операция не найдена")
    job = dict(row)
    if job["status"] == "completed":
        return {"ok": True, "status": "completed", "message": "Поток успешно создан"}
    if job["status"] not in {"attention", "warning"}:
        raise HTTPException(409, "Сначала дождитесь создания чатов")
    result = _load_steps(job.get("result_json") or "{}")
    sheet_result = result.get("sheet") if isinstance(result.get("sheet"), dict) else {}
    fields = _module("getcourse-chat-fields", "service_registry_flow_sheet_status")
    sheet_status = await fields.service_registry_flow_sheet_status(
        course_key=job["course_key"],
        stream=job["stream"],
        date_start=job["date_start"],
        expected_sheet_id=int(sheet_result.get("sheet_id") or 0),
    )
    if not sheet_status.get("ok"):
        stages = result.get("stages") if isinstance(result.get("stages"), dict) else {}
        stages["sheet"] = "failed"
        result["stages"] = stages
        result["sheet"] = {**sheet_result, **sheet_status, "verified": False}
        error = _clean(sheet_status.get("error") or "Лист потока не прошёл проверку", 2000)
        async with _connect() as db:
            await db.execute(
                "UPDATE flow_jobs SET status='attention',result_json=?,error=?,updated_at=? WHERE id=?",
                (json.dumps(result, ensure_ascii=False), error, _now(), job["id"]),
            )
            await db.commit()
        raise HTTPException(409, error)
    creator = _module("course-chat-creator", "service_set_manual_vk_link")
    try:
        saved = await creator.service_set_manual_vk_link(
            course_key=job["course_key"], stream_number=job["stream"], link=link,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    create_result = result.get("create") if isinstance(result.get("create"), dict) else {}
    setup = _module("course-chat-creator", "service_flow_setup")
    setup_data = await asyncio.to_thread(setup.service_flow_setup)
    teacher = next(
        (item for item in setup_data.get("teachers") or [] if int(item.get("id") or 0) == int(job["teacher_id"])),
        {"id": job["teacher_id"], "name": "", "offer_id": 0},
    )
    persisted = await _persist_created_flow(job, teacher, create_result, final_vk_link=link, ready=True)
    _clear_snapshot_cache()
    sync_result = {"ok": True, "status": "scheduled"}
    result["manual"] = {
        "required": False, "opened_as_community": True, "history_250_enabled": True,
        "members_admin_only": True, "system_notifications_disabled": True, "invite_link_saved": True,
    }
    result["manual_link"] = saved
    result["sync"] = sync_result
    avatar_ready = bool(((create_result.get("vk") or {}) if isinstance(create_result.get("vk"), dict) else {}).get("avatar_ready"))
    completed = persisted.get("status") == "ready" and avatar_ready
    stages = result.get("stages") if isinstance(result.get("stages"), dict) else {}
    stages["manual_vk"] = "completed"
    stages["avatar"] = "completed" if avatar_ready else "running"
    stages["ready"] = "completed" if completed else "running"
    result["stages"] = stages
    status = "completed" if completed else "attention"
    error = "" if completed else "Настройки сохранены, но логотип VK-беседы не установлен. Нажмите «Обновить логотип VK» в карточке потока."
    async with _connect() as db:
        await db.execute(
            "UPDATE flow_jobs SET status=?,result_json=?,error=?,updated_at=? WHERE id=?",
            (status, json.dumps(result, ensure_ascii=False), error, _now(), job["id"]),
        )
        await db.commit()
    _schedule_registry_sync()
    return {
        "ok": True,
        "status": status,
        "message": "Поток успешно создан" if completed else error,
    }


@router.post("/flows/{course_key}/{stream}/vk-avatar")
async def reapply_flow_vk_avatar(course_key: str, stream: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-avatar", limit=8, window_seconds=300)
    course_key = _clean(course_key, 50)
    stream = _clean(stream, 50)
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise HTTPException(400, "Некорректный курс или номер потока")
    creator = _module("course-chat-creator", "service_reapply_flow_vk_avatar")
    try:
        result = await creator.service_reapply_flow_vk_avatar(course_key=course_key, stream_number=stream)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc
    return result


@router.get("/flows/jobs")
async def flow_jobs(request: Request, limit: int = 50, compact: str = "0"):
    await _require_operator(request)
    compact_mode = compact == "1"
    columns = (
        "id,status,course_key,stream,date_start,teacher_id,operator_id,operator_name,error,created_at,updated_at"
        if compact_mode else "*"
    )
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                f"SELECT {columns} FROM flow_jobs ORDER BY created_at DESC,id DESC LIMIT ?",
                (max(1, min(200, int(limit))),),
            )
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        if not compact_mode:
            item["result"] = _load_steps(item.pop("result_json", "{}"))
        items.append(item)
    return {"items": items}


@router.get("/flows/jobs/{job_id}")
async def flow_job(job_id: str, request: Request):
    await _require_operator(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM flow_jobs WHERE id=?", (_clean(job_id, 64),))).fetchone()
    if not row:
        raise HTTPException(404, "Операция не найдена")
    item = dict(row)
    result = _load_steps(item.pop("result_json", "{}"))
    item["result"] = _flow_job_ui_result(result, item["course_key"], item["stream"])
    return item


@router.post("/flows/jobs/{job_id}/retry")
async def retry_flow_job(job_id: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-retry", limit=8, window_seconds=300)
    async with _connect() as db:
        row = await (await db.execute(
            "SELECT status FROM flow_jobs WHERE id=?", (_clean(job_id, 64),)
        )).fetchone()
        if not row:
            raise HTTPException(404, "Операция не найдена")
        if row[0] not in {"failed", "warning"}:
            raise HTTPException(409, "Эту операцию сейчас нельзя повторить")
        await db.execute(
            "UPDATE flow_jobs SET status='queued',error='',updated_at=? WHERE id=?",
            (_now(), _clean(job_id, 64)),
        )
        await db.commit()
    return {"ok": True, "id": _clean(job_id, 64), "status": "queued"}


@router.put("/flows/{course_key}/{stream}/vk-link")
async def set_flow_vk_link(course_key: str, stream: str, data: VkLinkIn, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-vk-link", limit=12, window_seconds=300)
    course_key = _clean(course_key, 50)
    stream = _clean(stream, 50)
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise HTTPException(400, "Некорректный курс или номер потока")
    creator = _module("course-chat-creator", "service_set_manual_vk_link")
    try:
        result = await creator.service_set_manual_vk_link(course_key=course_key, stream_number=stream, link=data.link)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    link = _clean(data.link, 2000)
    async with _connect() as db:
        await db.execute(
            """UPDATE flow_registry SET vk_link=?,
               status=CASE WHEN COALESCE(tg_link,'')<>'' THEN 'ready' ELSE status END,updated_at=?
               WHERE course_key=? AND stream=?""",
            (link, _now(), course_key, stream),
        )
        job_row = await (await db.execute(
            """SELECT id,result_json FROM flow_jobs
               WHERE course_key=? AND stream=? ORDER BY created_at DESC,id DESC LIMIT 1""",
            (course_key, stream),
        )).fetchone()
        if job_row:
            job_result = _load_steps(job_row["result_json"] or "{}")
            manual_link = job_result.get("manual_link") if isinstance(job_result.get("manual_link"), dict) else {}
            job_result["manual_link"] = {**manual_link, "ok": True, "status": "saved_locally", "link": link}
            await db.execute(
                "UPDATE flow_jobs SET result_json=?,updated_at=? WHERE id=?",
                (json.dumps(job_result, ensure_ascii=False), _now(), job_row["id"]),
            )
        await db.commit()
    _clear_snapshot_cache()
    _schedule_registry_sync()
    result["sync"] = {"ok": True, "status": "scheduled"}
    return result


@router.put("/flows/{course_key}/{stream}/curator")
async def set_flow_curator(course_key: str, stream: str, data: FlowCuratorIn, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-flow-curator", limit=8, window_seconds=120)
    course_key = _clean(course_key, 50)
    stream = _clean(stream, 50)
    if course_key not in {"puppy", "dog"} or not stream.isdigit():
        raise HTTPException(400, "Некорректный курс или номер потока")
    curator = _clean(data.curator, 100)
    if curator not in CURATOR_NAMES:
        raise HTTPException(400, "Куратор не поддерживается")
    flow = next((item for item in await _flow_rows() if _flow_key(item) == (_clean(course_key, 50), _clean(stream, 50))), None)
    if not flow:
        raise HTTPException(404, "Поток не найден")
    creator = _module("course-chat-creator", "service_set_flow_curator")
    setup = _module("course-chat-creator", "service_flow_setup")
    teacher = next((item for item in setup.service_flow_setup().get("teachers") or [] if _norm(item.get("name")) == _norm(CURATOR_NAMES[curator])), None)
    if not teacher:
        raise HTTPException(409, "Куратор не настроен")
    try:
        source = await asyncio.to_thread(
            creator.service_set_flow_curator,
            course_key=course_key,
            stream_number=stream,
            teacher_id=int(teacher["id"]),
        )
        if not source.get("found"):
            fields = _module("getcourse-chat-fields", "service_set_registry_flow_curator")
            source = await fields.service_set_registry_flow_curator(course_key=course_key, stream=stream, curator=curator)
    except Exception as exc:
        if "429" in str(exc) or "Too Many Requests" in str(exc):
            raise HTTPException(429, "Google занят. Повторите через минуту") from exc
        raise HTTPException(409, _clean(exc, 500)) from exc
    now = _now()
    async with _connect() as db:
        await db.execute(
            "UPDATE flow_registry SET teacher_id=?,teacher=?,teacher_code=?,curator_source='streams',offer_id=?,updated_at=? WHERE course_key=? AND stream=?",
            (int(teacher["id"]), CURATOR_NAMES[curator], curator, int(teacher.get("offer_id") or CURATOR_OFFERS[curator]), now, course_key, stream),
        )
        await db.execute(
            "UPDATE enrollments SET teacher=?,teacher_code=?,updated_at=? WHERE course_key=? AND stream=? AND status<>'removed'",
            (CURATOR_NAMES[curator], curator, now, course_key, stream),
        )
        await db.commit()
    _clear_snapshot_cache()
    fields = _module("getcourse-chat-fields", "service_reconcile_registry_curators")
    getcourse = await fields.service_reconcile_registry_curators(flows=await _mirror_payload())
    return {"ok": True, "curator": curator, "teacher": CURATOR_NAMES[curator], "getcourse": getcourse}


@router.get("/sync")
async def sync_status(request: Request):
    await _require_operator(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute("SELECT * FROM registry_sync_runs ORDER BY id DESC LIMIT 20")
        ).fetchall()
    return {"updated_at": await _meta_get("last_sync_at"), "items": [dict(row) for row in rows]}


@router.post("/sync")
async def run_sync(request: Request):
    await _require_operator(request)
    _schedule_registry_sync()
    return {
        "ok": True,
        "accepted": True,
        "status": "queued",
        "scheduled_at": _sheet_operation_time(300),
        "message": "Синхронизация принята",
    }


@router.post("/preview")
async def preview(data: TransferRef, request: Request):
    await _require_operator(request)
    return await _remember_transfer_preview(data, await _preview(data))


@router.post("/transfers")
async def create_transfer(data: TransferRef, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-transfer-create", limit=15, window_seconds=120)
    preview_data = await _saved_transfer_preview(data)
    if not preview_data["can_transfer"]:
        raise HTTPException(409, "; ".join(preview_data["blockers"]))
    return await _queue_operation(preview_data, operator)


@router.post("/curator-preview")
async def curator_preview(data: CuratorChangeRef, request: Request):
    await _require_operator(request)
    return await _preview_curator_change(data, refresh=True)


@router.post("/curator-changes")
async def create_curator_change(data: CuratorChangeRef, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-curator-change", limit=15, window_seconds=120)
    # Queue from the current registry snapshot. The durable worker performs
    # provider writes and validates the actual assignment before committing.
    preview_data = await _preview_curator_change(data, refresh=False)
    if not preview_data["can_change"]:
        raise HTTPException(409, "; ".join(preview_data["blockers"]))
    return await _queue_operation(preview_data, operator)


@router.get("/access-operations")
async def access_operations(request: Request, limit: int = 100, compact: str = "0"):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-operations", limit=40, window_seconds=120)
    return {"items": await _access_operations(limit=limit, compact=compact == "1")}


@router.get("/access-operations/{request_id}")
async def access_operation_detail(request_id: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-operation", limit=60, window_seconds=120)
    items = await _access_operations(limit=1, request_id=request_id)
    if not items:
        raise HTTPException(404, "Операция с доступами не найдена")
    return items[0]


@router.get("/transfers")
async def transfers(request: Request, limit: int = 100, compact: str = "0"):
    await _require_operator(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                "SELECT * FROM transfers ORDER BY created_at DESC,id DESC LIMIT ?",
                (max(1, min(500, int(limit))),),
            )
        ).fetchall()
    return {"items": [_transfer_view(dict(row), compact=compact == "1") for row in rows]}


@router.get("/transfers/{transfer_id}")
async def transfer_detail(transfer_id: str, request: Request):
    await _require_operator(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM transfers WHERE id=?", (_clean(transfer_id, 64),))).fetchone()
    if not row:
        raise HTTPException(404, "Перенос не найден")
    return _transfer_view(dict(row))


@router.post("/transfers/{transfer_id}/retry")
async def retry_transfer(transfer_id: str, request: Request):
    await _require_operator(request)
    async with _connect() as db:
        cur = await db.execute(
            """
            UPDATE transfers SET status='queued',error='',updated_at=?
            WHERE id=? AND status IN ('failed','warning','needs_delivery')
              AND NOT (
                status='warning'
                AND json_extract(steps_json,'$.preview.action')='lesson_update'
              )
            """,
            (_now(), _clean(transfer_id, 64)),
        )
        await db.commit()
    if not cur.rowcount:
        raise HTTPException(409, "Перенос нельзя повторить в текущем статусе")
    return {"ok": True, "status": "queued"}


@router.get("/admin/operators")
async def admin_operators(request: Request):
    await _require_admin(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM operators ORDER BY login_key")).fetchall()
    return {"items": [{
        key: value for key, value in dict(row).items() if key != "password_hash"
    } | {"password_set": bool(row["password_hash"])} for row in rows]}


@router.post("/admin/operators")
async def admin_create_operator(data: OperatorIn, request: Request):
    await _require_admin(request)
    login = _clean(data.login, 200)
    if not login:
        raise HTTPException(400, "Логин обязателен")
    password = str(data.password or "")
    if password and len(password) < PASSWORD_MIN_LENGTH:
        raise HTTPException(400, f"Пароль должен быть не короче {PASSWORD_MIN_LENGTH} символов")
    password_hash = await asyncio.to_thread(_password_ctx.hash, password) if password else ""
    now = _now()
    async with _connect() as db:
        try:
            cur = await db.execute(
                "INSERT INTO operators(login,login_key,display_name,password_hash,active,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (login, _norm(login), _clean(data.display_name, 200), password_hash, 1 if data.active else 0, now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(409, "Такой логин уже существует")
    return {"ok": True, "id": int(cur.lastrowid)}


@router.put("/admin/operators/{operator_id}")
async def admin_update_operator(operator_id: int, data: OperatorIn, request: Request):
    await _require_admin(request)
    login = _clean(data.login, 200)
    if not login:
        raise HTTPException(400, "Логин обязателен")
    password = str(data.password or "")
    if password and len(password) < PASSWORD_MIN_LENGTH:
        raise HTTPException(400, f"Пароль должен быть не короче {PASSWORD_MIN_LENGTH} символов")
    async with _connect() as db:
        try:
            revoke_sessions = bool(password or data.clear_password or not data.active)
            if data.clear_password:
                cur = await db.execute(
                    "UPDATE operators SET login=?,login_key=?,display_name=?,password_hash='',active=?,updated_at=? WHERE id=?",
                    (login, _norm(login), _clean(data.display_name, 200), 1 if data.active else 0, _now(), operator_id),
                )
            elif password:
                password_hash = await asyncio.to_thread(_password_ctx.hash, password)
                cur = await db.execute(
                    "UPDATE operators SET login=?,login_key=?,display_name=?,password_hash=?,active=?,updated_at=? WHERE id=?",
                    (login, _norm(login), _clean(data.display_name, 200), password_hash, 1 if data.active else 0, _now(), operator_id),
                )
            else:
                cur = await db.execute(
                    "UPDATE operators SET login=?,login_key=?,display_name=?,active=?,updated_at=? WHERE id=?",
                    (login, _norm(login), _clean(data.display_name, 200), 1 if data.active else 0, _now(), operator_id),
                )
            if revoke_sessions:
                await db.execute("DELETE FROM sessions WHERE operator_id=?", (operator_id,))
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(409, "Такой логин уже существует")
    if not cur.rowcount:
        raise HTTPException(404, "Оператор не найден")
    return {"ok": True}
