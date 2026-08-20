from __future__ import annotations

import asyncio
import base64
import hashlib
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
from fastapi.responses import HTMLResponse, JSONResponse
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.auth import enforce_rate_limit, require_admin, verify_token_from_request

router = APIRouter()

MODULE_ID = "student-transfer"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SESSION_COOKIE = "student_transfer_session"
SESSION_TTL_DAYS = 30
PASSWORD_MIN_LENGTH = 8
ACCESS_VERIFY_DELAY_SECONDS = 60
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
_access_sync_task: asyncio.Task | None = None
_access_queue_task: asyncio.Task | None = None
_transfer_lock = asyncio.Lock()
_operation_queue_lock = asyncio.Lock()
_chat_delivery_lock = asyncio.Lock()
_registry_lock = asyncio.Lock()
_snapshot_lock = asyncio.Lock()
_flow_creation_lock = asyncio.Lock()
_access_apply_locks: dict[str, asyncio.Lock] = {}
_snapshot_refresh_task: asyncio.Task | None = None
_registry_sync_task: asyncio.Task | None = None
_last_registry_sync = 0.0
_registry_retry_at = 0.0
_snapshot_cache: dict[str, Any] = {"expires_at": 0.0, "data": None}
_password_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
_dummy_password_hash = _password_ctx.hash("streams-dummy-password")


@asynccontextmanager
async def _connect():
    db = await aiosqlite.connect(_must_db(), timeout=30)
    try:
        await db.execute("PRAGMA busy_timeout=30000")
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
    enrollment_id: str = Field(default="", max_length=100)
    email: str = Field(max_length=320)
    source_course_key: str = Field(max_length=50)
    source_stream: str = Field(max_length=50)
    source_row: int
    target_course_key: str = Field(max_length=50)
    target_stream: str = Field(max_length=50)
    move_sheet_row: bool = True


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


class LessonUpdateIn(StrictInput):
    value: bool
    expected_value: bool


class StudentNoteIn(StrictInput):
    note: str = Field(default="", max_length=2000)


class MessengerSendIn(StrictInput):
    channel_id: str = Field(max_length=200)
    transport: str = Field(max_length=40)
    provider: str = Field(max_length=40)
    chat_id: str = Field(default="", max_length=250)
    text: str = Field(default="", max_length=4000)
    attachment_url: str = Field(default="", max_length=4000)
    attachment_type: str = Field(default="", max_length=100)


class MessengerTemplatePreviewIn(StrictInput):
    template_id: int = Field(default=0, ge=0)
    body: str = Field(default="", max_length=20_000)


class MessengerTemplateFavoriteIn(StrictInput):
    template_id: int = Field(gt=0)
    favorite: bool


def setup(ctx):
    global _db_path, _module_dir, _logger, _worker_task, _access_sync_task, _access_queue_task
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _logger = getattr(ctx, "logger", None)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        lifecycle = getattr(ctx, "lifecycle", None)
        if lifecycle is not None:
            _worker_task = lifecycle.create_task(_worker_loop(), name="student-transfer-worker")
            _access_sync_task = lifecycle.create_task(_access_sync_loop(), name="student-transfer-access-sync")
            _access_queue_task = lifecycle.create_task(_access_queue_loop(), name="student-transfer-access-queue")
        else:
            _worker_task = loop.create_task(_worker_loop(), name="student-transfer-worker")
            _access_sync_task = loop.create_task(_access_sync_loop(), name="student-transfer-access-sync")
            _access_queue_task = loop.create_task(_access_queue_loop(), name="student-transfer-access-queue")
    else:
        loop.run_until_complete(_init_db())


async def shutdown():
    global _worker_task, _access_sync_task, _access_queue_task, _snapshot_refresh_task, _registry_sync_task
    tasks = [task for task in (_worker_task, _access_sync_task, _access_queue_task, _snapshot_refresh_task, _registry_sync_task) if task and not task.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _worker_task = None
    _access_sync_task = None
    _access_queue_task = None
    _snapshot_refresh_task = None
    _registry_sync_task = None


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
        await db.execute("DELETE FROM sessions WHERE expires_at<=?", (now,))
        await db.execute("UPDATE transfers SET status='queued',updated_at=? WHERE status='running'", (now,))
        await db.execute("UPDATE flow_jobs SET status='queued',updated_at=? WHERE status='running'", (now,))
        await db.commit()


async def _require_operator(request: Request) -> dict[str, Any]:
    _require_same_origin(request)
    token = request.cookies.get(SESSION_COOKIE)
    if token:
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
                "SELECT * FROM enrollments WHERE status<>'removed' ORDER BY CASE course_key WHEN 'puppy' THEN 0 ELSE 1 END,CAST(stream AS INTEGER) DESC,name,email"
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

        for flow in snapshot.get("items") or []:
            key = (_clean(flow.get("course_key"), 50), _clean(flow.get("stream"), 50))
            if key in skip_student_keys or key in changed_flow_keys:
                continue
            for student in flow.get("students") or []:
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
        for flow in snapshot.get("items") or []:
            course_key = _clean(flow.get("course_key"), 50)
            stream = _clean(flow.get("stream"), 50)
            registry_flow = flow_map.get((course_key, stream)) or {}
            for student in flow.get("students") or []:
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
            for row in await (await db.execute("SELECT email FROM enrollments WHERE status<>'removed'")).fetchall()
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
        for email, matches in candidates.items():
            rows = await (await db.execute(
                "SELECT id,course_key,course,stream,tariff,teacher,teacher_code,source_json "
                "FROM enrollments WHERE lower(email)=? AND status<>'removed'",
                (email,),
            )).fetchall()
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
                UPDATE enrollments SET course_key=?,course=?,stream=?,tariff=?,teacher=?,teacher_code=?,
                    source_json=?,status='assigned',updated_at=? WHERE id=?
                """,
                (
                    course_key, _clean(flow.get("course") or registry_flow.get("course") or row["course"], 100), stream,
                    _clean(student.get("tariff") or row["tariff"], 100),
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
        await db.commit()
    return changed


async def _student_by_id(enrollment_id: str) -> dict[str, Any]:
    snapshot = await _snapshot()
    for flow in snapshot.get("items") or []:
        for student in flow.get("students") or []:
            if _clean(student.get("enrollment_id"), 100) == _clean(enrollment_id, 100):
                item = _student_result(flow, student)
                await _enrich_order_identities([item])
                await _enrich_successful_managers([item])
                await _enrich_student_notes([item])
                return item
    raise HTTPException(404, "Ученик не найден")


async def service_widget_student(
    *, gc_user_id: str = "", email: str = "", phone: str = "", include_access: bool = True,
    summary_only: bool = False,
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
        return {"ok": True, "found": False, "paid_access": False}
    matched_gc_id = _clean(row["gc_user_id"], 100)
    profile_url = (
        f"https://club.sobakovod.pro/user/control/user/update/id/{quote(matched_gc_id)}"
        if matched_gc_id else ""
    )
    if summary_only:
        return {
            "ok": True, "found": True, "paid_access": True,
            "gc_user_id": matched_gc_id, "profile_url": profile_url,
        }
    item = await _student_by_id(_clean(row["id"], 100))
    result = {
        "ok": True, "found": True, "paid_access": True, "item": item,
        "profile_url": item.get("user_url") or profile_url,
    }
    if include_access:
        result["access"] = await _student_access_view(item["enrollment_id"], live=False)
    return result


async def _student_access_view(enrollment_id: str, *, live: bool = False) -> dict[str, Any]:
    identity = await _access_identity(enrollment_id)
    current = await _get_access_view(identity, live=live, force=live, allow_stale=not live)
    if not current.get("ok") or current.get("refresh_due") or current.get("stale"):
        current = await _queue_access_refresh(enrollment_id, current)
    pending = await _pending_access(identity)
    if not pending.get("pending"):
        return current
    if live and current.get("ok") and not current.get("stale") and current.get("source") == "live":
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
    fields = _module("getcourse-chat-fields", "service_getcourse_access_budget")
    budget = await fields.service_getcourse_access_budget()
    verification_delayed = (
        int(budget.get("requests_left_2h") or 0) < int(budget.get("needed_for_verification") or 6)
    )
    current = await _get_access_view(identity, live=False, allow_stale=True)
    if not current.get("ok"):
        raise RuntimeError("Не удалось проверить текущие доступы. Попробуйте ещё раз через минуту")
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
            "access": _access_target_view(
                current,
                target_groups,
                scheduled.get("next_check_at") or "",
                ready_by=scheduled.get("ready_by") or "",
                stage="queued",
            ),
        }


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
    now = _now()
    inserted = 0
    async with _connect() as db:
        for order in orders.get("items") or []:
            source_record_id = int(order.get("source_record_id") or 0)
            try:
                order_date = datetime.fromisoformat(_clean(order.get("date"), 100).replace("Z", "+00:00"))
            except ValueError:
                order_date = None
            candidates = [item for item in ready.get(_clean(order.get("course_key"), 50), []) if order_date and item[0].date() <= order_date.date()]
            flow = max(candidates, key=lambda item: (item[0], int(item[1]["stream"])))[1] if candidates else None
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
            inserted += max(0, int(cur.rowcount or 0))
        await db.commit()
    return inserted


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
    stream_display = " / ".join(
        _course_stream_label(item.get("course_key"), item.get("stream"))
        for item in assignments
    )
    assigned_courses = {_clean(item.get("course_key"), 50) for item in assignments}

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
        "course_display": "Щенок + Собака" if assigned_courses == {"puppy", "dog"} else _clean(flow.get("course"), 100),
        "stream_display": stream_display or _course_stream_label(flow.get("course_key"), flow.get("stream")),
        "course_assignments": assignments,
        "tariff": _clean(student.get("tariff"), 100),
        "phone": _clean(student.get("phone"), 100),
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


async def _enrich_successful_managers(items: list[dict[str, Any]]) -> None:
    if not items:
        return
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
        return
    matches = {str(item.get("key") or ""): item for item in result.get("items") or []}
    for item in items:
        match = matches.get(item["enrollment_id"]) or {}
        item["manager_name"] = _clean(match.get("manager_name"), 300)
        item["manager_id"] = _clean(match.get("manager_id"), 64)
        item["amo_deal_id"] = _clean(match.get("deal_id"), 64)
        item["amo_deal_url"] = _clean(match.get("deal_url"), 1000)


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


async def _enrich_order_identities(items: list[dict[str, Any]]) -> None:
    if not items:
        return
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
        return
    for item in items:
        match = matches.get(item["enrollment_id"]) or {}
        item["phone"] = _clean(item.get("phone") or match.get("phone"), 100)
        item["tariff"] = _clean(item.get("tariff") or match.get("tariff"), 100)
        item["utm_term"] = _clean(match.get("utm_term"), 1000)
        item["product_kind"] = _clean(match.get("product_kind"), 50)
        assignment = match.get("assignment")
        if isinstance(assignment, dict):
            item["getcourse_assignment"] = assignment


def _direct_profile_url(value: Any) -> str:
    """Keep only public VK or Telegram profile URLs suitable for the sheet."""
    raw = _clean(value, 500)
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return ""
    host = (parsed.hostname or "").casefold()
    if parsed.scheme != "https" or not parsed.path or host not in {
        "vk.com", "www.vk.com", "m.vk.com", "t.me", "www.t.me",
    }:
        return ""
    return raw


def _telegram_profile_url(username: Any) -> str:
    handle = _clean(username, 200).lstrip("@")
    return f"https://t.me/{handle}" if re.fullmatch(r"[A-Za-z0-9_]{5,32}", handle) else ""


def _vk_profile_url(platform_id: Any) -> str:
    value = _clean(platform_id, 100)
    return f"https://vk.com/id{value}" if re.fullmatch(r"\d{3,20}", value) else ""


async def _resolve_student_profile_link(student: dict[str, Any]) -> str:
    """Resolve one trustworthy public account URL for the combined TG/VK sheet column."""
    if current := _direct_profile_url(student.get("tg_account")):
        return current
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
    )


async def _chat_delivery_view(item: dict[str, Any]) -> dict[str, Any]:
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
    messenger = _module("messenger-widget", "service_transfer_delivery_target")
    target = await messenger.service_transfer_delivery_target(
        email=_clean(item.get("email"), 320), gc_user_id=_clean(item.get("gc_user_id"), 100),
        phone=_clean(item.get("phone"), 100), utm_term=_clean(item.get("utm_term"), 1000),
    )
    if not reason and not target.get("ok"):
        reason = _clean(target.get("reason") or "Доставка недоступна", 500)
    course = "Щ+С" if item.get("product_kind") == "combo" else _clean(item.get("course"), 100)
    channel = "VK" if target.get("provider") == "vk" else "TG" if target.get("provider") == "salebot" else ""
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
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT id,name,email,gc_user_id,course_key,tariff,source_json FROM enrollments "
                "WHERE id=? AND status<>'removed' LIMIT 1",
                (_clean(enrollment_id, 100),),
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


async def _get_access_view(
    identity: dict[str, Any], *, live: bool, force: bool = False, allow_stale: bool = False
) -> dict[str, Any]:
    fields = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
    access = _module("chat-moderators", "service_access_catalog")
    snapshot = await fields.service_getcourse_access_snapshot(
        gc_user_id=_clean(identity.get("gc_user_id"), 100),
        email=_clean(identity.get("email"), 300),
        live=live,
        force=force,
    )
    if not live and not allow_stale and (not snapshot.get("ok") or snapshot.get("refresh_due")):
        snapshot = await fields.service_getcourse_access_snapshot(
            gc_user_id=_clean(identity.get("gc_user_id"), 100),
            email=_clean(identity.get("email"), 300),
            live=True,
        )
    catalog_result = await asyncio.to_thread(access.service_access_catalog)
    catalog = catalog_result.get("items") or []
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


def _access_due_epoch(value: Any, default_delay: int = 60) -> float:
    raw = _clean(value, 60)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(time.time() + 15, parsed.timestamp())
        except ValueError:
            pass
    return time.time() + default_delay


async def _queue_access_refresh(enrollment_id: str, current: dict[str, Any]) -> dict[str, Any]:
    due = _access_due_epoch(current.get("next_at") or current.get("next_check_at"), 60)
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
            if result.get("ok") and not result.get("stale") and result.get("source") == "live":
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


async def _verify_pending_access() -> None:
    access = _module("chat-moderators", "service_pending_access_verifications")
    pending = await asyncio.to_thread(access.service_pending_access_verifications, limit=1)
    for request in pending.get("items") or []:
        scheduler = _module("chat-moderators", "service_schedule_access_verification")
        fields = _module("getcourse-chat-fields", "service_getcourse_access_budget")
        budget = await fields.service_getcourse_access_budget()
        if int(budget.get("requests_left_2h") or 0) < int(budget.get("needed_for_verification") or 6):
            await asyncio.to_thread(
                scheduler.service_schedule_access_verification,
                request_id=request["request_id"], delay_seconds=15 * 60,
                error="Лимит GetCourse API",
            )
            continue
        identity = {"gc_user_id": request.get("gc_user_id"), "email": request.get("identifier")}
        actual = await _get_access_after_write(identity)
        if not actual.get("ok") or actual.get("stale") or actual.get("source") != "live":
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
            fields = _module("getcourse-chat-fields", "service_getcourse_access_budget")
            budget = await fields.service_getcourse_access_budget()
            scheduler = _module("chat-moderators", "service_schedule_access_verification")
            await asyncio.to_thread(
                scheduler.service_schedule_access_verification,
                request_id=request_id,
                delay_seconds=_access_verification_delay(budget),
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


async def _access_queue_loop() -> None:
    """Keep employee access changes responsive even while the bulk GC sync is busy."""
    await asyncio.sleep(5)
    while True:
        try:
            await _apply_pending_access()
            await _verify_pending_access()
            await _process_pending_access_refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("GetCourse access queue iteration failed: %s", exc)
        await asyncio.sleep(10)


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
    if (
        _clean(ref.source_course_key, 50) == _clean(ref.target_course_key, 50)
        and _clean(ref.source_stream, 50) == _clean(ref.target_stream, 50)
    ):
        raise HTTPException(400, "Выберите другой поток")
    snapshot = await _snapshot(refresh=refresh)
    source_flow, student = _find_source(snapshot, ref)
    target = _find_target(snapshot, ref)
    curator = _clean(target.get("curator_value"), 100)
    offer_id = int(target.get("offer_id") or CURATOR_OFFERS.get(curator, 0))
    source = _student_result(source_flow, student)
    await _enrich_order_identities([source])
    await _enrich_successful_managers([source])
    assignment = source.get("getcourse_assignment") or {}
    registry_repair = all((
        _clean(assignment.get("course_key"), 50) == _clean(target.get("course_key"), 50),
        _clean(assignment.get("stream"), 50) == _clean(target.get("stream"), 50),
        _clean(assignment.get("vk_link"), 2000) == _clean(target.get("vk_link"), 2000),
        _clean(assignment.get("tg_link"), 2000) == _clean(target.get("tg_link"), 2000),
        _clean(assignment.get("curator"), 100) == curator,
    ))
    chat_readiness: dict[str, Any] = {}
    if not registry_repair:
        chat_service = _module("course-chat-creator", "service_transfer_chat_readiness")
        chat_readiness = chat_service.service_transfer_chat_readiness(
            _clean(source_flow.get("course_key"), 50), _clean(source_flow.get("stream"), 50)
        )
    warnings: list[str] = []
    if registry_repair:
        warnings.append("GetCourse и чаты не изменяются")
    elif source_flow.get("course_key") != target.get("course_key"):
        warnings.append("Меняется курс; доступ к обучению должен уже существовать в GetCourse")
    for platform, label in (("vk", "VK"), ("telegram", "Telegram")):
        if chat_readiness.get(platform, {}).get("status") == "legacy_inaccessible":
            warnings.append(f"Из старого чата {label} потребуется удалить вручную")
    blockers: list[str] = []
    if not _clean(student.get("gc_user_id"), 100):
        blockers.append("У ученика не найден ID GetCourse")
    if not offer_id and not registry_repair:
        blockers.append("Для куратора целевого потока не задано предложение")
    return {
        "action": "registry_repair" if registry_repair else "transfer",
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
            "move": bool(ref.move_sheet_row),
        },
        "chat_readiness": chat_readiness,
        "warnings": warnings,
        "blockers": blockers,
    }


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
        try:
            fields_service = _module("getcourse-chat-fields", "service_transfer_write_getcourse")
            action = _clean((steps.get("preview") or {}).get("action"), 50) or "transfer"
            move_sheet_row = bool(((steps.get("preview") or {}).get("sheet") or {}).get("move", True))
            steps.pop("curator_order", None)
            steps.pop("delivery", None)
            retry_state = steps.pop("retry", None)
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
            if not (steps.get("getcourse") or {}).get("ok"):
                result = await fields_service.service_transfer_write_getcourse(
                    email=transfer["email"],
                    gc_user_id=transfer["gc_user_id"],
                    order_id=_clean(student.get("order_id"), 100),
                    deal_number=_clean(student.get("deal_number") or student.get("order_id"), 100),
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
            if not move_sheet_row:
                steps["chat_removal"] = {"ok": True, "status": "preserved", "items": {}}
                await _save_transfer(transfer_id, status="completed", steps=steps)
                return
            if not steps.get("chat_removal"):
                identity_service = _module("messenger-widget", "service_transfer_recipients")
                recipients = await identity_service.service_transfer_recipients(
                    email=transfer["email"], gc_user_id=transfer["gc_user_id"], name=transfer["student_name"]
                )
                chat_service = _module("course-chat-creator", "service_remove_transfer_member")
                removals = {}
                readiness = (steps.get("preview") or {}).get("chat_readiness") or {}
                for channel in ("telegram", "vk"):
                    channel_readiness = readiness.get(channel) or {}
                    if not channel_readiness.get("manageable"):
                        removals[channel] = {"ok": True, "status": "manual", "reason": channel_readiness.get("status") or "not_manageable"}
                        continue
                    recipient_id = _clean(recipients.get(channel), 200)
                    if not recipient_id:
                        removals[channel] = {"ok": False, "status": "no_identity"}
                        continue
                    removals[channel] = await chat_service.service_remove_transfer_member(
                        platform=channel,
                        target=recipient_id,
                        course_key=transfer["source_course_key"],
                        stream_number=transfer["source_stream"],
                        dry_run=False,
                    )
                removal_ok = all(item.get("ok") for item in removals.values())
                manual = any(item.get("status") == "manual" for item in removals.values())
                steps["chat_removal"] = {"ok": removal_ok, "items": removals}
                await _save_transfer(
                    transfer_id,
                    status="warning" if manual or not removal_ok else "completed",
                    steps=steps,
                    error="Ученик перенесён. Из старого VK-чата его нужно удалить вручную." if manual else ("" if removal_ok else "Не из всех старых чатов удалось удалить ученика"),
                )
            else:
                removal_ok = bool((steps.get("chat_removal") or {}).get("ok"))
                await _save_transfer(transfer_id, status="completed" if removal_ok else "warning", steps=steps)
        except Exception as exc:
            if _is_google_rate_limit(exc):
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

        if stages.get("sheet") != "completed":
            await save_stage("sheet", "running")
            fields = _module("getcourse-chat-fields", "service_create_registry_flow_sheet")
            sheet_result = await fields.service_create_registry_flow_sheet(
                course_key=job["course_key"],
                stream=job["stream"],
                date_start=job["date_start"],
                curator=curator_code,
            )
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
                        WHERE status='queued' OR (
                            status='waiting' AND COALESCE(json_extract(steps_json,'$.retry.next_retry_at'),'')<=?
                        )
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
    item["action"] = _clean((item["steps"].get("preview") or {}).get("action"), 50) or "transfer"
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
                    SELECT id,status FROM transfers
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
                return {"ok": True, "id": active["id"], "status": active["status"], "action": action, "existing": True}
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
    return {"ok": True, "id": transfer_id, "status": "queued", "action": action}


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
        "img-src 'self' data:",
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
        await db.execute("DELETE FROM sessions WHERE expires_at<=?", (_now(),))
        await db.execute(
            "INSERT INTO sessions(token,operator_id,expires_at,created_at) VALUES(?,?,?,?)",
            (token, operator["id"], _session_expires(), _now()),
        )
        await db.commit()
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
        async with _connect() as db:
            await db.execute("DELETE FROM sessions WHERE token=?", (token,))
            await db.commit()
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
    snapshot = await _snapshot(refresh=refresh == "1")
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
    return {"items": items, "updated_at": snapshot.get("updated_at") or snapshot.get("cache_updated_at") or ""}


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
):
    await _require_operator(request)
    query = _norm(q)
    query_phone = _phone_search_key(q)
    phone_lookup = len(query_phone) >= 10
    snapshot = await _snapshot(refresh=refresh == "1")
    candidates: list[dict[str, Any]] = []
    for flow in snapshot.get("items") or []:
        if course_key and _clean(flow.get("course_key"), 50) != _clean(course_key, 50):
            continue
        if stream and _clean(flow.get("stream"), 50) != _clean(stream, 50):
            continue
        for student in flow.get("students") or []:
            item = _student_result(flow, student)
            candidates.append(item)
    identities_enriched = bool(query and phone_lookup)
    if identities_enriched:
        await _enrich_order_identities(candidates)
    base: list[dict[str, Any]] = []
    for item in candidates:
        haystack = _norm(" ".join(_clean(item.get(key), 500) for key in ("name", "email", "phone", "gc_user_id", "order_id")))
        phone_match = bool(phone_lookup and query_phone in _phone_search_key(item.get("phone")))
        if query and query not in haystack and not phone_match:
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
    start = max(0, min(100000, int(offset)))
    page_size = max(1, min(250, int(limit)))
    page = found[start : start + page_size]
    if not identities_enriched:
        await _enrich_order_identities(page)
    await _enrich_successful_managers(page)
    await _enrich_student_notes(page)
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
        "updated_at": snapshot.get("updated_at") or snapshot.get("cache_updated_at") or "",
    }


@router.get("/students/{enrollment_id}")
async def student(enrollment_id: str, request: Request):
    await _require_operator(request)
    return {
        "item": await _student_by_id(enrollment_id),
        "curators": [{"value": key, "name": CURATOR_NAMES[key], "offer_id": value} for key, value in CURATOR_OFFERS.items()],
    }


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
async def student_messenger(enrollment_id: str, request: Request):
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
    )


@router.post("/students/{enrollment_id}/messenger/send")
async def student_messenger_send(enrollment_id: str, data: MessengerSendIn, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-messenger-send", limit=60, window_seconds=300)
    item = await _student_by_id(enrollment_id)
    messenger = _module("messenger-widget", "service_streams_send")
    try:
        return await messenger.service_streams_send(
            channel_id=data.channel_id,
            transport=data.transport,
            provider=data.provider,
            chat_id=data.chat_id,
            phone=item.get("phone") or "",
            text=data.text,
            operator_name=operator.get("display_name") or operator.get("login") or "",
            email=item.get("email") or "",
            gc_user_id=item.get("gc_user_id") or "",
            name=item.get("name") or "",
            attachment_url=data.attachment_url,
            attachment_type=data.attachment_type,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


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
    result = await _chat_delivery_view(await _student_by_id(enrollment_id))
    result.pop("recipient_id", None)
    result.pop("content", None)
    return result


@router.post("/students/{enrollment_id}/chat-delivery")
async def send_student_chats(enrollment_id: str, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-chat-delivery-send", limit=4, window_seconds=300)
    async with _chat_delivery_lock:
        delivery = await _chat_delivery_view(await _student_by_id(enrollment_id))
        if not delivery.get("can_send"):
            raise HTTPException(409, delivery.get("reason") or "Доставка недоступна")
        messenger = _module("messenger-widget", "service_send_transfer_message")
        result = await messenger.service_send_transfer_message(
            provider=delivery["provider"], recipient_id=delivery["recipient_id"],
            content=delivery["content"], operation_id=uuid.uuid4().hex,
        )
        if not result.get("ok"):
            raise HTTPException(502, result.get("error") or "Сообщение не отправлено")
    return {"ok": True, "status": "sent", "channel": delivery["channel"]}


@router.get("/students/{enrollment_id}/access")
async def student_access(enrollment_id: str, request: Request, live: str = "0"):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-access-read", limit=40, window_seconds=120)
    return await _student_access_view(enrollment_id, live=live == "1")


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
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-sheet-row", limit=20, window_seconds=120)
    student = await _student_by_id(enrollment_id)
    student["tg_account"] = await _resolve_student_profile_link(student)
    fields = _module("getcourse-chat-fields", "service_registry_ensure_student")
    try:
        result = await fields.service_registry_ensure_student(
            course_key=student["course_key"],
            stream=student["stream"],
            student=student,
        )
    except Exception as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc
    lesson_columns = result.get("lesson_columns") or []
    await _bind_sheet_row(enrollment_id, int(result.get("row") or 0), lesson_columns)
    return {
        "ok": True,
        "status": result.get("status"),
        "row": int(result.get("row") or 0),
        "sheet_title": _clean(result.get("sheet_title"), 300),
        "lessons": [
            {"key": _clean(item.get("key"), 5), "label": _clean(item.get("label"), 200), "value": False}
            for item in lesson_columns
        ],
    }


@router.put("/students/{enrollment_id}/lessons/{lesson_key}")
async def update_lesson(enrollment_id: str, lesson_key: str, data: LessonUpdateIn, request: Request):
    await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-lesson", limit=120, window_seconds=120)
    student = await _student_by_id(enrollment_id)
    if not int(student.get("row") or 0):
        raise HTTPException(409, "Ученик ещё не добавлен в таблицу")
    lesson = next(
        (item for item in student.get("lessons") or [] if _clean(item.get("key"), 5).upper() == _clean(lesson_key, 5).upper()),
        None,
    )
    if not lesson:
        raise HTTPException(404, "Отметка не найдена")
    fields = _module("getcourse-chat-fields", "service_registry_write_lesson")
    try:
        result = await fields.service_registry_write_lesson(
            course_key=student["course_key"], stream=student["stream"], email=student["email"],
            source_row=int(student["row"]), lesson_key=_clean(lesson_key, 5).upper(),
            value=data.value, expected_value=data.expected_value,
        )
    except Exception as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc
    now = _now()
    numeric = 1 if result.get("value") else 0
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at)
            VALUES(?,?,?,?,?,0,?)
            ON CONFLICT(enrollment_id,lesson_key) DO UPDATE SET
                value=excluded.value,sheet_value=excluded.sheet_value,dirty=0,updated_at=excluded.updated_at
            """,
            (enrollment_id, _clean(lesson_key, 5).upper(), _clean(lesson.get("label"), 200), numeric, numeric, now),
        )
        await db.commit()
    _clear_snapshot_cache()
    if int(result.get("row") or 0) != int(student["row"]):
        await _bind_sheet_row(enrollment_id, int(result.get("row") or 0), [])
    return {"ok": True, "value": bool(numeric), "row": int(result.get("row") or student["row"])}


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
    creator = _module("course-chat-creator", "service_set_manual_vk_link")
    try:
        saved = await creator.service_set_manual_vk_link(
            course_key=job["course_key"], stream_number=job["stream"], link=link,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    result = _load_steps(job.get("result_json") or "{}")
    create_result = result.get("create") if isinstance(result.get("create"), dict) else {}
    setup = _module("course-chat-creator", "service_flow_setup")
    setup_data = await asyncio.to_thread(setup.service_flow_setup)
    teacher = next(
        (item for item in setup_data.get("teachers") or [] if int(item.get("id") or 0) == int(job["teacher_id"])),
        {"id": job["teacher_id"], "name": "", "offer_id": 0},
    )
    persisted = await _persist_created_flow(job, teacher, create_result, final_vk_link=link, ready=True)
    _clear_snapshot_cache()
    _schedule_registry_sync()
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
    result = await _sync_registry(force=True)
    if not result.get("ok"):
        detail = "Google занят. Обновление продолжится автоматически" if "429" in str(result.get("error")) or result.get("status") == "deferred" else "Обновление не выполнено"
        raise HTTPException(503, detail)
    return result


@router.post("/preview")
async def preview(data: TransferRef, request: Request):
    await _require_operator(request)
    return await _preview(data)


@router.post("/transfers")
async def create_transfer(data: TransferRef, request: Request):
    operator = await _require_operator(request)
    enforce_rate_limit(request, "student-transfer-transfer-create", limit=15, window_seconds=120)
    preview_data = await _preview(data)
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
    preview_data = await _preview_curator_change(data, refresh=True)
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
            "UPDATE transfers SET status='queued',error='',updated_at=? WHERE id=? AND status IN ('failed','warning','needs_delivery')",
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
