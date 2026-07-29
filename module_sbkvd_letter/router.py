from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import logging
import math
import mimetypes
import os
import re
import secrets
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, AsyncIterator
from zoneinfo import ZoneInfo

import aiofiles
import aiosqlite
import httpx
from fastapi import APIRouter, File, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel, Field

from orchestrator.auth import can_access_module, verify_token_from_request
from orchestrator.telegram_proxy import telegram_bot_api_base, telegram_bot_api_proxy_url

router = APIRouter()

MODULE_ID = "sbkvd-letter"
SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
VARIABLE_RE = re.compile(r"{{\s*([a-zA-Z0-9_.\[\]-]+)\s*}}")
FINAL_STATUSES = {"completed", "completed_with_errors", "failed", "cancelled"}
ACTIVE_STATUSES = {"preparing", "scheduled", "queued", "running", "paused"}
SYSTEM_FIELDS = {"id", "platform_id", "created_at", "updated_at", "table"}
FILE_FOLDER = "sbkvd-letter"
TEMPLATE_FOLDER = "templates"
ATTACHMENT_FOLDER = "attachments"
VK_NOT_ALLOW_CODES = {901}
VK_RETRYABLE_CODES = {6, 9, 10, 29}
MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
TELEGRAM_NOT_ALLOW_MARKERS = (
    "blocked by the user",
    "bot was blocked",
    "bot can't initiate conversation",
    "bot cannot initiate conversation",
)
DEFAULT_CONFIG: dict[str, Any] = {
    "sheets": [],
    "send_concurrency": 4,
    "vk_rate_per_sec": 3.0,
    "telegram_rate_per_sec": 10.0,
    "adaptive_rate_enabled": True,
    "max_attempts": 3,
    "network_max_attempts": 96,
    "network_retry_base_sec": 300,
    "network_retry_max_sec": 1800,
    "auto_pause_network_errors": 20,
}

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_worker_task: asyncio.Task | None = None
_worker_generation = ""
_module_instance: Any = None
_deletion_tasks: dict[str, asyncio.Task] = {}
_deletion_lock: asyncio.Lock | None = None
_db_write_lock: asyncio.Lock | None = None
_delivery_write_queue: list[tuple[dict[str, Any], asyncio.Future[None]]] = []
_delivery_flush_task: asyncio.Task | None = None
_count_refresh_locks: dict[str, asyncio.Lock] = {}
_count_refresh_last: dict[str, float] = {}
_adaptive_rate_gates: dict[str, "AdaptiveRateGate"] = {}
DELIVERY_WRITE_BATCH_WINDOW_SEC = 0.20
DELIVERY_WRITE_BATCH_SIZE = 200


class ConditionIn(BaseModel):
    field: str = ""
    op: str = "contains"
    value: Any = ""
    value2: Any = ""


class ManualRecipientIn(BaseModel):
    channel: str
    recipient_id: str
    label: str = ""


class AudienceIn(BaseModel):
    tables: list[str] = Field(default_factory=list)
    mode: str = "and"
    conditions: list[ConditionIn] = Field(default_factory=list)
    include_ids: list[str] = Field(default_factory=list)
    exclude_ids: list[str] = Field(default_factory=list)
    manual_recipients: list[ManualRecipientIn] = Field(default_factory=list)


class SheetIn(BaseModel):
    name: str
    enabled: bool = True
    channel: str = "vk"
    recipient_field: str = "platform_id"
    display_name: str = ""


class SettingsIn(BaseModel):
    sheets: list[SheetIn] = Field(default_factory=list)
    send_concurrency: int = 4
    vk_rate_per_sec: float = 3.0
    telegram_rate_per_sec: float = 10.0
    adaptive_rate_enabled: bool = True
    max_attempts: int = 3
    network_max_attempts: int = 96
    network_retry_base_sec: int = 300
    network_retry_max_sec: int = 1800
    auto_pause_network_errors: int = 20


class TokensIn(BaseModel):
    vk_token: str = ""
    telegram_token: str = ""
    clear_vk: bool = False
    clear_telegram: bool = False


class SegmentIn(BaseModel):
    name: str
    description: str = ""
    audience: AudienceIn


class TemplateIn(BaseModel):
    name: str
    content: str
    channels: list[str] = Field(default_factory=lambda: ["vk"])
    keyboard: dict[str, Any] = Field(default_factory=dict)
    attachment_ids: list[int] = Field(default_factory=list)
    vk_attachment: str = ""
    parse_mode: str = ""


class CampaignIn(BaseModel):
    name: str = ""
    audience: AudienceIn = Field(default_factory=AudienceIn)
    segment_id: int | None = None
    segment_ids: list[int] = Field(default_factory=list)
    template_id: int | None = None
    content: str = ""
    channels: list[str] = Field(default_factory=list)
    keyboard: dict[str, Any] = Field(default_factory=dict)
    attachment_ids: list[int] = Field(default_factory=list)
    vk_attachment: str = ""
    parse_mode: str = ""
    scheduled_at: str | None = None
    exclude_buyers: bool = True
    exclude_stop_list: bool = True
    exclude_not_allow: bool = True


class SegmentPreviewIn(BaseModel):
    segment_ids: list[int] = Field(default_factory=list)
    exclude_buyers: bool = True
    exclude_stop_list: bool = True
    exclude_not_allow: bool = True


class ContentDeletionIn(BaseModel):
    content: str = ""
    expected_remaining: int | None = None
    refresh_remote: bool = True


for _model in (ConditionIn, ManualRecipientIn, AudienceIn, SheetIn, SettingsIn, TokensIn, SegmentIn, TemplateIn, CampaignIn, SegmentPreviewIn, ContentDeletionIn):
    if hasattr(_model, "model_rebuild"):
        _model.model_rebuild()


async def setup(ctx):
    global _db_path, _module_dir, _logger, _worker_task, _worker_generation, _module_instance
    global _deletion_lock, _db_write_lock, _delivery_write_queue, _delivery_flush_task
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.sbkvd-letter"))
    _module_instance = sys.modules.get(__name__)
    _deletion_lock = asyncio.Lock()
    _db_write_lock = asyncio.Lock()
    previous = getattr(builtins, "_nexus_sbkvd_letter_worker", None)
    if isinstance(previous, asyncio.Task) and not previous.done():
        previous.cancel()
        try:
            await previous
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            _log("warning", "previous worker shutdown error: %s", _delivery_error_text(exc))
    previous_flush = getattr(builtins, "_nexus_sbkvd_letter_delivery_flush", None)
    if isinstance(previous_flush, asyncio.Task) and not previous_flush.done():
        try:
            await asyncio.wait_for(asyncio.shield(previous_flush), timeout=30.0)
        except asyncio.TimeoutError:
            previous_flush.cancel()
            await asyncio.gather(previous_flush, return_exceptions=True)
        except Exception as exc:
            _log("warning", "previous delivery batch shutdown error: %s", _delivery_error_text(exc))
    _delivery_write_queue = []
    _delivery_flush_task = None
    await _init_db()
    await _ensure_storage_folders()

    _worker_generation = uuid.uuid4().hex
    _worker_task = asyncio.create_task(_worker_loop(_worker_generation), name="sbkvd-letter-worker")
    setattr(builtins, "_nexus_sbkvd_letter_worker", _worker_task)
    _log("info", "sbkvd-letter initialized generation=%s", _worker_generation)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("module is not initialized")
    return _db_path


def _writer_lock() -> asyncio.Lock:
    """Return the per-process SQLite writer lock, recreating it for test loops."""
    global _db_write_lock
    loop = asyncio.get_running_loop()
    bound_loop = getattr(_db_write_lock, "_loop", None) if _db_write_lock is not None else None
    if _db_write_lock is None or (bound_loop is not None and bound_loop is not loop):
        _db_write_lock = asyncio.Lock()
    return _db_write_lock


def _is_database_busy(exc: BaseException) -> bool:
    message = str(exc).casefold()
    return isinstance(exc, aiosqlite.OperationalError) and (
        "database is locked" in message or "database is busy" in message or "database table is locked" in message
    )


@asynccontextmanager
async def _write_db(*, operation: str = "write") -> AsyncIterator[aiosqlite.Connection]:
    """Serialize module writes and wait out SQLite writers outside this module.

    SQLite WAL permits concurrent readers but still has only one writer. Delivery
    coroutines therefore queue here instead of competing until busy_timeout expires
    and failing an otherwise healthy campaign.
    """
    async with _writer_lock():
        retry = 0
        while True:
            db = await aiosqlite.connect(_must_db(), timeout=30)
            try:
                await db.execute("PRAGMA busy_timeout=30000")
                await db.execute("PRAGMA foreign_keys=ON")
                await db.execute("BEGIN IMMEDIATE")
                break
            except asyncio.CancelledError:
                await db.close()
                raise
            except Exception as exc:
                await db.close()
                if not _is_database_busy(exc):
                    raise
                retry += 1
                if retry == 1 or retry % 10 == 0:
                    _log("warning", "sqlite writer waiting operation=%s retry=%s", operation, retry)
                await asyncio.sleep(min(2.0, 0.1 * retry))
        try:
            yield db
            await db.commit()
        except BaseException:
            await db.rollback()
            raise
        finally:
            await db.close()


async def _apply_delivery_outcome(
    db: aiosqlite.Connection,
    outcome: dict[str, Any],
) -> tuple[int, int, int]:
    row = outcome["row"]
    recipient_row_id = int(row["id"])
    campaign_id = str(row["campaign_id"])
    attempt = int(outcome["attempt"])
    kind = str(outcome["kind"])
    now = str(outcome["now"])
    sent_delta = failed_delta = not_allowed_delta = 0
    if kind == "sent":
        inserted = await db.execute(
            "INSERT OR IGNORE INTO sent_messages(campaign_id,recipient_row_id,channel,recipient_id,external_message_id,sent_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                campaign_id, recipient_row_id, row["channel"], row["recipient_id"],
                outcome["external_id"], now,
            ),
        )
        await db.execute(
            "UPDATE recipients SET status='sent',attempts=?,external_message_id=?,sent_at=?,last_error='',updated_at=? WHERE id=?",
            (attempt, outcome["external_id"], now, now, recipient_row_id),
        )
        if inserted.rowcount:
            sent_delta = 1
            await db.execute(
                "INSERT INTO delivery_attempts(campaign_id,recipient_id,attempt_no,status,response_json) VALUES(?,?,?,?,?)",
                (campaign_id, recipient_row_id, attempt, "sent", _dump(outcome["response"])),
            )
        await db.execute(
            "DELETE FROM not_allow WHERE channel=? AND recipient_id=?",
            (row["channel"], row["recipient_id"]),
        )
    elif kind == "not_allowed":
        error_text = str(outcome["error"])
        await db.execute(
            """
            INSERT INTO not_allow(channel,recipient_id,reason,api_code,created_at,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(channel,recipient_id) DO UPDATE SET
                reason=excluded.reason,api_code=excluded.api_code,updated_at=excluded.updated_at
            """,
            (row["channel"], row["recipient_id"], error_text, outcome["api_code"], now, now),
        )
        changed = await db.execute(
            "UPDATE recipients SET status='not_allowed',attempts=?,next_attempt_at=0,last_error=?,updated_at=? "
            "WHERE id=? AND status IN ('sending','pending')",
            (attempt, error_text, now, recipient_row_id),
        )
        not_allowed_delta = 1 if changed.rowcount else 0
        await db.execute(
            "INSERT INTO delivery_attempts(campaign_id,recipient_id,attempt_no,status,error) VALUES(?,?,?,?,?)",
            (campaign_id, recipient_row_id, attempt, "not_allowed", error_text),
        )
    else:
        status = str(outcome["status"])
        changed = await db.execute(
            "UPDATE recipients SET status=?,attempts=?,next_attempt_at=?,last_error=?,updated_at=? "
            "WHERE id=? AND status IN ('sending','pending')",
            (
                status, attempt, float(outcome["next_at"]), str(outcome["error"]),
                now, recipient_row_id,
            ),
        )
        failed_delta = 1 if status == "failed" and changed.rowcount else 0
        await db.execute(
            "INSERT INTO delivery_attempts(campaign_id,recipient_id,attempt_no,status,error) VALUES(?,?,?,?,?)",
            (
                campaign_id, recipient_row_id, attempt, outcome["attempt_status"],
                str(outcome["error"]),
            ),
        )
    return sent_delta, failed_delta, not_allowed_delta


async def _flush_delivery_outcomes() -> None:
    global _delivery_flush_task
    try:
        while _delivery_write_queue:
            await asyncio.sleep(DELIVERY_WRITE_BATCH_WINDOW_SEC)
            batch = _delivery_write_queue[:DELIVERY_WRITE_BATCH_SIZE]
            del _delivery_write_queue[:len(batch)]
            try:
                deltas: dict[str, list[int]] = {}
                async with _write_db(operation=f"record {len(batch)} delivery outcomes") as db:
                    for outcome, _ in batch:
                        sent, failed, not_allowed = await _apply_delivery_outcome(db, outcome)
                        values = deltas.setdefault(str(outcome["row"]["campaign_id"]), [0, 0, 0])
                        values[0] += sent
                        values[1] += failed
                        values[2] += not_allowed
                    now = _now()
                    for campaign_id, (sent, failed, not_allowed) in deltas.items():
                        await db.execute(
                            """
                            UPDATE campaigns
                            SET sent=sent+?,failed=failed+?,not_allowed=not_allowed+?,
                                heartbeat_at=?,lease_until=CASE WHEN status='running' THEN ? ELSE lease_until END,
                                updated_at=?
                            WHERE id=?
                            """,
                            (sent, failed, not_allowed, now, time.time() + 30, now, campaign_id),
                        )
                for _, future in batch:
                    if not future.done():
                        future.set_result(None)
            except BaseException as exc:
                for _, future in batch:
                    if not future.done():
                        future.set_exception(exc)
                if isinstance(exc, asyncio.CancelledError):
                    raise
    finally:
        if _delivery_flush_task is asyncio.current_task():
            _delivery_flush_task = None


async def _queue_delivery_outcome(outcome: dict[str, Any]) -> None:
    global _delivery_flush_task
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    _delivery_write_queue.append((outcome, future))
    if _delivery_flush_task is None or _delivery_flush_task.done():
        _delivery_flush_task = asyncio.create_task(
            _flush_delivery_outcomes(), name="sbkvd-letter-delivery-writer",
        )
        setattr(builtins, "_nexus_sbkvd_letter_delivery_flush", _delivery_flush_task)
    await asyncio.shield(future)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _loads(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _model_dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump() if hasattr(model, "model_dump") else model.dict()


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS module_settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                audience_json TEXT NOT NULL,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS campaigns (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                segment_id INTEGER,
                template_item_id INTEGER,
                template_snapshot_json TEXT NOT NULL,
                audience_snapshot_json TEXT NOT NULL,
                scheduled_at TEXT,
                created_by TEXT NOT NULL DEFAULT '',
                total INTEGER NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                run_sent_baseline INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT,
                lease_until REAL,
                heartbeat_at TEXT,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_row_id INTEGER,
                source_json TEXT NOT NULL,
                rendered_content TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                external_message_id TEXT,
                sent_at TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                UNIQUE(campaign_id, channel, recipient_id)
            );
            CREATE TABLE IF NOT EXISTS delivery_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT NOT NULL,
                recipient_id INTEGER NOT NULL REFERENCES recipients(id) ON DELETE CASCADE,
                attempt_no INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                response_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT NOT NULL,
                recipient_row_id INTEGER NOT NULL UNIQUE REFERENCES recipients(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                external_message_id TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                deleted_at TEXT,
                delete_error TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS stop_list (
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                PRIMARY KEY(channel, recipient_id)
            );
            CREATE TABLE IF NOT EXISTS not_allow (
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                api_code TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                PRIMARY KEY(channel, recipient_id)
            );
            CREATE TABLE IF NOT EXISTS buyers (
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                match_value TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT 'getcourse_orders',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                PRIMARY KEY(channel, recipient_id)
            );
            CREATE TABLE IF NOT EXISTS buyer_terms (
                term TEXT NOT NULL,
                source TEXT NOT NULL,
                imported_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                PRIMARY KEY(term, source)
            );
            CREATE TABLE IF NOT EXISTS deletion_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_hash TEXT NOT NULL,
                search_content TEXT NOT NULL,
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                external_message_id TEXT NOT NULL,
                sent_at TEXT,
                deleted_at TEXT,
                delete_error TEXT NOT NULL DEFAULT '',
                discovered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                UNIQUE(content_hash, channel, external_message_id)
            );
            CREATE TABLE IF NOT EXISTS attachment_cache (
                channel TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                PRIMARY KEY(channel, file_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_campaign_status ON campaigns(status, scheduled_at, lease_until);
            CREATE INDEX IF NOT EXISTS idx_recipient_queue ON recipients(campaign_id, status, next_attempt_at);
            CREATE INDEX IF NOT EXISTS idx_attempt_recipient ON delivery_attempts(recipient_id, id DESC);
            CREATE INDEX IF NOT EXISTS idx_sent_campaign ON sent_messages(campaign_id);
            CREATE INDEX IF NOT EXISTS idx_deletion_matches_hash ON deletion_matches(content_hash, channel);
            """
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value_json) VALUES('config',?)",
            (_dump(DEFAULT_CONFIG),),
        )
        campaign_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(campaigns)")).fetchall()}
        if "segment_id" not in campaign_columns:
            await db.execute("ALTER TABLE campaigns ADD COLUMN segment_id INTEGER")
        if "not_allowed" not in campaign_columns:
            await db.execute("ALTER TABLE campaigns ADD COLUMN not_allowed INTEGER NOT NULL DEFAULT 0")
        if "run_sent_baseline" not in campaign_columns:
            await db.execute("ALTER TABLE campaigns ADD COLUMN run_sent_baseline INTEGER NOT NULL DEFAULT 0")
        await db.execute(
            "UPDATE campaigns SET status='queued', lease_owner=NULL, lease_until=NULL "
            "WHERE status='running'"
        )
        await db.execute(
            "UPDATE campaigns SET status='failed',last_error='Подготовка рассылки прервана перезапуском; создайте её повторно',"
            "lease_owner=NULL,lease_until=NULL,completed_at=?,updated_at=? WHERE status='preparing'",
            (_now(), _now()),
        )
        await db.execute(
            "UPDATE recipients SET status='failed',last_error='Состояние отправки неизвестно после перезапуска; повторите вручную',updated_at=? "
            "WHERE status='sending' AND campaign_id IN (SELECT id FROM campaigns WHERE status='queued')",
            (_now(),),
        )
        await db.commit()


async def _require_user(request: Request, *, edit: bool = False, admin: bool = False) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    if admin and user.get("role") != "admin":
        raise HTTPException(403, "Требуются права администратора")
    if edit and user.get("role") not in {"admin", "editor"}:
        raise HTTPException(403, "Недостаточно прав")
    return user


def _customer_db_path() -> Path:
    override = os.getenv("SBKVD_LETTER_CUSTOMER_DB_PATH", "").strip()
    if override:
        return Path(override)
    if _module_dir is None:
        raise RuntimeError("module is not initialized")
    candidates = [
        _module_dir.parent / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


def _file_storage_paths() -> tuple[Path, Path]:
    override = os.getenv("SBKVD_LETTER_FILE_STORAGE_DB_PATH", "").strip()
    if override:
        db_path = Path(override)
    else:
        if _module_dir is None:
            raise RuntimeError("module is not initialized")
        candidates = [
            _module_dir.parent / "file-storage" / "data" / "file-storage.db",
            _module_dir.parent.parent / "module_file_storage" / "data" / "file-storage.db",
            _module_dir.parent.parent / "modules" / "file-storage" / "data" / "file-storage.db",
        ]
        db_path = next((path for path in candidates if path.exists()), candidates[0])
    return db_path, db_path.parent / "blobs"


def _storage_service():
    return sys.modules.get("_nexus_mod_file-storage")


async def _storage_ensure_folder(name: str, parent_id: int = 1) -> int:
    service = _storage_service()
    if service and hasattr(service, "service_ensure_folder"):
        return int(await service.service_ensure_folder(name, parent_id))
    db_path, _ = _file_storage_paths()
    if not db_path.exists():
        raise RuntimeError("Файловое хранилище не установлено")
    now = _now()
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT id FROM items WHERE parent_id=? AND name=? AND kind='folder'", (parent_id, name))
        row = await cur.fetchone()
        if row:
            return int(row[0])
        cur = await db.execute(
            "INSERT INTO items(parent_id,kind,name,created_at,updated_at) VALUES(?,'folder',?,?,?)",
            (parent_id, name, now, now),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _storage_write(name: str, content: bytes, folder_id: int, mime_type: str, item_id: int | None = None) -> int:
    service = _storage_service()
    if service and hasattr(service, "service_write_file"):
        return int(await service.service_write_file(name, content, folder_id=folder_id, mime_type=mime_type, item_id=item_id))
    db_path, blob_dir = _file_storage_paths()
    blob_dir.mkdir(parents=True, exist_ok=True)
    now = _now()
    ext = name.rsplit(".", 1)[1].lower() if "." in name else ""
    async with aiosqlite.connect(db_path) as db:
        if item_id:
            row = await (await db.execute("SELECT stored_name FROM items WHERE id=? AND kind='file'", (item_id,))).fetchone()
            if not row:
                raise HTTPException(404, "Файл шаблона не найден")
            stored_name = row[0] or uuid.uuid4().hex
            tmp = blob_dir / f".{stored_name}.{uuid.uuid4().hex}.tmp"
            async with aiofiles.open(tmp, "wb") as fh:
                await fh.write(content)
            os.replace(tmp, blob_dir / stored_name)
            await db.execute(
                "UPDATE items SET name=?,stored_name=?,ext=?,mime_type=?,size=?,updated_at=? WHERE id=?",
                (name, stored_name, ext, mime_type, len(content), now, item_id),
            )
            await db.commit()
            return item_id
        stored_name = uuid.uuid4().hex
        async with aiofiles.open(blob_dir / stored_name, "wb") as fh:
            await fh.write(content)
        cur = await db.execute(
            "INSERT INTO items(parent_id,kind,name,stored_name,ext,mime_type,size,token,auth_required,created_at,updated_at) "
            "VALUES(?,'file',?,?,?,?,?,?,1,?,?)",
            (folder_id, name, stored_name, ext, mime_type, len(content), secrets.token_urlsafe(32), now, now),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _storage_read(item_id: int) -> tuple[dict[str, Any], bytes]:
    service = _storage_service()
    if service and hasattr(service, "service_read_file"):
        return await service.service_read_file(item_id)
    db_path, blob_dir = _file_storage_paths()
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM items WHERE id=? AND kind='file'", (item_id,))).fetchone()
    if not row or not row["stored_name"]:
        raise HTTPException(404, "Файл не найден")
    async with aiofiles.open(blob_dir / row["stored_name"], "rb") as fh:
        return dict(row), await fh.read()


async def _storage_delete(item_id: int) -> None:
    service = _storage_service()
    if service and hasattr(service, "service_delete_item"):
        await service.service_delete_item(item_id)
        return
    db_path, blob_dir = _file_storage_paths()
    async with aiosqlite.connect(db_path) as db:
        row = await (await db.execute("SELECT stored_name FROM items WHERE id=?", (item_id,))).fetchone()
        await db.execute("DELETE FROM items WHERE id=?", (item_id,))
        await db.commit()
    if row and row[0]:
        (blob_dir / row[0]).unlink(missing_ok=True)


async def _storage_list(folder_id: int) -> list[dict[str, Any]]:
    db_path, _ = _file_storage_paths()
    if not db_path.exists():
        return []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT id,name,size,mime_type,created_at,updated_at FROM items WHERE parent_id=? AND kind='file' ORDER BY updated_at DESC",
            (folder_id,),
        )).fetchall()
    return [dict(row) for row in rows]


async def _ensure_storage_folders() -> tuple[int, int, int]:
    root = await _storage_ensure_folder(FILE_FOLDER, 1)
    templates = await _storage_ensure_folder(TEMPLATE_FOLDER, root)
    attachments = await _storage_ensure_folder(ATTACHMENT_FOLDER, root)
    return root, templates, attachments


async def _get_config() -> dict[str, Any]:
    async with aiosqlite.connect(_must_db()) as db:
        row = await (await db.execute("SELECT value_json FROM module_settings WHERE key='config'")).fetchone()
    data = _loads(row[0] if row else "{}", {})
    return {**DEFAULT_CONFIG, **data}


async def _known_tables() -> list[dict[str, Any]]:
    path = _customer_db_path()
    if not path.exists():
        return []
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        try:
            rows = await (await db.execute("SELECT name,display_name,description,schema_json FROM _cdb_tables ORDER BY id")).fetchall()
        except Exception:
            return []
        result = []
        for row in rows:
            name = str(row["name"])
            if not SAFE_NAME.fullmatch(name):
                continue
            try:
                count = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{name}")).fetchone())[0]
            except Exception:
                count = 0
            result.append({**dict(row), "count": count, "schema": _loads(row["schema_json"], [])})
        return result


def _path_values(data: Any, path: str) -> list[Any]:
    if not path:
        return []
    parts = path.split(".")
    current = [data]
    for raw_part in parts:
        part = raw_part[:-2] if raw_part.endswith("[]") else raw_part
        next_values = []
        for value in current:
            if isinstance(value, dict) and part in value:
                found = value[part]
                next_values.extend(found if isinstance(found, list) else [found])
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and part in item:
                        found = item[part]
                        next_values.extend(found if isinstance(found, list) else [found])
        current = next_values
    return current


def _values(record: dict[str, Any], field: str) -> list[Any]:
    field = str(field or "").strip().strip(".")
    if field in SYSTEM_FIELDS:
        return [record.get(field)]
    return _path_values(record.get("custom_fields") or {}, field)


def _empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace(" ", "").replace(",", "."))
    except Exception:
        return None


def _condition_matches(record: dict[str, Any], condition: ConditionIn | dict[str, Any]) -> bool:
    data = _model_dump(condition) if isinstance(condition, BaseModel) else condition
    op = str(data.get("op") or "contains").lower()
    values = _values(record, str(data.get("field") or ""))
    expected = data.get("value")
    expected2 = data.get("value2")
    if op in {"empty", "is_empty"}:
        return not values or all(_empty(value) for value in values)
    if op in {"not_empty", "is_not_empty"}:
        return any(not _empty(value) for value in values)
    if not values:
        values = [None]
    expected_text = str(expected or "").casefold()
    expected_list = [item.strip().casefold() for item in str(expected or "").split(",") if item.strip()]
    per_value_matches = []
    for value in values:
        text = str(value or "")
        folded = text.casefold()
        value_num, first, second = _number(value), _number(expected), _number(expected2)
        matched = False
        if op in {"contains", "has"}:
            matched = expected_text in folded
        elif op == "not_contains":
            matched = expected_text not in folded
        elif op in {"eq", "equals"}:
            matched = folded == expected_text
        elif op in {"neq", "not_equals"}:
            matched = folded != expected_text
        elif op == "starts":
            matched = folded.startswith(expected_text)
        elif op == "ends":
            matched = folded.endswith(expected_text)
        elif op == "in":
            matched = folded in expected_list
        elif op == "gt":
            matched = value_num is not None and first is not None and value_num > first
        elif op == "lt":
            matched = value_num is not None and first is not None and value_num < first
        elif op == "between":
            matched = None not in {value_num, first, second} and first <= value_num <= second
        per_value_matches.append(matched)
    if op in {"not_contains", "neq", "not_equals"}:
        return all(per_value_matches)
    return any(per_value_matches)


def _matches(record: dict[str, Any], audience: AudienceIn | dict[str, Any]) -> bool:
    data = _model_dump(audience) if isinstance(audience, BaseModel) else audience
    conditions = data.get("conditions") or []
    if not conditions:
        return True
    matches = [_condition_matches(record, item) for item in conditions if (item.get("field") if isinstance(item, dict) else item.field)]
    if not matches:
        return True
    return any(matches) if str(data.get("mode") or "and").lower() == "or" else all(matches)


def _first_value(record: dict[str, Any], field: str) -> str:
    values = _values(record, field)
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _render(content: str, record: dict[str, Any]) -> tuple[str, list[str]]:
    missing = []
    def replace(match: re.Match) -> str:
        field = match.group(1)
        value = _first_value(record, field)
        if not value:
            missing.append(field)
        return value
    return VARIABLE_RE.sub(replace, content), sorted(set(missing))


class _PlainTextHTMLParser(HTMLParser):
    _BLOCK_TAGS = {"address", "article", "blockquote", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "p", "pre", "section", "tr"}
    _HIDDEN_TAGS = {"head", "script", "style"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        if tag in self._HIDDEN_TAGS:
            self.hidden_depth += 1
        elif tag == "br" and not self.hidden_depth:
            self.parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() == "br" and not self.hidden_depth:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag in self._HIDDEN_TAGS:
            self.hidden_depth = max(0, self.hidden_depth - 1)
        elif tag in self._BLOCK_TAGS and not self.hidden_depth:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth:
            self.parts.append(data)


def _normalize_plain_text(content: str) -> str:
    content = str(content or "").replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    content = "\n".join(line.rstrip() for line in content.split("\n"))
    return re.sub(r"\n{3,}", "\n\n", content).strip()


def _html_to_plain_text(content: str) -> str:
    parser = _PlainTextHTMLParser()
    try:
        parser.feed(str(content or ""))
        parser.close()
    except Exception:
        # HTMLParser is deliberately forgiving, but malformed upstream text
        # must never prevent a VK delivery.
        return _normalize_plain_text(re.sub(r"<[^>]*>", "", str(content or "")))
    return _normalize_plain_text("".join(parser.parts))


def _markdown_v2_to_plain_text(content: str) -> str:
    text = str(content or "")
    text = re.sub(r"```(?:[^\n`]*)\n?(.*?)```", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"(?<!\\)`([^`]*)`", r"\1", text, flags=re.DOTALL)

    def replace_link(match: re.Match) -> str:
        label = match.group(1)
        url = re.sub(r"\\([()])", r"\1", match.group(2))
        return label if label.strip() == url.strip() else f"{label} ({url})"

    text = re.sub(r"(?<!\\)!?\[([^\]]*)\]\(([^\s]+?)\)", replace_link, text)
    for pattern in (
        r"(?<!\\)\|\|(.*?)(?<!\\)\|\|",
        r"(?<!\\)__(.*?)(?<!\\)__",
        r"(?<!\\)\*(.*?)(?<!\\)\*",
        r"(?<!\\)_(.*?)(?<!\\)_",
        r"(?<!\\)~(.*?)(?<!\\)~",
    ):
        text = re.sub(pattern, r"\1", text, flags=re.DOTALL)
    text = re.sub(r"(?m)^(?<!\\)#{1,6}[ \t]+", "", text)
    text = re.sub(r"(?m)^(?<!\\)>[ \t]?", "", text)
    text = re.sub(r"\\([_*\[\]()~`>#+\-=|{}.!\\])", r"\1", text)
    return _normalize_plain_text(text)


def _content_for_channel(content: str, template: dict[str, Any], channel: str) -> str:
    if channel != "vk":
        return str(content or "")
    parse_mode = str(template.get("parse_mode") or "").strip()
    if parse_mode == "HTML":
        return _html_to_plain_text(content)
    if parse_mode == "MarkdownV2":
        return _markdown_v2_to_plain_text(content)
    return str(content or "")


async def _audience_records(audience: AudienceIn | dict[str, Any], limit: int | None = None) -> list[dict[str, Any]]:
    data = _model_dump(audience) if isinstance(audience, BaseModel) else audience
    config = await _get_config()
    allowed = {item["name"]: item for item in config.get("sheets", []) if item.get("enabled")}
    requested = [str(name) for name in data.get("tables", [])]
    table_names = requested
    invalid = [name for name in table_names if name not in allowed]
    if invalid:
        raise HTTPException(400, "Листы не разрешены: " + ", ".join(invalid))
    include_ids = {str(value).strip() for value in data.get("include_ids", []) if str(value).strip()}
    exclude_ids = {str(value).strip() for value in data.get("exclude_ids", []) if str(value).strip()}
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for manual in data.get("manual_recipients", []):
        manual_data = _model_dump(manual) if isinstance(manual, BaseModel) else manual
        channel = str(manual_data.get("channel") or "").strip().lower()
        recipient_id = str(manual_data.get("recipient_id") or "").strip()
        if channel not in {"vk", "telegram"} or not recipient_id or recipient_id in exclude_ids:
            continue
        key = (channel, recipient_id)
        if key in seen:
            continue
        seen.add(key)
        label = str(manual_data.get("label") or "").strip()
        custom_fields = {"name": label, "manual_recipient": True}
        result.append({
            "table": "manual", "id": None, "platform_id": recipient_id,
            "created_at": "", "updated_at": "", "custom_fields": custom_fields,
            "channel": channel, "recipient_id": recipient_id,
        })
        if limit and len(result) >= limit:
            return result
    path = _customer_db_path()
    if not path.exists():
        return result
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        for table in table_names:
            if not SAFE_NAME.fullmatch(table):
                continue
            sheet = allowed[table]
            async with db.execute(f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM cdb_{table} ORDER BY id") as cur:
                async for row in cur:
                    record = {
                        "table": table, "id": row["id"], "platform_id": row["platform_id"],
                        "created_at": row["created_at"], "updated_at": row["updated_at"],
                        "custom_fields": _loads(row["custom_fields"], {}),
                    }
                    recipient_id = _first_value(record, sheet.get("recipient_field") or "platform_id")
                    if not recipient_id:
                        continue
                    if recipient_id in exclude_ids:
                        continue
                    if recipient_id not in include_ids and not _matches(record, data):
                        continue
                    key = (sheet.get("channel") or "vk", recipient_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    result.append({**record, "channel": key[0], "recipient_id": recipient_id})
                    if limit and len(result) >= limit:
                        return result
    return result


async def _load_segment(segment_id: int) -> tuple[str, dict[str, Any]]:
    async with aiosqlite.connect(_must_db()) as db:
        row = await (await db.execute(
            "SELECT name,audience_json FROM segments WHERE id=?", (int(segment_id),)
        )).fetchone()
    if not row:
        raise HTTPException(404, f"Сегмент не найден: {segment_id}")
    return str(row[0]), _loads(row[1], {})


async def _resolve_campaign_records(
    data: CampaignIn,
) -> tuple[list[dict[str, Any]], dict[str, Any], int | None]:
    ids = [int(value) for value in data.segment_ids if int(value) > 0]
    if data.segment_id is not None and int(data.segment_id) > 0 and int(data.segment_id) not in ids:
        ids.insert(0, int(data.segment_id))
    if not ids:
        audience_data = _model_dump(data.audience)
        records = await _audience_records(audience_data)
        return records, {
            **audience_data,
            "segment_id": None,
            "segment_ids": [],
            "segment_names": [],
        }, None

    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    segment_names: list[str] = []
    segment_audiences: list[dict[str, Any]] = []
    for segment_id in ids:
        segment_name, audience = await _load_segment(segment_id)
        segment_names.append(segment_name)
        segment_audiences.append({
            "id": segment_id,
            "name": segment_name,
            "audience": audience,
        })
        for record in await _audience_records(audience):
            key = (str(record.get("channel") or ""), str(record.get("recipient_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
    return records, {
        "mode": "segments_union",
        "tables": [],
        "conditions": [],
        "include_ids": [],
        "exclude_ids": [],
        "manual_recipients": [],
        "segment_id": ids[0] if len(ids) == 1 else None,
        "segment_ids": ids,
        "segment_names": segment_names,
        "segments": segment_audiences,
    }, ids[0] if len(ids) == 1 else None


def _exclusion_options(data: Any) -> dict[str, bool]:
    return {
        "buyers": bool(getattr(data, "exclude_buyers", True)),
        "stop_list": bool(getattr(data, "exclude_stop_list", True)),
        "not_allow": bool(getattr(data, "exclude_not_allow", True)),
    }


def _snapshot_exclusion_options(snapshot: dict[str, Any]) -> dict[str, bool]:
    configured = snapshot.get("exclusions")
    if not isinstance(configured, dict):
        # Preserve the behavior of campaigns created before buyer exclusions existed.
        return {"buyers": False, "stop_list": True, "not_allow": True}
    return {
        "buyers": bool(configured.get("buyers", True)),
        "stop_list": bool(configured.get("stop_list", True)),
        "not_allow": bool(configured.get("not_allow", True)),
    }


def _retry_exclusion_sql(options: dict[str, bool]) -> str:
    clauses = []
    if options.get("buyers"):
        clauses.append(
            "AND NOT EXISTS (SELECT 1 FROM buyers b WHERE b.channel=r.channel AND b.recipient_id=r.recipient_id)"
        )
    if options.get("stop_list"):
        clauses.append(
            "AND NOT EXISTS (SELECT 1 FROM stop_list sl WHERE sl.channel=r.channel AND sl.recipient_id=r.recipient_id)"
        )
    if options.get("not_allow"):
        clauses.append(
            "AND NOT EXISTS (SELECT 1 FROM not_allow na WHERE na.channel=r.channel AND na.recipient_id=r.recipient_id)"
        )
    return "\n              ".join(clauses)


async def _apply_exclusions(
    records: list[dict[str, Any]], options: dict[str, bool]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    lookup: dict[str, set[tuple[str, str]]] = {
        "buyers": set(), "stop_list": set(), "not_allow": set(),
    }
    tables = {"buyers": "buyers", "stop_list": "stop_list", "not_allow": "not_allow"}
    async with aiosqlite.connect(_must_db()) as db:
        for name, table in tables.items():
            if not options.get(name, True):
                continue
            rows = await (await db.execute(f"SELECT channel,recipient_id FROM {table}")).fetchall()
            lookup[name] = {(str(row[0]), str(row[1])) for row in rows}
    excluded = {"buyers": 0, "stop_list": 0, "not_allow": 0, "total": 0}
    result: list[dict[str, Any]] = []
    for record in records:
        key = (str(record.get("channel") or ""), str(record.get("recipient_id") or ""))
        reason = next((name for name in ("buyers", "stop_list", "not_allow") if key in lookup[name]), "")
        if reason:
            excluded[reason] += 1
            excluded["total"] += 1
        else:
            result.append(record)
    return result, excluded


async def _load_template(item_id: int) -> dict[str, Any]:
    meta, content = await _storage_read(item_id)
    try:
        data = json.loads(content.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(500, f"Шаблон поврежден: {exc}")
    data["id"] = item_id
    data["updated_at"] = meta.get("updated_at")
    return data


async def _list_templates() -> list[dict[str, Any]]:
    _, folder_id, _ = await _ensure_storage_folders()
    result = []
    for item in await _storage_list(folder_id):
        try:
            template = await _load_template(int(item["id"]))
            result.append(template)
        except Exception as exc:
            _log("warning", "template item=%s ignored: %s", item.get("id"), exc)
    return result


def _clean_channels(channels: list[str]) -> list[str]:
    result = []
    for channel in channels:
        channel = str(channel).lower().strip()
        if channel in {"vk", "telegram"} and channel not in result:
            result.append(channel)
    return result


def _compile_universal_keyboard(keyboard: dict[str, Any], channel: str) -> dict[str, Any] | None:
    universal = keyboard.get("universal") if isinstance(keyboard, dict) else None
    if not isinstance(universal, dict):
        return None
    rows = universal.get("rows")
    if not isinstance(rows, list):
        return None
    compiled_rows = []
    for row in rows[:10]:
        if not isinstance(row, list):
            continue
        compiled_row = []
        for item in row[:4]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "Кнопка").strip()[:80]
            kind = str(item.get("type") or "text").strip().lower()
            value = str(item.get("value") or label).strip()
            color = str(item.get("color") or "secondary").strip().lower()
            if color not in {"primary", "secondary", "positive", "negative"}:
                color = "secondary"
            if channel == "vk":
                if kind == "link":
                    if not value.startswith(("http://", "https://")):
                        continue
                    compiled_row.append({"action": {"type": "open_link", "label": label, "link": value}})
                else:
                    action_type = "callback" if kind == "callback" else "text"
                    payload = _dump({"value": value})
                    compiled_row.append({"action": {"type": action_type, "label": label, "payload": payload}, "color": color})
            else:
                if kind == "link":
                    if not value.startswith(("http://", "https://")):
                        continue
                    compiled_row.append({"text": label, "url": value})
                else:
                    callback_data = value.encode("utf-8")[:64].decode("utf-8", errors="ignore") or label[:32]
                    compiled_row.append({"text": label, "callback_data": callback_data})
        if compiled_row:
            compiled_rows.append(compiled_row)
    if not compiled_rows:
        return None
    if channel == "vk":
        return {"inline": bool(universal.get("inline", True)), "buttons": compiled_rows}
    return {"inline_keyboard": compiled_rows}


def _keyboard_for_channel(template: dict[str, Any], channel: str) -> dict[str, Any] | str | None:
    keyboard = template.get("keyboard") or {}
    if not isinstance(keyboard, dict):
        return None
    explicit = keyboard.get(channel)
    if explicit:
        return explicit
    return _compile_universal_keyboard(keyboard, channel)


def _parse_schedule(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            # The panel uses datetime-local, which intentionally carries no
            # offset. Scheduling is an operator-facing Moscow-time contract,
            # independent of the host timezone (production runs in UTC).
            parsed = parsed.replace(tzinfo=MOSCOW_TIMEZONE)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        raise HTTPException(400, "Некорректное время запуска")


@router.get("/health")
async def health():
    rates = []
    for channel in ("vk", "telegram"):
        gate = _adaptive_rate_gates.get(channel)
        if gate is not None:
            rates.append(await gate.snapshot())
    return {
        "ok": True,
        "module": MODULE_ID,
        "worker": bool(_worker_task and not _worker_task.done()),
        "rates": rates,
    }


@router.get("/rate-status")
async def rate_status(request: Request):
    await _require_user(request)
    items = []
    for channel in ("vk", "telegram"):
        gate = _adaptive_rate_gates.get(channel)
        if gate is not None:
            items.append(await gate.snapshot())
    return {"items": items}


@router.get("/config")
async def get_config(request: Request):
    user = await _require_user(request)
    config = await _get_config()
    return {
        **config,
        "role": user.get("role"),
        "env": {
            "vk": bool(os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()),
            "telegram": bool(os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()),
        },
        "customer_db_ready": _customer_db_path().exists(),
        "file_storage_ready": _file_storage_paths()[0].exists(),
    }


def _nexus_env_path() -> Path:
    if _module_dir is None:
        raise RuntimeError("module is not initialized")
    return _module_dir.parent.parent / ".env"


def _write_env_values(updates: dict[str, str | None]) -> None:
    path = _nexus_env_path()
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = dict(updates)
    output = []
    for line in lines:
        if "=" not in line or line.lstrip().startswith("#"):
            output.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key not in remaining:
            output.append(line)
            continue
        value = remaining.pop(key)
        if value is not None:
            output.append(f"{key}={value}")
    for key, value in remaining.items():
        if value is not None:
            output.append(f"{key}={value}")
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text("\n".join(output) + "\n", encoding="utf-8")
    if path.exists():
        os.chmod(tmp, path.stat().st_mode & 0o777)
    os.replace(tmp, path)
    for key, value in updates.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@router.put("/tokens")
async def update_tokens(data: TokensIn, request: Request):
    user = await _require_user(request, admin=True)
    updates: dict[str, str | None] = {}
    vk_token = data.vk_token.strip()
    telegram_token = data.telegram_token.strip()
    if data.clear_vk:
        updates["SBKVD_LETTER_VK_TOKEN"] = None
    elif vk_token:
        if len(vk_token) < 20 or any(char.isspace() for char in vk_token):
            raise HTTPException(400, "Некорректный токен VK")
        updates["SBKVD_LETTER_VK_TOKEN"] = vk_token
    if data.clear_telegram:
        updates["SBKVD_LETTER_TELEGRAM_BOT_TOKEN"] = None
    elif telegram_token:
        if not re.fullmatch(r"\d{5,20}:[A-Za-z0-9_-]{20,}", telegram_token):
            raise HTTPException(400, "Некорректный Telegram bot token")
        updates["SBKVD_LETTER_TELEGRAM_BOT_TOKEN"] = telegram_token
    if updates:
        _write_env_values(updates)
        _log("info", "channel credentials updated by=%s keys=%s", user.get("username", "admin"), ",".join(sorted(updates)))
    return {
        "ok": True,
        "env": {
            "vk": bool(os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()),
            "telegram": bool(os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()),
        },
    }


@router.put("/config")
async def update_config(data: SettingsIn, request: Request):
    await _require_user(request, admin=True)
    known = {item["name"] for item in await _known_tables()}
    sheets = []
    for sheet in data.sheets:
        item = _model_dump(sheet)
        if item["name"] not in known or not SAFE_NAME.fullmatch(item["name"]):
            raise HTTPException(400, f"Неизвестный лист: {item['name']}")
        if item["channel"] not in {"vk", "telegram"}:
            raise HTTPException(400, "Канал должен быть vk или telegram")
        sheets.append(item)
    payload = {
        "sheets": sheets,
        "send_concurrency": max(1, min(100, data.send_concurrency)),
        "vk_rate_per_sec": max(0.2, min(20.0, data.vk_rate_per_sec)),
        "telegram_rate_per_sec": max(0.2, min(30.0, data.telegram_rate_per_sec)),
        "adaptive_rate_enabled": bool(data.adaptive_rate_enabled),
        "max_attempts": max(1, min(8, data.max_attempts)),
        "network_max_attempts": max(3, min(288, data.network_max_attempts)),
        "network_retry_base_sec": max(30, min(3600, data.network_retry_base_sec)),
        "network_retry_max_sec": max(60, min(21600, data.network_retry_max_sec)),
        "auto_pause_network_errors": max(0, min(200, data.auto_pause_network_errors)),
    }
    async with _write_db(operation="update settings") as db:
        await db.execute(
            "INSERT INTO module_settings(key,value_json,updated_at) VALUES('config',?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            (_dump(payload), _now()),
        )
    return {"ok": True, **payload}


@router.get("/tables")
async def tables(request: Request):
    await _require_user(request)
    config = await _get_config()
    configured = {item["name"]: item for item in config.get("sheets", [])}
    items = []
    for table in await _known_tables():
        table["config"] = configured.get(table["name"])
        items.append(table)
    return {"items": items}


@router.get("/fields")
async def fields(request: Request, tables: str = ""):
    await _require_user(request)
    config = await _get_config()
    allowed = {item["name"] for item in config.get("sheets", []) if item.get("enabled")}
    selected = [name for name in tables.split(",") if name in allowed] or list(allowed)
    found = set(SYSTEM_FIELDS)
    path = _customer_db_path()
    if path.exists():
        async with aiosqlite.connect(path) as db:
            for table in selected:
                rows = await (await db.execute(f"SELECT custom_fields FROM cdb_{table} ORDER BY id DESC LIMIT 200")).fetchall()
                for row in rows:
                    stack = [("", _loads(row[0], {}))]
                    while stack:
                        prefix, value = stack.pop()
                        if isinstance(value, dict):
                            for key, child in value.items():
                                field = f"{prefix}.{key}" if prefix else str(key)
                                found.add(field)
                                stack.append((field, child))
                        elif isinstance(value, list):
                            found.add(prefix + "[]")
                            for child in value[:3]:
                                stack.append((prefix + "[]", child))
    return {"items": sorted(found)}


@router.post("/audience/preview")
async def audience_preview(data: AudienceIn, request: Request):
    await _require_user(request)
    records = await _audience_records(data)
    per_channel: dict[str, int] = {}
    per_table: dict[str, int] = {}
    for item in records:
        per_channel[item["channel"]] = per_channel.get(item["channel"], 0) + 1
        per_table[item["table"]] = per_table.get(item["table"], 0) + 1
    return {"total": len(records), "per_channel": per_channel, "per_table": per_table, "items": records[:100]}


@router.post("/audience/segments-preview")
async def segments_audience_preview(data: SegmentPreviewIn, request: Request):
    await _require_user(request)
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    segment_ids = [int(value) for value in data.segment_ids if int(value) > 0]
    for segment_id in segment_ids:
        _, audience = await _load_segment(segment_id)
        for item in await _audience_records(audience):
            key = (str(item.get("channel") or ""), str(item.get("recipient_id") or ""))
            if key in seen:
                continue
            seen.add(key)
            records.append(item)
    total_before_exclusions = len(records)
    records, excluded = await _apply_exclusions(records, _exclusion_options(data))
    per_channel: dict[str, int] = {}
    per_table: dict[str, int] = {}
    for item in records:
        per_channel[item["channel"]] = per_channel.get(item["channel"], 0) + 1
        per_table[item["table"]] = per_table.get(item["table"], 0) + 1
    return {
        "total": len(records),
        "total_before_exclusions": total_before_exclusions,
        "excluded": excluded,
        "per_channel": per_channel,
        "per_table": per_table,
        "items": records[:100],
    }


@router.get("/segments")
async def segments(request: Request):
    await _require_user(request)
    async with _write_db(operation="create segment") as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM segments ORDER BY updated_at DESC,id DESC")).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        item["audience"] = _loads(item.pop("audience_json"), {})
        items.append(item)
    return {"items": items}


@router.post("/segments")
async def create_segment(data: SegmentIn, request: Request):
    user = await _require_user(request, edit=True)
    name = data.name.strip()[:160]
    if not name:
        raise HTTPException(400, "Название обязательно")
    async with _write_db(operation="create segment") as db:
        cur = await db.execute(
            "INSERT INTO segments(name,description,audience_json,created_by) VALUES(?,?,?,?)",
            (name, data.description.strip()[:1000], _dump(_model_dump(data.audience)), user.get("username", "")),
        )
    return {"ok": True, "id": int(cur.lastrowid)}


@router.put("/segments/{segment_id}")
async def update_segment(segment_id: int, data: SegmentIn, request: Request):
    await _require_user(request, edit=True)
    async with _write_db(operation="update segment") as db:
        cur = await db.execute(
            "UPDATE segments SET name=?,description=?,audience_json=?,updated_at=? WHERE id=?",
            (data.name.strip()[:160], data.description.strip()[:1000], _dump(_model_dump(data.audience)), _now(), segment_id),
        )
    if not cur.rowcount:
        raise HTTPException(404, "Сегмент не найден")
    return {"ok": True}


@router.delete("/segments/{segment_id}")
async def delete_segment(segment_id: int, request: Request):
    await _require_user(request, edit=True)
    async with _write_db(operation="delete segment") as db:
        await db.execute("DELETE FROM segments WHERE id=?", (segment_id,))
    return {"ok": True}


@router.get("/templates")
async def templates(request: Request):
    await _require_user(request)
    return {"items": await _list_templates()}


def _template_payload(data: TemplateIn, *, version: int = 1) -> dict[str, Any]:
    channels = _clean_channels(data.channels)
    if not channels:
        raise HTTPException(400, "Выберите хотя бы один канал")
    if not data.name.strip() or not data.content.strip():
        raise HTTPException(400, "Название и текст обязательны")
    return {
        "schema_version": 1, "version": version, "name": data.name.strip()[:160],
        "content": data.content, "channels": channels, "keyboard": data.keyboard,
        "attachment_ids": list(dict.fromkeys(data.attachment_ids)),
        "vk_attachment": data.vk_attachment.strip(), "parse_mode": data.parse_mode.strip(),
        "updated_at": _now(),
    }


@router.post("/templates")
async def create_template(data: TemplateIn, request: Request):
    await _require_user(request, edit=True)
    _, folder_id, _ = await _ensure_storage_folders()
    payload = _template_payload(data)
    filename = f"template-{uuid.uuid4().hex}.json"
    item_id = await _storage_write(filename, _dump(payload).encode("utf-8"), folder_id, "application/json")
    return {"ok": True, "id": item_id, **payload}


@router.put("/templates/{item_id}")
async def update_template(item_id: int, data: TemplateIn, request: Request):
    await _require_user(request, edit=True)
    current = await _load_template(item_id)
    payload = _template_payload(data, version=int(current.get("version") or 1) + 1)
    _, folder_id, _ = await _ensure_storage_folders()
    meta, _ = await _storage_read(item_id)
    await _storage_write(meta["name"], _dump(payload).encode("utf-8"), folder_id, "application/json", item_id=item_id)
    return {"ok": True, "id": item_id, **payload}


@router.delete("/templates/{item_id}")
async def delete_template(item_id: int, request: Request):
    await _require_user(request, edit=True)
    await _load_template(item_id)
    await _storage_delete(item_id)
    return {"ok": True}


@router.post("/attachments")
async def upload_attachment(request: Request, file: UploadFile = File(...)):
    await _require_user(request, edit=True)
    name = Path(file.filename or "attachment.bin").name[:140]
    content = await file.read()
    await file.close()
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(400, "Файл больше 50 МБ")
    _, _, folder_id = await _ensure_storage_folders()
    mime = file.content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    item_id = await _storage_write(f"{uuid.uuid4().hex[:8]}-{name}", content, folder_id, mime)
    return {"ok": True, "id": item_id, "name": name, "size": len(content), "mime_type": mime}


@router.get("/attachments/{item_id}")
async def preview_attachment(item_id: int, request: Request):
    await _require_user(request)
    meta, content = await _storage_read(item_id)
    mime = str(meta.get("mime_type") or "application/octet-stream")
    return Response(
        content=content,
        media_type=mime,
        headers={"Content-Disposition": f'inline; filename="attachment-{item_id}"', "Cache-Control": "private, max-age=300"},
    )


@router.post("/campaigns")
async def create_campaign(data: CampaignIn, request: Request):
    user = await _require_user(request, edit=True)
    all_records, audience_snapshot_base, stored_segment_id = await _resolve_campaign_records(data)
    if data.template_id:
        template = await _load_template(data.template_id)
        # The compose screen may start from a saved template and then make a
        # campaign-only edit. Preserve the reference while snapshotting edits.
        if data.content.strip():
            template["content"] = data.content
        if data.channels:
            template["channels"] = _clean_channels(data.channels)
        if data.keyboard:
            template["keyboard"] = data.keyboard
        if data.attachment_ids:
            template["attachment_ids"] = data.attachment_ids
        if data.vk_attachment.strip():
            template["vk_attachment"] = data.vk_attachment.strip()
        if data.parse_mode.strip():
            template["parse_mode"] = data.parse_mode.strip()
    else:
        template = {
            "name": data.name or "Без шаблона", "content": data.content,
            "channels": _clean_channels(data.channels), "keyboard": data.keyboard,
            "attachment_ids": data.attachment_ids, "vk_attachment": data.vk_attachment,
            "parse_mode": data.parse_mode, "version": 1,
        }
    if not str(template.get("content") or "").strip():
        raise HTTPException(400, "Текст сообщения обязателен")
    channels = _clean_channels(list(template.get("channels") or data.channels))
    records = [item for item in all_records if item["channel"] in channels]
    total_before_exclusions = len(records)
    exclusion_options = _exclusion_options(data)
    records, excluded = await _apply_exclusions(records, exclusion_options)
    if not records:
        raise HTTPException(400, "После исключений в аудитории нет получателей выбранных каналов")
    schedule = _parse_schedule(data.scheduled_at)
    status = "scheduled" if schedule and schedule > _now() else "queued"
    campaign_id = uuid.uuid4().hex
    name = (data.name or template.get("name") or "Рассылка").strip()[:160]
    unknown: set[str] = set()
    content = str(template["content"])
    personalized = bool(VARIABLE_RE.search(content))
    audience_snapshot = {
        **audience_snapshot_base,
        "resolved_at": _now(),
        "count": len(records),
        "count_before_exclusions": total_before_exclusions,
        "exclusions": exclusion_options,
        "excluded": excluded,
        "personalized": personalized,
    }
    async with _write_db(operation="create campaign") as db:
        await db.execute(
            "INSERT INTO campaigns(id,name,status,segment_id,template_item_id,template_snapshot_json,audience_snapshot_json,scheduled_at,created_by,total) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                campaign_id, name, "preparing", stored_segment_id, data.template_id,
                _dump(template), _dump(audience_snapshot), schedule,
                user.get("username", ""), len(records),
            ),
        )
    try:
        for offset in range(0, len(records), 5000):
            values = []
            for row in records[offset:offset + 5000]:
                if personalized:
                    rendered, missing = _render(content, row)
                    unknown.update(missing)
                    source_json = _dump(row)
                else:
                    # Static content already lives in the immutable campaign
                    # snapshot. Avoid duplicating it and the full customer
                    # payload hundreds of thousands of times.
                    rendered = ""
                    source_json = "{}"
                values.append(
                    (
                        campaign_id, row["channel"], row["recipient_id"],
                        row["table"], row["id"], source_json, rendered,
                    )
                )
            async with _write_db(operation="populate campaign recipients") as db:
                await db.executemany(
                    "INSERT OR IGNORE INTO recipients(campaign_id,channel,recipient_id,source_table,source_row_id,source_json,rendered_content) "
                    "VALUES(?,?,?,?,?,?,?)",
                    values,
                )
        async with _write_db(operation="activate prepared campaign") as db:
            await db.execute(
                "UPDATE campaigns SET status=?,updated_at=? WHERE id=? AND status='preparing'",
                (status, _now(), campaign_id),
            )
    except Exception:
        async with _write_db(operation="remove incomplete campaign") as db:
            await db.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
        raise
    _log("info", "campaign=%s created recipients=%s status=%s compact=%s", campaign_id, len(records), status, not personalized)
    return {
        "ok": True,
        "id": campaign_id,
        "status": status,
        "total": len(records),
        "excluded": excluded,
        "missing_variables": sorted(unknown),
    }


def _campaign_row(row: aiosqlite.Row, *, details: bool = False) -> dict[str, Any]:
    item = dict(row)
    if details:
        item["template_snapshot"] = _loads(item.pop("template_snapshot_json"), {})
        item["audience_snapshot"] = _loads(item.pop("audience_snapshot_json"), {})
    else:
        item.pop("template_snapshot_json", None)
        item.pop("audience_snapshot_json", None)
    return item


def _deletion_summary(total: int = 0, deleted: int = 0, errors: int = 0, *, active: bool = False) -> dict[str, Any]:
    total = int(total or 0)
    deleted = int(deleted or 0)
    remaining = max(0, total - deleted)
    return {
        "total": total,
        "deleted": deleted,
        "remaining": remaining,
        "errors": int(errors or 0),
        "active": bool(active),
        "percent_deleted": round((deleted / total * 100) if total else 0, 1),
    }


def _deletion_active(key: str) -> bool:
    task = _deletion_tasks.get(key)
    return bool(task and not task.done())


@router.get("/campaigns")
async def campaigns(request: Request, limit: int = 100, offset: int = 0):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        total = (await (await db.execute("SELECT COUNT(*) FROM campaigns")).fetchone())[0]
        rows = await (await db.execute(
            "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?", (max(1, min(500, limit)), max(0, offset))
        )).fetchall()
        deletion_rows = await (await db.execute(
            """
            SELECT campaign_id,COUNT(*) AS total,
                   SUM(CASE WHEN deleted_at IS NOT NULL THEN 1 ELSE 0 END) AS deleted,
                   SUM(CASE WHEN delete_error<>'' THEN 1 ELSE 0 END) AS errors
            FROM sent_messages GROUP BY campaign_id
            """
        )).fetchall()
    deletion_by_campaign = {str(row[0]): row for row in deletion_rows}
    items = []
    for row in rows:
        item = _campaign_row(row)
        deletion = deletion_by_campaign.get(str(item["id"]))
        item["message_deletion"] = _deletion_summary(
            deletion[1] if deletion else 0,
            deletion[2] if deletion else 0,
            deletion[3] if deletion else 0,
            active=_deletion_active(f"campaign:{item['id']}"),
        )
        items.append(item)
    return {"total": total, "items": items}


@router.get("/campaigns/{campaign_id}")
async def campaign_detail(campaign_id: str, request: Request, message_limit: int = 200):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Рассылка не найдена")
        recipients = await (await db.execute(
            "SELECT id,channel,recipient_id,source_table,status,attempts,last_error,external_message_id,sent_at "
            "FROM recipients WHERE campaign_id=? ORDER BY id DESC LIMIT ?", (campaign_id, max(1, min(1000, message_limit)))
        )).fetchall()
        retryable_count = 0
        if row["status"] in FINAL_STATUSES:
            exclusion_options = _snapshot_exclusion_options(_loads(row["audience_snapshot_json"], {}))
            exclusion_sql = _retry_exclusion_sql(exclusion_options)
            retryable_count = (await (await db.execute(
                f"""
                SELECT COUNT(*)
                FROM recipients r
                WHERE r.campaign_id=? AND r.status IN ('pending','skipped')
                  AND NOT EXISTS (SELECT 1 FROM sent_messages sm WHERE sm.recipient_row_id=r.id)
                  {exclusion_sql}
                """,
                (campaign_id,),
            )).fetchone())[0]
        # A paused campaign cannot start message deletion, but its existing
        # deletion progress is still useful as a read-only operational state.
        if row["status"] in ACTIVE_STATUSES and row["status"] != "paused":
            deletion = (0, 0, 0)
        else:
            deletion = await (await db.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN deleted_at IS NOT NULL THEN 1 ELSE 0 END) AS deleted,
                       SUM(CASE WHEN delete_error<>'' THEN 1 ELSE 0 END) AS errors
                FROM sent_messages WHERE campaign_id=?
                """,
                (campaign_id,),
            )).fetchone()
    return {
        "campaign": _campaign_row(row, details=True),
        "recipients": [dict(item) for item in recipients],
        "retryable_count": int(retryable_count or 0),
        "message_deletion": _deletion_summary(
            deletion[0], deletion[1], deletion[2],
            active=_deletion_active(f"campaign:{campaign_id}"),
        ),
    }


async def _set_campaign_status(campaign_id: str, allowed: set[str], status: str) -> bool:
    async with _write_db(operation=f"set campaign {status}") as db:
        placeholders = ",".join("?" for _ in allowed)
        cur = await db.execute(
            f"UPDATE campaigns SET status=?,lease_owner=NULL,lease_until=NULL,updated_at=? WHERE id=? AND status IN ({placeholders})",
            (status, _now(), campaign_id, *allowed),
        )
    return bool(cur.rowcount)


@router.post("/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"queued", "running", "scheduled"}, "paused"):
        raise HTTPException(409, "Рассылку нельзя приостановить")
    async with _write_db(operation="cancel campaign recipients") as db:
        remaining = (await (await db.execute(
            "SELECT COUNT(*) FROM recipients WHERE campaign_id=? AND status IN ('pending','sending')",
            (campaign_id,),
        )).fetchone())[0]
    return {"ok": True, "remaining": int(remaining or 0)}


@router.post("/campaigns/{campaign_id}/resume")
async def resume_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"paused"}, "queued"):
        raise HTTPException(409, "Рассылка не на паузе")
    async with aiosqlite.connect(_must_db()) as db:
        remaining = (await (await db.execute(
            "SELECT COUNT(*) FROM recipients WHERE campaign_id=? AND status IN ('pending','sending')",
            (campaign_id,),
        )).fetchone())[0]
    return {"ok": True, "remaining": int(remaining or 0)}


@router.post("/campaigns/{campaign_id}/cancel")
async def cancel_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"queued", "running", "scheduled", "paused"}, "cancelled"):
        raise HTTPException(409, "Рассылку нельзя отменить")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE recipients SET status='skipped',last_error='cancelled',updated_at=? "
            "WHERE campaign_id=? AND status='pending'",
            (_now(), campaign_id),
        )
    return {"ok": True}


async def _requeue_unsent_campaign(campaign_id: str, *, include_failed: bool = False) -> int:
    statuses = "'pending','failed','skipped'" if include_failed else "'pending','skipped'"
    async with _write_db(operation="requeue campaign") as db:
        campaign = await (await db.execute(
            "SELECT status,audience_snapshot_json FROM campaigns WHERE id=?", (campaign_id,)
        )).fetchone()
        if not campaign:
            raise HTTPException(404, "Рассылка не найдена")
        if str(campaign[0]) in ACTIVE_STATUSES:
            raise HTTPException(409, "Сначала поставьте рассылку на паузу или отмените её")
        exclusion_options = _snapshot_exclusion_options(_loads(campaign[1], {}))
        exclusion_sql = _retry_exclusion_sql(exclusion_options)
        eligible = (await (await db.execute(
            f"""
            SELECT COUNT(*)
            FROM recipients r
            WHERE r.campaign_id=? AND r.status IN ({statuses})
              AND NOT EXISTS (SELECT 1 FROM sent_messages sm WHERE sm.recipient_row_id=r.id)
              {exclusion_sql}
            """,
            (campaign_id,),
        )).fetchone())[0]
        if not eligible:
            raise HTTPException(409, "Нет неотправленных получателей для доотправки")
        await db.execute(
            f"""
            UPDATE recipients AS r
            SET status='pending',attempts=0,next_attempt_at=0,last_error='',updated_at=?
            WHERE r.campaign_id=? AND r.status IN ({statuses})
              AND NOT EXISTS (SELECT 1 FROM sent_messages sm WHERE sm.recipient_row_id=r.id)
              {exclusion_sql}
            """,
            (_now(), campaign_id),
        )
        counts = {
            row[0]: int(row[1])
            for row in await (await db.execute(
                "SELECT status,COUNT(*) FROM recipients WHERE campaign_id=? GROUP BY status",
                (campaign_id,),
            )).fetchall()
        }
        await db.execute(
            """
            UPDATE campaigns
            SET status='queued',sent=?,failed=?,skipped=?,not_allowed=?,last_error='',
                run_sent_baseline=?,lease_owner=NULL,lease_until=NULL,completed_at=NULL,updated_at=?
            WHERE id=?
            """,
            (
                counts.get("sent", 0), counts.get("failed", 0), counts.get("skipped", 0),
                counts.get("not_allowed", 0), counts.get("sent", 0), _now(), campaign_id,
            ),
        )
    return int(eligible)


@router.post("/campaigns/{campaign_id}/retry")
async def retry_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    count = await _requeue_unsent_campaign(campaign_id, include_failed=True)
    return {"ok": True, "count": count}


@router.post("/campaigns/{campaign_id}/continue")
async def continue_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    count = await _requeue_unsent_campaign(campaign_id, include_failed=False)
    return {"ok": True, "count": count}


@router.delete("/campaigns/{campaign_id}")
async def clear_campaign(campaign_id: str, request: Request):
    await _require_user(request, admin=True)
    async with _write_db(operation="delete campaign history") as db:
        status = await (await db.execute("SELECT status FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
        if status and status[0] in ACTIVE_STATUSES:
            raise HTTPException(409, "Сначала отмените рассылку")
        await db.execute("DELETE FROM delivery_attempts WHERE campaign_id=?", (campaign_id,))
        await db.execute("DELETE FROM sent_messages WHERE campaign_id=?", (campaign_id,))
        await db.execute("DELETE FROM recipients WHERE campaign_id=?", (campaign_id,))
        await db.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
    return {"ok": True}


async def _delete_message_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deleted = 0
    errors: list[str] = []
    external_vk_rows = [row for row in rows if row.get("_tracking") == "deletion_matches"]
    stored_rows = [row for row in rows if row.get("_tracking") != "deletion_matches"]
    vk_rows = [row for row in stored_rows if row["channel"] == "vk"]
    other_rows = [row for row in stored_rows if row["channel"] != "vk"]
    if vk_rows:
        async def persist_vk_batch(batch_deleted: set[int], batch_errors: dict[int, str]) -> None:
            async with _write_db(operation="persist VK deletion batch") as db:
                if batch_deleted:
                    placeholders = ",".join("?" for _ in batch_deleted)
                    await db.execute(
                        f"UPDATE sent_messages SET deleted_at=?,delete_error='' WHERE id IN ({placeholders})",
                        (_now(), *sorted(batch_deleted)),
                    )
                for row_id, message in batch_errors.items():
                    await db.execute(
                        "UPDATE sent_messages SET delete_error=? WHERE id=?",
                        (message[:1000], int(row_id)),
                    )

        vk_deleted, vk_errors = await _delete_vk_messages(vk_rows, on_batch=persist_vk_batch)
        deleted += len(vk_deleted)
        errors.extend(f"vk:{row_id}: {message}" for row_id, message in vk_errors.items())
    if external_vk_rows:
        async def persist_external_vk_batch(batch_deleted: set[int], batch_errors: dict[int, str]) -> None:
            async with _write_db(operation="persist VK history deletion batch") as db:
                if batch_deleted:
                    placeholders = ",".join("?" for _ in batch_deleted)
                    await db.execute(
                        f"UPDATE deletion_matches SET deleted_at=?,delete_error='',updated_at=? WHERE id IN ({placeholders})",
                        (_now(), _now(), *sorted(batch_deleted)),
                    )
                for row_id, message in batch_errors.items():
                    await db.execute(
                        "UPDATE deletion_matches SET delete_error=?,updated_at=? WHERE id=?",
                        (message[:1000], _now(), int(row_id)),
                    )

        external_deleted, external_errors = await _delete_vk_messages(
            external_vk_rows, on_batch=persist_external_vk_batch
        )
        deleted += len(external_deleted)
        errors.extend(f"vk-history:{row_id}: {message}" for row_id, message in external_errors.items())
    for row in other_rows:
        try:
            await _delete_remote(row)
            async with _write_db(operation="persist deleted message") as db:
                await db.execute("UPDATE sent_messages SET deleted_at=?,delete_error='' WHERE id=?", (_now(), row["id"]))
            deleted += 1
        except Exception as exc:
            errors.append(f"{row['channel']}:{row['recipient_id']}: {exc}")
            async with _write_db(operation="persist message deletion error") as db:
                await db.execute("UPDATE sent_messages SET delete_error=? WHERE id=?", (str(exc)[:1000], row["id"]))
    return {"ok": not errors, "deleted": deleted, "errors": errors[:100]}


async def _run_deletion_task(key: str, loader) -> None:
    global _deletion_lock
    if _deletion_lock is None:
        _deletion_lock = asyncio.Lock()
    try:
        async with _deletion_lock:
            rows = await loader()
            result = await _delete_message_rows(rows)
            _log(
                "info", "deletion key=%s finished deleted=%s errors=%s",
                key, result["deleted"], len(result["errors"]),
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log("exception", "deletion key=%s failed: %s", key, exc)
    finally:
        _deletion_tasks.pop(key, None)


def _start_deletion_task(key: str, loader) -> bool:
    if _deletion_active(key):
        return False
    task = asyncio.create_task(_run_deletion_task(key, loader), name=f"sbkvd-letter-delete-{key[-16:]}")
    _deletion_tasks[key] = task
    return True


@router.post("/campaigns/{campaign_id}/messages/delete")
async def delete_remote_messages(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        campaign = await (await db.execute(
            "SELECT status FROM campaigns WHERE id=?", (campaign_id,)
        )).fetchone()
        if not campaign:
            raise HTTPException(404, "Рассылка не найдена")
        if str(campaign[0]) in ACTIVE_STATUSES:
            raise HTTPException(409, "Сначала завершите или отмените рассылку")
        remaining = (await (await db.execute(
            "SELECT COUNT(*) FROM sent_messages WHERE campaign_id=? AND deleted_at IS NULL",
            (campaign_id,),
        )).fetchone())[0]

    async def load_rows() -> list[dict[str, Any]]:
        async with aiosqlite.connect(_must_db(), timeout=30) as db:
            await db.execute("PRAGMA busy_timeout=30000")
            db.row_factory = aiosqlite.Row
            rows = await (await db.execute(
                "SELECT * FROM sent_messages WHERE campaign_id=? AND deleted_at IS NULL ORDER BY id",
                (campaign_id,),
            )).fetchall()
        return [dict(row) for row in rows]

    key = f"campaign:{campaign_id}"
    started = bool(remaining) and _start_deletion_task(key, load_rows)
    return {
        "ok": True,
        "started": started,
        "active": _deletion_active(key),
        "remaining": int(remaining or 0),
    }


def _content_key(content: str) -> str:
    return "content:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _vk_search_query(content: str) -> str:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    return (lines[0] if lines else content.strip())[:512]


async def _discover_vk_deletion_matches(content: str) -> dict[str, Any]:
    token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
    if not token:
        return {"found": 0, "remote_error": "VK token не настроен"}
    query = _vk_search_query(content)
    matches: dict[str, dict[str, Any]] = {}
    total = 0
    gate = RateGate(3.0)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            for offset in range(0, 10000, 100):
                await gate.wait()
                response = await client.post(
                    "https://api.vk.com/method/messages.search",
                    data={
                        "access_token": token,
                        "v": "5.199",
                        "q": query,
                        "count": 100,
                        "offset": offset,
                        "extended": 0,
                    },
                )
                response.raise_for_status()
                data = response.json()
                if "response" not in data:
                    raise _api_error(data, "vk")
                payload = data["response"] if isinstance(data["response"], dict) else {}
                total = int(payload.get("count") or 0)
                items = payload.get("items") if isinstance(payload.get("items"), list) else []
                for item in items:
                    message_id = str(item.get("id") or "").strip()
                    text = str(item.get("text") or "")
                    if not message_id or int(item.get("out") or 0) != 1 or content not in text:
                        continue
                    matches[message_id] = {
                        "recipient_id": str(item.get("peer_id") or ""),
                        "external_message_id": message_id,
                        "sent_at": datetime.fromtimestamp(
                            int(item.get("date") or 0), tz=timezone.utc
                        ).strftime("%Y-%m-%dT%H:%M:%SZ") if item.get("date") else None,
                    }
                if not items or offset + len(items) >= total:
                    break
            else:
                raise RuntimeError("VK нашёл больше 10 000 сообщений; укажите более точный фрагмент")
    except Exception as exc:
        return {"found": len(matches), "remote_error": _delivery_error_text(exc)}

    content_hash = _content_hash(content)
    now = _now()
    if matches:
        async with _write_db(operation="store VK deletion discovery") as db:
            await db.executemany(
                """
                INSERT INTO deletion_matches(
                    content_hash,search_content,channel,recipient_id,external_message_id,sent_at,updated_at
                ) VALUES(?,?,'vk',?,?,?,?)
                ON CONFLICT(content_hash,channel,external_message_id) DO UPDATE SET
                    recipient_id=excluded.recipient_id,
                    sent_at=excluded.sent_at,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        content_hash, content, item["recipient_id"], item["external_message_id"],
                        item["sent_at"], now,
                    )
                    for item in matches.values()
                ],
            )
    return {"found": len(matches), "remote_error": "", "vk_search_total": total}


async def _content_deletion_summary(content: str) -> dict[str, Any]:
    key = _content_key(content)
    content_hash = _content_hash(content)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        stored = await (await db.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN sm.deleted_at IS NOT NULL THEN 1 ELSE 0 END) AS deleted,
                   SUM(CASE WHEN sm.delete_error<>'' THEN 1 ELSE 0 END) AS errors,
                   COUNT(DISTINCT sm.campaign_id) AS campaigns
            FROM sent_messages sm
            JOIN recipients r ON r.id=sm.recipient_row_id
            WHERE instr(r.rendered_content,?)>0
            """,
            (content,),
        )).fetchone()
        external = await (await db.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN dm.deleted_at IS NOT NULL THEN 1 ELSE 0 END) AS deleted,
                   SUM(CASE WHEN dm.delete_error<>'' THEN 1 ELSE 0 END) AS errors
            FROM deletion_matches dm
            WHERE dm.content_hash=? AND dm.search_content=?
              AND NOT EXISTS (
                  SELECT 1 FROM sent_messages sm
                  WHERE sm.channel=dm.channel AND sm.external_message_id=dm.external_message_id
              )
            """,
            (content_hash, content),
        )).fetchone()
        channels = {
            str(item[0]): int(item[1])
            for item in await (await db.execute(
                """
                SELECT channel,SUM(amount) FROM (
                    SELECT sm.channel AS channel,COUNT(*) AS amount
                    FROM sent_messages sm
                    JOIN recipients r ON r.id=sm.recipient_row_id
                    WHERE instr(r.rendered_content,?)>0 AND sm.deleted_at IS NULL
                    GROUP BY sm.channel
                    UNION ALL
                    SELECT dm.channel AS channel,COUNT(*) AS amount
                    FROM deletion_matches dm
                    WHERE dm.content_hash=? AND dm.search_content=? AND dm.deleted_at IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM sent_messages sm
                          WHERE sm.channel=dm.channel AND sm.external_message_id=dm.external_message_id
                      )
                    GROUP BY dm.channel
                ) GROUP BY channel
                """,
                (content, content_hash, content),
            )).fetchall()
        }
        stored_samples = await (await db.execute(
            """
            SELECT sm.campaign_id,c.name,sm.channel,sm.recipient_id,r.rendered_content,
                   sm.deleted_at,sm.delete_error,'module' AS source
            FROM sent_messages sm
            JOIN recipients r ON r.id=sm.recipient_row_id
            JOIN campaigns c ON c.id=sm.campaign_id
            WHERE instr(r.rendered_content,?)>0
            ORDER BY sm.id DESC LIMIT 20
            """,
            (content,),
        )).fetchall()
        external_samples = await (await db.execute(
            """
            SELECT '' AS campaign_id,'История VK' AS name,dm.channel,dm.recipient_id,
                   dm.search_content AS rendered_content,dm.deleted_at,dm.delete_error,'vk_history' AS source
            FROM deletion_matches dm
            WHERE dm.content_hash=? AND dm.search_content=?
              AND NOT EXISTS (
                  SELECT 1 FROM sent_messages sm
                  WHERE sm.channel=dm.channel AND sm.external_message_id=dm.external_message_id
              )
            ORDER BY dm.id DESC LIMIT 20
            """,
            (content_hash, content),
        )).fetchall()
    total = int(stored["total"] or 0) + int(external["total"] or 0)
    deleted = int(stored["deleted"] or 0) + int(external["deleted"] or 0)
    errors = int(stored["errors"] or 0) + int(external["errors"] or 0)
    summary = _deletion_summary(total, deleted, errors, active=_deletion_active(key))
    summary.update({
        "campaigns": int(stored["campaigns"] or 0),
        "remaining_by_channel": channels,
        "sources": {
            "module": int(stored["total"] or 0),
            "vk_history": int(external["total"] or 0),
        },
        "items": [dict(item) for item in list(stored_samples) + list(external_samples)][:20],
        "key": key,
    })
    return summary


def _validated_deletion_content(value: str) -> str:
    content = str(value or "").strip()
    if len(content) < 20:
        raise HTTPException(400, "Для безопасного поиска укажите минимум 20 символов сообщения")
    if len(content) > 20000:
        raise HTTPException(400, "Слишком длинный поисковый текст")
    return content


@router.post("/deletions/search")
async def search_messages_for_deletion(data: ContentDeletionIn, request: Request):
    await _require_user(request)
    content = _validated_deletion_content(data.content)
    discovery = {"found": 0, "remote_error": ""}
    if data.refresh_remote and not _deletion_active(_content_key(content)):
        discovery = await _discover_vk_deletion_matches(content)
    return {**await _content_deletion_summary(content), **discovery}


@router.post("/deletions/by-content")
async def delete_messages_by_content(data: ContentDeletionIn, request: Request):
    await _require_user(request, edit=True)
    content = _validated_deletion_content(data.content)
    summary = await _content_deletion_summary(content)
    if data.expected_remaining is None:
        raise HTTPException(400, "Сначала выполните подсчёт сообщений")
    if int(data.expected_remaining) != int(summary["remaining"]):
        raise HTTPException(409, "Количество изменилось; повторите подсчёт перед удалением")
    if not summary["remaining"]:
        return {**summary, "started": False}

    async def load_rows() -> list[dict[str, Any]]:
        async with aiosqlite.connect(_must_db()) as db:
            db.row_factory = aiosqlite.Row
            stored_rows = await (await db.execute(
                """
                SELECT sm.*
                FROM sent_messages sm
                JOIN recipients r ON r.id=sm.recipient_row_id
                WHERE instr(r.rendered_content,?)>0 AND sm.deleted_at IS NULL
                ORDER BY sm.id
                """,
                (content,),
            )).fetchall()
            external_rows = await (await db.execute(
                """
                SELECT id,channel,recipient_id,external_message_id
                FROM deletion_matches dm
                WHERE dm.content_hash=? AND dm.search_content=? AND dm.deleted_at IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM sent_messages sm
                      WHERE sm.channel=dm.channel AND sm.external_message_id=dm.external_message_id
                  )
                ORDER BY id
                """,
                (_content_hash(content), content),
            )).fetchall()
        return [dict(row) for row in stored_rows] + [
            {**dict(row), "_tracking": "deletion_matches"} for row in external_rows
        ]

    key = _content_key(content)
    started = _start_deletion_task(key, load_rows)
    return {**summary, "started": started, "active": _deletion_active(key)}


def _has_payment(fields: dict[str, Any]) -> bool:
    state = str(fields.get("payment_state") or "").strip().lower()
    try:
        paid_money = float(str(fields.get("payed_money") or "0").replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        paid_money = 0.0
    return paid_money > 0 or state in {"paid", "partial"}


async def _buyers_summary() -> dict[str, Any]:
    async with aiosqlite.connect(_must_db()) as db:
        rows = await (await db.execute(
            "SELECT channel,COUNT(*) FROM buyers GROUP BY channel"
        )).fetchall()
        meta_row = await (await db.execute(
            "SELECT value_json FROM module_settings WHERE key='buyers_meta'"
        )).fetchone()
    per_channel = {str(row[0]): int(row[1]) for row in rows}
    meta = _loads(meta_row[0] if meta_row else "{}", {})
    return {
        "total": sum(per_channel.values()),
        "per_channel": per_channel,
        **meta,
    }


async def _refresh_buyers_cache() -> dict[str, Any]:
    path = _customer_db_path()
    if not path.exists():
        raise HTTPException(503, "База клиентов недоступна")
    qualifying_orders = 0
    live_paid_terms: set[str] = set()
    buyers: dict[tuple[str, str], tuple[str, str]] = {}
    matched_paid_terms: set[str] = set()
    async with aiosqlite.connect(path) as source:
        try:
            order_rows = await (await source.execute(
                "SELECT custom_fields FROM cdb_getcourse_orders"
            )).fetchall()
        except Exception as exc:
            raise HTTPException(503, f"Лист заказов GetCourse недоступен: {exc}")
        for row in order_rows:
            fields = _loads(row[0], {})
            if not isinstance(fields, dict) or not _has_payment(fields):
                continue
            qualifying_orders += 1
            term = str(fields.get("utm_term") or "").strip()
            if term.isdigit():
                live_paid_terms.add(term)
        async with aiosqlite.connect(_must_db()) as module_db:
            imported_rows = await (await module_db.execute(
                "SELECT term,source FROM buyer_terms ORDER BY source,term"
            )).fetchall()
        imported_sources: dict[str, set[str]] = {}
        for term, source_name in imported_rows:
            term = str(term or "").strip()
            source_name = str(source_name or "import").strip() or "import"
            if term.isdigit():
                imported_sources.setdefault(source_name, set()).add(term)
        imported_paid_terms = {
            term for source_terms in imported_sources.values() for term in source_terms
        }
        paid_terms = live_paid_terms | imported_paid_terms

        def source_for(term: str) -> str:
            sources = [name for name, terms in imported_sources.items() if term in terms]
            if term in live_paid_terms:
                sources.insert(0, "getcourse_orders")
            return "+".join(sources) or "getcourse_orders"

        if paid_terms:
            async with source.execute("SELECT platform_id FROM cdb_vk_clients") as cursor:
                async for row in cursor:
                    platform_id = str(row[0] or "").strip()
                    if platform_id in paid_terms:
                        matched_paid_terms.add(platform_id)
                        buyers[("vk", platform_id)] = (platform_id, source_for(platform_id))
            async with source.execute("SELECT platform_id,custom_fields FROM cdb_telegram_clients") as cursor:
                async for row in cursor:
                    platform_id = str(row[0] or "").strip()
                    fields = _loads(row[1], {})
                    possible = fields.get("possible_accounts") if isinstance(fields, dict) else {}
                    candidate_values = [
                        platform_id,
                        str(fields.get("salebot_id") or "").strip() if isinstance(fields, dict) else "",
                        str(possible.get("salebot_id") or "").strip() if isinstance(possible, dict) else "",
                    ]
                    matched_values = [value for value in candidate_values if value in paid_terms]
                    if matched_values and platform_id:
                        matched_paid_terms.update(matched_values)
                        matched = matched_values[0]
                        buyers[("telegram", platform_id)] = (matched, source_for(matched))
    matched_terms = matched_paid_terms
    now = _now()
    per_channel: dict[str, int] = {}
    for channel, _ in buyers:
        per_channel[channel] = per_channel.get(channel, 0) + 1
    meta = {
        "updated_at": now,
        "qualifying_orders": qualifying_orders,
        "live_paid_terms": len(live_paid_terms),
        "imported_paid_terms": len(imported_paid_terms),
        "imported_sources": {name: len(terms) for name, terms in imported_sources.items()},
        "paid_terms": len(paid_terms),
        "matched_terms": len(matched_terms),
        "unmatched_terms": len(paid_terms - matched_terms),
    }
    async with _write_db(operation="refresh buyers cache") as db:
        await db.execute("DELETE FROM buyers")
        if buyers:
            await db.executemany(
                "INSERT INTO buyers(channel,recipient_id,match_value,source,updated_at) VALUES(?,?,?,?,?)",
                [
                    (channel, recipient_id, match_value, source_name, now)
                    for (channel, recipient_id), (match_value, source_name) in buyers.items()
                ],
            )
        await db.execute(
            "INSERT INTO module_settings(key,value_json,updated_at) VALUES('buyers_meta',?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            (_dump(meta), now),
        )
    _log(
        "info", "buyers refreshed total=%s vk=%s telegram=%s terms=%s unmatched=%s",
        len(buyers), per_channel.get("vk", 0), per_channel.get("telegram", 0),
        len(paid_terms), meta["unmatched_terms"],
    )
    return {"ok": True, "total": len(buyers), "per_channel": per_channel, **meta}


@router.get("/buyers")
async def buyers_summary(request: Request):
    await _require_user(request)
    return await _buyers_summary()


@router.post("/buyers/refresh")
async def refresh_buyers(request: Request):
    await _require_user(request, admin=True)
    return await _refresh_buyers_cache()


@router.get("/stop-list")
async def stop_list(request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute("SELECT * FROM stop_list ORDER BY created_at DESC")).fetchall()
    return {"items": [dict(row) for row in rows]}


@router.post("/stop-list/{channel}/{recipient_id}")
async def add_stop(channel: str, recipient_id: str, request: Request):
    await _require_user(request, edit=True)
    if channel not in {"vk", "telegram"}:
        raise HTTPException(400, "Некорректный канал")
    async with _write_db(operation="add stop-list recipient") as db:
        await db.execute("INSERT OR IGNORE INTO stop_list(channel,recipient_id) VALUES(?,?)", (channel, recipient_id))
    return {"ok": True}


@router.delete("/stop-list/{channel}/{recipient_id}")
async def remove_stop(channel: str, recipient_id: str, request: Request):
    await _require_user(request, edit=True)
    async with _write_db(operation="remove stop-list recipient") as db:
        await db.execute("DELETE FROM stop_list WHERE channel=? AND recipient_id=?", (channel, recipient_id))
    return {"ok": True}


@router.get("/not-allow")
async def not_allow_list(request: Request, limit: int = 500, offset: int = 0):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        total = (await (await db.execute("SELECT COUNT(*) FROM not_allow")).fetchone())[0]
        per_channel = {
            row[0]: int(row[1])
            for row in await (await db.execute(
                "SELECT channel,COUNT(*) FROM not_allow GROUP BY channel"
            )).fetchall()
        }
        rows = await (await db.execute(
            "SELECT * FROM not_allow ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            (max(1, min(2000, int(limit))), max(0, int(offset))),
        )).fetchall()
    return {"total": int(total or 0), "per_channel": per_channel, "items": [dict(row) for row in rows]}


@router.delete("/not-allow/{channel}/{recipient_id}")
async def remove_not_allow(channel: str, recipient_id: str, request: Request):
    await _require_user(request, edit=True)
    async with _write_db(operation="remove not-allow recipient") as db:
        await db.execute(
            "DELETE FROM not_allow WHERE channel=? AND recipient_id=?",
            (channel, recipient_id),
        )
    return {"ok": True}


class RateGate:
    def __init__(self, rate: float):
        self.interval = 1.0 / max(0.1, rate)
        self.lock = asyncio.Lock()
        self.next_at = 0.0

    async def wait(self):
        async with self.lock:
            delay = self.next_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self.next_at = time.monotonic() + self.interval


class AdaptiveRateGate:
    """Shared per-channel AIMD limiter.

    The configured rate is a ceiling, not a promise that it is currently safe.
    A rate-limit response applies one channel-wide cooldown and multiplicatively
    lowers the effective rate. Error-free traffic restores it in small steps.
    """

    def __init__(self, channel: str, ceiling: float, *, enabled: bool = True):
        self.channel = channel
        self.ceiling = max(0.2, float(ceiling))
        self.floor = min(self.ceiling, max(0.5, self.ceiling * 0.10))
        self.effective_rate = self.ceiling
        self.enabled = bool(enabled)
        self.lock = asyncio.Lock()
        self.next_at = 0.0
        self.blocked_until = 0.0
        self.last_limit_at = 0.0
        self.last_recovery_at = time.monotonic()
        self.successes_since_recovery = 0
        self.limit_events = 0

    async def wait(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                ready_at = max(self.next_at, self.blocked_until)
                delay = ready_at - now
                if delay <= 0:
                    rate = self.effective_rate if self.enabled else self.ceiling
                    self.next_at = now + (1.0 / max(0.1, rate))
                    return
            await asyncio.sleep(delay)

    async def on_success(self) -> None:
        if not self.enabled:
            return
        recovered_from = 0.0
        recovered_to = 0.0
        async with self.lock:
            now = time.monotonic()
            self.successes_since_recovery += 1
            threshold = max(20, int(math.ceil(self.effective_rate * 3.0)))
            if (
                self.effective_rate < self.ceiling
                and now >= self.blocked_until
                and now - self.last_limit_at >= 5.0
                and now - self.last_recovery_at >= 5.0
                and self.successes_since_recovery >= threshold
            ):
                recovered_from = self.effective_rate
                self.effective_rate = min(
                    self.ceiling,
                    self.effective_rate + max(0.2, self.ceiling * 0.05),
                )
                recovered_to = self.effective_rate
                self.successes_since_recovery = 0
                self.last_recovery_at = now
        if recovered_to > recovered_from:
            _log(
                "info", "adaptive-rate channel=%s recovery %.2f->%.2f msg/s ceiling=%.2f",
                self.channel, recovered_from, recovered_to, self.ceiling,
            )

    async def on_rate_limit(self, exc: "TransientDeliveryError") -> float:
        retry_after = float(getattr(exc, "retry_after", 0.0) or 0.0)
        fallback = 2.0 if self.channel == "telegram" else 1.5
        cooldown = max(fallback, min(120.0, retry_after))
        old_rate = self.effective_rate
        reduced = False
        async with self.lock:
            now = time.monotonic()
            # Concurrent requests often return the same 429 burst. Treat that as
            # one congestion event instead of multiplying the penalty per task.
            if now - self.last_limit_at >= max(1.0, min(10.0, cooldown)):
                self.effective_rate = max(self.floor, self.effective_rate * 0.60)
                reduced = self.effective_rate < old_rate
                self.limit_events += 1
            self.last_limit_at = now
            self.blocked_until = max(self.blocked_until, now + cooldown)
            self.next_at = max(self.next_at, self.blocked_until)
            self.successes_since_recovery = 0
        _log(
            "warning",
            "adaptive-rate channel=%s limited cooldown=%.1fs rate=%.2f->%.2f msg/s event=%s error=%s",
            self.channel, cooldown, old_rate, self.effective_rate,
            "reduced" if reduced else "coalesced", _delivery_error_text(exc),
        )
        return cooldown

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            now = time.monotonic()
            return {
                "channel": self.channel,
                "enabled": self.enabled,
                "ceiling": round(self.ceiling, 2),
                "effective_rate": round(self.effective_rate, 2),
                "cooldown_remaining": round(max(0.0, self.blocked_until - now), 2),
                "limit_events": self.limit_events,
            }


async def _worker_loop(generation: str) -> None:
    try:
        while generation == _worker_generation and sys.modules.get(__name__) is _module_instance:
            try:
                campaign_id = await _claim_campaign(generation)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log("warning", "worker claim retry generation=%s error=%s", generation, _delivery_error_text(exc))
                await asyncio.sleep(1.0)
                continue
            if campaign_id:
                try:
                    await _run_campaign(campaign_id, generation)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log("exception", "campaign=%s worker failure: %s", campaign_id, exc)
                    async with _write_db(operation="record worker failure") as db:
                        await db.execute(
                            "UPDATE campaigns SET status='failed',last_error=?,lease_owner=NULL,lease_until=NULL,completed_at=?,updated_at=? WHERE id=?",
                            (str(exc)[:2000], _now(), _now(), campaign_id),
                        )
            else:
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        _log("info", "worker generation=%s stopped", generation)


async def _claim_campaign(owner: str) -> str | None:
    now = time.time()
    async with _write_db(operation="claim campaign") as db:
        row = await (await db.execute(
            "SELECT id FROM campaigns WHERE status IN ('queued','scheduled') "
            "AND (scheduled_at IS NULL OR scheduled_at<=?) AND (lease_until IS NULL OR lease_until<?) "
            "ORDER BY created_at LIMIT 1", (_now(), now)
        )).fetchone()
        if not row:
            return None
        campaign_id = str(row[0])
        await db.execute(
            "UPDATE campaigns SET status='running',run_sent_baseline=sent,lease_owner=?,lease_until=?,heartbeat_at=?,started_at=COALESCE(started_at,?),updated_at=? WHERE id=?",
            (owner, now + 30, _now(), _now(), _now(), campaign_id),
        )
        return campaign_id


async def _run_campaign(campaign_id: str, owner: str) -> None:
    config = await _get_config()
    # Concurrency hides HTTP/proxy latency; AdaptiveRateGate remains the sole
    # request-rate authority. A deeper pipeline does not raise the configured
    # messages/second ceiling.
    concurrency = max(1, min(100, int(config.get("send_concurrency") or 4)))
    adaptive = bool(config.get("adaptive_rate_enabled", True))
    gates = {
        "vk": AdaptiveRateGate("vk", float(config.get("vk_rate_per_sec") or 3), enabled=adaptive),
        "telegram": AdaptiveRateGate(
            "telegram", float(config.get("telegram_rate_per_sec") or 10), enabled=adaptive,
        ),
    }
    _adaptive_rate_gates.update(gates)
    limits = httpx.Limits(
        max_connections=max(50, concurrency * 2),
        max_keepalive_connections=max(20, concurrency),
        keepalive_expiry=30.0,
    )
    telegram_timeout = httpx.Timeout(30.0, connect=8.0, read=25.0, write=25.0, pool=8.0)
    async with (
        httpx.AsyncClient(timeout=35.0, limits=limits) as vk_client,
        httpx.AsyncClient(
            timeout=telegram_timeout, proxy=_telegram_proxy_url() or None, limits=limits,
        ) as telegram_client,
    ):
        clients = {"vk": vk_client, "telegram": telegram_client}
        await asyncio.gather(*[
            _run_campaign_channel(
                campaign_id, owner, channel, config, concurrency, gates[channel], clients[channel],
            )
            for channel in ("vk", "telegram")
        ])
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        campaign = await (await db.execute(
            "SELECT status,lease_owner FROM campaigns WHERE id=?", (campaign_id,)
        )).fetchone()
        remaining = (await (await db.execute(
            "SELECT COUNT(*) FROM recipients WHERE campaign_id=? AND status IN ('pending','sending')",
            (campaign_id,),
        )).fetchone())[0]
    if campaign and campaign[0] == "running" and campaign[1] == owner and not remaining:
        await _finalize_campaign(campaign_id)


async def _run_campaign_channel(
    campaign_id: str,
    owner: str,
    channel: str,
    config: dict[str, Any],
    concurrency: int,
    gate: AdaptiveRateGate,
    client: httpx.AsyncClient,
) -> None:
    active: set[asyncio.Task[Any]] = set()
    template: dict[str, Any] = {}
    refill_at = max(1, concurrency // 2)
    try:
        while owner == _worker_generation and sys.modules.get(__name__) is _module_instance:
            rows: list[Any] = []
            if len(active) <= refill_at:
                async with _write_db(operation=f"claim {channel} recipients") as db:
                    db.row_factory = aiosqlite.Row
                    campaign = await (await db.execute(
                        "SELECT * FROM campaigns WHERE id=?", (campaign_id,)
                    )).fetchone()
                    if not campaign or campaign["status"] != "running" or campaign["lease_owner"] != owner:
                        if active:
                            await asyncio.gather(*active, return_exceptions=True)
                        return
                    template = _loads(campaign["template_snapshot_json"], {})
                    capacity = concurrency - len(active)
                    rows = await (await db.execute(
                        "SELECT * FROM recipients WHERE campaign_id=? AND channel=? AND status='pending' "
                        "AND next_attempt_at<=? ORDER BY id LIMIT ?",
                        (campaign_id, channel, time.time(), capacity),
                    )).fetchall()
                    if rows:
                        ids = [row["id"] for row in rows]
                        placeholders = ",".join("?" for _ in ids)
                        await db.execute(
                            f"UPDATE recipients SET status='sending',updated_at=? WHERE id IN ({placeholders})",
                            (_now(), *ids),
                        )
                    await db.execute(
                        "UPDATE campaigns SET lease_until=?,heartbeat_at=?,updated_at=? WHERE id=?",
                        (time.time() + 30, _now(), _now(), campaign_id),
                    )
                for row in rows:
                    active.add(asyncio.create_task(
                        _deliver_recipient_safe(dict(row), template, config, gate, client)
                    ))

            if active:
                done, pending_tasks = await asyncio.wait(
                    active, timeout=0.5, return_when=asyncio.FIRST_COMPLETED,
                )
                active = set(pending_tasks)
                if done:
                    results = await asyncio.gather(*done, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            _log(
                                "exception", "campaign=%s channel=%s delivery task failed: %s",
                                campaign_id, channel, _delivery_error_text(result),
                            )
                    try:
                        await _refresh_counts(campaign_id)
                    except (aiosqlite.OperationalError, TimeoutError) as exc:
                        _log(
                            "warning", "campaign=%s channel=%s count refresh deferred: %s",
                            campaign_id, channel, _delivery_error_text(exc),
                        )
                continue

            async with aiosqlite.connect(_must_db(), timeout=30) as db:
                await db.execute("PRAGMA busy_timeout=30000")
                pending = await (await db.execute(
                    "SELECT MIN(next_attempt_at) FROM recipients "
                    "WHERE campaign_id=? AND channel=? AND status='pending'",
                    (campaign_id, channel),
                )).fetchone()
                sending = (await (await db.execute(
                    "SELECT COUNT(*) FROM recipients WHERE campaign_id=? AND channel=? AND status='sending'",
                    (campaign_id, channel),
                )).fetchone())[0]
            if sending:
                await asyncio.sleep(0.2)
                continue
            if pending and pending[0] is not None:
                await asyncio.sleep(max(0.2, min(2.0, float(pending[0]) - time.time())))
                continue
            return
    finally:
        if active:
            for task in active:
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)


async def _deliver_recipient_safe(
    row: dict[str, Any], template: dict[str, Any], config: dict[str, Any], gate: AdaptiveRateGate,
    client: httpx.AsyncClient | None = None,
) -> None:
    try:
        await _deliver_recipient(row, template, config, gate, client)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # A bookkeeping lock must never fail the whole 169k campaign. The
        # durable sent_messages row is authoritative; only an unsent claimed
        # row is returned to pending.
        error_text = f"Внутренняя повторная попытка: {_delivery_error_text(exc)}"[:2000]
        _log(
            "exception", "campaign=%s channel=%s recipient=%s delivery bookkeeping retry: %s",
            row.get("campaign_id"), row.get("channel"), row.get("recipient_id"), error_text,
        )
        async with _write_db(operation="restore recipient after bookkeeping error") as db:
            await db.execute(
                "UPDATE recipients SET status='pending',next_attempt_at=?,last_error=?,updated_at=? "
                "WHERE id=? AND status='sending' AND NOT EXISTS "
                "(SELECT 1 FROM sent_messages WHERE recipient_row_id=?)",
                (time.time() + 5, error_text, _now(), int(row["id"]), int(row["id"])),
            )


async def _deliver_recipient(
    row: dict[str, Any], template: dict[str, Any], config: dict[str, Any], gate: AdaptiveRateGate,
    client: httpx.AsyncClient | None = None,
) -> None:
    recipient_row_id = int(row["id"])
    exclusion_status = ""
    exclusion_note = ""
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        sent = await (await db.execute("SELECT id FROM sent_messages WHERE recipient_row_id=?", (recipient_row_id,))).fetchone()
        campaign = await (await db.execute(
            "SELECT audience_snapshot_json FROM campaigns WHERE id=?", (row["campaign_id"],)
        )).fetchone()
        exclusion_options = _snapshot_exclusion_options(_loads(campaign[0] if campaign else "{}", {}))
        buyer = await (await db.execute(
            "SELECT 1 FROM buyers WHERE channel=? AND recipient_id=?",
            (row["channel"], row["recipient_id"]),
        )).fetchone()
        blocked = await (await db.execute("SELECT 1 FROM stop_list WHERE channel=? AND recipient_id=?", (row["channel"], row["recipient_id"]))).fetchone()
        not_allowed = await (await db.execute(
            "SELECT reason FROM not_allow WHERE channel=? AND recipient_id=?",
            (row["channel"], row["recipient_id"]),
        )).fetchone()
        if sent:
            exclusion_status = "sent"
        elif exclusion_options["buyers"] and buyer:
            exclusion_status, exclusion_note = "skipped", "buyers"
        elif exclusion_options["stop_list"] and blocked:
            exclusion_status, exclusion_note = "skipped", "stop-list"
        elif exclusion_options["not_allow"] and not_allowed:
            exclusion_status = "not_allowed"
            exclusion_note = "not_allow" + (f": {not_allowed[0]}" if str(not_allowed[0] or "").strip() else "")
    if exclusion_status:
        async with _write_db(operation="apply recipient exclusion") as db:
            if exclusion_status == "sent":
                await db.execute(
                    "UPDATE recipients SET status='sent',updated_at=? WHERE id=?",
                    (_now(), recipient_row_id),
                )
            else:
                await db.execute(
                    "UPDATE recipients SET status=?,last_error=?,updated_at=? WHERE id=?",
                    (exclusion_status, exclusion_note[:2000], _now(), recipient_row_id),
                )
        return
    await gate.wait()
    attempt = int(row["attempts"] or 0) + 1
    try:
        async with aiosqlite.connect(_must_db(), timeout=30) as db:
            await db.execute("PRAGMA busy_timeout=30000")
            current = await (await db.execute(
                "SELECT c.status,c.lease_owner,r.status FROM recipients r JOIN campaigns c ON c.id=r.campaign_id WHERE r.id=?",
                (recipient_row_id,),
            )).fetchone()
            if (
                not current
                or current[0] != "running"
                or current[1] != _worker_generation
                or current[2] != "sending"
            ):
                if current and current[2] == "sending":
                    target_status = "skipped" if current[0] == "cancelled" else "pending"
                    note = "cancelled" if target_status == "skipped" else ""
                    async with _write_db(operation="restore recipient after state change") as write_db:
                        await write_db.execute(
                            "UPDATE recipients SET status=?,last_error=?,updated_at=? WHERE id=? AND status='sending' "
                            "AND NOT EXISTS (SELECT 1 FROM sent_messages WHERE recipient_row_id=?)",
                            (target_status, note, _now(), recipient_row_id, recipient_row_id),
                        )
                return
        delivery_row = dict(row)
        if not str(delivery_row.get("rendered_content") or ""):
            delivery_row["rendered_content"] = str(template.get("content") or "")
        if client is None:
            external_id, response = await _send_remote(delivery_row, template)
        else:
            external_id, response = await _send_remote(delivery_row, template, client)
        await gate.on_success()
        await _queue_delivery_outcome({
            "kind": "sent", "row": row, "attempt": attempt, "external_id": external_id,
            "response": response, "now": _now(),
        })
        _log("debug", "campaign=%s channel=%s recipient=%s sent", row["campaign_id"], row["channel"], row["recipient_id"])
    except NotAllowedDeliveryError as exc:
        error_text = _delivery_error_text(exc)
        await _queue_delivery_outcome({
            "kind": "not_allowed", "row": row, "attempt": attempt, "error": error_text,
            "api_code": exc.api_code, "now": _now(),
        })
        _log(
            "info", "campaign=%s channel=%s recipient=%s added_to_not_allow code=%s",
            row["campaign_id"], row["channel"], row["recipient_id"], exc.api_code,
        )
    except Exception as exc:
        error_text = _delivery_error_text(exc)
        transient = _is_transient_delivery_error(exc)
        rate_limit_cooldown = 0.0
        if isinstance(exc, TransientDeliveryError) and exc.rate_limited:
            rate_limit_cooldown = await gate.on_rate_limit(exc)
        if transient:
            max_attempts = max(3, int(config.get("network_max_attempts") or DEFAULT_CONFIG["network_max_attempts"]))
            final = attempt >= max_attempts
            if final:
                next_at = 0
            elif rate_limit_cooldown:
                next_at = time.time() + max(1.0, rate_limit_cooldown)
            else:
                next_at = time.time() + _network_retry_delay(attempt, config)
        else:
            max_attempts = max(1, int(config.get("max_attempts") or DEFAULT_CONFIG["max_attempts"]))
            final = attempt >= max_attempts or isinstance(exc, PermanentDeliveryError)
            next_at = 0 if final else time.time() + min(300, 2 ** attempt * 2)
        status = "failed" if final else "pending"
        attempt_status = "failed" if final else "retry"
        await _queue_delivery_outcome({
            "kind": "failed", "row": row, "attempt": attempt, "status": status,
            "next_at": next_at, "attempt_status": attempt_status, "error": error_text,
            "now": _now(),
        })
        if transient and not final:
            await _maybe_auto_pause_network(row["campaign_id"], row["channel"], config, error_text)
        _log("warning", "campaign=%s channel=%s recipient=%s attempt=%s error=%s", row["campaign_id"], row["channel"], row["recipient_id"], attempt, error_text)


async def _maybe_auto_pause_network(campaign_id: str, channel: str, config: dict[str, Any], error_text: str) -> None:
    threshold = int(config.get("auto_pause_network_errors") or DEFAULT_CONFIG["auto_pause_network_errors"])
    if threshold <= 0:
        return
    async with _write_db(operation="auto-pause campaign") as db:
        rows = await (await db.execute(
            """
            SELECT da.status,da.error
            FROM delivery_attempts da
            JOIN recipients r ON r.id=da.recipient_id
            WHERE da.campaign_id=? AND r.channel=?
            ORDER BY da.id DESC
            LIMIT ?
            """,
            (campaign_id, channel, threshold),
        )).fetchall()
        if len(rows) < threshold:
            return
        if any(row[0] != "retry" for row in rows):
            return
        network_words = ("timeout", "сетевая", "недоступ", "временно", "connect", "proxy")
        if any(not any(word in str(row[1]).casefold() for word in network_words) for row in rows):
            return
        message = f"Автопауза: {threshold} сетевых ошибок подряд в канале {channel}. Последняя ошибка: {error_text[:500]}"
        cur = await db.execute(
            "UPDATE campaigns SET status='paused',last_error=?,lease_owner=NULL,lease_until=NULL,updated_at=? WHERE id=? AND status='running'",
            (message, _now(), campaign_id),
        )
    if cur.rowcount:
        _log("warning", "campaign=%s auto-paused channel=%s threshold=%s error=%s", campaign_id, channel, threshold, error_text)


class PermanentDeliveryError(RuntimeError):
    pass


class NotAllowedDeliveryError(PermanentDeliveryError):
    def __init__(self, message: str, *, api_code: Any = ""):
        super().__init__(message)
        self.api_code = str(api_code or "")


class TransientDeliveryError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        rate_limited: bool = False,
        retry_after: float = 0.0,
    ):
        super().__init__(message)
        self.rate_limited = bool(rate_limited)
        self.retry_after = max(0.0, float(retry_after or 0.0))


def _is_transient_delivery_error(exc: Exception) -> bool:
    return isinstance(exc, (TransientDeliveryError, httpx.TimeoutException, httpx.NetworkError, asyncio.TimeoutError))


def _network_retry_delay(attempt: int, config: dict[str, Any]) -> int:
    base = max(30, min(3600, int(config.get("network_retry_base_sec") or DEFAULT_CONFIG["network_retry_base_sec"])))
    cap = max(base, min(21600, int(config.get("network_retry_max_sec") or DEFAULT_CONFIG["network_retry_max_sec"])))
    return min(cap, base * (2 ** max(0, int(attempt) - 1)))


def _delivery_error_text(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message[:2000]
    if isinstance(exc, asyncio.TimeoutError):
        return "Таймаут сети при обращении к API канала"
    return f"{exc.__class__.__module__}.{exc.__class__.__name__}"


def _telegram_api_base() -> str:
    return telegram_bot_api_base(os.getenv("SBKVD_LETTER_TELEGRAM_API_BASE", ""))


def _telegram_proxy_url() -> str:
    return telegram_bot_api_proxy_url(os.getenv("SBKVD_LETTER_TELEGRAM_PROXY_URL", ""))


async def _send_remote(
    row: dict[str, Any], template: dict[str, Any], client: httpx.AsyncClient | None = None,
) -> tuple[str, dict[str, Any]]:
    if row["channel"] == "vk":
        return await _send_vk(row, template, client)
    if row["channel"] == "telegram":
        return await _send_telegram(row, template, client)
    raise PermanentDeliveryError("Неизвестный канал")


def _api_error(data: Any, channel: str) -> Exception:
    if channel == "vk":
        error = data.get("error") if isinstance(data, dict) else None
        code = error.get("error_code") if isinstance(error, dict) else None
        message = error.get("error_msg") if isinstance(error, dict) else str(data)
        if code in VK_NOT_ALLOW_CODES:
            return NotAllowedDeliveryError(f"VK {code}: {message}", api_code=code)
        if code in VK_RETRYABLE_CODES:
            retry_after = 60.0 if code == 29 else (10.0 if code == 9 else 0.0)
            return TransientDeliveryError(
                f"VK {code}: {message}",
                rate_limited=code in {6, 9, 29},
                retry_after=retry_after,
            )
        if code in {5, 7, 15, 902, 914, 936}:
            return PermanentDeliveryError(f"VK {code}: {message}")
        return RuntimeError(f"VK {code}: {message}")
    description = data.get("description") if isinstance(data, dict) else str(data)
    code = data.get("error_code") if isinstance(data, dict) else None
    folded = str(description or "").casefold()
    if any(marker in folded for marker in TELEGRAM_NOT_ALLOW_MARKERS):
        return NotAllowedDeliveryError(f"Telegram {code}: {description}", api_code=code)
    if code == 429 or (isinstance(code, int) and code >= 500):
        parameters = data.get("parameters") if isinstance(data, dict) else None
        retry_after = parameters.get("retry_after", 0) if isinstance(parameters, dict) else 0
        return TransientDeliveryError(
            f"Telegram {code}: {description}",
            rate_limited=code == 429,
            retry_after=retry_after,
        )
    if code in {400, 401, 403, 404}:
        return PermanentDeliveryError(f"Telegram {code}: {description}")
    return RuntimeError(f"Telegram {code}: {description}")


async def _send_vk(
    row: dict[str, Any], template: dict[str, Any], client: httpx.AsyncClient | None = None,
) -> tuple[str, dict[str, Any]]:
    token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
    if not token:
        raise PermanentDeliveryError("SBKVD_LETTER_VK_TOKEN не настроен")
    params: dict[str, Any] = {
        "access_token": token, "v": "5.199", "peer_id": row["recipient_id"],
        "random_id": _stable_random_id(row["campaign_id"], row["recipient_id"]),
        "message": _content_for_channel(row["rendered_content"], template, "vk"), "disable_mentions": 1,
    }
    attachment = str(template.get("vk_attachment") or "").strip()
    if not attachment and template.get("attachment_ids"):
        prepared = []
        for item_id in list(template["attachment_ids"])[:10]:
            prepared.append(await _vk_prepare_attachment(int(item_id), token, row["recipient_id"]))
        attachment = ",".join(prepared)
    if attachment:
        params["attachment"] = attachment
    keyboard = _keyboard_for_channel(template, "vk")
    if keyboard:
        params["keyboard"] = _dump(keyboard) if not isinstance(keyboard, str) else keyboard
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=35.0)
    try:
        response = await client.post("https://api.vk.com/method/messages.send", data=params)
        response.raise_for_status()
        data = response.json()
    finally:
        if owns_client:
            await client.aclose()
    if "response" not in data:
        raise _api_error(data, "vk")
    value = data["response"]
    message_id = value.get("message_id") if isinstance(value, dict) else value
    return str(message_id), {"message_id": message_id}


def _stable_random_id(campaign_id: str, recipient_id: str) -> int:
    raw = hashlib.sha256(f"{campaign_id}:{recipient_id}".encode()).digest()
    value = int.from_bytes(raw[:4], "big") & 0x7FFFFFFF
    return value or 1


async def _vk_prepare_attachment(item_id: int, token: str, peer_id: str) -> str:
    meta, content = await _storage_read(item_id)
    file_hash = hashlib.sha256(content).hexdigest()
    async with aiosqlite.connect(_must_db()) as db:
        cached = await (await db.execute("SELECT remote_id FROM attachment_cache WHERE channel='vk' AND file_hash=?", (file_hash,))).fetchone()
    if cached:
        return str(cached[0])
    if not str(meta.get("mime_type") or "").startswith("image/"):
        raise PermanentDeliveryError("VK автоматически загружает только изображения; укажите vk_attachment для другого типа")
    async with httpx.AsyncClient(timeout=60.0) as client:
        server_response = await client.post("https://api.vk.com/method/photos.getMessagesUploadServer", data={"access_token": token, "v": "5.199", "peer_id": peer_id})
        server_data = server_response.json()
        if "response" not in server_data:
            raise _api_error(server_data, "vk")
        upload_url = server_data["response"]["upload_url"]
        upload = await client.post(upload_url, files={"photo": (meta["name"], content, meta.get("mime_type") or "image/jpeg")})
        upload.raise_for_status()
        upload_data = upload.json()
        saved_response = await client.post("https://api.vk.com/method/photos.saveMessagesPhoto", data={"access_token": token, "v": "5.199", **upload_data})
        saved = saved_response.json()
    if "response" not in saved or not saved["response"]:
        raise _api_error(saved, "vk")
    photo = saved["response"][0]
    remote_id = f"photo{photo['owner_id']}_{photo['id']}"
    if photo.get("access_key"):
        remote_id += f"_{photo['access_key']}"
    async with _write_db(operation="cache VK attachment") as db:
        await db.execute("INSERT OR REPLACE INTO attachment_cache(channel,file_hash,remote_id,updated_at) VALUES('vk',?,?,?)", (file_hash, remote_id, _now()))
    return remote_id


async def _send_telegram(
    row: dict[str, Any], template: dict[str, Any], client: httpx.AsyncClient | None = None,
) -> tuple[str, dict[str, Any]]:
    token = os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise PermanentDeliveryError("SBKVD_LETTER_TELEGRAM_BOT_TOKEN не настроен")
    base = f"{_telegram_api_base()}/bot{token}"
    payload: dict[str, Any] = {"chat_id": row["recipient_id"]}
    parse_mode = str(template.get("parse_mode") or "").strip()
    if parse_mode in {"HTML", "MarkdownV2"}:
        payload["parse_mode"] = parse_mode
    keyboard = _keyboard_for_channel(template, "telegram")
    if keyboard:
        payload["reply_markup"] = _dump(keyboard) if not isinstance(keyboard, str) else keyboard
    attachments = template.get("attachment_ids") or []
    owns_client = client is None
    if client is None:
        timeout = httpx.Timeout(30.0, connect=8.0, read=25.0, write=25.0, pool=8.0)
        client = httpx.AsyncClient(timeout=timeout, proxy=_telegram_proxy_url() or None)
    try:
        if len(attachments) > 1:
            media = []
            files = {}
            for index, item_id in enumerate(list(attachments)[:10]):
                meta, content = await _storage_read(int(item_id))
                field = f"file{index}"
                media_item: dict[str, Any] = {
                    "type": "photo" if str(meta.get("mime_type") or "").startswith("image/") else "document",
                    "media": f"attach://{field}",
                }
                if index == 0:
                    media_item["caption"] = row["rendered_content"][:1024]
                    if parse_mode:
                        media_item["parse_mode"] = parse_mode
                media.append(media_item)
                files[field] = (meta["name"], content, meta.get("mime_type"))
            response = await client.post(
                f"{base}/sendMediaGroup",
                data={"chat_id": row["recipient_id"], "media": _dump(media)},
                files=files,
            )
        elif attachments:
            meta, content = await _storage_read(int(attachments[0]))
            is_image = str(meta.get("mime_type") or "").startswith("image/")
            method = "sendPhoto" if is_image else "sendDocument"
            field = "photo" if is_image else "document"
            payload["caption"] = row["rendered_content"][:1024]
            response = await client.post(
                f"{base}/{method}", data=payload,
                files={field: (meta["name"], content, meta.get("mime_type"))},
            )
        else:
            payload["text"] = row["rendered_content"]
            payload["disable_web_page_preview"] = True
            response = await client.post(f"{base}/sendMessage", json=payload)
        try:
            data = response.json()
        except Exception:
            response.raise_for_status()
            raise RuntimeError("Telegram вернул некорректный ответ")
    except httpx.ConnectTimeout as exc:
        raise TransientDeliveryError(f"Telegram API временно недоступен: timeout подключения к {_telegram_api_base()}") from exc
    except httpx.ReadTimeout as exc:
        raise TransientDeliveryError(f"Telegram API временно недоступен: timeout ответа от {_telegram_api_base()}") from exc
    except httpx.ConnectError as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        raise TransientDeliveryError(f"Telegram API временно недоступен: ошибка подключения к {_telegram_api_base()} ({detail})") from exc
    except httpx.NetworkError as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        raise TransientDeliveryError(f"Telegram API временно недоступен: сетевая ошибка {_telegram_api_base()} ({detail})") from exc
    finally:
        if owns_client:
            await client.aclose()
    if not data.get("ok"):
        raise _api_error(data, "telegram")
    response.raise_for_status()
    if isinstance(data["result"], list):
        message_ids = [int(item["message_id"]) for item in data["result"]]
        return _dump(message_ids), {"message_ids": message_ids}
    return str(data["result"]["message_id"]), {"message_id": data["result"]["message_id"]}


def _vk_deleted_message_ids(response: Any, message_ids: list[int]) -> set[int]:
    if response is True or response == 1:
        return set(message_ids)
    if isinstance(response, dict):
        deleted: set[int] = set()
        for message_id in message_ids:
            value = response.get(str(message_id), response.get(message_id))
            if value is True or value == 1 or (isinstance(value, dict) and value.get("peer_id")):
                deleted.add(message_id)
        return deleted
    if isinstance(response, list) and len(response) == len(message_ids):
        return {
            message_id for message_id, value in zip(message_ids, response)
            if value is True or value == 1 or (isinstance(value, dict) and value.get("peer_id"))
        }
    return set()


async def _delete_vk_messages(messages: list[dict[str, Any]], on_batch=None) -> tuple[set[int], dict[int, str]]:
    token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
    if not token:
        return set(), {int(row["id"]): "VK token не настроен" for row in messages}
    valid: list[tuple[dict[str, Any], int]] = []
    errors: dict[int, str] = {}
    for row in messages:
        try:
            valid.append((row, int(row["external_message_id"])))
        except (TypeError, ValueError):
            errors[int(row["id"])] = "Некорректный VK message_id"
    deleted_rows: set[int] = set()
    gate = RateGate(3.0)
    async with httpx.AsyncClient(timeout=30.0) as client:
        for start in range(0, len(valid), 100):
            chunk = valid[start:start + 100]
            message_ids = [message_id for _, message_id in chunk]
            last_error = "VK не подтвердил удаление сообщения"
            deleted_ids: set[int] = set()
            for attempt in range(1, 4):
                await gate.wait()
                try:
                    response = await client.post(
                        "https://api.vk.com/method/messages.delete",
                        data={
                            "access_token": token,
                            "v": "5.199",
                            "message_ids": ",".join(str(value) for value in message_ids),
                            "delete_for_all": 1,
                        },
                    )
                    response.raise_for_status()
                    data = response.json()
                    if "response" in data:
                        deleted_ids = _vk_deleted_message_ids(data["response"], message_ids)
                        last_error = "VK вернул неполный результат удаления"
                        break
                    exc = _api_error(data, "vk")
                    last_error = _delivery_error_text(exc)
                    if not _is_transient_delivery_error(exc):
                        break
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_error = _delivery_error_text(exc)
                if attempt < 3:
                    await asyncio.sleep(min(8, 2 ** attempt))
            for row, message_id in chunk:
                row_id = int(row["id"])
                if message_id in deleted_ids:
                    deleted_rows.add(row_id)
                else:
                    errors[row_id] = last_error
            if on_batch:
                batch_row_ids = {int(row["id"]) for row, _ in chunk}
                await on_batch(
                    deleted_rows & batch_row_ids,
                    {row_id: message for row_id, message in errors.items() if row_id in batch_row_ids},
                )
    return deleted_rows, errors


async def _delete_remote(message: dict[str, Any]) -> None:
    if message["channel"] == "vk":
        token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
        if not token:
            raise RuntimeError("VK token не настроен")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post("https://api.vk.com/method/messages.delete", data={
                "access_token": token, "v": "5.199", "message_ids": message["external_message_id"], "delete_for_all": 1,
            })
            data = response.json()
        if "response" not in data:
            raise _api_error(data, "vk")
        return
    token = os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Telegram token не настроен")
    raw_ids = _loads(message["external_message_id"], None)
    message_ids = raw_ids if isinstance(raw_ids, list) else [int(message["external_message_id"])]
    async with httpx.AsyncClient(timeout=30.0, proxy=_telegram_proxy_url() or None) as client:
        for message_id in message_ids:
            response = await client.post(f"{_telegram_api_base()}/bot{token}/deleteMessage", json={
                "chat_id": message["recipient_id"], "message_id": int(message_id),
            })
            data = response.json()
            if not data.get("ok"):
                raise _api_error(data, "telegram")


async def _refresh_counts(campaign_id: str, *, force: bool = False) -> None:
    lock = _count_refresh_locks.setdefault(campaign_id, asyncio.Lock())
    if lock.locked() and not force:
        return
    async with lock:
        now = time.monotonic()
        if not force and now - _count_refresh_last.get(campaign_id, 0.0) < 2.0:
            return
        async with _write_db(operation="refresh campaign counts") as db:
            rows = await (await db.execute(
                "SELECT status,COUNT(*) FROM recipients WHERE campaign_id=? GROUP BY status",
                (campaign_id,),
            )).fetchall()
            counts = {row[0]: row[1] for row in rows}
            await db.execute(
                "UPDATE campaigns SET sent=?,failed=?,skipped=?,not_allowed=?,heartbeat_at=?,lease_until=?,updated_at=? WHERE id=?",
                (
                    counts.get("sent", 0), counts.get("failed", 0), counts.get("skipped", 0),
                    counts.get("not_allowed", 0), _now(), time.time() + 30, _now(), campaign_id,
                ),
            )
        _count_refresh_last[campaign_id] = time.monotonic()


async def _finalize_campaign(campaign_id: str) -> None:
    await _refresh_counts(campaign_id, force=True)
    async with _write_db(operation="finalize campaign") as db:
        row = await (await db.execute("SELECT failed FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
        status = "completed_with_errors" if row and row[0] else "completed"
        await db.execute(
            "UPDATE campaigns SET status=?,lease_owner=NULL,lease_until=NULL,completed_at=?,updated_at=? WHERE id=? AND status='running'",
            (status, _now(), _now(), campaign_id),
        )
    _log("info", "campaign=%s finalized status=%s", campaign_id, status)
    _count_refresh_last.pop(campaign_id, None)
    _count_refresh_locks.pop(campaign_id, None)
