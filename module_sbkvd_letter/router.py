from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import logging
import mimetypes
import os
import re
import secrets
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiofiles
import aiosqlite
import httpx
from fastapi import APIRouter, File, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel, Field

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "sbkvd-letter"
SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
VARIABLE_RE = re.compile(r"{{\s*([a-zA-Z0-9_.\[\]-]+)\s*}}")
FINAL_STATUSES = {"completed", "completed_with_errors", "failed", "cancelled"}
ACTIVE_STATUSES = {"scheduled", "queued", "running", "paused"}
SYSTEM_FIELDS = {"id", "platform_id", "created_at", "updated_at", "table"}
FILE_FOLDER = "sbkvd-letter"
TEMPLATE_FOLDER = "templates"
ATTACHMENT_FOLDER = "attachments"

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_worker_task: asyncio.Task | None = None
_worker_generation = ""
_module_instance: Any = None


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
    telegram_rate_per_sec: float = 15.0
    max_attempts: int = 3


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
    template_id: int | None = None
    content: str = ""
    channels: list[str] = Field(default_factory=list)
    keyboard: dict[str, Any] = Field(default_factory=dict)
    attachment_ids: list[int] = Field(default_factory=list)
    vk_attachment: str = ""
    parse_mode: str = ""
    scheduled_at: str | None = None


for _model in (ConditionIn, ManualRecipientIn, AudienceIn, SheetIn, SettingsIn, TokensIn, SegmentIn, TemplateIn, CampaignIn):
    if hasattr(_model, "model_rebuild"):
        _model.model_rebuild()


async def setup(ctx):
    global _db_path, _module_dir, _logger, _worker_task, _worker_generation, _module_instance
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.sbkvd-letter"))
    _module_instance = sys.modules.get(__name__)
    previous = getattr(builtins, "_nexus_sbkvd_letter_worker", None)
    if isinstance(previous, asyncio.Task) and not previous.done():
        previous.cancel()
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
    async with aiosqlite.connect(_must_db()) as db:
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
            """
        )
        defaults = {
            "sheets": [],
            "send_concurrency": 4,
            "vk_rate_per_sec": 3.0,
            "telegram_rate_per_sec": 15.0,
            "max_attempts": 3,
        }
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value_json) VALUES('config',?)",
            (_dump(defaults),),
        )
        campaign_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(campaigns)")).fetchall()}
        if "segment_id" not in campaign_columns:
            await db.execute("ALTER TABLE campaigns ADD COLUMN segment_id INTEGER")
        await db.execute(
            "UPDATE campaigns SET status='queued', lease_owner=NULL, lease_until=NULL "
            "WHERE status='running'"
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
    return _loads(row[0] if row else "{}", {})


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
    for value in values:
        text = str(value or "")
        folded = text.casefold()
        if op in {"contains", "has"} and expected_text in folded:
            return True
        if op == "not_contains" and expected_text not in folded:
            return True
        if op in {"eq", "equals"} and folded == expected_text:
            return True
        if op in {"neq", "not_equals"} and folded != expected_text:
            return True
        if op == "starts" and folded.startswith(expected_text):
            return True
        if op == "ends" and folded.endswith(expected_text):
            return True
        if op == "in" and folded in expected_list:
            return True
        value_num, first, second = _number(value), _number(expected), _number(expected2)
        if op == "gt" and value_num is not None and first is not None and value_num > first:
            return True
        if op == "lt" and value_num is not None and first is not None and value_num < first:
            return True
        if op == "between" and None not in {value_num, first, second} and first <= value_num <= second:
            return True
    return False


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


async def _audience_records(audience: AudienceIn | dict[str, Any], limit: int | None = None) -> list[dict[str, Any]]:
    data = _model_dump(audience) if isinstance(audience, BaseModel) else audience
    config = await _get_config()
    allowed = {item["name"]: item for item in config.get("sheets", []) if item.get("enabled")}
    requested = [str(name) for name in data.get("tables", [])]
    table_names = requested or list(allowed)
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
            parsed = parsed.astimezone()
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        raise HTTPException(400, "Некорректное время запуска")


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID, "worker": bool(_worker_task and not _worker_task.done())}


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
        "send_concurrency": max(1, min(20, data.send_concurrency)),
        "vk_rate_per_sec": max(0.2, min(20.0, data.vk_rate_per_sec)),
        "telegram_rate_per_sec": max(0.2, min(30.0, data.telegram_rate_per_sec)),
        "max_attempts": max(1, min(8, data.max_attempts)),
    }
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "INSERT INTO module_settings(key,value_json,updated_at) VALUES('config',?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            (_dump(payload), _now()),
        )
        await db.commit()
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


@router.get("/segments")
async def segments(request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
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
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "INSERT INTO segments(name,description,audience_json,created_by) VALUES(?,?,?,?)",
            (name, data.description.strip()[:1000], _dump(_model_dump(data.audience)), user.get("username", "")),
        )
        await db.commit()
    return {"ok": True, "id": int(cur.lastrowid)}


@router.put("/segments/{segment_id}")
async def update_segment(segment_id: int, data: SegmentIn, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "UPDATE segments SET name=?,description=?,audience_json=?,updated_at=? WHERE id=?",
            (data.name.strip()[:160], data.description.strip()[:1000], _dump(_model_dump(data.audience)), _now(), segment_id),
        )
        await db.commit()
    if not cur.rowcount:
        raise HTTPException(404, "Сегмент не найден")
    return {"ok": True}


@router.delete("/segments/{segment_id}")
async def delete_segment(segment_id: int, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM segments WHERE id=?", (segment_id,))
        await db.commit()
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
    segment_name = ""
    audience_data: AudienceIn | dict[str, Any] = data.audience
    if data.segment_id is not None:
        async with aiosqlite.connect(_must_db()) as db:
            row = await (await db.execute(
                "SELECT name,audience_json FROM segments WHERE id=?", (int(data.segment_id),)
            )).fetchone()
        if not row:
            raise HTTPException(404, "Сегмент не найден")
        segment_name = str(row[0])
        audience_data = _loads(row[1], {})
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
    records = [item for item in await _audience_records(audience_data) if item["channel"] in channels]
    if not records:
        raise HTTPException(400, "В аудитории нет получателей выбранных каналов")
    schedule = _parse_schedule(data.scheduled_at)
    status = "scheduled" if schedule and schedule > _now() else "queued"
    campaign_id = uuid.uuid4().hex
    name = (data.name or template.get("name") or "Рассылка").strip()[:160]
    unknown: set[str] = set()
    recipients = []
    for record in records:
        rendered, missing = _render(str(template["content"]), record)
        unknown.update(missing)
        recipients.append((record, rendered))
    audience_payload = _model_dump(audience_data) if isinstance(audience_data, BaseModel) else dict(audience_data)
    audience_snapshot = {
        **audience_payload,
        "segment_id": data.segment_id,
        "segment_name": segment_name,
        "resolved_at": _now(),
        "count": len(recipients),
    }
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute(
            "INSERT INTO campaigns(id,name,status,segment_id,template_item_id,template_snapshot_json,audience_snapshot_json,scheduled_at,created_by,total) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (campaign_id, name, status, data.segment_id, data.template_id, _dump(template), _dump(audience_snapshot), schedule, user.get("username", ""), len(recipients)),
        )
        await db.executemany(
            "INSERT OR IGNORE INTO recipients(campaign_id,channel,recipient_id,source_table,source_row_id,source_json,rendered_content) "
            "VALUES(?,?,?,?,?,?,?)",
            [(campaign_id, row["channel"], row["recipient_id"], row["table"], row["id"], _dump(row), rendered) for row, rendered in recipients],
        )
        await db.commit()
    _log("info", "campaign=%s created recipients=%s status=%s", campaign_id, len(recipients), status)
    return {"ok": True, "id": campaign_id, "status": status, "total": len(recipients), "missing_variables": sorted(unknown)}


def _campaign_row(row: aiosqlite.Row, *, details: bool = False) -> dict[str, Any]:
    item = dict(row)
    if details:
        item["template_snapshot"] = _loads(item.pop("template_snapshot_json"), {})
        item["audience_snapshot"] = _loads(item.pop("audience_snapshot_json"), {})
    else:
        item.pop("template_snapshot_json", None)
        item.pop("audience_snapshot_json", None)
    return item


@router.get("/campaigns")
async def campaigns(request: Request, limit: int = 100, offset: int = 0):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        total = (await (await db.execute("SELECT COUNT(*) FROM campaigns")).fetchone())[0]
        rows = await (await db.execute(
            "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT ? OFFSET ?", (max(1, min(500, limit)), max(0, offset))
        )).fetchall()
    return {"total": total, "items": [_campaign_row(row) for row in rows]}


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
    return {"campaign": _campaign_row(row, details=True), "recipients": [dict(item) for item in recipients]}


async def _set_campaign_status(campaign_id: str, allowed: set[str], status: str) -> bool:
    async with aiosqlite.connect(_must_db()) as db:
        placeholders = ",".join("?" for _ in allowed)
        cur = await db.execute(
            f"UPDATE campaigns SET status=?,lease_owner=NULL,lease_until=NULL,updated_at=? WHERE id=? AND status IN ({placeholders})",
            (status, _now(), campaign_id, *allowed),
        )
        await db.commit()
    return bool(cur.rowcount)


@router.post("/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"queued", "running", "scheduled"}, "paused"):
        raise HTTPException(409, "Рассылку нельзя приостановить")
    return {"ok": True}


@router.post("/campaigns/{campaign_id}/resume")
async def resume_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"paused"}, "queued"):
        raise HTTPException(409, "Рассылка не на паузе")
    return {"ok": True}


@router.post("/campaigns/{campaign_id}/cancel")
async def cancel_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    if not await _set_campaign_status(campaign_id, {"queued", "running", "scheduled", "paused"}, "cancelled"):
        raise HTTPException(409, "Рассылку нельзя отменить")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("UPDATE recipients SET status='skipped',updated_at=? WHERE campaign_id=? AND status IN ('pending','sending')", (_now(), campaign_id))
        await db.commit()
    return {"ok": True}


@router.post("/campaigns/{campaign_id}/retry")
async def retry_campaign(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "UPDATE recipients SET status='pending',attempts=0,next_attempt_at=0,last_error='',updated_at=? "
            "WHERE campaign_id=? AND status='failed'", (_now(), campaign_id)
        )
        if not cur.rowcount:
            raise HTTPException(409, "Нет ошибок для повторной отправки")
        await db.execute("UPDATE campaigns SET status='queued',failed=0,last_error='',completed_at=NULL,updated_at=? WHERE id=?", (_now(), campaign_id))
        await db.commit()
    return {"ok": True, "count": cur.rowcount}


@router.delete("/campaigns/{campaign_id}")
async def clear_campaign(campaign_id: str, request: Request):
    await _require_user(request, admin=True)
    async with aiosqlite.connect(_must_db()) as db:
        status = await (await db.execute("SELECT status FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
        if status and status[0] in ACTIVE_STATUSES:
            raise HTTPException(409, "Сначала отмените рассылку")
        await db.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
        await db.commit()
    return {"ok": True}


@router.post("/campaigns/{campaign_id}/messages/delete")
async def delete_remote_messages(campaign_id: str, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT * FROM sent_messages WHERE campaign_id=? AND deleted_at IS NULL", (campaign_id,)
        )).fetchall()
    deleted = 0
    errors = []
    for row in rows:
        try:
            await _delete_remote(dict(row))
            async with aiosqlite.connect(_must_db()) as db:
                await db.execute("UPDATE sent_messages SET deleted_at=?,delete_error='' WHERE id=?", (_now(), row["id"]))
                await db.commit()
            deleted += 1
        except Exception as exc:
            errors.append(f"{row['channel']}:{row['recipient_id']}: {exc}")
            async with aiosqlite.connect(_must_db()) as db:
                await db.execute("UPDATE sent_messages SET delete_error=? WHERE id=?", (str(exc)[:1000], row["id"]))
                await db.commit()
    return {"ok": not errors, "deleted": deleted, "errors": errors[:100]}


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
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("INSERT OR IGNORE INTO stop_list(channel,recipient_id) VALUES(?,?)", (channel, recipient_id))
        await db.commit()
    return {"ok": True}


@router.delete("/stop-list/{channel}/{recipient_id}")
async def remove_stop(channel: str, recipient_id: str, request: Request):
    await _require_user(request, edit=True)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM stop_list WHERE channel=? AND recipient_id=?", (channel, recipient_id))
        await db.commit()
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


async def _worker_loop(generation: str) -> None:
    try:
        while generation == _worker_generation and sys.modules.get(__name__) is _module_instance:
            campaign_id = await _claim_campaign(generation)
            if campaign_id:
                try:
                    await _run_campaign(campaign_id, generation)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log("exception", "campaign=%s worker failure: %s", campaign_id, exc)
                    async with aiosqlite.connect(_must_db()) as db:
                        await db.execute(
                            "UPDATE campaigns SET status='failed',last_error=?,lease_owner=NULL,lease_until=NULL,completed_at=?,updated_at=? WHERE id=?",
                            (str(exc)[:2000], _now(), _now(), campaign_id),
                        )
                        await db.commit()
            else:
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        _log("info", "worker generation=%s stopped", generation)


async def _claim_campaign(owner: str) -> str | None:
    now = time.time()
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("BEGIN IMMEDIATE")
        row = await (await db.execute(
            "SELECT id FROM campaigns WHERE status IN ('queued','scheduled') "
            "AND (scheduled_at IS NULL OR scheduled_at<=?) AND (lease_until IS NULL OR lease_until<?) "
            "ORDER BY created_at LIMIT 1", (_now(), now)
        )).fetchone()
        if not row:
            await db.commit()
            return None
        campaign_id = str(row[0])
        await db.execute(
            "UPDATE campaigns SET status='running',lease_owner=?,lease_until=?,heartbeat_at=?,started_at=COALESCE(started_at,?),updated_at=? WHERE id=?",
            (owner, now + 30, _now(), _now(), _now(), campaign_id),
        )
        await db.commit()
        return campaign_id


async def _run_campaign(campaign_id: str, owner: str) -> None:
    config = await _get_config()
    concurrency = max(1, min(20, int(config.get("send_concurrency") or 4)))
    gates = {
        "vk": RateGate(float(config.get("vk_rate_per_sec") or 3)),
        "telegram": RateGate(float(config.get("telegram_rate_per_sec") or 15)),
    }
    while owner == _worker_generation and sys.modules.get(__name__) is _module_instance:
        async with aiosqlite.connect(_must_db()) as db:
            db.row_factory = aiosqlite.Row
            campaign = await (await db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
            if not campaign or campaign["status"] != "running" or campaign["lease_owner"] != owner:
                return
            rows = await (await db.execute(
                "SELECT * FROM recipients WHERE campaign_id=? AND status='pending' AND next_attempt_at<=? ORDER BY id LIMIT ?",
                (campaign_id, time.time(), concurrency),
            )).fetchall()
            if rows:
                ids = [row["id"] for row in rows]
                placeholders = ",".join("?" for _ in ids)
                await db.execute(f"UPDATE recipients SET status='sending',updated_at=? WHERE id IN ({placeholders})", (_now(), *ids))
            await db.execute("UPDATE campaigns SET lease_until=?,heartbeat_at=?,updated_at=? WHERE id=?", (time.time() + 30, _now(), _now(), campaign_id))
            await db.commit()
        if not rows:
            async with aiosqlite.connect(_must_db()) as db:
                pending = await (await db.execute(
                    "SELECT MIN(next_attempt_at) FROM recipients WHERE campaign_id=? AND status='pending'",
                    (campaign_id,),
                )).fetchone()
            if pending and pending[0] is not None:
                await asyncio.sleep(max(0.2, min(2.0, float(pending[0]) - time.time())))
                continue
            await _finalize_campaign(campaign_id)
            return
        template = _loads(campaign["template_snapshot_json"], {})
        await asyncio.gather(*[_deliver_recipient(dict(row), template, config, gates[row["channel"]]) for row in rows])
        await _refresh_counts(campaign_id)


async def _deliver_recipient(row: dict[str, Any], template: dict[str, Any], config: dict[str, Any], gate: RateGate) -> None:
    recipient_row_id = int(row["id"])
    async with aiosqlite.connect(_must_db()) as db:
        sent = await (await db.execute("SELECT id FROM sent_messages WHERE recipient_row_id=?", (recipient_row_id,))).fetchone()
        blocked = await (await db.execute("SELECT 1 FROM stop_list WHERE channel=? AND recipient_id=?", (row["channel"], row["recipient_id"]))).fetchone()
        if sent:
            await db.execute("UPDATE recipients SET status='sent',updated_at=? WHERE id=?", (_now(), recipient_row_id))
            await db.commit()
            return
        if blocked:
            await db.execute("UPDATE recipients SET status='skipped',last_error='stop-list',updated_at=? WHERE id=?", (_now(), recipient_row_id))
            await db.commit()
            return
    await gate.wait()
    attempt = int(row["attempts"] or 0) + 1
    try:
        external_id, response = await _send_remote(row, template)
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute(
                "INSERT OR IGNORE INTO sent_messages(campaign_id,recipient_row_id,channel,recipient_id,external_message_id,sent_at) VALUES(?,?,?,?,?,?)",
                (row["campaign_id"], recipient_row_id, row["channel"], row["recipient_id"], external_id, _now()),
            )
            await db.execute(
                "UPDATE recipients SET status='sent',attempts=?,external_message_id=?,sent_at=?,last_error='',updated_at=? WHERE id=?",
                (attempt, external_id, _now(), _now(), recipient_row_id),
            )
            await db.execute(
                "INSERT INTO delivery_attempts(campaign_id,recipient_id,attempt_no,status,response_json) VALUES(?,?,?,?,?)",
                (row["campaign_id"], recipient_row_id, attempt, "sent", _dump(response)),
            )
            await db.commit()
        _log("info", "campaign=%s channel=%s recipient=%s sent", row["campaign_id"], row["channel"], row["recipient_id"])
    except Exception as exc:
        max_attempts = max(1, int(config.get("max_attempts") or 3))
        final = attempt >= max_attempts or isinstance(exc, PermanentDeliveryError)
        next_at = 0 if final else time.time() + min(300, 2 ** attempt * 2)
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "UPDATE recipients SET status=?,attempts=?,next_attempt_at=?,last_error=?,updated_at=? WHERE id=?",
                ("failed" if final else "pending", attempt, next_at, str(exc)[:2000], _now(), recipient_row_id),
            )
            await db.execute(
                "INSERT INTO delivery_attempts(campaign_id,recipient_id,attempt_no,status,error) VALUES(?,?,?,?,?)",
                (row["campaign_id"], recipient_row_id, attempt, "failed" if final else "retry", str(exc)[:2000]),
            )
            await db.commit()
        _log("warning", "campaign=%s channel=%s recipient=%s attempt=%s error=%s", row["campaign_id"], row["channel"], row["recipient_id"], attempt, exc)


class PermanentDeliveryError(RuntimeError):
    pass


async def _send_remote(row: dict[str, Any], template: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if row["channel"] == "vk":
        return await _send_vk(row, template)
    if row["channel"] == "telegram":
        return await _send_telegram(row, template)
    raise PermanentDeliveryError("Неизвестный канал")


def _api_error(data: Any, channel: str) -> Exception:
    if channel == "vk":
        error = data.get("error") if isinstance(data, dict) else None
        code = error.get("error_code") if isinstance(error, dict) else None
        message = error.get("error_msg") if isinstance(error, dict) else str(data)
        if code in {5, 7, 15, 901, 902, 914}:
            return PermanentDeliveryError(f"VK {code}: {message}")
        return RuntimeError(f"VK {code}: {message}")
    description = data.get("description") if isinstance(data, dict) else str(data)
    code = data.get("error_code") if isinstance(data, dict) else None
    if code in {400, 401, 403, 404}:
        return PermanentDeliveryError(f"Telegram {code}: {description}")
    return RuntimeError(f"Telegram {code}: {description}")


async def _send_vk(row: dict[str, Any], template: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
    if not token:
        raise PermanentDeliveryError("SBKVD_LETTER_VK_TOKEN не настроен")
    params: dict[str, Any] = {
        "access_token": token, "v": "5.199", "peer_id": row["recipient_id"],
        "random_id": _stable_random_id(row["campaign_id"], row["recipient_id"]),
        "message": row["rendered_content"], "disable_mentions": 1,
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
    async with httpx.AsyncClient(timeout=35.0) as client:
        response = await client.post("https://api.vk.com/method/messages.send", data=params)
        response.raise_for_status()
        data = response.json()
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
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("INSERT OR REPLACE INTO attachment_cache(channel,file_hash,remote_id,updated_at) VALUES('vk',?,?,?)", (file_hash, remote_id, _now()))
        await db.commit()
    return remote_id


async def _send_telegram(row: dict[str, Any], template: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    token = os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise PermanentDeliveryError("SBKVD_LETTER_TELEGRAM_BOT_TOKEN не настроен")
    base = f"https://api.telegram.org/bot{token}"
    payload: dict[str, Any] = {"chat_id": row["recipient_id"]}
    parse_mode = str(template.get("parse_mode") or "").strip()
    if parse_mode in {"HTML", "MarkdownV2"}:
        payload["parse_mode"] = parse_mode
    keyboard = _keyboard_for_channel(template, "telegram")
    if keyboard:
        payload["reply_markup"] = _dump(keyboard) if not isinstance(keyboard, str) else keyboard
    attachments = template.get("attachment_ids") or []
    async with httpx.AsyncClient(timeout=60.0) as client:
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
            response = await client.post(f"{base}/{method}", data=payload, files={field: (meta["name"], content, meta.get("mime_type"))})
        else:
            payload["text"] = row["rendered_content"]
            payload["disable_web_page_preview"] = True
            response = await client.post(f"{base}/sendMessage", json=payload)
        try:
            data = response.json()
        except Exception:
            response.raise_for_status()
            raise RuntimeError("Telegram вернул некорректный ответ")
    if not data.get("ok"):
        raise _api_error(data, "telegram")
    response.raise_for_status()
    if isinstance(data["result"], list):
        message_ids = [int(item["message_id"]) for item in data["result"]]
        return _dump(message_ids), {"message_ids": message_ids}
    return str(data["result"]["message_id"]), {"message_id": data["result"]["message_id"]}


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
    async with httpx.AsyncClient(timeout=30.0) as client:
        for message_id in message_ids:
            response = await client.post(f"https://api.telegram.org/bot{token}/deleteMessage", json={
                "chat_id": message["recipient_id"], "message_id": int(message_id),
            })
            data = response.json()
            if not data.get("ok"):
                raise _api_error(data, "telegram")


async def _refresh_counts(campaign_id: str) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        rows = await (await db.execute("SELECT status,COUNT(*) FROM recipients WHERE campaign_id=? GROUP BY status", (campaign_id,))).fetchall()
        counts = {row[0]: row[1] for row in rows}
        await db.execute(
            "UPDATE campaigns SET sent=?,failed=?,skipped=?,heartbeat_at=?,lease_until=?,updated_at=? WHERE id=?",
            (counts.get("sent", 0), counts.get("failed", 0), counts.get("skipped", 0), _now(), time.time() + 30, _now(), campaign_id),
        )
        await db.commit()


async def _finalize_campaign(campaign_id: str) -> None:
    await _refresh_counts(campaign_id)
    async with aiosqlite.connect(_must_db()) as db:
        row = await (await db.execute("SELECT failed FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
        status = "completed_with_errors" if row and row[0] else "completed"
        await db.execute(
            "UPDATE campaigns SET status=?,lease_owner=NULL,lease_until=NULL,completed_at=?,updated_at=? WHERE id=? AND status='running'",
            (status, _now(), _now(), campaign_id),
        )
        await db.commit()
    _log("info", "campaign=%s finalized status=%s", campaign_id, status)
