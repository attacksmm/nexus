from __future__ import annotations

import hashlib
import asyncio
import json
import os
import re
import secrets
import unicodedata
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.auth import can_access_module, enforce_rate_limit, require_admin, verify_token_from_request

router = APIRouter()

MODULE_ID = "sales-chats"
SESSION_COOKIE = "sales_chats_session"
SESSION_TTL_DAYS = 30
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.199"
DEFAULT_VK_GROUP_ID = "225075265"
SAFE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
SYSTEM_FIELDS = {"id", "platform_id", "created_at", "updated_at", "table"}
AUTO_TABLE_CONFIGS = {
    "vk_clients": {
        "table_name": "vk_clients",
        "display_name": "Клиенты ВКонтакте",
        "enabled": 1,
        "channel": "vk",
        "recipient_field": "platform_id",
        "name_field": "contact_fields.name",
    },
    "telegram_clients": {
        "table_name": "telegram_clients",
        "display_name": "Клиенты Telegram",
        "enabled": 1,
        "channel": "telegram",
        "recipient_field": "platform_id",
        "name_field": "contact_fields.name",
    },
}
DEFAULT_MESSAGE_TEMPLATES = [
    ("Приветствие", "Здравствуйте, {name}! Пишу из команды Собаковода. Подскажите, пожалуйста, чем могу помочь?"),
    ("Уточнить вопрос", "{name}, вижу ваше сообщение. Уточните, пожалуйста, что именно не получилось, и я помогу разобраться."),
    ("Ссылка на эфир", "{name}, отправляю ссылку на эфир: "),
    (
        "Ссылка с UTM / yclid / ym_uid",
        "Здравствуйте, {name}! Ваша ссылка: https://sobakovod.pro/?utm_source={url.utm_source}&utm_medium={url.utm_medium}&utm_campaign={url.utm_campaign}&utm_content={url.utm_content}&utm_term={url.utm_term}&yclid={url.yclid}&ym_uid={url.ym_uid}&platform_id={url.platform_id}",
    ),
    ("Завершение", "Спасибо за обращение! Если появятся вопросы, напишите сюда, мы на связи."),
]
TEMPLATE_VAR_RE = re.compile(r"\{([A-Za-z0-9_.:-]{1,120})\}")

_db_path: Path | None = None
_module_dir: Path | None = None
_logger = None


class LoginIn(BaseModel):
    login: str


class AccountIn(BaseModel):
    login: str
    display_name: str = ""
    active: bool = True


class TableConfigIn(BaseModel):
    table_name: str
    display_name: str = ""
    enabled: bool = True
    channel: str = "vk"
    recipient_field: str = "platform_id"
    name_field: str = ""


class TablesConfigIn(BaseModel):
    items: list[TableConfigIn] = Field(default_factory=list)


class SendIn(BaseModel):
    thread_id: int
    text: str


class TemplateIn(BaseModel):
    title: str
    text: str
    active: bool = True


class InboundIn(BaseModel):
    channel: str = "telegram"
    recipient_id: str = ""
    chat_id: str = ""
    user_id: str = ""
    platform_id: str = ""
    text: str = ""
    message_id: str = ""
    created_at: str = ""
    name: str = ""
    raw: dict[str, Any] = Field(default_factory=dict)


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", None)
    import asyncio

    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("sales-chats module is not initialized")
    return _db_path


def _modules_dir() -> Path:
    if _module_dir is not None:
        return _module_dir.parent
    return Path(__file__).resolve().parents[1] / "modules"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _session_expires() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, separators=(",", ":"))


def _loads(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return fallback


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _normalize_login(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "").strip()).lower().replace("ё", "е")
    return " ".join(raw.split())


def _cookie_path(request: Request) -> str:
    root_path = request.scope.get("root_path", "") or ""
    return f"{root_path}/{MODULE_ID}".replace("//", "/")


def _customer_db_path() -> Path:
    override = os.getenv("SALES_CHATS_CUSTOMER_DB_PATH", "").strip()
    if override:
        return Path(override)
    candidates = [
        _modules_dir() / "customer-db" / "data" / "customer-db.db",
        _modules_dir().parent / "module_customer_db" / "data" / "customer-db.db",
        _modules_dir().parent / "modules" / "customer-db" / "data" / "customer-db.db",
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


def _telegram_api_base() -> str:
    return (os.getenv("SBKVD_LETTER_TELEGRAM_API_BASE", "").strip() or "https://api.telegram.org").rstrip("/")


def _telegram_proxy_url() -> str:
    return (
        os.getenv("SBKVD_LETTER_TELEGRAM_PROXY_URL", "").strip()
        or os.getenv("TELEGRAM_BOT_API_PROXY_URL", "").strip()
        or os.getenv("TELEGRAM_HTTPS_PROXY_URL", "").strip()
    )


def _vk_group_id() -> str:
    return _clean(os.getenv("VK_GROUP_ID"), 40) or DEFAULT_VK_GROUP_ID


def _vk_dialog_url(recipient_id: str) -> str:
    return f"https://vk.ru/gim{_vk_group_id()}?sel={_clean(recipient_id, 80)}"


@asynccontextmanager
async def _connect(path: Path | None = None):
    db_path = path or _must_db()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(db_path, timeout=60)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA busy_timeout=60000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        yield db
    finally:
        await db.close()


async def _init_db() -> None:
    async with _connect() as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                login_key TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                account_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS allowed_tables (
                table_name TEXT PRIMARY KEY,
                display_name TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                channel TEXT NOT NULL DEFAULT 'vk',
                recipient_field TEXT NOT NULL DEFAULT 'platform_id',
                name_field TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                customer_table TEXT NOT NULL DEFAULT '',
                customer_record_id INTEGER,
                customer_platform_id TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                last_message TEXT NOT NULL DEFAULT '',
                last_message_at TEXT NOT NULL DEFAULT '',
                unread_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(channel, recipient_id)
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                channel TEXT NOT NULL,
                direction TEXT NOT NULL,
                account_id INTEGER,
                author_name TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                external_message_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'sent',
                error TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                dedupe_key TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS message_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                title TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_messages_dedupe ON messages(dedupe_key);
            CREATE INDEX IF NOT EXISTS idx_sales_threads_updated ON threads(updated_at);
            CREATE INDEX IF NOT EXISTS idx_sales_messages_thread ON messages(thread_id, id);
            CREATE INDEX IF NOT EXISTS idx_sales_templates_scope ON message_templates(account_id, active, sort_order, id);
            """
        )
        row = await (await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")).fetchone()
        if not row:
            await db.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES('webhook_secret',?,?)",
                (secrets.token_urlsafe(32), _now()),
            )
        row = await (await db.execute("SELECT COALESCE(MAX(sort_order),0) AS max_order FROM message_templates WHERE account_id IS NULL")).fetchone()
        sort_order = int(row["max_order"] or 0)
        now = _now()
        for title, text in DEFAULT_MESSAGE_TEMPLATES:
            existing = await (await db.execute(
                "SELECT id FROM message_templates WHERE account_id IS NULL AND title=?",
                (title,),
            )).fetchone()
            if not existing:
                sort_order += 1
                await db.execute(
                    """
                    INSERT INTO message_templates(account_id,title,text,active,sort_order,created_at,updated_at)
                    VALUES(NULL,?,?,?,?,?,?)
                    """,
                    (title, text, 1, sort_order, now, now),
                )
        await db.commit()
    _log("info", "sales-chats DB initialized")


async def _require_panel_user(request: Request, *, admin: bool = False) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    if admin and not require_admin(user):
        raise HTTPException(403, "admin required")
    return user


async def _chat_account(request: Request) -> dict[str, Any]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(401, "unauthorized")
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT a.*
            FROM sessions s
            JOIN accounts a ON a.id=s.account_id
            WHERE s.token=? AND s.expires_at>? AND a.active=1
            """,
            (token, _now()),
        )
        row = await cur.fetchone()
    if not row:
        raise HTTPException(401, "unauthorized")
    return dict(row)


async def _setting(key: str, default: str = "") -> str:
    async with _connect() as db:
        row = await (await db.execute("SELECT value FROM settings WHERE key=?", (key,))).fetchone()
    return str(row["value"]) if row else default


async def _known_customer_tables() -> list[dict[str, Any]]:
    path = _customer_db_path()
    if not path.exists():
        return []
    async with _connect(path) as db:
        try:
            rows = await (await db.execute("SELECT name,display_name,description,schema_json FROM _cdb_tables ORDER BY id")).fetchall()
        except Exception:
            return []
        result = []
        for row in rows:
            name = str(row["name"] or "")
            if not SAFE_NAME.fullmatch(name):
                continue
            try:
                count = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{name}")).fetchone())[0]
            except Exception:
                count = 0
            schema = _loads(row["schema_json"], [])
            result.append({**dict(row), "schema_json": schema, "count": int(count or 0)})
    return result


async def _allowed_table_configs() -> list[dict[str, Any]]:
    async with _connect() as db:
        rows = await (await db.execute("SELECT * FROM allowed_tables WHERE enabled=1 ORDER BY display_name,table_name")).fetchall()
    return [dict(row) for row in rows]


async def _effective_table_configs() -> list[dict[str, Any]]:
    configs = await _allowed_table_configs()
    if configs:
        return configs
    known = {item["name"]: item for item in await _known_customer_tables()}
    result: list[dict[str, Any]] = []
    for table_name, cfg in AUTO_TABLE_CONFIGS.items():
        if table_name in known:
            result.append({**cfg, "display_name": known[table_name].get("display_name") or cfg["display_name"]})
    return result


def _path_values(data: Any, path: str) -> list[Any]:
    if not path:
        return []
    current = [data]
    for raw_part in path.split("."):
        part = raw_part[:-2] if raw_part.endswith("[]") else raw_part
        next_values: list[Any] = []
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
    field = _clean(field, 300).strip(".")
    if field in SYSTEM_FIELDS:
        return [record.get(field)]
    return _path_values(record.get("custom_fields") or {}, field)


def _first_value(record: dict[str, Any], field: str) -> str:
    for value in _values(record, field):
        text = _clean(value, 500)
        if text:
            return text
    return ""


def _guess_display_name(record: dict[str, Any], configured_field: str = "") -> str:
    if configured_field:
        value = _first_value(record, configured_field)
        if value:
            return value
    first = _first_value(record, "first_name") or _first_value(record, "name")
    last = _first_value(record, "last_name") or _first_value(record, "second_name")
    full = " ".join(part for part in (first, last) if part).strip()
    if full:
        return full
    for field in ("name", "full_name", "first_name", "client_name", "contact.name", "email", "phone"):
        value = _first_value(record, field)
        if value:
            return value
    for field in ("contact_fields.name", "contact_fields.email", "contact_fields.phone"):
        value = _first_value(record, field)
        if value:
            return value
    return record.get("platform_id") or record.get("recipient_id") or "Клиент"


def _template_lookup_value(key: str, thread: dict[str, Any], customer: dict[str, Any] | None) -> str:
    key = _clean(key, 120).strip()
    if not key:
        return ""
    if key in {"recipient_id", "channel", "customer_table"}:
        return _clean(thread.get(key), 2000)
    if key == "platform_id":
        return _clean((customer or {}).get("platform_id") or thread.get("customer_platform_id") or thread.get("recipient_id"), 2000)
    if key == "name":
        return _guess_display_name(customer) if customer else _clean(thread.get("display_name") or thread.get("recipient_id"), 2000)
    if key == "first_name":
        if customer:
            first = _first_value(customer, "first_name")
            if first:
                return first
            name = _guess_display_name(customer)
        else:
            name = _clean(thread.get("display_name"), 2000)
        return name.split()[0] if name.split() else ""
    if key in SYSTEM_FIELDS and customer:
        return _clean(customer.get(key), 2000)
    if customer:
        value = _first_value(customer, key)
        if value:
            return value
        fields = customer.get("custom_fields") or {}
        if isinstance(fields, dict) and key in fields:
            return _clean(fields.get(key), 2000)
        utms = fields.get("utms") if isinstance(fields, dict) else None
        if isinstance(utms, dict):
            if key in utms:
                return _clean(utms.get(key), 2000)
            if key == "ym_uid" and "_ym_uid" in utms:
                return _clean(utms.get("_ym_uid"), 2000)
    return ""


def _render_template_text(text: str, thread: dict[str, Any], customer: dict[str, Any] | None) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        encode = False
        if key.startswith("url."):
            key = key[4:]
            encode = True
        elif key.startswith("urlencode:"):
            key = key[10:]
            encode = True
        value = _template_lookup_value(key, thread, customer)
        return quote_plus(value) if encode else value

    return _clean(TEMPLATE_VAR_RE.sub(repl, text), 20000)


async def _upsert_thread(
    *,
    channel: str,
    recipient_id: str,
    display_name: str = "",
    customer_table: str = "",
    customer_record_id: int | None = None,
    customer_platform_id: str = "",
    last_message: str = "",
    last_message_at: str = "",
    unread_delta: int = 0,
) -> int:
    channel = _clean(channel, 20).lower()
    recipient_id = _clean(recipient_id, 120)
    now = _now()
    if not channel or not recipient_id:
        raise HTTPException(400, "channel and recipient_id are required")
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO threads(channel,recipient_id,customer_table,customer_record_id,customer_platform_id,display_name,last_message,last_message_at,unread_count,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(channel,recipient_id) DO UPDATE SET
                customer_table=CASE WHEN excluded.customer_table!='' THEN excluded.customer_table ELSE threads.customer_table END,
                customer_record_id=COALESCE(excluded.customer_record_id, threads.customer_record_id),
                customer_platform_id=CASE WHEN excluded.customer_platform_id!='' THEN excluded.customer_platform_id ELSE threads.customer_platform_id END,
                display_name=CASE WHEN excluded.display_name!='' THEN excluded.display_name ELSE threads.display_name END,
                last_message=CASE WHEN excluded.last_message!='' THEN excluded.last_message ELSE threads.last_message END,
                last_message_at=CASE WHEN excluded.last_message_at!='' THEN excluded.last_message_at ELSE threads.last_message_at END,
                unread_count=threads.unread_count + ?,
                updated_at=excluded.updated_at
            """,
            (
                channel,
                recipient_id,
                customer_table,
                customer_record_id,
                customer_platform_id,
                display_name,
                last_message,
                last_message_at,
                max(0, unread_delta),
                now,
                now,
                max(0, unread_delta),
            ),
        )
        cur = await db.execute("SELECT id FROM threads WHERE channel=? AND recipient_id=?", (channel, recipient_id))
        row = await cur.fetchone()
        await db.commit()
    return int(row["id"])


async def _store_message(
    *,
    thread_id: int,
    channel: str,
    direction: str,
    text: str,
    account_id: int | None = None,
    author_name: str = "",
    external_message_id: str = "",
    status: str = "sent",
    error: str = "",
    raw: Any = None,
    created_at: str = "",
) -> bool:
    created = created_at or _now()
    dedupe_key = ""
    if external_message_id:
        dedupe_key = hashlib.sha256(f"{channel}:{external_message_id}".encode()).hexdigest()
    else:
        dedupe_key = hashlib.sha256(f"{thread_id}:{direction}:{created}:{text}".encode()).hexdigest()
    async with _connect() as db:
        try:
            await db.execute(
                """
                INSERT INTO messages(thread_id,channel,direction,account_id,author_name,text,external_message_id,status,error,raw_json,created_at,dedupe_key)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(thread_id),
                    channel,
                    direction,
                    account_id,
                    author_name,
                    _clean(text, 20000),
                    _clean(external_message_id, 160),
                    _clean(status, 40),
                    _clean(error, 2000),
                    _json(raw),
                    created,
                    dedupe_key,
                ),
            )
        except aiosqlite.IntegrityError:
            return False
        await db.execute(
            "UPDATE threads SET last_message=?, last_message_at=?, updated_at=? WHERE id=?",
            (_clean(text, 2000), created, _now(), int(thread_id)),
        )
        await db.commit()
    return True


async def _sync_customer_threads(q: str = "", limit_per_table: int = 1000, channel: str = "") -> int:
    configs = await _effective_table_configs()
    path = _customer_db_path()
    if not configs or not path.exists():
        return 0
    q = _clean(q, 300)
    channel = _clean(channel, 20).lower()
    total = 0
    async with _connect(path) as db:
        for cfg in configs:
            if channel in {"vk", "telegram"} and str(cfg.get("channel") or "").lower() != channel:
                continue
            table = str(cfg["table_name"] or "")
            if not SAFE_NAME.fullmatch(table):
                continue
            try:
                if q:
                    pat = f"%{q}%"
                    rows = await (await db.execute(
                        f"""
                        SELECT id,platform_id,custom_fields,created_at,updated_at
                        FROM cdb_{table}
                        WHERE platform_id LIKE ? OR custom_fields LIKE ?
                        ORDER BY updated_at DESC,id DESC
                        LIMIT ?
                        """,
                        (pat, pat, max(1, min(1000, int(limit_per_table)))),
                    )).fetchall()
                else:
                    rows = await (await db.execute(
                        f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM cdb_{table} ORDER BY updated_at DESC,id DESC LIMIT ?",
                        (max(1, min(2000, int(limit_per_table))),),
                    )).fetchall()
            except Exception:
                continue
            for row in rows:
                record = {
                    "table": table,
                    "id": row["id"],
                    "platform_id": row["platform_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "custom_fields": _loads(row["custom_fields"], {}),
                }
                recipient_id = _first_value(record, cfg.get("recipient_field") or "platform_id")
                if not recipient_id:
                    continue
                await _upsert_thread(
                    channel=str(cfg.get("channel") or "vk"),
                    recipient_id=recipient_id,
                    display_name=_guess_display_name(record, cfg.get("name_field") or ""),
                    customer_table=table,
                    customer_record_id=int(row["id"]),
                    customer_platform_id=str(row["platform_id"] or ""),
                )
                total += 1
    return total


async def _customer_record(table: str, record_id: int | None) -> dict[str, Any] | None:
    if not table or not record_id or not SAFE_NAME.fullmatch(table):
        return None
    path = _customer_db_path()
    if not path.exists():
        return None
    async with _connect(path) as db:
        try:
            row = await (await db.execute(f"SELECT * FROM cdb_{table} WHERE id=?", (record_id,))).fetchone()
        except Exception:
            return None
    if not row:
        return None
    item = dict(row)
    item["custom_fields"] = _loads(item.get("custom_fields"), {})
    item["source_table"] = table
    return item


async def _find_customer_for_thread(channel: str, recipient_id: str) -> dict[str, Any] | None:
    channel = _clean(channel, 20).lower()
    recipient_id = _clean(recipient_id, 120)
    if channel not in {"vk", "telegram"} or not recipient_id:
        return None
    configs = [cfg for cfg in await _effective_table_configs() if str(cfg.get("channel") or "").lower() == channel]
    path = _customer_db_path()
    if not configs or not path.exists():
        return None
    async with _connect(path) as db:
        for cfg in configs:
            table = str(cfg.get("table_name") or "")
            if not SAFE_NAME.fullmatch(table):
                continue
            recipient_field = str(cfg.get("recipient_field") or "platform_id")
            rows = []
            try:
                if recipient_field == "platform_id":
                    rows = await (await db.execute(
                        f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM cdb_{table} WHERE platform_id=? ORDER BY updated_at DESC,id DESC LIMIT 5",
                        (recipient_id,),
                    )).fetchall()
                if not rows:
                    rows = await (await db.execute(
                        f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM cdb_{table} WHERE custom_fields LIKE ? ORDER BY updated_at DESC,id DESC LIMIT 20",
                        (f"%{recipient_id}%",),
                    )).fetchall()
            except Exception:
                continue
            for row in rows:
                record = {
                    "source_table": table,
                    "table": table,
                    "id": row["id"],
                    "platform_id": row["platform_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "custom_fields": _loads(row["custom_fields"], {}),
                }
                if _first_value(record, recipient_field) == recipient_id or record.get("platform_id") == recipient_id or recipient_id in json.dumps(record.get("custom_fields") or {}, ensure_ascii=False):
                    return record
    return None


async def _table_name_from_identifier(identifier: str) -> str:
    raw = _clean(identifier, 180)
    if not raw:
        return ""
    known = await _known_customer_tables()
    by_name = {item["name"]: item for item in known}
    if raw in by_name:
        return raw
    folded = raw.casefold()
    for item in known:
        if str(item.get("display_name") or "").casefold() == folded:
            return str(item["name"])
    slug = re.sub(r"[^0-9a-zа-яё]+", "-", folded).strip("-")
    for item in known:
        candidates = [
            str(item.get("name") or "").casefold(),
            str(item.get("display_name") or "").casefold(),
        ]
        for candidate in candidates:
            if re.sub(r"[^0-9a-zа-яё]+", "-", candidate).strip("-") == slug:
                return str(item["name"])
    return raw if SAFE_NAME.fullmatch(raw) else ""


async def _config_for_table(table: str) -> dict[str, Any]:
    table = _clean(table, 80)
    for cfg in await _effective_table_configs():
        if cfg.get("table_name") == table:
            return cfg
    if table in AUTO_TABLE_CONFIGS:
        return AUTO_TABLE_CONFIGS[table]
    return {
        "table_name": table,
        "display_name": table,
        "enabled": 1,
        "channel": "telegram" if "telegram" in table else "vk",
        "recipient_field": "telegram_id" if "telegram" in table else "vk_id",
        "name_field": "contact_fields.name",
    }


async def _record_by_platform_id(table: str, platform_id: str) -> dict[str, Any] | None:
    table = _clean(table, 80)
    platform_id = _clean(platform_id, 160)
    path = _customer_db_path()
    if not table or not platform_id or not SAFE_NAME.fullmatch(table) or not path.exists():
        return None
    async with _connect(path) as db:
        try:
            row = await (await db.execute(
                f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM cdb_{table} WHERE platform_id=? ORDER BY updated_at DESC,id DESC LIMIT 1",
                (platform_id,),
            )).fetchone()
        except Exception:
            return None
    if not row:
        return None
    return {
        "source_table": table,
        "table": table,
        "id": row["id"],
        "platform_id": row["platform_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "custom_fields": _loads(row["custom_fields"], {}),
    }


async def _vk_api_call(method: str, params: dict[str, Any], *, timeout: float = 20.0) -> dict[str, Any]:
    token = os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()
    if not token:
        raise RuntimeError("SBKVD_LETTER_VK_TOKEN не настроен")
    payload = dict(params)
    payload["access_token"] = token
    payload.setdefault("v", VK_API_VERSION)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(f"{VK_API_BASE}/{method}", data=payload)
    data = response.json()
    if isinstance(data, dict) and data.get("error"):
        error = data["error"]
        raise RuntimeError(f"VK {error.get('error_code')}: {error.get('error_msg') or error}")
    if "response" not in data:
        raise RuntimeError("VK вернул некорректный ответ")
    return data["response"]


def _utc_from_epoch(value: Any) -> str:
    try:
        ts = int(value or 0)
    except Exception:
        ts = 0
    if not ts:
        return _now()
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _sync_vk_conversations() -> int:
    if not os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip():
        return 0
    try:
        response = await _vk_api_call("messages.getConversations", {"count": 100, "filter": "all", "extended": 1}, timeout=20.0)
    except Exception as exc:
        _log("warning", "sales-chats VK conversations sync failed: %s", exc)
        return 0
    profiles: dict[str, str] = {}
    for profile in response.get("profiles") or []:
        pid = _clean(profile.get("id"), 80)
        name = _clean(f"{profile.get('first_name') or ''} {profile.get('last_name') or ''}", 200)
        if pid and name:
            profiles[pid] = name
    count = 0
    for item in response.get("items") or []:
        conversation = item.get("conversation") or {}
        message = item.get("last_message") or {}
        peer = conversation.get("peer") or {}
        peer_id = _clean(peer.get("id"), 80)
        if not peer_id or peer_id.startswith("-"):
            continue
        text = _clean(message.get("text"), 2000)
        msg_id = _clean(message.get("id") or message.get("conversation_message_id"), 80)
        msg_at = _utc_from_epoch(message.get("date"))
        direction = "out" if str(message.get("out") or "0") in {"1", "true", "True"} else "in"
        thread_id = await _upsert_thread(
            channel="vk",
            recipient_id=peer_id,
            display_name=profiles.get(peer_id, ""),
            last_message=text,
            last_message_at=msg_at,
            unread_delta=0,
        )
        async with _connect() as db:
            await db.execute(
                "UPDATE threads SET unread_count=? WHERE id=?",
                (int(conversation.get("unread_count") or 0), thread_id),
            )
            await db.commit()
        if text or msg_id:
            await _store_message(
                thread_id=thread_id,
                channel="vk",
                direction=direction,
                text=text,
                external_message_id=f"{peer_id}:{msg_id}" if msg_id else "",
                status="sent",
                raw=message,
                created_at=msg_at,
            )
        count += 1
    return count


async def _sync_vk_history(thread: dict[str, Any]) -> int:
    if thread.get("channel") != "vk" or not os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip():
        return 0
    peer_id = _clean(thread.get("recipient_id"), 80)
    if not peer_id:
        return 0
    try:
        response = await _vk_api_call("messages.getHistory", {"peer_id": peer_id, "count": 80}, timeout=20.0)
    except Exception as exc:
        _log("warning", "sales-chats VK history sync failed peer_id=%s error=%s", peer_id, exc)
        return 0
    count = 0
    for item in reversed(response.get("items") or []):
        msg_id = _clean(item.get("id") or item.get("conversation_message_id"), 80)
        text = _clean(item.get("text"), 20000)
        created = _utc_from_epoch(item.get("date"))
        direction = "out" if str(item.get("out") or "0") in {"1", "true", "True"} else "in"
        saved = await _store_message(
            thread_id=int(thread["id"]),
            channel="vk",
            direction=direction,
            text=text,
            external_message_id=f"{peer_id}:{msg_id}" if msg_id else "",
            raw=item,
            created_at=created,
        )
        count += 1 if saved else 0
    return count


async def _send_vk(recipient_id: str, text: str) -> tuple[str, dict[str, Any]]:
    random_id = int.from_bytes(hashlib.sha256(f"{recipient_id}:{text}:{_now()}".encode()).digest()[:4], "big") & 0x7FFFFFFF
    response = await _vk_api_call(
        "messages.send",
        {"peer_id": recipient_id, "random_id": random_id or 1, "message": text, "disable_mentions": 1},
        timeout=35.0,
    )
    message_id = response.get("message_id") if isinstance(response, dict) else response
    return str(message_id), {"response": response}


async def _send_telegram(chat_id: str, text: str) -> tuple[str, dict[str, Any]]:
    token = os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("SBKVD_LETTER_TELEGRAM_BOT_TOKEN не настроен")
    url = f"{_telegram_api_base()}/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    async with httpx.AsyncClient(timeout=30.0, proxy=_telegram_proxy_url() or None) as client:
        response = await client.post(url, json=payload)
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {data.get('error_code')}: {data.get('description') or data}")
    message_id = data.get("result", {}).get("message_id")
    return str(message_id), {"response": data}


def _thread_payload(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["vk_url"] = _vk_dialog_url(item["recipient_id"]) if item["channel"] == "vk" else ""
    return item


@router.get("/health")
async def health():
    return {
        "ok": True,
        "module": MODULE_ID,
        "customer_db_ready": _customer_db_path().exists(),
        "env": {
            "vk": bool(os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()),
            "telegram": bool(os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()),
        },
    }


@router.post("/login")
async def login(data: LoginIn, request: Request):
    enforce_rate_limit(request, "sales-chats-login", limit=20, window_seconds=300)
    login_key = _normalize_login(data.login)
    async with _connect() as db:
        row = await (await db.execute("SELECT * FROM accounts WHERE login_key=? AND active=1", (login_key,))).fetchone()
        if not row:
            raise HTTPException(401, "Логин не найден в списке доступа")
        token = secrets.token_urlsafe(40)
        await db.execute("DELETE FROM sessions WHERE expires_at<=?", (_now(),))
        await db.execute(
            "INSERT INTO sessions(token,account_id,expires_at,created_at) VALUES(?,?,?,?)",
            (token, int(row["id"]), _session_expires(), _now()),
        )
        await db.commit()
    response = JSONResponse({"ok": True, "account": {"id": row["id"], "login": row["login"], "display_name": row["display_name"] or row["login"]}})
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=SESSION_TTL_DAYS * 24 * 3600,
        path=_cookie_path(request),
    )
    return response


@router.post("/logout")
async def logout(request: Request):
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
    account = None
    try:
        account = await _chat_account(request)
    except HTTPException:
        pass
    nexus_user = await verify_token_from_request(request)
    return {
        "authenticated": bool(account),
        "account": account and {"id": account["id"], "login": account["login"], "display_name": account["display_name"] or account["login"]},
        "nexus_admin": require_admin(nexus_user),
    }


@router.get("/templates")
async def templates(request: Request):
    account = await _chat_account(request)
    async with _connect() as db:
        rows = await (await db.execute(
            """
            SELECT * FROM message_templates
            WHERE active=1 AND (account_id IS NULL OR account_id=?)
            ORDER BY CASE WHEN account_id IS NULL THEN 0 ELSE 1 END, sort_order, id
            """,
            (int(account["id"]),),
        )).fetchall()
    return {
        "items": [
            {
                "id": int(row["id"]),
                "scope": "personal" if row["account_id"] else "common",
                "title": row["title"],
                "text": row["text"],
            }
            for row in rows
        ]
    }


@router.post("/templates")
async def create_template(data: TemplateIn, request: Request):
    account = await _chat_account(request)
    title = _clean(data.title, 120)
    text = _clean(data.text, 4000)
    if not title or not text:
        raise HTTPException(400, "template title and text are required")
    now = _now()
    async with _connect() as db:
        row = await (await db.execute(
            "SELECT COALESCE(MAX(sort_order),0)+1 AS next_order FROM message_templates WHERE account_id=?",
            (int(account["id"]),),
        )).fetchone()
        cur = await db.execute(
            """
            INSERT INTO message_templates(account_id,title,text,active,sort_order,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (int(account["id"]), title, text, int(data.active), int(row["next_order"] or 1), now, now),
        )
        await db.commit()
    return {"ok": True, "id": int(cur.lastrowid)}


@router.put("/templates/{template_id}")
async def update_template(template_id: int, data: TemplateIn, request: Request):
    account = await _chat_account(request)
    title = _clean(data.title, 120)
    text = _clean(data.text, 4000)
    if not title or not text:
        raise HTTPException(400, "template title and text are required")
    async with _connect() as db:
        cur = await db.execute(
            """
            UPDATE message_templates
            SET title=?, text=?, active=?, updated_at=?
            WHERE id=? AND account_id=?
            """,
            (title, text, int(data.active), _now(), int(template_id), int(account["id"])),
        )
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "template not found")
    return {"ok": True}


@router.delete("/templates/{template_id}")
async def delete_template(template_id: int, request: Request):
    account = await _chat_account(request)
    async with _connect() as db:
        await db.execute(
            "UPDATE message_templates SET active=0, updated_at=? WHERE id=? AND account_id=?",
            (_now(), int(template_id), int(account["id"])),
        )
        await db.commit()
    return {"ok": True}


@router.get("/threads")
async def threads(
    request: Request,
    q: str = "",
    channel: str = "",
    limit: int = Query(120, le=300),
    sync_live: bool = False,
):
    await _chat_account(request)
    channel = _clean(channel, 20).lower()
    await _sync_customer_threads(q=q, limit_per_table=(1000 if q else limit), channel=channel)
    if sync_live and not q and channel in {"", "vk"}:
        await _sync_vk_conversations()
    where = []
    params: list[Any] = []
    if channel in {"vk", "telegram"}:
        where.append("channel=?")
        params.append(channel)
    if q:
        pat = f"%{q}%"
        where.append("(display_name LIKE ? OR recipient_id LIKE ? OR last_message LIKE ? OR customer_platform_id LIKE ?)")
        params.extend([pat, pat, pat, pat])
    sql = "SELECT * FROM threads"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(NULLIF(last_message_at,''), updated_at) DESC LIMIT ?"
    params.append(max(1, min(300, int(limit or 120))))
    async with _connect() as db:
        rows = [dict(row) for row in await (await db.execute(sql, params)).fetchall()]
    return {"items": [_thread_payload(row) for row in rows]}


@router.get("/customer-thread")
async def customer_thread(request: Request, table: str = "", platform_id: str = ""):
    await _chat_account(request)
    table_name = await _table_name_from_identifier(table)
    if not table_name:
        raise HTTPException(404, "customer table not found")
    record = await _record_by_platform_id(table_name, platform_id)
    if not record:
        raise HTTPException(404, "customer record not found")
    cfg = await _config_for_table(table_name)
    recipient_id = _first_value(record, cfg.get("recipient_field") or "platform_id") or str(record.get("platform_id") or "")
    channel = str(cfg.get("channel") or ("telegram" if "telegram" in table_name else "vk")).lower()
    if channel not in {"vk", "telegram"}:
        channel = "vk"
    if not recipient_id:
        raise HTTPException(400, "recipient id not found in customer record")
    thread_id = await _upsert_thread(
        channel=channel,
        recipient_id=recipient_id,
        display_name=_guess_display_name(record, cfg.get("name_field") or ""),
        customer_table=table_name,
        customer_record_id=int(record["id"]),
        customer_platform_id=str(record.get("platform_id") or ""),
    )
    return {"ok": True, "thread_id": thread_id, "channel": channel, "recipient_id": recipient_id, "table": table_name}


@router.get("/threads/{thread_id}")
async def thread_detail(thread_id: int, request: Request):
    await _chat_account(request)
    async with _connect() as db:
        row = await (await db.execute("SELECT * FROM threads WHERE id=?", (int(thread_id),))).fetchone()
    if not row:
        raise HTTPException(404, "thread not found")
    thread = dict(row)
    if thread.get("channel") == "vk" and (thread.get("last_message") or thread.get("last_message_at")):
        try:
            await asyncio.wait_for(_sync_vk_history(thread), timeout=5.0)
        except Exception as exc:
            _log("warning", "sales-chats VK history sync skipped thread_id=%s error=%s", thread_id, exc)
    customer = await _customer_record(thread.get("customer_table") or "", thread.get("customer_record_id"))
    if not customer:
        customer = await _find_customer_for_thread(thread.get("channel") or "", thread.get("recipient_id") or "")
        if customer:
            display_name = _guess_display_name(customer)
            async with _connect() as db:
                await db.execute(
                    """
                    UPDATE threads
                    SET customer_table=?, customer_record_id=?, customer_platform_id=?, display_name=CASE WHEN display_name='' OR display_name=recipient_id THEN ? ELSE display_name END, updated_at=?
                    WHERE id=?
                    """,
                    (
                        customer["source_table"],
                        int(customer["id"]),
                        str(customer.get("platform_id") or ""),
                        display_name,
                        _now(),
                        int(thread_id),
                    ),
                )
                await db.commit()
            thread["customer_table"] = customer["source_table"]
            thread["customer_record_id"] = int(customer["id"])
            thread["customer_platform_id"] = str(customer.get("platform_id") or "")
            if not thread.get("display_name") or thread.get("display_name") == thread.get("recipient_id"):
                thread["display_name"] = display_name
    async with _connect() as db:
        await db.execute("UPDATE threads SET unread_count=0 WHERE id=?", (int(thread_id),))
        rows = await (await db.execute("SELECT * FROM messages WHERE thread_id=? ORDER BY created_at ASC,id ASC LIMIT 500", (int(thread_id),))).fetchall()
        await db.commit()
    return {"thread": _thread_payload(thread), "customer": customer, "messages": [dict(row) for row in rows]}


@router.post("/send")
async def send_message(data: SendIn, request: Request):
    account = await _chat_account(request)
    text = _clean(data.text, 20000)
    if not text:
        raise HTTPException(400, "message is required")
    async with _connect() as db:
        thread = await (await db.execute("SELECT * FROM threads WHERE id=?", (int(data.thread_id),))).fetchone()
    if not thread:
        raise HTTPException(404, "thread not found")
    thread_data = dict(thread)
    customer = await _customer_record(thread_data.get("customer_table") or "", thread_data.get("customer_record_id"))
    text = _render_template_text(text, thread_data, customer)
    if not text:
        raise HTTPException(400, "message is empty after variable substitution")
    try:
        if thread_data["channel"] == "vk":
            external_id, details = await _send_vk(thread_data["recipient_id"], text)
        elif thread_data["channel"] == "telegram":
            external_id, details = await _send_telegram(thread_data["recipient_id"], text)
        else:
            raise RuntimeError("unknown channel")
        status, error = "sent", ""
    except Exception as exc:
        external_id, details = "", {}
        status, error = "failed", str(exc)[:2000]
    await _store_message(
        thread_id=int(data.thread_id),
        channel=thread_data["channel"],
        direction="out",
        account_id=int(account["id"]),
        author_name=account["display_name"] or account["login"],
        text=text,
        external_message_id=external_id,
        status=status,
        error=error,
        raw=details,
        created_at=_now(),
    )
    if status != "sent":
        raise HTTPException(502, error or "send failed")
    return {"ok": True, "external_message_id": external_id}


@router.post("/inbound/telegram")
async def inbound_telegram(data: InboundIn, request: Request, secret: str = ""):
    expected = os.getenv("SALES_CHATS_WEBHOOK_SECRET", "").strip() or await _setting("webhook_secret")
    if expected and secret != expected:
        raise HTTPException(403, "invalid secret")
    recipient_id = _clean(data.chat_id or data.recipient_id or data.platform_id or data.user_id, 120)
    text = _clean(data.text, 20000)
    if not recipient_id or not text:
        raise HTTPException(400, "chat_id and text are required")
    thread_id = await _upsert_thread(
        channel="telegram",
        recipient_id=recipient_id,
        display_name=_clean(data.name, 200),
        last_message=text,
        last_message_at=data.created_at or _now(),
        unread_delta=1,
    )
    saved = await _store_message(
        thread_id=thread_id,
        channel="telegram",
        direction="in",
        text=text,
        external_message_id=_clean(data.message_id, 160),
        raw=data.raw or (data.model_dump() if hasattr(data, "model_dump") else data.dict()),
        created_at=data.created_at or _now(),
    )
    return {"ok": True, "stored": saved, "thread_id": thread_id}


@router.get("/admin/accounts")
async def admin_accounts(request: Request):
    await _require_panel_user(request, admin=True)
    async with _connect() as db:
        rows = await (await db.execute("SELECT * FROM accounts ORDER BY login")).fetchall()
    return {"items": [dict(row) for row in rows]}


@router.post("/admin/accounts")
async def admin_create_account(data: AccountIn, request: Request):
    await _require_panel_user(request, admin=True)
    login = _clean(data.login, 160)
    if not login:
        raise HTTPException(400, "login is required")
    now = _now()
    async with _connect() as db:
        try:
            cur = await db.execute(
                "INSERT INTO accounts(login,login_key,display_name,active,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (login, _normalize_login(login), _clean(data.display_name, 160) or login, int(data.active), now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError as exc:
            raise HTTPException(400, "account already exists") from exc
    return {"ok": True, "id": cur.lastrowid}


@router.put("/admin/accounts/{account_id}")
async def admin_update_account(account_id: int, data: AccountIn, request: Request):
    await _require_panel_user(request, admin=True)
    login = _clean(data.login, 160)
    if not login:
        raise HTTPException(400, "login is required")
    async with _connect() as db:
        await db.execute(
            "UPDATE accounts SET login=?,login_key=?,display_name=?,active=?,updated_at=? WHERE id=?",
            (login, _normalize_login(login), _clean(data.display_name, 160) or login, int(data.active), _now(), int(account_id)),
        )
        await db.execute("DELETE FROM sessions WHERE account_id=? AND ?=0", (int(account_id), int(data.active)))
        await db.commit()
    return {"ok": True}


@router.delete("/admin/accounts/{account_id}")
async def admin_delete_account(account_id: int, request: Request):
    await _require_panel_user(request, admin=True)
    async with _connect() as db:
        await db.execute("DELETE FROM sessions WHERE account_id=?", (int(account_id),))
        await db.execute("DELETE FROM accounts WHERE id=?", (int(account_id),))
        await db.commit()
    return {"ok": True}


@router.get("/admin/templates")
async def admin_templates(request: Request):
    await _require_panel_user(request, admin=True)
    async with _connect() as db:
        rows = await (await db.execute(
            "SELECT * FROM message_templates WHERE account_id IS NULL ORDER BY sort_order,id"
        )).fetchall()
    return {
        "items": [
            {
                "id": int(row["id"]),
                "title": row["title"],
                "text": row["text"],
                "active": bool(row["active"]),
                "sort_order": int(row["sort_order"] or 0),
            }
            for row in rows
        ]
    }


@router.post("/admin/templates")
async def admin_create_template(data: TemplateIn, request: Request):
    await _require_panel_user(request, admin=True)
    title = _clean(data.title, 120)
    text = _clean(data.text, 4000)
    if not title or not text:
        raise HTTPException(400, "template title and text are required")
    now = _now()
    async with _connect() as db:
        row = await (await db.execute(
            "SELECT COALESCE(MAX(sort_order),0)+1 AS next_order FROM message_templates WHERE account_id IS NULL"
        )).fetchone()
        cur = await db.execute(
            """
            INSERT INTO message_templates(account_id,title,text,active,sort_order,created_at,updated_at)
            VALUES(NULL,?,?,?,?,?,?)
            """,
            (title, text, int(data.active), int(row["next_order"] or 1), now, now),
        )
        await db.commit()
    return {"ok": True, "id": int(cur.lastrowid)}


@router.put("/admin/templates/{template_id}")
async def admin_update_template(template_id: int, data: TemplateIn, request: Request):
    await _require_panel_user(request, admin=True)
    title = _clean(data.title, 120)
    text = _clean(data.text, 4000)
    if not title or not text:
        raise HTTPException(400, "template title and text are required")
    async with _connect() as db:
        cur = await db.execute(
            """
            UPDATE message_templates
            SET title=?, text=?, active=?, updated_at=?
            WHERE id=? AND account_id IS NULL
            """,
            (title, text, int(data.active), _now(), int(template_id)),
        )
        await db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "template not found")
    return {"ok": True}


@router.delete("/admin/templates/{template_id}")
async def admin_delete_template(template_id: int, request: Request):
    await _require_panel_user(request, admin=True)
    async with _connect() as db:
        await db.execute(
            "UPDATE message_templates SET active=0, updated_at=? WHERE id=? AND account_id IS NULL",
            (_now(), int(template_id)),
        )
        await db.commit()
    return {"ok": True}


@router.get("/admin/customer-tables")
async def admin_customer_tables(request: Request):
    await _require_panel_user(request, admin=True)
    known = await _known_customer_tables()
    async with _connect() as db:
        configs = {row["table_name"]: dict(row) for row in await (await db.execute("SELECT * FROM allowed_tables")).fetchall()}
    return {"items": [{**item, "config": configs.get(item["name"])} for item in known], "customer_db_ready": _customer_db_path().exists()}


@router.get("/admin/table-config")
async def admin_table_config(request: Request):
    await _require_panel_user(request, admin=True)
    async with _connect() as db:
        rows = await (await db.execute("SELECT * FROM allowed_tables ORDER BY display_name,table_name")).fetchall()
    return {"items": [dict(row) for row in rows]}


@router.put("/admin/table-config")
async def admin_update_table_config(data: TablesConfigIn, request: Request):
    await _require_panel_user(request, admin=True)
    known = {item["name"] for item in await _known_customer_tables()}
    now = _now()
    async with _connect() as db:
        await db.execute("DELETE FROM allowed_tables")
        for item in data.items:
            table = _clean(item.table_name, 80)
            if table not in known or not SAFE_NAME.fullmatch(table):
                continue
            channel = _clean(item.channel, 20).lower()
            if channel not in {"vk", "telegram"}:
                channel = "vk"
            await db.execute(
                """
                INSERT INTO allowed_tables(table_name,display_name,enabled,channel,recipient_field,name_field,updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    table,
                    _clean(item.display_name, 160),
                    int(item.enabled),
                    channel,
                    _clean(item.recipient_field, 300) or "platform_id",
                    _clean(item.name_field, 300),
                    now,
                ),
            )
        await db.commit()
    synced = await _sync_customer_threads()
    return {"ok": True, "synced": synced}


@router.get("/admin/status")
async def admin_status(request: Request):
    await _require_panel_user(request, admin=True)
    secret = os.getenv("SALES_CHATS_WEBHOOK_SECRET", "").strip() or await _setting("webhook_secret")
    root_path = request.scope.get("root_path", "") or ""
    origin = str(request.base_url).rstrip("/")
    base_url = origin if root_path and origin.endswith(root_path) else f"{origin}{root_path}"
    webhook = f"{base_url}/{MODULE_ID}/api/inbound/telegram?secret={secret}"
    return {
        "customer_db_ready": _customer_db_path().exists(),
        "customer_db_path": str(_customer_db_path()),
        "env": {
            "vk": bool(os.getenv("SBKVD_LETTER_VK_TOKEN", "").strip()),
            "telegram": bool(os.getenv("SBKVD_LETTER_TELEGRAM_BOT_TOKEN", "").strip()),
            "telegram_api_base": _telegram_api_base(),
            "telegram_proxy": bool(_telegram_proxy_url()),
            "vk_group_id": _vk_group_id(),
        },
        "webhook_secret_source": "env" if os.getenv("SALES_CHATS_WEBHOOK_SECRET", "").strip() else "db",
        "telegram_inbound_webhook": webhook,
    }
