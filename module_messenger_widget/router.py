from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from orchestrator.auth import (
    _read_env_values,
    _write_env_values,
    can_access_module,
    enforce_rate_limit,
    require_admin,
    verify_token_from_request,
)


_identity_spec = importlib.util.spec_from_file_location(
    "_nexus_mod_getcourse_wazzup_identity",
    Path(__file__).with_name("identity.py"),
)
if _identity_spec is None or _identity_spec.loader is None:
    raise RuntimeError("identity module is unavailable")
_identity = importlib.util.module_from_spec(_identity_spec)
sys.modules[_identity_spec.name] = _identity
_identity_spec.loader.exec_module(_identity)
list_vk_identities = _identity.list_vk_identities
resolve_client_identity = _identity.resolve_client_identity
resolve_sync_identity = _identity.resolve_sync_identity
resolve_vk_identity = _identity.resolve_vk_identity

_graph_spec = importlib.util.spec_from_file_location(
    "_nexus_mod_messenger_widget_identity_graph",
    Path(__file__).with_name("identity_graph.py"),
)
if _graph_spec is None or _graph_spec.loader is None:
    raise RuntimeError("identity graph module is unavailable")
_graph = importlib.util.module_from_spec(_graph_spec)
sys.modules[_graph_spec.name] = _graph
_graph_spec.loader.exec_module(_graph)
IdentityIndex = _graph.IdentityIndex
render_message_template = _graph.render_template
build_context_variables = _graph.build_variables
parse_utm_term = _graph.parse_utm_term


router = APIRouter()

MODULE_ID = "messenger-widget"
ENV_KEY = "NEXUS_MESSENGER_WIDGET_API_KEY"
LEGACY_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_API_KEY"
ORIGIN_ENV_KEY = "NEXUS_MESSENGER_WIDGET_GETCOURSE_ORIGIN"
LEGACY_ORIGIN_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_ALLOWED_ORIGIN"
AMO_ORIGIN_ENV_KEY = "NEXUS_MESSENGER_WIDGET_AMOCRM_ORIGIN"
CUSTOMER_DB_ENV_KEY = "NEXUS_MESSENGER_WIDGET_CUSTOMER_DB_PATH"
WAZZUP_API = "https://api.wazzup24.com/v3"
WAZZUP_APP_API = "https://app.wazzup24.com/api/v2"
DEFAULT_ALLOWED_ORIGIN = "https://club.sobakovod.pro"
DEVICE_TTL_DAYS = 180
ACTIVATION_EXPIRES_AT = "9999-12-31T23:59:59Z"
AUDIT_RETENTION_DAYS = 30
MAX_BODY_BYTES = 32 * 1024
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024
HISTORY_SYNC_TTL_MINUTES = 12 * 60
HISTORY_NOT_FOUND_TTL_MINUTES = 60
HISTORY_ERROR_TTL_MINUTES = 10
HISTORY_NO_ACCESS_TTL_MINUTES = 60
HISTORY_PAGE_SIZE = 50
HISTORY_MAX_CHATS = 500
HISTORY_MAX_MESSAGES = 500
HISTORY_IDENTITY_PROBES = 12
INBOX_LIMIT = 50
CONVERSATION_PAGE_SIZE = 12
CHANNEL_CACHE_SECONDS = 60
CHANNEL_REQUEST_TIMEOUT_SECONDS = 5
CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
CHAT_TRANSPORTS = {
    "max", "maxgroup", "whatsapp", "whatsgroup", "telegram", "telegroup",
    "vk", "viber", "instagram", "avito", "cian", "salebot",
}
CHANNEL_CHAT_TYPES = {
    "max": "max",
    "maxbot": "max",
    "whatsapp": "whatsapp",
    "wapi": "whatsapp",
    "telegram": "telegram",
    "tgapi": "telegram",
    "vk": "vk",
    "viber": "viber",
    "instagram": "instagram",
    "avito": "avito",
    "cian": "cian",
}
PUBLIC_API_BASE = f"https://junior.sobakovod.pro/nexus/{MODULE_ID}/api"
VK_API = "https://api.vk.com/method"
VK_API_VERSION = "5.199"
VK_TOKEN_ENV_KEY = "NEXUS_MESSENGER_WIDGET_VK_GROUP_TOKEN"
LEGACY_VK_TOKEN_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_VK_GROUP_TOKEN"
VK_GROUP_ENV_KEY = "NEXUS_MESSENGER_WIDGET_VK_GROUP_ID"
LEGACY_VK_GROUP_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_VK_GROUP_ID"
VK_PAGE_SIZE = 200
VK_HISTORY_PAGE_SIZE = CONVERSATION_PAGE_SIZE
TELEGRAM_PROVIDER = "telegram_personal"
SALEBOT_PROVIDER = "salebot"
SALEBOT_API_BASE = "https://chatter.salebot.pro/api"
SALEBOT_HISTORY_CACHE_SECONDS = 120
TELEGRAM_SESSION_ENV_KEY = "NEXUS_MESSENGER_WIDGET_TELEGRAM_SESSION_FILE"
LEGACY_TELEGRAM_SESSION_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_TELEGRAM_SESSION_FILE"
TELEGRAM_SYNC_SECONDS = 30
TELEGRAM_DIALOG_LIMIT = 500
TELEGRAM_BACKGROUND_DIALOG_LIMIT = 50
TELEGRAM_HISTORY_PAGE_SIZE = CONVERSATION_PAGE_SIZE
TELEGRAM_HISTORY_CACHE_SECONDS = 60
DIRECT_HISTORY_CACHE_LIMIT = 256
STAFF_CATALOG_CACHE_SECONDS = 300
MAX_AMO_TEMPLATE_IMPORT = 1000
TEMPLATE_VARIABLES = [
    {"key": "contact.name", "label": "Имя и фамилия"},
    {"key": "contact.first_name", "label": "Имя"},
    {"key": "contact.last_name", "label": "Фамилия"},
    {"key": "contact.phone", "label": "Телефон"},
    {"key": "contact.email", "label": "Email"},
    {"key": "manager.name", "label": "Менеджер"},
    {"key": "utm.source", "label": "utm_source"},
    {"key": "utm.medium", "label": "utm_medium"},
    {"key": "utm.campaign", "label": "utm_campaign"},
    {"key": "utm.content", "label": "utm_content"},
    {"key": "utm.term", "label": "utm_term"},
    {"key": "amo.lead.id", "label": "Сделка amoCRM"},
    {"key": "getcourse.id", "label": "Пользователь GetCourse"},
    {"key": "getcourse.order.id", "label": "Заказ GetCourse"},
    {"key": "vk.id", "label": "VK ID"},
    {"key": "telegram.id", "label": "Telegram ID"},
]

_db_path: Path | None = None
_logger = None
_channel_cache: tuple[float, list[dict[str, str]]] = (0.0, [])
_vk_history_cache: dict[tuple[str, int], tuple[float, bool]] = {}
_telegram_history_cache: dict[tuple[str, int], tuple[float, bool]] = {}
_salebot_history_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_telegram_history_inflight: set[tuple[str, int]] = set()
_card_link_cache: dict[tuple[str, ...], tuple[float, dict[str, str]]] = {}
_telegram_state_cache: tuple[float, dict[str, Any]] = (0.0, {})
_telegram_auth_pending: dict[str, dict[str, Any]] = {}
_telegram_lock = asyncio.Lock()
_identity_index: Any = None
_identity_index_status: dict[str, Any] = {"status": "pending", "records": 0}
_staff_catalog_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])


def _remember_direct_history(
    cache: dict[tuple[str, int], tuple[float, bool]],
    key: tuple[str, int],
    has_more: bool,
    ttl_seconds: int,
) -> None:
    now = time.monotonic()
    cache[key] = (now + ttl_seconds, has_more)
    if len(cache) <= DIRECT_HISTORY_CACHE_LIMIT:
        return
    for stale_key, (expires_at, _) in list(cache.items()):
        if expires_at <= now:
            cache.pop(stale_key, None)
    while len(cache) > DIRECT_HISTORY_CACHE_LIMIT:
        cache.pop(next(iter(cache)))


def _card_link_cache_key(context: dict[str, Any], device: dict[str, Any], provider: str, reference: str) -> tuple[str, ...]:
    return (
        str(device.get("id") or ""), provider, context["platform"], context["entity_type"], context["entity_id"],
        _clean(reference, 250), _normalize_phone(context.get("phone")), _clean(context.get("email"), 320).casefold(),
    )


def _remember_card_link(key: tuple[str, ...], link: dict[str, str]) -> None:
    now = time.monotonic()
    _card_link_cache[key] = (now + 60, dict(link))
    while len(_card_link_cache) > DIRECT_HISTORY_CACHE_LIMIT:
        _card_link_cache.pop(next(iter(_card_link_cache)))


async def setup(ctx) -> None:
    global _db_path, _logger, _identity_index
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    await _cleanup()
    _identity_index = IdentityIndex(_customer_db_path(), _must_db().parent / "identity-index.db")
    _identity_index.cleanup_staging()
    lifecycle = getattr(ctx, "lifecycle", None)
    if lifecycle is not None:
        lifecycle.create_task(vk_background_loop(), name="messenger-widget-vk-sync")
        lifecycle.create_task(telegram_background_loop(), name="messenger-widget-telegram-sync")
        lifecycle.create_task(identity_index_loop(), name="messenger-widget-identity-index")


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("messenger-widget is not initialized")
    return _db_path


def _now_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(value: datetime | None = None) -> str:
    return (value or _now_dt()).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _api_key() -> str:
    values = _read_env_values()
    return _clean(os.environ.get(ENV_KEY) or values.get(ENV_KEY) or os.environ.get(LEGACY_ENV_KEY) or values.get(LEGACY_ENV_KEY), 4000)


def _allowed_origin() -> str:
    values = _read_env_values()
    value = _clean(
        os.environ.get(ORIGIN_ENV_KEY) or values.get(ORIGIN_ENV_KEY)
        or os.environ.get(LEGACY_ORIGIN_ENV_KEY) or values.get(LEGACY_ORIGIN_ENV_KEY),
        1000,
    )
    return value.rstrip("/") or DEFAULT_ALLOWED_ORIGIN


def _amo_origin() -> str:
    values = _read_env_values()
    value = _clean(os.environ.get(AMO_ORIGIN_ENV_KEY) or values.get(AMO_ORIGIN_ENV_KEY), 1000).rstrip("/")
    if not value:
        base = _clean(os.environ.get("AMO_BASE_URL") or values.get("AMO_BASE_URL"), 1000).rstrip("/")
        value = base if base.startswith("https://") else ""
    return value


def _allowed_origins() -> set[str]:
    return {value for value in (_allowed_origin(), _amo_origin()) if value}


def _customer_db_path() -> Path:
    values = _read_env_values()
    configured = _clean(os.environ.get(CUSTOMER_DB_ENV_KEY) or values.get(CUSTOMER_DB_ENV_KEY), 4000)
    if configured:
        return Path(configured)
    try:
        return _must_db().parents[2] / "customer-db" / "data" / "customer-db.db"
    except IndexError:
        return Path("/__nexus_no_customer_db__")


def _normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if not 8 <= len(digits) <= 15:
        return ""
    return "+" + digits


def _mask_phone(value: Any) -> str:
    phone = _normalize_phone(value)
    if not phone:
        return ""
    digits = phone[1:]
    if len(digits) <= 6:
        return "+" + "*" * max(0, len(digits) - 2) + digits[-2:]
    return f"+{digits[:2]}{'*' * (len(digits) - 6)}{digits[-4:]}"


def _page_context(value: Any) -> tuple[str, str]:
    text = _clean(value, 2000)
    match = re.search(r"/user/control/user/update/id/(\d+)(?:/|$|[?#])", text, re.I)
    if match:
        return "user", match.group(1)
    match = re.search(r"/sales/control/deal/update/id/(\d+)(?:/|$|[?#])", text, re.I)
    if match:
        return "order", match.group(1)
    return "", ""


def _activation_code() -> str:
    raw = "".join(secrets.choice(CODE_ALPHABET) for _ in range(12))
    return "-".join(raw[index : index + 4] for index in range(0, 12, 4))


def _normalize_code(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())[:32]


def _cors_headers(origin: str) -> dict[str, str]:
    if origin.rstrip("/") not in _allowed_origins():
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Max-Age": "600",
        "Vary": "Origin",
    }


def _is_same_origin_test_request(request: Request) -> bool:
    origin = request.headers.get("origin", "").rstrip("/")
    request_origin = f"{request.url.scheme}://{request.url.netloc}".rstrip("/")
    return (
        request.headers.get("x-nexus-wazzup-test", "") == "1"
        and bool(origin)
        and origin == request_origin
    )


async def _widget_request_mode(request: Request) -> str:
    origin = request.headers.get("origin", "").rstrip("/")
    if origin == _allowed_origin():
        return "getcourse"
    if _amo_origin() and origin == _amo_origin():
        return "amocrm"
    request_origin = f"{request.url.scheme}://{request.url.netloc}".rstrip("/")
    if origin == request_origin and request.headers.get("x-nexus-messenger-platform", "").lower() == "amocrm":
        return "amocrm"
    if _is_same_origin_test_request(request):
        try:
            await _require_admin(request)
        except HTTPException:
            return ""
        return "test"
    return ""


def _widget_response(request: Request, body: dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(body, status_code=status, headers=_cors_headers(request.headers.get("origin", "")))


async def _read_json(request: Request) -> dict[str, Any]:
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_BODY_BYTES:
                raise HTTPException(413, "request too large")
        except ValueError:
            pass
    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        raise HTTPException(413, "request too large")
    try:
        value = json.loads(body or b"{}")
    except json.JSONDecodeError:
        raise HTTPException(400, "invalid json")
    if not isinstance(value, dict):
        raise HTTPException(400, "json object required")
    return value


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


async def _connect():
    db = await aiosqlite.connect(_must_db(), timeout=30)
    await db.execute("PRAGMA busy_timeout=30000")
    await db.execute("PRAGMA foreign_keys=ON")
    db.row_factory = aiosqlite.Row
    return db


async def _init_db() -> None:
    db = await _connect()
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wazzup_user_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'employee',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS activation_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                code_hash TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                used_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                token_hash TEXT NOT NULL UNIQUE,
                token_hint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                device_id INTEGER REFERENCES devices(id) ON DELETE SET NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                page_kind TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                phone_mask TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_kind TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                wazzup_contact_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL DEFAULT '',
                phone_mask TEXT NOT NULL DEFAULT '',
                admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                synced_at TEXT NOT NULL,
                UNIQUE(page_kind,entity_id)
            );
            CREATE TABLE IF NOT EXISTS module_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wazzup_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                phone_hash TEXT NOT NULL DEFAULT '',
                contact_name TEXT NOT NULL DEFAULT '',
                last_message_at TEXT NOT NULL DEFAULT '',
                last_message_preview TEXT NOT NULL DEFAULT '',
                responsible_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(channel_id,chat_type,chat_id)
            );
            CREATE TABLE IF NOT EXISTS wazzup_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT NOT NULL UNIQUE,
                channel_id TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                phone_hash TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                content_uri TEXT NOT NULL DEFAULT '',
                author_name TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS webhook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS history_sync_state (
                channel_id TEXT NOT NULL,
                phone_hash TEXT NOT NULL,
                last_attempt_at TEXT NOT NULL,
                last_success_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                imported INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(channel_id,phone_hash)
            );
            CREATE TABLE IF NOT EXISTS client_links (
                phone_hash TEXT PRIMARY KEY,
                phone TEXT NOT NULL,
                getcourse_user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                responsible_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inbox_devices (
                device_id INTEGER PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
                initialized_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inbox_reads (
                device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
                channel_id TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                last_read_at TEXT NOT NULL,
                PRIMARY KEY(device_id,channel_id,chat_type,chat_id)
            );
            CREATE TABLE IF NOT EXISTS external_identity_links (
                provider TEXT NOT NULL,
                external_user_id TEXT NOT NULL,
                getcourse_user_id TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                source TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(provider,external_user_id)
            );
            CREATE TABLE IF NOT EXISTS manager_bindings (
                platform TEXT NOT NULL,
                platform_user_id TEXT NOT NULL,
                platform_user_email TEXT NOT NULL DEFAULT '',
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(platform,platform_user_id)
            );
            CREATE TABLE IF NOT EXISTS message_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_admin_id INTEGER REFERENCES admins(id) ON DELETE CASCADE,
                folder TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS identity_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                field_key TEXT NOT NULL,
                field_label TEXT NOT NULL DEFAULT '',
                identity_type TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source,entity_type,field_key,identity_type)
            );
            CREATE TABLE IF NOT EXISTS field_catalog (
                source TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                field_key TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                value_type TEXT NOT NULL DEFAULT 'text',
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY(source,entity_type,field_key)
            );
            CREATE TABLE IF NOT EXISTS entity_identity_links (
                platform TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                external_user_id TEXT NOT NULL,
                confirmed_by INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(platform,entity_type,entity_id,provider)
            );
            CREATE INDEX IF NOT EXISTS ix_gcw_devices_token ON devices(token_hash);
            CREATE INDEX IF NOT EXISTS ix_gcw_codes_hash ON activation_codes(code_hash);
            CREATE INDEX IF NOT EXISTS ix_gcw_events_created ON events(created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_contacts_synced ON contacts(synced_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_chats_phone ON wazzup_chats(phone_hash,updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_messages_chat ON wazzup_messages(channel_id,chat_type,chat_id,sent_at,id);
            CREATE INDEX IF NOT EXISTS ix_gcw_messages_phone ON wazzup_messages(phone_hash,sent_at,id);
            CREATE INDEX IF NOT EXISTS ix_gcw_webhook_received ON webhook_events(received_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_history_sync_attempt ON history_sync_state(last_attempt_at DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_client_links_gc ON client_links(getcourse_user_id,updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_inbox_reads_device ON inbox_reads(device_id,last_read_at DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_external_links_gc ON external_identity_links(provider,getcourse_user_id);
            CREATE INDEX IF NOT EXISTS ix_mw_manager_admin ON manager_bindings(admin_id,platform);
            CREATE INDEX IF NOT EXISTS ix_mw_templates_owner ON message_templates(owner_admin_id,folder,enabled,sort_order,id);
            CREATE INDEX IF NOT EXISTS ix_mw_rules_priority ON identity_rules(enabled,priority,id);
            CREATE INDEX IF NOT EXISTS ix_mw_entity_links_external ON entity_identity_links(provider,external_user_id);
            """
        )
        columns = {row[1] for row in await (await db.execute("PRAGMA table_info(admins)")).fetchall()}
        if "phone" not in columns:
            await db.execute("ALTER TABLE admins ADD COLUMN phone TEXT NOT NULL DEFAULT ''")
        if "role" not in columns:
            await db.execute("ALTER TABLE admins ADD COLUMN role TEXT NOT NULL DEFAULT 'employee'")
        chat_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(wazzup_chats)")).fetchall()}
        if "responsible_admin_id" not in chat_columns:
            await db.execute("ALTER TABLE wazzup_chats ADD COLUMN responsible_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL")
        link_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(client_links)")).fetchall()}
        if "responsible_admin_id" not in link_columns:
            await db.execute("ALTER TABLE client_links ADD COLUMN responsible_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL")
        template_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(message_templates)")).fetchall()}
        if "folder" not in template_columns:
            await db.execute("ALTER TABLE message_templates ADD COLUMN folder TEXT NOT NULL DEFAULT ''")
        await db.execute("CREATE INDEX IF NOT EXISTS ix_mw_chats_responsible ON wazzup_chats(responsible_admin_id,last_message_at DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS ix_mw_external_links_phone ON external_identity_links(provider,phone)")
        await db.execute(
            "UPDATE admins SET role='admin' WHERE trim(name) IN ('Никита','никита','НИКИТА') OR trim(name) LIKE 'Никита %'"
        )
        device_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(devices)")).fetchall()}
        for name, ddl in (
            ("platform", "TEXT NOT NULL DEFAULT 'getcourse'"),
            ("platform_user_id", "TEXT NOT NULL DEFAULT ''"),
            ("platform_user_email", "TEXT NOT NULL DEFAULT ''"),
        ):
            if name not in device_columns:
                await db.execute(f"ALTER TABLE devices ADD COLUMN {name} {ddl}")
        now = _iso()
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('webhook_secret',?,?)",
            (secrets.token_urlsafe(32), now),
        )
        defaults = (
            ("getcourse", "user", "id", "ID пользователя", "getcourse_user", 10),
            ("getcourse", "user", "vk_id", "VK ID", "vk", 20),
            ("getcourse", "user", "telegram_id", "Telegram ID", "telegram", 20),
            ("getcourse", "user", "salebot_id", "SaleBot ID", "salebot", 30),
            ("getcourse", "user", "phone", "Телефон", "phone", 80),
            ("getcourse", "user", "email", "Email", "email", 90),
            ("getcourse", "order", "user_id", "ID пользователя", "getcourse_user", 10),
            ("getcourse", "order", "utm_term", "utm_term", "utm_term", 40),
            ("getcourse", "order", "phone", "Телефон", "phone", 80),
            ("getcourse", "order", "email", "Email", "email", 90),
            ("amocrm", "lead", "utm_term", "utm_term", "utm_term", 40),
            ("amocrm", "lead", "phone", "Телефон", "phone", 80),
            ("amocrm", "lead", "email", "Email", "email", 90),
            ("amocrm", "contact", "phone", "Телефон", "phone", 80),
            ("amocrm", "contact", "email", "Email", "email", 90),
        )
        await db.executemany(
            """INSERT OR IGNORE INTO identity_rules(
               source,entity_type,field_key,field_label,identity_type,priority,enabled,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,1,?,?)""",
            ((*row, now, now) for row in defaults),
        )
        await db.commit()
    finally:
        await db.close()


async def _cleanup() -> None:
    now = _iso()
    cutoff = _iso(_now_dt() - timedelta(days=AUDIT_RETENTION_DAYS))
    db = await _connect()
    try:
        await db.execute("DELETE FROM activation_codes WHERE expires_at<? OR used_at<>''", (now,))
        await db.execute("DELETE FROM events WHERE created_at<?", (cutoff,))
        await db.execute("DELETE FROM webhook_events WHERE received_at<?", (_iso(_now_dt() - timedelta(days=14)),))
        await db.execute("DELETE FROM wazzup_messages WHERE created_at<?", (_iso(_now_dt() - timedelta(days=180)),))
        await db.commit()
    finally:
        await db.close()


async def _audit(
    action: str,
    status: str,
    *,
    admin_id: int | None = None,
    device_id: int | None = None,
    page_kind: str = "",
    entity_id: str = "",
    phone: str = "",
    error: str = "",
) -> None:
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO events(admin_id,device_id,action,status,page_kind,entity_id,phone_mask,error,created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                admin_id,
                device_id,
                _clean(action, 80),
                _clean(status, 40),
                _clean(page_kind, 20),
                _clean(entity_id, 100),
                _mask_phone(phone),
                _clean(error, 400),
                _iso(),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user) or not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "Доступ разрешён только администратору")
    return user


async def _wazzup_request(
    method: str, path: str, payload: Any = None, *, timeout_seconds: float = 12,
) -> Any:
    key = _api_key()
    if not key:
        raise HTTPException(503, "Wazzup API key не настроен")
    headers = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=False) as client:
            response = await client.request(method, WAZZUP_API + path, headers=headers, json=payload)
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        raise HTTPException(502, f"Wazzup недоступен: {type(exc).__name__}")
    try:
        body = response.json()
    except ValueError:
        body = {}
    if response.status_code >= 400:
        error = body.get("error") if isinstance(body, dict) else ""
        if isinstance(error, dict):
            error = error.get("code") or error.get("description") or ""
        detail = _clean(error or (body.get("description") if isinstance(body, dict) else ""), 200)
        raise HTTPException(502, f"Wazzup HTTP {response.status_code}" + (f": {detail}" if detail else ""))
    return body


def _users_from_response(body: Any) -> list[dict[str, str]]:
    rows = body.get("data") if isinstance(body, dict) else body
    if not isinstance(rows, list):
        return []
    result: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        user_id = _clean(row.get("id"), 200)
        name = _clean(row.get("name"), 200)
        if user_id:
            result.append({"id": user_id, "name": name or user_id})
    return result


def _active_chat_channels(body: Any) -> list[dict[str, str]]:
    rows = body if isinstance(body, list) else body.get("data") if isinstance(body, dict) else []
    if not isinstance(rows, list):
        return []
    channels: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        channel_transport = _clean(row.get("transport"), 40).lower()
        transport = CHANNEL_CHAT_TYPES.get(channel_transport, "")
        channel_id = _clean(row.get("channelId") or row.get("id"), 200)
        if transport not in CHAT_TRANSPORTS or not channel_id or channel_id in seen:
            continue
        if _clean(row.get("state"), 40).lower() != "active":
            continue
        seen.add(channel_id)
        if channel_transport == "telegram":
            label = f"Telegram Bot · {_clean(row.get('name'), 120) or transport.upper()}"
        elif channel_transport == "tgapi":
            label = f"Telegram Personal · {_clean(row.get('name'), 120) or transport.upper()}"
        else:
            label = f"{transport.upper()} · {_clean(row.get('name'), 120) or transport.upper()}"
        channels.append(
            {
                "channel_id": channel_id,
                "provider": "wazzup",
                "transport": transport,
                "channel_transport": channel_transport,
                "name": _clean(row.get("name"), 120) or transport.upper(),
                "plain_id": _clean(row.get("plainId"), 120),
                "label": label,
            }
        )
    return channels


def _vk_attachment_views(rows: Any) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for attachment in rows if isinstance(rows, list) else []:
        if not isinstance(attachment, dict):
            continue
        kind = _clean(attachment.get("type"), 40).lower()
        data = attachment.get(kind) if isinstance(attachment.get(kind), dict) else {}
        uri = filename = content_type = ""
        if kind == "photo":
            sizes = data.get("sizes") if isinstance(data.get("sizes"), list) else []
            candidates = [item for item in sizes if isinstance(item, dict) and _clean(item.get("url"), 4000)]
            if candidates:
                best = max(candidates, key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0))
                uri, content_type = _clean(best.get("url"), 4000), "image"
        elif kind == "doc":
            uri = _clean(data.get("url"), 4000)
            filename = _clean(data.get("title"), 500)
            content_type = "document"
        elif kind == "audio":
            uri = _clean(data.get("url"), 4000)
            filename = _clean(data.get("title"), 500)
            content_type = "audio"
        elif kind == "sticker":
            images = data.get("images_with_background") or data.get("images") or []
            candidates = [item for item in images if isinstance(item, dict) and _clean(item.get("url"), 4000)]
            if candidates:
                best = max(candidates, key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0))
                uri, content_type = _clean(best.get("url"), 4000), "image"
        elif kind == "video":
            owner = _clean(data.get("owner_id"), 80)
            item_id = _clean(data.get("id"), 80)
            if owner and item_id:
                uri, content_type = f"https://vk.com/video{owner}_{item_id}", "video"
        if uri:
            result.append({"content_uri": uri, "content_type": content_type, "filename": filename})
    return result


async def _store_vk_messages(peer_id: str, rows: list[dict[str, Any]], identity: dict[str, Any]) -> int:
    channel_id = _vk_channel_id()
    group_id = _vk_group_id()
    if not channel_id or not peer_id:
        return 0
    now = _iso()
    inserted = 0
    name = _clean(identity.get("name"), 200) or f"VK {peer_id}"
    db = await _connect()
    try:
        for row in rows:
            message_id = _clean(row.get("id") or row.get("conversation_message_id"), 200)
            if not message_id:
                continue
            direction = "outgoing" if _clean(row.get("from_id"), 200).lstrip("-") == group_id else "incoming"
            sent_at = _message_time(row.get("date"))
            text_value = _clean(row.get("text"), 20_000)
            attachments = _vk_attachment_views(row.get("attachments"))
            first = attachments[0] if attachments else {"content_uri": "", "content_type": "", "filename": ""}
            raw = dict(row)
            raw["nexus_attachments"] = attachments
            external_id = f"vk:{group_id}:{peer_id}:{message_id}"
            cursor = await db.execute(
                """INSERT OR IGNORE INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (external_id, channel_id, "vk", peer_id, "", direction, "delivered", text_value, first["content_uri"], name if direction == "incoming" else "Сообщество", sent_at, json.dumps(raw, ensure_ascii=False, separators=(",", ":"))[:50_000], now),
            )
            inserted += max(0, cursor.rowcount)
            preview = text_value or ("Изображение" if first["content_type"] == "image" else "Вложение")
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
                   contact_name=excluded.contact_name,last_message_at=CASE WHEN excluded.last_message_at>wazzup_chats.last_message_at THEN excluded.last_message_at ELSE wazzup_chats.last_message_at END,
                   last_message_preview=CASE WHEN excluded.last_message_at>=wazzup_chats.last_message_at THEN excluded.last_message_preview ELSE wazzup_chats.last_message_preview END,
                   updated_at=excluded.updated_at""",
                (channel_id, "vk", peer_id, "", name, sent_at, preview[:500], now, now),
            )
        await db.commit()
    finally:
        await db.close()
    return inserted


async def _refresh_vk_placeholder_names() -> int:
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """SELECT DISTINCT chat_id FROM wazzup_chats
                   WHERE chat_type='vk' AND contact_name='VK '||chat_id
                     AND chat_id<>'' AND chat_id NOT GLOB '*[^0-9]*'
                     AND CAST(chat_id AS INTEGER) BETWEEN 1 AND 1999999999
                   ORDER BY last_message_at DESC LIMIT 5000"""
            )
        ).fetchall()
    finally:
        await db.close()
    peer_ids = [_clean(row["chat_id"], 200) for row in rows]
    profiles: list[dict[str, Any]] = []
    for start in range(0, len(peer_ids), 1000):
        result = await _vk_request("users.get", {"user_ids": ",".join(peer_ids[start:start + 1000])})
        if isinstance(result, list):
            profiles.extend(row for row in result if isinstance(row, dict))
    names = {
        _clean(row.get("id"), 200): " ".join(filter(None, (
            _clean(row.get("first_name"), 120), _clean(row.get("last_name"), 120),
        ))).strip()
        for row in profiles
    }
    names = {peer_id: name for peer_id, name in names.items() if peer_id and name}
    if not names:
        return 0
    now = _iso()
    db = await _connect()
    try:
        for peer_id, name in names.items():
            await db.execute(
                "UPDATE wazzup_chats SET contact_name=?,updated_at=? WHERE chat_type='vk' AND chat_id=? AND contact_name='VK '||chat_id",
                (name, now, peer_id),
            )
            await db.execute(
                """INSERT INTO external_identity_links(provider,external_user_id,getcourse_user_id,phone,email,name,source,updated_at)
                   VALUES('vk',?,'','','',?,'vk-profile',?) ON CONFLICT(provider,external_user_id) DO UPDATE SET
                   name=excluded.name,source=excluded.source,updated_at=excluded.updated_at""",
                (peer_id, name, now),
            )
        await db.commit()
    finally:
        await db.close()
    return len(names)


async def _sync_vk_conversations(*, full: bool = False) -> dict[str, int]:
    if not _vk_token() or not _vk_group_id():
        return {"conversations": 0, "messages": 0, "names": 0}
    try:
        names = await _refresh_vk_placeholder_names()
    except Exception:
        names = 0
        _log("warning", "VK profile refresh failed")
    await _refresh_vk_links()
    offset = conversations = messages = 0
    max_pages = 100 if full else 1
    for _ in range(max_pages):
        response = await _vk_request("messages.getConversations", {"group_id": _vk_group_id(), "count": VK_PAGE_SIZE, "offset": offset, "extended": 1})
        items = response.get("items") if isinstance(response, dict) and isinstance(response.get("items"), list) else []
        profiles = response.get("profiles") if isinstance(response, dict) and isinstance(response.get("profiles"), list) else []
        names = {
            _clean(row.get("id"), 200): " ".join(filter(None, (
                _clean(row.get("first_name"), 120), _clean(row.get("last_name"), 120),
            ))).strip()
            for row in profiles if isinstance(row, dict)
        }
        if not items:
            break
        for item in items:
            conversation = item.get("conversation") if isinstance(item, dict) and isinstance(item.get("conversation"), dict) else {}
            peer = conversation.get("peer") if isinstance(conversation.get("peer"), dict) else {}
            peer_id = _clean(peer.get("id"), 200)
            if not peer_id or _clean(peer.get("type"), 40) != "user":
                continue
            link = await _external_link(peer_id=peer_id)
            profile_name = _clean(names.get(peer_id), 200)
            if not link or (profile_name and _clean(link.get("name"), 200) != profile_name):
                identity = dict(link or {})
                identity["name"] = profile_name or f"VK {peer_id}"
                await _remember_external_link(
                    identity,
                    "vk-dialog", provider="vk", external_user_id=peer_id,
                )
                link = await _external_link(peer_id=peer_id)
            last = item.get("last_message") if isinstance(item.get("last_message"), dict) else {}
            if last:
                messages += await _store_vk_messages(peer_id, [last], link)
            conversations += 1
        offset += len(items)
        if len(items) < VK_PAGE_SIZE:
            break
    await _set_setting("vk_last_sync_at", _iso())
    return {"conversations": conversations, "messages": messages, "names": names}


async def _load_vk_history(peer_id: str, *, offset: int = 0, identity: dict[str, Any] | None = None) -> tuple[int, bool]:
    cache_key = (peer_id, offset)
    cached = _vk_history_cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return 0, cached[1]
    if cached:
        _vk_history_cache.pop(cache_key, None)
    link = await _external_link(peer_id=peer_id) or identity
    if not link:
        raise HTTPException(404, "Диалог не связан с GetCourse")
    response = await _vk_request("messages.getHistory", {"group_id": _vk_group_id(), "peer_id": peer_id, "count": VK_HISTORY_PAGE_SIZE, "offset": max(0, offset)})
    rows = response.get("items") if isinstance(response, dict) and isinstance(response.get("items"), list) else []
    await _store_vk_messages(peer_id, rows, link)
    total = int(response.get("count") or 0) if isinstance(response, dict) else 0
    has_more = offset + len(rows) < total
    _remember_direct_history(_vk_history_cache, cache_key, has_more, 60)
    return len(rows), has_more


async def vk_background_loop() -> None:
    while True:
        try:
            if _vk_token() and _vk_group_id():
                await _sync_vk_conversations(full=False)
        except Exception:
            _log("warning", "VK reconciliation failed")
        await __import__("asyncio").sleep(300)


async def identity_index_loop() -> None:
    global _identity_index_status
    first_run = True
    while True:
        try:
            if _identity_index is not None:
                current = _identity_index.status()
                if first_run and current.get("status") == "ready":
                    _identity_index_status = current
                else:
                    _identity_index_status = await asyncio.to_thread(_identity_index.build_if_changed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _identity_index_status = {"status": "error", "error": _clean(exc, 300), "records": 0}
            _log("warning", "Messenger identity index failed: %s", exc)
        first_run = False
        await asyncio.sleep(3600)


async def _cached_active_channels(*, refresh: bool = False) -> list[dict[str, str]]:
    global _channel_cache
    expires_at, rows = _channel_cache
    if not refresh and rows and expires_at > time.monotonic():
        return [dict(row) for row in rows]
    try:
        rows = _active_chat_channels(await _wazzup_request(
            "GET", "/channels", timeout_seconds=CHANNEL_REQUEST_TIMEOUT_SECONDS,
        ))
    except HTTPException:
        if rows:
            _channel_cache = (time.monotonic() + CHANNEL_CACHE_SECONDS, [dict(row) for row in rows])
            return [dict(row) for row in rows]
        stored = await _stored_wazzup_channels()
        if stored:
            _channel_cache = (time.monotonic() + CHANNEL_CACHE_SECONDS, [dict(row) for row in stored])
            return stored
        raise
    _channel_cache = (time.monotonic() + CHANNEL_CACHE_SECONDS, [dict(row) for row in rows])
    return rows


async def _stored_wazzup_channels() -> list[dict[str, str]]:
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT DISTINCT channel_id,chat_type FROM wazzup_chats
               WHERE channel_id NOT LIKE 'vk:%' AND channel_id NOT LIKE 'telegram-personal:%'
               ORDER BY channel_id,chat_type"""
        )).fetchall()
    finally:
        await db.close()
    return [
        {
            "channel_id": _clean(row["channel_id"], 200),
            "provider": "wazzup",
            "transport": _clean(row["chat_type"], 40).lower(),
            "channel_transport": _clean(row["chat_type"], 40).lower(),
            "name": _clean(row["chat_type"], 40).upper(),
            "plain_id": "",
            "label": _clean(row["chat_type"], 40).upper(),
        }
        for row in rows
        if _clean(row["channel_id"], 200) and _clean(row["chat_type"], 40).lower() in CHAT_TRANSPORTS
    ]


async def _setting(key: str) -> str:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT value FROM module_settings WHERE key=?", (key,))).fetchone()
        return _clean(row["value"], 1000) if row else ""
    finally:
        await db.close()


async def _set_setting(key: str, value: Any) -> None:
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
            (_clean(key, 120), _clean(value, 4000), _iso()),
        )
        await db.commit()
    finally:
        await db.close()


def _vk_token() -> str:
    values = _read_env_values()
    return _clean(
        os.environ.get(VK_TOKEN_ENV_KEY)
        or values.get(VK_TOKEN_ENV_KEY)
        or os.environ.get(LEGACY_VK_TOKEN_ENV_KEY)
        or values.get(LEGACY_VK_TOKEN_ENV_KEY)
        or os.environ.get("VK_GROUP_TOKEN")
        or values.get("VK_GROUP_TOKEN"),
        8000,
    )


def _vk_group_id() -> str:
    values = _read_env_values()
    value = (
        os.environ.get(VK_GROUP_ENV_KEY)
        or values.get(VK_GROUP_ENV_KEY)
        or os.environ.get(LEGACY_VK_GROUP_ENV_KEY)
        or values.get(LEGACY_VK_GROUP_ENV_KEY)
        or os.environ.get("VK_GROUP_ID")
        or values.get("VK_GROUP_ID")
    )
    return re.sub(r"\D+", "", _clean(value, 200))


def _vk_channel_id() -> str:
    return f"vk:{_vk_group_id()}" if _vk_group_id() else ""


def _vk_callback_secret(value: Any = "") -> str:
    current = _clean(value, 100)
    return current if re.fullmatch(r"[A-Za-z0-9]{1,50}", current) else secrets.token_hex(16)


def _vk_reference(value: Any) -> str:
    text = _clean(value, 1000).strip().rstrip("/")
    if re.fullmatch(r"\d+", text):
        return text
    match = re.search(r"(?:^|/)id(\d+)(?:$|[/?#])", text, re.I)
    if match:
        return match.group(1)
    match = re.search(r"(?:vk\.com|vkontakte\.ru)/([A-Za-z0-9_.]+)", text, re.I)
    return match.group(1) if match else ""


async def _vk_peer_id(value: Any) -> str:
    reference = _vk_reference(value)
    if not reference or reference.isdigit():
        return reference
    response = await _vk_request("users.get", {"user_ids": reference})
    rows = response if isinstance(response, list) else []
    return _clean(rows[0].get("id"), 200) if rows and isinstance(rows[0], dict) else ""


async def _vk_request(method: str, params: dict[str, Any] | None = None) -> Any:
    token = _vk_token()
    if not token or not _vk_group_id():
        raise HTTPException(503, "VK не подключён")
    payload = {**(params or {}), "access_token": token, "v": VK_API_VERSION}
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
            response = await client.post(f"{VK_API}/{method}", data=payload)
        body = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(502, f"VK недоступен: {type(exc).__name__}")
    if response.status_code >= 400:
        raise HTTPException(502, f"VK HTTP {response.status_code}")
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        code = _clean(error.get("error_code"), 20)
        message = _clean(error.get("error_msg"), 240)
        raise HTTPException(502, f"VK {code}: {message}".rstrip(": "))
    return body.get("response") if isinstance(body, dict) else body


async def _remember_external_link(
    identity: dict[str, Any],
    source: str,
    *,
    provider: str = "vk",
    external_user_id: Any = "",
) -> None:
    provider = _clean(provider, 40)
    peer_id = _clean(external_user_id or identity.get("vk_id"), 200)
    gc_id = _clean(identity.get("getcourse_user_id"), 200)
    if not provider or not peer_id:
        return
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO external_identity_links(provider,external_user_id,getcourse_user_id,phone,email,name,source,updated_at)
               VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(provider,external_user_id) DO UPDATE SET
               getcourse_user_id=CASE WHEN excluded.getcourse_user_id<>'' THEN excluded.getcourse_user_id ELSE external_identity_links.getcourse_user_id END,
               phone=CASE WHEN excluded.phone<>'' THEN excluded.phone ELSE external_identity_links.phone END,
               email=CASE WHEN excluded.email<>'' THEN excluded.email ELSE external_identity_links.email END,
               name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE external_identity_links.name END,
               source=excluded.source,updated_at=excluded.updated_at""",
            (provider, peer_id, gc_id, _normalize_phone(identity.get("phone")), _clean(identity.get("email"), 320), _clean(identity.get("name"), 200), _clean(source, 80), _iso()),
        )
        await db.commit()
    finally:
        await db.close()


async def _external_link(*, peer_id: str = "", gc_id: str = "", provider: str = "vk") -> dict[str, str]:
    if not peer_id and not gc_id:
        return {}
    provider = _clean(provider, 40)
    db = await _connect()
    try:
        if peer_id:
            row = await (await db.execute(
                "SELECT * FROM external_identity_links WHERE provider=? AND external_user_id=?", (provider, peer_id)
            )).fetchone()
        else:
            row = await (await db.execute(
                "SELECT * FROM external_identity_links WHERE provider=? AND getcourse_user_id=? ORDER BY updated_at DESC LIMIT 1", (provider, gc_id)
            )).fetchone()
        return dict(row) if row else {}
    finally:
        await db.close()


async def _external_link_for_identity(provider: str, *, phone: str = "", gc_id: str = "") -> dict[str, str]:
    phone = _normalize_phone(phone)
    gc_id = _clean(gc_id, 200)
    if not phone and not gc_id:
        return {}
    clauses: list[str] = []
    params: list[Any] = [_clean(provider, 40)]
    if gc_id:
        clauses.append("getcourse_user_id=?")
        params.append(gc_id)
    if phone:
        clauses.append("phone=?")
        params.append(phone)
    db = await _connect()
    try:
        row = await (await db.execute(
            f"SELECT * FROM external_identity_links WHERE provider=? AND ({' OR '.join(clauses)}) ORDER BY updated_at DESC LIMIT 1",
            params,
        )).fetchone()
        return dict(row) if row else {}
    finally:
        await db.close()


async def _entity_external_link(
    platform: str,
    entity_type: str,
    entity_id: str,
    provider: str,
) -> dict[str, str]:
    if not platform or not entity_type or not entity_id or not provider:
        return {}
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT external_user_id FROM entity_identity_links
               WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?""",
            (platform, entity_type, entity_id, provider),
        )).fetchone()
    finally:
        await db.close()
    return await _external_link(peer_id=row["external_user_id"], provider=provider) if row else {}


async def _remember_entity_external_link(
    context: dict[str, Any],
    provider: str,
    external_user_id: str,
    admin_id: int | None,
) -> None:
    platform = _clean(context.get("platform"), 40)
    entity_type = _clean(context.get("entity_type"), 40)
    entity_id = _clean(context.get("entity_id"), 200)
    if not platform or not entity_type or not entity_id or not provider or not external_user_id:
        return
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO entity_identity_links(platform,entity_type,entity_id,provider,external_user_id,confirmed_by,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(platform,entity_type,entity_id,provider) DO UPDATE SET
               external_user_id=excluded.external_user_id,confirmed_by=excluded.confirmed_by,updated_at=excluded.updated_at""",
            (platform, entity_type, entity_id, provider, external_user_id, admin_id, now, now),
        )
        await db.commit()
    finally:
        await db.close()


async def _vk_card_link(data: dict[str, Any]) -> dict[str, str]:
    page_kind, gc_id = _page_context(data.get("source_url"))
    if page_kind != "user" or not gc_id:
        return {}
    existing = await _external_link(gc_id=gc_id)
    if existing:
        return existing
    peer_id = await _vk_peer_id(data.get("vk_id")) if data.get("vk_id") else ""
    identity = await resolve_client_identity(
        phone=data.get("phone"), email=data.get("email"), getcourse_user_id=gc_id
    )
    peer_id = peer_id or _clean(identity.get("vk_id"), 200)
    if not peer_id:
        return {}
    identity.update({
        "vk_id": peer_id,
        "getcourse_user_id": gc_id,
        "phone": _normalize_phone(data.get("phone")) or identity.get("phone", ""),
        "email": _clean(data.get("email"), 320) or identity.get("email", ""),
        "name": _clean(data.get("name"), 200) or identity.get("name", ""),
    })
    await _remember_external_link(identity, "getcourse-card")
    return await _external_link(peer_id=peer_id)


async def _refresh_vk_links() -> int:
    identities = list_vk_identities()
    for identity in identities.values():
        await _remember_external_link(identity, "sync-control")
    return len(identities)


def _telegram_session_file() -> Path:
    values = _read_env_values()
    explicit = _clean(
        os.environ.get(TELEGRAM_SESSION_ENV_KEY)
        or values.get(TELEGRAM_SESSION_ENV_KEY)
        or os.environ.get(LEGACY_TELEGRAM_SESSION_ENV_KEY)
        or values.get(LEGACY_TELEGRAM_SESSION_ENV_KEY)
        or os.environ.get("TELEGRAM_SESSION_FILE")
        or values.get("TELEGRAM_SESSION_FILE"),
        2000,
    )
    if explicit:
        return Path(explicit).expanduser()
    db_path = _must_db()
    if len(db_path.parents) >= 3:
        shared = db_path.parents[2] / "course-chat-creator" / "data" / "telegram.session"
        if shared.is_file():
            return shared
    return db_path.parent / "telegram-personal.session"


def _telegram_credentials() -> tuple[int, str]:
    values = _read_env_values()
    api_id = _clean(os.environ.get("TELEGRAM_API_ID") or values.get("TELEGRAM_API_ID"), 30)
    api_hash = _clean(os.environ.get("TELEGRAM_API_HASH") or values.get("TELEGRAM_API_HASH"), 200)
    if not api_id or not api_hash:
        raise HTTPException(503, "TELEGRAM_API_ID и TELEGRAM_API_HASH не заданы")
    try:
        return int(api_id), api_hash
    except ValueError as exc:
        raise HTTPException(503, "TELEGRAM_API_ID должен быть числом") from exc


def _telegram_client() -> Any:
    try:
        from telethon import TelegramClient
        from orchestrator.telegram_proxy import telethon_proxy_config
    except Exception as exc:
        raise HTTPException(503, f"Telegram недоступен: {type(exc).__name__}") from exc
    api_id, api_hash = _telegram_credentials()
    connection, proxy = telethon_proxy_config()
    kwargs: dict[str, Any] = {
        "connection_retries": 1,
        "request_retries": 1,
        "timeout": 8,
    }
    if connection and proxy:
        kwargs.update({"connection": connection, "proxy": proxy})
    return TelegramClient(str(_telegram_session_file()), api_id, api_hash, **kwargs)


async def _telegram_run(callback: Any) -> Any:
    async with _telegram_lock:
        client = _telegram_client()
        try:
            await asyncio.wait_for(client.connect(), timeout=40)
            return await callback(client)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(502, f"Telegram: {_clean(exc, 300)}") from exc
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass


def _telegram_user_view(entity: Any) -> dict[str, str]:
    first = _clean(getattr(entity, "first_name", ""), 120)
    last = _clean(getattr(entity, "last_name", ""), 120)
    return {
        "id": _clean(getattr(entity, "id", ""), 200),
        "phone": _normalize_phone(getattr(entity, "phone", "")),
        "username": _clean(getattr(entity, "username", ""), 200).lstrip("@"),
        "name": " ".join(part for part in (first, last) if part).strip(),
    }


def _telegram_is_user(entity: Any) -> bool:
    return entity is not None and hasattr(entity, "first_name") and not hasattr(entity, "title")


async def _telegram_auth_state(*, refresh: bool = False) -> dict[str, Any]:
    global _telegram_state_cache
    expires_at, cached = _telegram_state_cache
    if not refresh and cached and expires_at > time.monotonic():
        return dict(cached)
    try:
        async def read(client: Any) -> dict[str, Any]:
            authorized = bool(await client.is_user_authorized())
            me = await client.get_me() if authorized else None
            return {
                "api": True,
                "authorized": authorized,
                "account": _telegram_user_view(me) if me else {},
                "shared_session": _telegram_session_file().name == "telegram.session",
            }

        state = await _telegram_run(read)
    except HTTPException as exc:
        state = {"api": exc.status_code != 503, "authorized": False, "account": {}, "error": str(exc.detail)}
    _telegram_state_cache = (time.monotonic() + 3600, dict(state))
    return state


async def _telegram_channel(*, refresh: bool = False) -> dict[str, str] | None:
    state = dict(_telegram_state_cache[1]) if not refresh and _telegram_state_cache[1] else {}
    if not state and not refresh:
        db = await _connect()
        try:
            row = await (await db.execute(
                "SELECT channel_id FROM wazzup_chats WHERE channel_id LIKE 'telegram-personal:%' ORDER BY updated_at DESC LIMIT 1"
            )).fetchone()
        finally:
            await db.close()
        account_id = _clean(row["channel_id"], 200).removeprefix("telegram-personal:") if row else ""
        if account_id:
            state = {"authorized": True, "account": {"id": account_id}}
    if not state:
        state = await _telegram_auth_state(refresh=refresh)
    account = state.get("account") if isinstance(state.get("account"), dict) else {}
    account_id = _clean(account.get("id"), 200)
    if not state.get("authorized") or not account_id:
        return None
    name = _clean(account.get("username") or account.get("name"), 200) or account_id
    return {
        "channel_id": f"telegram-personal:{account_id}",
        "transport": "telegram",
        "channel_transport": "personal",
        "provider": TELEGRAM_PROVIDER,
        "name": name,
        "plain_id": account_id,
        "label": f"Telegram Personal · {name}",
    }


async def _telegram_entity(client: Any, identity: dict[str, Any]) -> Any | None:
    peer_id = _clean(identity.get("telegram_id"), 200)
    username = _clean(identity.get("telegram_username"), 200).lstrip("@").casefold()
    phone = _normalize_phone(identity.get("phone"))
    for reference in (int(peer_id) if peer_id.isdigit() else None, username or None):
        if reference is None:
            continue
        try:
            entity = await client.get_entity(reference)
            if _telegram_is_user(entity) and _clean(getattr(entity, "id", ""), 200):
                return entity
        except Exception:
            pass
    if phone:
        try:
            from telethon.tl.functions.contacts import ResolvePhoneRequest

            result = await client(ResolvePhoneRequest(phone=phone.removeprefix("+")))
            for entity in getattr(result, "users", []) or []:
                if _telegram_is_user(entity) and _clean(getattr(entity, "id", ""), 200):
                    return entity
        except Exception as exc:
            _log("warning", "Telegram phone resolve failed: %s", type(exc).__name__)
    async for dialog in client.iter_dialogs(limit=TELEGRAM_DIALOG_LIMIT):
        entity = getattr(dialog, "entity", None)
        if not _telegram_is_user(entity):
            continue
        view = _telegram_user_view(entity)
        if not view["id"]:
            continue
        if (
            (peer_id and view["id"] == peer_id)
            or (username and view["username"].casefold() == username)
            or (phone and view["phone"] == phone)
        ):
            return entity
    return None


async def _telegram_import_phone(client: Any, identity: dict[str, Any]) -> Any | None:
    phone = _normalize_phone(identity.get("phone"))
    if not phone:
        return None
    try:
        from telethon.tl.functions.contacts import ImportContactsRequest
        from telethon.tl.types import InputPhoneContact

        name = _clean(identity.get("name"), 200).strip()
        first, _, last = name.partition(" ")
        result = await client(ImportContactsRequest([InputPhoneContact(
            client_id=secrets.randbits(63), phone=phone,
            first_name=first or "Контакт", last_name=last,
        )]))
    except Exception as exc:
        _log("warning", "Telegram phone import failed: %s", type(exc).__name__)
        return None
    for entity in getattr(result, "users", []) or []:
        if _telegram_is_user(entity) and _clean(getattr(entity, "id", ""), 200):
            return entity
    return None


async def _telegram_store_messages(
    channel: dict[str, str],
    entity: Any,
    rows: list[Any],
    link: dict[str, Any],
) -> int:
    peer = _telegram_user_view(entity)
    peer_id = peer["id"]
    phone = _normalize_phone(link.get("phone") or peer.get("phone"))
    phone_hash = _phone_hash(phone)
    name = _clean(link.get("name") or peer.get("name") or peer.get("username"), 200) or peer_id
    now = _iso()
    records: list[dict[str, str]] = []
    for message in rows:
        message_id = _clean(getattr(message, "id", ""), 200)
        if not message_id:
            continue
        file_info = getattr(message, "file", None)
        filename = _clean(getattr(file_info, "name", ""), 500) if file_info else ""
        content_type = _clean(getattr(file_info, "mime_type", ""), 200) if file_info else ""
        text_value = _clean(getattr(message, "message", ""), 20_000)
        if not text_value and file_info:
            text_value = f"[Вложение: {filename or 'файл'}]"
        date = getattr(message, "date", None)
        sent_at = _iso(date.astimezone(timezone.utc)) if isinstance(date, datetime) else now
        outgoing = bool(getattr(message, "out", False))
        raw = {
            "id": message_id,
            "contentType": content_type,
            "filename": filename,
            "senderId": _clean(getattr(message, "sender_id", ""), 200),
        }
        records.append({
            "external_id": f"{channel['channel_id']}:{peer_id}:{message_id}",
            "direction": "outgoing" if outgoing else "incoming",
            "text": text_value,
            "content_uri": "",
            "author_name": "Telegram" if outgoing else name,
            "sent_at": sent_at,
            "raw_json": json.dumps(raw, ensure_ascii=False, separators=(",", ":")),
        })
    db = await _connect()
    inserted = 0
    try:
        for record in records:
            cursor = await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,?,'delivered',?,?,?,?,?,?) ON CONFLICT(external_id) DO UPDATE SET
                   text=excluded.text,content_uri=excluded.content_uri,author_name=excluded.author_name,
                   sent_at=excluded.sent_at,raw_json=excluded.raw_json""",
                (record["external_id"], channel["channel_id"], "telegram", peer_id, phone_hash,
                 record["direction"], record["text"], record["content_uri"], record["author_name"],
                 record["sent_at"], record["raw_json"], now),
            )
            inserted += max(0, cursor.rowcount)
        latest = max(records, key=lambda row: row["sent_at"]) if records else None
        await db.execute(
            """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
               phone_hash=excluded.phone_hash,contact_name=excluded.contact_name,
               last_message_at=CASE WHEN excluded.last_message_at<>'' THEN excluded.last_message_at ELSE wazzup_chats.last_message_at END,
               last_message_preview=CASE WHEN excluded.last_message_preview<>'' THEN excluded.last_message_preview ELSE wazzup_chats.last_message_preview END,
               updated_at=excluded.updated_at""",
            (channel["channel_id"], "telegram", peer_id, phone_hash, name,
             latest["sent_at"] if latest else "", latest["text"][:500] if latest else "", now, now),
        )
        await db.commit()
    finally:
        await db.close()
    return inserted


async def _telegram_card_link(data: dict[str, Any]) -> dict[str, str]:
    page_kind, gc_id = _page_context(data.get("source_url"))
    if page_kind != "user" or not gc_id:
        return {}
    existing = await _external_link(gc_id=gc_id, provider=TELEGRAM_PROVIDER)
    if existing:
        return existing
    identity = await resolve_client_identity(
        phone=data.get("phone"), email=data.get("email"), getcourse_user_id=gc_id
    )
    identity.update({
        "telegram_id": _clean(data.get("telegram_id") or identity.get("telegram_id"), 200),
        "telegram_username": _clean(data.get("telegram_username") or identity.get("telegram_username"), 200),
        "getcourse_user_id": gc_id,
        "phone": _normalize_phone(data.get("phone")) or identity.get("phone", ""),
        "email": _clean(data.get("email"), 320) or identity.get("email", ""),
        "name": _clean(data.get("name"), 200) or identity.get("name", ""),
    })

    async def resolve(client: Any) -> dict[str, str]:
        if not await client.is_user_authorized():
            return {}
        entity = await _telegram_entity(client, identity)
        if not entity:
            return {}
        peer = _telegram_user_view(entity)
        await _remember_external_link(
            identity,
            "getcourse-card",
            provider=TELEGRAM_PROVIDER,
            external_user_id=peer["id"],
        )
        return await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)

    return await _telegram_run(resolve)


async def _sync_telegram_history(peer_id: str, *, offset: int = 0, identity: dict[str, Any] | None = None) -> tuple[int, bool]:
    key = (_clean(peer_id, 200), max(0, offset))
    expires_at, cached_more = _telegram_history_cache.get(key, (0.0, False))
    if expires_at > time.monotonic():
        return 0, cached_more
    _telegram_history_cache.pop(key, None)
    channel = await _telegram_channel()
    link = await _external_link(peer_id=peer_id, provider=TELEGRAM_PROVIDER) or identity
    if not channel or not link:
        return 0, False

    async def sync(client: Any) -> tuple[int, bool]:
        if not await client.is_user_authorized():
            return 0, False
        try:
            entity = await client.get_entity(int(peer_id))
        except Exception:
            return 0, False
        rows = [row async for row in client.iter_messages(entity, limit=TELEGRAM_HISTORY_PAGE_SIZE, add_offset=max(0, offset))]
        inserted = await _telegram_store_messages(channel, entity, rows, link)
        return inserted, len(rows) >= TELEGRAM_HISTORY_PAGE_SIZE

    result = await _telegram_run(sync)
    _remember_direct_history(_telegram_history_cache, key, result[1], TELEGRAM_HISTORY_CACHE_SECONDS)
    return result


def _schedule_telegram_history(peer_id: str, identity: dict[str, Any]) -> None:
    key = (_clean(peer_id, 200), 0)
    if not key[0] or key in _telegram_history_inflight:
        return
    _telegram_history_inflight.add(key)

    async def run() -> None:
        try:
            await _sync_telegram_history(peer_id, identity=identity)
        except Exception:
            _log("warning", "Telegram history refresh failed peer=%s", peer_id)
        finally:
            _telegram_history_inflight.discard(key)

    asyncio.create_task(run())


async def _telegram_send_text(peer_id: str, text: str, *, identity: dict[str, Any] | None = None) -> dict[str, Any]:
    channel = await _telegram_channel()
    link = await _external_link(peer_id=peer_id, provider=TELEGRAM_PROVIDER) or identity
    if not channel or not link:
        raise HTTPException(404, "Диалог Telegram не найден")

    async def send(client: Any) -> dict[str, Any]:
        if not await client.is_user_authorized():
            raise HTTPException(409, "Telegram не авторизован")
        try:
            entity = await client.get_entity(int(peer_id))
        except Exception as exc:
            raise HTTPException(404, "Диалог Telegram не найден") from exc
        message = await client.send_message(entity, text)
        await _telegram_store_messages(channel, entity, [message], link)
        return {
            "external_id": f"{channel['channel_id']}:{peer_id}:{_clean(getattr(message, 'id', ''), 200)}",
            "direction": "outgoing",
            "status": "delivered",
            "text": text,
            "content_uri": "",
            "author_name": "Telegram",
            "sent_at": _iso(getattr(message, "date", None)),
        }

    return await _telegram_run(send)


async def _sync_telegram_dialogs(*, full: bool = False) -> dict[str, int]:
    channel = await _telegram_channel()
    if not channel:
        return {"dialogs": 0, "messages": 0}

    async def sync(client: Any) -> dict[str, int]:
        if not await client.is_user_authorized():
            return {"dialogs": 0, "messages": 0}
        dialogs = messages = 0
        limit = TELEGRAM_DIALOG_LIMIT if full else TELEGRAM_BACKGROUND_DIALOG_LIMIT
        async for dialog in client.iter_dialogs(limit=limit):
            entity = getattr(dialog, "entity", None)
            if not _telegram_is_user(entity):
                continue
            peer = _telegram_user_view(entity)
            if not peer["id"]:
                continue
            link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)
            if not link and peer["phone"]:
                db = await _connect()
                try:
                    row = await (await db.execute(
                        "SELECT phone,getcourse_user_id,name FROM client_links WHERE phone_hash=? AND getcourse_user_id<>''",
                        (_phone_hash(peer["phone"]),),
                    )).fetchone()
                finally:
                    await db.close()
                if row:
                    identity = {"phone": row["phone"], "getcourse_user_id": row["getcourse_user_id"], "name": row["name"]}
                    await _remember_external_link(identity, "telegram-dialog", provider=TELEGRAM_PROVIDER, external_user_id=peer["id"])
                    link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)
            if not link:
                await _remember_external_link(
                    {"phone": peer["phone"], "name": peer["name"] or peer["username"]},
                    "telegram-dialog", provider=TELEGRAM_PROVIDER, external_user_id=peer["id"],
                )
                link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)
            dialogs += 1
            latest = getattr(dialog, "message", None)
            if latest:
                messages += await _telegram_store_messages(channel, entity, [latest], link)
        await _set_setting("telegram_last_sync_at", _iso())
        return {"dialogs": dialogs, "messages": messages}

    return await _telegram_run(sync)


async def telegram_background_loop() -> None:
    while True:
        try:
            await _sync_telegram_dialogs()
        except Exception:
            _log("warning", "Telegram Personal reconciliation failed")
        await asyncio.sleep(TELEGRAM_SYNC_SECONDS)


async def _vk_channel() -> dict[str, str] | None:
    channel_id = _vk_channel_id()
    if not channel_id or not _vk_token():
        return None
    name = await _setting("vk_group_name") or f"Сообщество {_vk_group_id()}"
    return {
        "channel_id": channel_id,
        "transport": "vk",
        "channel_transport": "vk",
        "provider": "vk",
        "name": name,
        "plain_id": _vk_group_id(),
        "label": f"VK · {name}",
    }


def _salebot_key() -> str:
    for name in ("SALEBOT_API_KEY", "SALEBOT_API_KEY_3", "SALEBOT_API_KEY_2", "SALEBOT_API_KEY_1"):
        if value := _clean(os.environ.get(name), 5000):
            return value
    return ""


def _salebot_channel() -> dict[str, str] | None:
    if not _salebot_key():
        return None
    return {
        "channel_id": "salebot:project", "transport": "salebot", "channel_transport": "salebot",
        "provider": SALEBOT_PROVIDER, "name": "Проект", "plain_id": "project", "label": "SaleBot · Проект",
    }


async def _all_channels(*, refresh: bool = False) -> list[dict[str, str]]:
    channels_result, vk, telegram = await asyncio.gather(
        _cached_active_channels(refresh=refresh),
        _vk_channel(),
        _telegram_channel(refresh=refresh),
        return_exceptions=True,
    )
    try:
        if isinstance(channels_result, BaseException):
            raise channels_result
        channels = channels_result
    except HTTPException:
        channels = []
    direct = [row for row in (vk, telegram, _salebot_channel()) if isinstance(row, dict)]
    return channels + direct


def _phone_hash(value: Any) -> str:
    phone = _normalize_phone(value)
    return _hash(phone) if phone else ""


def _webhook_messages(payload: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        return [], []
    messages = payload.get("messages")
    statuses = payload.get("statuses")
    return (
        [row for row in messages if isinstance(row, dict)] if isinstance(messages, list) else [],
        [row for row in statuses if isinstance(row, dict)] if isinstance(statuses, list) else [],
    )


def _message_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        try:
            stamp = float(value)
            if stamp > 10_000_000_000:
                stamp /= 1000
            return _iso(datetime.fromtimestamp(stamp, timezone.utc))
        except (OverflowError, OSError, ValueError):
            return _iso()
    text = _clean(value, 80)
    return text or _iso()


def _message_contact_phone(row: Any, chat_type: str = "", chat_id: str = "") -> str:
    if not isinstance(row, dict):
        return ""
    contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
    sender = row.get("sender") if isinstance(row.get("sender"), dict) else {}
    recipient = row.get("recipient") if isinstance(row.get("recipient"), dict) else {}
    for value in (
        row.get("authorPhone"),
        row.get("userPhone"),
        contact.get("phone"),
        sender.get("phone"),
        recipient.get("phone"),
    ):
        if phone := _normalize_phone(value):
            return phone
    return _normalize_phone(chat_id) if chat_type in {"whatsapp", "viber"} else ""


def _message_content(row: Any) -> dict[str, str]:
    if not isinstance(row, dict):
        return {"content_uri": "", "content_type": "", "filename": ""}
    attachment = row.get("attachment") if isinstance(row.get("attachment"), dict) else {}
    filename = _clean(row.get("filename") or attachment.get("name"), 500)
    content_type = _clean(row.get("contentType") or attachment.get("mimetype"), 200).lower()
    content_uri = _clean(row.get("contentUri") or attachment.get("url") or row.get("url"), 4000)
    content_sha = _clean(row.get("contentSha") or attachment.get("sha1"), 100).lower()
    if not content_uri and re.fullmatch(r"[0-9a-f]{40}", content_sha):
        content_uri = f"https://store.wazzup24.com/{content_sha}/"
        if filename:
            content_uri += f"?filename={quote(filename)}"
    message_type = _clean(row.get("type"), 50).lower()
    if not content_type and message_type in {"image", "audio", "video", "document"}:
        content_type = message_type
    if not content_type and filename.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")):
        content_type = "image"
    return {"content_uri": content_uri, "content_type": content_type, "filename": filename}


def _message_view(row: Any) -> dict[str, Any]:
    item = dict(row) if not isinstance(row, dict) else dict(row)
    raw = {}
    try:
        raw = json.loads(_clean(item.pop("raw_json", ""), 50_000) or "{}")
    except (json.JSONDecodeError, TypeError):
        raw = {}
    content = _message_content(raw)
    if not content["content_uri"]:
        content["content_uri"] = _clean(item.get("content_uri"), 4000)
    item.update(content)
    attachments = raw.get("nexus_attachments") if isinstance(raw, dict) else []
    item["attachments"] = [
        {
            "content_uri": _clean(attachment.get("content_uri"), 4000),
            "content_type": _clean(attachment.get("content_type"), 200),
            "filename": _clean(attachment.get("filename"), 500),
        }
        for attachment in attachments if isinstance(attachment, dict) and _clean(attachment.get("content_uri"), 4000)
    ] if isinstance(attachments, list) else []
    return item


def _conversation_message_query(where: str) -> str:
    return f"""SELECT external_id,direction,status,text,content_uri,author_name,sent_at,raw_json FROM (
        SELECT id,external_id,direction,status,text,content_uri,author_name,sent_at,raw_json,
               ROW_NUMBER() OVER (PARTITION BY CASE
                   WHEN chat_type='vk' AND json_valid(raw_json) AND json_extract(raw_json,'$.id') IS NOT NULL
                   THEN 'vk:' || json_extract(raw_json,'$.id') ELSE external_id END
                   ORDER BY id DESC) AS duplicate_rank
        FROM wazzup_messages WHERE {where}
    ) WHERE duplicate_rank=1 ORDER BY sent_at DESC,id DESC LIMIT ? OFFSET ?"""


async def _remember_client_link(
    phone: str,
    *,
    getcourse_user_id: str = "",
    name: str = "",
    source: str = "",
) -> None:
    normalized = _normalize_phone(phone)
    if not normalized:
        return
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO client_links(phone_hash,phone,getcourse_user_id,name,source,updated_at)
               VALUES(?,?,?,?,?,?) ON CONFLICT(phone_hash) DO UPDATE SET
               phone=excluded.phone,
               getcourse_user_id=CASE WHEN excluded.getcourse_user_id<>'' THEN excluded.getcourse_user_id ELSE client_links.getcourse_user_id END,
               name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE client_links.name END,
               source=CASE WHEN excluded.source<>'' THEN excluded.source ELSE client_links.source END,
               updated_at=excluded.updated_at""",
            (
                _phone_hash(normalized),
                normalized,
                _clean(getcourse_user_id, 200),
                _clean(name, 200),
                _clean(source, 80),
                _iso(),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _ingest_webhook(payload: dict[str, Any]) -> dict[str, int]:
    messages, statuses = _webhook_messages(payload)
    now = _iso()
    db = await _connect()
    inserted = updated = 0
    link_candidates: dict[str, str] = {}
    try:
        await db.execute(
            "INSERT INTO webhook_events(received_at,payload_json) VALUES(?,?)",
            (now, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))[:MAX_WEBHOOK_BYTES]),
        )
        for row in messages:
            external_id = _clean(row.get("messageId") or row.get("id"), 250)
            channel_id = _clean(row.get("channelId"), 200)
            chat_type = _clean(row.get("chatType"), 40).lower()
            chat_id = _clean(row.get("chatId") or row.get("messengerChatId"), 250)
            if not external_id or not channel_id or chat_type not in CHAT_TRANSPORTS or not chat_id:
                continue
            incoming = row.get("incoming")
            if incoming is None:
                incoming = not bool(row.get("isEcho"))
            direction = "incoming" if bool(incoming) else "outgoing"
            sent_at = _message_time(row.get("dateTime") or row.get("datetime") or row.get("createdAt"))
            text_value = row.get("text")
            if not isinstance(text_value, str) and isinstance(row.get("message"), dict):
                text_value = row["message"].get("text")
            message_text = _clean(text_value, 20_000)
            content = _message_content(row)
            content_uri = content["content_uri"]
            if not message_text and content["filename"]:
                message_text = f"[Вложение: {content['filename']}]"
            contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
            author_name = _clean(row.get("authorName") or row.get("contactName") or contact.get("name"), 200)
            contact_phone = _message_contact_phone(row, chat_type, chat_id)
            phone_hash = _phone_hash(contact_phone)
            if contact_phone and direction == "incoming":
                link_candidates.setdefault(contact_phone, author_name)
            raw = json.dumps(row, ensure_ascii=False, separators=(",", ":"))[:50_000]
            cursor = await db.execute(
                """INSERT OR IGNORE INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (external_id, channel_id, chat_type, chat_id, phone_hash, direction, _clean(row.get("status"), 80), message_text, content_uri, author_name, sent_at, raw, now),
            )
            inserted += max(0, cursor.rowcount)
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
                   phone_hash=CASE WHEN excluded.phone_hash<>'' THEN excluded.phone_hash ELSE wazzup_chats.phone_hash END,
                   contact_name=CASE WHEN excluded.contact_name<>'' THEN excluded.contact_name ELSE wazzup_chats.contact_name END,
                   last_message_at=excluded.last_message_at,last_message_preview=excluded.last_message_preview,updated_at=excluded.updated_at""",
                (channel_id, chat_type, chat_id, phone_hash, author_name, sent_at, message_text[:500], now, now),
            )
            if phone_hash:
                await db.execute(
                    """UPDATE wazzup_chats SET responsible_admin_id=COALESCE(
                           responsible_admin_id,(SELECT responsible_admin_id FROM client_links WHERE phone_hash=?))
                       WHERE channel_id=? AND chat_type=? AND chat_id=?""",
                    (phone_hash, channel_id, chat_type, chat_id),
                )
        for row in statuses:
            external_id = _clean(row.get("messageId") or row.get("id"), 250)
            status = _clean(row.get("status"), 80)
            if external_id and status:
                cursor = await db.execute("UPDATE wazzup_messages SET status=? WHERE external_id=?", (status, external_id))
                updated += max(0, cursor.rowcount)
        await db.commit()
    finally:
        await db.close()
    for phone, name in link_candidates.items():
        identity = resolve_sync_identity(phone=phone)
        await _remember_client_link(
            phone,
            getcourse_user_id=_clean(identity.get("getcourse_user_id"), 200),
            name=name,
            source="webhook",
        )
    return {"messages": inserted, "statuses": updated}


async def _device(request: Request) -> dict[str, Any] | None:
    auth = _clean(request.headers.get("authorization"), 5000)
    if not auth.lower().startswith("bearer "):
        return None
    token = auth[7:].strip()
    if len(token) < 32:
        return None
    now = _iso()
    expires = _iso(_now_dt() + timedelta(days=DEVICE_TTL_DAYS))
    db = await _connect()
    try:
        row = await (
            await db.execute(
                """SELECT d.*,a.wazzup_user_id,a.name AS admin_name,a.role AS admin_role,a.enabled
                   FROM devices d JOIN admins a ON a.id=d.admin_id WHERE d.token_hash=?""",
                (_hash(token),),
            )
        ).fetchone()
        if not row or row["revoked_at"] or row["expires_at"] <= now or not row["enabled"]:
            return None
        await db.execute(
            "UPDATE devices SET last_used_at=?,expires_at=? WHERE id=?",
            (now, expires, row["id"]),
        )
        await db.commit()
        return dict(row)
    finally:
        await db.close()


def _device_platform(device: dict[str, Any]) -> str:
    return _clean(device.get("platform"), 40).lower() or "getcourse"


def _validate_device_context(device: dict[str, Any], data: dict[str, Any], mode: str) -> None:
    platform = "getcourse" if mode == "test" else mode
    if _device_platform(device) != platform:
        raise HTTPException(401, "Требуется активация в этой системе")
    if platform != "amocrm":
        return
    expected_id = _clean(device.get("platform_user_id"), 200)
    expected_email = _clean(device.get("platform_user_email"), 320).casefold()
    actual_id = _clean(data.get("platform_user_id"), 200)
    actual_email = _clean(data.get("platform_user_email"), 320).casefold()
    if expected_id and actual_id != expected_id:
        raise HTTPException(401, "Сменился пользователь amoCRM")
    if expected_email and actual_email != expected_email:
        raise HTTPException(401, "Сменился пользователь amoCRM")


def _widget_context(data: dict[str, Any], mode: str, device: dict[str, Any]) -> dict[str, Any]:
    fields = data.get("fields") if isinstance(data.get("fields"), dict) else {}
    platform = "getcourse" if mode == "test" else mode
    entity_type = _clean(data.get("entity_type"), 40).lower()
    entity_id = _clean(data.get("entity_id"), 200)
    if platform == "getcourse":
        page_kind, page_id = _page_context(data.get("source_url"))
        entity_type = entity_type or page_kind
        entity_id = entity_id or page_id
    return {
        "platform": platform,
        "service": "amo" if platform == "amocrm" else "getcourse_order" if entity_type == "order" else platform,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "name": _clean(data.get("name"), 500),
        "first_name": _clean(data.get("first_name"), 200),
        "last_name": _clean(data.get("last_name"), 200),
        "phone": _normalize_phone(data.get("phone")),
        "email": _clean(data.get("email"), 320),
        "manager_name": _clean(device.get("admin_name"), 200),
        "fields": fields,
    }


def _field_value(fields: dict[str, Any], path: str) -> Any:
    current: Any = fields
    for part in path.split("."):
        if not isinstance(current, dict):
            return ""
        if part in current:
            current = current[part]
            continue
        matched = next((key for key in current if str(key).casefold() == part.casefold()), None)
        if matched is None:
            return ""
        current = current[matched]
    return current


async def _responsible_admin_id(data: dict[str, Any], mode: str, device: dict[str, Any]) -> int | None:
    context = _widget_context(data, mode, device)
    fields = context.get("fields") if isinstance(context.get("fields"), dict) else {}

    def first(*keys: str) -> str:
        for key in keys:
            value = data.get(key) or fields.get(key) or _field_value(fields, key)
            if value:
                return _clean(value, 320)
        return ""

    platform = context["platform"]
    user_id = first(
        "responsible_user_id", "manager_user_id", "lead.responsible_user_id",
        "contact.responsible_user_id", "order.responsible_user_id",
    )
    email = first(
        "responsible_user_email", "manager_user_email", "lead.responsible_user_email",
        "contact.responsible_user_email",
    ).casefold()
    db = await _connect()
    try:
        row = None
        if user_id:
            row = await (await db.execute(
                "SELECT admin_id FROM manager_bindings WHERE platform=? AND platform_user_id=?",
                (platform, user_id),
            )).fetchone()
        if not row and email:
            row = await (await db.execute(
                "SELECT admin_id FROM manager_bindings WHERE platform=? AND platform_user_email=? ORDER BY updated_at DESC LIMIT 1",
                (platform, email),
            )).fetchone()
        if row:
            return int(row["admin_id"])
    finally:
        await db.close()
    return int(device["admin_id"]) if _clean(device.get("admin_role"), 20) == "employee" else None


async def _assign_client_threads(
    admin_id: int | None,
    *,
    phone: str = "",
    direct_links: list[tuple[str, str]] | None = None,
) -> None:
    if not admin_id:
        return
    phone_hash = _phone_hash(phone)
    direct_links = direct_links or []
    if not phone_hash and not direct_links:
        return
    db = await _connect()
    try:
        if phone_hash:
            await db.execute(
                "UPDATE client_links SET responsible_admin_id=? WHERE phone_hash=?",
                (admin_id, phone_hash),
            )
            await db.execute(
                "UPDATE wazzup_chats SET responsible_admin_id=? WHERE phone_hash=?",
                (admin_id, phone_hash),
            )
        for provider, peer_id in direct_links:
            channel_prefix = "vk:" if provider == "vk" else "telegram-personal:"
            await db.execute(
                "UPDATE wazzup_chats SET responsible_admin_id=? WHERE channel_id LIKE ? AND chat_id=?",
                (admin_id, channel_prefix + "%", _clean(peer_id, 250)),
            )
        await db.commit()
    finally:
        await db.close()


async def _apply_identity_rules(context: dict[str, Any]) -> None:
    source = context["platform"]
    entity_type = context["entity_type"]
    db = await _connect()
    try:
        rows = await (await db.execute(
            "SELECT field_key,identity_type FROM identity_rules WHERE source=? AND entity_type=? AND enabled=1 ORDER BY priority,id",
            (source, entity_type),
        )).fetchall()
    finally:
        await db.close()
    fields = context.get("fields") if isinstance(context.get("fields"), dict) else {}
    aliases = {
        "vk_platform": "platform_id", "getcourse_user": "getcourse_user_id",
        "telegram": "telegram_id", "vk": "vk_id", "salebot": "salebot_id",
    }
    for row in rows:
        value = context.get(row["field_key"]) or _field_value(fields, row["field_key"])
        if not value:
            continue
        target = aliases.get(row["identity_type"], row["identity_type"])
        context.setdefault(target, value)
        fields.setdefault(target, value)


async def _resolve_widget_context(data: dict[str, Any], mode: str, device: dict[str, Any]) -> dict[str, Any]:
    context = _widget_context(data, mode, device)
    await _apply_identity_rules(context)
    if _identity_index is None:
        return {"status": "unavailable", "accounts": [], "variables": build_context_variables([], context), "conflicts": []}
    return await asyncio.to_thread(_identity_index.resolve, context)


async def service_transfer_recipients(*, email: str = "", gc_user_id: str = "", name: str = "") -> dict[str, Any]:
    if _identity_index is None:
        return {"ok": False, "status": "unavailable", "telegram": "", "vk": "", "conflicts": []}
    context = {
        "service": "getcourse",
        "entity_type": "user",
        "entity_id": _clean(gc_user_id, 200),
        "getcourse_user_id": _clean(gc_user_id, 200),
        "email": _clean(email, 320),
        "name": _clean(name, 200),
        "fields": {"email": _clean(email, 320), "gc_user_id": _clean(gc_user_id, 200)},
    }
    telegram, vk = await asyncio.gather(*(
        asyncio.to_thread(_identity_index.provider_id_for_exact_context, service, context)
        for service in ("telegram", "vk")
    ))
    if not telegram:
        telegram = await asyncio.to_thread(_identity_index.platform_id_for_context, "telegram", context)
    if not vk:
        vk = await asyncio.to_thread(_identity_index.platform_id_for_context, "vk", context)
    return {
        "ok": bool(telegram or vk),
        "status": "resolved" if telegram or vk else "not_found",
        "telegram": telegram,
        "vk": vk,
        "conflicts": [],
    }


async def service_transfer_delivery_target(
    *, email: str = "", gc_user_id: str = "", phone: str = "", utm_term: str = "",
) -> dict[str, Any]:
    """Resolve only the explicit GetCourse order recipient used by the chat-link action."""

    term = _clean(utm_term, 1000)
    parsed = parse_utm_term(term)
    if not parsed or _identity_index is None:
        return {"ok": False, "status": "not_found", "provider": "", "recipient_id": "", "reason": "utm_term не содержит ID доставки"}
    context = {
        "service": "getcourse_order",
        "entity_type": "order",
        "entity_id": "",
        "getcourse_user_id": _clean(gc_user_id, 200),
        "email": _clean(email, 320),
        "phone": _normalize_phone(phone),
        "utm_term": term,
        "fields": {"email": _clean(email, 320), "phone": _normalize_phone(phone), "utm_term": term},
    }
    kind, candidate = parsed[0]
    targets: list[tuple[str, str]] = []
    if kind == "vk_platform":
        verified = await asyncio.to_thread(_identity_index.provider_id_for_exact_context, "vk", context)
        targets = [("vk", candidate)] if verified == candidate else []
    elif kind == "salebot":
        verified = await asyncio.to_thread(_identity_index.provider_id_for_exact_context, SALEBOT_PROVIDER, context)
        targets = [(SALEBOT_PROVIDER, candidate)] if verified == candidate else []
    elif kind == "candidate":
        vk, salebot = await asyncio.gather(*(
            asyncio.to_thread(_identity_index.provider_id_for_exact_context, provider, context)
            for provider in ("vk", SALEBOT_PROVIDER)
        ))
        targets = [(provider, value) for provider, value in (("vk", vk), (SALEBOT_PROVIDER, salebot)) if value == candidate]
    if len(targets) != 1:
        reason = "utm_term относится к нескольким каналам" if len(targets) > 1 else "ID из utm_term не найден в базе"
        return {"ok": False, "status": "conflict" if len(targets) > 1 else "not_found", "provider": "", "recipient_id": "", "reason": reason}
    provider, recipient_id = targets[0]
    if provider == SALEBOT_PROVIDER:
        ready = bool(_salebot_key())
        return {
            "ok": ready, "status": "ready" if ready else "unavailable", "provider": provider,
            "recipient_id": recipient_id, "reason": "" if ready else "SaleBot не настроен",
        }
    try:
        allowed = await _vk_request("messages.isMessagesFromGroupAllowed", {"user_id": recipient_id, "group_id": _vk_group_id()})
        ready = bool(int((allowed or {}).get("is_allowed") or 0)) if isinstance(allowed, dict) else False
    except Exception:
        ready = False
    return {
        "ok": ready, "status": "ready" if ready else "unavailable", "provider": provider,
        "recipient_id": recipient_id, "reason": "" if ready else "Пользователь не разрешил сообщения сообщества",
    }


async def service_send_transfer_message(
    *, provider: str, recipient_id: str, content: str, operation_id: str,
) -> dict[str, Any]:
    provider = _clean(provider, 40).lower()
    recipient_id = _clean(recipient_id, 200)
    content = _clean(content, 4000)
    if provider not in {"vk", SALEBOT_PROVIDER} or not recipient_id or not content or not _clean(operation_id, 100):
        return {"ok": False, "status": "invalid", "error": "Некорректные параметры доставки"}
    try:
        if provider == SALEBOT_PROVIDER:
            details = await _salebot_send(recipient_id, content)
            return {"ok": True, "status": "sent", "provider": provider, "recipient_id": recipient_id, "details": details}
        result = await _vk_request("messages.send", {
            "group_id": _vk_group_id(), "peer_id": recipient_id,
            "random_id": secrets.randbelow(2_000_000_000) + 1, "message": content,
        })
        return {"ok": True, "status": "sent", "provider": provider, "recipient_id": recipient_id, "message_id": result}
    except Exception as exc:
        return {"ok": False, "status": "failed", "provider": provider, "recipient_id": recipient_id, "error": _clean(exc, 1000)}


def _account_identity_value(accounts: list[dict[str, Any]], provider: str) -> str:
    services = {"vk"} if provider == "vk" else {"telegram", "telegram_personal"}
    for account in accounts:
        if not isinstance(account, dict) or _clean(account.get("service"), 80).lower() not in services:
            continue
        value = _clean(account.get("platform_id"), 200)
        if value:
            return value
    keys = ("vk_id", "vkontakte_id", "senler_id") if provider == "vk" else ("telegram_id", "tg_id")
    for account in accounts:
        if not isinstance(account, dict):
            continue
        fields = account.get("fields") if isinstance(account.get("fields"), dict) else {}
        for key in keys:
            value = _clean(_field_value(fields, key), 200)
            if value:
                return value
    return ""


def _identity_field_value(source: dict[str, Any], *keys: str) -> str:
    fields = source.get("fields") if isinstance(source.get("fields"), dict) else {}
    for key in keys:
        value = source.get(key) or _field_value(fields, key)
        if value:
            return _clean(value, 300)
    return ""


def _card_link_matches_context(link: dict[str, Any], context: dict[str, Any], gc_id: str) -> bool:
    phone = _normalize_phone(context.get("phone"))
    email = _clean(context.get("email"), 320).casefold()
    if phone and _normalize_phone(link.get("phone")) != phone:
        return False
    if gc_id and _clean(link.get("getcourse_user_id"), 200) != gc_id:
        return False
    if email and _clean(link.get("email"), 320).casefold() not in {"", email}:
        return False
    return bool(phone or gc_id or email)


async def _provider_card_link(
    data: dict[str, Any],
    mode: str,
    device: dict[str, Any],
    provider: str,
    *,
    allow_phone_import: bool = False,
) -> dict[str, str]:
    context = _widget_context(data, mode, device)
    await _apply_identity_rules(context)
    existing = await _entity_external_link(
        context["platform"], context["entity_type"], context["entity_id"], provider,
    )
    page_kind, page_id = _page_context(data.get("source_url"))
    gc_id = page_id if page_kind == "user" else _identity_field_value(context, "getcourse_user_id")
    if existing and _card_link_matches_context(existing, context, gc_id):
        return existing

    identity = {
        "getcourse_user_id": gc_id,
        "phone": _normalize_phone(context.get("phone")),
        "email": _clean(context.get("email"), 320),
        "name": _clean(context.get("name"), 200),
    }

    if provider == SALEBOT_PROVIDER:
        reference = await asyncio.to_thread(
            _identity_index.provider_id_for_exact_context, "salebot", context,
        ) if _identity_index is not None else ""
        reference = reference or _identity_field_value(context, "salebot_id", "salebot_client_id", "sb_id")
        if not reference:
            reference = next((value for kind, value in parse_utm_term(_identity_field_value(context, "utm_term")) if kind == "salebot"), "")
        if not reference:
            return {}
        if mode != "test":
            await _remember_external_link(identity, context["platform"], provider=provider, external_user_id=reference)
        return {
            **identity, "provider": provider, "external_user_id": reference,
            "source": context["platform"], "updated_at": _iso(),
        }

    if provider == "vk":
        reference = await asyncio.to_thread(
            _identity_index.provider_id_for_exact_context, "vk", context,
        ) if _identity_index is not None else ""
        reference = reference or _identity_field_value(context, "vk_id", "vkontakte_id", "senler_id")
        if not reference and _identity_index is not None:
            lookup = {
                **context,
                "getcourse_user_id": gc_id,
                "fields": {**(context.get("fields") or {}), "getcourse_user_id": gc_id},
            }
            reference = await asyncio.to_thread(_identity_index.platform_id_for_context, "vk", lookup)
        if not reference:
            resolved = await _resolve_widget_context(data, mode, device)
            reference = _account_identity_value(resolved.get("accounts", []), "vk")
        if not reference and _identity_index is not None:
            for _, candidate in parse_utm_term(_identity_field_value(context, "utm_term")):
                reference = await asyncio.to_thread(_identity_index.platform_id_for_service, "vk", candidate)
                if reference:
                    break
        cache_key = _card_link_cache_key(context, device, provider, reference)
        cached = _card_link_cache.get(cache_key)
        if cached and cached[0] > time.monotonic():
            return dict(cached[1])
        peer_id = await _vk_peer_id(reference) if reference else ""
        if not peer_id:
            return {}
    else:
        identity["telegram_id"] = (
            _identity_field_value(context, "telegram_id", "tg_id")
        )
        if not identity["telegram_id"] and _identity_index is not None:
            identity["telegram_id"] = await asyncio.to_thread(
                _identity_index.provider_id_for_exact_context, "telegram", context,
            )
        identity["telegram_id"] = identity["telegram_id"] or _identity_field_value(context, "platform_id")
        identity["telegram_username"] = (
            _identity_field_value(context, "telegram_username", "tg_username")
            or _clean(identity.get("telegram_username"), 200)
        )
        if not identity["telegram_id"] and not identity["telegram_username"]:
            resolved = await _resolve_widget_context(data, mode, device)
            identity["telegram_id"] = _account_identity_value(resolved.get("accounts", []), provider)
        cache_key = _card_link_cache_key(
            context, device, provider, identity["telegram_id"] or identity["telegram_username"],
        )
        cached = _card_link_cache.get(cache_key)
        if cached and cached[0] > time.monotonic():
            return dict(cached[1])

        async def resolve_telegram(client: Any) -> str:
            if not await client.is_user_authorized():
                return ""
            entity = await _telegram_entity(client, identity)
            if not entity and allow_phone_import:
                entity = await _telegram_import_phone(client, identity)
            return _telegram_user_view(entity)["id"] if entity else ""

        try:
            peer_id = await asyncio.wait_for(_telegram_run(resolve_telegram), timeout=6)
        except TimeoutError:
            return {"pending": "1"}
        if not peer_id:
            return {}

    if mode != "test":
        await _remember_external_link(
            identity, f"{context['platform']}-card", provider=provider, external_user_id=peer_id,
        )
        owner_id = await _responsible_admin_id(data, mode, device)
        await _remember_entity_external_link(context, provider, peer_id, owner_id)
        await _assign_client_threads(owner_id, phone=identity.get("phone", ""), direct_links=[(provider, peer_id)])
        link = await _external_link(peer_id=peer_id, provider=provider)
        _remember_card_link(cache_key, link)
        return link
    # Test cards keep an exact, request-scoped identity: no phone/name lookup and no DB link.
    link = {**identity, "external_user_id": peer_id}
    _remember_card_link(cache_key, link)
    return link


def _template_view(row: Any, current_admin_id: int | None = None, can_edit_shared: bool = False) -> dict[str, Any]:
    owner_id = row["owner_admin_id"]
    return {
        "id": int(row["id"]),
        "folder": _clean(row["folder"], 120),
        "title": row["title"],
        "body": row["body"],
        "scope": "personal" if owner_id is not None else "shared",
        "editable": (owner_id is None and can_edit_shared) or (owner_id is not None and int(owner_id) == int(current_admin_id or 0)),
        "enabled": bool(row["enabled"]),
        "sort_order": int(row["sort_order"]),
        "updated_at": row["updated_at"],
    }


async def _template_rows(
    admin_id: int | None = None,
    *,
    include_disabled: bool = False,
    can_edit_shared: bool = False,
) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if admin_id is not None:
        where.append("(owner_admin_id IS NULL OR owner_admin_id=?)")
        params.append(admin_id)
    if not include_disabled:
        where.append("enabled=1")
    sql = "SELECT * FROM message_templates"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY owner_admin_id IS NOT NULL,folder,sort_order,title,id"
    db = await _connect()
    try:
        rows = await (await db.execute(sql, params)).fetchall()
        return [_template_view(row, admin_id, can_edit_shared) for row in rows]
    finally:
        await db.close()


async def _save_template(data: dict[str, Any], owner_admin_id: int | None, template_id: int | None = None) -> dict[str, Any]:
    folder = _clean(data.get("folder"), 120)
    title = _clean(data.get("title"), 120)
    body = _clean(data.get("body"), 20_000)
    if not title or not body:
        raise HTTPException(400, "Укажите название и текст")
    now = _iso()
    db = await _connect()
    try:
        if template_id is None:
            cursor = await db.execute(
                "INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (owner_admin_id, folder, title, body, 1, int(data.get("sort_order") or 0), now, now),
            )
            template_id = int(cursor.lastrowid)
        else:
            row = await (await db.execute("SELECT owner_admin_id FROM message_templates WHERE id=?", (template_id,))).fetchone()
            if not row or row["owner_admin_id"] != owner_admin_id:
                raise HTTPException(404, "Шаблон не найден")
            await db.execute(
                "UPDATE message_templates SET folder=?,title=?,body=?,enabled=?,sort_order=?,updated_at=? WHERE id=?",
                (folder, title, body, int(data.get("enabled", True)), int(data.get("sort_order") or 0), now, template_id),
            )
        await db.commit()
        row = await (await db.execute("SELECT * FROM message_templates WHERE id=?", (template_id,))).fetchone()
        return _template_view(row, owner_admin_id, owner_admin_id is None)
    finally:
        await db.close()


async def _delete_template(template_id: int, owner_admin_id: int | None) -> None:
    db = await _connect()
    try:
        cursor = await db.execute(
            "DELETE FROM message_templates WHERE id=? AND owner_admin_id IS ?",
            (template_id, owner_admin_id),
        )
        if not cursor.rowcount:
            raise HTTPException(404, "Шаблон не найден")
        await db.commit()
    finally:
        await db.close()


async def _import_amo_templates(items: Any, folder: Any = "amoCRM") -> dict[str, int]:
    if not isinstance(items, list) or len(items) > MAX_AMO_TEMPLATE_IMPORT:
        raise HTTPException(400, f"Передайте до {MAX_AMO_TEMPLATE_IMPORT} шаблонов")
    folder_name = _clean(folder, 120) or "amoCRM"
    prepared: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    skipped = invalid = with_buttons = with_attachments = 0
    for item in items:
        if not isinstance(item, dict):
            invalid += 1
            continue
        title = _clean(item.get("name") or item.get("title"), 120)
        raw_body = str(item.get("content") or item.get("body") or "").strip()
        if not title or not raw_body or len(raw_body) > 20_000:
            invalid += 1
            continue
        body = _clean(raw_body, 20_000)
        key = (title, body)
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        prepared.append(key)
        with_buttons += int(bool(item.get("buttons")))
        with_attachments += int(bool(item.get("attachment")))
    now = _iso()
    imported = 0
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        existing = {
            (row[0], row[1])
            for row in await (
                await db.execute("SELECT title,body FROM message_templates WHERE owner_admin_id IS NULL")
            ).fetchall()
        }
        for title, body in prepared:
            if (title, body) in existing:
                skipped += 1
                continue
            await db.execute(
                "INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at) VALUES(NULL,?,?,?,?,?,?,?)",
                (folder_name, title, body, 1, 0, now, now),
            )
            imported += 1
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()
    return {
        "received": len(items), "imported": imported, "skipped": skipped, "invalid": invalid,
        "with_buttons": with_buttons, "with_attachments": with_attachments,
    }


async def _inbox_initialized_at(device_id: int) -> str:
    now = _iso()
    db = await _connect()
    try:
        row = await (
            await db.execute("SELECT initialized_at FROM inbox_devices WHERE device_id=?", (device_id,))
        ).fetchone()
        if row:
            return _clean(row["initialized_at"], 80)
        await db.execute(
            "INSERT INTO inbox_devices(device_id,initialized_at) VALUES(?,?)",
            (device_id, now),
        )
        await db.commit()
        return now
    finally:
        await db.close()


async def _mark_thread_read(device_id: int, channel_id: str, chat_type: str, chat_id: str) -> None:
    if not channel_id or not chat_type or not chat_id:
        return
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO inbox_reads(device_id,channel_id,chat_type,chat_id,last_read_at)
               VALUES(?,?,?,?,?) ON CONFLICT(device_id,channel_id,chat_type,chat_id)
               DO UPDATE SET last_read_at=excluded.last_read_at""",
            (device_id, channel_id, chat_type, chat_id, _iso()),
        )
        await db.commit()
    finally:
        await db.close()


def _inbox_preview(message: dict[str, Any]) -> str:
    content_type = _clean(message.get("content_type"), 200).lower()
    if content_type.startswith("image") or content_type == "image":
        return "Изображение"
    if _clean(message.get("content_uri"), 4000):
        return "Вложение"
    text = _clean(message.get("text"), 500)
    if text.startswith("[Вложение:"):
        return "Вложение"
    return text or "Сообщение"


def _sort_inbox_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: (item["unread"] > 0, item["sent_at"]), reverse=True)


async def _inbox_items(
    device: dict[str, Any],
    channels: list[dict[str, str]],
    *,
    query: str = "",
    selected_channel_ids: list[str] | None = None,
) -> dict[str, Any]:
    device_id = int(device["id"])
    initialized_at = await _inbox_initialized_at(device_id)
    channel_map = {row["channel_id"]: row for row in channels}
    selected = {_clean(value, 200) for value in (selected_channel_ids or []) if _clean(value, 200)}
    channel_ids = [channel_id for channel_id in channel_map if not selected or channel_id in selected]
    if not channel_ids:
        return {"items": [], "unread": 0, "unanswered": 0}
    placeholders = ",".join("?" for _ in channel_ids)
    where = [f"c.channel_id IN ({placeholders})"]
    params: list[Any] = list(channel_ids)
    if _clean(device.get("admin_role"), 20) == "employee":
        where.append("c.responsible_admin_id=?")
        params.append(int(device["admin_id"]))
    query = _clean(query, 200)
    if query:
        like = f"%{query}%"
        phone_query = re.sub(r"\D+", "", query)
        where.append("(c.contact_name LIKE ? OR c.chat_id LIKE ? OR l.name LIKE ? OR l.phone LIKE ? OR l.getcourse_user_id LIKE ?)")
        params.extend((like, like, like, f"%{phone_query or query}%", like))
    where_sql = " AND ".join(where)
    unread_owner = " AND c.responsible_admin_id=?" if _clean(device.get("admin_role"), 20) == "employee" else ""
    unread_params: list[Any] = [device_id, *channel_ids]
    if unread_owner:
        unread_params.append(int(device["admin_id"]))
    unread_params.append(initialized_at)
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                f"""SELECT c.channel_id,c.chat_type,c.chat_id,c.phone_hash,c.contact_name,c.last_message_at,
                           l.phone,l.getcourse_user_id,l.name AS link_name,l.source AS link_source,
                           m.external_id,m.direction,m.status,m.text,m.content_uri,m.author_name,m.sent_at,m.raw_json,
                           (SELECT mi.raw_json FROM wazzup_messages mi
                            WHERE mi.channel_id=c.channel_id AND mi.chat_type=c.chat_type AND mi.chat_id=c.chat_id
                              AND mi.direction='incoming' ORDER BY mi.sent_at DESC,mi.id DESC LIMIT 1) AS incoming_raw_json,
                           (SELECT mi.author_name FROM wazzup_messages mi
                            WHERE mi.channel_id=c.channel_id AND mi.chat_type=c.chat_type AND mi.chat_id=c.chat_id
                              AND mi.direction='incoming' ORDER BY mi.sent_at DESC,mi.id DESC LIMIT 1) AS incoming_author
                    FROM wazzup_chats c
                    JOIN wazzup_messages m ON m.id=(
                        SELECT lm.id FROM wazzup_messages lm
                        WHERE lm.channel_id=c.channel_id AND lm.chat_type=c.chat_type AND lm.chat_id=c.chat_id
                        ORDER BY lm.sent_at DESC,lm.id DESC LIMIT 1
                    )
                    LEFT JOIN client_links l ON l.phone_hash=c.phone_hash
                    WHERE {where_sql}
                    ORDER BY c.last_message_at DESC,c.id DESC LIMIT ?""",
                (*params, INBOX_LIMIT),
            )
        ).fetchall()
        unread_rows = await (
            await db.execute(
                f"""SELECT m.channel_id,m.chat_type,m.chat_id,COUNT(*) AS unread
                    FROM wazzup_messages m
                    JOIN wazzup_chats c ON c.channel_id=m.channel_id AND c.chat_type=m.chat_type AND c.chat_id=m.chat_id
                    LEFT JOIN inbox_reads r ON r.device_id=? AND r.channel_id=m.channel_id
                         AND r.chat_type=m.chat_type AND r.chat_id=m.chat_id
                    WHERE m.direction='incoming' AND m.channel_id IN ({placeholders})
                      {unread_owner}
                      AND m.sent_at>COALESCE(r.last_read_at,?)
                    GROUP BY m.channel_id,m.chat_type,m.chat_id""",
                unread_params,
            )
        ).fetchall()
    finally:
        await db.close()
    unread_map = {
        (row["channel_id"], row["chat_type"], row["chat_id"]): int(row["unread"])
        for row in unread_rows
    }
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        message = _message_view(item)
        channel = channel_map[item["channel_id"]]
        external_link: dict[str, Any] = {}
        if channel.get("provider") in {"vk", TELEGRAM_PROVIDER}:
            external_link = await _external_link(
                peer_id=_clean(item.get("chat_id"), 200),
                provider=channel.get("provider", "vk"),
            )
            if not external_link:
                continue
        phone = _normalize_phone(item.get("phone"))
        if not phone:
            try:
                incoming_raw = json.loads(_clean(item.get("incoming_raw_json"), 50_000) or "{}")
            except json.JSONDecodeError:
                incoming_raw = {}
            phone = _message_contact_phone(incoming_raw, item["chat_type"], item["chat_id"])
        gc_user_id = _clean(external_link.get("getcourse_user_id") or item.get("getcourse_user_id"), 200)
        if phone and not gc_user_id and _clean(item.get("link_source"), 80) != "inbox-resolved":
            identity = resolve_sync_identity(phone=phone)
            gc_user_id = _clean(identity.get("getcourse_user_id"), 200)
        name = _clean(
            external_link.get("name") or item.get("link_name") or item.get("contact_name") or item.get("incoming_author") or item.get("author_name"),
            200,
        ) or phone or "Клиент"
        should_remember = bool(phone) and (
            not _normalize_phone(item.get("phone"))
            or gc_user_id != _clean(item.get("getcourse_user_id"), 200)
            or (name and not _clean(item.get("link_name"), 200))
            or (not gc_user_id and _clean(item.get("link_source"), 80) != "inbox-resolved")
        )
        if should_remember:
            await _remember_client_link(
                phone,
                getcourse_user_id=gc_user_id,
                name=name,
                source="inbox" if gc_user_id else "inbox-resolved",
            )
        unread = unread_map.get((item["channel_id"], item["chat_type"], item["chat_id"]), 0)
        items.append(
            {
                "channel_id": item["channel_id"],
                "chat_type": item["chat_type"],
                "chat_id": item["chat_id"],
                "channel_label": channel["label"],
                "provider": channel.get("provider", "wazzup"),
                "name": name,
                "phone": phone,
                "getcourse_user_id": gc_user_id,
                "preview": _inbox_preview(message),
                "direction": item["direction"],
                "needs_reply": item["direction"] == "incoming",
                "sent_at": item["sent_at"],
                "unread": unread,
            }
        )
    items = _sort_inbox_items(items)
    return {
        "items": items,
        "unread": sum(item["unread"] for item in items),
        "unanswered": sum(1 for item in items if item["needs_reply"]),
    }


@router.get("/health")
async def health() -> dict[str, Any]:
    db = await _connect()
    try:
        admins = int((await (await db.execute("SELECT COUNT(*) FROM admins WHERE enabled=1")).fetchone())[0])
        devices = int((await (await db.execute("SELECT COUNT(*) FROM devices WHERE revoked_at='' AND expires_at>?", (_iso(),))).fetchone())[0])
        contacts = int((await (await db.execute("SELECT COUNT(*) FROM contacts")).fetchone())[0])
        chats = int((await (await db.execute("SELECT COUNT(*) FROM wazzup_chats")).fetchone())[0])
        messages = int((await (await db.execute("SELECT COUNT(*) FROM wazzup_messages")).fetchone())[0])
    finally:
        await db.close()
    identity = _identity_index.status() if _identity_index is not None else {"status": "unavailable"}
    return {"ok": True, "module": MODULE_ID, "api_key_configured": bool(_api_key()), "admins": admins, "devices": devices, "contacts": contacts, "chats": chats, "messages": messages, "identity": identity}


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {
        "ok": True,
        "api_key_configured": bool(_api_key()),
        "api_key_hint": f"задан, длина {len(_api_key())}" if _api_key() else "не задан",
        "allowed_origin": _allowed_origin(),
        "amocrm_origin": _amo_origin(),
        "customer_db": {"path": str(_customer_db_path()), "ready": _customer_db_path().is_file()},
        "identity": _identity_index.status() if _identity_index is not None else {"status": "unavailable"},
        "activation_persistent": True,
        "device_ttl_days": DEVICE_TTL_DAYS,
        "vk_configured": bool(_vk_token() and _vk_group_id()),
        "vk_group_id": _vk_group_id(),
        "vk_group_name": await _setting("vk_group_name"),
        "vk_last_sync_at": await _setting("vk_last_sync_at"),
        "telegram_last_sync_at": await _setting("telegram_last_sync_at"),
    }


@router.put("/settings")
async def put_settings(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-settings", limit=20, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    key = _clean(data.get("api_key"), 4000)
    origin = _clean(data.get("allowed_origin"), 1000).rstrip("/")
    amo_origin = _clean(data.get("amocrm_origin"), 1000).rstrip("/")
    vk_token = _clean(data.get("vk_token"), 8000)
    vk_group_raw = _clean(data.get("vk_group_id"), 1000)
    vk_group_id = re.sub(r"\D+", "", vk_group_raw)
    values = _read_env_values()
    if key:
        if len(key) < 20:
            raise HTTPException(400, "API key слишком короткий")
        values[ENV_KEY] = key
        os.environ[ENV_KEY] = key
        _log("info", "Wazzup API key updated by admin=%s", user.get("username"))
    if origin:
        parsed = urlsplit(origin)
        if parsed.scheme != "https" or not parsed.hostname or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            raise HTTPException(400, "Укажите корень GetCourse-домена в формате https://example.ru")
        values[ORIGIN_ENV_KEY] = origin
        os.environ[ORIGIN_ENV_KEY] = origin
    if amo_origin:
        parsed = urlsplit(amo_origin)
        if parsed.scheme != "https" or not parsed.hostname or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            raise HTTPException(400, "Укажите корень amoCRM в формате https://example.amocrm.ru")
        values[AMO_ORIGIN_ENV_KEY] = amo_origin
        os.environ[AMO_ORIGIN_ENV_KEY] = amo_origin
    if vk_token:
        if len(vk_token) < 40:
            raise HTTPException(400, "Ключ VK слишком короткий")
        values[VK_TOKEN_ENV_KEY] = vk_token
        os.environ[VK_TOKEN_ENV_KEY] = vk_token
    if vk_group_raw:
        if not vk_group_id:
            raise HTTPException(400, "Укажите ID сообщества VK")
        values[VK_GROUP_ENV_KEY] = vk_group_id
        os.environ[VK_GROUP_ENV_KEY] = vk_group_id
    if key or origin or amo_origin or vk_token or vk_group_raw:
        _write_env_values(values)
    return await get_settings(request)


@router.post("/settings/test")
async def test_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    channels = await _wazzup_request("GET", "/channels")
    rows = channels if isinstance(channels, list) else []
    active = sum(1 for row in rows if isinstance(row, dict) and _clean(row.get("state"), 40).lower() == "active")
    return {"ok": True, "channels": len(rows), "active_channels": active, "message": f"Wazzup доступен: {active} активных каналов из {len(rows)}"}


@router.get("/telegram/status")
async def telegram_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    state = await _telegram_auth_state(refresh=True)
    return {"ok": True, **state, "last_sync_at": await _setting("telegram_last_sync_at")}


@router.post("/telegram/auth/send-code")
async def telegram_send_code(request: Request) -> dict[str, Any]:
    global _telegram_state_cache
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-telegram-code", limit=8, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    phone = _normalize_phone(data.get("phone"))
    if not phone:
        raise HTTPException(400, "Укажите телефон")

    async def send(client: Any) -> dict[str, Any]:
        if await client.is_user_authorized():
            return {"status": "ready", "account": _telegram_user_view(await client.get_me())}
        sent = await client.send_code_request(phone)
        _telegram_auth_pending[phone] = {
            "phone_code_hash": _clean(getattr(sent, "phone_code_hash", ""), 300),
            "created_at": time.time(),
            "password_required": False,
        }
        return {"status": "code_required"}

    result = await _telegram_run(send)
    _telegram_state_cache = (0.0, {})
    return {"ok": True, **result}


@router.post("/telegram/auth/confirm")
async def telegram_confirm(request: Request) -> dict[str, Any]:
    global _telegram_state_cache
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-telegram-confirm", limit=20, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    phone = _normalize_phone(data.get("phone"))
    code = re.sub(r"\s+", "", _clean(data.get("code"), 30))
    password = _clean(data.get("password"), 500)
    pending = _telegram_auth_pending.get(phone)
    if not phone or not pending or time.time() - float(pending.get("created_at", 0)) > 600:
        raise HTTPException(400, "Запросите код повторно")
    if not pending.get("password_required") and not code:
        raise HTTPException(400, "Введите код")

    async def confirm(client: Any) -> dict[str, Any]:
        try:
            from telethon.errors import SessionPasswordNeededError
        except Exception as exc:
            raise HTTPException(503, "Telethon не установлен") from exc
        if pending.get("password_required"):
            if not password:
                return {"status": "password_required"}
            await client.sign_in(password=password)
        else:
            try:
                await client.sign_in(phone=phone, code=code, phone_code_hash=pending["phone_code_hash"])
            except SessionPasswordNeededError:
                pending["password_required"] = True
                if not password:
                    return {"status": "password_required"}
                await client.sign_in(password=password)
        if not await client.is_user_authorized():
            raise HTTPException(401, "Авторизация не завершена")
        return {"status": "ready", "account": _telegram_user_view(await client.get_me())}

    result = await _telegram_run(confirm)
    if result.get("status") == "ready":
        _telegram_auth_pending.pop(phone, None)
    _telegram_state_cache = (0.0, {})
    return {"ok": True, **result}


@router.post("/telegram/sync")
async def telegram_sync(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    result = await _sync_telegram_dialogs(full=True)
    return {"ok": True, **result}


async def _vk_connection_info() -> dict[str, Any]:
    groups = await _vk_request("groups.getById", {"group_id": _vk_group_id()})
    group = groups.get("groups", [])[0] if isinstance(groups, dict) and groups.get("groups") else groups[0] if isinstance(groups, list) and groups else {}
    name = _clean(group.get("name"), 200) if isinstance(group, dict) else ""
    if name:
        await _set_setting("vk_group_name", name)
    conversations = await _vk_request("messages.getConversations", {"group_id": _vk_group_id(), "count": 1})
    total = int(conversations.get("count") or 0) if isinstance(conversations, dict) else 0
    return {"ok": True, "group_id": _vk_group_id(), "name": name, "conversations": total}


async def _vk_stats() -> dict[str, int]:
    channel_id = _vk_channel_id()
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT
               (SELECT COUNT(*) FROM external_identity_links WHERE provider='vk') AS links,
               (SELECT COUNT(*) FROM wazzup_chats WHERE channel_id=? AND chat_type='vk') AS chats,
               (SELECT COUNT(*) FROM wazzup_messages WHERE channel_id=? AND chat_type='vk') AS messages""",
            (channel_id, channel_id),
        )).fetchone()
        return {key: int(row[key] or 0) for key in ("links", "chats", "messages")}
    finally:
        await db.close()


@router.post("/vk/test")
async def test_vk(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return await _vk_connection_info()


@router.get("/vk/status")
async def vk_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {
        "ok": True,
        "configured": bool(_vk_token() and _vk_group_id()),
        "callback": bool(await _setting("vk_callback_server_id")),
        "last_sync_at": await _setting("vk_last_sync_at"),
        "group_name": await _setting("vk_group_name"),
        **await _vk_stats(),
    }


async def _register_vk_callback() -> dict[str, Any]:
    connection = await _vk_connection_info()
    callback_key = await _setting("vk_callback_key") or secrets.token_urlsafe(24)
    callback_secret = _vk_callback_secret(await _setting("vk_callback_secret"))
    await _set_setting("vk_callback_key", callback_key)
    await _set_setting("vk_callback_secret", callback_secret)
    confirmation = await _vk_request("groups.getCallbackConfirmationCode", {"group_id": _vk_group_id()})
    code = _clean(confirmation.get("code"), 200) if isinstance(confirmation, dict) else ""
    await _set_setting("vk_confirmation_code", code)
    title = "GetCourse"
    url = f"{PUBLIC_API_BASE}/vk/callback/{callback_key}"
    server_id = await _setting("vk_callback_server_id")
    if not server_id:
        servers = await _vk_request("groups.getCallbackServers", {"group_id": _vk_group_id()})
        items = servers.get("items") if isinstance(servers, dict) and isinstance(servers.get("items"), list) else []
        server_id = next(
            (_clean(item.get("id"), 80) for item in items if isinstance(item, dict) and _clean(item.get("url"), 4000) == url),
            "",
        )
    if server_id:
        await _vk_request("groups.editCallbackServer", {"group_id": _vk_group_id(), "server_id": server_id, "url": url, "title": title, "secret_key": callback_secret})
    else:
        result = await _vk_request("groups.addCallbackServer", {"group_id": _vk_group_id(), "url": url, "title": title, "secret_key": callback_secret})
        server_id = _clean(result.get("server_id"), 80) if isinstance(result, dict) else _clean(result, 80)
    if not server_id:
        raise HTTPException(502, "VK не вернул ID сервера")
    await _vk_request("groups.setCallbackSettings", {"group_id": _vk_group_id(), "server_id": server_id, "api_version": VK_API_VERSION, "message_new": 1, "message_reply": 1, "message_edit": 1})
    await _set_setting("vk_callback_server_id", server_id)
    return {**connection, "callback": True, "server_id": server_id}


@router.post("/vk/connect")
async def connect_vk(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-vk-connect", limit=10, window_seconds=3600, subject=user["username"])
    callback = await _register_vk_callback()
    result_sync = await _sync_vk_conversations(full=True)
    return {**callback, **result_sync}


@router.post("/vk/sync")
async def sync_vk(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, **await _sync_vk_conversations(full=True)}


@router.post("/vk/callback/{key}")
async def vk_callback(key: str, request: Request) -> PlainTextResponse:
    expected_key = await _setting("vk_callback_key")
    if not expected_key or not secrets.compare_digest(_clean(key, 200), expected_key):
        return PlainTextResponse("not found", status_code=404)
    body = await request.body()
    if len(body) > MAX_WEBHOOK_BYTES:
        return PlainTextResponse("error", status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return PlainTextResponse("error", status_code=400)
    if _clean(payload.get("group_id"), 80) != _vk_group_id():
        return PlainTextResponse("error", status_code=403)
    secret = _clean(payload.get("secret"), 1000)
    expected_secret = await _setting("vk_callback_secret")
    if expected_secret and not secrets.compare_digest(secret, expected_secret):
        return PlainTextResponse("error", status_code=403)
    if payload.get("type") == "confirmation":
        return PlainTextResponse(await _setting("vk_confirmation_code"))
    if payload.get("type") in {"message_new", "message_reply", "message_edit"}:
        obj = payload.get("object") if isinstance(payload.get("object"), dict) else {}
        message = obj.get("message") if isinstance(obj.get("message"), dict) else obj
        peer_id = _clean(message.get("peer_id"), 200) if isinstance(message, dict) else ""
        link = await _external_link(peer_id=peer_id)
        if not link and peer_id:
            identity = resolve_vk_identity(peer_id)
            await _remember_external_link(
                identity or {"name": f"VK {peer_id}"}, "callback", external_user_id=peer_id,
            )
            link = await _external_link(peer_id=peer_id)
        if peer_id and link and isinstance(message, dict):
            await _store_vk_messages(peer_id, [message], link)
    return PlainTextResponse("ok")


@router.get("/webhook/status")
async def webhook_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    try:
        data = await _wazzup_request("GET", "/webhooks")
    except HTTPException as exc:
        return {
            "ok": False,
            "configured": False,
            "messages_and_statuses": False,
            "error": _clean(exc.detail, 240),
        }
    expected = f"{PUBLIC_API_BASE}/webhook/inbound/{await _setting('webhook_secret')}"
    current = _clean(data.get("webhooksUri"), 4000) if isinstance(data, dict) else ""
    subscriptions = data.get("subscriptions") if isinstance(data, dict) and isinstance(data.get("subscriptions"), dict) else {}
    return {
        "ok": True,
        "configured": bool(current) and secrets.compare_digest(current, expected),
        "messages_and_statuses": bool(subscriptions.get("messagesAndStatuses")),
    }


@router.post("/webhook/register")
async def register_webhook(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-webhook-register", limit=10, window_seconds=3600, subject=user["username"])
    uri = f"{PUBLIC_API_BASE}/webhook/inbound/{await _setting('webhook_secret')}"
    await _wazzup_request(
        "PATCH",
        "/webhooks",
        {
            "webhooksUri": uri,
            "subscriptions": {"messagesAndStatuses": True},
        },
    )
    _log("info", "Wazzup messages/statuses webhook registered by=%s", user.get("username"))
    return {"ok": True, "configured": True, "messages_and_statuses": True}


@router.post("/webhook/inbound/{secret}")
async def inbound_webhook(secret: str, request: Request) -> JSONResponse:
    expected = await _setting("webhook_secret")
    if not expected or not secrets.compare_digest(_clean(secret, 1000), expected):
        return JSONResponse({"ok": False}, status_code=404)
    body = await request.body()
    if len(body) > MAX_WEBHOOK_BYTES:
        return JSONResponse({"ok": False, "error": "payload too large"}, status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "json object required"}, status_code=400)
    result = await _ingest_webhook(payload)
    return JSONResponse({"ok": True, **result})


@router.get("/snippet")
async def snippet(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    public_root = PUBLIC_API_BASE.rsplit("/api", 1)[0]
    static_url = f"{public_root}/static/widget.js"
    api_url = f"{PUBLIC_API_BASE}/widget"
    code = f'<script src="{static_url}" data-nexus-wazzup-api="{api_url}" async></script>'
    return {"ok": True, "snippet": code, "static_url": static_url, "api_url": api_url}


@router.post("/admins/sync")
async def sync_admins(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-sync", limit=20, window_seconds=3600, subject=user["username"])
    db = await _connect()
    try:
        local_rows = await (await db.execute(
            "SELECT wazzup_user_id AS id,name,phone FROM admins WHERE enabled=1 ORDER BY id"
        )).fetchall()
    finally:
        await db.close()
    pushed = await _upsert_admins([dict(row) for row in local_rows], user.get("username", ""))
    users = _users_from_response(await _wazzup_request("GET", "/users"))
    now = _iso()
    db = await _connect()
    try:
        for item in users:
            await db.execute(
                """INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)
                   ON CONFLICT(wazzup_user_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at""",
                (item["id"], item["name"], now, now),
            )
        await db.commit()
    finally:
        await db.close()
    _log("info", "Wazzup users synced count=%s by=%s", len(users), user.get("username"))
    return {"ok": True, "synced": len(users), "pushed": pushed}


async def _upsert_admins(items: list[dict[str, str]], actor: str = "") -> int:
    if not items:
        return 0
    payload = []
    for item in items:
        row = {"id": item["id"], "name": item["name"]}
        phone = _normalize_phone(item.get("phone"))
        if phone:
            row["phone"] = phone[1:]
        payload.append(row)
    await _wazzup_request("POST", "/users", payload)
    now = _iso()
    db = await _connect()
    try:
        for item in items:
            await db.execute(
                """INSERT INTO admins(wazzup_user_id,name,phone,enabled,created_at,updated_at) VALUES(?,?,?,1,?,?)
                   ON CONFLICT(wazzup_user_id) DO UPDATE SET name=excluded.name,phone=excluded.phone,updated_at=excluded.updated_at""",
                (item["id"], item["name"], _normalize_phone(item.get("phone")), now, now),
            )
            if item["id"].startswith("getcourse-"):
                admin = await (await db.execute(
                    "SELECT id FROM admins WHERE wazzup_user_id=?", (item["id"],)
                )).fetchone()
                await db.execute(
                    """INSERT INTO manager_bindings(platform,platform_user_id,admin_id,created_at,updated_at)
                       VALUES('getcourse',?,?,?,?) ON CONFLICT(platform,platform_user_id) DO UPDATE SET
                       admin_id=excluded.admin_id,updated_at=excluded.updated_at""",
                    (item["id"].removeprefix("getcourse-"), int(admin["id"]), now, now),
                )
        await db.commit()
    finally:
        await db.close()
    _log("info", "Wazzup users provisioned count=%s by=%s", len(items), actor)
    return len(items)


@router.post("/admins")
async def create_admin(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-admin-create", limit=60, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    name = _clean(data.get("name"), 150)
    if len(name) < 2:
        raise HTTPException(400, "Укажите имя сотрудника")
    db = await _connect()
    try:
        existing = await (await db.execute(
            "SELECT id,wazzup_user_id,name FROM admins WHERE lower(name)=lower(?) LIMIT 1", (name,),
        )).fetchone()
    finally:
        await db.close()
    if existing:
        return {"ok": True, "existing": True, "wazzup_user_id": existing["wazzup_user_id"], "admin": dict(existing)}
    wazzup_id = _clean(data.get("wazzup_user_id"), 100) or f"nexus-gcw-{secrets.token_hex(12)}"
    await _upsert_admins([{"id": wazzup_id, "name": name, "phone": _clean(data.get("phone"), 50)}], user["username"])
    await _audit("provision_admin", "ok", error=f"created:{name}")
    return {"ok": True, "wazzup_user_id": wazzup_id, "admin": {"name": name, "wazzup_user_id": wazzup_id}}


@router.post("/admins/sync-getcourse")
async def sync_getcourse_staff(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await _read_json(request)
    rows = data.get("staff")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(400, "На странице GetCourse не найдены сотрудники")
    if len(rows) > 100:
        raise HTTPException(400, "За один раз можно синхронизировать не более 100 сотрудников")
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        gc_id = re.sub(r"\D+", "", _clean(raw.get("id"), 40))
        name = _clean(raw.get("name"), 150)
        if not gc_id or not name or gc_id in seen:
            continue
        seen.add(gc_id)
        items.append({"id": f"getcourse-{gc_id}", "name": name, "phone": _clean(raw.get("phone"), 50)})
    if not items:
        raise HTTPException(400, "Не удалось прочитать ID и имена сотрудников GetCourse")
    count = await _upsert_admins(items, user["username"])
    await _audit("sync_getcourse_staff", "ok", error=f"count:{count}")
    return {"ok": True, "synced": count}


@router.get("/admins")
async def list_admins(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """SELECT a.*,COUNT(d.id) AS devices,
                   COALESCE(SUM(CASE WHEN d.revoked_at='' AND d.expires_at>? THEN 1 ELSE 0 END),0) AS active_devices,
                   EXISTS(SELECT 1 FROM activation_codes c WHERE c.admin_id=a.id AND c.expires_at>?) AS has_activation_code
                   FROM admins a LEFT JOIN devices d ON d.admin_id=a.id GROUP BY a.id ORDER BY a.name""",
                (_iso(), _iso()),
            )
        ).fetchall()
        bindings = await (await db.execute(
            "SELECT platform,platform_user_id,platform_user_email,admin_id FROM manager_bindings ORDER BY platform,platform_user_id"
        )).fetchall()
    finally:
        await db.close()
    by_admin: dict[int, list[dict[str, str]]] = {}
    for binding in bindings:
        by_admin.setdefault(int(binding["admin_id"]), []).append({
            "platform": binding["platform"],
            "platform_user_id": binding["platform_user_id"],
            "platform_user_email": binding["platform_user_email"],
        })
    return {"ok": True, "admins": [
        {**dict(row), "enabled": bool(row["enabled"]), "bindings": by_admin.get(int(row["id"]), [])}
        for row in rows
    ]}


async def _amocrm_staff_catalog() -> list[dict[str, Any]]:
    global _staff_catalog_cache
    expires_at, cached = _staff_catalog_cache
    if expires_at > time.monotonic():
        return [dict(row) for row in cached]
    base_url = _clean(os.environ.get("AMO_BASE_URL"), 1000).rstrip("/")
    token = _clean(os.environ.get("AMO_ACCESS_TOKEN"), 5000)
    if not base_url or not token:
        return []
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(
            base_url + "/api/v4/users?limit=250",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
    if response.status_code != 200:
        _log("warning", "amoCRM staff catalog failed status=%s", response.status_code)
        return []
    body = response.json()
    rows = ((body.get("_embedded") or {}).get("users") or []) if isinstance(body, dict) else []
    result = [
        {
            "id": _clean(row.get("id"), 200),
            "name": _clean(row.get("name") or row.get("email") or row.get("id"), 200),
            "email": _clean(row.get("email"), 320).casefold(),
            "active": bool(((row.get("rights") or {}).get("is_active", True))),
        }
        for row in rows if isinstance(row, dict) and _clean(row.get("id"), 200)
    ]
    _staff_catalog_cache = (time.monotonic() + STAFF_CATALOG_CACHE_SECONDS, [dict(row) for row in result])
    return result


@router.get("/staff/catalog")
async def staff_catalog(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT b.platform_user_id AS id,b.platform_user_email AS email,a.name
               FROM manager_bindings b JOIN admins a ON a.id=b.admin_id
               WHERE b.platform='getcourse' ORDER BY a.name,b.platform_user_id"""
        )).fetchall()
    finally:
        await db.close()
    getcourse = [
        {"id": row["id"], "name": row["name"], "email": row["email"], "active": True}
        for row in rows
    ]
    return {"ok": True, "getcourse": getcourse, "amocrm": await _amocrm_staff_catalog()}


@router.put("/admins/{admin_id}/bindings")
async def save_admin_bindings(admin_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _read_json(request)
    rows = data.get("bindings")
    if not isinstance(rows, list) or len(rows) > 20:
        raise HTTPException(400, "Некорректные привязки")
    normalized: list[tuple[str, str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform = _clean(row.get("platform"), 40).lower()
        platform_user_id = _clean(row.get("platform_user_id"), 200)
        email = _clean(row.get("platform_user_email"), 320).casefold()
        if platform not in {"getcourse", "amocrm"} or not platform_user_id:
            raise HTTPException(400, "Проверьте ID сотрудников")
        normalized.append((platform, platform_user_id, email))
    now = _iso()
    db = await _connect()
    try:
        admin = await (await db.execute("SELECT id FROM admins WHERE id=?", (admin_id,))).fetchone()
        if not admin:
            raise HTTPException(404, "Сотрудник не найден")
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("DELETE FROM manager_bindings WHERE admin_id=?", (admin_id,))
        for platform, platform_user_id, email in normalized:
            await db.execute(
                """INSERT INTO manager_bindings(platform,platform_user_id,platform_user_email,admin_id,created_at,updated_at)
                   VALUES(?,?,?,?,?,?) ON CONFLICT(platform,platform_user_id) DO UPDATE SET
                   platform_user_email=excluded.platform_user_email,admin_id=excluded.admin_id,updated_at=excluded.updated_at""",
                (platform, platform_user_id, email, admin_id, now, now),
            )
        await db.commit()
    finally:
        await db.close()
    return {"ok": True, "bindings": [
        {"platform": platform, "platform_user_id": platform_user_id, "platform_user_email": email}
        for platform, platform_user_id, email in normalized
    ]}


@router.patch("/admins/{admin_id}")
async def update_admin(admin_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _read_json(request)
    if "enabled" not in data and "role" not in data:
        raise HTTPException(400, "Нет изменений")
    role = _clean(data.get("role"), 20).lower()
    if "role" in data and role not in {"admin", "employee"}:
        raise HTTPException(400, "Неизвестная роль")
    db = await _connect()
    try:
        updates = ["updated_at=?"]
        params: list[Any] = [_iso()]
        if "enabled" in data:
            updates.append("enabled=?")
            params.append(1 if data["enabled"] else 0)
        if "role" in data:
            updates.append("role=?")
            params.append(role)
        params.append(admin_id)
        cur = await db.execute(f"UPDATE admins SET {','.join(updates)} WHERE id=?", params)
        if not cur.rowcount:
            raise HTTPException(404, "Сотрудник не найден")
        if "enabled" in data and not data["enabled"]:
            await db.execute("UPDATE devices SET revoked_at=? WHERE admin_id=? AND revoked_at=''", (_iso(), admin_id))
        await db.commit()
    finally:
        await db.close()
    return {"ok": True}


@router.post("/admins/{admin_id}/activation-code")
async def create_activation_code(admin_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-codes", limit=100, window_seconds=3600, subject=user["username"])
    code = _activation_code()
    expires = ACTIVATION_EXPIRES_AT
    db = await _connect()
    try:
        admin = await (await db.execute("SELECT id,name,enabled FROM admins WHERE id=?", (admin_id,))).fetchone()
        if not admin:
            raise HTTPException(404, "Сотрудник не найден")
        if not admin["enabled"]:
            raise HTTPException(409, "Сотрудник выключен")
        previous = await (await db.execute(
            "SELECT 1 FROM activation_codes WHERE admin_id=? AND expires_at>? LIMIT 1", (admin_id, _iso()),
        )).fetchone()
        await db.execute("DELETE FROM activation_codes WHERE admin_id=?", (admin_id,))
        revoked = 0
        if previous:
            cursor = await db.execute(
                "UPDATE devices SET revoked_at=? WHERE admin_id=? AND revoked_at=''", (_iso(), admin_id),
            )
            revoked = cursor.rowcount
        await db.execute(
            "INSERT INTO activation_codes(admin_id,code_hash,expires_at,created_at) VALUES(?,?,?,?)",
            (admin_id, _hash(_normalize_code(code)), expires, _iso()),
        )
        await db.commit()
    finally:
        await db.close()
    return {
        "ok": True, "code": code, "expires_at": expires, "admin_name": admin["name"],
        "persistent": True, "reissued": bool(previous), "revoked_devices": revoked,
    }


@router.get("/devices")
async def list_devices(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """SELECT d.id,d.admin_id,d.token_hint,d.created_at,d.last_used_at,d.expires_at,d.revoked_at,a.name AS admin_name
                   FROM devices d JOIN admins a ON a.id=d.admin_id ORDER BY d.last_used_at DESC,d.id DESC"""
            )
        ).fetchall()
    finally:
        await db.close()
    return {"ok": True, "devices": [dict(row) for row in rows]}


@router.delete("/devices/{device_id}")
async def revoke_device(device_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        cur = await db.execute("UPDATE devices SET revoked_at=? WHERE id=? AND revoked_at=''", (_iso(), device_id))
        if not cur.rowcount:
            raise HTTPException(404, "Активное устройство не найдено")
        await db.commit()
    finally:
        await db.close()
    return {"ok": True}


@router.get("/events")
async def list_events(request: Request, limit: int = Query(100, ge=1, le=300)) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """SELECT e.*,a.name AS admin_name FROM events e LEFT JOIN admins a ON a.id=e.admin_id
                   ORDER BY e.id DESC LIMIT ?""",
                (limit,),
            )
        ).fetchall()
    finally:
        await db.close()
    return {"ok": True, "events": [dict(row) for row in rows]}


@router.get("/contacts")
async def list_contacts(request: Request, limit: int = Query(100, ge=1, le=300)) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT c.*,a.name AS admin_name FROM contacts c LEFT JOIN admins a ON a.id=c.admin_id
               ORDER BY c.synced_at DESC,c.id DESC LIMIT ?""", (limit,)
        )).fetchall()
    finally:
        await db.close()
    return {"ok": True, "contacts": [dict(row) for row in rows]}


@router.get("/channels")
async def list_channels(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    body = await _wazzup_request("GET", "/channels")
    rows = body if isinstance(body, list) else []
    channels = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        channels.append({
            "id": _clean(row.get("channelId"), 100), "name": _clean(row.get("name") or row.get("plainId"), 200),
            "transport": _clean(row.get("transport"), 40), "state": _clean(row.get("state"), 40),
        })
    return {"ok": True, "channels": channels}


@router.get("/templates")
async def list_templates(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "templates": await _template_rows(include_disabled=True)}


@router.post("/templates")
async def create_shared_template(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "template": await _save_template(await _read_json(request), None)}


@router.patch("/templates/{template_id}")
async def update_shared_template(template_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "template": await _save_template(await _read_json(request), None, template_id)}


@router.delete("/templates/{template_id}")
async def delete_shared_template(template_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    await _delete_template(template_id, None)
    return {"ok": True}


@router.get("/identity/status")
async def identity_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "index": _identity_index.status() if _identity_index is not None else {"status": "unavailable"}}


@router.post("/identity/rebuild")
async def identity_rebuild(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    if _identity_index is None:
        raise HTTPException(503, "Индекс недоступен")
    result = await asyncio.to_thread(_identity_index.build_if_changed, force=True)
    return {"ok": result.get("status") not in {"error", "missing"}, "index": result}


@router.get("/identity/rules")
async def identity_rules(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (await db.execute("SELECT * FROM identity_rules ORDER BY source,entity_type,priority,id")).fetchall()
        catalog = await (await db.execute("SELECT * FROM field_catalog ORDER BY source,entity_type,label,field_key")).fetchall()
        return {"ok": True, "rules": [dict(row) for row in rows], "fields": [dict(row) for row in catalog]}
    finally:
        await db.close()


@router.put("/identity/rules")
async def save_identity_rules(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _read_json(request)
    rows = data.get("rules")
    if not isinstance(rows, list) or len(rows) > 100:
        raise HTTPException(400, "Некорректные правила")
    now = _iso()
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("DELETE FROM identity_rules")
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            platform = _clean(row.get("source"), 40).lower()
            scope = _clean(row.get("entity_type"), 40).lower()
            field_name = _clean(row.get("field_key"), 200)
            identifier_type = _clean(row.get("identity_type"), 60).lower()
            if platform not in {"getcourse", "amocrm"} or scope not in {"user", "order", "contact", "lead"} or not field_name or not identifier_type:
                raise HTTPException(400, "Проверьте поля правил")
            await db.execute(
                "INSERT INTO identity_rules(source,entity_type,field_key,field_label,identity_type,priority,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (platform, scope, field_name, _clean(row.get("field_label"), 200), identifier_type, int(row.get("priority") or index * 10), int(row.get("enabled", True)), now, now),
            )
        await db.commit()
    finally:
        await db.close()
    return await identity_rules(request)


@router.options("/widget/{path:path}")
async def widget_options(path: str, request: Request) -> Response:
    origin = request.headers.get("origin", "")
    if origin.rstrip("/") not in _allowed_origins():
        return Response(status_code=403)
    return Response(status_code=204, headers=_cors_headers(origin))


@router.post("/widget/activate")
async def widget_activate(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        enforce_rate_limit(request, "getcourse-wazzup-activate", limit=60, window_seconds=3600)
        data = await _read_json(request)
        platform = "getcourse" if mode == "test" else mode
        platform_user_id = _clean(data.get("platform_user_id"), 200)
        platform_user_email = _clean(data.get("platform_user_email"), 320).casefold()
        if platform == "amocrm" and not platform_user_id:
            return _widget_response(request, {"ok": False, "error": "Не удалось определить пользователя amoCRM"}, 400)
        code = _normalize_code(data.get("code"))
        if len(code) != 12:
            return _widget_response(request, {"ok": False, "error": "Неверный код активации"}, 400)
        now = _iso()
        db = await _connect()
        try:
            await db.execute("BEGIN IMMEDIATE")
            row = await (
                await db.execute(
                    """SELECT c.*,a.name,a.enabled FROM activation_codes c JOIN admins a ON a.id=c.admin_id
                       WHERE c.code_hash=?""",
                    (_hash(code),),
                )
            ).fetchone()
            if not row or row["expires_at"] <= now or not row["enabled"]:
                await db.rollback()
                return _widget_response(request, {"ok": False, "error": "Код недействителен"}, 401)
            token = secrets.token_urlsafe(40)
            expires = _iso(_now_dt() + timedelta(days=DEVICE_TTL_DAYS))
            cur = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at,platform,platform_user_id,platform_user_email)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (row["admin_id"], _hash(token), f"••••{token[-4:]}", now, now, expires, platform, platform_user_id, platform_user_email),
            )
            device_id = int(cur.lastrowid)
            if platform == "amocrm":
                await db.execute(
                    """INSERT INTO manager_bindings(platform,platform_user_id,platform_user_email,admin_id,created_at,updated_at)
                       VALUES('amocrm',?,?,?,?,?) ON CONFLICT(platform,platform_user_id) DO UPDATE SET
                       platform_user_email=excluded.platform_user_email,admin_id=excluded.admin_id,updated_at=excluded.updated_at""",
                    (platform_user_id, platform_user_email, row["admin_id"], now, now),
                )
            await db.commit()
        finally:
            await db.close()
        await _audit("activate", "ok", admin_id=row["admin_id"], device_id=device_id)
        return _widget_response(
            request,
            {"ok": True, "device_token": token, "platform": platform, "admin": {"id": row["admin_id"], "name": row["name"]}, "expires_at": expires},
        )
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "GetCourse Wazzup activation failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось активировать устройство"}, 500)


@router.post("/widget/context")
async def widget_context(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        context = _widget_context(data, mode, device)
        fields = context.get("fields", {})
        if fields:
            now = _iso()
            db = await _connect()
            try:
                for key, value in list(fields.items())[:500]:
                    field_key = _clean(key, 200)
                    if not field_key:
                        continue
                    await db.execute(
                        """INSERT INTO field_catalog(source,entity_type,field_key,label,value_type,last_seen_at)
                           VALUES(?,?,?,?,?,?) ON CONFLICT(source,entity_type,field_key) DO UPDATE SET
                           label=excluded.label,value_type=excluded.value_type,last_seen_at=excluded.last_seen_at""",
                        (context["platform"], context["entity_type"], field_key, field_key, "object" if isinstance(value, (dict, list)) else "text", now),
                    )
                await db.commit()
            finally:
                await db.close()
        resolved = await _resolve_widget_context(data, mode, device)
        return _widget_response(request, {"ok": True, "context": context, **resolved})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger context resolution failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось определить клиента"}, 500)


@router.post("/widget/templates")
async def widget_templates(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        action = _clean(data.get("action"), 20).lower() or "list"
        admin_id = int(device["admin_id"])
        shared = _clean(data.get("scope"), 20).lower() == "shared"
        if shared and _clean(device.get("admin_role"), 20) != "admin":
            raise HTTPException(403, "Общие шаблоны может менять администратор")
        owner_id = None if shared else admin_id
        if action == "list":
            return _widget_response(request, {
                "ok": True,
                "can_manage_shared": _clean(device.get("admin_role"), 20) == "admin",
                "templates": await _template_rows(admin_id, can_edit_shared=_clean(device.get("admin_role"), 20) == "admin"),
                "variables": TEMPLATE_VARIABLES,
            })
        if action == "create":
            template = await _save_template(data, owner_id)
            return _widget_response(request, {"ok": True, "template": template})
        if action == "import":
            if mode != "amocrm" or _clean(device.get("admin_role"), 20) != "admin":
                raise HTTPException(403, "Импорт доступен администратору amoCRM")
            return _widget_response(request, {"ok": True, **await _import_amo_templates(data.get("templates"), data.get("folder"))})
        template_id = int(data.get("id") or 0)
        if not template_id:
            raise HTTPException(400, "Шаблон не указан")
        if action == "update":
            return _widget_response(request, {"ok": True, "template": await _save_template(data, owner_id, template_id)})
        if action == "delete":
            await _delete_template(template_id, owner_id)
            return _widget_response(request, {"ok": True})
        raise HTTPException(400, "Неизвестное действие")
    except (TypeError, ValueError):
        return _widget_response(request, {"ok": False, "error": "Некорректный шаблон"}, 400)
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger templates failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось обработать шаблоны"}, 500)


@router.post("/widget/template-preview")
async def widget_template_preview(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        body = _clean(data.get("body"), 20_000)
        template_id = int(data.get("id") or 0)
        if template_id:
            db = await _connect()
            try:
                row = await (await db.execute(
                    "SELECT body FROM message_templates WHERE id=? AND enabled=1 AND (owner_admin_id IS NULL OR owner_admin_id=?)",
                    (template_id, int(device["admin_id"])),
                )).fetchone()
            finally:
                await db.close()
            if not row:
                raise HTTPException(404, "Шаблон не найден")
            body = row["body"]
        if not body:
            raise HTTPException(400, "Текст не указан")
        resolved = await _resolve_widget_context(data, mode, device)
        rendered = render_message_template(body, resolved.get("variables", {}))
        return _widget_response(request, {"ok": True, **rendered, "variables": resolved.get("variables", {}), "accounts": resolved.get("accounts", [])})
    except (TypeError, ValueError):
        return _widget_response(request, {"ok": False, "error": "Некорректный шаблон"}, 400)
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger template preview failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось подставить переменные"}, 500)


@router.post("/widget/link")
async def widget_link(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        source_url = _clean(data.get("source_url"), 2000)
        page_kind, entity_id = _page_context(source_url)
        phone = _normalize_phone(data.get("phone"))
        if page_kind != "user" or not source_url.startswith(_allowed_origin() + "/"):
            raise HTTPException(400, "Откройте карточку пользователя GetCourse")
        if phone:
            await _remember_client_link(
                phone,
                getcourse_user_id=entity_id,
                name=_clean(data.get("name"), 200),
                source="getcourse-card",
            )
        await _vk_card_link(data)
        return _widget_response(request, {"ok": True})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup client link failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось связать карточку клиента"}, 500)


@router.post("/widget/inbox")
async def widget_inbox(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enforce_rate_limit(request, "getcourse-wazzup-inbox", limit=900, window_seconds=3600, subject=str(device["id"]))
        channel_ids = data.get("channel_ids") if isinstance(data.get("channel_ids"), list) else []
        result = await _inbox_items(
            device,
            await _all_channels(),
            query=_clean(data.get("query"), 200),
            selected_channel_ids=[_clean(value, 200) for value in channel_ids[:30]],
        )
        return _widget_response(request, {"ok": True, **result})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup inbox failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось загрузить входящие"}, 500)


@router.post("/widget/inbox/read")
async def widget_inbox_read(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        channel_id = _clean(data.get("channel_id"), 200)
        chat_type = _clean(data.get("chat_type"), 40).lower()
        chat_id = _clean(data.get("chat_id"), 250)
        channels = await _all_channels()
        if chat_type not in CHAT_TRANSPORTS or not any(row["channel_id"] == channel_id for row in channels):
            raise HTTPException(400, "Канал недоступен")
        await _inbox_thread_context(channel_id, chat_type, chat_id, channels, device)
        await _mark_thread_read(int(device["id"]), channel_id, chat_type, chat_id)
        return _widget_response(request, {"ok": True})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup inbox read failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось обновить входящие"}, 500)


@router.post("/widget/iframe-link")
async def widget_iframe_link(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    device: dict[str, Any] | None = None
    page_kind = entity_id = phone = ""
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(
            request,
            "getcourse-wazzup-iframe",
            limit=120,
            window_seconds=3600,
            subject=str(device["id"]),
        )
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        source_url = _clean(data.get("source_url"), 2000)
        page_kind, entity_id = _page_context(source_url)
        if not page_kind or not source_url.startswith(_allowed_origin() + "/"):
            return _widget_response(request, {"ok": False, "error": "Откройте карточку пользователя или заказа GetCourse"}, 400)
        phone = _normalize_phone(data.get("phone"))
        name = _clean(data.get("name"), 200) or f"GetCourse {page_kind} #{entity_id}"
        if phone and page_kind == "user":
            await _remember_client_link(
                phone,
                getcourse_user_id=entity_id,
                name=name,
                source="getcourse-card",
            )
        payload: dict[str, Any] = {"user": {"id": device["wazzup_user_id"], "name": device["admin_name"]}, "scope": "global"}
        channels: list[dict[str, str]] = []
        transport = ""
        if phone:
            channels = _active_chat_channels(await _wazzup_request("GET", "/channels"))
            if not channels:
                raise HTTPException(409, "В Wazzup нет активных каналов для нового диалога")
            requested_transport = _clean(data.get("transport"), 40).lower()
            requested_channel_id = _clean(data.get("channel_id"), 200)
            if requested_transport and requested_transport not in CHAT_TRANSPORTS:
                raise HTTPException(400, "Выбранный канал Wazzup недоступен")
            transport = requested_transport or "whatsapp"
            selected_channel = next(
                (
                    channel
                    for channel in channels
                    if channel["transport"] == transport
                    and (not requested_channel_id or channel["channel_id"] == requested_channel_id)
                ),
                None,
            )
            if not selected_channel:
                raise HTTPException(409, "Выбранный канал Wazzup отключён или недоступен")
            known_chat_id, _, _ = await _conversation_rows(selected_channel["channel_id"], transport, phone, 1)
            if transport in {"whatsapp", "viber"}:
                chat_id = known_chat_id or phone[1:]
            elif known_chat_id:
                chat_id = known_chat_id
            else:
                raise HTTPException(
                    409,
                    "Wazzup ещё не вернул ID этого чата. Начните диалог в окне интеграции; после первого сообщения кнопка откроет его и в Wazzup.",
                )
            contact_id = f"getcourse-{page_kind}-{entity_id}"
            await _wazzup_request(
                "POST",
                "/contacts",
                [{"id": contact_id, "responsibleUserId": device["wazzup_user_id"], "name": name, "contactData": [{"chatType": transport, "chatId": chat_id}], "uri": source_url}],
            )
            db = await _connect()
            try:
                await db.execute(
                    """INSERT INTO contacts(page_kind,entity_id,wazzup_contact_id,name,phone_mask,admin_id,synced_at)
                       VALUES(?,?,?,?,?,?,?) ON CONFLICT(page_kind,entity_id) DO UPDATE SET
                       wazzup_contact_id=excluded.wazzup_contact_id,name=excluded.name,phone_mask=excluded.phone_mask,
                       admin_id=excluded.admin_id,synced_at=excluded.synced_at""",
                    (page_kind, entity_id, contact_id, name, _mask_phone(phone), device["admin_id"], _iso()),
                )
                await db.commit()
            finally:
                await db.close()
            payload = {
                "user": {"id": device["wazzup_user_id"], "name": device["admin_name"]},
                "scope": "card",
                "filter": [{"chatType": transport, "chatId": chat_id, "name": name}],
                "activeChat": {"channelId": selected_channel["channel_id"], "chatType": transport, "chatId": chat_id},
                "options": {"clientType": "GetCourse"},
            }
            await _audit("sync_contact", "ok", admin_id=device["admin_id"], device_id=device["id"], page_kind=page_kind, entity_id=entity_id, phone=phone)
        body = await _wazzup_request("POST", "/iframe", payload)
        link = ""
        if isinstance(body, dict):
            link = _clean(body.get("url"), 5000)
            if not link and isinstance(body.get("data"), dict):
                link = _clean(body["data"].get("link"), 5000)
        if not link.startswith("https://") or "wazzup24" not in link.lower():
            raise HTTPException(502, "Wazzup не вернул ссылку на окно чатов")
        await _audit(
            "open_iframe",
            "ok",
            admin_id=device["admin_id"],
            device_id=device["id"],
            page_kind=page_kind,
            entity_id=entity_id,
            phone=phone,
        )
        return _widget_response(
            request,
            {
                "ok": True,
                "url": link,
                "admin_name": device["admin_name"],
                "phone": phone,
                "page_kind": page_kind,
                "entity_id": entity_id,
                "transport": transport,
                "channels": channels,
            },
        )
    except HTTPException as exc:
        if device:
            await _audit(
                "open_iframe",
                "error",
                admin_id=device["admin_id"],
                device_id=device["id"],
                page_kind=page_kind,
                entity_id=entity_id,
                phone=phone,
                error=str(exc.detail),
            )
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup iframe link failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось открыть Wazzup"}, 500)


@router.post("/widget/channels")
async def widget_channels(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(request, "getcourse-wazzup-channels", limit=120, window_seconds=3600, subject=str(device["id"]))
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        channels = await _all_channels()
        scope = _clean(data.get("scope"), 20).lower()
        thread_fields = (
            _clean(data.get("thread_channel_id"), 200),
            _clean(data.get("thread_chat_type"), 40).lower(),
            _clean(data.get("thread_chat_id"), 250),
        )
        thread = await _inbox_thread_context(*thread_fields, channels, device) if any(thread_fields) else None
        if scope == "inbox" and not thread:
            return _widget_response(request, {"ok": True, "channels": [
                {**channel, "available": True, "can_send": True, "has_chat": False, "send_reason": ""}
                for channel in channels
            ]})

        phone = _normalize_phone((thread or {}).get("phone") or data.get("phone"))
        gc_id = _clean((thread or {}).get("getcourse_user_id"), 200)
        views: list[dict[str, Any]] = []
        direct_links: list[tuple[str, str]] = []
        for channel in channels:
            provider = channel.get("provider", "wazzup")
            has_chat = False
            can_send = False
            reason = ""
            if provider == "wazzup":
                has_chat = await _has_conversation(
                    channel["channel_id"], channel["transport"], phone,
                ) if phone else False
                can_send, reason = _channel_send_state(channel, has_chat)
                if not phone and not has_chat:
                    can_send, reason = False, "Телефон не найден"
            else:
                same_provider = bool(
                    thread
                    and ((provider == "vk" and thread["channel_id"].startswith("vk:"))
                         or (provider == TELEGRAM_PROVIDER and thread["channel_id"].startswith("telegram-personal:"))
                         or (provider == SALEBOT_PROVIDER and thread["channel_id"].startswith("salebot:")))
                )
                link = await _external_link(
                    peer_id=_clean((thread or {}).get("chat_id"), 200), provider=provider,
                ) if same_provider else {}
                if not link and thread:
                    link = await _external_link_for_identity(provider, phone=phone, gc_id=gc_id)
                deferred_card = not thread and provider == TELEGRAM_PROVIDER
                if not link and not deferred_card:
                    link = await _provider_card_link(
                        data, mode, device, provider,
                        allow_phone_import=provider == TELEGRAM_PROVIDER,
                    )
                peer_id = _clean(link.get("external_user_id"), 200)
                if peer_id:
                    _, has_chat, _ = await _conversation_rows(
                        channel["channel_id"], channel["transport"], "", 1, exact_chat_id=peer_id,
                    )
                    direct_links.append((provider, peer_id))
                if provider == "vk":
                    context = _widget_context(data, mode, device) if deferred_card else {}
                    can_send = bool(peer_id or _identity_field_value(
                        context, "vk_id", "vkontakte_id", "senler_id", "platform_id", "utm_term",
                    ))
                    reason = "" if can_send else "VK клиента не найден"
                elif provider == SALEBOT_PROVIDER:
                    context = _widget_context(data, mode, device) if deferred_card else {}
                    explicit_id = _identity_field_value(context, "salebot_id", "salebot_client_id", "sb_id")
                    if not explicit_id:
                        explicit_id = next((value for kind, value in parse_utm_term(_identity_field_value(context, "utm_term")) if kind == "salebot"), "")
                    can_send = bool(peer_id or explicit_id)
                    reason = "" if can_send else "SaleBot ID не найден. Нужен salebot_id в карточке или utm_term."
                else:
                    can_send = bool(peer_id or phone)
                    reason = "" if peer_id else "Проверка Telegram…" if link.get("pending") else "" if phone else "Telegram клиента не найден"
            views.append({
                **channel,
                "available": can_send,
                "can_send": can_send,
                "has_chat": has_chat,
                "send_reason": reason,
                **({"pending": True} if provider == TELEGRAM_PROVIDER and link.get("pending") else {}),
            })
        if not thread:
            owner_id = await _responsible_admin_id(data, mode, device)
            await _assign_client_threads(owner_id, phone=phone, direct_links=direct_links)
        return _widget_response(request, {"ok": True, "channels": views})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup channel list failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось получить каналы Wazzup"}, 500)


async def _requested_channel(channel_id: str, transport: str, provider: str = "wazzup") -> dict[str, str]:
    channels = await _all_channels()
    for channel in channels:
        if channel["channel_id"] == channel_id and channel["transport"] == transport and channel.get("provider", "wazzup") == provider:
            return channel
    raise HTTPException(409, "Канал отключён или недоступен")


def _history_chat_candidate(
    rows: Any,
    channel_id: str,
    transport: str,
    phone: str,
    identity: dict[str, str] | None = None,
) -> dict[str, str] | None:
    if not isinstance(rows, list):
        return None
    identity = identity or {}
    telegram_id = _clean(identity.get("telegram_id"), 250)
    telegram_username = _clean(identity.get("telegram_username"), 200).lstrip("@").casefold()
    for row in rows:
        if not isinstance(row, dict):
            continue
        chat_id = _clean(row.get("chatId"), 250)
        chat_type = _clean(row.get("chatType"), 40).lower()
        if not chat_id or chat_type != transport:
            continue
        channel_rows = row.get("chats") if isinstance(row.get("chats"), list) else []
        channel_match = next(
            (
                item
                for item in channel_rows
                if isinstance(item, dict)
                and _clean(item.get("channelId"), 200) == channel_id
                and _clean(item.get("chatType"), 40).lower() == transport
                and _clean(item.get("chatId"), 250) == chat_id
            ),
            None,
        )
        row_phone = _normalize_phone(row.get("userPhone"))
        row_username = _clean(row.get("userName"), 200).lstrip("@").casefold()
        identity_match = row_phone == phone or (
            transport == "telegram"
            and (
                (telegram_id and chat_id == telegram_id)
                or (telegram_username and row_username == telegram_username)
            )
        )
        if channel_match and identity_match:
            return {
                "channel_id": channel_id,
                "chat_type": transport,
                "chat_id": chat_id,
                "contact_name": _clean(row.get("contactName"), 200),
            }
    return None


def _history_chat_route(row: Any, channel_id: str, transport: str) -> dict[str, str] | None:
    if not isinstance(row, dict):
        return None
    chat_id = _clean(row.get("chatId"), 250)
    chat_type = _clean(row.get("chatType"), 40).lower()
    if not chat_id or chat_type != transport:
        return None
    channel_rows = row.get("chats") if isinstance(row.get("chats"), list) else []
    if not any(
        isinstance(item, dict)
        and _clean(item.get("channelId"), 200) == channel_id
        and _clean(item.get("chatType"), 40).lower() == transport
        and _clean(item.get("chatId"), 250) == chat_id
        for item in channel_rows
    ):
        return None
    return {
        "channel_id": channel_id,
        "chat_type": transport,
        "chat_id": chat_id,
        "contact_name": _clean(row.get("contactName"), 200),
    }


def _same_person_name(left: Any, right: Any) -> bool:
    normalize = lambda value: re.sub(r"\s+", " ", _clean(value, 200)).strip().casefold()
    return bool(normalize(left)) and normalize(left) == normalize(right)


def _history_message_matches_phone(rows: Any, phone: str) -> bool:
    if not isinstance(rows, list):
        return False
    for row in rows:
        if not isinstance(row, dict):
            continue
        contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
        sender = row.get("sender") if isinstance(row.get("sender"), dict) else {}
        recipient = row.get("recipient") if isinstance(row.get("recipient"), dict) else {}
        for value in (
            row.get("authorPhone"),
            row.get("userPhone"),
            contact.get("phone"),
            sender.get("phone"),
            recipient.get("phone"),
        ):
            if _normalize_phone(value) == phone:
                return True
    return False


def _history_message_record(
    row: Any,
    channel_id: str,
    transport: str,
    chat_id: str,
    phone: str,
) -> dict[str, str] | None:
    if not isinstance(row, dict):
        return None
    external_id = _clean(row.get("id") or row.get("messageId"), 250)
    row_channel_id = _clean(row.get("channelId"), 200)
    row_chat_type = _clean(row.get("chatType"), 40).lower()
    row_chat_id = _clean(row.get("chatId"), 250)
    if (
        not external_id
        or row_channel_id != channel_id
        or row_chat_type != transport
        or row_chat_id != chat_id
    ):
        return None
    content = _message_content(row)
    text_value = _clean(row.get("text"), 20_000)
    if not text_value and content["filename"]:
        text_value = f"[Вложение: {content['filename']}]"
    status_value = row.get("status")
    status = _clean(status_value, 80) if isinstance(status_value, str) else ""
    return {
        "external_id": external_id,
        "channel_id": channel_id,
        "chat_type": transport,
        "chat_id": chat_id,
        "phone_hash": _phone_hash(phone),
        "direction": "incoming" if bool(row.get("incoming")) else "outgoing",
        "status": status,
        "text": text_value,
        "content_uri": content["content_uri"],
        "author_name": _clean(row.get("authorName") or row.get("displayAuthorName"), 200),
        "sent_at": _message_time(row.get("datetime") or row.get("dateTime") or row.get("timestampMsg")),
        "raw_json": json.dumps(row, ensure_ascii=False, separators=(",", ":"))[:50_000],
    }


async def _history_sync_due(channel_id: str, phone: str) -> bool:
    db = await _connect()
    try:
        row = await (
            await db.execute(
                "SELECT status,last_attempt_at FROM history_sync_state WHERE channel_id=? AND phone_hash=?",
                (channel_id, _phone_hash(phone)),
            )
        ).fetchone()
        if not row:
            return True
        retry_minutes = _history_retry_minutes(_clean(row["status"], 80))
        cutoff = _iso(_now_dt() - timedelta(minutes=retry_minutes))
        return _clean(row["last_attempt_at"], 80) < cutoff
    finally:
        await db.close()


def _history_retry_minutes(status: str) -> int:
    if status == "imported":
        return HISTORY_SYNC_TTL_MINUTES
    if status == "not_found":
        return HISTORY_NOT_FOUND_TTL_MINUTES
    if status == "no_access":
        return HISTORY_NO_ACCESS_TTL_MINUTES
    return HISTORY_ERROR_TTL_MINUTES


def _missing_history_status(scanned_chats: int) -> str:
    return "no_access" if scanned_chats == 0 else "not_found"


async def _history_sync_info(channel_id: str, phone: str) -> dict[str, Any]:
    db = await _connect()
    try:
        row = await (
            await db.execute(
                "SELECT status,imported,last_success_at FROM history_sync_state WHERE channel_id=? AND phone_hash=?",
                (channel_id, _phone_hash(phone)),
            )
        ).fetchone()
        return dict(row) if row else {"status": "pending", "imported": 0, "last_success_at": ""}
    finally:
        await db.close()


async def _record_history_sync(
    channel_id: str,
    phone: str,
    status: str,
    imported: int,
    *,
    success: bool,
) -> None:
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO history_sync_state(channel_id,phone_hash,last_attempt_at,last_success_at,status,imported)
               VALUES(?,?,?,?,?,?) ON CONFLICT(channel_id,phone_hash) DO UPDATE SET
               last_attempt_at=excluded.last_attempt_at,
               last_success_at=CASE WHEN excluded.last_success_at<>'' THEN excluded.last_success_at ELSE history_sync_state.last_success_at END,
               status=excluded.status,imported=excluded.imported""",
            (channel_id, _phone_hash(phone), now, now if success else "", _clean(status, 80), max(0, imported)),
        )
        await db.commit()
    finally:
        await db.close()


async def _store_history(
    candidate: dict[str, str],
    phone: str,
    rows: list[dict[str, Any]],
) -> int:
    records = [
        record
        for row in rows
        if (
            record := _history_message_record(
                row,
                candidate["channel_id"],
                candidate["chat_type"],
                candidate["chat_id"],
                phone,
            )
        )
    ]
    now = _iso()
    inserted = 0
    db = await _connect()
    try:
        for record in records:
            cursor = await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(external_id) DO UPDATE SET
                   status=excluded.status,text=excluded.text,content_uri=excluded.content_uri,
                   author_name=excluded.author_name,sent_at=excluded.sent_at,raw_json=excluded.raw_json""",
                (
                    record["external_id"], record["channel_id"], record["chat_type"], record["chat_id"],
                    record["phone_hash"], record["direction"], record["status"], record["text"],
                    record["content_uri"], record["author_name"], record["sent_at"], record["raw_json"], now,
                ),
            )
            inserted += max(0, cursor.rowcount)
        latest = max(records, key=lambda item: item["sent_at"]) if records else None
        await db.execute(
            """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
               phone_hash=excluded.phone_hash,
               contact_name=CASE WHEN excluded.contact_name<>'' THEN excluded.contact_name ELSE wazzup_chats.contact_name END,
               last_message_at=CASE WHEN excluded.last_message_at<>'' THEN excluded.last_message_at ELSE wazzup_chats.last_message_at END,
               last_message_preview=CASE WHEN excluded.last_message_preview<>'' THEN excluded.last_message_preview ELSE wazzup_chats.last_message_preview END,
               updated_at=excluded.updated_at""",
            (
                candidate["channel_id"], candidate["chat_type"], candidate["chat_id"], _phone_hash(phone),
                candidate["contact_name"], latest["sent_at"] if latest else "", latest["text"][:500] if latest else "", now, now,
            ),
        )
        await db.commit()
    finally:
        await db.close()
    return inserted


async def _import_wazzup_history(
    device: dict[str, Any],
    channel: dict[str, str],
    phone: str,
    *,
    name: str = "",
    identity: dict[str, str] | None = None,
    offset: int = 0,
    known_chat_id: str = "",
) -> dict[str, Any]:
    iframe = await _wazzup_request(
        "POST",
        "/iframe",
        {"user": {"id": device["wazzup_user_id"], "name": device["admin_name"]}, "scope": "global"},
    )
    link = _clean(iframe.get("url"), 5000) if isinstance(iframe, dict) else ""
    parsed = urlsplit(link)
    token = _clean(parse_qs(parsed.query).get("token", [""])[0], 5000)
    if parsed.scheme != "https" or parsed.hostname != "app.wazzup24.com" or not token:
        raise RuntimeError("invalid iframe session")
    headers = {"Authorization": token, "Accept": "application/json"}
    candidate: dict[str, str] | None = {
        "channel_id": channel["channel_id"],
        "chat_type": channel["transport"],
        "chat_id": _clean(known_chat_id, 250),
        "contact_name": _clean(name, 200),
    } if known_chat_id else None
    scanned_chats = 0
    chat_rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        for chat_offset in range(0, HISTORY_MAX_CHATS, HISTORY_PAGE_SIZE):
            if candidate:
                break
            response = await client.get(
                f"{WAZZUP_APP_API}/chats",
                headers=headers,
                params={"limit": HISTORY_PAGE_SIZE, "offset": chat_offset, "filterChannels": ""},
            )
            if response.status_code != 200:
                raise RuntimeError(f"history chats HTTP {response.status_code}")
            body = response.json()
            rows = body.get("data") if isinstance(body, dict) else []
            if isinstance(rows, list):
                scanned_chats += len(rows)
                chat_rows.extend(row for row in rows if isinstance(row, dict))
            candidate = _history_chat_candidate(
                rows,
                channel["channel_id"],
                channel["transport"],
                phone,
                identity,
            )
            if candidate or not isinstance(rows, list) or len(rows) < HISTORY_PAGE_SIZE:
                break
        if not candidate and name:
            routes = [
                route
                for row in chat_rows
                if _same_person_name(row.get("contactName"), name)
                and (route := _history_chat_route(row, channel["channel_id"], channel["transport"]))
            ][:HISTORY_IDENTITY_PROBES]
            for route in routes:
                response = await client.get(
                    f"{WAZZUP_APP_API}/messages",
                    headers=headers,
                    params={
                        "limit": HISTORY_PAGE_SIZE,
                        "offset": 0,
                        "chatType": route["chat_type"],
                        "chatId": route["chat_id"],
                    },
                )
                if response.status_code != 200:
                    continue
                body = response.json()
                page_rows = body.get("messages") if isinstance(body, dict) else []
                if _history_message_matches_phone(page_rows, phone):
                    candidate = route
                    break
        if not candidate:
            status = _missing_history_status(scanned_chats)
            await _record_history_sync(channel["channel_id"], phone, status, 0, success=True)
            return {"status": status, "imported": 0, "complete": False}
        response = await client.get(
            f"{WAZZUP_APP_API}/messages",
            headers=headers,
            params={
                "limit": CONVERSATION_PAGE_SIZE,
                "offset": max(0, offset),
                "chatType": candidate["chat_type"],
                "chatId": candidate["chat_id"],
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"history messages HTTP {response.status_code}")
        body = response.json()
        page_rows = body.get("messages") if isinstance(body, dict) else []
        if not isinstance(page_rows, list):
            raise RuntimeError("invalid history response")
        messages = [row for row in page_rows if isinstance(row, dict)]
        complete = len(page_rows) < CONVERSATION_PAGE_SIZE
        imported = await _store_history(candidate, phone, messages)
        await _record_history_sync(channel["channel_id"], phone, "imported", imported, success=True)
        return {"status": "imported", "imported": imported, "complete": complete}


async def _conversation_rows(
    channel_id: str,
    transport: str,
    phone: str,
    limit: int = 150,
    *,
    exact_chat_id: str = "",
    offset: int = 0,
) -> tuple[str, bool, list[dict[str, Any]]]:
    limit = max(1, min(int(limit), 500))
    offset = max(0, min(int(offset), 100_000))
    phone_hash = _phone_hash(phone)
    digits = phone[1:] if phone else ""
    db = await _connect()
    try:
        if exact_chat_id:
            chat = await (
                await db.execute(
                    """SELECT chat_id FROM wazzup_chats
                       WHERE channel_id=? AND chat_type=? AND chat_id=? LIMIT 1""",
                    (channel_id, transport, exact_chat_id),
                )
            ).fetchone()
        else:
            chat = await (
                await db.execute(
                    """SELECT chat_id FROM wazzup_chats WHERE channel_id=? AND chat_type=?
                       AND (phone_hash=? OR chat_id=?) ORDER BY updated_at DESC,id DESC LIMIT 1""",
                    (channel_id, transport, phone_hash, digits),
                )
            ).fetchone()
        chat_id = _clean(chat["chat_id"], 250) if chat else ""
        if exact_chat_id:
            rows = await (
                await db.execute(
                    _conversation_message_query("channel_id=? AND chat_type=? AND chat_id=?"),
                    (channel_id, transport, exact_chat_id, limit, offset),
                )
            ).fetchall()
        elif chat_id:
            rows = await (
                await db.execute(
                    _conversation_message_query("channel_id=? AND chat_type=? AND (chat_id=? OR phone_hash=?)"),
                    (channel_id, transport, chat_id, phone_hash, limit, offset),
                )
            ).fetchall()
        else:
            rows = await (
                await db.execute(
                    _conversation_message_query("channel_id=? AND chat_type=? AND phone_hash=?"),
                    (channel_id, transport, phone_hash, limit, offset),
                )
            ).fetchall()
    finally:
        await db.close()
    return chat_id, bool(chat), [_message_view(row) for row in reversed(rows)]


async def _has_conversation(channel_id: str, transport: str, phone: str) -> bool:
    phone_hash = _phone_hash(phone)
    digits = phone[1:] if phone else ""
    if not phone_hash and not digits:
        return False
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT 1 FROM wazzup_chats WHERE channel_id=? AND chat_type=?
               AND (phone_hash=? OR chat_id=?) LIMIT 1""",
            (channel_id, transport, phone_hash, digits),
        )).fetchone()
        return bool(row)
    finally:
        await db.close()


async def _inbox_thread_context(
    channel_id: str,
    chat_type: str,
    chat_id: str,
    channels: list[dict[str, str]],
    device: dict[str, Any],
) -> dict[str, str]:
    if (
        not channel_id
        or chat_type not in CHAT_TRANSPORTS
        or not chat_id
        or not any(row["channel_id"] == channel_id and row["transport"] == chat_type for row in channels)
    ):
        raise HTTPException(400, "Диалог недоступен")
    db = await _connect()
    try:
        row = await (
            await db.execute(
                """SELECT c.channel_id,c.chat_type,c.chat_id,c.phone_hash,c.contact_name,c.responsible_admin_id,
                          l.phone,l.getcourse_user_id,l.name AS link_name,
                          (SELECT m.raw_json FROM wazzup_messages m
                           WHERE m.channel_id=c.channel_id AND m.chat_type=c.chat_type AND m.chat_id=c.chat_id
                             AND m.direction='incoming' ORDER BY m.sent_at DESC,m.id DESC LIMIT 1) AS incoming_raw
                   FROM wazzup_chats c LEFT JOIN client_links l ON l.phone_hash=c.phone_hash
                   WHERE c.channel_id=? AND c.chat_type=? AND c.chat_id=? LIMIT 1""",
                (channel_id, chat_type, chat_id),
            )
        ).fetchone()
    finally:
        await db.close()
    if not row:
        raise HTTPException(404, "Диалог не найден")
    item = dict(row)
    if (
        _clean(device.get("admin_role"), 20) == "employee"
        and int(item.get("responsible_admin_id") or 0) != int(device["admin_id"])
    ):
        raise HTTPException(403, "Диалог закреплён за другим сотрудником")
    direct_provider = "vk" if channel_id.startswith("vk:") else TELEGRAM_PROVIDER if channel_id.startswith("telegram-personal:") else ""
    if direct_provider:
        link = await _external_link(peer_id=chat_id, provider=direct_provider)
        if not link:
            raise HTTPException(404, "Диалог не связан с GetCourse")
        return {
            "channel_id": channel_id,
            "chat_type": "vk" if direct_provider == "vk" else "telegram",
            "chat_id": chat_id,
            "phone_hash": "",
            "phone": _normalize_phone(link.get("phone")),
            "name": _clean(link.get("name") or item.get("contact_name"), 200),
            "getcourse_user_id": _clean(link.get("getcourse_user_id"), 200),
        }
    phone = _normalize_phone(item.get("phone"))
    if not phone:
        try:
            raw = json.loads(_clean(item.get("incoming_raw"), 50_000) or "{}")
        except json.JSONDecodeError:
            raw = {}
        phone = _message_contact_phone(raw, chat_type, chat_id)
    return {
        "channel_id": channel_id,
        "chat_type": chat_type,
        "chat_id": chat_id,
        "phone_hash": _clean(item.get("phone_hash"), 100),
        "phone": phone,
        "name": _clean(item.get("link_name") or item.get("contact_name"), 200),
        "getcourse_user_id": _clean(item.get("getcourse_user_id"), 200),
    }


async def _resolved_client_identity(data: dict[str, Any], phone: str) -> dict[str, str]:
    page_kind, entity_id = _page_context(data.get("source_url"))
    return await resolve_client_identity(
        phone=phone,
        email=_clean(data.get("email"), 320),
        getcourse_user_id=entity_id if page_kind == "user" else "",
    )


def _channel_send_state(channel: dict[str, str], has_chat: bool) -> tuple[bool, str]:
    if has_chat:
        return True, ""
    if channel.get("channel_transport") == "telegram":
        return False, "Клиент ещё не написал Telegram-боту."
    return True, ""


def _first_message_recipient(
    channel: dict[str, str], transport: str, phone: str, identity: dict[str, Any],
) -> dict[str, str]:
    digits = _normalize_phone(phone).removeprefix("+")
    if transport in {"whatsapp", "viber"}:
        return {"chatId": digits}
    if transport == "max":
        return {"phone": digits}
    if transport == "telegram" and channel.get("channel_transport") == "tgapi":
        username = _clean(identity.get("telegram_username"), 200).lstrip("@")
        return {"username": username} if username else {"phone": digits}
    if transport == "telegram" and channel.get("channel_transport") == "telegram":
        raise HTTPException(409, "Клиент ещё не написал Telegram-боту.")
    raise HTTPException(
        409,
        "Для этого канала Wazzup нужен ID чата из входящего сообщения; по одному номеру начать диалог нельзя.",
    )


def _send_failure_notice(transport: str, detail: str) -> str:
    if transport == "max" and "CHANNEL_MAX_PHONE_NOT_OCCUPIED" in detail:
        return "У клиента не найден MAX. Сообщение не доставлено."
    return ""


def _salebot_messages(payload: Any) -> list[dict[str, Any]]:
    rows = payload
    if isinstance(payload, dict):
        rows = payload.get("result") or payload.get("messages") or payload.get("history") or payload.get("data") or []
    if not isinstance(rows, list):
        return []
    result: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        replica = row.get("client_replica")
        if isinstance(replica, str):
            replica = replica.strip().lower() in {"1", "true", "yes"}
        outside = row.get("message_from_outside")
        if outside not in (None, ""):
            try:
                incoming = int(outside) > 0
            except (TypeError, ValueError):
                incoming = bool(replica)
        else:
            incoming = bool(replica)
        attachments: list[dict[str, str]] = []
        raw_attachments = row.get("attachments") if isinstance(row.get("attachments"), list) else []
        for attachment in raw_attachments:
            if isinstance(attachment, str):
                url, content_type = attachment, ""
            elif isinstance(attachment, dict):
                url = attachment.get("attachment_url") or attachment.get("url") or attachment.get("link") or attachment.get("src") or attachment.get("file")
                content_type = attachment.get("attachment_type") or attachment.get("type") or attachment.get("mime") or ""
            else:
                continue
            if _clean(url, 4000).startswith("https://"):
                attachments.append({"content_uri": _clean(url, 4000), "content_type": _clean(content_type, 200), "filename": ""})
        if not attachments:
            for key in ("attachment_url", "attachment", "file", "media", "image", "photo", "video"):
                value = row.get(key)
                url = value if isinstance(value, str) else value.get("url") if isinstance(value, dict) else ""
                if _clean(url, 4000).startswith("https://"):
                    attachments.append({"content_uri": _clean(url, 4000), "content_type": _clean(row.get("attachment_type"), 200), "filename": ""})
                    break
        result.append({
            "external_id": f"salebot:{_clean(row.get('id') or row.get('message_id') or index, 200)}",
            "direction": "incoming" if incoming else "outgoing",
            "status": "delivered" if row.get("delivered", True) else "sent",
            "text": _clean(row.get("text") or row.get("message"), 20_000),
            "content_uri": attachments[0]["content_uri"] if attachments else "",
            "attachments": attachments,
            "author_name": _clean(row.get("name") or row.get("author_name"), 200),
            "sent_at": _message_time(row.get("created_at") or row.get("date") or row.get("time")),
        })
    return sorted(result, key=lambda item: item["sent_at"])


async def _salebot_history(client_id: str) -> list[dict[str, Any]]:
    cached = _salebot_history_cache.get(client_id)
    if cached and cached[0] > time.monotonic():
        return list(cached[1])
    key = _salebot_key()
    if not key:
        raise HTTPException(503, "SaleBot не настроен")
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(f"{SALEBOT_API_BASE}/{key}/get_history", params={"client_id": client_id, "limit": 2000})
    if response.status_code >= 400:
        raise HTTPException(502, "SaleBot не отдал историю. Повторите позже.")
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(502, "SaleBot вернул некорректную историю") from exc
    if isinstance(payload, dict) and payload.get("error"):
        raise HTTPException(502, "SaleBot не отдал историю. Повторите позже.")
    messages = _salebot_messages(payload)
    _salebot_history_cache[client_id] = (time.monotonic() + SALEBOT_HISTORY_CACHE_SECONDS, messages)
    while len(_salebot_history_cache) > DIRECT_HISTORY_CACHE_LIMIT:
        _salebot_history_cache.pop(next(iter(_salebot_history_cache)))
    return list(messages)


async def _salebot_send(client_id: str, text: str, attachment_url: str = "", attachment_type: str = "") -> dict[str, Any]:
    key = _salebot_key()
    if not key:
        raise HTTPException(503, "SaleBot не настроен")
    body: dict[str, str] = {"client_id": client_id}
    if text:
        body["message"] = text
    if attachment_url:
        if not attachment_url.startswith("https://"):
            raise HTTPException(400, "Вложение должно иметь HTTPS-ссылку")
        body["attachment_url"] = attachment_url
        body["attachment_type"] = attachment_type or "document"
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(f"{SALEBOT_API_BASE}/{key}/message", json=body)
    if response.status_code >= 400:
        raise HTTPException(502, "SaleBot не принял сообщение")
    _salebot_history_cache.pop(client_id, None)
    try:
        return response.json()
    except ValueError:
        return {"ok": True}


@router.post("/widget/conversation")
async def widget_conversation(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        channel_id = _clean(data.get("channel_id"), 200)
        transport = _clean(data.get("transport"), 40).lower()
        provider = _clean(data.get("provider"), 40).lower() or (
            "vk" if channel_id.startswith("vk:") else TELEGRAM_PROVIDER if channel_id.startswith("telegram-personal:") else SALEBOT_PROVIDER if channel_id.startswith("salebot:") else "wazzup"
        )
        if not channel_id or transport not in CHAT_TRANSPORTS:
            raise HTTPException(400, "Канал не указан")
        active_channels = await _all_channels()
        thread_fields = (
            _clean(data.get("thread_channel_id"), 200),
            _clean(data.get("thread_chat_type"), 40).lower(),
            _clean(data.get("thread_chat_id"), 250),
        )
        thread = None
        if any(thread_fields):
            thread = await _inbox_thread_context(*thread_fields, active_channels, device)
        offset = max(0, min(int(data.get("offset") or 0), 100_000))
        if provider == SALEBOT_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            link = await _provider_card_link(data, mode, device, provider)
            client_id = _clean(link.get("external_user_id"), 200)
            if not client_id:
                raise HTTPException(404, "SaleBot ID не найден. Нужен salebot_id в карточке или utm_term.")
            all_messages = await _salebot_history(client_id)
            end = max(0, len(all_messages) - offset)
            start = max(0, end - CONVERSATION_PAGE_SIZE)
            messages = all_messages[start:end]
            has_more = start > 0
            return _widget_response(request, {
                "ok": True, "channel": channel, "chat_id": client_id, "has_chat": bool(all_messages),
                "phone": _normalize_phone(link.get("phone")), "messages": messages,
                "history_complete": not has_more, "history_status": "imported",
                "can_send": True, "send_reason": "", "provider": provider,
                "offset": offset, "next_offset": offset + len(messages), "has_more": has_more,
            })
        if provider == "vk":
            channel = await _requested_channel(channel_id, transport, provider)
            same_provider = bool(thread and thread["channel_id"].startswith("vk:"))
            peer_id = _clean((thread or {}).get("chat_id"), 200) if same_provider else ""
            card_link: dict[str, Any] = {}
            peer_id = peer_id or _clean(data.get("vk_id"), 200)
            if not peer_id and thread:
                peer_id = _clean((await _external_link_for_identity(
                    "vk", phone=thread.get("phone", ""), gc_id=thread.get("getcourse_user_id", ""),
                )).get("external_user_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, "vk")
                peer_id = _clean(card_link.get("external_user_id"), 200)
            link = await _external_link(peer_id=peer_id) or card_link
            if not peer_id or not link:
                raise HTTPException(404, "Диалог VK не найден")
            _, has_more = await _load_vk_history(peer_id, offset=offset, identity=link)
            chat_id, has_chat, messages = await _conversation_rows(
                channel_id, "vk", "", CONVERSATION_PAGE_SIZE, exact_chat_id=peer_id, offset=offset,
            )
            if has_chat:
                await _mark_thread_read(int(device["id"]), channel_id, "vk", peer_id)
            return _widget_response(request, {
                "ok": True, "channel": channel, "chat_id": chat_id, "has_chat": has_chat,
                "phone": _normalize_phone(link.get("phone")),
                "messages": messages, "history_complete": not has_more,
                "history_status": "imported" if has_chat else "not_started",
                "can_send": True, "send_reason": "",
                "getcourse_user_id": _clean(link.get("getcourse_user_id"), 200),
                "provider": "vk", "offset": offset,
                "next_offset": offset + (len(messages) or (CONVERSATION_PAGE_SIZE if has_more else 0)), "has_more": has_more,
            })
        if provider == TELEGRAM_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            same_provider = bool(thread and thread["channel_id"].startswith("telegram-personal:"))
            peer_id = _clean((thread or {}).get("chat_id"), 200) if same_provider else ""
            card_link: dict[str, Any] = {}
            peer_id = peer_id or _clean(data.get("telegram_id"), 200)
            if not peer_id and thread:
                peer_id = _clean((await _external_link_for_identity(
                    TELEGRAM_PROVIDER, phone=thread.get("phone", ""), gc_id=thread.get("getcourse_user_id", ""),
                )).get("external_user_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, TELEGRAM_PROVIDER, allow_phone_import=True)
                peer_id = _clean(card_link.get("external_user_id"), 200)
            if card_link.get("pending"):
                raise HTTPException(504, "Telegram не ответил. Повторите.")
            link = await _external_link(peer_id=peer_id, provider=TELEGRAM_PROVIDER) or card_link
            if not peer_id or not link:
                raise HTTPException(404, "Диалог Telegram не найден")
            _, cached_chat, _ = await _conversation_rows(
                channel_id, "telegram", "", 1, exact_chat_id=peer_id,
            )
            history_status = "imported"
            has_more = False
            if offset:
                _, has_more = await _sync_telegram_history(peer_id, offset=offset, identity=link)
            elif not cached_chat:
                _schedule_telegram_history(peer_id, link)
                history_status = "syncing"
            chat_id, has_chat, messages = await _conversation_rows(
                channel_id, "telegram", "", CONVERSATION_PAGE_SIZE, exact_chat_id=peer_id, offset=offset,
            )
            if has_chat:
                await _mark_thread_read(int(device["id"]), channel_id, "telegram", peer_id)
            return _widget_response(request, {
                "ok": True, "channel": channel, "chat_id": chat_id, "has_chat": has_chat,
                "phone": _normalize_phone(link.get("phone")), "messages": messages,
                "history_complete": not has_more and history_status != "syncing", "history_status": history_status,
                "can_send": True, "send_reason": "",
                "getcourse_user_id": _clean(link.get("getcourse_user_id"), 200),
                "provider": TELEGRAM_PROVIDER,
                "offset": offset, "next_offset": offset + (len(messages) or (CONVERSATION_PAGE_SIZE if has_more else 0)), "has_more": has_more,
            })
        phone = thread["phone"] if thread else _normalize_phone(data.get("phone"))
        if not phone and not thread:
            raise HTTPException(400, "Телефон не указан")
        channel = await _requested_channel(channel_id, transport, provider)
        exact_thread = bool(
            thread
            and thread["channel_id"] == channel_id
            and thread["chat_type"] == transport
        )
        page_kind, entity_id = _page_context(data.get("source_url")) if not thread else ("inbox", thread["chat_id"])
        if not thread and page_kind == "user":
            await _remember_client_link(
                phone,
                getcourse_user_id=entity_id,
                name=_clean(data.get("name"), 200),
                source="getcourse-card",
            )
        history: dict[str, Any] = {"status": "imported", "imported": 0, "complete": True}
        if exact_thread:
            chat_id, has_chat, messages = await _conversation_rows(
                channel_id,
                transport,
                phone,
                CONVERSATION_PAGE_SIZE,
                exact_chat_id=thread["chat_id"],
                offset=offset,
            )
        else:
            if not phone:
                raise HTTPException(409, "Для этого канала нужен телефон клиента")
            identity = await _resolved_client_identity(data, phone)
            chat_id, has_chat, messages = await _conversation_rows(
                channel_id, transport, phone, CONVERSATION_PAGE_SIZE, offset=offset,
            )
            history = await _history_sync_info(channel_id, phone)
            should_import = not messages and (offset > 0 or await _history_sync_due(channel_id, phone))
            if should_import:
                try:
                    history = await _import_wazzup_history(
                        device,
                        channel,
                        phone,
                        name=_clean((thread or {}).get("name") or data.get("name"), 200),
                        identity=identity,
                        offset=offset,
                        known_chat_id=chat_id,
                    )
                except Exception:
                    await _record_history_sync(channel_id, phone, "error", 0, success=False)
                    history = {"status": "error", "imported": 0, "complete": False}
                    _log("warning", "Wazzup history read failed channel=%s", channel_id)
                chat_id, has_chat, messages = await _conversation_rows(
                    channel_id, transport, phone, CONVERSATION_PAGE_SIZE, offset=offset,
                )
        if has_chat:
            await _mark_thread_read(int(device["id"]), channel_id, transport, chat_id)
            owner_id = await _responsible_admin_id(data, mode, device) if not thread else None
            await _assign_client_threads(owner_id, phone=phone)
        can_send, send_reason = _channel_send_state(channel, has_chat)
        has_more = len(messages) >= CONVERSATION_PAGE_SIZE or not bool(history.get("complete", True))
        return _widget_response(
            request,
            {
                "ok": True,
                "channel": channel,
                "chat_id": chat_id,
                "has_chat": has_chat,
                "phone": phone,
                "messages": messages,
                "history_complete": not has_more,
                "history_status": history.get("status", "pending"),
                "can_send": can_send,
                "send_reason": send_reason,
                "getcourse_user_id": _clean((thread or {}).get("getcourse_user_id"), 200),
                "offset": offset,
                "next_offset": offset + (len(messages) or (CONVERSATION_PAGE_SIZE if has_more else 0)),
                "has_more": has_more,
            },
        )
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup conversation load failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось загрузить переписку"}, 500)


@router.post("/widget/send")
async def widget_send(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    device: dict[str, Any] | None = None
    phone = ""
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(request, "getcourse-wazzup-send", limit=120, window_seconds=3600, subject=str(device["id"]))
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        channel_id = _clean(data.get("channel_id"), 200)
        transport = _clean(data.get("transport"), 40).lower()
        provider = _clean(data.get("provider"), 40).lower() or (
            "vk" if channel_id.startswith("vk:") else TELEGRAM_PROVIDER if channel_id.startswith("telegram-personal:") else SALEBOT_PROVIDER if channel_id.startswith("salebot:") else "wazzup"
        )
        message_text = _clean(data.get("text"), 4000)
        source_url = _clean(data.get("source_url"), 2000)
        attachment_url = _clean(data.get("attachment_url"), 4000)
        attachment_type = _clean(data.get("attachment_type"), 100)
        if not message_text and not attachment_url:
            raise HTTPException(400, "Введите сообщение")
        if re.search(r"\{\{\s*[a-zA-Z0-9_.:-]+\s*\}\}", message_text):
            raise HTTPException(400, "Подставьте все переменные шаблона")
        if not channel_id or transport not in CHAT_TRANSPORTS:
            raise HTTPException(400, "Канал не указан")
        active_channels = await _all_channels()
        thread_fields = (
            _clean(data.get("thread_channel_id"), 200),
            _clean(data.get("thread_chat_type"), 40).lower(),
            _clean(data.get("thread_chat_id"), 250),
        )
        thread = None
        if any(thread_fields):
            thread = await _inbox_thread_context(*thread_fields, active_channels, device)
        if provider == SALEBOT_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            link = await _provider_card_link(data, mode, device, provider)
            client_id = _clean(link.get("external_user_id"), 200)
            if not client_id:
                raise HTTPException(404, "SaleBot ID не найден. Нужен salebot_id в карточке или utm_term.")
            await _salebot_send(client_id, message_text, attachment_url, attachment_type)
            await _audit("send_message", "ok", admin_id=device["admin_id"], device_id=device["id"], entity_id=client_id)
            return _widget_response(request, {
                "ok": True, "channel": channel,
                "message": {"external_id": f"salebot:local:{secrets.token_hex(8)}", "direction": "outgoing", "status": "sent", "text": message_text, "content_uri": attachment_url, "author_name": device["admin_name"], "sent_at": _iso()},
            })
        if provider == "vk":
            channel = await _requested_channel(channel_id, transport, provider)
            page_kind, entity_id = _page_context(source_url) if not thread else ("inbox", thread["chat_id"])
            peer_id = _clean((thread or {}).get("chat_id"), 200)
            card_link: dict[str, Any] = {}
            peer_id = peer_id or _clean(data.get("vk_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, "vk")
                peer_id = _clean(card_link.get("external_user_id"), 200)
            stored_link = await _external_link(peer_id=peer_id)
            link = stored_link or card_link
            if not peer_id or not link:
                raise HTTPException(404, "Диалог VK не найден")
            result = await _vk_request("messages.send", {
                "group_id": _vk_group_id(), "peer_id": peer_id,
                "random_id": secrets.randbelow(2_000_000_000) + 1, "message": message_text,
            })
            message_id = _clean(result, 200) if not isinstance(result, dict) else _clean(result.get("message_id") or result.get("conversation_message_id"), 200)
            now_stamp = int(_now_dt().timestamp())
            await _store_vk_messages(peer_id, [{
                "id": message_id or f"local-{secrets.token_hex(12)}", "from_id": f"-{_vk_group_id()}",
                "peer_id": peer_id, "date": now_stamp, "text": message_text, "attachments": [],
            }], link)
            await _audit("send_message", "ok", admin_id=device["admin_id"], device_id=device["id"], page_kind=page_kind, entity_id=entity_id, phone=link.get("phone", ""))
            return _widget_response(request, {
                "ok": True,
                "message": {"external_id": f"vk:{_vk_group_id()}:{peer_id}:{message_id}", "direction": "outgoing", "status": "delivered", "text": message_text, "content_uri": "", "author_name": "Сообщество", "sent_at": _iso()},
                "channel": channel,
            })
        if provider == TELEGRAM_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            page_kind, entity_id = _page_context(source_url) if not thread else ("inbox", thread["chat_id"])
            peer_id = _clean((thread or {}).get("chat_id"), 200)
            card_link: dict[str, Any] = {}
            peer_id = peer_id or _clean(data.get("telegram_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, TELEGRAM_PROVIDER, allow_phone_import=True)
                peer_id = _clean(card_link.get("external_user_id"), 200)
            if card_link.get("pending"):
                raise HTTPException(504, "Telegram не ответил. Повторите.")
            stored_link = await _external_link(peer_id=peer_id, provider=TELEGRAM_PROVIDER)
            link = stored_link or card_link
            if not peer_id or not link:
                raise HTTPException(404, "Диалог Telegram не найден")
            message = await _telegram_send_text(peer_id, message_text, **({} if stored_link else {"identity": link}))
            await _audit(
                "send_message", "ok", admin_id=device["admin_id"], device_id=device["id"],
                page_kind=page_kind, entity_id=entity_id, phone=link.get("phone", ""),
            )
            return _widget_response(request, {"ok": True, "message": message, "channel": channel})
        phone = thread["phone"] if thread else _normalize_phone(data.get("phone"))
        if thread:
            page_kind, entity_id = "inbox", thread["chat_id"]
        else:
            page_kind, entity_id = _page_context(source_url)
            if not phone:
                raise HTTPException(400, "Телефон не указан")
            if mode != "amocrm" and (not page_kind or not source_url.startswith(_allowed_origin() + "/")):
                raise HTTPException(400, "Откройте карточку GetCourse с заполненным телефоном")
        channel = await _requested_channel(channel_id, transport, provider)
        exact_thread = bool(
            thread
            and thread["channel_id"] == channel_id
            and thread["chat_type"] == transport
        )
        identity = await _resolved_client_identity(data, phone) if phone else {}
        if exact_thread:
            chat_id, has_chat = thread["chat_id"], True
        else:
            if not phone:
                raise HTTPException(409, "Для этого канала нужен телефон клиента")
            chat_id, has_chat, _ = await _conversation_rows(channel_id, transport, phone, 1)
        crm_message_id = f"nexus-{secrets.token_hex(16)}"
        message_payload: dict[str, Any] = {
            "channelId": channel_id,
            "chatType": transport,
            "text": message_text,
            "crmUserId": device["wazzup_user_id"],
            "crmMessageId": crm_message_id,
        }
        if has_chat:
            message_payload["chatId"] = chat_id
        else:
            message_payload.update(_first_message_recipient(channel, transport, phone, identity))
        failure_notice = ""
        try:
            result = await _wazzup_request("POST", "/message", message_payload)
        except HTTPException as exc:
            failure_notice = _send_failure_notice(transport, str(exc.detail))
            if not failure_notice:
                raise
            result = {}
        external_id = ""
        if isinstance(result, dict):
            external_id = _clean(result.get("messageId") or result.get("id"), 250)
            if not external_id and isinstance(result.get("data"), dict):
                external_id = _clean(result["data"].get("messageId") or result["data"].get("id"), 250)
        external_id = external_id or f"local-{crm_message_id}"
        response_chat_id = _clean(result.get("chatId"), 250) if isinstance(result, dict) else ""
        if not response_chat_id and isinstance(result, dict) and isinstance(result.get("data"), dict):
            response_chat_id = _clean(result["data"].get("chatId"), 250)
        chat_id = response_chat_id or (chat_id if has_chat else "")
        now = _iso()
        stored_phone_hash = _phone_hash(phone) or _clean((thread or {}).get("phone_hash"), 100)
        db = await _connect()
        try:
            delivery_status = "failed" if failure_notice else "accepted"
            await db.execute(
                """INSERT OR IGNORE INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,'outgoing',?,?,'',?,?,?,?)""",
                (external_id, channel_id, transport, chat_id, stored_phone_hash, delivery_status,
                 message_text, device["admin_name"], now,
                 json.dumps({"error": failure_notice}, ensure_ascii=False) if failure_notice else "", now),
            )
            if chat_id:
                await db.execute(
                    """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
                       phone_hash=excluded.phone_hash,last_message_at=excluded.last_message_at,
                       last_message_preview=excluded.last_message_preview,updated_at=excluded.updated_at""",
                    (channel_id, transport, chat_id, stored_phone_hash, _clean((thread or {}).get("name") or data.get("name"), 200), now, message_text[:500], now, now),
                )
            await db.commit()
        finally:
            await db.close()
        if not thread:
            await _assign_client_threads(await _responsible_admin_id(data, mode, device), phone=phone)
        await _audit(
            "send_message", "not_delivered" if failure_notice else "ok",
            admin_id=device["admin_id"], device_id=device["id"], page_kind=page_kind,
            entity_id=entity_id, phone=phone, error=failure_notice,
        )
        return _widget_response(
            request,
            {
                "ok": True,
                "sent": not failure_notice,
                "notice": failure_notice,
                "message": {"external_id": external_id, "direction": "outgoing", "status": delivery_status, "text": message_text, "content_uri": "", "author_name": device["admin_name"], "sent_at": now},
                "channel": channel,
            },
        )
    except HTTPException as exc:
        if device:
            await _audit("send_message", "error", admin_id=device["admin_id"], device_id=device["id"], phone=phone, error=str(exc.detail))
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup send failed")
        if device:
            await _audit("send_message", "error", admin_id=device["admin_id"], device_id=device["id"], phone=phone, error="internal")
        return _widget_response(request, {"ok": False, "error": "Не удалось отправить сообщение"}, 500)


@router.post("/widget/staff-sync")
async def widget_staff_sync(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        rows = data.get("staff")
        if not isinstance(rows, list) or not rows:
            raise HTTPException(400, "Сотрудники на странице не найдены")
        items = []
        seen: set[str] = set()
        for raw in rows[:100]:
            if not isinstance(raw, dict):
                continue
            gc_id = re.sub(r"\D+", "", _clean(raw.get("id"), 40))
            name = _clean(raw.get("name"), 150)
            if gc_id and name and gc_id not in seen:
                seen.add(gc_id)
                items.append({"id": f"getcourse-{gc_id}", "name": name, "phone": _clean(raw.get("phone"), 50)})
        if not items:
            raise HTTPException(400, "Не удалось прочитать сотрудников GetCourse")
        count = await _upsert_admins(items, f"device:{device['id']}")
        await _audit("sync_getcourse_staff", "ok", admin_id=device["admin_id"], device_id=device["id"], error=f"count:{count}")
        return _widget_response(request, {"ok": True, "synced": count})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
