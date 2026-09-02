from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import importlib.util
import inspect
import ipaddress
import json
import os
import re
import secrets
import socket
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, parse_qsl, quote, unquote, urlsplit, urlunsplit

import aiosqlite
import httpx
from jose import jwt
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, Response
from starlette.requests import ClientDisconnect

from orchestrator.auth import (
    _read_env_values,
    _write_env_values,
    can_access_module,
    enforce_rate_limit,
    require_admin,
    verify_token_from_request,
)
from orchestrator.telegram_proxy import (
    httpx_client_kwargs,
    telegram_bot_api_base,
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
MAX_WIDGET_IMAGE_BYTES = 8 * 1024 * 1024
WIDGET_IMAGE_RETENTION_DAYS = 30
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
    "vk", "viber", "instagram", "avito", "cian", "salebot", "email",
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
EMAIL_PROVIDER = "email"
SALEBOT_API_BASE = "https://chatter.salebot.pro/api"
SALEBOT_PROFILE_BASE = "https://salebot.pro/projects/397724/clients"
SALEBOT_HISTORY_CACHE_SECONDS = 120
SALEBOT_ATTACHMENT_TTL_SECONDS = 60 * 60
SALEBOT_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
SALEBOT_SAFE_MEDIA_TYPES = {
    "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp",
    "audio/mpeg", "audio/mp4", "audio/ogg", "audio/wav", "audio/webm",
    "video/mp4", "video/webm", "video/quicktime", "application/pdf",
}
SALEBOT_CALLBACK_MARKERS = {"instagram"}
TELEGRAM_SESSION_ENV_KEY = "NEXUS_MESSENGER_WIDGET_TELEGRAM_SESSION_FILE"
LEGACY_TELEGRAM_SESSION_ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_TELEGRAM_SESSION_FILE"
TELEGRAM_SYNC_SECONDS = 30
TELEGRAM_DIALOG_LIMIT = 500
TELEGRAM_BACKGROUND_DIALOG_LIMIT = 50
TELEGRAM_HISTORY_PAGE_SIZE = CONVERSATION_PAGE_SIZE
TELEGRAM_HISTORY_CACHE_SECONDS = 60
# Internal/personal Telegram accounts must never leak into the shared sales
# widget.  Stable peer ids are the authoritative boundary; username and phone
# aliases stop the same accounts before a live lookup has resolved an id.
TELEGRAM_HIDDEN_PEER_IDS = frozenset({"943871493", "328268937"})
TELEGRAM_HIDDEN_USERNAMES = frozenset({"papaproduser", "rareru"})
TELEGRAM_HIDDEN_PHONES = frozenset({"+79997301959"})
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
    {"key": "yclid", "label": "yclid"},
    {"key": "ym_uid", "label": "ym_uid"},
    {"key": "conversation_id", "label": "conversation_id"},
]
AUTO_MARKUP_DEFAULT_DOMAINS = "club.sobakovod.pro;sobakovod.pro;start.bizon365.ru"
AUTO_MARKUP_DEFAULT_TAIL = "?utm_term={{utm.term}}&utm_source={{utm.source}}&utm_medium={{utm.medium}}&utm_campaign={{utm.campaign}}&utm_content={{utm.content}}&param1={{ym_uid}}&param2={{conversation_id}}"
AUTO_MARKUP_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
NOTIFY_TELEGRAM_TOKEN_ENV_KEY = "NEXUS_MESSENGER_NOTIFY_TELEGRAM_BOT_TOKEN"
NOTIFY_TELEGRAM_USERNAME = "attackpng_notify_bot"
# A short debounce produced several amoCRM tasks while a client was still
# typing.  Keep one rolling five-minute batch per dialog instead.
NOTIFY_BATCH_SECONDS = 5 * 60
WIDGET_OPERATION_ACTIONS = (
    "widget_getcourse_access", "widget_trial_issue", "widget_trial_revoke",
)
NOTIFY_PAIRING_TTL_MINUTES = 10
NOTIFY_MAX_ATTEMPTS = 8
NOTIFY_EVENT_RETENTION_DAYS = 30
NOTIFY_RETRY_SECONDS = (30, 120, 600, 1800, 3600, 10800, 21600, 43200)
AMO_TASK_SOURCES = ("max", "vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER, EMAIL_PROVIDER)
AMO_TASK_SOURCE_LABELS = {
    "max": "MAX",
    "vk": "VK",
    TELEGRAM_PROVIDER: "Telegram Personal",
    SALEBOT_PROVIDER: "SaleBot",
    EMAIL_PROVIDER: "Email",
}
NOTIFY_SOURCES = set(AMO_TASK_SOURCES)
BROWSER_NOTIFY_CLAIM_SECONDS = 45
BROWSER_NOTIFY_PAGE_SIZE = 20
WEB_PUSH_RECORD_SIZE = 4096
WEB_PUSH_MAX_PAYLOAD = 3500
WEB_PUSH_SUBJECT = "mailto:admin@sobakovod.pro"
DEVICE_TOUCH_INTERVAL_SECONDS = 300
OUTBOUND_MAX_ATTEMPTS = 8
OUTBOUND_RETRY_SECONDS = (15, 45, 120, 300, 900, 1800, 3600, 10800)
AMO_TASK_MAX_ATTEMPTS = 8
AMO_TASK_RETRY_SECONDS = (30, 120, 300, 900, 1800, 3600, 10800, 21600)
AMO_NEXUS_TASK_PREFIX = "Новое сообщение ·"
AMO_TASK_TEXT_LIMIT = 2000
COMMUNICATION_RETENTION_DAYS = 730

_db_path: Path | None = None
_logger = None
_channel_cache: tuple[float, list[dict[str, str]]] = (0.0, [])
_all_channels_cache: tuple[float, list[dict[str, str]]] = (0.0, [])
_all_channels_inflight: asyncio.Task[Any] | None = None
_all_channels_cache_owner: str = ""
_vk_history_cache: dict[tuple[str, int], tuple[float, bool]] = {}
_telegram_history_cache: dict[tuple[str, int], tuple[float, bool]] = {}
_salebot_history_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_telegram_history_inflight: set[tuple[str, int]] = set()
_wazzup_history_inflight: set[tuple[str, str, int]] = set()
_card_link_cache: dict[tuple[str, ...], tuple[float, dict[str, str]]] = {}
_telegram_profile_inflight: dict[tuple[str, ...], asyncio.Task[Any]] = {}
_telegram_state_cache: tuple[float, dict[str, Any]] = (0.0, {})
_telegram_auth_pending: dict[str, dict[str, Any]] = {}
_telegram_lock = asyncio.Lock()
_vk_queue_lock = asyncio.Lock()
_vk_callback_config: dict[str, str] = {"key": "", "secret": "", "confirmation": ""}
_vk_callback_write_queue: asyncio.Queue[tuple[str, str, str, asyncio.Future[Exception | None]]] | None = None
_vk_callback_writer_task: asyncio.Task[Any] | None = None
_identity_index: Any = None
_identity_index_status: dict[str, Any] = {"status": "pending", "records": 0}
_identity_lookup_loop: asyncio.AbstractEventLoop | None = None
_identity_lookup_gate: asyncio.Semaphore | None = None
_identity_resolve_cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}
_identity_resolve_inflight: dict[tuple[str, ...], asyncio.Task[Any]] = {}
_identity_exact_cache: dict[tuple[str, ...], tuple[float, str]] = {}
_identity_exact_inflight: dict[tuple[str, ...], asyncio.Task[Any]] = {}
_identity_cache_owner: Any = None
_profile_links_cache: dict[tuple[str, ...], tuple[float, list[dict[str, Any]], bool]] = {}
_profile_links_inflight: dict[tuple[str, ...], asyncio.Task[Any]] = {}
_staff_catalog_cache: tuple[float, list[dict[str, Any]]] = (0.0, [])
_notification_wakeup = asyncio.Event()
_outbound_wakeup = asyncio.Event()
_amo_task_wakeup = asyncio.Event()
_notification_bot_poll_at = ""
_notification_bot_poll_error = ""
_module_lifecycle: Any = None
_telegram_realtime_task: asyncio.Task[Any] | None = None


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


def _card_link_state(key: tuple[str, ...]) -> str:
    """Return the live card-check state without starting another lookup."""

    task = _telegram_profile_inflight.get(key)
    if task and not task.done():
        return "pending"
    cached = _card_link_cache.get(key)
    if not cached or cached[0] <= time.monotonic():
        return "unknown"
    link = cached[1]
    if link.get("pending"):
        return "pending"
    return "verified" if _clean(link.get("external_user_id"), 250) else "missing"


async def _successful_card_delivery_link(
    context: dict[str, Any], provider: str, *, db: aiosqlite.Connection | None = None,
) -> dict[str, str]:
    """Return an exact direct identity only after a successful card delivery."""

    if (
        context.get("platform") != "amocrm"
        or context.get("entity_type") != "lead"
        or not _clean(context.get("entity_id"), 100)
    ):
        return {}
    lead_id = _clean(context.get("entity_id"), 100)
    chat_type = "telegram" if provider == TELEGRAM_PROVIDER else provider
    owns_db = db is None
    if db is None:
        db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT chat_id,client_name FROM communication_messages
               WHERE amo_lead_id=? AND provider=? AND direction='outgoing'
                 AND status IN ('sent','delivered','read') AND chat_id<>''
               ORDER BY sent_at DESC,id DESC LIMIT 1""",
            (lead_id, provider),
        )).fetchone()
        if not row:
            row = await (await db.execute(
                """SELECT context.external_user_id AS chat_id,'' AS client_name
                   FROM conversation_contexts context
                   WHERE context.provider=? AND context.platform='amocrm'
                     AND context.entity_type='lead' AND context.entity_id=?
                     AND EXISTS(
                       SELECT 1 FROM wazzup_messages message
                       WHERE message.chat_type=? AND message.chat_id=context.external_user_id
                         AND message.direction='outgoing'
                         AND message.status IN ('sent','delivered','read')
                     )
                   ORDER BY context.updated_at DESC LIMIT 1""",
                (provider, lead_id, chat_type),
            )).fetchone()
    finally:
        if owns_db:
            await db.close()
    if not row:
        return {}
    peer_id = _clean(row["chat_id"], 250)
    stored = await _external_link(
        peer_id=peer_id,
        provider=provider,
        db=db if not owns_db else None,
    )
    return stored or {
        "provider": provider,
        "external_user_id": peer_id,
        "name": _clean(row["client_name"], 200),
    }


async def _amocrm_telegram_profile_link(
    data: dict[str, Any], mode: str, device: dict[str, Any], context: dict[str, Any],
) -> dict[str, str]:
    """Start a bounded background check without delaying the profile header."""

    cache_key = _card_link_cache_key(context, device, TELEGRAM_PROVIDER, "")
    cached = _card_link_cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return dict(cached[1])
    task = _telegram_profile_inflight.get(cache_key)
    if task and not task.done():
        return {"pending": "1"}

    async def verify() -> None:
        try:
            result = await _provider_card_link(
                data, mode, device, TELEGRAM_PROVIDER,
                allow_phone_import=False, resolution_timeout=30,
            )
            if result.get("pending"):
                _remember_card_link(cache_key, {"pending": "1"})
        except Exception as exc:
            _log("warning", "Telegram profile verification deferred: %s", type(exc).__name__)
            _remember_card_link(cache_key, {"pending": "1"})
        finally:
            _telegram_profile_inflight.pop(cache_key, None)

    _telegram_profile_inflight[cache_key] = asyncio.create_task(verify())
    return {"pending": "1"}


async def setup(ctx) -> None:
    global _db_path, _logger, _identity_index, _module_lifecycle, _telegram_realtime_task
    global _vk_callback_write_queue
    global _vk_callback_writer_task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    await _init_vk_callback_queue()
    await _refresh_vk_callback_config()
    _identity_index = IdentityIndex(_customer_db_path(), _must_db().parent / "identity-index.db")
    _identity_index.cleanup_staging()
    lifecycle = getattr(ctx, "lifecycle", None)
    _module_lifecycle = lifecycle
    if lifecycle is not None:
        _vk_callback_write_queue = asyncio.Queue(maxsize=64)
        lifecycle.create_task(vk_background_loop(), name="messenger-widget-vk-sync")
        _vk_callback_writer_task = lifecycle.create_task(
            vk_callback_writer_loop(), name="messenger-widget-vk-callback-writer",
        )
        lifecycle.create_task(vk_callback_queue_loop(), name="messenger-widget-vk-callback-queue")
        lifecycle.create_task(maintenance_loop(), name="messenger-widget-maintenance")
        lifecycle.create_task(telegram_background_loop(), name="messenger-widget-telegram-sync")
        _telegram_realtime_task = lifecycle.create_task(
            telegram_realtime_loop(), name="messenger-widget-telegram-realtime",
        )
        lifecycle.create_task(identity_index_loop(), name="messenger-widget-identity-index")
        lifecycle.create_task(notification_delivery_loop(), name="messenger-widget-notification-delivery")
        lifecycle.create_task(notification_telegram_poll_loop(), name="messenger-widget-notification-telegram")
        for worker_id in range(1, 5):
            lifecycle.create_task(
                outbound_delivery_loop(worker_id), name=f"messenger-widget-outbound-{worker_id}",
            )
        lifecycle.create_task(amo_task_delivery_loop(), name="messenger-widget-amo-tasks")
        lifecycle.create_task(widget_operation_loop(), name="messenger-widget-operations")
    else:
        _vk_callback_write_queue = None
        _vk_callback_writer_task = None


async def on_telegram_proxy_changed() -> dict[str, Any]:
    global _telegram_realtime_task
    task = _telegram_realtime_task
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if _module_lifecycle is not None:
        _telegram_realtime_task = _module_lifecycle.create_task(
            telegram_realtime_loop(), name="messenger-widget-telegram-realtime",
        )
    return {"ok": True, "reconnected": _telegram_realtime_task is not None}


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("messenger-widget is not initialized")
    return _db_path


def _now_dt() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(value: datetime | None = None) -> str:
    return (value or _now_dt()).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    raw = _clean(value, 80)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


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


def _telegram_dialog_hidden(
    identity: dict[str, Any] | None = None,
    *,
    peer_id: Any = "",
    phone: Any = "",
    username: Any = "",
) -> bool:
    """Return whether a personal Telegram account is excluded from widgets."""

    identity = identity if isinstance(identity, dict) else {}
    ids = {
        _clean(value, 200)
        for value in (
            peer_id,
            identity.get("external_user_id"),
            identity.get("telegram_id"),
            identity.get("id"),
        )
        if _clean(value, 200)
    }
    phones = {
        normalized
        for value in (phone, identity.get("phone"))
        if (normalized := _normalize_phone(value))
    }
    usernames = {
        _clean(value, 200).lstrip("@").casefold()
        for value in (
            username,
            identity.get("username"),
            identity.get("telegram_username"),
            identity.get("tg_username"),
        )
        if _clean(value, 200)
    }
    return bool(
        ids.intersection(TELEGRAM_HIDDEN_PEER_IDS)
        or phones.intersection(TELEGRAM_HIDDEN_PHONES)
        or usernames.intersection(TELEGRAM_HIDDEN_USERNAMES)
    )


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
        "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Nexus-Messenger-Platform",
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


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    raw = value.encode("ascii")
    return base64.urlsafe_b64decode(raw + b"=" * (-len(raw) % 4))


async def _connect():
    db = await aiosqlite.connect(_must_db(), timeout=30)
    await db.execute("PRAGMA busy_timeout=30000")
    await db.execute("PRAGMA foreign_keys=ON")
    # A widget request opens several short-lived read connections.  Keep each
    # connection's private page cache deliberately small so a burst of cards
    # cannot evict the Bizon browser or the OS file cache into swap.
    await db.execute("PRAGMA cache_size=-512")
    await db.execute("PRAGMA temp_store=FILE")
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
            CREATE TABLE IF NOT EXISTS template_favorites (
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                template_id INTEGER NOT NULL REFERENCES message_templates(id) ON DELETE CASCADE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                PRIMARY KEY(admin_id,template_id)
            );
            CREATE TABLE IF NOT EXISTS template_user_order (
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                template_id INTEGER NOT NULL REFERENCES message_templates(id) ON DELETE CASCADE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(admin_id,template_id)
            );
            CREATE TABLE IF NOT EXISTS widget_media (
                token TEXT PRIMARY KEY,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                stored_name TEXT NOT NULL UNIQUE,
                mime_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                created_at TEXT NOT NULL
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
            CREATE TABLE IF NOT EXISTS notification_destinations (
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                connected_at TEXT NOT NULL,
                verified_at TEXT NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(admin_id,provider),
                UNIQUE(provider,recipient_id)
            );
            CREATE TABLE IF NOT EXISTS notification_preferences (
                admin_id INTEGER PRIMARY KEY REFERENCES admins(id) ON DELETE CASCADE,
                fallback_unassigned INTEGER NOT NULL DEFAULT 0,
                course_chats INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_route_policies (
                source_admin_id INTEGER PRIMARY KEY REFERENCES admins(id) ON DELETE CASCADE,
                configured INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_routes (
                source_admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                recipient_admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(source_admin_id,recipient_admin_id)
            );
            CREATE TABLE IF NOT EXISTS notification_pairings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                code_hash TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                used_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS conversation_contexts (
                provider TEXT NOT NULL,
                external_user_id TEXT NOT NULL,
                admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                platform TEXT NOT NULL DEFAULT '',
                entity_type TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                entity_url TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(provider,external_user_id)
            );
            CREATE TABLE IF NOT EXISTS notification_events (
                external_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                thread_key TEXT NOT NULL,
                channel_id TEXT NOT NULL DEFAULT '',
                chat_type TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                target_admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                client_name TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                content_type TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL,
                available_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL REFERENCES notification_events(external_id) ON DELETE CASCADE,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                external_message_id TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(event_id,admin_id,provider)
            );
            CREATE TABLE IF NOT EXISTS browser_notification_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
                token_hash TEXT NOT NULL UNIQUE,
                token_hint TEXT NOT NULL DEFAULT '',
                label TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 0,
                enabled_at TEXT NOT NULL DEFAULT '',
                last_seen_at TEXT NOT NULL DEFAULT '',
                push_endpoint TEXT NOT NULL DEFAULT '',
                push_p256dh TEXT NOT NULL DEFAULT '',
                push_auth TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(device_id)
            );
            CREATE TABLE IF NOT EXISTS browser_notification_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL REFERENCES browser_notification_subscriptions(id) ON DELETE CASCADE,
                event_id TEXT NOT NULL REFERENCES notification_events(external_id) ON DELETE CASCADE,
                status TEXT NOT NULL DEFAULT 'pending',
                claimed_at TEXT NOT NULL DEFAULT '',
                shown_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(subscription_id,event_id)
            );
            CREATE TABLE IF NOT EXISTS communication_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                external_id TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL,
                channel_id TEXT NOT NULL DEFAULT '',
                chat_type TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                content_uri TEXT NOT NULL DEFAULT '',
                client_name TEXT NOT NULL DEFAULT '',
                phone_hash TEXT NOT NULL DEFAULT '',
                platform TEXT NOT NULL DEFAULT '',
                entity_type TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                entity_url TEXT NOT NULL DEFAULT '',
                amo_lead_id TEXT NOT NULL DEFAULT '',
                admin_id INTEGER REFERENCES admins(id) ON DELETE SET NULL,
                manager_name TEXT NOT NULL DEFAULT '',
                transport_author TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,
                latency_ms INTEGER,
                error TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbound_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_key TEXT NOT NULL UNIQUE,
                admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
                device_id INTEGER REFERENCES devices(id) ON DELETE SET NULL,
                provider TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                chat_id TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                client_name TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                getcourse_user_id TEXT NOT NULL DEFAULT '',
                platform TEXT NOT NULL DEFAULT '',
                entity_type TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                entity_url TEXT NOT NULL DEFAULT '',
                amo_lead_id TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                attachment_url TEXT NOT NULL DEFAULT '',
                attachment_type TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                external_id TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                queued_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '',
                sent_at TEXT NOT NULL DEFAULT '',
                latency_ms INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS amo_task_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_key TEXT NOT NULL UNIQUE,
                communication_id INTEGER REFERENCES communication_messages(id) ON DELETE SET NULL,
                amo_lead_id TEXT NOT NULL,
                responsible_user_id TEXT NOT NULL DEFAULT '',
                messenger TEXT NOT NULL,
                client_name TEXT NOT NULL DEFAULT '',
                message_text TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                amo_task_id TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
            CREATE INDEX IF NOT EXISTS ix_mw_template_favorites_template ON template_favorites(template_id,admin_id);
            CREATE INDEX IF NOT EXISTS ix_mw_template_user_order ON template_user_order(admin_id,sort_order,template_id);
            CREATE INDEX IF NOT EXISTS ix_mw_widget_media_created ON widget_media(created_at);
            CREATE INDEX IF NOT EXISTS ix_mw_rules_priority ON identity_rules(enabled,priority,id);
            CREATE INDEX IF NOT EXISTS ix_mw_entity_links_external ON entity_identity_links(provider,external_user_id);
            CREATE INDEX IF NOT EXISTS ix_mw_notify_pairings ON notification_pairings(provider,expires_at,used_at);
            CREATE INDEX IF NOT EXISTS ix_mw_notify_context_admin ON conversation_contexts(admin_id,updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_notify_events_due ON notification_events(status,available_at,thread_key);
            CREATE INDEX IF NOT EXISTS ix_mw_notify_delivery_due ON notification_deliveries(status,next_attempt_at,updated_at);
            CREATE INDEX IF NOT EXISTS ix_mw_notify_routes_recipient ON notification_routes(recipient_admin_id,source_admin_id);
            CREATE INDEX IF NOT EXISTS ix_mw_browser_sub_admin ON browser_notification_subscriptions(admin_id,enabled,last_seen_at DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_browser_delivery_due ON browser_notification_deliveries(subscription_id,status,claimed_at);
            CREATE INDEX IF NOT EXISTS ix_mw_communications_time ON communication_messages(sent_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_communications_admin ON communication_messages(admin_id,sent_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_communications_client ON communication_messages(amo_lead_id,chat_id,sent_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_communications_provider ON communication_messages(provider,direction,status,sent_at DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_outbound_due ON outbound_jobs(status,next_attempt_at,id);
            CREATE INDEX IF NOT EXISTS ix_mw_outbound_metrics ON outbound_jobs(provider,status,created_at DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_outbound_admin ON outbound_jobs(admin_id,id DESC);
            CREATE INDEX IF NOT EXISTS ix_mw_amo_tasks_due ON amo_task_jobs(status,next_attempt_at,id);
            CREATE INDEX IF NOT EXISTS ix_mw_events_action_status ON events(action,status,id DESC);
            """
        )
        cursor = await db.execute(
            "UPDATE outbound_jobs SET status='retry',next_attempt_at=?,updated_at=? WHERE status='processing'",
            (_iso(), _iso()),
        )
        await db.execute(
            "UPDATE amo_task_jobs SET status='retry',next_attempt_at=?,updated_at=? WHERE status='processing'",
            (_iso(), _iso()),
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
        browser_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(browser_notification_subscriptions)")).fetchall()}
        for name in ("push_endpoint", "push_p256dh", "push_auth"):
            if name not in browser_columns:
                await db.execute(f"ALTER TABLE browser_notification_subscriptions ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        preference_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(notification_preferences)")).fetchall()}
        if "course_chats" not in preference_columns:
            await db.execute("ALTER TABLE notification_preferences ADD COLUMN course_chats INTEGER NOT NULL DEFAULT 0")
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
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('auto_markup_domains',?,?)",
            (AUTO_MARKUP_DEFAULT_DOMAINS, now),
        )
        vapid_row = await (await db.execute(
            "SELECT value FROM module_settings WHERE key='webpush_vapid_private'"
        )).fetchone()
        if not vapid_row or not _clean(vapid_row["value"], 10000):
            private_key = ec.generate_private_key(ec.SECP256R1())
            private_pem = private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ).decode("ascii")
            public_raw = private_key.public_key().public_bytes(
                serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint,
            )
            await db.executemany(
                "INSERT OR REPLACE INTO module_settings(key,value,updated_at) VALUES(?,?,?)",
                (
                    ("webpush_vapid_private", private_pem, now),
                    ("webpush_vapid_public", _b64url(public_raw), now),
                ),
            )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('auto_markup_tail',?,?)",
            (AUTO_MARKUP_DEFAULT_TAIL, now),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_live_since',?,?)",
            (now, now),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_telegram_callback_secret',?,?)",
            (secrets.token_urlsafe(32), now),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_salebot_secret',?,?)",
            (secrets.token_urlsafe(32), now),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_telegram_update_offset','0',?)",
            (now,),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_telegram_poll_at','',?)",
            (now,),
        )
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('notification_telegram_poll_error','',?)",
            (now,),
        )
        email_task_migration = await (await db.execute(
            "SELECT 1 FROM module_settings WHERE key='amo_task_email_default_enabled_v1'"
        )).fetchone()
        if not email_task_migration:
            source_rows = await (await db.execute(
                "SELECT key,value FROM module_settings WHERE key LIKE 'admin_amo_task_sources:%'"
            )).fetchall()
            for source_row in source_rows:
                sources = _parse_amo_task_sources(source_row["value"])
                if EMAIL_PROVIDER not in sources:
                    sources.append(EMAIL_PROVIDER)
                    await db.execute(
                        "UPDATE module_settings SET value=?,updated_at=? WHERE key=?",
                        (json.dumps(sources, ensure_ascii=False), now, source_row["key"]),
                    )
            await db.execute(
                "INSERT INTO module_settings(key,value,updated_at) VALUES('amo_task_email_default_enabled_v1','1',?)",
                (now,),
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
        await db.execute(
            "DELETE FROM notification_pairings WHERE expires_at<? OR used_at<>''",
            (_iso(_now_dt() - timedelta(days=1)),),
        )
        await db.execute(
            "DELETE FROM notification_events WHERE created_at<?",
            (_iso(_now_dt() - timedelta(days=NOTIFY_EVENT_RETENTION_DAYS)),),
        )
        await db.execute(
            "DELETE FROM browser_notification_subscriptions WHERE enabled=0 AND updated_at<?",
            (_iso(_now_dt() - timedelta(days=30)),),
        )
        await db.execute(
            "DELETE FROM communication_messages WHERE created_at<?",
            (_iso(_now_dt() - timedelta(days=COMMUNICATION_RETENTION_DAYS)),),
        )
        completed_cutoff = _iso(_now_dt() - timedelta(days=180))
        await db.execute(
            "DELETE FROM outbound_jobs WHERE status IN ('sent','failed') AND created_at<?", (completed_cutoff,),
        )
        await db.execute(
            "DELETE FROM amo_task_jobs WHERE status IN ('sent','failed') AND created_at<?", (completed_cutoff,),
        )
        await db.commit()
    finally:
        await db.close()
    message_cutoff = _iso(_now_dt() - timedelta(days=180))
    last_id = 0
    while True:
        db = await _connect()
        try:
            rows = await (await db.execute(
                "SELECT id FROM wazzup_messages WHERE id>? AND created_at<? ORDER BY id LIMIT 750",
                (last_id, message_cutoff),
            )).fetchall()
            ids = [int(row[0]) for row in rows]
            if not ids:
                break
            last_id = ids[-1]
            placeholders = ",".join("?" for _ in ids)
            cursor = await db.execute(f"DELETE FROM wazzup_messages WHERE id IN ({placeholders})", ids)
            deleted = max(0, cursor.rowcount)
            await db.commit()
        finally:
            await db.close()
        if deleted < len(ids):
            break
        # Retention must never monopolise the 2+ GB message database while a
        # manager is opening templates or a conversation.
        await asyncio.sleep(0.2)


async def maintenance_loop() -> None:
    # Let the user-facing APIs and channel workers become usable first.
    await asyncio.sleep(90)
    while True:
        try:
            await _cleanup()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "Messenger maintenance failed: %s", exc)
        await asyncio.sleep(6 * 60 * 60)


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


def _operation_payload(value: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


async def _queue_widget_operation(
    *, action: str, device: dict[str, Any], context: dict[str, Any],
    enrollment_id: str, details: dict[str, Any],
) -> int:
    if action not in WIDGET_OPERATION_ACTIONS:
        raise ValueError("unsupported widget operation")
    payload = {
        **details,
        "enrollment_id": _clean(enrollment_id, 100),
        "client_name": _clean(context.get("name"), 200),
        "email": _clean(context.get("email"), 320),
        "platform": _clean(context.get("platform"), 40),
        "entity_type": _clean(context.get("entity_type"), 40),
        "entity_id": _clean(context.get("entity_id"), 100),
        "entity_url": _clean(context.get("entity_url"), 1000),
        "manager_name": _clean(device.get("admin_name"), 200),
        "attempts": 0,
        "note_status": "pending" if (
            context.get("platform") == "amocrm"
            and context.get("entity_type") == "lead"
            and _clean(context.get("entity_id"), 64).isdigit()
        ) else "skipped",
    }
    now = _iso()
    db = await _connect()
    try:
        cursor = await db.execute(
            """INSERT INTO events(
               admin_id,device_id,action,status,page_kind,entity_id,phone_mask,error,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                int(device["admin_id"]), int(device["id"]), action, "pending",
                _clean(context.get("platform"), 20), _clean(enrollment_id, 100),
                _mask_phone(context.get("phone")),
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"))[:8000], now,
            ),
        )
        await db.commit()
        return int(cursor.lastrowid or 0)
    finally:
        await db.close()


def _schedule_widget_operation(**kwargs: Any) -> None:
    async def persist() -> None:
        for attempt in range(120):
            try:
                await _queue_widget_operation(**kwargs)
                return
            except asyncio.CancelledError:
                raise
            except (sqlite3.Error, OSError) as exc:
                if attempt == 119:
                    _log("error", "Widget operation journal write failed: %s", exc)
                    return
                await asyncio.sleep(min(5, 0.1 * (attempt + 1)))
            except Exception as exc:
                _log("error", "Widget operation journal rejected: %s", exc)
                return

    if _module_lifecycle is not None:
        _module_lifecycle.create_task(persist(), name="messenger-widget-operation-journal")
    else:
        asyncio.create_task(persist())


async def _update_widget_operation(event_id: int, status: str, details: dict[str, Any]) -> None:
    db = await _connect()
    try:
        await db.execute(
            "UPDATE events SET status=?,error=? WHERE id=?",
            (
                _clean(status, 40),
                json.dumps(details, ensure_ascii=False, separators=(",", ":"))[:8000],
                int(event_id),
            ),
        )
        await db.commit()
    finally:
        await db.close()


def _operation_title(action: str) -> str:
    return {
        "widget_getcourse_access": "Доступы GetCourse",
        "widget_trial_issue": "Тестовый период",
        "widget_trial_revoke": "Досрочное закрытие тестового периода",
    }.get(action, "Операция Nexus")


def _friendly_operation_error(value: Any, provider: Any = "") -> str:
    """Translate transport diagnostics into a short instruction for sales staff."""

    raw = _clean(value, 2000)
    if not raw:
        return "Сообщение не доставлено."
    text = raw.casefold()
    if any(marker in text for marker in ("429", "too many", "rate limit", "flood", "слишком много")):
        return "Канал ограничил частоту отправки. Nexus повторит автоматически."
    if any(marker in text for marker in ("401", "unauthorized", "token", "auth", "авторизац")):
        return "Канал нужно переподключить в настройках Nexus."
    if any(marker in text for marker in ("403", "forbidden", "blocked", "bot was blocked", "заблок", "запрет")):
        return "Клиент запретил сообщения в этом канале."
    if any(marker in text for marker in ("404", "not found", "recipient", "peer", "chat not", "пользователь не найден", "получатель")):
        return "Профиль клиента в канале не найден."
    if any(marker in text for marker in (
        "timeout", "timed out", "time out", "502", "503", "504", "connection", "network",
        "temporar", "unavailable", "не ответ", "недоступ", "соединен",
    )):
        return "Канал временно не ответил. Nexus попробует ещё раз."
    if any(marker in text for marker in ("400", "bad request", "invalid", "некоррект", "отклонил")):
        return "Канал отклонил сообщение. Проверьте адресата и текст."
    label = _clean(provider, 40).replace("telegram_personal", "TG Personal").replace("salebot", "SaleBot")
    return f"{label or 'Канал'} не доставил сообщение. Nexus сохранил ошибку для проверки."


def _operation_note_text(action: str, status: str, details: dict[str, Any]) -> str:
    courses = details.get("courses") if isinstance(details.get("courses"), list) else []
    course_names = {"puppy": "Щенок", "dog": "Собака"}
    lines = [
        f"Nexus · {_operation_title(action)}",
        f"Результат: {_clean(details.get('result'), 1000) or ('выполнено' if status == 'success' else 'ошибка')}",
    ]
    if courses:
        lines.append("Курсы: " + ", ".join(course_names.get(str(item), str(item)) for item in courses))
    if details.get("days"):
        lines.append(f"Срок: {int(details['days'])} дн.")
    if details.get("expires_at"):
        lines.append(f"Доступ закроется: {_clean(details['expires_at'], 60)}")
    if details.get("manager_name"):
        lines.append(f"Выполнил: {_clean(details['manager_name'], 200)}")
    lines.append(f"Время: {_clean(details.get('completed_at') or _iso(), 60)}")
    return "\n".join(lines)


async def _send_amo_operation_note(lead_id: str, text: str) -> None:
    values = _read_env_values()
    base_url = _clean(os.environ.get("AMO_BASE_URL") or values.get("AMO_BASE_URL"), 1000).rstrip("/")
    token = _clean(os.environ.get("AMO_ACCESS_TOKEN") or values.get("AMO_ACCESS_TOKEN"), 5000)
    if not base_url or not token:
        raise AmoTaskDeliveryError("AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы")
    if not _clean(lead_id, 64).isdigit():
        raise AmoTaskDeliveryError("Некорректный ID сделки amoCRM", permanent=True)
    async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
        await _amo_task_api_request(
            client, "POST", f"{base_url}/api/v4/leads/{lead_id}/notes", token,
            payload=[{"note_type": "common", "params": {"text": _clean(text, 10000)}}],
        )


async def _check_widget_operation(row: dict[str, Any]) -> None:
    details = _operation_payload(row.get("error"))
    action = _clean(row.get("action"), 80)
    status = _clean(row.get("status"), 40)
    if status == "pending":
        attempts = int(details.get("attempts") or 0) + 1
        details["attempts"] = attempts
        try:
            if action == "widget_getcourse_access":
                service = _module_service("student-transfer", "service_widget_access_operation")
                operation = await service(request_id=_clean(details.get("request_id"), 64))
                state = _clean(operation.get("status"), 30)
                if state == "verified":
                    status, details["result"] = "success", "Доступы применены и подтверждены GetCourse."
                elif state == "failed" or (state == "missing" and attempts >= 120):
                    status, details["result"] = "failed", "GetCourse не подтвердил изменение доступов."
            else:
                service = _module_service("student-transfer", "service_widget_test_period")
                trial = await service(
                    enrollment_id=_clean(details.get("enrollment_id"), 100), action="status",
                    requester_user_id="messenger-operation-worker",
                )
                trial_status = _clean(trial.get("status"), 40)
                details["expires_at"] = _clean(trial.get("expires_at"), 60) or details.get("expires_at", "")
                if action == "widget_trial_issue" and trial_status == "active":
                    status, details["result"] = "success", "Тестовый период выдан."
                elif action == "widget_trial_revoke" and trial_status in {"completed", "blocked_used"}:
                    status, details["result"] = "success", "Тестовый доступ закрыт."
                elif trial_status == "blocked_used":
                    status, details["result"] = "failed", "Тестовый период уже использовался."
                elif action == "widget_trial_issue" and trial_status == "completed":
                    status, details["result"] = "success", "Тестовый период выдан и уже завершён."
            if status != "pending":
                details["completed_at"] = _iso()
        except Exception as exc:
            details["last_error"] = _clean(exc, 500)
        await _update_widget_operation(int(row["id"]), status, details)

    if status in {"success", "failed"} and details.get("note_status") == "pending":
        next_at = _parse_iso(details.get("note_next_at"))
        if next_at and next_at > _now_dt():
            return
        try:
            await _send_amo_operation_note(
                _clean(details.get("entity_id"), 64),
                _operation_note_text(action, status, details),
            )
            details["note_status"] = "sent"
            details.pop("note_error", None)
            details.pop("note_next_at", None)
        except Exception as exc:
            note_attempts = int(details.get("note_attempts") or 0) + 1
            details["note_attempts"] = note_attempts
            details["note_error"] = _clean(exc, 500)
            details["note_next_at"] = _iso(_now_dt() + timedelta(seconds=min(3600, 15 * 2 ** min(note_attempts, 8))))
        await _update_widget_operation(int(row["id"]), status, details)


async def widget_operation_loop() -> None:
    await asyncio.sleep(2)
    while True:
        try:
            cutoff = _iso(_now_dt() - timedelta(days=AUDIT_RETENTION_DAYS))
            placeholders = ",".join("?" for _ in WIDGET_OPERATION_ACTIONS)
            db = await _connect()
            try:
                rows = await (await db.execute(
                    f"""SELECT * FROM events
                        WHERE action IN ({placeholders}) AND created_at>=?
                          AND (status='pending' OR error LIKE '%\"note_status\":\"pending\"%')
                        ORDER BY id LIMIT 12""",
                    (*WIDGET_OPERATION_ACTIONS, cutoff),
                )).fetchall()
            finally:
                await db.close()
            async def check(row: Any) -> None:
                try:
                    await asyncio.wait_for(_check_widget_operation(dict(row)), timeout=30)
                except TimeoutError:
                    _log("warning", "Widget operation check timed out id=%s", row["id"])

            # A slow GetCourse check must not block every other manager's
            # operation. Keep concurrency deliberately small to protect the
            # shared GetCourse queue while allowing ten widget users to work.
            for offset in range(0, len(rows), 4):
                await asyncio.gather(*(check(row) for row in rows[offset:offset + 4]))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "Messenger operation worker failed: %s", exc)
        await asyncio.sleep(5)


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


async def _store_vk_messages(
    peer_id: str, rows: list[dict[str, Any]], identity: dict[str, Any], *, outgoing_author_name: str = "",
    course_context: dict[str, Any] | None = None,
) -> int:
    channel_id = _vk_channel_id()
    group_id = _vk_group_id()
    if not channel_id or not peer_id:
        return 0
    now = _iso()
    inserted = 0
    name = _clean(identity.get("name"), 200) or f"VK {peer_id}"
    notification_records: list[dict[str, Any]] = []
    db = await _connect()
    try:
        for row in rows:
            message_id = _clean(row.get("id") or row.get("conversation_message_id"), 200)
            if not message_id:
                continue
            # VK may keep the real employee in ``from_id`` when a manager
            # answers on behalf of the community. ``out`` is the authoritative
            # side-of-dialog flag; fall back to the community id for old rows.
            out_flag = row.get("out")
            outgoing = bool(out_flag) if out_flag is not None else (
                _clean(row.get("from_id"), 200).lstrip("-") == group_id
            )
            direction = "outgoing" if outgoing else "incoming"
            sent_at = _message_time(row.get("date"))
            text_value = _clean(row.get("text"), 20_000)
            attachments = _vk_attachment_views(row.get("attachments"))
            first = attachments[0] if attachments else {"content_uri": "", "content_type": "", "filename": ""}
            raw = dict(row)
            raw["nexus_attachments"] = attachments
            external_id = f"vk:{group_id}:{peer_id}:{message_id}"
            cursor = await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(external_id) DO UPDATE SET
                   direction=excluded.direction,status=excluded.status,text=excluded.text,
                   content_uri=excluded.content_uri,
                   author_name=CASE
                     WHEN wazzup_messages.direction='outgoing' AND wazzup_messages.author_name NOT IN ('','Сообщество')
                     THEN wazzup_messages.author_name ELSE excluded.author_name END,
                   sent_at=excluded.sent_at,raw_json=excluded.raw_json""",
                (external_id, channel_id, "vk", peer_id, "", direction, "delivered", text_value, first["content_uri"], name if direction == "incoming" else (_clean(outgoing_author_name, 200) or "Сообщество"), sent_at, json.dumps(raw, ensure_ascii=False, separators=(",", ":"))[:50_000], now),
            )
            inserted += max(0, cursor.rowcount)
            if (
                direction == "incoming" and not _messenger_button_event(row)
                and not (course_context and course_context.get("suppress_notification"))
            ):
                notification_records.append({
                    "external_id": external_id, "text": text_value,
                    "content_type": first["content_type"], "sent_at": sent_at,
                    "raw_payload": row,
                })
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
    for record in notification_records:
        await _enqueue_notification_message(
            external_id=record["external_id"], channel_id=channel_id, chat_type="vk",
            chat_id=peer_id, provider="vk", client_name=name, text=record["text"],
            content_type=record["content_type"], sent_at=record["sent_at"],
            course_context=course_context, raw_payload=record.get("raw_payload"),
        )
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
    await asyncio.sleep(30)
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
                # The identity index contains hundreds of thousands of rows.
                # COUNT queries must never run on the event loop or once per
                # health request: ten simultaneously opened widgets would
                # otherwise serialize those scans and stall unrelated work.
                current = await asyncio.to_thread(_identity_index.status)
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
        cache_key = {
            "vk_callback_key": "key",
            "vk_callback_secret": "secret",
            "vk_confirmation_code": "confirmation",
        }.get(key)
        if cache_key:
            _vk_callback_config[cache_key] = _clean(value, 1000)
    finally:
        await db.close()


class NotificationDeliveryError(RuntimeError):
    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent


def _notification_bot_token() -> str:
    values = _read_env_values()
    return _clean(
        os.environ.get(NOTIFY_TELEGRAM_TOKEN_ENV_KEY)
        or values.get(NOTIFY_TELEGRAM_TOKEN_ENV_KEY),
        1000,
    )


async def _notification_tg_call(method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    token = _notification_bot_token()
    if not token:
        raise NotificationDeliveryError("Токен Telegram-бота уведомлений не задан")
    try:
        async with httpx.AsyncClient(**httpx_client_kwargs(timeout=httpx.Timeout(20, connect=10))) as client:
            response = await client.post(
                f"{telegram_bot_api_base().rstrip('/')}/bot{token}/{method}",
                json=payload or {},
            )
        body = response.json() if response.content else {}
    except (httpx.HTTPError, ValueError) as exc:
        raise NotificationDeliveryError(f"Telegram недоступен: {type(exc).__name__}: {_clean(exc, 240)}") from exc
    if response.status_code >= 400 or not isinstance(body, dict) or not body.get("ok"):
        description = _clean(body.get("description") if isinstance(body, dict) else "", 300)
        code = int(body.get("error_code") or response.status_code) if isinstance(body, dict) else response.status_code
        raise NotificationDeliveryError(
            f"Telegram {code}: {description or 'ошибка Bot API'}",
            permanent=code in {400, 403},
        )
    result = body.get("result")
    return result if isinstance(result, dict) else {"value": result}


def _notification_source(channel_id: str, chat_type: str, provider: str = "") -> str:
    provider = _clean(provider, 40).lower()
    chat_type = _clean(chat_type, 40).lower()
    if provider == SALEBOT_PROVIDER or channel_id.startswith("salebot:") or chat_type == "salebot":
        return SALEBOT_PROVIDER
    if provider == TELEGRAM_PROVIDER or channel_id.startswith("telegram-personal:"):
        return TELEGRAM_PROVIDER
    if provider == "vk" or channel_id.startswith("vk:"):
        return "vk"
    if chat_type in {"max", "maxgroup"}:
        return "max"
    return ""


def _messenger_button_event(payload: Any) -> bool:
    """Recognise explicit keyboard/callback events without guessing from text."""

    if not isinstance(payload, dict):
        return False
    if payload.get("payload") not in (None, "", {}, []):
        return True
    event_type = _clean(
        payload.get("event_type") or payload.get("eventType") or payload.get("type"), 100,
    ).casefold().replace("-", "_")
    return event_type in {
        "callback", "callback_query", "button", "button_click", "button_pressed",
        "keyboard", "message_event", "postback", "quick_reply",
    }


_FUNNEL_ACK_WORDS = frozenset({
    "+", "да", "нет", "ага", "угу", "ок", "окей", "хорошо", "понятно",
    "спасибо", "благодарю", "приду", "буду", "получилось", "договорились",
})
_GENERIC_OUTBOUND_AUTHORS = frozenset({
    "", "сообщество", "telegram", "бот", "bot", "system", "система", "wazzup",
})


def _reply_message_ids(payload: Any) -> set[str]:
    """Extract provider reply/quote ids without treating arbitrary ids as replies."""

    if not isinstance(payload, dict):
        return set()
    result: set[str] = set()
    reply_nodes = []
    for key in (
        "reply_message", "replyMessage", "quoted_message", "quotedMessage",
        "reply_to", "replyTo", "quoted", "reply", "context",
    ):
        value = payload.get(key)
        if isinstance(value, dict):
            reply_nodes.append(value)
        elif key not in {"context"} and value not in (None, "", 0, False):
            result.add(_clean(value, 250))
    for node in reply_nodes:
        for key in (
            "id", "message_id", "messageId", "conversation_message_id",
            "conversationMessageId", "external_id", "externalId",
        ):
            if node.get(key) not in (None, "", 0, False):
                result.add(_clean(node.get(key), 250))
    return {value for value in result if value}


def _is_low_information_funnel_ack(text: Any) -> bool:
    value = _clean(text, 1000).casefold().replace("ё", "е")
    if not value:
        return False
    parts = [
        part for part in re.split(r"[\s,.;:!?—–()\[\]{}]+", value)
        if part and not re.fullmatch(r"[+👌👍🙏✅🙂😊😉]+", part)
    ]
    if not parts:
        return True
    return len(parts) <= 8 and all(part in _FUNNEL_ACK_WORDS for part in parts)


def _raw_message_ids(raw: dict[str, Any]) -> set[str]:
    return {
        _clean(raw.get(key), 250)
        for key in ("id", "message_id", "messageId", "conversation_message_id", "conversationMessageId")
        if raw.get(key) not in (None, "", 0, False)
    }


async def _is_funnel_reply(
    *, source: str, channel_id: str, chat_type: str, chat_id: str,
    text: str, sent_at: str, raw_payload: Any = None,
) -> bool:
    """Suppress only replies attributable to automation, never by text alone.

    An explicit quote of a non-manager message is authoritative.  Without a
    quote, only a short acknowledgement after a long/voice generic outbound is
    considered funnel traffic.  A Nexus manager-authored message always wins.
    """

    reply_ids = _reply_message_ids(raw_payload)
    low_information = _is_low_information_funnel_ack(text)
    if not reply_ids and not low_information:
        return False
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT external_id,text,content_uri,author_name,sent_at,raw_json
               FROM wazzup_messages
               WHERE channel_id=? AND chat_type=? AND chat_id=? AND direction='outgoing'
                 AND sent_at<=?
               ORDER BY sent_at DESC,id DESC LIMIT 50""",
            (_clean(channel_id, 200), _clean(chat_type, 40), _clean(chat_id, 250), _message_time(sent_at)),
        )).fetchall()
        admin_names = {
            _clean(row["name"], 200).casefold()
            for row in await (await db.execute("SELECT name FROM admins WHERE enabled=1")).fetchall()
        }
        manager_external_ids = {
            _clean(row["external_id"], 250)
            for row in await (await db.execute(
                """SELECT external_id FROM communication_messages
                   WHERE provider=? AND chat_id=? AND direction='outgoing' AND manager_name<>''
                   ORDER BY sent_at DESC,id DESC LIMIT 50""",
                (_clean(source, 40), _clean(chat_id, 250)),
            )).fetchall()
        }
    finally:
        await db.close()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            raw = json.loads(_clean(item.get("raw_json"), 50_000) or "{}")
        except json.JSONDecodeError:
            raw = {}
        item["ids"] = {_clean(item.get("external_id"), 250), *_raw_message_ids(raw)}
        candidates.append(item)
    if reply_ids:
        candidates = [item for item in candidates if item["ids"] & reply_ids]
        if not candidates:
            return False
    elif candidates:
        candidates = candidates[:1]
    for item in candidates:
        author = _clean(item.get("author_name"), 200).casefold()
        external_id = _clean(item.get("external_id"), 250)
        manager_authored = external_id in manager_external_ids or (author in admin_names and author not in _GENERIC_OUTBOUND_AUTHORS)
        if manager_authored:
            return False
        explicit_automation = author in _GENERIC_OUTBOUND_AUTHORS
        funnel_shape = len(_clean(item.get("text"), 20_000)) >= 80 or bool(_clean(item.get("content_uri"), 4000))
        if explicit_automation and (reply_ids or funnel_shape):
            return True
    return False


def _notification_entity_url(context: dict[str, Any]) -> str:
    if _clean(context.get("platform"), 40) != "amocrm" or not _amo_origin():
        return ""
    entity_id = _clean(context.get("entity_id"), 200)
    entity_type = _clean(context.get("entity_type"), 40).lower()
    if not entity_id or entity_type not in {"lead", "contact", "company", "customer"}:
        return ""
    plural = {"lead": "leads", "contact": "contacts", "company": "companies", "customer": "customers"}[entity_type]
    return f"{_amo_origin()}/{plural}/detail/{quote(entity_id, safe='')}"


def _person_key(value: Any) -> str:
    translit = str.maketrans({
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
        "ж": "zh", "з": "z", "и": "i", "й": "i", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sh",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    })
    return re.sub(r"[^a-z0-9]+", "", _clean(value, 200).casefold().translate(translit))


def _course_chat_addressed(
    context: dict[str, Any], text: str = "", *, reply_sender_id: str = "", reply_sender_ref: str = "",
) -> bool:
    """Return whether a course-chat message explicitly addresses its curator."""
    value = _clean(text, 5000).casefold()
    reply_id = re.sub(r"\D+", "", _clean(reply_sender_id, 200))
    reply_ref = _clean(reply_sender_ref, 200).lstrip("@").casefold()
    curator_id = re.sub(r"\D+", "", _clean(context.get("curator_vk_id"), 200))
    vk_ref = _clean(context.get("curator_vk_ref"), 200).lstrip("@").casefold()
    telegram_ref = _clean(context.get("curator_telegram"), 200).lstrip("@").casefold()
    if curator_id and (
        reply_id == curator_id
        or re.search(rf"\[id{re.escape(curator_id)}\|", value)
        or re.search(rf"https?://(?:m\.)?vk\.(?:com|ru)/(?:id)?{re.escape(curator_id)}(?:\b|/)", value)
    ):
        return True
    references = {item for item in (vk_ref, telegram_ref) if item}
    if reply_ref and reply_ref in references:
        return True
    return any(re.search(rf"(?<![\w.])@{re.escape(reference)}(?![\w.])", value) for reference in references)


async def _course_chat_context(
    provider: str, chat_id: str, title: str = "", *, text: str = "",
    reply_sender_id: str = "", reply_sender_ref: str = "", require_addressed: bool = False,
) -> dict[str, Any]:
    platform = "telegram" if provider == TELEGRAM_PROVIDER else "vk" if provider == "vk" else ""
    if not platform:
        return {}
    try:
        service = _module_service("course-chat-creator", "service_notification_chat_context")
        result = service(platform=platform, chat_id=_clean(chat_id, 250), title=_clean(title, 500))
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, dict) or not result.get("found"):
            return {}
        result["addressed"] = _course_chat_addressed(
            result, text, reply_sender_id=reply_sender_id, reply_sender_ref=reply_sender_ref,
        )
        return {} if require_addressed and not result["addressed"] else result
    except Exception:
        return {}


async def _course_chat_target(context: dict[str, Any]) -> int | None:
    curator_key = _person_key(context.get("curator_name"))
    if not curator_key:
        return None
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT a.id,a.name FROM admins a
               JOIN notification_preferences p ON p.admin_id=a.id
               WHERE a.enabled=1 AND p.course_chats=1 ORDER BY a.id"""
        )).fetchall()
    finally:
        await db.close()
    matches = []
    for row in rows:
        admin_key = _person_key(row["name"])
        if admin_key == curator_key or admin_key.startswith(curator_key) or curator_key.startswith(admin_key):
            matches.append(int(row["id"]))
    return matches[0] if len(matches) == 1 else None


async def _remember_notification_context(
    context: dict[str, Any], provider: str, external_user_id: str, admin_id: int | None,
) -> bool:
    provider = _clean(provider, 40).lower()
    external_user_id = _clean(external_user_id, 250)
    if not provider or not external_user_id:
        return False
    now = _iso()
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        platform = _clean(context.get("platform"), 40)
        entity_type = _clean(context.get("entity_type"), 40)
        entity_id = _clean(context.get("entity_id"), 200)
        if platform == "amocrm" and entity_type == "lead" and entity_id:
            conflict = await (await db.execute(
                """SELECT external_user_id FROM entity_identity_links
                   WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?
                     AND external_user_id<>? LIMIT 1""",
                (platform, entity_type, entity_id, provider, external_user_id),
            )).fetchone()
            if conflict:
                await db.rollback()
                _log(
                    "warning",
                    "Rejected conflicting messenger context provider=%s external=%s entity=%s:%s",
                    provider, external_user_id, entity_type, entity_id,
                )
                return False
        await db.execute(
            """INSERT INTO conversation_contexts(
               provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
               ) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(provider,external_user_id) DO UPDATE SET
               admin_id=COALESCE(excluded.admin_id,conversation_contexts.admin_id),
               platform=CASE WHEN excluded.platform<>'' THEN excluded.platform ELSE conversation_contexts.platform END,
               entity_type=CASE WHEN excluded.entity_type<>'' THEN excluded.entity_type ELSE conversation_contexts.entity_type END,
               entity_id=CASE WHEN excluded.entity_id<>'' THEN excluded.entity_id ELSE conversation_contexts.entity_id END,
               entity_url=CASE WHEN excluded.entity_url<>'' THEN excluded.entity_url ELSE conversation_contexts.entity_url END,
               updated_at=excluded.updated_at""",
            (
                provider, external_user_id, admin_id, _clean(context.get("platform"), 40),
                _clean(context.get("entity_type"), 40), _clean(context.get("entity_id"), 200),
                _clean(context.get("entity_url"), 2000) or _notification_entity_url(context), now,
            ),
        )
        await db.commit()
        return True
    finally:
        await db.close()


async def _trusted_conversation_context(
    db: aiosqlite.Connection, provider: str, external_user_id: str,
) -> Any:
    """Return a context only when it agrees with the current exact entity link."""

    return await (await db.execute(
        """SELECT c.* FROM conversation_contexts c
           WHERE c.provider=? AND c.external_user_id=?
             AND NOT EXISTS(
                 SELECT 1 FROM entity_identity_links e
                 WHERE e.platform=c.platform AND e.entity_type=c.entity_type
                   AND e.entity_id=c.entity_id AND e.provider=c.provider
                   AND e.external_user_id<>c.external_user_id
             )""",
        (_clean(provider, 40), _clean(external_user_id, 250)),
    )).fetchone()


async def _notification_owner(
    db: aiosqlite.Connection, *, source: str, channel_id: str, chat_type: str,
    chat_id: str, phone_hash: str,
) -> int | None:
    row = await (await db.execute(
        "SELECT responsible_admin_id FROM wazzup_chats WHERE channel_id=? AND chat_type=? AND chat_id=?",
        (channel_id, chat_type, chat_id),
    )).fetchone()
    if row and row["responsible_admin_id"]:
        return int(row["responsible_admin_id"])
    row = await _trusted_conversation_context(db, source, chat_id)
    if row and row["admin_id"]:
        return int(row["admin_id"])
    if phone_hash:
        row = await (await db.execute(
            "SELECT responsible_admin_id FROM client_links WHERE phone_hash=?",
            (phone_hash,),
        )).fetchone()
        if row and row["responsible_admin_id"]:
            return int(row["responsible_admin_id"])
    return None


async def _current_amo_notification_owner(source: str, chat_id: str) -> int | None:
    """Resolve the current amoCRM responsible manager from local deal data."""
    db = await _connect()
    try:
        context = await _trusted_conversation_context(db, source, chat_id)
    finally:
        await db.close()
    if not context or context["platform"] != "amocrm" or context["entity_type"] != "lead" or not context["entity_id"]:
        return None
    details = await asyncio.to_thread(_amo_deal_delivery_details, _clean(context["entity_id"], 64))
    responsible_user_id = _clean(details.get("responsible_user_id"), 64)
    if not responsible_user_id:
        return None
    db = await _connect()
    try:
        admin = await (await db.execute(
            """SELECT a.id FROM manager_bindings b JOIN admins a ON a.id=b.admin_id
               WHERE b.platform='amocrm' AND b.platform_user_id=? AND a.enabled=1
               ORDER BY b.updated_at DESC LIMIT 1""",
            (responsible_user_id,),
        )).fetchone()
        if not admin:
            return None
        admin_id = int(admin["id"])
        await db.execute(
            "UPDATE conversation_contexts SET admin_id=?,updated_at=? WHERE provider=? AND external_user_id=?",
            (admin_id, _iso(), _clean(source, 40), _clean(chat_id, 250)),
        )
        await db.commit()
        return admin_id
    finally:
        await db.close()


async def _enqueue_notification_message(
    *, external_id: str, channel_id: str, chat_type: str, chat_id: str,
    phone_hash: str = "", provider: str = "", client_name: str = "",
    text: str = "", content_type: str = "", sent_at: str = "",
    course_context: dict[str, Any] | None = None,
    delay_seconds: int | None = None,
    raw_payload: Any = None,
) -> bool:
    source = _notification_source(channel_id, chat_type, provider)
    external_id = _clean(external_id, 250)
    chat_id = _clean(chat_id, 250)
    sent_at = _message_time(sent_at)
    if source not in NOTIFY_SOURCES or not external_id or not chat_id:
        return False
    # Pairing commands and messenger keyboard/callback payloads are service
    # traffic, not a message which a sales manager should answer.
    if re.fullmatch(r"(?:NEXUS|НЕКСУС)[-\s]+[A-Z0-9]{8,24}", _clean(text, 200).upper()):
        return False
    if await _is_funnel_reply(
        source=source, channel_id=channel_id, chat_type=chat_type, chat_id=chat_id,
        text=text, sent_at=sent_at, raw_payload=raw_payload,
    ):
        _log("info", "Suppressed funnel reply source=%s chat=%s external=%s", source, chat_id, external_id)
        return False
    live_since = await _setting("notification_live_since")
    if live_since and sent_at < live_since:
        return False
    now = _iso()
    delay = NOTIFY_BATCH_SECONDS if delay_seconds is None else max(0, min(int(delay_seconds), NOTIFY_BATCH_SECONDS))
    available_at = _iso(_now_dt() + timedelta(seconds=delay))
    thread_key = f"{source}:{_clean(channel_id, 200)}:{chat_id}"
    course_context = course_context if isinstance(course_context, dict) and course_context.get("found") else {}
    direct_target = await _course_chat_target(course_context) if course_context else None
    if course_context and not direct_target:
        return False
    if course_context:
        await _remember_notification_context(
            {
                "platform": "course_chat", "entity_type": "chat",
                "entity_id": _clean(course_context.get("chat_id") or chat_id, 200),
                "entity_url": _clean(course_context.get("chat_url"), 2000),
            },
            source, chat_id, direct_target,
        )
    else:
        # Backfill legacy conversations before choosing the current amoCRM
        # owner and before rendering the manager notification.
        await _notification_context(source, chat_id)
    current_amo_owner = None if direct_target else await _current_amo_notification_owner(source, chat_id)
    db = await _connect()
    inserted = False
    try:
        await db.execute("BEGIN IMMEDIATE")
        owner_id = direct_target or current_amo_owner or await _notification_owner(
            db, source=source, channel_id=_clean(channel_id, 200),
            chat_type=_clean(chat_type, 40).lower(), chat_id=chat_id,
            phone_hash=_clean(phone_hash, 100),
        )
        if owner_id is None:
            linked_lead = await (await db.execute(
                """SELECT 1 FROM conversation_contexts c
                   WHERE c.provider=? AND c.external_user_id=? AND c.platform='amocrm'
                     AND c.entity_type='lead' AND c.entity_id<>''
                     AND NOT EXISTS(
                         SELECT 1 FROM entity_identity_links e
                         WHERE e.platform=c.platform AND e.entity_type=c.entity_type
                           AND e.entity_id=c.entity_id AND e.provider=c.provider
                           AND e.external_user_id<>c.external_user_id
                     )
                   UNION ALL
                   SELECT 1 FROM entity_identity_links
                   WHERE provider=? AND external_user_id=? AND platform='amocrm' AND entity_type='lead' AND entity_id<>''
                   LIMIT 1""",
                (source, chat_id, source, chat_id),
            )).fetchone()
            if not linked_lead:
                await db.rollback()
                return False
        # Rolling debounce: every new client message postpones the whole open
        # group, so a burst becomes one notification and one amoCRM task.
        await db.execute(
            """UPDATE notification_events SET available_at=?,updated_at=?
               WHERE status='pending' AND thread_key=? AND target_admin_id IS ?""",
            (available_at, now, thread_key, owner_id),
        )
        cursor = await db.execute(
            """INSERT OR IGNORE INTO notification_events(
               external_id,source,thread_key,channel_id,chat_type,chat_id,target_admin_id,
               client_name,text,content_type,sent_at,available_at,status,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
            (
                external_id, source, thread_key, _clean(channel_id, 200),
                _clean(chat_type, 40).lower(), chat_id, owner_id,
                _clean(client_name, 200), _clean(text, 3500), _clean(content_type, 100),
                sent_at, available_at, now, now,
            ),
        )
        inserted = cursor.rowcount > 0
        await db.commit()
    finally:
        await db.close()
    if inserted:
        context = await _communication_context(
            source, chat_id, channel_id=_clean(channel_id, 200), chat_type=_clean(chat_type, 40).lower(),
        )
        communication_id = await _record_communication(
            dedupe_key=f"incoming:{external_id}", external_id=external_id, provider=source,
            channel_id=channel_id, chat_type=chat_type, chat_id=chat_id,
            direction="incoming", status="received", text=text, sent_at=sent_at,
            client_name=client_name, phone_hash=phone_hash, admin_id=owner_id,
            transport_author=client_name, context=context,
        )
        task_text = _clean(text, 2000) or {
            "audio": "Голосовое сообщение", "voice": "Голосовое сообщение",
            "image": "Изображение", "video": "Видео", "document": "Вложение",
        }.get(_clean(content_type, 100).lower(), "Вложение без текста")
        await _enqueue_amo_task_for_message(
            message_key=f"incoming:{external_id}", communication_id=communication_id,
            context=context, source=source, messenger=_notification_label(source),
            client_name=client_name, message_text=task_text, admin_id=owner_id,
        )
        _notification_wakeup.set()
    return inserted


async def service_email_inbound(
    *, external_id: str, thread_token: str, client_name: str, text: str,
    sent_at: str, context: dict[str, Any], content_type: str = "",
) -> dict[str, Any]:
    """Accept only an exactly bound email reply from email-channel.

    The opaque Reply-To token, never the email address or client name, is the
    messenger identity. A token that is already bound to another card is
    rejected instead of silently moving context between deals.
    """
    token = _clean(thread_token, 250)
    platform = _clean(context.get("platform"), 40).lower()
    entity_type = _clean(context.get("entity_type"), 40).lower()
    entity_id = _clean(context.get("entity_id"), 200)
    if not token or platform != "amocrm" or entity_type != "lead" or not entity_id:
        return {"ok": True, "queued": False, "reason": "no_exact_amo_context"}
    db = await _connect()
    try:
        existing = await (await db.execute(
            "SELECT platform,entity_type,entity_id FROM conversation_contexts WHERE provider=? AND external_user_id=?",
            (EMAIL_PROVIDER, token),
        )).fetchone()
        if existing and (
            existing["platform"] != platform or existing["entity_type"] != entity_type
            or existing["entity_id"] != entity_id
        ):
            _log("error", "Rejected conflicting email context token=%s", token[:12])
            return {"ok": False, "queued": False, "reason": "context_conflict"}
        now = _iso()
        await db.execute(
            """INSERT INTO conversation_contexts(provider,external_user_id,platform,entity_type,entity_id,entity_url,updated_at)
               VALUES(?,?,?,?,?,?,?) ON CONFLICT(provider,external_user_id) DO UPDATE SET updated_at=excluded.updated_at""",
            (EMAIL_PROVIDER, token, platform, entity_type, entity_id,
             f"{_amo_origin()}/leads/detail/{quote(entity_id, safe='')}", now),
        )
        await db.commit()
    finally:
        await db.close()
    inserted = await _enqueue_notification_message(
        external_id=_clean(external_id, 250), channel_id="email:info",
        chat_type="email", chat_id=token, provider=EMAIL_PROVIDER,
        client_name=_clean(client_name, 200), text=_clean(text, 3500),
        content_type=_clean(content_type, 100), sent_at=sent_at, delay_seconds=NOTIFY_BATCH_SECONDS,
    )
    return {"ok": True, "queued": inserted}


def _notification_label(source: str) -> str:
    return {
        "max": "MAX",
        "vk": "VK",
        TELEGRAM_PROVIDER: "Telegram Personal",
        SALEBOT_PROVIDER: "SaleBot",
        EMAIL_PROVIDER: "Email",
    }.get(source, source.upper())


def _admin_amo_task_setting_key(admin_id: int) -> str:
    return f"admin_amo_task_enabled:{max(0, int(admin_id))}"


def _admin_amo_task_sources_setting_key(admin_id: int) -> str:
    return f"admin_amo_task_sources:{max(0, int(admin_id))}"


def _parse_amo_task_sources(value: Any) -> list[str]:
    try:
        rows = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        rows = None
    if not isinstance(rows, list):
        return list(AMO_TASK_SOURCES)
    selected = {_clean(row, 40).lower() for row in rows}
    return [source for source in AMO_TASK_SOURCES if source in selected]


async def _admin_amo_task_enabled(admin_id: int | None) -> bool:
    """Keep the historical enabled behavior unless an exact employee opted out."""

    if not admin_id:
        return True
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT value FROM module_settings WHERE key=?",
            (_admin_amo_task_setting_key(admin_id),),
        )).fetchone()
    finally:
        await db.close()
    return row is None or _clean(row["value"], 20) != "0"


async def _admin_amo_task_allowed(admin_id: int | None, source: str) -> bool:
    if not await _admin_amo_task_enabled(admin_id):
        return False
    if not admin_id:
        return _clean(source, 40).lower() in AMO_TASK_SOURCES
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT value FROM module_settings WHERE key=?",
            (_admin_amo_task_sources_setting_key(admin_id),),
        )).fetchone()
    finally:
        await db.close()
    sources = _parse_amo_task_sources(row["value"] if row else None)
    return _clean(source, 40).lower() in sources


async def _amo_task_context_allowed(context: dict[str, Any], source: str) -> bool:
    """Block a task when its messenger profile contradicts the exact lead link."""

    lead_id = _clean(context.get("amo_lead_id") or context.get("entity_id"), 64)
    external_user_id = _clean(context.get("external_user_id"), 250)
    source = _clean(source, 40).lower()
    if not lead_id or not external_user_id:
        return True
    db = await _connect()
    try:
        exact = await (await db.execute(
            """SELECT external_user_id FROM entity_identity_links
               WHERE platform='amocrm' AND entity_type='lead' AND entity_id=? AND provider=?""",
            (lead_id, source),
        )).fetchone()
    finally:
        await db.close()
    return exact is None or _clean(exact["external_user_id"], 250) == external_user_id


async def _notification_context(source: str, chat_id: str) -> dict[str, str]:
    db = await _connect()
    try:
        row = await _trusted_conversation_context(db, source, chat_id)
    finally:
        await db.close()
    context = dict(row) if row else {}
    if (
        _clean(context.get("platform"), 40) != "course_chat"
        and not (
            _clean(context.get("platform"), 40) == "amocrm"
            and _clean(context.get("entity_type"), 40) == "lead"
            and _clean(context.get("entity_id"), 64)
        )
    ):
        recovered = await _identity_amo_notification_context(source, chat_id)
        if recovered:
            remembered = await _remember_notification_context(
                recovered, source, chat_id,
                int(context.get("admin_id") or 0) or None,
            )
            if remembered:
                context.update(recovered)
    return context


async def _notification_admin_name(admin_id: Any) -> str:
    try:
        clean_id = int(admin_id or 0)
    except (TypeError, ValueError):
        return ""
    if not clean_id:
        return ""
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT name FROM admins WHERE id=? AND enabled=1", (clean_id,),
        )).fetchone()
    finally:
        await db.close()
    return _clean(row["name"], 200) if row else ""


async def _notification_text(rows: list[dict[str, Any]]) -> tuple[str, list[tuple[str, str]]]:
    first = rows[0]
    name = _clean(first.get("client_name"), 200) or "Клиент"
    context = await _notification_context(first["source"], first["chat_id"])
    course_chat = _clean(context.get("platform"), 40) == "course_chat"
    course_context = await _course_chat_context(first["source"], first["chat_id"]) if course_chat else {}
    responsible_name = await _notification_admin_name(first.get("target_admin_id"))
    lines = [f"💬 Новое сообщение · {_notification_label(first['source'])}"]
    if course_chat:
        chat_title = _clean(course_context.get("title"), 300) or _clean(context.get("entity_id"), 200) or "Учебный чат"
        sender_name = name.split(" · ", 1)[0].strip() if " · " in name else ""
        lines.append(f"Учебный чат: {chat_title}")
        if course_context.get("curator_name"):
            lines.append(f"Куратор: {_clean(course_context.get('curator_name'), 200)}")
        if sender_name and sender_name != chat_title:
            lines.append(f"Отправитель: {sender_name}")
    else:
        lines.append(f"Клиент: {name}")
        if responsible_name:
            lines.append(f"Ответственный: {responsible_name}")
    lines.append("")
    for row in rows[:12]:
        value = _clean(row.get("text"), 1200)
        if not value:
            content_type = _clean(row.get("content_type"), 100).lower()
            value = {
                "audio": "🎤 Голосовое сообщение",
                "voice": "🎤 Голосовое сообщение",
                "image": "🖼 Изображение",
                "video": "🎬 Видео",
                "document": "📎 Вложение",
            }.get(content_type)
            if not value:
                value = (
                    "🎤 Голосовое сообщение" if content_type.startswith("audio/")
                    else "🖼 Изображение" if content_type.startswith("image/")
                    else "🎬 Видео" if content_type.startswith("video/")
                    else "📎 Вложение"
                )
        lines.append(value)
    if len(rows) > 12:
        lines.append(f"…ещё сообщений: {len(rows) - 12}")
    links: list[tuple[str, str]] = []
    entity_url = _clean(course_context.get("chat_url"), 2000) or _clean(context.get("entity_url"), 2000)
    if entity_url:
        links.append(("Открыть учебный чат" if course_chat else "Открыть сделку amoCRM", entity_url))
    if not course_chat and first["source"] == "vk" and str(first["chat_id"]).isdigit():
        links.append(("Профиль VK", f"https://vk.com/id{first['chat_id']}"))
    elif first["source"] == SALEBOT_PROVIDER:
        links.append(("Профиль SaleBot", f"{SALEBOT_PROFILE_BASE}/{quote(str(first['chat_id']), safe='')}"))
    if links:
        lines.extend(["", *[f"{label}: {url}" for label, url in links]])
    return "\n".join(lines)[:4000], links


async def _send_notification_destination(
    provider: str, recipient_id: str, text_value: str, links: list[tuple[str, str]],
) -> str:
    if provider == "telegram":
        payload: dict[str, Any] = {
            "chat_id": recipient_id,
            "text": text_value,
            "link_preview_options": {"is_disabled": True},
        }
        if links:
            payload["reply_markup"] = {
                "inline_keyboard": [[{"text": label, "url": url}] for label, url in links[:3]]
            }
        result = await _notification_tg_call("sendMessage", payload)
        return _clean(result.get("message_id"), 200)
    if provider == "vk":
        try:
            result = await _vk_request("messages.send", {
                "group_id": _vk_group_id(), "peer_id": recipient_id,
                "random_id": secrets.randbelow(2_000_000_000) + 1,
                "message": text_value,
            })
        except HTTPException as exc:
            detail = _clean(exc.detail, 300)
            raise NotificationDeliveryError(detail, permanent=any(code in detail for code in ("VK 901", "VK 902", "VK 917"))) from exc
        return _clean(result.get("message_id") if isinstance(result, dict) else result, 200)
    raise NotificationDeliveryError("Неизвестный канал уведомлений", permanent=True)


def _webpush_encrypt(payload: bytes, receiver_key: str, auth_secret: str) -> bytes:
    if not payload or len(payload) > WEB_PUSH_MAX_PAYLOAD:
        raise NotificationDeliveryError("Некорректный размер Web Push", permanent=True)
    try:
        receiver_raw = _b64url_decode(receiver_key)
        auth_raw = _b64url_decode(auth_secret)
        receiver = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), receiver_raw)
    except Exception as exc:
        raise NotificationDeliveryError("Повреждены ключи браузера", permanent=True) from exc
    sender = ec.generate_private_key(ec.SECP256R1())
    sender_raw = sender.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint,
    )
    shared = sender.exchange(ec.ECDH(), receiver)
    ikm = HKDF(
        algorithm=hashes.SHA256(), length=32, salt=auth_raw,
        info=b"WebPush: info\x00" + receiver_raw + sender_raw,
    ).derive(shared)
    salt = os.urandom(16)
    key = HKDF(
        algorithm=hashes.SHA256(), length=16, salt=salt,
        info=b"Content-Encoding: aes128gcm\x00",
    ).derive(ikm)
    nonce = HKDF(
        algorithm=hashes.SHA256(), length=12, salt=salt,
        info=b"Content-Encoding: nonce\x00",
    ).derive(ikm)
    encrypted = AESGCM(key).encrypt(nonce, payload + b"\x02", None)
    return salt + WEB_PUSH_RECORD_SIZE.to_bytes(4, "big") + bytes((len(sender_raw),)) + sender_raw + encrypted


async def _webpush_vapid_headers(endpoint: str) -> dict[str, str]:
    parsed = urlsplit(endpoint)
    audience = f"{parsed.scheme}://{parsed.netloc}"
    private_pem = await _setting("webpush_vapid_private")
    public_key = await _setting("webpush_vapid_public")
    if not private_pem or not public_key:
        raise NotificationDeliveryError("Web Push ещё не настроен")
    token = jwt.encode(
        {"aud": audience, "exp": int(time.time()) + 12 * 3600, "sub": WEB_PUSH_SUBJECT},
        private_pem, algorithm="ES256",
    )
    return {
        "Authorization": f"vapid t={token}, k={public_key}",
        "Content-Encoding": "aes128gcm",
        "Content-Type": "application/octet-stream",
        "TTL": "86400",
        "Urgency": "high",
    }


async def _require_public_push_endpoint(endpoint: str) -> str:
    parsed = urlsplit(_clean(endpoint, 4000))
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise NotificationDeliveryError("Некорректный адрес Web Push", permanent=True)
    try:
        addresses = await asyncio.to_thread(
            socket.getaddrinfo, parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise NotificationDeliveryError("Сервис Web Push временно недоступен") from exc
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise NotificationDeliveryError("Запрещённый адрес Web Push", permanent=True)
    return endpoint


async def _send_webpush_destination(destination: dict[str, Any], payload: dict[str, Any]) -> str:
    endpoint = await _require_public_push_endpoint(destination.get("push_endpoint") or "")
    body = _webpush_encrypt(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        _clean(destination.get("push_p256dh"), 1000),
        _clean(destination.get("push_auth"), 1000),
    )
    headers = await _webpush_vapid_headers(endpoint)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=8), follow_redirects=False) as client:
            response = await client.post(endpoint, content=body, headers=headers)
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        raise NotificationDeliveryError("Сервис Web Push временно недоступен") from exc
    if response.status_code in {200, 201, 202}:
        return _clean(response.headers.get("location") or f"push-{int(time.time())}", 300)
    permanent = response.status_code in {400, 404, 410}
    raise NotificationDeliveryError(f"Web Push HTTP {response.status_code}", permanent=permanent)


async def _browser_push_destinations(admin_id: int) -> list[dict[str, Any]]:
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT * FROM browser_notification_subscriptions
               WHERE admin_id=? AND enabled=1 AND push_endpoint<>'' AND push_p256dh<>'' AND push_auth<>''
               ORDER BY id""",
            (admin_id,),
        )).fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def _webpush_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    context = await _notification_context(first["source"], first["chat_id"])
    text_value = _clean(first.get("text"), 420) or {
        "audio": "Голосовое сообщение", "voice": "Голосовое сообщение",
        "image": "Изображение", "video": "Видео", "document": "Вложение",
    }.get(_clean(first.get("content_type"), 100).lower(), "Новое вложение")
    if len(rows) > 1:
        text_value = f"{text_value}\nЕщё сообщений: {len(rows) - 1}"
    responsible_name = await _notification_admin_name(first.get("target_admin_id"))
    if responsible_name and _clean(context.get("platform"), 40) != "course_chat":
        text_value = f"Ответственный: {responsible_name}\n{text_value}"
    course_context = (
        await _course_chat_context(first["source"], first["chat_id"])
        if _clean(context.get("platform"), 40) == "course_chat" else {}
    )
    title_name = (
        _clean(course_context.get("title"), 120)
        or _clean(first.get("client_name"), 120)
        or "Клиент"
    )
    return {
        "title": f"{_notification_label(first['source'])} · {title_name}",
        "body": text_value,
        "url": _clean(course_context.get("chat_url"), 2000) or _clean(context.get("entity_url"), 2000),
        "tag": "nexus-" + hashlib.sha256(first["thread_key"].encode()).hexdigest()[:20],
    }


async def _notification_targets(target_admin_id: int | None, *, direct: bool = False) -> list[int]:
    db = await _connect()
    try:
        if target_admin_id:
            policy = await (await db.execute(
                "SELECT configured FROM notification_route_policies WHERE source_admin_id=?",
                (target_admin_id,),
            )).fetchone()
            if policy:
                rows = await (await db.execute(
                    """SELECT a.id FROM notification_routes r JOIN admins a ON a.id=r.recipient_admin_id
                       WHERE r.source_admin_id=? AND a.enabled=1 ORDER BY a.name,a.id""",
                    (target_admin_id,),
                )).fetchall()
                return [int(row["id"]) for row in rows]
            row = await (await db.execute("SELECT id FROM admins WHERE id=? AND enabled=1", (target_admin_id,))).fetchone()
            return [int(row["id"])] if row else []
        rows = await (await db.execute(
            """SELECT a.id FROM admins a JOIN notification_preferences p ON p.admin_id=a.id
               WHERE a.enabled=1 AND a.role='admin' AND p.fallback_unassigned=1 ORDER BY a.id"""
        )).fetchall()
        return [int(row["id"]) for row in rows]
    finally:
        await db.close()


async def _notification_destinations(admin_id: int) -> list[dict[str, Any]]:
    db = await _connect()
    try:
        rows = await (await db.execute(
            "SELECT * FROM notification_destinations WHERE admin_id=? AND enabled=1 ORDER BY provider",
            (admin_id,),
        )).fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def _notification_delivery_state(
    event_ids: list[str], admin_id: int, provider: str,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in event_ids)
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"SELECT * FROM notification_deliveries WHERE event_id IN ({placeholders}) AND admin_id=? AND provider=?",
            (*event_ids, admin_id, provider),
        )).fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def _record_notification_delivery(
    event_ids: list[str], admin_id: int, provider: str, recipient_id: str,
    *, status: str, attempts: int, next_attempt_at: str = "",
    external_message_id: str = "", error: str = "",
) -> None:
    now = _iso()
    db = await _connect()
    try:
        await db.executemany(
            """INSERT INTO notification_deliveries(
               event_id,admin_id,provider,recipient_id,status,attempts,next_attempt_at,
               external_message_id,last_error,sent_at,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id,admin_id,provider) DO UPDATE SET
               recipient_id=excluded.recipient_id,status=excluded.status,attempts=excluded.attempts,
               next_attempt_at=excluded.next_attempt_at,external_message_id=excluded.external_message_id,
               last_error=excluded.last_error,sent_at=excluded.sent_at,updated_at=excluded.updated_at""",
            [(
                event_id, admin_id, provider, recipient_id, status, attempts, next_attempt_at,
                external_message_id, _clean(error, 500), now if status == "sent" else "", now, now,
            ) for event_id in event_ids],
        )
        await db.commit()
    finally:
        await db.close()


async def _next_notification_group() -> list[dict[str, Any]]:
    now = _iso()
    db = await _connect()
    try:
        first = await (await db.execute(
            """SELECT * FROM notification_events WHERE status='pending' AND available_at<=?
               ORDER BY available_at,created_at LIMIT 1""",
            (now,),
        )).fetchone()
        if not first:
            return []
        rows = await (await db.execute(
            """SELECT * FROM notification_events
               WHERE status='pending' AND available_at<=? AND thread_key=? AND target_admin_id IS ?
               ORDER BY sent_at,created_at LIMIT 50""",
            (now, first["thread_key"], first["target_admin_id"]),
        )).fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def _finish_notification_events(event_ids: list[str], status: str, available_at: str = "") -> None:
    placeholders = ",".join("?" for _ in event_ids)
    db = await _connect()
    try:
        if available_at:
            await db.execute(
                f"UPDATE notification_events SET available_at=?,updated_at=? WHERE external_id IN ({placeholders})",
                (available_at, _iso(), *event_ids),
            )
        else:
            await db.execute(
                f"UPDATE notification_events SET status=?,updated_at=? WHERE external_id IN ({placeholders})",
                (status, _iso(), *event_ids),
            )
        await db.commit()
    finally:
        await db.close()


async def _deliver_notification_group(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    event_ids = [row["external_id"] for row in rows]
    first_context = await _notification_context(rows[0]["source"], rows[0]["chat_id"])
    if rows[0].get("target_admin_id") is None and not (
        _clean(first_context.get("platform"), 40) == "amocrm"
        and _clean(first_context.get("entity_type"), 40) == "lead"
        and _clean(first_context.get("entity_id"), 100)
    ):
        await _finish_notification_events(event_ids, "no_destination")
        return
    targets = await _notification_targets(
        rows[0].get("target_admin_id"), direct=_clean(first_context.get("platform"), 40) == "course_chat",
    )
    if not targets:
        await _finish_notification_events(event_ids, "no_destination")
        return
    text_value, links = await _notification_text(rows)
    webpush_payload = await _webpush_payload(rows)
    had_destination = False
    retry_at_values: list[str] = []
    for admin_id in targets:
        destinations = await _notification_destinations(admin_id)
        destinations.extend({**row, "provider": f"browser:{row['id']}", "recipient_id": str(row["id"]), "_browser": True} for row in await _browser_push_destinations(admin_id))
        for destination in destinations:
            had_destination = True
            provider = destination["provider"]
            states = await _notification_delivery_state(event_ids, admin_id, provider)
            if states and all(row["status"] in {"sent", "dead"} for row in states) and len(states) == len(event_ids):
                continue
            attempts = max([int(row.get("attempts") or 0) for row in states] or [0]) + 1
            future = [row["next_attempt_at"] for row in states if row.get("next_attempt_at") and row["next_attempt_at"] > _iso()]
            if future:
                retry_at_values.append(min(future))
                continue
            try:
                if destination.get("_browser"):
                    external_message_id = await _send_webpush_destination(destination, webpush_payload)
                else:
                    external_message_id = await _send_notification_destination(
                        provider, destination["recipient_id"], text_value, links,
                    )
                await _record_notification_delivery(
                    event_ids, admin_id, provider, destination["recipient_id"],
                    status="sent", attempts=attempts, external_message_id=external_message_id,
                )
            except NotificationDeliveryError as exc:
                dead = exc.permanent or attempts >= NOTIFY_MAX_ATTEMPTS
                next_attempt = "" if dead else _iso(
                    _now_dt() + timedelta(seconds=NOTIFY_RETRY_SECONDS[min(attempts - 1, len(NOTIFY_RETRY_SECONDS) - 1)])
                )
                await _record_notification_delivery(
                    event_ids, admin_id, provider, destination["recipient_id"],
                    status="dead" if dead else "failed", attempts=attempts,
                    next_attempt_at=next_attempt, error=str(exc),
                )
                if dead and exc.permanent:
                    db = await _connect()
                    try:
                        if destination.get("_browser"):
                            await db.execute(
                                "UPDATE browser_notification_subscriptions SET enabled=0,updated_at=? WHERE id=?",
                                (_iso(), destination["id"]),
                            )
                        else:
                            await db.execute(
                                "UPDATE notification_destinations SET enabled=0,last_error=?,updated_at=? WHERE admin_id=? AND provider=?",
                                (_clean(exc, 500), _iso(), admin_id, provider),
                            )
                        await db.commit()
                    finally:
                        await db.close()
                elif next_attempt:
                    retry_at_values.append(next_attempt)
    if not had_destination:
        await _finish_notification_events(event_ids, "no_destination")
    elif retry_at_values:
        await _finish_notification_events(event_ids, "pending", min(retry_at_values))
    else:
        await _finish_notification_events(event_ids, "delivered")


async def notification_delivery_loop() -> None:
    await asyncio.sleep(2)
    while True:
        try:
            rows = await _next_notification_group()
            if rows:
                await _deliver_notification_group(rows)
                continue
            _notification_wakeup.clear()
            try:
                await asyncio.wait_for(_notification_wakeup.wait(), timeout=5)
            except TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _log("exception", "Notification delivery loop failed")
            await asyncio.sleep(5)


def _amo_deal_delivery_details(lead_id: str) -> dict[str, str]:
    clean_id = _clean(lead_id, 64)
    path = _customer_db_path()
    if not clean_id or not path.is_file():
        return {}
    try:
        with sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=3) as db:
            row = db.execute(
                "SELECT custom_fields FROM cdb_amo_deals WHERE platform_id=? ORDER BY updated_at DESC LIMIT 1",
                (clean_id,),
            ).fetchone()
        payload = json.loads(row[0]) if row and row[0] else {}
    except (sqlite3.Error, json.JSONDecodeError, TypeError):
        return {}
    contact = payload.get("contact_fields") if isinstance(payload.get("contact_fields"), dict) else {}
    amo = payload.get("amo") if isinstance(payload.get("amo"), dict) else {}
    return {
        "responsible_user_id": _clean(amo.get("responsible_user_id") or payload.get("responsible_user_id"), 64),
        "client_name": _clean(contact.get("name") or payload.get("deal_name"), 200),
        "entity_url": _clean(payload.get("deal_url"), 1000),
    }


async def _identity_amo_notification_context(source: str, chat_id: str) -> dict[str, str]:
    """Recover an exact amoCRM deal from the durable cross-channel graph.

    Old messenger conversations were created before conversation_contexts was
    populated. The identity graph still has their exact VK/Telegram/SaleBot to
    amo deal edge, so use it as a safe fallback instead of omitting the deal.
    A conflicted graph deliberately returns no accounts and therefore no link.
    """
    if _identity_index is None:
        return {}
    clean_source = _clean(source, 40).lower()
    clean_chat_id = _clean(chat_id, 250)
    if clean_source not in NOTIFY_SOURCES or not clean_chat_id:
        return {}
    try:
        resolved = await _resolve_identity_context({
            "service": clean_source,
            "platform": clean_source,
            "entity_type": "contact",
            "entity_id": clean_chat_id,
            "platform_id": clean_chat_id,
            "fields": {"platform_id": clean_chat_id},
        })
    except Exception as exc:
        _log("warning", "amoCRM notification context recovery skipped: %s", exc)
        return {}
    accounts = resolved.get("accounts") if isinstance(resolved, dict) else []
    lead_id = next((
        re.sub(r"\D+", "", _clean(row.get("platform_id"), 100))
        for row in accounts or []
        if isinstance(row, dict) and _clean(row.get("service"), 40).lower() in {"amo", "amocrm"}
        and re.sub(r"\D+", "", _clean(row.get("platform_id"), 100))
    ), "")
    if not lead_id:
        return {}
    details = await asyncio.to_thread(_amo_deal_delivery_details, lead_id)
    entity_url = _clean(details.get("entity_url"), 2000)
    if not entity_url and _amo_origin():
        entity_url = f"{_amo_origin()}/leads/detail/{lead_id}"
    return {
        "platform": "amocrm", "entity_type": "lead",
        "entity_id": lead_id, "entity_url": entity_url,
    }


async def _communication_context(
    provider: str, chat_id: str, *, channel_id: str = "", chat_type: str = "",
) -> dict[str, Any]:
    db = await _connect()
    try:
        context = await _trusted_conversation_context(db, provider, chat_id)
        entity = await (await db.execute(
            """SELECT platform,entity_type,entity_id,external_user_id FROM entity_identity_links
               WHERE provider=? AND external_user_id=? ORDER BY updated_at DESC LIMIT 1""",
            (_clean(provider, 40), _clean(chat_id, 250)),
        )).fetchone()
        chat = await (await db.execute(
            """SELECT contact_name,phone_hash,responsible_admin_id FROM wazzup_chats
               WHERE channel_id=? AND chat_type=? AND chat_id=?""",
            (_clean(channel_id, 200), _clean(chat_type, 40), _clean(chat_id, 250)),
        )).fetchone()
    finally:
        await db.close()
    result = dict(context) if context else {}
    if entity:
        for key in ("platform", "entity_type", "entity_id", "external_user_id"):
            result.setdefault(key, entity[key])
    if chat:
        result.setdefault("client_name", _clean(chat["contact_name"], 200))
        result.setdefault("phone_hash", _clean(chat["phone_hash"], 100))
        result.setdefault("admin_id", chat["responsible_admin_id"])
    result["amo_lead_id"] = (
        _clean(result.get("entity_id"), 64)
        if result.get("platform") == "amocrm" and result.get("entity_type") == "lead"
        else ""
    )
    if result["amo_lead_id"]:
        details = await asyncio.to_thread(_amo_deal_delivery_details, result["amo_lead_id"])
        if not result.get("client_name"):
            result["client_name"] = details.get("client_name", "")
        if not result.get("entity_url"):
            result["entity_url"] = details.get("entity_url", "")
        result["responsible_user_id"] = details.get("responsible_user_id", "")
    return result


async def _record_communication(
    *, dedupe_key: str, external_id: str, provider: str, channel_id: str,
    chat_type: str, chat_id: str, direction: str, status: str, text: str,
    sent_at: str, client_name: str = "", phone_hash: str = "",
    admin_id: int | None = None, manager_name: str = "", transport_author: str = "",
    attempts: int = 0, latency_ms: int | None = None, error: str = "",
    context: dict[str, Any] | None = None,
) -> int:
    context = context or await _communication_context(
        provider, chat_id, channel_id=channel_id, chat_type=chat_type,
    )
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO communication_messages(
               dedupe_key,external_id,provider,channel_id,chat_type,chat_id,direction,status,text,
               client_name,phone_hash,platform,entity_type,entity_id,entity_url,amo_lead_id,
               admin_id,manager_name,transport_author,attempts,latency_ms,error,sent_at,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(dedupe_key) DO UPDATE SET
               external_id=CASE WHEN excluded.external_id<>'' THEN excluded.external_id ELSE communication_messages.external_id END,
               status=excluded.status,text=excluded.text,
               admin_id=COALESCE(excluded.admin_id,communication_messages.admin_id),
               manager_name=CASE WHEN excluded.manager_name<>'' THEN excluded.manager_name ELSE communication_messages.manager_name END,
               transport_author=CASE WHEN excluded.transport_author<>'' THEN excluded.transport_author ELSE communication_messages.transport_author END,
               attempts=MAX(communication_messages.attempts,excluded.attempts),latency_ms=excluded.latency_ms,
               error=excluded.error,updated_at=excluded.updated_at""",
            (
                _clean(dedupe_key, 300), _clean(external_id, 250), _clean(provider, 40),
                _clean(channel_id, 200), _clean(chat_type, 40), _clean(chat_id, 250),
                _clean(direction, 20), _clean(status, 40), _clean(text, 20_000),
                _clean(client_name or context.get("client_name"), 200),
                _clean(phone_hash or context.get("phone_hash"), 100),
                _clean(context.get("platform"), 40), _clean(context.get("entity_type"), 40),
                _clean(context.get("entity_id"), 100), _clean(context.get("entity_url"), 1000),
                _clean(context.get("amo_lead_id"), 64), admin_id or context.get("admin_id"),
                _clean(manager_name, 200), _clean(transport_author, 200), max(0, attempts),
                latency_ms, _clean(error, 1000), _message_time(sent_at), now, now,
            ),
        )
        row = await (await db.execute(
            "SELECT id FROM communication_messages WHERE dedupe_key=?", (_clean(dedupe_key, 300),),
        )).fetchone()
        await db.commit()
        return int(row["id"]) if row else 0
    finally:
        await db.close()


async def _enqueue_amo_task_for_message(
    *, message_key: str, communication_id: int, context: dict[str, Any],
    source: str, messenger: str, client_name: str, message_text: str,
    admin_id: int | None = None,
) -> bool:
    if not await _admin_amo_task_allowed(admin_id, source):
        return False
    if not await _amo_task_context_allowed(context, source):
        _log(
            "warning", "Skipped amoCRM task for conflicting context source=%s lead=%s",
            _clean(source, 40), _clean(context.get("amo_lead_id") or context.get("entity_id"), 64),
        )
        return False
    lead_id = _clean(context.get("amo_lead_id"), 64)
    if not lead_id:
        return False
    details = await asyncio.to_thread(_amo_deal_delivery_details, lead_id)
    now = _iso()
    due_at = _iso(_now_dt() + timedelta(seconds=NOTIFY_BATCH_SECONDS))
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        open_job = await (await db.execute(
            """SELECT id,messenger,message_text FROM amo_task_jobs
               WHERE amo_lead_id=? AND status IN ('pending','retry')
               ORDER BY id DESC LIMIT 1""",
            (lead_id,),
        )).fetchone()
        if open_job:
            messengers = [item.strip() for item in _clean(open_job["messenger"], 400).split(",") if item.strip()]
            current_messenger = _clean(messenger, 80)
            if current_messenger and current_messenger not in messengers:
                messengers.append(current_messenger)
            previous = _clean(open_job["message_text"], 5000)
            addition = _clean(f"{current_messenger}: {message_text}" if current_messenger else message_text, 2200)
            combined = "\n".join(part for part in (previous, addition) if part)[-5000:]
            await db.execute(
                """UPDATE amo_task_jobs SET communication_id=?,responsible_user_id=?,messenger=?,
                   client_name=?,message_text=?,status='pending',next_attempt_at=?,error='',updated_at=?
                   WHERE id=?""",
                (
                    communication_id or None, _clean(details.get("responsible_user_id"), 64),
                    ", ".join(messengers), _clean(client_name or details.get("client_name"), 200),
                    combined, due_at, now, open_job["id"],
                ),
            )
            await db.commit()
            _amo_task_wakeup.set()
            return True
        cursor = await db.execute(
            """INSERT OR IGNORE INTO amo_task_jobs(
               message_key,communication_id,amo_lead_id,responsible_user_id,messenger,
               client_name,message_text,status,attempts,next_attempt_at,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,'pending',0,?,?,?)""",
            (
                _clean(message_key, 300), communication_id or None, lead_id,
                _clean(details.get("responsible_user_id"), 64), _clean(messenger, 80),
                _clean(client_name or details.get("client_name"), 200),
                _clean(message_text, 2000), due_at, now, now,
            ),
        )
        inserted = cursor.rowcount > 0
        await db.commit()
    finally:
        await db.close()
    if inserted:
        _amo_task_wakeup.set()
    return inserted


class AmoTaskDeliveryError(RuntimeError):
    def __init__(self, message: str, *, permanent: bool = False, retry_after: int = 0):
        super().__init__(message)
        self.permanent = permanent
        self.retry_after = max(0, retry_after)


def _retry_after_seconds_header(value: str) -> int:
    try:
        return max(0, min(86400, int(float(_clean(value, 100)))))
    except (TypeError, ValueError):
        return 0


def _amo_task_job_body(job: dict[str, Any]) -> str:
    preview = _clean(job.get("message_text"), 5000) or "Вложение без текста"
    client_name = _clean(job.get("client_name"), 200) or "Клиент"
    return _clean(f"{client_name}: {preview}", 5400)


def _amo_task_text_messengers(value: Any) -> list[str]:
    first_line = _clean(value, AMO_TASK_TEXT_LIMIT).splitlines()[0] if _clean(value, AMO_TASK_TEXT_LIMIT) else ""
    if not first_line.startswith(AMO_NEXUS_TASK_PREFIX):
        return []
    return [item.strip() for item in first_line[len(AMO_NEXUS_TASK_PREFIX):].split(",") if item.strip()]


def _amo_task_legacy_body(value: Any) -> str:
    text = _clean(value, AMO_TASK_TEXT_LIMIT)
    lines = text.splitlines()
    if lines and lines[0].startswith(AMO_NEXUS_TASK_PREFIX):
        return _clean("\n".join(lines[1:]), AMO_TASK_TEXT_LIMIT)
    return text


def _compose_amo_task_text(messengers: list[str], bodies: list[str]) -> str:
    unique_messengers: list[str] = []
    for messenger in messengers:
        clean_messenger = _clean(messenger, 80)
        if clean_messenger and clean_messenger not in unique_messengers:
            unique_messengers.append(clean_messenger)
    header = AMO_NEXUS_TASK_PREFIX + (", ".join(unique_messengers) or "Мессенджер")
    body = "\n\n".join(_clean(item, 5400) for item in bodies if _clean(item, 5400))
    text = header + ("\n" + body if body else "")
    if len(text) <= AMO_TASK_TEXT_LIMIT:
        return text
    marker = "… предыдущие сообщения сокращены …\n"
    body_limit = max(0, AMO_TASK_TEXT_LIMIT - len(header) - len(marker) - 1)
    return _clean(f"{header}\n{marker}{body[-body_limit:]}", AMO_TASK_TEXT_LIMIT)


def _is_open_nexus_amo_task(task: Any, lead_id: str) -> bool:
    if not isinstance(task, dict) or bool(task.get("is_completed")):
        return False
    if _clean(task.get("entity_type"), 32) not in {"", "leads"}:
        return False
    if _clean(task.get("entity_id"), 64) not in {"", lead_id}:
        return False
    return _clean(task.get("text"), AMO_TASK_TEXT_LIMIT).startswith(AMO_NEXUS_TASK_PREFIX)


async def _linked_amo_task_jobs(task_ids: list[str]) -> list[dict[str, Any]]:
    clean_ids = list(dict.fromkeys(_clean(task_id, 64) for task_id in task_ids if _clean(task_id, 64)))
    if not clean_ids:
        return []
    placeholders = ",".join("?" for _ in clean_ids)
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"""SELECT * FROM amo_task_jobs WHERE amo_task_id IN ({placeholders})
                ORDER BY created_at,id""",
            clean_ids,
        )).fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def _amo_task_api_request(
    client: httpx.AsyncClient, method: str, url: str, token: str,
    *, payload: Any = None, params: dict[str, Any] | None = None,
) -> Any:
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        response = await client.request(method, url, json=payload, params=params, headers=headers)
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        raise AmoTaskDeliveryError(f"amoCRM недоступен: {type(exc).__name__}") from exc
    if response.status_code == 429 or response.status_code >= 500:
        raise AmoTaskDeliveryError(
            f"amoCRM HTTP {response.status_code}: {_clean(response.text, 400)}",
            retry_after=_retry_after_seconds_header(response.headers.get("Retry-After", "")),
        )
    if response.status_code >= 400:
        raise AmoTaskDeliveryError(
            f"amoCRM HTTP {response.status_code}: {_clean(response.text, 400)}", permanent=True,
        )
    if not response.content:
        return {}
    try:
        return response.json()
    except (ValueError, TypeError) as exc:
        raise AmoTaskDeliveryError("amoCRM вернула некорректный ответ") from exc


async def _send_amo_task(job: dict[str, Any]) -> tuple[str, list[str]]:
    values = _read_env_values()
    base_url = _clean(os.environ.get("AMO_BASE_URL") or values.get("AMO_BASE_URL"), 1000).rstrip("/")
    token = _clean(os.environ.get("AMO_ACCESS_TOKEN") or values.get("AMO_ACCESS_TOKEN"), 5000)
    if not base_url or not token:
        raise AmoTaskDeliveryError("AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы")
    lead_id = _clean(job.get("amo_lead_id"), 64)
    if not lead_id.isdigit():
        raise AmoTaskDeliveryError("Некорректный ID сделки amoCRM", permanent=True)
    responsible = _clean(job.get("responsible_user_id"), 64)
    due_at = int(time.time()) + 24 * 60 * 60
    async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
        body = await _amo_task_api_request(
            client, "GET", base_url + "/api/v4/tasks", token,
            params={
                "filter[entity_type]": "leads", "filter[entity_id]": int(lead_id),
                "filter[is_completed]": 0, "limit": 250,
            },
        )
        open_tasks = [
            task for task in (((body or {}).get("_embedded") or {}).get("tasks") or [])
            if _is_open_nexus_amo_task(task, lead_id)
        ]
        open_tasks.sort(key=lambda task: (int(task.get("created_at") or 0), int(task.get("id") or 0)))
        task_ids = [_clean(task.get("id"), 64) for task in open_tasks if _clean(task.get("id"), 64)]
        try:
            linked_jobs = await _linked_amo_task_jobs(task_ids)
        except (sqlite3.Error, OSError) as exc:
            raise AmoTaskDeliveryError(f"Не удалось собрать историю задач: {type(exc).__name__}") from exc

        linked_task_ids = {_clean(item.get("amo_task_id"), 64) for item in linked_jobs}
        messengers: list[str] = []
        bodies: list[str] = []
        for linked_job in linked_jobs:
            messengers.extend(
                item.strip() for item in _clean(linked_job.get("messenger"), 400).split(",") if item.strip()
            )
            bodies.append(_amo_task_job_body(linked_job))

        current_body = _amo_task_job_body(job)
        current_already_in_legacy = False
        for task in open_tasks:
            messengers.extend(_amo_task_text_messengers(task.get("text")))
            if _clean(task.get("id"), 64) in linked_task_ids:
                continue
            legacy_body = _amo_task_legacy_body(task.get("text"))
            if legacy_body:
                bodies.append(legacy_body)
                if legacy_body == current_body or legacy_body.endswith("\n\n" + current_body):
                    current_already_in_legacy = True
        messengers.extend(
            item.strip() for item in _clean(job.get("messenger"), 400).split(",") if item.strip()
        )
        if not current_already_in_legacy:
            bodies.append(current_body)
        task_text = _compose_amo_task_text(messengers, bodies)

        if open_tasks:
            same_manager = [
                task for task in open_tasks
                if responsible.isdigit() and _clean(task.get("responsible_user_id"), 64) == responsible
            ]
            canonical = (same_manager or open_tasks)[-1]
            canonical_id = _clean(canonical.get("id"), 64)
            updates: list[dict[str, Any]] = [{
                "id": int(canonical_id), "text": task_text, "complete_till": due_at,
                "is_completed": False,
            }]
            if responsible.isdigit():
                updates[0]["responsible_user_id"] = int(responsible)
            for task in open_tasks:
                duplicate_id = _clean(task.get("id"), 64)
                if duplicate_id and duplicate_id != canonical_id:
                    updates.append({
                        "id": int(duplicate_id), "is_completed": True,
                        "result": {"text": f"Объединено Nexus в задачу #{canonical_id}"},
                    })
            await _amo_task_api_request(
                client, "PATCH", base_url + "/api/v4/tasks", token, payload=updates,
            )
            if len(task_ids) > 1:
                _log(
                    "info", "amoCRM Nexus tasks merged lead=%s canonical=%s duplicates=%s",
                    lead_id, canonical_id, len(task_ids) - 1,
                )
            return canonical_id, task_ids

        task: dict[str, Any] = {
            "entity_id": int(lead_id), "entity_type": "leads", "task_type_id": 1,
            "text": task_text, "complete_till": due_at,
        }
        if responsible.isdigit():
            task["responsible_user_id"] = int(responsible)
        created = await _amo_task_api_request(
            client, "POST", base_url + "/api/v4/tasks", token, payload=[task],
        )
        task_id = _clean(
            ((((created or {}).get("_embedded") or {}).get("tasks") or [{}])[0].get("id")), 64,
        )
        if not task_id:
            raise AmoTaskDeliveryError("amoCRM не вернула ID созданной задачи")
        return task_id, []


async def _claim_amo_task_job() -> dict[str, Any] | None:
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT * FROM amo_task_jobs WHERE status IN ('pending','retry')
               AND (next_attempt_at='' OR next_attempt_at<=?) ORDER BY id LIMIT 1""", (_iso(),),
        )).fetchone()
        if not row:
            return None
        await db.execute("BEGIN IMMEDIATE")
        row = await (await db.execute(
            """SELECT * FROM amo_task_jobs WHERE status IN ('pending','retry')
               AND (next_attempt_at='' OR next_attempt_at<=?) ORDER BY id LIMIT 1""", (_iso(),),
        )).fetchone()
        if not row:
            await db.commit()
            return None
        await db.execute(
            "UPDATE amo_task_jobs SET status='processing',attempts=attempts+1,updated_at=? WHERE id=?",
            (_iso(), row["id"]),
        )
        await db.commit()
        result = dict(row)
        result["attempts"] = int(row["attempts"] or 0) + 1
        return result
    finally:
        await db.close()


def _amo_task_job_sources(job: dict[str, Any]) -> set[str]:
    labels = {
        **{label.casefold(): source for source, label in AMO_TASK_SOURCE_LABELS.items()},
        "tg personal": TELEGRAM_PROVIDER,
    }
    return {
        source
        for item in _clean(job.get("messenger"), 400).split(",")
        if (source := labels.get(item.strip().casefold()))
    }


async def _amo_task_job_still_allowed(job: dict[str, Any]) -> bool:
    """Recheck employee channel settings immediately before amoCRM delivery."""

    db = await _connect()
    try:
        communication = None
        if job.get("communication_id"):
            communication = await (await db.execute(
                "SELECT admin_id,provider FROM communication_messages WHERE id=?",
                (job["communication_id"],),
            )).fetchone()
        admin_id = int(communication["admin_id"]) if communication and communication["admin_id"] else None
        if not admin_id and _clean(job.get("responsible_user_id"), 64):
            binding = await (await db.execute(
                """SELECT admin_id FROM manager_bindings
                   WHERE platform='amocrm' AND platform_user_id=?
                   ORDER BY updated_at DESC LIMIT 1""",
                (_clean(job.get("responsible_user_id"), 64),),
            )).fetchone()
            admin_id = int(binding["admin_id"]) if binding else None
        sources = _amo_task_job_sources(job)
        if communication and _clean(communication["provider"], 40) in AMO_TASK_SOURCES:
            sources.add(_clean(communication["provider"], 40))
    finally:
        await db.close()
    for source in sources:
        if not await _admin_amo_task_allowed(admin_id, source):
            return False
    return True


async def _process_amo_task_job(job: dict[str, Any]) -> None:
    if not await _amo_task_job_still_allowed(job):
        db = await _connect()
        try:
            await db.execute(
                """UPDATE amo_task_jobs SET status='cancelled',next_attempt_at='',
                   error='Канал отключён в настройках сотрудника',updated_at=? WHERE id=?""",
                (_iso(), job["id"]),
            )
            await db.commit()
        finally:
            await db.close()
        return
    try:
        task_id, replaced_task_ids = await _send_amo_task(job)
        status, next_at, error = "sent", "", ""
    except AmoTaskDeliveryError as exc:
        dead = exc.permanent or int(job["attempts"]) >= AMO_TASK_MAX_ATTEMPTS
        delay = exc.retry_after or AMO_TASK_RETRY_SECONDS[min(int(job["attempts"]) - 1, len(AMO_TASK_RETRY_SECONDS) - 1)]
        status, next_at, error, task_id, replaced_task_ids = (
            "failed" if dead else "retry",
            "" if dead else _iso(_now_dt() + timedelta(seconds=delay)),
            str(exc), "", [],
        )
    except Exception as exc:
        dead = int(job["attempts"]) >= AMO_TASK_MAX_ATTEMPTS
        delay = AMO_TASK_RETRY_SECONDS[min(int(job["attempts"]) - 1, len(AMO_TASK_RETRY_SECONDS) - 1)]
        status, next_at, error, task_id, replaced_task_ids = (
            "failed" if dead else "retry",
            "" if dead else _iso(_now_dt() + timedelta(seconds=delay)),
            f"{type(exc).__name__}: {exc}", "", [],
        )
    db = await _connect()
    try:
        if task_id and replaced_task_ids:
            clean_ids = list(dict.fromkeys(_clean(item, 64) for item in replaced_task_ids if _clean(item, 64)))
            placeholders = ",".join("?" for _ in clean_ids)
            if placeholders:
                await db.execute(
                    f"UPDATE amo_task_jobs SET amo_task_id=?,updated_at=? WHERE amo_task_id IN ({placeholders})",
                    [task_id, _iso(), *clean_ids],
                )
        await db.execute(
            "UPDATE amo_task_jobs SET status=?,next_attempt_at=?,amo_task_id=?,error=?,updated_at=? WHERE id=?",
            (status, next_at, task_id, _clean(error, 1000), _iso(), job["id"]),
        )
        await db.commit()
    finally:
        await db.close()


async def amo_task_delivery_loop() -> None:
    await asyncio.sleep(3)
    while True:
        try:
            job = await _claim_amo_task_job()
            if job:
                await _process_amo_task_job(job)
                continue
            _amo_task_wakeup.clear()
            try:
                await asyncio.wait_for(_amo_task_wakeup.wait(), timeout=5)
            except TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _log("exception", "amoCRM task delivery loop failed")
            await asyncio.sleep(5)


def _delivery_retry_delay(error: Any, attempts: int) -> int:
    text_value = _clean(getattr(error, "detail", error), 1000)
    match = re.search(r"(?:FLOOD_WAIT_|wait of\s+)(\d+)", text_value, re.I)
    if match:
        return max(5, min(86400, int(match.group(1)) + 2))
    if "too many requests" in text_value.casefold() or "http 429" in text_value.casefold():
        # Some providers omit Retry-After. Retrying those responses every few
        # seconds only prolongs the rate limit and needlessly burns attempts.
        return max(300, OUTBOUND_RETRY_SECONDS[min(max(0, attempts - 1), len(OUTBOUND_RETRY_SECONDS) - 1)])
    return OUTBOUND_RETRY_SECONDS[min(max(0, attempts - 1), len(OUTBOUND_RETRY_SECONDS) - 1)]


def _delivery_error_is_transient(error: Any) -> bool:
    if isinstance(error, (TimeoutError, httpx.TimeoutException, ConnectionError)):
        return True
    response = getattr(error, "response", None)
    status_code = int(getattr(error, "status_code", 0) or getattr(response, "status_code", 0) or 0)
    text_value = _clean(getattr(error, "detail", error), 1000).casefold()
    if any(token in text_value for token in (
        "vk 901", "without permission", "не найден max", "не найден telegram",
        "не найден salebot", "диалог vk не найден", "диалог telegram не найден",
        "диалог salebot не найден", "salebot не принял сообщение",
        "channel_max_phone_not_occupied",
    )):
        return False
    return status_code in {408, 425, 429, 500, 502, 503, 504} or any(token in text_value for token in (
        "too many requests", "flood_wait", "wait of", "timeout", "timed out",
        "временно", "недоступен", "connection", "transport", "http 429",
        "http 500", "http 502", "http 503", "http 504", "500 server error",
        "502 bad gateway", "503 service unavailable", "504 gateway timeout",
    ))


def _salutation_name_mismatch(text: str, client_name: str) -> tuple[str, str] | None:
    """Catch a pasted greeting addressed to another person before any channel sends it."""

    expected = _clean(client_name, 200).split(" ", 1)[0].strip(" ,.!?:;—–-")
    if len(expected) < 2:
        return None
    match = re.match(
        r"^\s*(?:здравствуйте|добрый\s+(?:день|вечер)|доброе\s+утро|привет)"
        r"\s*[,!]?\s+([А-ЯЁA-Z][А-ЯЁа-яёA-Za-z'-]{1,40})\b",
        _clean(text, 4000), re.I,
    )
    addressed = match.group(1) if match else ""
    normalize = lambda value: value.casefold().replace("ё", "е")
    if addressed and normalize(addressed) != normalize(expected):
        return addressed, expected
    return None


async def _start_outbound_job(
    *, request_key: str, device: dict[str, Any], provider: str, channel_id: str,
    chat_type: str, chat_id: str, phone: str, client_name: str, email: str,
    getcourse_user_id: str, text: str, attachment_url: str, attachment_type: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    request_key = _clean(request_key, 300) or secrets.token_urlsafe(24)
    now = _iso()
    amo_lead_id = (
        _clean(context.get("entity_id"), 64)
        if context.get("platform") == "amocrm" and context.get("entity_type") == "lead" else ""
    )
    db = await _connect()
    try:
        cursor = await db.execute(
            """INSERT OR IGNORE INTO outbound_jobs(
               request_key,admin_id,device_id,provider,channel_id,chat_type,chat_id,phone,
               client_name,email,getcourse_user_id,platform,entity_type,entity_id,entity_url,
               amo_lead_id,text,attachment_url,attachment_type,status,attempts,queued_at,started_at,
               created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',0,?,'',?,?)""",
            (
                request_key, int(device["admin_id"]), int(device["id"]), _clean(provider, 40),
                _clean(channel_id, 200), _clean(chat_type, 40), _clean(chat_id, 250),
                _normalize_phone(phone), _clean(client_name, 200), _clean(email, 320),
                _clean(getcourse_user_id, 200), _clean(context.get("platform"), 40),
                _clean(context.get("entity_type"), 40), _clean(context.get("entity_id"), 100),
                _clean(context.get("entity_url"), 1000), amo_lead_id, _clean(text, 4000),
                _clean(attachment_url, 4000), _clean(attachment_type, 100), now, now, now,
            ),
        )
        row = await (await db.execute(
            "SELECT * FROM outbound_jobs WHERE request_key=?", (request_key,),
        )).fetchone()
        await db.commit()
        result = dict(row) if row else {}
        result["_fresh"] = cursor.rowcount > 0
        result["manager_name"] = _clean(device.get("admin_name"), 200)
        return result
    finally:
        await db.close()


async def _widget_delivery_context(
    data: dict[str, Any], mode: str, device: dict[str, Any], provider: str,
    chat_id: str, channel_id: str, transport: str,
) -> dict[str, Any]:
    context = await _communication_context(
        provider, chat_id, channel_id=channel_id, chat_type=transport,
    )
    card = _widget_context(data, mode, device)
    for key in ("platform", "entity_type", "entity_id", "entity_url"):
        if not context.get(key):
            context[key] = card.get(key, "")
    if not context.get("client_name"):
        context["client_name"] = _clean(data.get("name"), 200)
    if context.get("platform") == "amocrm" and context.get("entity_type") == "lead":
        context["amo_lead_id"] = _clean(context.get("entity_id"), 64)
    return context


def _outbound_duplicate_payload(job: dict[str, Any]) -> dict[str, Any] | None:
    if job.get("_fresh"):
        return None
    status = _clean(job.get("status"), 40)
    if status == "sent":
        return {"ok": True, "sent": True, "duplicate": True, "message": {
            "external_id": _clean(job.get("external_id"), 250), "direction": "outgoing",
            "status": "sent", "text": _clean(job.get("text"), 4000),
            "content_uri": _clean(job.get("attachment_url"), 4000),
            "content_type": _clean(job.get("attachment_type"), 100),
            "author_name": _clean(job.get("manager_name"), 200),
            "sent_at": _clean(job.get("sent_at"), 80) or _iso(),
        }}
    if status in {"pending", "processing", "retry"}:
        return {
            "ok": True, "sent": False, "queued": True, "duplicate": True,
            "notice": "Сообщение уже в очереди. Nexus доставит его автоматически.",
        }
    return None


async def _queued_outbound_response(
    request: Request, job: dict[str, Any], channel: dict[str, Any], device: dict[str, Any],
    *, phone: str = "", page_kind: str = "", entity_id: str = "",
) -> JSONResponse:
    """Acknowledge durable acceptance without waiting for a messenger provider."""

    _outbound_wakeup.set()
    try:
        await _audit(
            "send_message", "queued", admin_id=device["admin_id"], device_id=device["id"],
            page_kind=page_kind, entity_id=entity_id, phone=phone,
        )
    except Exception:
        # The outbound row is the delivery source of truth. An unavailable
        # audit journal must never turn an accepted message into a UI error.
        _log("exception", "Outbound acceptance audit failed")
    return _widget_response(request, {
        "ok": True, "sent": False, "queued": True, "channel": channel,
        "notice": "Сообщение принято. Nexus доставит его в фоне и покажет результат в «Операциях».",
        "message": {
            "external_id": f"queued:{job['request_key']}", "direction": "outgoing",
            "status": "queued", "text": _clean(job.get("text"), 4000),
            "content_uri": _clean(job.get("attachment_url"), 4000),
            "content_type": _clean(job.get("attachment_type"), 100),
            "author_name": _clean(device.get("admin_name"), 200),
            "sent_at": _clean(job.get("queued_at"), 80) or _iso(),
        },
    }, 202)


async def _finish_outbound_job(
    job: dict[str, Any], message: dict[str, Any], *, status: str = "sent", error: str = "",
) -> None:
    now = _iso()
    started = _parse_iso(job.get("queued_at") or job.get("started_at"))
    latency_ms = max(0, int((_now_dt() - started).total_seconds() * 1000)) if started else None
    external_id = _clean(message.get("external_id"), 250)
    db = await _connect()
    try:
        await db.execute(
            """UPDATE outbound_jobs SET status=?,next_attempt_at='',external_id=?,error=?,
               sent_at=?,latency_ms=?,updated_at=? WHERE id=?""",
            (status, external_id, _clean(error, 1000), now if status == "sent" else "", latency_ms, now, job["id"]),
        )
        await db.commit()
    finally:
        await db.close()
    if status == "failed" and job.get("provider") == "wazzup":
        failed_id = f"failed:{_clean(job.get('request_key'), 220)}"
        db = await _connect()
        try:
            await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,
                   content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,'outgoing','failed',?,?,?,?,?,?)
                   ON CONFLICT(external_id) DO UPDATE SET
                   status='failed',text=excluded.text,
                   content_uri=CASE WHEN excluded.content_uri<>'' THEN excluded.content_uri ELSE wazzup_messages.content_uri END,
                   author_name=excluded.author_name,sent_at=excluded.sent_at,raw_json=excluded.raw_json""",
                (
                    failed_id, job["channel_id"], job["chat_type"], _clean(job.get("chat_id"), 250),
                    _phone_hash(job.get("phone")), job["text"], _clean(job.get("attachment_url"), 4000),
                    _clean(job.get("manager_name"), 200), now,
                    json.dumps({
                        "error": _clean(error, 1000),
                        "contentType": _clean(job.get("attachment_type"), 100),
                        "contentUri": _clean(job.get("attachment_url"), 4000),
                    }, ensure_ascii=False), now,
                ),
            )
            await db.commit()
        finally:
            await db.close()
    manager_name = _clean(job.get("manager_name"), 200)
    await _record_communication(
        dedupe_key=f"outbound:{job['request_key']}", external_id=external_id,
        provider=job["provider"], channel_id=job["channel_id"], chat_type=job["chat_type"],
        chat_id=_clean(message.get("chat_id") or job.get("chat_id"), 250),
        direction="outgoing", status=status, text=job["text"], sent_at=message.get("sent_at") or now,
        client_name=job.get("client_name", ""), phone_hash=_phone_hash(job.get("phone")),
        admin_id=int(job["admin_id"]), manager_name=manager_name,
        transport_author=_clean(message.get("author_name"), 200),
        attempts=int(job.get("attempts") or 1), latency_ms=latency_ms, error=error,
        context={
            "platform": job.get("platform", ""), "entity_type": job.get("entity_type", ""),
            "entity_id": job.get("entity_id", ""), "entity_url": job.get("entity_url", ""),
            "amo_lead_id": job.get("amo_lead_id", ""),
        },
    )


async def _fail_or_retry_outbound_job(job: dict[str, Any], error: Any) -> bool:
    attempts = int(job.get("attempts") or 1)
    retry = _delivery_error_is_transient(error) and attempts < OUTBOUND_MAX_ATTEMPTS
    delay = _delivery_retry_delay(error, attempts)
    next_at = _iso(_now_dt() + timedelta(seconds=delay)) if retry else ""
    detail = _clean(getattr(error, "detail", error), 1000) or type(error).__name__
    db = await _connect()
    try:
        await db.execute(
            "UPDATE outbound_jobs SET status=?,next_attempt_at=?,error=?,updated_at=? WHERE id=?",
            ("retry" if retry else "failed", next_at, detail, _iso(), job["id"]),
        )
        await db.commit()
    finally:
        await db.close()
    if retry:
        _outbound_wakeup.set()
    else:
        await _finish_outbound_job(job, {}, status="failed", error=detail)
    return retry


async def _claim_outbound_job() -> dict[str, Any] | None:
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT j.*,a.name AS manager_name FROM outbound_jobs j JOIN admins a ON a.id=j.admin_id
               WHERE j.status IN ('pending','retry') AND (j.next_attempt_at='' OR j.next_attempt_at<=?)
               ORDER BY CASE j.status WHEN 'pending' THEN 0 ELSE 1 END,j.next_attempt_at,j.id LIMIT 1""", (_iso(),),
        )).fetchone()
        if not row:
            return None
        await db.execute("BEGIN IMMEDIATE")
        row = await (await db.execute(
            """SELECT j.*,a.name AS manager_name FROM outbound_jobs j JOIN admins a ON a.id=j.admin_id
               WHERE j.status IN ('pending','retry') AND (j.next_attempt_at='' OR j.next_attempt_at<=?)
               ORDER BY CASE j.status WHEN 'pending' THEN 0 ELSE 1 END,j.next_attempt_at,j.id LIMIT 1""", (_iso(),),
        )).fetchone()
        if not row:
            await db.commit()
            return None
        now = _iso()
        await db.execute(
            "UPDATE outbound_jobs SET status='processing',attempts=attempts+1,started_at=?,updated_at=? WHERE id=?",
            (now, now, row["id"]),
        )
        await db.commit()
        result = dict(row)
        result["attempts"] = int(row["attempts"] or 0) + 1
        result["started_at"] = now
        return result
    finally:
        await db.close()


async def _retry_outbound_job(job: dict[str, Any]) -> None:
    try:
        result = await asyncio.wait_for(
            service_streams_send(
                channel_id=job["channel_id"], transport=job["chat_type"], provider=job["provider"],
                chat_id=job["chat_id"], phone=job["phone"], text=job["text"],
                operator_name=job["manager_name"], email=job["email"],
                gc_user_id=job["getcourse_user_id"], name=job["client_name"],
                attachment_url=job["attachment_url"], attachment_type=job["attachment_type"],
                idempotency_key=job["request_key"], record_communication=False,
            ),
            timeout=45,
        )
        await _finish_outbound_job(job, result.get("message") or {})
    except Exception as exc:
        await _fail_or_retry_outbound_job(job, exc)


async def outbound_delivery_loop(worker_id: int) -> None:
    await asyncio.sleep(2 + worker_id * 0.15)
    while True:
        try:
            job = await _claim_outbound_job()
            if job:
                await _retry_outbound_job(job)
                continue
            _outbound_wakeup.clear()
            try:
                await asyncio.wait_for(_outbound_wakeup.wait(), timeout=3)
            except TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _log("exception", "Outbound worker %s failed", worker_id)
            await asyncio.sleep(3)


def _notification_pairing_value(provider: str, text_value: Any) -> str:
    text_value = _clean(text_value, 500).strip()
    if provider == "telegram":
        match = re.fullmatch(r"/start(?:@[A-Za-z0-9_]+)?\s+nx_([A-Za-z0-9_-]{20,100})", text_value)
        return match.group(1) if match else ""
    match = re.fullmatch(r"(?:NEXUS|НЕКСУС)[-\s]+([A-Z0-9]{8,24})", text_value.upper())
    return match.group(1) if match else ""


async def _consume_notification_pairing(
    provider: str, text_value: Any, recipient_id: Any, label: Any = "", received_at: Any = "",
) -> dict[str, Any] | None:
    code = _notification_pairing_value(provider, text_value)
    recipient_id = _clean(recipient_id, 200)
    if not code or not recipient_id:
        return None
    code_hash = _hash(code)
    now = _iso()
    observed_at = _message_time(received_at) if received_at else now
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        pairing = await (await db.execute(
            """SELECT p.*,a.name,a.enabled FROM notification_pairings p
               JOIN admins a ON a.id=p.admin_id
               WHERE p.provider=? AND p.code_hash=? AND p.used_at=''
                 AND p.created_at<=? AND p.expires_at>=?""",
            (provider, code_hash, observed_at, observed_at),
        )).fetchone()
        if not pairing or not pairing["enabled"]:
            await db.rollback()
            return None
        await db.execute(
            "DELETE FROM notification_destinations WHERE provider=? AND recipient_id=? AND admin_id<>?",
            (provider, recipient_id, pairing["admin_id"]),
        )
        await db.execute(
            """INSERT INTO notification_destinations(
               admin_id,provider,recipient_id,label,enabled,connected_at,verified_at,last_error,updated_at
               ) VALUES(?,?,?,?,1,?,?,?,?) ON CONFLICT(admin_id,provider) DO UPDATE SET
               recipient_id=excluded.recipient_id,label=excluded.label,enabled=1,
               verified_at=excluded.verified_at,last_error='',updated_at=excluded.updated_at""",
            (
                pairing["admin_id"], provider, recipient_id, _clean(label, 200),
                now, now, "", now,
            ),
        )
        await db.execute("UPDATE notification_pairings SET used_at=? WHERE id=?", (now, pairing["id"]))
        await db.commit()
        return {"admin_id": int(pairing["admin_id"]), "admin_name": pairing["name"], "provider": provider}
    finally:
        await db.close()


async def _handle_notification_telegram_update(payload: Any) -> bool:
    """Consume one Bot API update; delayed updates use their original Telegram timestamp."""
    if not isinstance(payload, dict):
        return False
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    sender = message.get("from") if isinstance(message.get("from"), dict) else {}
    chat_id = _clean(chat.get("id"), 200)
    if not chat_id:
        return False
    label = " ".join(filter(None, (
        _clean(sender.get("first_name"), 100), _clean(sender.get("last_name"), 100),
    ))).strip() or ("@" + _clean(sender.get("username"), 100) if sender.get("username") else chat_id)
    timestamp = message.get("date")
    try:
        received_at = _iso(datetime.fromtimestamp(float(timestamp), timezone.utc)) if timestamp else ""
    except (TypeError, ValueError, OverflowError):
        received_at = ""
    paired = await _consume_notification_pairing(
        "telegram", message.get("text"), chat_id, label, received_at=received_at,
    )
    if not paired:
        return False
    try:
        await _notification_tg_call("sendMessage", {
            "chat_id": chat_id,
            "text": f"✅ Уведомления Nexus подключены для {paired['admin_name']}.",
        })
    except NotificationDeliveryError:
        _log("warning", "Telegram notification pairing confirmation failed chat=%s", chat_id)
    return True


async def notification_telegram_poll_loop() -> None:
    """Poll the dedicated notification bot so public webhook reachability cannot block pairing."""
    global _notification_bot_poll_at, _notification_bot_poll_error
    await asyncio.sleep(2.5)
    try:
        offset = max(0, int(await _setting("notification_telegram_update_offset") or 0))
    except (TypeError, ValueError):
        offset = 0
    while True:
        try:
            if not _notification_bot_token():
                _notification_bot_poll_error = "Токен бота уведомлений не задан"
                await asyncio.sleep(15)
                continue
            expected = f"{PUBLIC_API_BASE}/notifications/telegram/{await _setting('notification_telegram_callback_secret')}"
            webhook = await _notification_tg_call("getWebhookInfo")
            current_url = _clean(webhook.get("url"), 4000)
            if current_url and current_url != expected:
                _notification_bot_poll_error = "У бота настроен чужой webhook"
                await asyncio.sleep(30)
                continue
            if current_url == expected:
                await _notification_tg_call("deleteWebhook", {"drop_pending_updates": False})
            result = await _notification_tg_call("getUpdates", {
                "offset": offset, "timeout": 15, "allowed_updates": ["message"],
            })
            updates = result.get("value") if isinstance(result.get("value"), list) else []
            next_offset = offset
            for update in updates:
                if not isinstance(update, dict):
                    continue
                await _handle_notification_telegram_update(update)
                next_offset = max(next_offset, int(update.get("update_id") or -1) + 1)
            if next_offset != offset:
                offset = next_offset
                await _set_setting("notification_telegram_update_offset", str(offset))
            _notification_bot_poll_at = _iso()
            _notification_bot_poll_error = ""
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _notification_bot_poll_error = _clean(exc, 300)
            _log("warning", "Telegram notification polling failed: %s", exc)
            await asyncio.sleep(5)


async def _notification_recipient(provider: str, recipient_id: str) -> bool:
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT 1 FROM notification_destinations WHERE provider=? AND recipient_id=? AND enabled=1",
            (provider, _clean(recipient_id, 200)),
        )).fetchone()
        return bool(row)
    finally:
        await db.close()


async def _notification_settings_view(admin_id: int) -> dict[str, Any]:
    db = await _connect()
    try:
        rows = await (await db.execute(
            "SELECT provider,recipient_id,label,enabled,connected_at,last_error FROM notification_destinations WHERE admin_id=?",
            (admin_id,),
        )).fetchall()
        pref = await (await db.execute(
            "SELECT fallback_unassigned FROM notification_preferences WHERE admin_id=?",
            (admin_id,),
        )).fetchone()
        browser_rows = await (await db.execute(
            """SELECT label,enabled,last_seen_at FROM browser_notification_subscriptions
               WHERE admin_id=? ORDER BY enabled DESC,last_seen_at DESC""",
            (admin_id,),
        )).fetchall()
    finally:
        await db.close()
    destinations = {
        provider: {"connected": False, "enabled": False, "label": "", "last_error": ""}
        for provider in ("telegram", "vk", "browser")
    }
    for row in rows:
        destinations[row["provider"]] = {
            "connected": True, "enabled": bool(row["enabled"]),
            "label": row["label"] or row["recipient_id"],
            "connected_at": row["connected_at"], "last_error": row["last_error"],
        }
    enabled_browsers = [row for row in browser_rows if row["enabled"]]
    destinations["browser"] = {
        "connected": bool(browser_rows),
        "enabled": bool(enabled_browsers),
        "label": (
            (enabled_browsers[0]["label"] or "этот браузер")
            if len(enabled_browsers) == 1 else f"браузеров: {len(enabled_browsers)}"
        ) if enabled_browsers else "",
        "last_seen_at": enabled_browsers[0]["last_seen_at"] if enabled_browsers else "",
        "last_error": "",
    }
    return {
        "destinations": destinations,
        "fallback_unassigned": bool(pref["fallback_unassigned"]) if pref else False,
        "telegram_bot": f"@{NOTIFY_TELEGRAM_USERNAME}",
    }


async def _browser_notification_subscription(request: Request, *, allow_disabled: bool = True) -> dict[str, Any]:
    auth = _clean(request.headers.get("authorization"), 5000)
    if not auth.lower().startswith("bearer "):
        raise HTTPException(401, "Страница уведомлений не подключена")
    raw = auth[7:].strip()
    if len(raw) < 32:
        raise HTTPException(401, "Страница уведомлений не подключена")
    now = _iso()
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT s.*,a.name AS admin_name,a.enabled AS admin_enabled,d.revoked_at
               FROM browser_notification_subscriptions s
               JOIN admins a ON a.id=s.admin_id JOIN devices d ON d.id=s.device_id
               WHERE s.token_hash=?""",
            (_hash(raw),),
        )).fetchone()
        if not row or not row["admin_enabled"] or row["revoked_at"] or (not allow_disabled and not row["enabled"]):
            raise HTTPException(401, "Страница уведомлений отключена. Откройте её заново из виджета Nexus")
        result = dict(row)
        last_seen = _parse_iso(row["last_seen_at"])
        if not last_seen or last_seen <= _now_dt() - timedelta(seconds=60):
            try:
                await db.execute(
                    "UPDATE browser_notification_subscriptions SET last_seen_at=?,updated_at=? WHERE id=?",
                    (now, now, row["id"]),
                )
                await db.commit()
                result["last_seen_at"] = now
            except sqlite3.OperationalError:
                await db.rollback()
        return result
    finally:
        await db.close()


async def _browser_notification_status(subscription: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "enabled": bool(subscription.get("enabled")),
        "manager": _clean(subscription.get("admin_name"), 200),
        "label": _clean(subscription.get("label"), 200),
        "last_seen_at": _clean(subscription.get("last_seen_at"), 80),
        "subscribed": bool(subscription.get("push_endpoint") and subscription.get("push_p256dh") and subscription.get("push_auth")),
        "vapid_public_key": await _setting("webpush_vapid_public"),
    }


@router.post("/widget/notifications/browser/open")
async def widget_notification_browser_open(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        raw = secrets.token_urlsafe(36)
        now = _iso()
        label = f"{device['admin_name']} · браузер"
        db = await _connect()
        try:
            await db.execute(
                """INSERT INTO browser_notification_subscriptions(
                   admin_id,device_id,token_hash,token_hint,label,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?) ON CONFLICT(device_id) DO UPDATE SET
                   admin_id=excluded.admin_id,token_hash=excluded.token_hash,token_hint=excluded.token_hint,
                   label=excluded.label,updated_at=excluded.updated_at""",
                (device["admin_id"], device["id"], _hash(raw), raw[-6:], label, now, now),
            )
            await db.commit()
        finally:
            await db.close()
        public_root = PUBLIC_API_BASE.rsplit("/api", 1)[0]
        url = f"{public_root}/static/notifications.html#token={quote(raw, safe='')}"
        return _widget_response(request, {"ok": True, "url": url})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


@router.post("/browser-notifications/status")
async def browser_notification_status(request: Request) -> JSONResponse:
    try:
        return JSONResponse(await _browser_notification_status(await _browser_notification_subscription(request)))
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)


@router.post("/browser-notifications/enable")
async def browser_notification_enable(request: Request) -> JSONResponse:
    try:
        subscription = await _browser_notification_subscription(request)
        data = await _read_json(request)
        label = _clean(data.get("label"), 200) or _clean(subscription.get("label"), 200) or "Браузер"
        push = data.get("subscription") if isinstance(data.get("subscription"), dict) else {}
        keys = push.get("keys") if isinstance(push.get("keys"), dict) else {}
        endpoint = _clean(push.get("endpoint"), 4000)
        p256dh = _clean(keys.get("p256dh"), 1000)
        auth_secret = _clean(keys.get("auth"), 1000)
        if not endpoint or not p256dh or not auth_secret:
            raise HTTPException(400, "Браузер не создал подписку Web Push")
        try:
            await _require_public_push_endpoint(endpoint)
        except NotificationDeliveryError as exc:
            raise HTTPException(400 if exc.permanent else 503, str(exc)) from exc
        try:
            if len(_b64url_decode(p256dh)) != 65 or len(_b64url_decode(auth_secret)) < 16:
                raise ValueError
        except Exception as exc:
            raise HTTPException(400, "Браузер передал повреждённые ключи") from exc
        now = _iso()
        enabled_at = subscription.get("enabled_at") if subscription.get("enabled") else now
        db = await _connect()
        try:
            await db.execute(
                """UPDATE browser_notification_subscriptions SET enabled=1,enabled_at=?,label=?,
                   push_endpoint=?,push_p256dh=?,push_auth=?,last_seen_at=?,updated_at=? WHERE id=?""",
                (enabled_at or now, label, endpoint, p256dh, auth_secret, now, now, subscription["id"]),
            )
            await db.commit()
        finally:
            await db.close()
        subscription.update({"enabled": 1, "enabled_at": enabled_at or now, "label": label, "last_seen_at": now,
                             "push_endpoint": endpoint, "push_p256dh": p256dh, "push_auth": auth_secret})
        return JSONResponse(await _browser_notification_status(subscription))
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)


@router.post("/browser-notifications/disable")
async def browser_notification_disable(request: Request) -> JSONResponse:
    try:
        subscription = await _browser_notification_subscription(request)
        db = await _connect()
        try:
            await db.execute(
                "UPDATE browser_notification_subscriptions SET enabled=0,updated_at=? WHERE id=?",
                (_iso(), subscription["id"]),
            )
            await db.commit()
        finally:
            await db.close()
        subscription["enabled"] = 0
        return JSONResponse(await _browser_notification_status(subscription))
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)


@router.post("/browser-notifications/test")
async def browser_notification_test(request: Request) -> JSONResponse:
    try:
        subscription = await _browser_notification_subscription(request, allow_disabled=False)
        external_id = await _send_webpush_destination(subscription, {
            "title": "Тест Nexus", "body": "Уведомления работают. Эту страницу теперь можно закрыть.",
            "url": "", "tag": "nexus-webpush-test",
        })
        return JSONResponse({"ok": True, "external_id": external_id})
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)
    except NotificationDeliveryError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)


@router.post("/browser-notifications/feed")
async def browser_notification_feed(request: Request) -> JSONResponse:
    try:
        subscription = await _browser_notification_subscription(request, allow_disabled=False)
        now = _iso()
        stale_claim = _iso(_now_dt() - timedelta(seconds=BROWSER_NOTIFY_CLAIM_SECONDS))
        claim_marker = now + ":" + secrets.token_hex(4)
        db = await _connect()
        try:
            pref = await (await db.execute(
                "SELECT fallback_unassigned FROM notification_preferences WHERE admin_id=?",
                (subscription["admin_id"],),
            )).fetchone()
            fallback = bool(pref and pref["fallback_unassigned"])
            candidates = await (await db.execute(
                """SELECT e.* FROM notification_events e
                   LEFT JOIN browser_notification_deliveries d
                     ON d.subscription_id=? AND d.event_id=e.external_id
                   WHERE e.created_at>=? AND (
                     e.target_admin_id=? OR (
                       e.target_admin_id IS NULL AND ?=1 AND EXISTS(
                         SELECT 1 FROM conversation_contexts c
                         WHERE c.provider=e.source AND c.external_user_id=e.chat_id
                           AND c.platform='amocrm' AND c.entity_type='lead' AND c.entity_id<>''
                       )
                     )
                   )
                     AND (d.id IS NULL OR d.status='pending' OR (d.status='claimed' AND d.claimed_at<=?))
                   ORDER BY e.created_at,e.external_id LIMIT ?""",
                (subscription["id"], subscription["enabled_at"], subscription["admin_id"],
                 1 if fallback else 0, stale_claim, BROWSER_NOTIFY_PAGE_SIZE),
            )).fetchall()
            rows = []
            if candidates:
                await db.execute("BEGIN IMMEDIATE")
                await db.executemany(
                    """INSERT OR IGNORE INTO browser_notification_deliveries(
                       subscription_id,event_id,status,created_at,updated_at
                       ) VALUES(?,?,'pending',?,?)""",
                    ((subscription["id"], row["external_id"], now, now) for row in candidates),
                )
                placeholders = ",".join("?" for _ in candidates)
                await db.execute(
                    f"""UPDATE browser_notification_deliveries SET status='claimed',claimed_at=?,updated_at=?
                        WHERE subscription_id=? AND event_id IN ({placeholders})
                          AND (status='pending' OR (status='claimed' AND claimed_at<=?))""",
                    (claim_marker, now, subscription["id"], *(row["external_id"] for row in candidates), stale_claim),
                )
                rows = await (await db.execute(
                    """SELECT e.* FROM browser_notification_deliveries d
                       JOIN notification_events e ON e.external_id=d.event_id
                       WHERE d.subscription_id=? AND d.claimed_at=? ORDER BY e.created_at,e.external_id""",
                    (subscription["id"], claim_marker),
                )).fetchall()
                await db.commit()
        finally:
            await db.close()
        notifications = []
        for row in rows:
            item = dict(row)
            context = await _notification_context(item["source"], item["chat_id"])
            body = _clean(item.get("text"), 500) or {
                "audio": "Голосовое сообщение", "voice": "Голосовое сообщение",
                "image": "Изображение", "video": "Видео", "document": "Вложение",
            }.get(_clean(item.get("content_type"), 100).lower(), "Новое вложение")
            notifications.append({
                "event_id": item["external_id"],
                "title": f"{_notification_label(item['source'])} · {_clean(item.get('client_name'), 120) or 'Клиент'}",
                "body": body,
                "url": _clean(context.get("entity_url"), 2000),
                "sent_at": item["sent_at"],
            })
        return JSONResponse({"ok": True, "notifications": notifications, "checked_at": now})
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)


@router.post("/browser-notifications/ack")
async def browser_notification_ack(request: Request) -> JSONResponse:
    try:
        subscription = await _browser_notification_subscription(request, allow_disabled=False)
        data = await _read_json(request)
        event_ids = [_clean(value, 250) for value in data.get("event_ids", []) if _clean(value, 250)][:BROWSER_NOTIFY_PAGE_SIZE]
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            now = _iso()
            db = await _connect()
            try:
                await db.execute(
                    f"UPDATE browser_notification_deliveries SET status='shown',shown_at=?,updated_at=? WHERE subscription_id=? AND event_id IN ({placeholders})",
                    (now, now, subscription["id"], *event_ids),
                )
                await db.commit()
            finally:
                await db.close()
        return JSONResponse({"ok": True})
    except HTTPException as exc:
        return JSONResponse({"ok": False, "error": str(exc.detail)}, status_code=exc.status_code)


async def _notification_widget_device(request: Request) -> tuple[str, dict[str, Any]]:
    mode = await _widget_request_mode(request)
    if not mode:
        raise HTTPException(403, "origin not allowed")
    device = await _device(request)
    if not device:
        raise HTTPException(401, "Требуется повторная активация")
    return mode, device


@router.post("/widget/notifications/settings")
async def widget_notification_settings(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        data = await _read_json(request)
        if "fallback_unassigned" in data:
            if device["admin_role"] != "admin":
                raise HTTPException(403, "Резервные уведомления доступны администратору")
            db = await _connect()
            try:
                await db.execute(
                    """INSERT INTO notification_preferences(admin_id,fallback_unassigned,updated_at)
                       VALUES(?,?,?) ON CONFLICT(admin_id) DO UPDATE SET
                       fallback_unassigned=excluded.fallback_unassigned,updated_at=excluded.updated_at""",
                    (device["admin_id"], 1 if data.get("fallback_unassigned") else 0, _iso()),
                )
                await db.commit()
            finally:
                await db.close()
        return _widget_response(request, {
            "ok": True, "admin_role": device["admin_role"],
            **await _notification_settings_view(int(device["admin_id"])),
        })
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


@router.put("/widget/notifications/settings")
async def widget_notification_settings_save(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        data = await _read_json(request)
        if device["admin_role"] != "admin" and "fallback_unassigned" in data:
            raise HTTPException(403, "Резервные уведомления доступны администратору")
        db = await _connect()
        try:
            await db.execute(
                """INSERT INTO notification_preferences(admin_id,fallback_unassigned,updated_at)
                   VALUES(?,?,?) ON CONFLICT(admin_id) DO UPDATE SET
                   fallback_unassigned=excluded.fallback_unassigned,updated_at=excluded.updated_at""",
                (device["admin_id"], 1 if data.get("fallback_unassigned") else 0, _iso()),
            )
            await db.commit()
        finally:
            await db.close()
        return _widget_response(request, {"ok": True, **await _notification_settings_view(int(device["admin_id"]))})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


@router.post("/widget/notifications/pair")
async def widget_notification_pair(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        data = await _read_json(request)
        provider = _clean(data.get("provider"), 40).lower()
        if provider not in {"telegram", "vk"}:
            raise HTTPException(400, "Выберите Telegram или VK")
        if provider == "telegram":
            if not _notification_bot_token():
                raise HTTPException(503, "Администратор ещё не подключил Telegram-бот уведомлений")
            me = await _notification_tg_call("getMe")
            username = _clean(me.get("username"), 200)
            if username.casefold() != NOTIFY_TELEGRAM_USERNAME.casefold():
                raise HTTPException(503, f"В Nexus подключён другой Telegram-бот: @{username or 'unknown'}")
        if provider == "vk" and (not _vk_token() or not _vk_group_id()):
            raise HTTPException(503, "Сообщество VK ещё не подключено")
        raw = secrets.token_urlsafe(24) if provider == "telegram" else "".join(secrets.choice(CODE_ALPHABET) for _ in range(10))
        now = _iso()
        expires_at = _iso(_now_dt() + timedelta(minutes=NOTIFY_PAIRING_TTL_MINUTES))
        db = await _connect()
        try:
            await db.execute(
                "DELETE FROM notification_pairings WHERE admin_id=? AND provider=? AND used_at=''",
                (device["admin_id"], provider),
            )
            await db.execute(
                "INSERT INTO notification_pairings(admin_id,provider,code_hash,expires_at,created_at) VALUES(?,?,?,?,?)",
                (device["admin_id"], provider, _hash(raw), expires_at, now),
            )
            await db.commit()
        finally:
            await db.close()
        if provider == "telegram":
            url = f"https://t.me/{NOTIFY_TELEGRAM_USERNAME}?start=nx_{raw}"
            command = "/start"
        else:
            url = f"https://vk.me/club{_vk_group_id()}"
            command = f"NEXUS-{raw}"
        return _widget_response(request, {
            "ok": True, "provider": provider, "url": url,
            "command": command, "expires_at": expires_at,
        })
    except NotificationDeliveryError as exc:
        return _widget_response(request, {"ok": False, "error": str(exc)}, 503)
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


@router.post("/widget/notifications/disconnect")
async def widget_notification_disconnect(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        data = await _read_json(request)
        provider = _clean(data.get("provider"), 40).lower()
        if provider not in {"telegram", "vk", "browser"}:
            raise HTTPException(400, "Выберите Telegram, VK или браузер")
        db = await _connect()
        try:
            if provider == "browser":
                await db.execute(
                    "UPDATE browser_notification_subscriptions SET enabled=0,updated_at=? WHERE admin_id=?",
                    (_iso(), device["admin_id"]),
                )
            else:
                await db.execute(
                    "DELETE FROM notification_destinations WHERE admin_id=? AND provider=?",
                    (device["admin_id"], provider),
                )
            await db.commit()
        finally:
            await db.close()
        return _widget_response(request, {"ok": True, **await _notification_settings_view(int(device["admin_id"]))})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


@router.post("/widget/notifications/test")
async def widget_notification_test(request: Request) -> JSONResponse:
    try:
        _, device = await _notification_widget_device(request)
        data = await _read_json(request)
        provider = _clean(data.get("provider"), 40).lower()
        destinations = await _notification_destinations(int(device["admin_id"]))
        destination = next((row for row in destinations if row["provider"] == provider), None)
        if not destination:
            raise HTTPException(404, "Сначала подключите этот канал")
        await _send_notification_destination(
            provider, destination["recipient_id"],
            "✅ Тест Nexus\nУведомления о новых сообщениях подключены.", [],
        )
        return _widget_response(request, {"ok": True, "message": "Тестовое уведомление отправлено"})
    except NotificationDeliveryError as exc:
        return _widget_response(request, {"ok": False, "error": str(exc)}, 502)
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail), "reauth": exc.status_code == 401}, exc.status_code)


def _auto_markup_domains(value: Any) -> set[str]:
    """Normalize the explicit host allow-list without accepting URL paths."""
    domains: set[str] = set()
    for raw in _clean(value, 2000).split(";"):
        host = raw.strip().lower().removeprefix("http://").removeprefix("https://").split("/", 1)[0]
        host = host.removeprefix("www.").rstrip(".")
        if re.fullmatch(r"[a-z0-9.-]{1,253}", host) and "." in host:
            domains.add(host)
    return domains


def _auto_markup_tail(value: Any) -> str:
    tail = _clean(value, 2000).strip()
    if not tail:
        return ""
    if "#" in tail or any(char.isspace() for char in tail):
        raise HTTPException(400, "Хвост разметки не должен содержать пробелы или #")
    return tail if tail.startswith(("?", "&")) else "?" + tail


def _url_needs_auto_markup(url: str, domains: set[str]) -> bool:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower().removeprefix("www.").rstrip(".")
    return bool(host and any(host == domain or host.endswith("." + domain) for domain in domains))


def _missing_auto_markup_tail(query: str, tail: str) -> str:
    """Return raw configured query parts whose keys are absent from the URL."""
    existing_keys = {
        unquote(part.partition("=")[0]).casefold()
        for part in query.split("&")
        if part.partition("=")[0]
    }
    missing: list[str] = []
    for part in tail.lstrip("?&").split("&"):
        key = unquote(part.partition("=")[0]).casefold()
        if not key or key in existing_keys:
            continue
        missing.append(part)
        existing_keys.add(key)
    return "&".join(missing)


def _apply_auto_markup(text: Any, domains_value: Any, tail_value: Any) -> str:
    """Complete allow-listed URLs with parameters missing from the configured tail."""
    source = _clean(text, 20_000)
    domains = _auto_markup_domains(domains_value)
    tail = _auto_markup_tail(tail_value)
    if not source or not domains or not tail:
        return source

    def replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        suffix = ""
        while raw and raw[-1] in ".,;:!?)]}":
            suffix = raw[-1] + suffix
            raw = raw[:-1]
        if not raw or not _url_needs_auto_markup(raw, domains):
            return raw + suffix
        parsed = urlsplit(raw)
        missing_tail = _missing_auto_markup_tail(parsed.query, tail)
        if not missing_tail:
            return raw + suffix
        appended = ("&" if parsed.query else "?") + missing_tail
        base = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))
        marked = base + appended + ("#" + parsed.fragment if parsed.fragment else "")
        return marked + suffix

    return AUTO_MARKUP_URL_RE.sub(replace, source)


async def _auto_markup_message(text: Any) -> str:
    domains, tail = await asyncio.gather(_setting("auto_markup_domains"), _setting("auto_markup_tail"))
    return _apply_auto_markup(text, domains, tail)


async def _auto_markup_for_send(text: Any, variables: dict[str, Any]) -> str:
    """Render only the configured automatic tail before appending it to a message."""
    return (await _auto_markup_values_for_send([text], variables))[0]


async def _auto_markup_values_for_send(values: list[Any], variables: dict[str, Any]) -> list[str]:
    """Apply one resolved attribution tail to several pieces of one message."""
    domains, tail = await asyncio.gather(_setting("auto_markup_domains"), _setting("auto_markup_tail"))
    rendered_tail = render_message_template(tail, variables).get("text", "") if tail else ""
    return [_apply_auto_markup(value, domains, rendered_tail) for value in values]


def _channel_delivery_rank(channel: dict[str, Any]) -> int:
    """Order channels by confidence without changing their relative provider order."""
    confirmed_chat = channel.get("confirmed_chat")
    if confirmed_chat is None:
        confirmed_chat = channel.get("has_chat")
    if confirmed_chat and not channel.get("pending"):
        return 0
    hint = " ".join((
        _clean(channel.get("label"), 300),
        _clean(channel.get("send_reason"), 500),
    )).casefold()
    if (
        channel.get("pending")
        or channel.get("provider") == "wazzup"
        or any(marker in hint for marker in ("найти", "попроб", "провер"))
    ):
        return 2
    if channel.get("can_send") is False:
        return 3
    return 1


def _prioritize_channels(channels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        channel for _, channel in sorted(
            enumerate(channels), key=lambda item: (_channel_delivery_rank(item[1]), item[0]),
        )
    ]


async def _refresh_vk_callback_config() -> None:
    _vk_callback_config.update(
        key=await _setting("vk_callback_key"),
        secret=await _setting("vk_callback_secret"),
        confirmation=await _setting("vk_confirmation_code"),
    )


def _vk_callback_queue_path() -> Path:
    return _must_db().with_name("vk-callback-queue.db")


async def _connect_vk_callback_queue():
    db = await aiosqlite.connect(_vk_callback_queue_path(), timeout=5)
    await db.execute("PRAGMA busy_timeout=5000")
    db.row_factory = aiosqlite.Row
    return db


async def _init_vk_callback_queue() -> None:
    async with _vk_queue_lock:
        db = await _connect_vk_callback_queue()
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute(
                """CREATE TABLE IF NOT EXISTS callback_events (
                   event_key TEXT PRIMARY KEY,
                   payload_json TEXT NOT NULL,
                   attempts INTEGER NOT NULL DEFAULT 0,
                   available_at TEXT NOT NULL,
                   created_at TEXT NOT NULL,
                   last_error TEXT NOT NULL DEFAULT ''
                )"""
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS ix_callback_events_ready "
                "ON callback_events(available_at,created_at)"
            )
            await db.commit()
        finally:
            await db.close()


async def _enqueue_vk_callback(body: bytes, payload: dict[str, Any]) -> None:
    event_key = hashlib.sha256(body).hexdigest()
    now = _iso()
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    queue = _vk_callback_write_queue
    writer_task = _vk_callback_writer_task
    if queue is not None and writer_task is not None and not writer_task.done():
        future: asyncio.Future[Exception | None] = asyncio.get_running_loop().create_future()
        try:
            queue.put_nowait((event_key, payload_json, now, future))
        except asyncio.QueueFull as exc:
            raise RuntimeError("VK callback durable queue is busy") from exc
        done, _pending = await asyncio.wait(
            {future, writer_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        if future not in done:
            try:
                writer_error = writer_task.exception()
            except asyncio.CancelledError:
                writer_error = None
            raise RuntimeError("VK callback writer stopped before durable commit") from writer_error
        error = future.result()
        if error is not None:
            raise error
        return
    async with _vk_queue_lock:
        db = await _connect_vk_callback_queue()
        try:
            await db.execute(
                "INSERT OR IGNORE INTO callback_events"
                "(event_key,payload_json,available_at,created_at) VALUES(?,?,?,?)",
                (event_key, payload_json, now, now),
            )
            await db.commit()
        finally:
            await db.close()


async def vk_callback_writer_loop() -> None:
    """Commit callback bursts in small batches before acknowledging VK."""

    queue = _vk_callback_write_queue
    if queue is None:
        return
    db: aiosqlite.Connection | None = None
    try:
        db = await _connect_vk_callback_queue()
        while True:
            batch = [await queue.get()]
            while len(batch) < 100:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                async with _vk_queue_lock:
                    await db.executemany(
                        "INSERT OR IGNORE INTO callback_events"
                        "(event_key,payload_json,available_at,created_at) VALUES(?,?,?,?)",
                        [(event_key, payload_json, created_at, created_at)
                         for event_key, payload_json, created_at, _future in batch],
                    )
                    await db.commit()
            except asyncio.CancelledError:
                for _event_key, _payload_json, _created_at, future in batch:
                    if not future.done():
                        future.set_result(RuntimeError("VK callback writer stopped"))
                raise
            except Exception as exc:
                try:
                    await db.rollback()
                except Exception:
                    pass
                for _event_key, _payload_json, _created_at, future in batch:
                    if not future.done():
                        future.set_result(exc)
            else:
                for _event_key, _payload_json, _created_at, future in batch:
                    if not future.done():
                        future.set_result(None)
            finally:
                for _item in batch:
                    queue.task_done()
    finally:
        if db is not None:
            await db.close()
        while True:
            try:
                _event_key, _payload_json, _created_at, future = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not future.done():
                future.set_result(RuntimeError("VK callback writer stopped"))
            queue.task_done()


async def _process_vk_callback_payload(payload: dict[str, Any]) -> None:
    obj = payload.get("object") if isinstance(payload.get("object"), dict) else {}
    message = obj.get("message") if isinstance(obj.get("message"), dict) else obj
    peer_id = _clean(message.get("peer_id"), 200) if isinstance(message, dict) else ""
    if peer_id and isinstance(message, dict):
        paired = await _consume_notification_pairing(
            "vk", message.get("text"), peer_id, f"VK {peer_id}",
        )
        if paired:
            await _vk_request("messages.send", {
                "group_id": _vk_group_id(), "peer_id": peer_id,
                "random_id": secrets.randbelow(2_000_000_000) + 1,
                "message": f"✅ Уведомления Nexus подключены для {paired['admin_name']}.",
            })
            return
        if bool(message.get("out")) and await _notification_recipient("vk", peer_id):
            return
    course_context: dict[str, Any] = {}
    if peer_id.isdigit() and int(peer_id) > 2_000_000_000:
        reply_message = message.get("reply_message") if isinstance(message.get("reply_message"), dict) else {}
        course_context = await _course_chat_context(
            "vk", peer_id, text=_clean(message.get("text"), 5000),
            reply_sender_id=_clean(reply_message.get("from_id"), 200), require_addressed=True,
        )
        if not course_context:
            return
        sender_id = _clean(message.get("from_id"), 200) if isinstance(message, dict) else ""
        sender_name = "Участник"
        sender_screen_name = ""
        if sender_id.isdigit() and int(sender_id) < 2_000_000_000:
            try:
                profiles = await _vk_request("users.get", {"user_ids": sender_id})
                profile = profiles[0] if isinstance(profiles, list) and profiles else {}
                sender_name = " ".join(filter(None, (
                    _clean(profile.get("first_name"), 120), _clean(profile.get("last_name"), 120),
                ))).strip() or sender_name
                sender_screen_name = _clean(profile.get("screen_name"), 200).lstrip("@").casefold()
            except Exception:
                pass
        course_context["suppress_notification"] = bool(
            sender_id and (
                sender_id == _clean(course_context.get("curator_vk_id"), 200)
                or sender_screen_name == _clean(course_context.get("curator_vk_ref"), 200).lstrip("@").casefold()
            )
        )
        link = {
            "name": f"{sender_name} · {_clean(course_context.get('title'), 300) or 'учебный чат'}",
            "external_user_id": peer_id,
        }
    else:
        link = await _external_link(peer_id=peer_id)
    if not link and peer_id:
        identity = resolve_vk_identity(peer_id)
        await _remember_external_link(
            identity or {"name": f"VK {peer_id}"}, "callback", external_user_id=peer_id,
        )
        link = await _external_link(peer_id=peer_id)
    if peer_id and link and isinstance(message, dict):
        await _store_vk_messages(peer_id, [message], link, course_context=course_context)


async def _drain_vk_callback_queue(limit: int = 50) -> int:
    async with _vk_queue_lock:
        db = await _connect_vk_callback_queue()
        try:
            rows = await (
                await db.execute(
                    "SELECT * FROM callback_events WHERE available_at<=? "
                    "ORDER BY created_at LIMIT ?",
                    (_iso(), limit),
                )
            ).fetchall()
        finally:
            await db.close()
    processed = 0
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
            await _process_vk_callback_payload(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempts = int(row["attempts"] or 0) + 1
            delay = min(300, 2 ** min(attempts, 8))
            available_at = _iso(_now_dt() + timedelta(seconds=delay))
            async with _vk_queue_lock:
                db = await _connect_vk_callback_queue()
                try:
                    await db.execute(
                        "UPDATE callback_events SET attempts=?,available_at=?,last_error=? "
                        "WHERE event_key=?",
                        (attempts, available_at, _clean(exc, 500), row["event_key"]),
                    )
                    await db.commit()
                finally:
                    await db.close()
            continue
        async with _vk_queue_lock:
            db = await _connect_vk_callback_queue()
            try:
                await db.execute("DELETE FROM callback_events WHERE event_key=?", (row["event_key"],))
                await db.commit()
            finally:
                await db.close()
        processed += 1
    return processed


async def vk_callback_queue_loop() -> None:
    while True:
        processed = await _drain_vk_callback_queue()
        await asyncio.sleep(0.1 if processed else 1.0)


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
    match = re.search(r"(?:vk\.com|vk\.ru|vkontakte\.ru)/([A-Za-z0-9_.]+)", text, re.I)
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


async def _external_link(
    *, peer_id: str = "", gc_id: str = "", provider: str = "vk",
    db: aiosqlite.Connection | None = None,
) -> dict[str, str]:
    if not peer_id and not gc_id:
        return {}
    provider = _clean(provider, 40)
    owns_db = db is None
    if db is None:
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
        if owns_db:
            await db.close()


async def _external_link_for_identity(
    provider: str, *, phone: str = "", gc_id: str = "",
    db: aiosqlite.Connection | None = None,
) -> dict[str, str]:
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
    owns_db = db is None
    if db is None:
        db = await _connect()
    try:
        row = await (await db.execute(
            f"SELECT * FROM external_identity_links WHERE provider=? AND ({' OR '.join(clauses)}) ORDER BY updated_at DESC LIMIT 1",
            params,
        )).fetchone()
        return dict(row) if row else {}
    finally:
        if owns_db:
            await db.close()


async def _entity_external_link(
    platform: str,
    entity_type: str,
    entity_id: str,
    provider: str,
    *,
    db: aiosqlite.Connection | None = None,
) -> dict[str, str]:
    if not platform or not entity_type or not entity_id or not provider:
        return {}
    owns_db = db is None
    if db is None:
        db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT external_user_id FROM entity_identity_links
               WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?""",
            (platform, entity_type, entity_id, provider),
        )).fetchone()
    finally:
        if owns_db:
            await db.close()
    return await _external_link(
        peer_id=row["external_user_id"], provider=provider,
        db=db if not owns_db else None,
    ) if row else {}


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
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            """INSERT INTO entity_identity_links(platform,entity_type,entity_id,provider,external_user_id,confirmed_by,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(platform,entity_type,entity_id,provider) DO UPDATE SET
               external_user_id=excluded.external_user_id,confirmed_by=excluded.confirmed_by,updated_at=excluded.updated_at""",
            (platform, entity_type, entity_id, provider, external_user_id, admin_id, now, now),
        )
        await db.execute(
            """DELETE FROM conversation_contexts
               WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?
                 AND external_user_id<>?""",
            (platform, entity_type, entity_id, provider, external_user_id),
        )
        await db.commit()
    finally:
        await db.close()


async def _forget_entity_external_link(context: dict[str, Any], provider: str) -> None:
    platform = _clean(context.get("platform"), 40)
    entity_type = _clean(context.get("entity_type"), 40)
    entity_id = _clean(context.get("entity_id"), 200)
    if not platform or not entity_type or not entity_id or not provider:
        return
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT external_user_id FROM entity_identity_links
               WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?""",
            (platform, entity_type, entity_id, provider),
        )).fetchone()
        await db.execute(
            "DELETE FROM entity_identity_links WHERE platform=? AND entity_type=? AND entity_id=? AND provider=?",
            (platform, entity_type, entity_id, provider),
        )
        if row:
            await db.execute(
                """DELETE FROM conversation_contexts
                   WHERE provider=? AND external_user_id=? AND platform=? AND entity_type=? AND entity_id=?""",
                (provider, row["external_user_id"], platform, entity_type, entity_id),
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
    private = db_path.parent / "telegram-personal.session"
    if private.is_file():
        return private
    if len(db_path.parents) >= 3:
        shared = db_path.parents[2] / "course-chat-creator" / "data" / "telegram.session"
        if shared.is_file():
            staging = private.with_name(f".{private.name}.{os.getpid()}.tmp")
            try:
                with sqlite3.connect(
                    f"file:{shared.resolve().as_posix()}?mode=ro", uri=True, timeout=10
                ) as source, sqlite3.connect(staging, timeout=10) as target:
                    source.backup(target)
                    if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise sqlite3.DatabaseError("Telegram session copy failed quick_check")
                os.chmod(staging, 0o600)
                os.replace(staging, private)
                _log("info", "Telegram Personal session isolated from the shared module session")
                return private
            except Exception as exc:
                staging.unlink(missing_ok=True)
                _log("warning", "Telegram Personal session isolation failed: %s", type(exc).__name__)
                return shared
    return private


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
    if _telegram_dialog_hidden(identity):
        return None
    peer_id = _clean(identity.get("telegram_id"), 200)
    username = _clean(identity.get("telegram_username"), 200).lstrip("@").casefold()
    phone = _normalize_phone(identity.get("phone"))
    for reference in (int(peer_id) if peer_id.isdigit() else None, username or None):
        if reference is None:
            continue
        try:
            entity = await client.get_entity(reference)
            if (
                _telegram_is_user(entity)
                and _clean(getattr(entity, "id", ""), 200)
                and not _telegram_dialog_hidden(_telegram_user_view(entity))
            ):
                return entity
        except Exception:
            pass
    if phone:
        try:
            from telethon.tl.functions.contacts import ResolvePhoneRequest

            result = await client(ResolvePhoneRequest(phone=phone.removeprefix("+")))
            for entity in getattr(result, "users", []) or []:
                if (
                    _telegram_is_user(entity)
                    and _clean(getattr(entity, "id", ""), 200)
                    and not _telegram_dialog_hidden(_telegram_user_view(entity))
                ):
                    return entity
        except Exception as exc:
            _log("warning", "Telegram phone resolve failed: %s", type(exc).__name__)
    async for dialog in client.iter_dialogs(limit=TELEGRAM_DIALOG_LIMIT):
        entity = getattr(dialog, "entity", None)
        if not _telegram_is_user(entity):
            continue
        view = _telegram_user_view(entity)
        if not view["id"] or _telegram_dialog_hidden(view):
            continue
        if (
            (peer_id and view["id"] == peer_id)
            or (username and view["username"].casefold() == username)
            or (phone and view["phone"] == phone)
        ):
            return entity
    return None


async def _telegram_import_phone(client: Any, identity: dict[str, Any]) -> Any | None:
    if _telegram_dialog_hidden(identity):
        return None
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
        if (
            _telegram_is_user(entity)
            and _clean(getattr(entity, "id", ""), 200)
            and not _telegram_dialog_hidden(_telegram_user_view(entity))
        ):
            return entity
    return None


async def _telegram_store_messages(
    channel: dict[str, str],
    entity: Any,
    rows: list[Any],
    link: dict[str, Any],
    *,
    outgoing_author_name: str = "",
    outgoing_attachment_url: str = "",
    outgoing_attachment_type: str = "",
    course_context: dict[str, Any] | None = None,
) -> int:
    if _telegram_is_user(entity):
        peer = _telegram_user_view(entity)
    else:
        peer = {
            "id": _clean(getattr(entity, "id", ""), 200),
            "name": _clean(getattr(entity, "title", "") or getattr(entity, "name", ""), 300),
            "username": _clean(getattr(entity, "username", ""), 200),
            "phone": "",
        }
    if _telegram_dialog_hidden(peer):
        return 0
    peer_id = peer["id"]
    phone = _normalize_phone(link.get("phone") or peer.get("phone"))
    phone_hash = _phone_hash(phone)
    # The Telegram peer is authoritative for the dialog title.  A CRM card
    # label can be stale or belong to another deal and must not rename the
    # person whose account actually sent the message.
    name = _clean(peer.get("name") or peer.get("username") or link.get("name"), 200) or peer_id
    now = _iso()
    records: list[dict[str, str]] = []
    notify_incoming = (
        _clean(peer.get("username"), 200).lstrip("@").casefold()
        != NOTIFY_TELEGRAM_USERNAME.casefold()
    )
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
        stored_attachment_url = _clean(outgoing_attachment_url, 4000) if outgoing else ""
        if outgoing and stored_attachment_url:
            content_type = _clean(outgoing_attachment_type, 200) or content_type or "image"
            if text_value.startswith("[Вложение:"):
                text_value = ""
        raw = {
            "id": message_id,
            "contentType": content_type,
            "contentUri": stored_attachment_url,
            "filename": filename,
            "senderId": _clean(getattr(message, "sender_id", ""), 200),
            "replyTo": _clean(getattr(message, "reply_to_msg_id", ""), 200),
        }
        records.append({
            "external_id": f"{channel['channel_id']}:{peer_id}:{message_id}",
            "direction": "outgoing" if outgoing else "incoming",
            "text": text_value,
            "content_uri": stored_attachment_url,
            "author_name": (_clean(outgoing_author_name, 200) or "Telegram") if outgoing else name,
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
                   text=CASE
                     WHEN excluded.text LIKE '[Вложение:%' AND wazzup_messages.content_uri<>''
                     THEN wazzup_messages.text ELSE excluded.text END,
                   content_uri=CASE
                     WHEN excluded.content_uri<>'' THEN excluded.content_uri ELSE wazzup_messages.content_uri END,
                   author_name=CASE
                     WHEN wazzup_messages.direction='outgoing' AND wazzup_messages.author_name NOT IN ('','Telegram')
                     THEN wazzup_messages.author_name ELSE excluded.author_name END,
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
    for record in records:
        if record["direction"] != "incoming" or not notify_incoming:
            continue
        try:
            raw = json.loads(record["raw_json"])
        except json.JSONDecodeError:
            raw = {}
        if course_context and course_context.get("suppress_notification"):
            continue
        await _enqueue_notification_message(
            external_id=record["external_id"], channel_id=channel["channel_id"],
            chat_type="telegram", chat_id=peer_id, phone_hash=phone_hash,
            provider=TELEGRAM_PROVIDER, client_name=name, text=record["text"],
            content_type=_clean(raw.get("contentType"), 100), sent_at=record["sent_at"],
            course_context=course_context, raw_payload=raw,
        )
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
    if _telegram_dialog_hidden(identity):
        return {}

    async def resolve(client: Any) -> dict[str, str]:
        if not await client.is_user_authorized():
            return {}
        entity = await _telegram_entity(client, identity)
        if not entity:
            return {}
        peer = _telegram_user_view(entity)
        identity["name"] = peer["name"] or peer["username"] or identity.get("name", "")
        identity["telegram_username"] = peer["username"] or identity.get("telegram_username", "")
        await _remember_external_link(
            identity,
            "getcourse-card",
            provider=TELEGRAM_PROVIDER,
            external_user_id=peer["id"],
        )
        return await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)

    return await _telegram_run(resolve)


async def _sync_telegram_history(peer_id: str, *, offset: int = 0, identity: dict[str, Any] | None = None) -> tuple[int, bool]:
    if _telegram_dialog_hidden(identity, peer_id=peer_id):
        return 0, False
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
    if _telegram_dialog_hidden(identity, peer_id=peer_id):
        return
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


async def _telegram_send_text(
    peer_id: str, text: str, *, identity: dict[str, Any] | None = None, author_name: str = "",
    attachment_url: str = "", attachment_type: str = "",
) -> dict[str, Any]:
    if _telegram_dialog_hidden(identity, peer_id=peer_id):
        raise HTTPException(404, "Диалог Telegram не найден")
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
        if attachment_url:
            path, _ = await _widget_media_path(attachment_url)
            message = await client.send_file(entity, str(path), caption=text or None)
        else:
            message = await client.send_message(entity, text)
        await _telegram_store_messages(
            channel, entity, [message], link, outgoing_author_name=author_name,
            outgoing_attachment_url=attachment_url,
            outgoing_attachment_type=attachment_type,
        )
        return {
            "external_id": f"{channel['channel_id']}:{peer_id}:{_clean(getattr(message, 'id', ''), 200)}",
            "direction": "outgoing",
            "status": "delivered",
            "text": text,
            "content_uri": attachment_url,
            "author_name": _clean(author_name, 200) or "Telegram",
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
            latest = getattr(dialog, "message", None)
            course_context: dict[str, Any] = {}
            if _telegram_is_user(entity):
                peer = _telegram_user_view(entity)
                if _telegram_dialog_hidden(peer):
                    continue
            else:
                course_context = await _course_chat_context(
                    TELEGRAM_PROVIDER, _clean(getattr(entity, "id", ""), 200),
                    _clean(getattr(dialog, "name", "") or getattr(entity, "title", ""), 500),
                    text=_clean(getattr(latest, "message", "") if latest else "", 5000),
                    require_addressed=True,
                )
                if not course_context:
                    continue
                peer = {"id": _clean(getattr(entity, "id", ""), 200), "phone": "", "name": _clean(getattr(dialog, "name", ""), 300), "username": ""}
            if not peer["id"]:
                continue
            link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER) if not course_context else {"name": peer["name"]}
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
            if latest:
                messages += await _telegram_store_messages(
                    channel, entity, [latest], link, course_context=course_context,
                )
        await _set_setting("telegram_last_sync_at", _iso())
        return {"dialogs": dialogs, "messages": messages}

    return await _telegram_run(sync)


async def telegram_background_loop() -> None:
    await asyncio.sleep(30)
    while True:
        try:
            await _sync_telegram_dialogs()
        except Exception:
            _log("warning", "Telegram Personal reconciliation failed")
        await asyncio.sleep(TELEGRAM_SYNC_SECONDS)


async def _telegram_string_session() -> str:
    async def export(client: Any) -> str:
        if not await client.is_user_authorized():
            return ""
        try:
            from telethon.sessions import StringSession
            return _clean(StringSession.save(client.session), 10_000)
        except Exception as exc:
            _log("warning", "Telegram realtime session export failed: %s", type(exc).__name__)
            return ""

    return await _telegram_run(export)


async def telegram_realtime_loop() -> None:
    """Receive Telegram Personal messages immediately without holding the SQLite session open."""
    await asyncio.sleep(8)
    while True:
        client = None
        try:
            session_value = await _telegram_string_session()
            channel = await _telegram_channel()
            if not session_value or not channel:
                await asyncio.sleep(30)
                continue
            from telethon import TelegramClient, events
            from telethon.sessions import StringSession
            from orchestrator.telegram_proxy import telethon_proxy_config

            api_id, api_hash = _telegram_credentials()
            connection, proxy = telethon_proxy_config()
            kwargs: dict[str, Any] = {
                "connection_retries": 3, "request_retries": 2, "timeout": 10,
            }
            if connection and proxy:
                kwargs.update({"connection": connection, "proxy": proxy})
            client = TelegramClient(StringSession(session_value), api_id, api_hash, **kwargs)

            async def on_message(event: Any) -> None:
                try:
                    entity = await event.get_chat()
                    course_context: dict[str, Any] = {}
                    if _telegram_is_user(entity):
                        peer = _telegram_user_view(entity)
                        if _telegram_dialog_hidden(peer):
                            return
                    else:
                        reply_sender_id = ""
                        reply_sender_ref = ""
                        if getattr(event.message, "reply_to", None):
                            try:
                                replied = await event.get_reply_message()
                                replied_sender = await replied.get_sender() if replied else None
                                reply_sender_id = _clean(getattr(replied_sender, "id", ""), 200)
                                reply_sender_ref = _clean(getattr(replied_sender, "username", ""), 200)
                            except Exception:
                                pass
                        course_context = await _course_chat_context(
                            TELEGRAM_PROVIDER, _clean(getattr(entity, "id", ""), 200),
                            _clean(getattr(entity, "title", ""), 500),
                            text=_clean(getattr(event.message, "message", ""), 5000),
                            reply_sender_id=reply_sender_id, reply_sender_ref=reply_sender_ref,
                            require_addressed=True,
                        )
                        if not course_context:
                            return
                        sender = await event.get_sender()
                        sender_username = _clean(getattr(sender, "username", ""), 200).lstrip("@").casefold()
                        course_context["suppress_notification"] = bool(
                            sender_username and sender_username == _clean(course_context.get("curator_telegram"), 200)
                        )
                        peer = {"id": _clean(getattr(entity, "id", ""), 200), "phone": "", "name": _clean(getattr(entity, "title", ""), 300), "username": ""}
                    link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER) if not course_context else {"name": peer["name"]}
                    if not link:
                        await _remember_external_link(
                            {"phone": peer["phone"], "name": peer["name"] or peer["username"]},
                            "telegram-realtime", provider=TELEGRAM_PROVIDER,
                            external_user_id=peer["id"],
                        )
                        link = await _external_link(peer_id=peer["id"], provider=TELEGRAM_PROVIDER)
                    await _telegram_store_messages(
                        channel, entity, [event.message], link, course_context=course_context,
                    )
                except Exception:
                    _log("exception", "Telegram realtime message store failed")

            client.add_event_handler(on_message, events.NewMessage(incoming=True))
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                client = None
                await asyncio.sleep(30)
                continue
            await client.run_until_disconnected()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "Telegram realtime listener reconnect: %s", type(exc).__name__)
            await asyncio.sleep(5)
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass


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


async def _email_channel(context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        service = _module_service("email-channel", "service_channel")
        result = await service(context=context or {})
        return result if isinstance(result, dict) else None
    except Exception as exc:
        if not isinstance(exc, HTTPException) or exc.status_code != 503:
            _log("warning", "Email channel catalogue failed: %s", type(exc).__name__)
        return None


async def _all_channels(*, refresh: bool = False) -> list[dict[str, str]]:
    """Return one shared channel catalogue for all simultaneously opened cards.

    Channel discovery includes Wazzup and Telegram account state.  Without a
    single-flight guard ten amoCRM cards opened at once could start ten equal
    provider requests.  The catalogue is account-wide, so it is safe to share
    for a short interval between all widget devices.
    """

    global _all_channels_cache, _all_channels_inflight, _all_channels_cache_owner
    owner = str(_db_path or "")
    if owner != _all_channels_cache_owner:
        _all_channels_cache_owner = owner
        _all_channels_cache = (0.0, [])
        _all_channels_inflight = None
    now = time.monotonic()
    expires_at, cached = _all_channels_cache
    if not refresh and cached and expires_at > now:
        return [dict(row) for row in cached]

    task = _all_channels_inflight
    if task is None or task.done():
        async def load() -> list[dict[str, str]]:
            global _all_channels_cache
            channels_result, vk, telegram, email_channel = await asyncio.gather(
                _cached_active_channels(refresh=refresh),
                _vk_channel(),
                _telegram_channel(refresh=refresh),
                _email_channel(),
                return_exceptions=True,
            )
            try:
                if isinstance(channels_result, BaseException):
                    raise channels_result
                channels = channels_result
            except HTTPException:
                channels = []
            direct = [row for row in (vk, telegram, _salebot_channel(), email_channel) if isinstance(row, dict)]
            rows = channels + direct
            _all_channels_cache = (time.monotonic() + CHANNEL_CACHE_SECONDS, [dict(row) for row in rows])
            return rows

        task = asyncio.create_task(load())
        _all_channels_inflight = task
    try:
        return [dict(row) for row in await asyncio.wait_for(asyncio.shield(task), timeout=0.7)]
    except TimeoutError:
        # Keep the account-wide refresh running, but never hold the whole card
        # open for a provider catalogue request.  Stored Wazzup chats and
        # configured direct channels are enough for the first usable paint.
        # Stale-while-revalidate: an expired full catalogue is safer than a
        # partial one.  In particular it keeps Email visible while a provider
        # refresh continues in the background.
        rows = [dict(row) for row in cached] or [dict(row) for row in _channel_cache[1]]
        if not rows:
            try:
                rows = await asyncio.wait_for(_stored_wazzup_channels(), timeout=0.25)
            except (TimeoutError, RuntimeError, aiosqlite.Error):
                rows = []
        if _vk_group_id() and _vk_token():
            rows.append({
                "channel_id": _vk_channel_id(), "transport": "vk", "channel_transport": "vk",
                "provider": "vk", "name": f"Сообщество {_vk_group_id()}",
                "plain_id": _vk_group_id(), "label": f"VK · Сообщество {_vk_group_id()}",
            })
        state = _telegram_state_cache[1] if _telegram_state_cache[1] else {}
        account = state.get("account") if isinstance(state.get("account"), dict) else {}
        account_id = _clean(account.get("id"), 200)
        if state.get("authorized") and account_id:
            name = _clean(account.get("username") or account.get("name"), 200) or account_id
            rows.append({
                "channel_id": f"telegram-personal:{account_id}", "transport": "telegram",
                "channel_transport": "personal", "provider": TELEGRAM_PROVIDER,
                "name": name, "plain_id": account_id, "label": f"Telegram Personal · {name}",
            })
        salebot = _salebot_channel()
        if salebot:
            rows.append(salebot)
        if not any(row.get("provider") == EMAIL_PROVIDER for row in rows):
            try:
                email_channel = await asyncio.wait_for(_email_channel(), timeout=0.2)
            except (TimeoutError, RuntimeError):
                email_channel = None
            if email_channel:
                rows.append(email_channel)
        unique: dict[tuple[str, str, str], dict[str, str]] = {}
        for row in rows:
            unique[(row["channel_id"], row["transport"], row.get("provider", "wazzup"))] = row
        return list(unique.values())
    finally:
        if task.done() and _all_channels_inflight is task:
            _all_channels_inflight = None


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
    if not content["content_type"] and re.search(
        r"\.(?:jpe?g|png|gif|webp|bmp)(?:[?#]|$)", content["content_uri"], re.IGNORECASE,
    ):
        content["content_type"] = "image"
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
        SELECT id,external_id,direction,status,text,
               CASE WHEN content_uri<>'' THEN content_uri ELSE COALESCE(
                   (SELECT attachment_url FROM outbound_jobs job
                    WHERE job.external_id=wazzup_messages.external_id AND job.attachment_url<>''
                    ORDER BY job.id DESC LIMIT 1),
                   (SELECT attachment_url FROM outbound_jobs job
                    WHERE wazzup_messages.external_id='failed:' || job.request_key AND job.attachment_url<>''
                    ORDER BY job.id DESC LIMIT 1), '') END AS content_uri,
               author_name,sent_at,raw_json,
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


async def _remember_client_links(items: list[dict[str, str]]) -> None:
    """Persist inbox identity enrichments in one transaction instead of N WAL writes."""

    rows: list[tuple[str, str, str, str, str, str]] = []
    for item in items:
        normalized = _normalize_phone(item.get("phone"))
        if not normalized:
            continue
        rows.append((
            _phone_hash(normalized), normalized,
            _clean(item.get("getcourse_user_id"), 200),
            _clean(item.get("name"), 200),
            _clean(item.get("source"), 80), _iso(),
        ))
    if not rows:
        return
    db = await _connect()
    try:
        await db.executemany(
            """INSERT INTO client_links(phone_hash,phone,getcourse_user_id,name,source,updated_at)
               VALUES(?,?,?,?,?,?) ON CONFLICT(phone_hash) DO UPDATE SET
               phone=excluded.phone,
               getcourse_user_id=CASE WHEN excluded.getcourse_user_id<>'' THEN excluded.getcourse_user_id ELSE client_links.getcourse_user_id END,
               name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE client_links.name END,
               source=CASE WHEN excluded.source<>'' THEN excluded.source ELSE client_links.source END,
               updated_at=excluded.updated_at""",
            rows,
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
    notification_records: list[dict[str, Any]] = []
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
            if direction == "incoming" and not _messenger_button_event(row):
                notification_records.append({
                    "external_id": external_id, "channel_id": channel_id,
                    "chat_type": chat_type, "chat_id": chat_id, "phone_hash": phone_hash,
                    "client_name": author_name, "text": message_text,
                    "content_type": content["content_type"], "sent_at": sent_at,
                    "raw_payload": row,
                })
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
    for record in notification_records:
        await _enqueue_notification_message(**record)
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
        result = dict(row)
        last_used = _parse_iso(row["last_used_at"])
        if not last_used or last_used <= _now_dt() - timedelta(seconds=DEVICE_TOUCH_INTERVAL_SECONDS):
            try:
                await db.execute(
                    "UPDATE devices SET last_used_at=?,expires_at=? WHERE id=?",
                    (now, expires, row["id"]),
                )
                await db.commit()
                result.update({"last_used_at": now, "expires_at": expires})
            except sqlite3.OperationalError as exc:
                await db.rollback()
                if "locked" not in str(exc).casefold():
                    raise
        return result
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
        "entity_url": _clean(data.get("source_url"), 1000),
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


async def _responsible_admin_id(
    data: dict[str, Any], mode: str, device: dict[str, Any], *,
    db: aiosqlite.Connection | None = None,
) -> int | None:
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
    owns_db = db is None
    if db is None:
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
        if owns_db:
            await db.close()
    return int(device["admin_id"]) if _clean(device.get("admin_role"), 20) == "employee" else None


async def _assign_client_threads(
    admin_id: int | None,
    *,
    phone: str = "",
    direct_links: list[tuple[str, str]] | None = None,
    db: aiosqlite.Connection | None = None,
) -> None:
    if not admin_id:
        return
    phone_hash = _phone_hash(phone)
    direct_links = direct_links or []
    if not phone_hash and not direct_links:
        return
    owns_db = db is None
    if db is None:
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
            channel_prefix = (
                "vk:" if provider == "vk"
                else "salebot:" if provider == SALEBOT_PROVIDER
                else "telegram-personal:"
            )
            await db.execute(
                "UPDATE wazzup_chats SET responsible_admin_id=? WHERE channel_id LIKE ? AND chat_id=?",
                (admin_id, channel_prefix + "%", _clean(peer_id, 250)),
            )
        await db.commit()
    finally:
        if owns_db:
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


def _identity_context_key(context: dict[str, Any]) -> tuple[str, ...]:
    fields = context.get("fields") if isinstance(context.get("fields"), dict) else {}
    def identity_value(*names: str) -> str:
        return next((
            _clean(context.get(name) or fields.get(name), 1000)
            for name in names
            if _clean(context.get(name) or fields.get(name), 1000)
        ), "")
    return (
        _clean(context.get("platform"), 40),
        _clean(context.get("entity_type"), 40),
        _clean(context.get("entity_id"), 200),
        _normalize_phone(context.get("phone")),
        _clean(context.get("email"), 320).casefold(),
        identity_value("getcourse_user_id", "gc_user_id"),
        identity_value("vk_platform_id", "vk_id", "vkontakte_id", "senler_id"),
        identity_value("telegram_id", "tg_id"),
        identity_value("salebot_id", "salebot_client_id", "sb_id"),
        identity_value("utm_term"),
    )


def _ensure_identity_cache_owner() -> None:
    global _identity_cache_owner
    if _identity_cache_owner is _identity_index:
        return
    _identity_cache_owner = _identity_index
    _identity_resolve_cache.clear()
    _identity_resolve_inflight.clear()
    _identity_exact_cache.clear()
    _identity_exact_inflight.clear()


async def _run_identity_lookup(function: Any, *args: Any) -> Any:
    """Bound random reads of the large identity databases.

    The default asyncio executor may otherwise start dozens of simultaneous
    SQLite readers when several amoCRM cards retry together.  On the small
    production host that turns ordinary indexed reads into swap/I/O thrash.
    """
    global _identity_lookup_loop, _identity_lookup_gate
    loop = asyncio.get_running_loop()
    if _identity_lookup_loop is not loop or _identity_lookup_gate is None:
        _identity_lookup_loop = loop
        _identity_lookup_gate = asyncio.Semaphore(2)
    async with _identity_lookup_gate:
        return await asyncio.to_thread(function, *args)


async def _resolve_identity_context(context: dict[str, Any]) -> dict[str, Any]:
    if _identity_index is None:
        return {"status": "unavailable", "accounts": [], "variables": build_context_variables([], context), "conflicts": []}
    _ensure_identity_cache_owner()
    key = _identity_context_key(context)
    now = time.monotonic()
    cached = _identity_resolve_cache.get(key)
    if cached and cached[0] > now:
        return cached[1]
    task = _identity_resolve_inflight.get(key)
    if task is None:
        task = asyncio.create_task(_run_identity_lookup(_identity_index.resolve, context))
        _identity_resolve_inflight[key] = task
    try:
        result = await asyncio.shield(task)
        _identity_resolve_cache[key] = (time.monotonic() + 30, result)
        if len(_identity_resolve_cache) > 256:
            oldest = min(_identity_resolve_cache, key=lambda item: _identity_resolve_cache[item][0])
            _identity_resolve_cache.pop(oldest, None)
        return result
    finally:
        if task.done() and _identity_resolve_inflight.get(key) is task:
            _identity_resolve_inflight.pop(key, None)


async def _exact_provider_identity(provider: str, context: dict[str, Any]) -> str:
    if _identity_index is None:
        return ""
    _ensure_identity_cache_owner()
    key = (provider, *_identity_context_key(context))
    now = time.monotonic()
    cached = _identity_exact_cache.get(key)
    if cached and cached[0] > now:
        return cached[1]
    task = _identity_exact_inflight.get(key)
    if task is None:
        task = asyncio.create_task(_run_identity_lookup(
            _identity_index.provider_id_for_exact_context, provider, context,
        ))
        _identity_exact_inflight[key] = task
    try:
        value = _clean(await asyncio.shield(task), 300)
        _identity_exact_cache[key] = (time.monotonic() + 30, value)
        if len(_identity_exact_cache) > 512:
            oldest = min(_identity_exact_cache, key=lambda item: _identity_exact_cache[item][0])
            _identity_exact_cache.pop(oldest, None)
        return value
    finally:
        if task.done() and _identity_exact_inflight.get(key) is task:
            _identity_exact_inflight.pop(key, None)


async def _resolve_widget_context(
    data: dict[str, Any], mode: str, device: dict[str, Any], *, context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if context is None:
        context = _widget_context(data, mode, device)
        await _apply_identity_rules(context)
    resolved = await _resolve_identity_context(context)
    # Identity resolution returns template variables and an internal graph id.
    # Channel services, however, need the original card coordinates and the
    # resolved contact details as ordinary top-level context fields.  Passing
    # the raw graph result made Email look unavailable even though
    # ``variables.contact.email`` had already been found successfully.
    identity_entity_id = resolved.get("entity_id")
    enriched = {**resolved, **context}
    if identity_entity_id not in (None, ""):
        enriched["identity_entity_id"] = identity_entity_id
    enriched["email"] = _clean(
        context.get("email") or _resolved_variable(
            resolved,
            "amo.lead.contact_fields.email", "amo.lead.email",
            "getcourse.email", "getcourse.order.email", "contact.email",
        ),
        320,
    ).casefold()
    enriched["phone"] = _normalize_phone(
        context.get("phone") or _resolved_variable(
            resolved,
            "amo.lead.contact_fields.phone", "amo.lead.phone",
            "getcourse.phone", "getcourse.order.phone", "contact.phone",
        )
    )
    enriched["name"] = _clean(
        context.get("name") or _resolved_variable(
            resolved,
            "amo.lead.contact_fields.name", "amo.lead.contact_name",
            "getcourse.name", "getcourse.order.name", "contact.name",
        ),
        500,
    )
    return enriched


def _resolved_variable(resolved: dict[str, Any], *keys: str) -> str:
    variables = resolved.get("variables") if isinstance(resolved, dict) else {}
    if not isinstance(variables, dict):
        return ""
    for key in keys:
        item = variables.get(key)
        value = item.get("value") if isinstance(item, dict) else item
        if _clean(value, 1000):
            return _clean(value, 1000)
    return ""


def _resolved_contact_name(resolved: dict[str, Any], data: dict[str, Any]) -> str:
    """Return a plain contact name, never the identity variable metadata object."""

    name = _resolved_variable(resolved, "contact.name", "contact.first_name")
    if name:
        return _clean(name, 200)
    fallback = data.get("name")
    if isinstance(fallback, dict):
        fallback = fallback.get("value")
    return _clean(fallback, 200) if isinstance(fallback, (str, int)) else ""


PROFILE_LINK_LABELS = {
    "getcourse": "GetCourse",
    "vk": "VK",
    "telegram_personal": "TG Personal",
    "salebot": "SaleBot",
    "max": "MAX",
}
PROFILE_LINK_ORDER = ("getcourse", "vk", "telegram_personal", "salebot", "max")


def _scalar_texts(value: Any, *, depth: int = 0) -> list[str]:
    if depth > 5:
        return []
    if isinstance(value, dict):
        result: list[str] = []
        for item in list(value.values())[:500]:
            result.extend(_scalar_texts(item, depth=depth + 1))
        return result
    if isinstance(value, (list, tuple)):
        result = []
        for item in list(value)[:500]:
            result.extend(_scalar_texts(item, depth=depth + 1))
        return result
    if isinstance(value, (str, int)):
        text = _clean(value, 4000)
        return [text] if text else []
    return []


def _profile_links_from_values(values: list[Any]) -> dict[str, str]:
    """Extract only direct, allow-listed social profile URLs from trusted card data."""

    found: dict[str, str] = {}
    pattern = re.compile(
        r"(?:https?://)?(?:www\.)?(?:vk\.com|vk\.ru|t\.me|max\.ru|salebot\.pro)/[^\s<>\"']+",
        re.I,
    )
    reserved_vk = {"app", "away", "club", "feed", "gim", "id", "im", "photo", "public", "video", "wall"}
    for raw in values:
        for text in _scalar_texts(raw):
            for match in pattern.finditer(text):
                candidate = match.group(0).rstrip(".,;:!?)]}")
                if not candidate.lower().startswith(("http://", "https://")):
                    candidate = "https://" + candidate
                parsed = urlsplit(candidate)
                host = (parsed.hostname or "").lower().removeprefix("www.")
                parts = [part for part in parsed.path.split("/") if part]
                if host in {"vk.com", "vk.ru"} and len(parts) == 1:
                    slug = parts[0]
                    if re.fullmatch(r"id\d+", slug, re.I) or (
                        re.fullmatch(r"[A-Za-z0-9_.]{3,64}", slug)
                        and slug.casefold() not in reserved_vk
                        and not re.fullmatch(r"(?:club|public|wall|photo|video)\d+.*", slug, re.I)
                    ):
                        found.setdefault("vk", f"https://vk.com/{slug}")
                elif host == "t.me" and len(parts) == 1 and re.fullmatch(r"[A-Za-z0-9_]{5,32}", parts[0]):
                    found.setdefault("telegram_personal", f"https://t.me/{parts[0]}")
                elif host == "t.me" and len(parts) == 1 and re.fullmatch(r"\+\d{8,15}", parts[0]):
                    found.setdefault("telegram_personal", f"https://t.me/{parts[0]}?profile")
                elif host == "max.ru" and len(parts) == 2 and parts[0].casefold() == "u" and re.fullmatch(r"[A-Za-z0-9_-]{16,160}", parts[1]):
                    found.setdefault("max", f"https://max.ru/u/{parts[1]}")
                elif host == "salebot.pro" and len(parts) == 4 and parts[0] == "projects" and parts[2] == "clients" and parts[1].isdigit():
                    client_id = quote(unquote(parts[3]), safe="")
                    if client_id:
                        found.setdefault("salebot", f"https://salebot.pro/projects/{parts[1]}/clients/{client_id}")
    return found


def _telegram_profile_url(username: Any = "", phone: Any = "") -> str:
    public_username = _clean(username, 200).lstrip("@")
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", public_username):
        return f"https://t.me/{public_username}?profile"
    normalized_phone = _normalize_phone(phone)
    phone_digits = normalized_phone.removeprefix("+")
    if re.fullmatch(r"\d{8,15}", phone_digits):
        return f"https://t.me/+{phone_digits}?profile"
    return ""


async def _provider_profile_name(
    provider: str, external_id: str, fallback: str = "", *,
    db: aiosqlite.Connection | None = None,
) -> str:
    """Prefer a name observed in the exact provider over the amoCRM card name."""

    provider = _clean(provider, 40).lower()
    external_id = _clean(external_id, 300)
    if not external_id:
        return ""
    if provider == "vk":
        name = await _streams_vk_profile_name(external_id)
        if name:
            return name
    source = "telegram_personal" if provider == TELEGRAM_PROVIDER else provider
    chat = message = event = None
    owns_db = db is None
    try:
        if db is None:
            db = await _connect()
        chat_type = "telegram" if provider == TELEGRAM_PROVIDER else provider
        chat = await (await db.execute(
            """SELECT contact_name FROM wazzup_chats
               WHERE chat_type=? AND chat_id=? AND contact_name<>''
               ORDER BY updated_at DESC LIMIT 1""",
            (chat_type, external_id),
        )).fetchone()
        message = await (await db.execute(
            """SELECT author_name FROM wazzup_messages
               WHERE chat_type=? AND chat_id=? AND direction='incoming' AND author_name<>''
               ORDER BY sent_at DESC,id DESC LIMIT 1""",
            (chat_type, external_id),
        )).fetchone()
        event = await (await db.execute(
            """SELECT client_name FROM notification_events
               WHERE source=? AND chat_id=? AND client_name<>''
               ORDER BY sent_at DESC,created_at DESC LIMIT 1""",
            (source, external_id),
        )).fetchone()
    except (RuntimeError, aiosqlite.Error):
        pass
    finally:
        if owns_db and db is not None:
            await db.close()
    for row, field in ((chat, "contact_name"), (message, "author_name"), (event, "client_name")):
        if row and _clean(row[field], 200):
            return _clean(row[field], 200)
    if provider == SALEBOT_PROVIDER:
        cached = _salebot_history_cache.get(external_id)
        if cached:
            for message in reversed(cached[1]):
                if message.get("direction") == "incoming" and _clean(message.get("author_name"), 200):
                    return _clean(message.get("author_name"), 200)
    return _clean(fallback, 200)


async def _remember_verified_card_identities(
    context: dict[str, Any], device: dict[str, Any], identities: dict[str, str], mode: str,
) -> None:
    """Share exact profile proof with conversation/send channel discovery."""

    if mode == "test" or _db_path is None:
        return
    identity = {
        "phone": _normalize_phone(context.get("phone")),
        "email": _clean(context.get("email"), 320),
        "name": _clean(context.get("name"), 200),
        "getcourse_user_id": _identity_field_value(
            context, "getcourse_user_id", "gc_user_id", "user_id",
        ),
    }
    admin_id = int(device.get("admin_id") or 0) or None
    for provider, external_id in identities.items():
        external_id = _clean(external_id, 300)
        if not external_id:
            continue
        await _remember_external_link(
            identity, "amocrm-card-verified",
            provider=provider, external_user_id=external_id,
        )
        await _remember_entity_external_link(
            context, provider, external_id, admin_id,
        )


async def _is_active_getcourse_staff(gc_id: str = "", email: str = "") -> bool:
    gc_id = _clean(gc_id, 100)
    email = _clean(email, 320).casefold()
    if not gc_id and not email:
        return False
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT 1 FROM manager_bindings b JOIN admins a ON a.id=b.admin_id
               WHERE b.platform='getcourse' AND a.enabled=1
                 AND ((?<>'' AND b.platform_user_id=?)
                      OR (?<>'' AND lower(b.platform_user_email)=?)) LIMIT 1""",
            (gc_id, gc_id, email, email),
        )).fetchone()
        return bool(row)
    finally:
        await db.close()


async def _widget_profile_links(data: dict[str, Any], mode: str, device: dict[str, Any]) -> list[dict[str, Any]]:
    context = _widget_context(data, mode, device)
    await _apply_identity_rules(context)
    resolved = await _resolve_widget_context(data, mode, device)
    context["email"] = context.get("email") or _resolved_variable(
        resolved, "contact.email", "getcourse.order.email", "getcourse.user.email",
    )
    context["phone"] = context.get("phone") or _resolved_variable(
        resolved, "contact.phone", "getcourse.order.phone", "getcourse.user.phone",
    )
    context["name"] = context.get("name") or _resolved_variable(
        resolved, "contact.name", "getcourse.order.name", "getcourse.user.name",
    )
    found = _profile_links_from_values([context, resolved.get("variables", {})])
    accounts = [row for row in resolved.get("accounts", []) if isinstance(row, dict)]
    exact_ids: dict[str, str] = {}
    telegram_profile_verification = ""
    telegram_profile_pending = False
    telegram_delivery_link: dict[str, str] = {}
    if context.get("platform") == "amocrm" and _identity_index is not None:
        # Historical amoCRM imports contain generated ``Диалог SaleBot`` URLs
        # whose client id was copied blindly from utm_term.  Those URLs are
        # useful as archival text, but they are not proof that the current
        # lead exists in SaleBot.  Only the exact Customer DB bridge below may
        # create a SaleBot profile button for an amoCRM card.
        found.pop(SALEBOT_PROVIDER, None)
        found.pop(TELEGRAM_PROVIDER, None)
        vk_exact, salebot_exact = await asyncio.gather(*(
            _exact_provider_identity(provider, context)
            for provider in ("vk", SALEBOT_PROVIDER)
        ))
        exact_ids = {
            "vk": _clean(vk_exact, 300) or _identity_field_value(context, "vk_id", "vkontakte_id", "senler_id"),
            SALEBOT_PROVIDER: _clean(salebot_exact, 300),
        }
        # Profile buttons and send channels must consume the same verified
        # card identity.  Exact Customer DB / VK / Telegram list resolution
        # used to create the profile button only, while /widget/channels read
        # entity_identity_links and therefore reported the same VK/SaleBot as
        # unavailable until another code path happened to persist it.  Store
        # the exact bridge here, inside the existing background enrichment,
        # so every subsequent channel poll sees the proof immediately.
        await _remember_verified_card_identities(context, device, exact_ids, mode)
        # A graph/phone match is useful for discovery, but it must never turn
        # into a profile button in an amoCRM deal unless the exact deal row
        # confirms the provider identifier.
        accounts = []

    gc_id = ""
    if context.get("platform") == "getcourse" and context.get("entity_type") in {"user", "contact"}:
        gc_id = re.sub(r"\D+", "", _clean(context.get("entity_id"), 100))
    if not gc_id:
        gc_id = re.sub(r"\D+", "", _identity_field_value(context, "getcourse_user_id", "gc_user_id", "user_id"))
    if not gc_id and _identity_index is not None and (context.get("phone") or context.get("email")):
        resolver = getattr(_identity_index, "platform_id_for_context", None)
        if callable(resolver):
            gc_id = re.sub(r"\D+", "", await _run_identity_lookup(resolver, "getcourse", context))
    if gc_id:
        found["getcourse"] = f"{_allowed_origin()}/user/control/user/update/id/{gc_id}"

    entity_links = await asyncio.gather(*(
        _entity_external_link(context["platform"], context["entity_type"], context["entity_id"], provider)
        for provider in ("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER)
    ))
    account_ids: dict[str, str] = {}
    for row in accounts:
        service = _clean(row.get("service"), 80).lower()
        platform_id = _clean(row.get("platform_id"), 300)
        if service == "vk" and platform_id:
            account_ids.setdefault("vk", platform_id)
        elif service in {"telegram", "telegram_personal"} and platform_id:
            account_ids.setdefault(TELEGRAM_PROVIDER, platform_id)
        elif service == SALEBOT_PROVIDER and platform_id:
            account_ids.setdefault(SALEBOT_PROVIDER, platform_id)
    for provider, link in zip(("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER), entity_links):
        if link and (
            context.get("platform") != "amocrm"
            or (
                bool(exact_ids.get(provider))
                and _clean(link.get("external_user_id"), 300) == exact_ids.get(provider)
            )
            or (
                provider == TELEGRAM_PROVIDER
                and _card_link_matches_context(link, context, gc_id)
            )
        ):
            account_ids.setdefault(provider, _clean(link.get("external_user_id"), 300))
    for provider, external_id in exact_ids.items():
        if external_id:
            account_ids[provider] = external_id

    if context.get("platform") == "amocrm":
        telegram_delivery_link = next((
            link for provider, link in zip(
                ("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER), entity_links,
            )
            if provider == TELEGRAM_PROVIDER
            and link
            and _card_link_matches_context(link, context, gc_id)
        ), {})
        if not telegram_delivery_link:
            telegram_delivery_link = await _successful_card_delivery_link(
                context, TELEGRAM_PROVIDER,
            )
        delivered_telegram_id = _clean(telegram_delivery_link.get("external_user_id"), 300)
        if delivered_telegram_id:
            account_ids[TELEGRAM_PROVIDER] = delivered_telegram_id
            telegram_profile_verification = "verified"

    # Use the same exact card resolver as the SaleBot channel.  A card can have
    # a verified SaleBot id in Customer DB even before an entity link has been
    # persisted by the channel request.
    if SALEBOT_PROVIDER not in account_ids and _identity_index is not None:
        exact_salebot_id = await _exact_provider_identity(SALEBOT_PROVIDER, context)
        if exact_salebot_id:
            account_ids[SALEBOT_PROVIDER] = exact_salebot_id

    if context.get("platform") == "amocrm":
        # A Telegram id bridged through SaleBot proves that the bot has seen
        # the person, but it does not prove that our connected personal
        # Telegram account can resolve or message that person.  Use the same
        # live, non-importing check as the TG Personal channel.  A definitive
        # miss hides the profile button; a timeout keeps only an explicitly
        # unverified "try to open" action.
        if account_ids.get(TELEGRAM_PROVIDER):
            telegram_link = telegram_delivery_link
        elif context.get("phone"):
            telegram_link = await _amocrm_telegram_profile_link(data, mode, device, context)
        else:
            telegram_link = {}
        telegram_id = _clean(telegram_link.get("external_user_id"), 300)
        if telegram_id:
            account_ids[TELEGRAM_PROVIDER] = telegram_id
            telegram_profile_verification = "verified"
        elif telegram_link.get("pending"):
            found.pop(TELEGRAM_PROVIDER, None)
            telegram_profile_pending = True
        else:
            account_ids.pop(TELEGRAM_PROVIDER, None)
            found.pop(TELEGRAM_PROVIDER, None)

    vk_value = account_ids.get("vk")
    if context.get("platform") != "amocrm":
        vk_value = vk_value or _identity_field_value(context, "vk_id", "vkontakte_id", "senler_id")
    vk_id = re.sub(r"\D+", "", vk_value or "")
    if vk_id:
        found.setdefault("vk", f"https://vk.com/id{vk_id}")

    telegram_id = account_ids.get(TELEGRAM_PROVIDER)
    telegram_username = ""
    if context.get("platform") != "amocrm":
        telegram_id = telegram_id or _identity_field_value(context, "telegram_id", "tg_id")
        telegram_username = _identity_field_value(context, "telegram_username", "tg_username").lstrip("@")
    if not telegram_username and telegram_id and _identity_index is not None:
        telegram_username = await _run_identity_lookup(
            _identity_index.telegram_username_for_platform_id, telegram_id,
        )
    telegram_profile_url = _telegram_profile_url(
        telegram_username,
        context.get("phone") if telegram_id else "",
    )
    if telegram_profile_url:
        found.setdefault(TELEGRAM_PROVIDER, telegram_profile_url)

    salebot_id = account_ids.get(SALEBOT_PROVIDER)
    if not salebot_id and context.get("platform") != "amocrm":
        salebot_id = _identity_field_value(context, "salebot_id", "salebot_client_id", "sb_id")
    if not salebot_id and context.get("platform") != "amocrm":
        salebot_id = next((value for kind, value in parse_utm_term(_identity_field_value(context, "utm_term")) if kind == "salebot"), "")
    if salebot_id:
        found.setdefault(SALEBOT_PROVIDER, f"{SALEBOT_PROFILE_BASE}/{quote(salebot_id, safe='')}")

    entity_by_provider = {
        provider: link for provider, link in zip(("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER), entity_links)
    }
    vk_name, telegram_name, salebot_name = await asyncio.gather(
        _provider_profile_name("vk", vk_id, entity_by_provider.get("vk", {}).get("name", "")),
        _provider_profile_name(
            TELEGRAM_PROVIDER, telegram_id,
            telegram_delivery_link.get("name", "")
            or entity_by_provider.get(TELEGRAM_PROVIDER, {}).get("name", ""),
        ),
        _provider_profile_name(
            SALEBOT_PROVIDER, salebot_id,
            entity_by_provider.get(SALEBOT_PROVIDER, {}).get("name", ""),
        ),
    )
    profile_names = {"vk": vk_name, TELEGRAM_PROVIDER: telegram_name, SALEBOT_PROVIDER: salebot_name}

    telegram_hidden = _telegram_dialog_hidden(
        telegram_delivery_link,
        peer_id=telegram_id,
        phone=context.get("phone"),
        username=telegram_username,
    )
    if telegram_hidden:
        found.pop(TELEGRAM_PROVIDER, None)
        telegram_profile_pending = False

    paid_access = False
    try:
        service = _module_service("student-transfer", "service_widget_student")
        stream_card = await service(
            gc_user_id=gc_id, email=context.get("email") or "", phone=context.get("phone") or "",
            name=context.get("name") or "",
            include_access=False, summary_only=True,
        )
        paid_access = bool(stream_card.get("found") and stream_card.get("paid_access"))
        if stream_card.get("found"):
            stream_gc_id = re.sub(r"\D+", "", _clean(stream_card.get("gc_user_id"), 100))
            stream_url = _clean(stream_card.get("profile_url"), 2000)
            if stream_gc_id:
                gc_id = stream_gc_id
                found["getcourse"] = stream_url or f"{_allowed_origin()}/user/control/user/update/id/{gc_id}"
    except Exception as exc:
        _log("warning", "Streams badge lookup skipped: %s", exc)
    if (
        context.get("platform") == "amocrm"
        and not paid_access
        and await _is_active_getcourse_staff(gc_id, context.get("email") or "")
    ):
        # A staff account is not a client profile.  Keep the button only when
        # the same person has a real paid student enrollment.
        found.pop("getcourse", None)
    links = [
        {"kind": kind,
         "label": (
             f"{PROFILE_LINK_LABELS[kind]}: {profile_names[kind]}"
             if profile_names.get(kind) else PROFILE_LINK_LABELS[kind]
         ),
         "url": found[kind],
         **({"verification": telegram_profile_verification} if kind == TELEGRAM_PROVIDER and telegram_profile_verification else {}),
         **({"paid_access": paid_access} if kind == "getcourse" else {})}
        for kind in PROFILE_LINK_ORDER
        if found.get(kind)
    ]
    if telegram_profile_pending:
        links.append({
            "kind": TELEGRAM_PROVIDER, "label": "", "url": "", "verification": "pending",
        })
    return links


async def _quick_widget_profile_links(
    data: dict[str, Any], mode: str, device: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return profile buttons proved by card fields or local delivery history.

    This path intentionally avoids live provider and GetCourse requests.  It
    gives the widget useful buttons while the complete enrichment continues
    in the background.
    """

    context = _widget_context(data, mode, device)
    try:
        await asyncio.wait_for(_apply_identity_rules(context), timeout=0.2)
    except (TimeoutError, RuntimeError, aiosqlite.Error):
        pass
    found = _profile_links_from_values([context])
    if context.get("platform") == "amocrm":
        # A copied SaleBot/TG URL in an amoCRM field is not proof that this
        # exact lead owns the profile.  Keep the same strict rule as the full
        # resolver.
        found.pop(SALEBOT_PROVIDER, None)
        found.pop(TELEGRAM_PROVIDER, None)

    gc_id = re.sub(
        r"\D+", "",
        _identity_field_value(context, "getcourse_user_id", "gc_user_id", "user_id"),
    )
    if context.get("platform") == "getcourse" and context.get("entity_type") in {"user", "contact"}:
        gc_id = gc_id or re.sub(r"\D+", "", _clean(context.get("entity_id"), 100))
    if gc_id:
        found["getcourse"] = f"{_allowed_origin()}/user/control/user/update/id/{gc_id}"

    async def quick_result(factory: Callable[[], Awaitable[Any]]) -> Any:
        try:
            return await asyncio.wait_for(factory(), timeout=0.42)
        except (TimeoutError, RuntimeError, aiosqlite.Error):
            return {}

    resolver = (
        getattr(_identity_index, "platform_id_for_context", None)
        if _identity_index is not None else None
    )
    # Schedule this first: a known GetCourse user is the most useful profile
    # action for the sales team and the indexed lookup must not sit behind
    # slower messenger/provider checks.
    lookups = [
        (lambda: _run_identity_lookup(resolver, "getcourse", context))
        if callable(resolver) else (lambda: asyncio.sleep(0, result="")),
        lambda: _exact_provider_identity("vk", context),
        lambda: _exact_provider_identity(SALEBOT_PROVIDER, context),
        *(
            (lambda provider=provider: _entity_external_link(
                context["platform"], context["entity_type"], context["entity_id"], provider,
            ))
            for provider in ("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER)
        ),
        lambda: _successful_card_delivery_link(context, TELEGRAM_PROVIDER),
    ]
    results = await asyncio.gather(*(quick_result(item) for item in lookups))

    quick_gc_id = re.sub(r"\D+", "", _clean(results[0], 300))
    vk_exact = _clean(results[1], 300)
    salebot_exact = _clean(results[2], 300)
    if quick_gc_id:
        gc_id = gc_id or quick_gc_id
        found.setdefault("getcourse", f"{_allowed_origin()}/user/control/user/update/id/{quick_gc_id}")
    if context.get("platform") == "amocrm":
        await _remember_verified_card_identities(context, device, {
            "vk": vk_exact, SALEBOT_PROVIDER: salebot_exact,
        }, mode)
    entity_links = [
        results[index] if len(results) > index and isinstance(results[index], dict) else {}
        for index in range(3, 6)
    ]
    telegram_delivery = results[6] if len(results) > 6 and isinstance(results[6], dict) else {}
    names: dict[str, str] = {}

    vk_value = vk_exact or (
        _identity_field_value(context, "vk_id", "vkontakte_id", "senler_id")
        if context.get("platform") != "amocrm" else ""
    )
    vk_id = re.sub(r"\D+", "", vk_value)
    if vk_id:
        found.setdefault("vk", f"https://vk.com/id{vk_id}")

    for provider, link in zip(("vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER), entity_links):
        external_id = _clean(link.get("external_user_id"), 300)
        accepted = context.get("platform") != "amocrm"
        if context.get("platform") == "amocrm":
            accepted = (
                (provider == "vk" and bool(vk_exact) and external_id == vk_exact)
                or (provider == SALEBOT_PROVIDER and bool(salebot_exact) and external_id == salebot_exact)
                or (provider == TELEGRAM_PROVIDER and _card_link_matches_context(link, context, gc_id))
            )
        if accepted and external_id:
            names[provider] = _clean(link.get("name"), 200)
            if provider == "vk":
                found.setdefault("vk", f"https://vk.com/id{re.sub(r'\D+', '', external_id)}")
            elif provider == SALEBOT_PROVIDER:
                found.setdefault(provider, f"{SALEBOT_PROFILE_BASE}/{quote(external_id, safe='')}")

    if salebot_exact:
        found.setdefault(SALEBOT_PROVIDER, f"{SALEBOT_PROFILE_BASE}/{quote(salebot_exact, safe='')}")

    telegram_id = _clean(telegram_delivery.get("external_user_id"), 300)
    if telegram_id:
        names[TELEGRAM_PROVIDER] = _clean(telegram_delivery.get("name"), 200)
        telegram_url = _telegram_profile_url(
            telegram_delivery.get("username"), context.get("phone"),
        )
        if telegram_url:
            found[TELEGRAM_PROVIDER] = telegram_url

    if _telegram_dialog_hidden(
        telegram_delivery,
        peer_id=telegram_id,
        phone=context.get("phone"),
        username=_identity_field_value(context, "telegram_username", "tg_username"),
    ):
        found.pop(TELEGRAM_PROVIDER, None)

    return [
        {
            "kind": kind,
            "label": f"{PROFILE_LINK_LABELS[kind]}: {names[kind]}" if names.get(kind) else PROFILE_LINK_LABELS[kind],
            "url": found[kind],
            **({"verification": "verified"} if kind == TELEGRAM_PROVIDER else {}),
        }
        for kind in PROFILE_LINK_ORDER
        if found.get(kind)
    ]


async def _cached_widget_profile_links(
    data: dict[str, Any], mode: str, device: dict[str, Any], *, foreground_seconds: float = 1.2,
) -> tuple[list[dict[str, Any]], bool]:
    """Return profile buttons quickly while one card lookup continues in background."""
    context = _widget_context(data, mode, device)
    key = (*_identity_context_key(context), str(int(device.get("id") or 0)))
    now = time.monotonic()
    cached = _profile_links_cache.get(key)
    if cached and cached[0] > now:
        return cached[1], cached[2]

    try:
        quick_links = await asyncio.wait_for(
            _quick_widget_profile_links(data, mode, device), timeout=0.7,
        )
    except (TimeoutError, RuntimeError, aiosqlite.Error):
        quick_links = cached[1] if cached else []

    task = _profile_links_inflight.get(key)
    if task is None:
        async def resolve() -> tuple[list[dict[str, Any]], bool]:
            pending = False
            ttl = 20
            try:
                links = await asyncio.wait_for(
                    _widget_profile_links(data, mode, device), timeout=20,
                )
            except (TimeoutError, RuntimeError, aiosqlite.Error) as exc:
                _log("warning", "Profile enrichment deferred: %s", exc)
                links, pending, ttl = quick_links, True, 5
            except Exception as exc:
                _log("warning", "Profile enrichment failed and will retry: %s", exc)
                links, pending, ttl = quick_links, True, 5
            _profile_links_cache[key] = (time.monotonic() + ttl, links, pending)
            if len(_profile_links_cache) > 256:
                oldest = min(_profile_links_cache, key=lambda item: _profile_links_cache[item][0])
                _profile_links_cache.pop(oldest, None)
            return links, pending

        coroutine = resolve()
        task = (
            _module_lifecycle.create_task(coroutine, name="messenger-widget-profile-links")
            if _module_lifecycle is not None
            else asyncio.create_task(coroutine)
        )
        _profile_links_inflight[key] = task

        def finished(done: asyncio.Task[Any]) -> None:
            if _profile_links_inflight.get(key) is done:
                _profile_links_inflight.pop(key, None)

        task.add_done_callback(finished)

    try:
        if quick_links:
            return quick_links, True
        links, pending = await asyncio.wait_for(
            asyncio.shield(task), timeout=max(0.05, float(foreground_seconds)),
        )
        return links, pending
    except TimeoutError:
        return quick_links, True


def _widget_profile_kind_state(
    data: dict[str, Any], mode: str, device: dict[str, Any], kind: str,
) -> str:
    """Expose background profile discovery state without starting new work."""

    context = _widget_context(data, mode, device)
    key = (*_identity_context_key(context), str(int(device.get("id") or 0)))
    task = _profile_links_inflight.get(key)
    if task is not None and not task.done():
        return "pending"
    cached = _profile_links_cache.get(key)
    if not cached or cached[0] <= time.monotonic():
        return "pending"
    links, pending = cached[1], cached[2]
    if any(_clean(row.get("kind"), 40) == kind and _clean(row.get("url"), 2000) for row in links):
        return "verified"
    return "pending" if pending else "missing"


def _module_service(module_id: str, service: str):
    module = sys.modules.get(f"_nexus_mod_{module_id}")
    if module is None or not hasattr(module, service):
        raise HTTPException(503, f"Модуль {module_id} недоступен")
    return getattr(module, service)


async def _widget_getcourse_card_data(
    data: dict[str, Any], mode: str, device: dict[str, Any], *, include_access: bool = True,
    summary_only: bool = False,
) -> dict[str, Any]:
    context = _widget_context(data, mode, device)
    await _apply_identity_rules(context)
    if _identity_index is None:
        resolved = {"variables": {}}
    else:
        resolved = await _resolve_identity_context(context)
    context["email"] = context.get("email") or _resolved_variable(
        resolved, "contact.email", "getcourse.order.email", "getcourse.user.email",
    )
    context["phone"] = context.get("phone") or _resolved_variable(
        resolved, "contact.phone", "getcourse.order.phone", "getcourse.user.phone",
    )
    context["name"] = context.get("name") or _resolved_variable(
        resolved, "contact.name", "getcourse.order.name", "getcourse.user.name",
    )
    gc_id = ""
    if context.get("platform") == "getcourse" and context.get("entity_type") in {"user", "contact"}:
        gc_id = re.sub(r"\D+", "", _clean(context.get("entity_id"), 100))
    if not gc_id:
        gc_id = re.sub(r"\D+", "", _identity_field_value(context, "getcourse_user_id", "gc_user_id", "user_id"))
    if not gc_id and _identity_index is not None and (context.get("phone") or context.get("email")):
        resolver = getattr(_identity_index, "platform_id_for_context", None)
        if callable(resolver):
            gc_id = re.sub(r"\D+", "", await _run_identity_lookup(resolver, "getcourse", context))
    service = _module_service("student-transfer", "service_widget_student")
    return await service(
        gc_user_id=gc_id, email=context.get("email") or "", phone=context.get("phone") or "",
        name=context.get("name") or "",
        include_access=include_access, summary_only=summary_only,
    )


async def service_transfer_recipients(
    *, email: str = "", gc_user_id: str = "", name: str = "", phone: str = "",
) -> dict[str, Any]:
    if _identity_index is None:
        return {
            "ok": False, "status": "unavailable", "telegram": "", "vk": "",
            SALEBOT_PROVIDER: "", "conflicts": [],
        }
    context = {
        "service": "getcourse",
        "entity_type": "user",
        "entity_id": _clean(gc_user_id, 200),
        "getcourse_user_id": _clean(gc_user_id, 200),
        "email": _clean(email, 320),
        "phone": _normalize_phone(phone),
        "name": _clean(name, 200),
        "fields": {
            "email": _clean(email, 320), "gc_user_id": _clean(gc_user_id, 200),
            "phone": _normalize_phone(phone),
        },
    }
    telegram, vk, salebot = await asyncio.gather(*(
        _exact_provider_identity(service, context)
        for service in ("telegram", "vk", SALEBOT_PROVIDER)
    ))
    if not telegram:
        telegram = await _run_identity_lookup(_identity_index.platform_id_for_context, "telegram", context)
    if not salebot:
        # SaleBot has no standalone Customer DB table, therefore it can only
        # be recovered from the exact GetCourse order fields (usually
        # utm_term) and their linked Telegram record.
        salebot = await _exact_provider_identity(SALEBOT_PROVIDER, context)
    telegram_username = await _run_identity_lookup(
        _identity_index.telegram_username_for_platform_id, telegram,
    ) if telegram else ""
    return {
        "ok": bool(telegram or vk or salebot),
        "status": "resolved" if telegram or vk or salebot else "not_found",
        "telegram": telegram,
        "telegram_username": telegram_username,
        "vk": vk,
        SALEBOT_PROVIDER: salebot,
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
        verified = await _exact_provider_identity("vk", context)
        targets = [("vk", candidate)] if verified == candidate else []
    elif kind == "salebot":
        verified = await _exact_provider_identity(SALEBOT_PROVIDER, context)
        targets = [(SALEBOT_PROVIDER, candidate)] if verified == candidate else []
    elif kind == "candidate":
        vk, salebot = await asyncio.gather(*(
            _exact_provider_identity(provider, context)
            for provider in ("vk", SALEBOT_PROVIDER)
        ))
        targets = [(provider, value) for provider, value in (("vk", vk), (SALEBOT_PROVIDER, salebot)) if value == candidate]
        if not targets and _vk_reference(term):
            try:
                resolved_vk = await _vk_peer_id(term)
            except Exception:
                resolved_vk = ""
            if resolved_vk:
                resolved_context = {
                    **context,
                    "fields": {**context["fields"], "vk_platform_id": resolved_vk},
                }
                verified_vk = await _exact_provider_identity("vk", resolved_context)
                if verified_vk == resolved_vk:
                    targets = [("vk", resolved_vk)]
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


async def service_resolve_onboarding_telegram_target(*, utm_term: str) -> dict[str, Any]:
    """Resolve the exact Telegram chat used by GetCourse onboarding."""

    if _identity_index is None:
        return {
            "ok": False,
            "status": "unavailable",
            "platform_id": "",
            "source": "",
            "matches": [],
            "error": "identity index unavailable",
        }
    return await _run_identity_lookup(
        _identity_index.telegram_target_for_utm_term, _clean(utm_term, 1000),
    )


async def service_resolve_onboarding_target(
    *, utm_term: str, email: str = "", gc_user_id: str = "", phone: str = "",
) -> dict[str, Any]:
    """Resolve one exact onboarding channel, preferring direct Telegram over VK."""

    telegram = await service_resolve_onboarding_telegram_target(utm_term=utm_term)
    if telegram.get("ok"):
        return {
            "ok": True,
            "status": "ready",
            "provider": "telegram",
            "recipient_id": _clean(telegram.get("platform_id"), 200),
            "source": _clean(telegram.get("source"), 100) or "telegram",
        }
    fallback = await service_transfer_delivery_target(
        email=email, gc_user_id=gc_user_id, phone=phone, utm_term=utm_term,
    )
    if fallback.get("ok") and fallback.get("provider") == "vk":
        return {
            "ok": True,
            "status": "ready",
            "provider": "vk",
            "recipient_id": _clean(fallback.get("recipient_id"), 200),
            "source": "vk",
        }
    return {
        "ok": False,
        "status": fallback.get("status") or telegram.get("status") or "not_found",
        "provider": "",
        "recipient_id": "",
        "source": "",
        "error": fallback.get("reason") or fallback.get("error") or telegram.get("error") or "Получатель не найден",
    }


async def service_resolve_onboarding_targets(
    *, utm_term: str, email: str = "", gc_user_id: str = "", phone: str = "", name: str = "",
) -> dict[str, Any]:
    """Return every verified direct channel so one unavailable provider cannot block another."""

    candidates: list[dict[str, str]] = []
    errors: list[str] = []

    def add(provider: str, recipient_id: Any, source: str) -> None:
        clean_id = _clean(recipient_id, 200)
        if clean_id and not any(
            item["provider"] == provider and item["recipient_id"] == clean_id for item in candidates
        ):
            candidates.append({"provider": provider, "recipient_id": clean_id, "source": source})

    try:
        exact_telegram = await service_resolve_onboarding_telegram_target(utm_term=utm_term)
        if exact_telegram.get("ok"):
            add("telegram", exact_telegram.get("platform_id"), exact_telegram.get("source") or "utm_term")
        elif exact_telegram.get("error"):
            errors.append(_clean(exact_telegram.get("error"), 500))
    except Exception as exc:
        errors.append(_clean(exc, 500))

    try:
        identities = await service_transfer_recipients(
            email=email, gc_user_id=gc_user_id, name=name, phone=phone,
        )
        if identities.get("ok"):
            add("telegram", identities.get("telegram"), "identity")
            vk_id = _clean(identities.get("vk"), 200)
            if vk_id:
                allowed = await _vk_request(
                    "messages.isMessagesFromGroupAllowed",
                    {"user_id": vk_id, "group_id": _vk_group_id()},
                )
                is_allowed = (
                    bool(int((allowed or {}).get("is_allowed") or 0))
                    if isinstance(allowed, dict)
                    else False
                )
                if is_allowed:
                    add("vk", vk_id, "identity")
    except Exception as exc:
        errors.append(_clean(exc, 500))

    try:
        exact = await service_transfer_delivery_target(
            email=email, gc_user_id=gc_user_id, phone=phone, utm_term=utm_term,
        )
        if exact.get("ok") and exact.get("provider") == "vk":
            add("vk", exact.get("recipient_id"), "utm_term")
    except Exception as exc:
        errors.append(_clean(exc, 500))

    return {
        "ok": bool(candidates),
        "status": "ready" if candidates else "not_found",
        "candidates": candidates,
        "errors": [item for item in errors if item][:8],
        "error": "; ".join(item for item in errors if item)[:1000],
    }


async def service_resolve_vk_test_target(*, reference: str) -> dict[str, Any]:
    """Resolve an explicit VK profile and verify that the community may message it."""

    try:
        peer_id = await _vk_peer_id(reference)
        if not peer_id:
            return {"ok": False, "status": "not_found", "recipient_id": "", "error": "VK-пользователь не найден"}
        allowed = await _vk_request(
            "messages.isMessagesFromGroupAllowed",
            {"user_id": peer_id, "group_id": _vk_group_id()},
        )
        ready = bool(int((allowed or {}).get("is_allowed") or 0)) if isinstance(allowed, dict) else False
        return {
            "ok": ready,
            "status": "ready" if ready else "unavailable",
            "recipient_id": peer_id,
            "error": "" if ready else "Пользователь не разрешил сообщения сообщества",
        }
    except Exception as exc:
        return {"ok": False, "status": "failed", "recipient_id": "", "error": _clean(exc, 500)}


async def service_send_transfer_message(
    *, provider: str, recipient_id: str, content: str, operation_id: str,
    keyboard: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    provider = _clean(provider, 40).lower()
    recipient_id = _clean(recipient_id, 200)
    content = _clean(content, 4000)
    clean_operation_id = _clean(operation_id, 100)
    if provider not in {"vk", SALEBOT_PROVIDER} or not recipient_id or not content or not clean_operation_id:
        return {"ok": False, "status": "invalid", "error": "Некорректные параметры доставки"}
    try:
        if provider == SALEBOT_PROVIDER:
            details = await _salebot_send(recipient_id, content)
            return {"ok": True, "status": "sent", "provider": provider, "recipient_id": recipient_id, "details": details}
        random_id = int.from_bytes(
            hashlib.sha256(f"{provider}:{recipient_id}:{clean_operation_id}".encode()).digest()[:4],
            "big",
        ) & 0x7FFFFFFF
        payload: dict[str, Any] = {
            "group_id": _vk_group_id(), "peer_id": recipient_id,
            "random_id": random_id or 1, "message": content,
        }
        if keyboard:
            payload["keyboard"] = keyboard if isinstance(keyboard, str) else json.dumps(
                keyboard, ensure_ascii=False, separators=(",", ":")
            )
        result = await _vk_request("messages.send", payload)
        return {"ok": True, "status": "sent", "provider": provider, "recipient_id": recipient_id, "message_id": result}
    except Exception as exc:
        return {"ok": False, "status": "failed", "provider": provider, "recipient_id": recipient_id, "error": _clean(exc, 1000)}


async def _streams_admin(operator_name: str) -> dict[str, Any]:
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT id,wazzup_user_id,name,role FROM admins WHERE enabled=1 AND lower(name)=lower(?) LIMIT 1",
            (_clean(operator_name, 200),),
        )).fetchone()
    finally:
        await db.close()
    return dict(row) if row else {
        "id": 0,
        "wazzup_user_id": f"streams-{hashlib.sha256(_clean(operator_name, 200).encode()).hexdigest()[:16]}",
        "name": _clean(operator_name, 200) or "Streams",
        "role": "employee",
    }


async def service_email_staff() -> list[dict[str, Any]]:
    """Return the active staff identity used by the email-channel sender registry."""
    db = await _connect()
    try:
        rows = await (await db.execute(
            "SELECT id,name FROM admins WHERE enabled=1 ORDER BY name,id",
        )).fetchall()
    finally:
        await db.close()
    return [{"id": str(row["id"]), "name": _clean(row["name"], 200)} for row in rows if _clean(row["name"], 200)]


async def _streams_vk_profile_name(vk_id: str) -> str:
    clean_id = _clean(vk_id, 200)
    if not clean_id:
        return ""
    try:
        profiles = await _vk_request("users.get", {"user_ids": clean_id})
        profile = profiles[0] if isinstance(profiles, list) and profiles and isinstance(profiles[0], dict) else {}
        name = " ".join(filter(None, (
            _clean(profile.get("first_name"), 120),
            _clean(profile.get("last_name"), 120),
        ))).strip()
        if name:
            return name
    except Exception:
        _log("warning", "VK Streams profile name lookup failed user_id=%s", clean_id)
    link = await _external_link(peer_id=clean_id, provider="vk")
    return _clean(link.get("name"), 200) if link else ""


async def _streams_salebot_profile_name(salebot_id: str, channels: list[dict[str, Any]]) -> str:
    clean_id = _clean(salebot_id, 200)
    if not clean_id:
        return ""
    for channel in channels:
        if _clean(channel.get("provider"), 40).lower() != SALEBOT_PROVIDER:
            continue
        for message in channel.get("messages") if isinstance(channel.get("messages"), list) else []:
            if not isinstance(message, dict) or _clean(message.get("direction"), 40).lower() != "incoming":
                continue
            if name := _clean(message.get("author_name"), 200):
                return name
    link = await _external_link(peer_id=clean_id, provider=SALEBOT_PROVIDER)
    return _clean(link.get("name"), 200) if link else ""


async def service_streams_conversations(
    *, email: str = "", gc_user_id: str = "", name: str = "", phone: str = "", operator_name: str = "",
    include_history: bool = True, history_channel_id: str = "",
) -> dict[str, Any]:
    """Return the same channel/template data used by the full messenger widget."""

    normalized_phone = _normalize_phone(phone)
    identities, channels, admin = await asyncio.gather(
        service_transfer_recipients(
            email=email, gc_user_id=gc_user_id, name=name, phone=normalized_phone,
        ),
        _all_channels(),
        _streams_admin(operator_name),
    )
    telegram_hidden = _telegram_dialog_hidden(
        {
            "external_user_id": identities.get("telegram"),
            "telegram_username": identities.get("telegram_username"),
            "phone": normalized_phone,
        },
    )
    if telegram_hidden:
        channels = [
            channel for channel in channels
            if _clean(channel.get("provider"), 40).lower() != TELEGRAM_PROVIDER
        ]
    selected_history_channel = _clean(history_channel_id, 200)
    conversation_presence = (
        set()
        if include_history and not selected_history_channel
        else await _conversation_presence(channels, normalized_phone)
    )
    email_context = {
        "service": "getcourse", "platform": "getcourse", "entity_type": "user",
        "entity_id": _clean(gc_user_id, 200), "getcourse_user_id": _clean(gc_user_id, 200),
        "email": _clean(email, 320), "phone": normalized_phone, "name": _clean(name, 200),
    }

    async def channel_view(channel: dict[str, Any]) -> dict[str, Any]:
        channel_id = _clean(channel.get("channel_id"), 200)
        provider = _clean(channel.get("provider"), 40).lower() or (
            "vk" if channel_id.startswith("vk:")
            else TELEGRAM_PROVIDER if channel_id.startswith("telegram-personal:")
            else "wazzup"
        )
        transport = _clean(channel.get("transport"), 40).lower()
        if provider == EMAIL_PROVIDER:
            try:
                email_service = _module_service("email-channel", "service_conversation")
                result = await email_service(
                    context=email_context, offset=0,
                    limit=100 if include_history and (not selected_history_channel or channel_id == selected_history_channel) else 1,
                )
                return {
                    **channel, **(result.get("channel") or {}),
                    "chat_id": _clean(result.get("thread_id"), 250),
                    "has_chat": bool(result.get("has_chat")), "can_send": bool(result.get("can_send")),
                    "confirmed_chat": bool(result.get("confirmed_chat")),
                    "send_reason": _clean(result.get("send_reason"), 500),
                    "messages": result.get("messages") if include_history else [],
                    "history_error": "", "history_loaded": include_history,
                    "subject": _clean(result.get("subject"), 300),
                    "requires_subject": bool(result.get("requires_subject")),
                    "email_guidelines_required": result.get("email_guidelines_required") is not False,
                }
            except Exception as exc:
                return {**channel, "chat_id": "", "has_chat": False, "can_send": False,
                        "send_reason": "Email-канал временно недоступен", "messages": [],
                        "history_error": _clean(exc, 500), "history_loaded": True}
        exact_id = ""
        lookup_phone = normalized_phone
        if provider == "vk":
            exact_id = _clean(identities.get("vk"), 200)
            lookup_phone = ""
        elif provider == TELEGRAM_PROVIDER:
            exact_id = _clean(identities.get("telegram"), 200)
            lookup_phone = ""
        elif provider == SALEBOT_PROVIDER:
            exact_id = _clean(identities.get(SALEBOT_PROVIDER), 200)
            lookup_phone = ""
        load_history = include_history and (
            not selected_history_channel or channel_id == selected_history_channel
        )
        if not exact_id and not lookup_phone:
            return {
                "channel_id": channel_id,
                "transport": transport, "provider": provider,
                "label": _clean(channel.get("label") or channel.get("name") or transport, 200),
                "chat_id": "", "has_chat": False, "can_send": False,
                "send_reason": "Клиент не связан с этим каналом", "messages": [], "history_error": "",
                "history_loaded": load_history,
            }
        history_error = ""
        if not load_history:
            messages = []
            chat_id = exact_id
            has_chat = (channel_id, transport) in conversation_presence
            can_send, reason = _channel_send_state(channel, has_chat)
        elif provider == SALEBOT_PROVIDER:
            try:
                all_messages = await _salebot_history(exact_id)
                messages = all_messages[-100:]
                has_chat = bool(all_messages)
            except Exception as exc:
                messages = []
                has_chat = False
                history_error = _clean(getattr(exc, "detail", exc), 500)
            chat_id = exact_id
            can_send = bool(exact_id)
            reason = "" if can_send else "Клиент не связан с SaleBot"
        else:
            chat_id, has_chat, messages = await _conversation_rows(
                channel_id, transport, lookup_phone, 100,
                exact_chat_id=exact_id,
            )
            can_send, reason = _channel_send_state(channel, has_chat)
        if provider in {"vk", TELEGRAM_PROVIDER}:
            can_send = bool(exact_id)
            reason = "" if can_send else "Клиент не связан с этим каналом"
        return {
            "channel_id": channel_id,
            "transport": transport,
            "provider": provider,
            "label": _clean(channel.get("label") or channel.get("name") or transport, 200),
            "chat_id": exact_id or chat_id,
            "has_chat": has_chat,
            "can_send": can_send,
            "send_reason": reason,
            "messages": messages,
            "history_error": history_error,
            "history_loaded": load_history,
        }

    items = _prioritize_channels(list(await asyncio.gather(*(channel_view(channel) for channel in channels))))
    templates = await _template_rows(int(admin.get("id") or 0), can_edit_shared=False)
    vk_id = _clean(identities.get("vk"), 200)
    salebot_id = _clean(identities.get(SALEBOT_PROVIDER), 200)
    if include_history and not selected_history_channel:
        vk_profile_name, salebot_profile_name = await asyncio.gather(
            _streams_vk_profile_name(vk_id),
            _streams_salebot_profile_name(salebot_id, items),
        )
    else:
        vk_profile_name = salebot_profile_name = ""
    profile_links: list[dict[str, str]] = []
    if vk_id:
        profile_links.append({
            "kind": "vk", "label": f"VK: {vk_profile_name}" if vk_profile_name else "VK",
            "url": f"https://vk.com/id{quote(vk_id, safe='')}",
        })
    if (
        not telegram_hidden
        and (telegram_username := _clean(identities.get("telegram_username"), 200).lstrip("@"))
    ):
        profile_links.append({"kind": TELEGRAM_PROVIDER, "label": "Telegram", "url": f"https://t.me/{quote(telegram_username, safe='')}"})
    if salebot_id:
        profile_links.append({
            "kind": SALEBOT_PROVIDER,
            "label": f"SaleBot: {salebot_profile_name}" if salebot_profile_name else "SaleBot",
            "url": f"{SALEBOT_PROFILE_BASE}/{quote(salebot_id, safe='')}",
        })
    return {
        "ok": True,
        "channels": items,
        "templates": [item for item in templates if item.get("enabled", True)],
        "profile_links": profile_links,
        "send_all_default": False,
        "updated_at": _iso(),
    }


def _streams_context(*, email: str, gc_user_id: str, name: str, phone: str) -> dict[str, Any]:
    normalized_phone = _normalize_phone(phone)
    return {
        "service": "getcourse", "platform": "getcourse", "entity_type": "user",
        "entity_id": _clean(gc_user_id, 200), "getcourse_user_id": _clean(gc_user_id, 200),
        "email": _clean(email, 320), "phone": normalized_phone, "name": _clean(name, 200),
        "fields": {"email": _clean(email, 320), "phone": normalized_phone, "gc_user_id": _clean(gc_user_id, 200)},
    }


async def service_streams_template_preview(
    *, template_id: int = 0, body: str = "", email: str = "", gc_user_id: str = "",
    name: str = "", phone: str = "", operator_name: str = "",
) -> dict[str, Any]:
    admin = await _streams_admin(operator_name)
    current_body = _clean(body, 20_000)
    if template_id:
        rows = await _template_rows(int(admin.get("id") or 0), can_edit_shared=False)
        template = next((row for row in rows if int(row.get("id") or 0) == int(template_id)), None)
        if not template:
            raise ValueError("Шаблон не найден")
        current_body = _clean(template.get("body"), 20_000)
    if not current_body:
        raise ValueError("Текст не указан")
    context = _streams_context(email=email, gc_user_id=gc_user_id, name=name, phone=phone)
    if _identity_index is not None:
        resolved = await _resolve_identity_context(context)
        variables = resolved.get("variables") if isinstance(resolved.get("variables"), dict) else {}
    else:
        variables = build_context_variables([], context, {})
    rendered = render_message_template(current_body, variables)
    rendered["text"] = await _auto_markup_for_send(rendered["text"], variables)
    return {"ok": True, **rendered}


async def service_streams_template_favorite(
    *, template_id: int, favorite: bool, operator_name: str,
) -> dict[str, Any]:
    admin = await _streams_admin(operator_name)
    admin_id = int(admin.get("id") or 0)
    if not admin_id:
        raise ValueError("Пользователь Streams не связан с сотрудником виджета")
    return {"ok": True, "id": int(template_id), "favorite": await _set_template_favorite(admin_id, int(template_id), favorite)}


async def service_streams_send(
    *, channel_id: str, transport: str, provider: str, chat_id: str, phone: str,
    text: str, operator_name: str, email: str = "", gc_user_id: str = "", name: str = "",
    attachment_url: str = "", attachment_type: str = "", idempotency_key: str = "",
    record_communication: bool = True, subject: str = "",
    email_guidelines_confirmed: bool = False, email_guidelines_version: str = "",
) -> dict[str, Any]:
    """Send one text message using a channel already exposed to Streams."""

    channel_id = _clean(channel_id, 200)
    transport = _clean(transport, 40).lower()
    provider = _clean(provider, 40).lower()
    chat_id = _clean(chat_id, 250)
    message_text = _clean(text, 4000)
    attachment_url = _clean(attachment_url, 4000)
    attachment_type = _clean(attachment_type, 100)
    normalized_phone = _normalize_phone(phone)
    if not channel_id or transport not in CHAT_TRANSPORTS or (not message_text and not attachment_url):
        raise ValueError("Выберите канал и введите сообщение")
    if attachment_url and not attachment_url.startswith("https://"):
        raise ValueError("Вложение должно иметь HTTPS-ссылку")
    channel = await _requested_channel(channel_id, transport, provider)
    admin = await _streams_admin(operator_name)
    now = _iso()
    message: dict[str, Any]
    if provider == EMAIL_PROVIDER:
        # Streams normally sends a previewed message, but the service boundary
        # must also be safe for old tabs and direct callers. Complete template
        # variables and URL attribution here; auto-markup is idempotent when
        # the preview has already done the same work.
        email_context = _streams_context(
            email=email, gc_user_id=gc_user_id, name=name, phone=normalized_phone,
        )
        if _identity_index is not None:
            resolved = await _resolve_identity_context(email_context)
            variables = resolved.get("variables") if isinstance(resolved.get("variables"), dict) else {}
        else:
            variables = build_context_variables([], email_context, {})
        message_text = render_message_template(message_text, variables)["text"]
        message_text, signature_url = await _auto_markup_values_for_send(
            [message_text, "https://sobakovod.pro/"], variables,
        )
        if len(message_text) > 4000:
            raise ValueError("После авторазметки сообщение длиннее 4000 символов")
        email_service = _module_service("email-channel", "service_send")
        return await email_service(
            context=email_context,
            text=message_text, subject=_clean(subject, 300),
            manager_id=str(admin["id"]), manager_name=admin["name"], from_name=admin["name"],
            idempotency_key=idempotency_key or secrets.token_urlsafe(24),
            attachment_url=attachment_url, attachment_type=attachment_type,
            signature_url=signature_url,
            email_guidelines_confirmed=email_guidelines_confirmed,
            email_guidelines_version=_clean(email_guidelines_version, 40),
        )
    if provider == SALEBOT_PROVIDER:
        if not chat_id:
            identities = await service_transfer_recipients(
                email=email, gc_user_id=gc_user_id, name=name, phone=normalized_phone,
            )
            chat_id = _clean(identities.get(SALEBOT_PROVIDER), 200)
        if not chat_id:
            raise ValueError("Диалог SaleBot не найден")
        await _salebot_send(chat_id, message_text, attachment_url, attachment_type)
        response = {
            "ok": True,
            "channel": channel,
            "message": {
                "external_id": f"salebot:local:{secrets.token_hex(8)}",
                "direction": "outgoing",
                "status": "sent",
                "text": message_text,
                "content_uri": attachment_url,
                "content_type": attachment_type,
                "author_name": admin["name"],
                "sent_at": now,
            },
        }
        if record_communication and _db_path is not None:
            await _record_communication(
                dedupe_key=f"streams:{idempotency_key or secrets.token_hex(16)}",
                external_id=response["message"]["external_id"], provider=provider,
                channel_id=channel_id, chat_type=transport, chat_id=chat_id,
                direction="outgoing", status="sent", text=message_text, sent_at=now,
                client_name=name, phone_hash=_phone_hash(normalized_phone),
                admin_id=int(admin["id"]), manager_name=admin["name"],
                transport_author=admin["name"],
            )
        return response
    if provider == "vk":
        if not chat_id:
            raise ValueError("Диалог VK не найден")
        send_params: dict[str, Any] = {
            "group_id": _vk_group_id(), "peer_id": chat_id,
            "random_id": (
                int(hashlib.sha256(idempotency_key.encode()).hexdigest()[:8], 16) % 2_000_000_000 + 1
                if idempotency_key else secrets.randbelow(2_000_000_000) + 1
            ), "message": message_text,
        }
        if attachment_url:
            send_params["attachment"] = await _vk_upload_widget_image(chat_id, attachment_url)
        result = await _vk_request("messages.send", send_params)
        link = await _external_link(peer_id=chat_id) or {
            "phone": normalized_phone, "getcourse_user_id": _clean(gc_user_id, 200), "name": _clean(name, 200),
        }
        message_id = _clean(result, 200) if not isinstance(result, dict) else _clean(result.get("message_id"), 200)
        await _store_vk_messages(chat_id, [{
            "id": message_id or f"local-{secrets.token_hex(12)}", "from_id": f"-{_vk_group_id()}",
            "peer_id": chat_id, "date": int(_now_dt().timestamp()), "text": message_text, "attachments": [],
        }], link, outgoing_author_name=admin["name"])
        message = {
            "external_id": f"vk:{_vk_group_id()}:{chat_id}:{message_id}", "direction": "outgoing",
            "status": "delivered", "text": message_text, "author_name": admin["name"], "sent_at": now,
        }
    elif provider == TELEGRAM_PROVIDER:
        if not chat_id:
            raise ValueError("Диалог Telegram не найден")
        telegram_options: dict[str, Any] = {"author_name": admin["name"]}
        if attachment_url:
            telegram_options.update(attachment_url=attachment_url, attachment_type=attachment_type)
        message = await _telegram_send_text(chat_id, message_text, **telegram_options)
    else:
        if not normalized_phone:
            raise ValueError("Для этого канала нужен телефон")
        known_chat_id, has_chat, _ = await _conversation_rows(channel_id, transport, normalized_phone, 1)
        target_chat = chat_id or known_chat_id
        identity = await resolve_client_identity(phone=normalized_phone, email=email, getcourse_user_id=gc_user_id)
        payload: dict[str, Any] = {
            "channelId": channel_id, "chatType": transport,
            "crmUserId": admin["wazzup_user_id"],
        }
        if has_chat and target_chat:
            payload["chatId"] = target_chat
        else:
            payload.update(_first_message_recipient(channel, transport, normalized_phone, identity))
        message_key = hashlib.sha256(idempotency_key.encode()).hexdigest()[:32] if idempotency_key else secrets.token_hex(16)
        deliveries: list[tuple[dict[str, Any], str, str]] = []
        if attachment_url:
            attachment_payload = {
                **payload, "contentUri": attachment_url,
                "crmMessageId": f"nexus-{message_key}-file",
            }
            deliveries.append((
                await _wazzup_request("POST", "/message", attachment_payload),
                attachment_payload["crmMessageId"], attachment_url,
            ))
        if message_text:
            text_payload = {
                **payload, "text": message_text,
                "crmMessageId": f"nexus-{message_key}" + ("-text" if attachment_url else ""),
            }
            deliveries.append((
                await _wazzup_request("POST", "/message", text_payload),
                text_payload["crmMessageId"], "",
            ))
        response, response_key, _ = deliveries[-1]
        external_id = _clean((response or {}).get("messageId") or (response or {}).get("id"), 250) or f"local-{response_key}"
        response_chat_id = _clean((response or {}).get("chatId"), 250) or target_chat
        db = await _connect()
        try:
            for delivery, delivery_key, content_uri in deliveries:
                delivery_id = _clean(
                    (delivery or {}).get("messageId") or (delivery or {}).get("id"), 250,
                ) or f"local-{delivery_key}"
                delivery_chat_id = _clean((delivery or {}).get("chatId"), 250) or target_chat
                await db.execute(
                    """INSERT OR IGNORE INTO wazzup_messages(
                       external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                       ) VALUES(?,?,?,?,?,'outgoing','accepted',?,?,?,?,?,?)""",
                    (
                        delivery_id, channel_id, transport, delivery_chat_id, _phone_hash(normalized_phone),
                        message_text if not content_uri else "", content_uri, admin["name"], now, "", now,
                    ),
                )
            await db.commit()
        finally:
            await db.close()
        message = {
            "external_id": external_id, "chat_id": response_chat_id, "direction": "outgoing",
            "status": "sent", "text": message_text, "content_uri": attachment_url,
            "content_type": attachment_type, "author_name": admin["name"], "sent_at": now,
        }
    if record_communication and _db_path is not None:
        await _record_communication(
            dedupe_key=f"streams:{idempotency_key or secrets.token_hex(16)}",
            external_id=_clean(message.get("external_id"), 250), provider=provider,
            channel_id=channel_id, chat_type=transport, chat_id=chat_id,
            direction="outgoing", status=_clean(message.get("status"), 40) or "sent",
            text=message_text, sent_at=message.get("sent_at") or now,
            client_name=name, phone_hash=_phone_hash(normalized_phone),
            admin_id=int(admin["id"]), manager_name=admin["name"],
            transport_author=_clean(message.get("author_name"), 200),
        )
    return {"ok": True, "status": "sent", "message": message}


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
    resolution_timeout: float = 6,
) -> dict[str, str]:
    context = _widget_context(data, mode, device)
    await _apply_identity_rules(context)
    if provider == TELEGRAM_PROVIDER and _telegram_dialog_hidden({
        **context,
        "telegram_id": _identity_field_value(context, "telegram_id", "tg_id"),
        "telegram_username": _identity_field_value(
            context, "telegram_username", "tg_username",
        ),
    }):
        return {}
    exact_provider = "telegram" if provider == TELEGRAM_PROVIDER else provider
    exact_reference = await _exact_provider_identity(exact_provider, context)
    existing = await _entity_external_link(
        context["platform"], context["entity_type"], context["entity_id"], provider,
    )
    page_kind, page_id = _page_context(data.get("source_url"))
    gc_id = page_id if page_kind == "user" else _identity_field_value(context, "getcourse_user_id")
    exact_amo_identity = context.get("platform") == "amocrm" and provider in {"vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER}
    if context.get("platform") == "amocrm" and provider == TELEGRAM_PROVIDER:
        # A phone/name link remembered for another card is not proof that the
        # current amoCRM contact exists in Telegram.  Existing direct threads
        # are handled before this resolver; a new card must pass a live phone
        # lookup in Telegram Personal.
        if existing and mode != "test":
            await _forget_entity_external_link(context, provider)
        existing = {}
        exact_reference = ""
    if existing and exact_amo_identity and (
        not exact_reference
        or _clean(existing.get("external_user_id"), 300) != _clean(exact_reference, 300)
    ):
        if mode != "test":
            await _forget_entity_external_link(context, provider)
        existing = {}
    if (
        existing
        and provider == TELEGRAM_PROVIDER
        and _telegram_dialog_hidden(existing)
    ):
        return {}
    if existing and _card_link_matches_context(existing, context, gc_id):
        if mode != "test":
            owner_id = await _responsible_admin_id(data, mode, device)
            await _remember_notification_context(
                context, provider, _clean(existing.get("external_user_id"), 250), owner_id,
            )
        return existing

    identity = {
        "getcourse_user_id": gc_id,
        "phone": _normalize_phone(context.get("phone")),
        "email": _clean(context.get("email"), 320),
        "name": _clean(context.get("name"), 200),
    }

    if provider == SALEBOT_PROVIDER:
        reference = exact_reference
        if not reference:
            return {}
        if mode != "test":
            await _remember_external_link(identity, context["platform"], provider=provider, external_user_id=reference)
            owner_id = await _responsible_admin_id(data, mode, device)
            await _remember_entity_external_link(context, provider, reference, owner_id)
            await _remember_notification_context(context, provider, reference, owner_id)
            await _assign_client_threads(owner_id, direct_links=[(provider, reference)])
        return {
            **identity, "provider": provider, "external_user_id": reference,
            "source": context["platform"], "updated_at": _iso(),
        }

    if provider == "vk":
        reference = exact_reference or _identity_field_value(context, "vk_id", "vkontakte_id", "senler_id")
        if not reference and _identity_index is not None and context.get("platform") != "amocrm":
            lookup = {
                **context,
                "getcourse_user_id": gc_id,
                "fields": {**(context.get("fields") or {}), "getcourse_user_id": gc_id},
            }
            reference = await _run_identity_lookup(_identity_index.platform_id_for_context, "vk", lookup)
        if not reference and context.get("platform") != "amocrm":
            resolved = await _resolve_widget_context(data, mode, device)
            reference = _account_identity_value(resolved.get("accounts", []), "vk")
        if not reference and _identity_index is not None and context.get("platform") != "amocrm":
            for _, candidate in parse_utm_term(_identity_field_value(context, "utm_term")):
                reference = await _run_identity_lookup(_identity_index.platform_id_for_service, "vk", candidate)
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
        identity["telegram_id"] = "" if context.get("platform") == "amocrm" else (
            exact_reference or _identity_field_value(context, "telegram_id", "tg_id")
        )
        if context.get("platform") != "amocrm":
            identity["telegram_id"] = identity["telegram_id"] or _identity_field_value(context, "platform_id")
        identity["telegram_username"] = "" if context.get("platform") == "amocrm" else (
            _identity_field_value(context, "telegram_username", "tg_username")
            or _clean(identity.get("telegram_username"), 200)
        )
        if (
            not identity["telegram_id"] and not identity["telegram_username"]
            and context.get("platform") != "amocrm"
        ):
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
            peer_id = await asyncio.wait_for(
                _telegram_run(resolve_telegram), timeout=max(1.0, float(resolution_timeout)),
            )
        except TimeoutError:
            return {"pending": "1"}
        if not peer_id:
            _remember_card_link(cache_key, {})
            return {}

    if provider == TELEGRAM_PROVIDER and _telegram_dialog_hidden(identity, peer_id=peer_id):
        _remember_card_link(cache_key, {})
        return {}

    if mode != "test":
        await _remember_external_link(
            identity, f"{context['platform']}-card", provider=provider, external_user_id=peer_id,
        )
        owner_id = await _responsible_admin_id(data, mode, device)
        await _remember_entity_external_link(context, provider, peer_id, owner_id)
        await _remember_notification_context(context, provider, peer_id, owner_id)
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
    keys = set(row.keys()) if hasattr(row, "keys") else set()
    return {
        "id": int(row["id"]),
        "folder": _clean(row["folder"], 120),
        "title": row["title"],
        "body": row["body"],
        "scope": "personal" if owner_id is not None else "shared",
        "editable": (owner_id is None and can_edit_shared) or (owner_id is not None and int(owner_id) == int(current_admin_id or 0)),
        "enabled": bool(row["enabled"]),
        "favorite": bool(row["is_favorite"]) if "is_favorite" in keys else False,
        "favorite_order": int(row["favorite_order"]) if "favorite_order" in keys and row["favorite_order"] is not None else None,
        "sort_order": int(row["sort_order"]),
        "user_order": int(row["user_sort_order"]) if "user_sort_order" in keys and row["user_sort_order"] is not None else None,
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
    if admin_id is None:
        sql = "SELECT message_templates.*,0 AS is_favorite,NULL AS favorite_order,NULL AS user_sort_order FROM message_templates"
    else:
        sql = """SELECT message_templates.*,
                 EXISTS(SELECT 1 FROM template_favorites favorite
                        WHERE favorite.admin_id=? AND favorite.template_id=message_templates.id) AS is_favorite,
                 (SELECT favorite.sort_order FROM template_favorites favorite
                  WHERE favorite.admin_id=? AND favorite.template_id=message_templates.id) AS favorite_order
                 ,(SELECT ordering.sort_order FROM template_user_order ordering
                   WHERE ordering.admin_id=? AND ordering.template_id=message_templates.id) AS user_sort_order
                 FROM message_templates"""
        params = [admin_id, admin_id, admin_id, *params]
    if where:
        sql += " WHERE " + " AND ".join(where)
    if admin_id is None:
        sql += " ORDER BY owner_admin_id IS NOT NULL,folder,sort_order,title,id"
    else:
        sql += " ORDER BY user_sort_order IS NULL,user_sort_order,owner_admin_id IS NOT NULL,folder,sort_order,title,id"
    db = await _connect()
    try:
        rows = await (await db.execute(sql, params)).fetchall()
        return [_template_view(row, admin_id, can_edit_shared) for row in rows]
    finally:
        await db.close()


async def _set_template_favorite(admin_id: int, template_id: int, favorite: bool) -> bool:
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute(
                "SELECT id FROM message_templates WHERE id=? AND (owner_admin_id IS NULL OR owner_admin_id=?)",
                (template_id, admin_id),
            )
        ).fetchone()
        if not row:
            raise HTTPException(404, "Шаблон не найден")
        if favorite:
            await db.execute(
                """INSERT OR IGNORE INTO template_favorites(admin_id,template_id,sort_order,created_at)
                   VALUES(?,?,COALESCE((SELECT MAX(sort_order)+1 FROM template_favorites WHERE admin_id=?),0),?)""",
                (admin_id, template_id, admin_id, _iso()),
            )
        else:
            await db.execute(
                "DELETE FROM template_favorites WHERE admin_id=? AND template_id=?",
                (admin_id, template_id),
            )
        await db.commit()
        return favorite
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


async def _save_template(data: dict[str, Any], owner_admin_id: int | None, template_id: int | None = None) -> dict[str, Any]:
    folder = "" if owner_admin_id is not None else _clean(data.get("folder"), 120)
    title = _clean(data.get("title"), 120)
    body = await _auto_markup_message(_clean(data.get("body"), 20_000))
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


async def _set_template_order(admin_id: int, template_ids: Any) -> list[int]:
    if not isinstance(template_ids, list) or len(template_ids) > 2000:
        raise HTTPException(400, "Некорректный порядок шаблонов")
    try:
        ordered = [int(value) for value in template_ids]
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, "Некорректный порядок шаблонов") from exc
    if not ordered or len(ordered) != len(set(ordered)) or any(value <= 0 for value in ordered):
        raise HTTPException(400, "Передайте уникальные шаблоны")
    placeholders = ",".join("?" for _ in ordered)
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"SELECT id FROM message_templates WHERE id IN ({placeholders}) AND enabled=1 AND (owner_admin_id IS NULL OR owner_admin_id=?)",
            (*ordered, admin_id),
        )).fetchall()
        if {int(row["id"]) for row in rows} != set(ordered):
            raise HTTPException(404, "Один из шаблонов недоступен")
        now = _iso()
        await db.execute("BEGIN IMMEDIATE")
        await db.executemany(
            """INSERT INTO template_user_order(admin_id,template_id,sort_order,updated_at) VALUES(?,?,?,?)
               ON CONFLICT(admin_id,template_id) DO UPDATE SET sort_order=excluded.sort_order,updated_at=excluded.updated_at""",
            ((admin_id, template_id, index, now) for index, template_id in enumerate(ordered)),
        )
        await db.commit()
        return ordered
    except Exception:
        await db.rollback()
        raise
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
    known_selected = selected.intersection(channel_map)
    # A browser may keep channel ids from an older Wazzup configuration.
    # When every saved id is stale, fall back to all current channels instead
    # of returning an apparently empty inbox forever.
    channel_ids = [channel_id for channel_id in channel_map if not known_selected or channel_id in known_selected]
    if not channel_ids:
        return {"items": [], "unread": 0, "unanswered": 0}
    placeholders = ",".join("?" for _ in channel_ids)
    where = [f"c.channel_id IN ({placeholders})"]
    params: list[Any] = list(channel_ids)
    hidden_peer_placeholders = ",".join("?" for _ in TELEGRAM_HIDDEN_PEER_IDS)
    where.append(
        f"NOT (c.channel_id LIKE 'telegram-personal:%' "
        f"AND c.chat_id IN ({hidden_peer_placeholders}))"
    )
    params.extend(sorted(TELEGRAM_HIDDEN_PEER_IDS))
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
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                    f"""SELECT c.channel_id,c.chat_type,c.chat_id,c.phone_hash,c.contact_name,c.last_message_at,
                           l.phone,l.getcourse_user_id,l.name AS link_name,l.source AS link_source,
                           x.provider AS external_provider,x.getcourse_user_id AS external_getcourse_user_id,
                           x.phone AS external_phone,x.name AS external_name,
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
                    LEFT JOIN external_identity_links x
                      ON x.external_user_id=c.chat_id AND x.provider=CASE
                           WHEN c.channel_id LIKE 'vk:%' THEN 'vk'
                           WHEN c.channel_id LIKE 'telegram-personal:%' THEN 'telegram_personal'
                           ELSE '' END
                    WHERE {where_sql}
                    ORDER BY c.last_message_at DESC,c.id DESC LIMIT ?""",
                (*params, INBOX_LIMIT),
            )
        ).fetchall()
        visible_keys = [(row["channel_id"], row["chat_type"], row["chat_id"]) for row in rows]
        if visible_keys:
            visible_values = ",".join("(?,?,?)" for _ in visible_keys)
            unread_params: list[Any] = []
            for key in visible_keys:
                unread_params.extend(key)
            unread_params.extend((device_id, initialized_at))
            unread_rows = await (
                await db.execute(
                    f"""WITH visible(channel_id,chat_type,chat_id) AS (VALUES {visible_values})
                        SELECT m.channel_id,m.chat_type,m.chat_id,COUNT(*) AS unread
                        FROM visible v
                        JOIN wazzup_messages m ON m.channel_id=v.channel_id
                             AND m.chat_type=v.chat_type AND m.chat_id=v.chat_id
                        LEFT JOIN inbox_reads r ON r.device_id=? AND r.channel_id=m.channel_id
                             AND r.chat_type=m.chat_type AND r.chat_id=m.chat_id
                        WHERE m.direction='incoming' AND m.sent_at>COALESCE(r.last_read_at,?)
                        GROUP BY m.channel_id,m.chat_type,m.chat_id""",
                    unread_params,
                )
            ).fetchall()
        else:
            unread_rows = []
    finally:
        await db.close()
    unread_map = {
        (row["channel_id"], row["chat_type"], row["chat_id"]): int(row["unread"])
        for row in unread_rows
    }
    items: list[dict[str, Any]] = []
    links_to_remember: list[dict[str, str]] = []
    for row in rows:
        item = dict(row)
        message = _message_view(item)
        channel = channel_map[item["channel_id"]]
        external_link: dict[str, Any] = {}
        if channel.get("provider") in {"vk", TELEGRAM_PROVIDER}:
            if not item.get("external_provider"):
                continue
            external_link = {
                "external_user_id": item["chat_id"],
                "getcourse_user_id": item.get("external_getcourse_user_id") or "",
                "phone": item.get("external_phone") or "",
                "name": item.get("external_name") or "",
            }
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
            links_to_remember.append({
                "phone": phone, "getcourse_user_id": gc_user_id, "name": name,
                "source": "inbox" if gc_user_id else "inbox-resolved",
            })
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
    await _remember_client_links(links_to_remember)
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
        contacts = int((await (await db.execute("SELECT COALESCE(MAX(id),0) FROM contacts")).fetchone())[0])
        chats = int((await (await db.execute("SELECT COALESCE(MAX(id),0) FROM wazzup_chats")).fetchone())[0])
        messages = int((await (await db.execute("SELECT COALESCE(MAX(id),0) FROM wazzup_messages")).fetchone())[0])
    finally:
        await db.close()
    identity = dict(_identity_index_status) if _identity_index is not None else {"status": "unavailable"}
    return {"ok": True, "module": MODULE_ID, "api_key_configured": bool(_api_key()), "admins": admins, "devices": devices, "contacts": contacts, "chats": chats, "messages": messages, "counts_approximate": True, "identity": identity}


@router.get("/guide", response_class=HTMLResponse)
@router.get("/help", response_class=HTMLResponse)
async def user_guide() -> HTMLResponse:
    path = _must_db().parent.parent / "panel" / "docs.html"
    return HTMLResponse(
        path.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-cache, must-revalidate",
            "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; frame-ancestors 'self'",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )


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
        "identity": dict(_identity_index_status) if _identity_index is not None else {"status": "unavailable"},
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
    expected_key = _vk_callback_config["key"]
    if not expected_key or not secrets.compare_digest(_clean(key, 200), expected_key):
        return PlainTextResponse("not found", status_code=404)
    try:
        body = await request.body()
    except ClientDisconnect:
        return PlainTextResponse("error", status_code=400)
    if len(body) > MAX_WEBHOOK_BYTES:
        return PlainTextResponse("error", status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return PlainTextResponse("error", status_code=400)
    if _clean(payload.get("group_id"), 80) != _vk_group_id():
        return PlainTextResponse("error", status_code=403)
    secret = _clean(payload.get("secret"), 1000)
    expected_secret = _vk_callback_config["secret"]
    if expected_secret and not secrets.compare_digest(secret, expected_secret):
        return PlainTextResponse("error", status_code=403)
    if payload.get("type") == "confirmation":
        return PlainTextResponse(_vk_callback_config["confirmation"])
    if payload.get("type") in {"message_new", "message_reply", "message_edit"}:
        try:
            await _enqueue_vk_callback(body, payload)
        except Exception as exc:
            _log("error", "VK callback queue write failed: %s", exc)
            return PlainTextResponse("error", status_code=503)
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


async def _notification_system_status() -> dict[str, Any]:
    callback_secret = await _setting("notification_telegram_callback_secret")
    callback_url = f"{PUBLIC_API_BASE}/notifications/telegram/{callback_secret}"
    salebot_url = f"{PUBLIC_API_BASE}/notifications/salebot/{await _setting('notification_salebot_secret')}"
    result: dict[str, Any] = {
        "telegram": {
            "configured": bool(_notification_bot_token()), "bot": f"@{NOTIFY_TELEGRAM_USERNAME}",
            "callback": False, "callback_url": callback_url, "conflict": False,
            "mode": "polling", "polling": bool(_notification_bot_poll_at and not _notification_bot_poll_error),
            "poll_at": _notification_bot_poll_at, "poll_error": _notification_bot_poll_error,
        },
        "salebot": {"url": salebot_url},
    }
    if not _notification_bot_token():
        return result
    try:
        me, webhook = await asyncio.gather(
            _notification_tg_call("getMe"), _notification_tg_call("getWebhookInfo"),
        )
        username = _clean(me.get("username"), 200)
        current_url = _clean(webhook.get("url"), 4000)
        result["telegram"].update({
            "valid": username.casefold() == NOTIFY_TELEGRAM_USERNAME.casefold(),
            "username": f"@{username}" if username else "",
            "callback": current_url == callback_url,
            "conflict": bool(current_url and current_url != callback_url),
            "pending_updates": int(webhook.get("pending_update_count") or 0),
            "last_error": _clean(webhook.get("last_error_message"), 300),
        })
    except NotificationDeliveryError as exc:
        result["telegram"].update({"valid": False, "error": str(exc)})
    return result


@router.get("/notification-system/status")
async def notification_system_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, **await _notification_system_status()}


@router.post("/notification-system/telegram/register")
async def notification_telegram_register(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-notify-telegram-register", limit=10, window_seconds=3600, subject=user["username"])
    me = await _notification_tg_call("getMe")
    username = _clean(me.get("username"), 200)
    if username.casefold() != NOTIFY_TELEGRAM_USERNAME.casefold():
        raise HTTPException(409, f"Ожидался @{NOTIFY_TELEGRAM_USERNAME}, токен принадлежит @{username or 'unknown'}")
    callback_url = f"{PUBLIC_API_BASE}/notifications/telegram/{await _setting('notification_telegram_callback_secret')}"
    webhook = await _notification_tg_call("getWebhookInfo")
    current_url = _clean(webhook.get("url"), 4000)
    if current_url and current_url != callback_url:
        raise HTTPException(409, "У бота уже настроен другой webhook. Nexus не стал его перезаписывать.")
    if current_url == callback_url:
        await _notification_tg_call("deleteWebhook", {"drop_pending_updates": False})
    return {"ok": True, **await _notification_system_status()}


@router.post("/notifications/telegram/{secret}")
async def notification_telegram_callback(secret: str, request: Request) -> JSONResponse:
    expected = await _setting("notification_telegram_callback_secret")
    if not expected or not secrets.compare_digest(_clean(secret, 1000), expected):
        return JSONResponse({"ok": False}, status_code=404)
    body = await request.body()
    if len(body) > 256 * 1024:
        return JSONResponse({"ok": False, "error": "payload too large"}, status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
    await _handle_notification_telegram_update(payload)
    return JSONResponse({"ok": True})


@router.post("/notifications/salebot/{secret}")
async def notification_salebot_callback(secret: str, request: Request) -> JSONResponse:
    expected = await _setting("notification_salebot_secret")
    if not expected or not secrets.compare_digest(_clean(secret, 1000), expected):
        return JSONResponse({"ok": False}, status_code=404)
    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        return JSONResponse({"ok": False, "error": "payload too large"}, status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "json object required"}, status_code=400)
    outside_raw = payload.get("message_from_outside")
    if outside_raw is not None:
        try:
            if int(outside_raw) not in {0, 1}:
                return JSONResponse({"ok": True, "inserted": False, "ignored": "service_event"})
        except (TypeError, ValueError):
            return JSONResponse({"ok": True, "inserted": False, "ignored": "service_event"})
    if _messenger_button_event(payload):
        return JSONResponse({"ok": True, "inserted": False, "ignored": "button_event"})
    client_id = _clean(payload.get("client_id"), 200)
    text_value = _clean(payload.get("text"), 20_000)
    attachments = payload.get("attachments") if isinstance(payload.get("attachments"), list) else []
    if not client_id or (not text_value and not attachments):
        return JSONResponse({"ok": False, "error": "client_id and text or attachments required"}, status_code=400)
    event_key = _clean(
        payload.get("message_id") or payload.get("id") or request.headers.get("idempotency-key"),
        250,
    ) or secrets.token_urlsafe(18)
    external_id = f"salebot-hook:{client_id}:{event_key}"
    sent_at = _message_time(payload.get("sent_at") or payload.get("created_at"))
    first_attachment = attachments[0] if attachments and isinstance(attachments[0], dict) else {}
    content_uri = _clean(first_attachment.get("url"), 4000)
    content_type = _clean(first_attachment.get("type") or first_attachment.get("content_type"), 100)
    link = await _external_link(peer_id=client_id, provider=SALEBOT_PROVIDER)
    client_name = _clean(payload.get("client_name") or link.get("name"), 200) or f"SaleBot {client_id}"
    now = _iso()
    db = await _connect()
    try:
        context = await (await db.execute(
            "SELECT admin_id FROM conversation_contexts WHERE provider=? AND external_user_id=?",
            (SALEBOT_PROVIDER, client_id),
        )).fetchone()
        owner_id = int(context["admin_id"]) if context and context["admin_id"] else None
        cursor = await db.execute(
            """INSERT OR IGNORE INTO wazzup_messages(
               external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,
               content_uri,author_name,sent_at,raw_json,created_at
               ) VALUES(?,'salebot:project','salebot',?,'','incoming','delivered',?,?,?,?,?,?)""",
            (
                external_id, client_id, text_value, content_uri, client_name, sent_at,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"))[:50_000], now,
            ),
        )
        inserted = cursor.rowcount > 0
        await db.execute(
            """INSERT INTO wazzup_chats(
               channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,
               last_message_preview,responsible_admin_id,created_at,updated_at
               ) VALUES('salebot:project','salebot',?,'',?,?,?,?,?,?)
               ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
               contact_name=excluded.contact_name,last_message_at=excluded.last_message_at,
               last_message_preview=excluded.last_message_preview,
               responsible_admin_id=COALESCE(excluded.responsible_admin_id,wazzup_chats.responsible_admin_id),
               updated_at=excluded.updated_at""",
            (client_id, client_name, sent_at, (text_value or "Вложение")[:500], owner_id, now, now),
        )
        await db.commit()
    finally:
        await db.close()
    if inserted:
        await _enqueue_notification_message(
            external_id=external_id, channel_id="salebot:project", chat_type="salebot",
            chat_id=client_id, provider=SALEBOT_PROVIDER, client_name=client_name,
            text=text_value, content_type=content_type, sent_at=sent_at, delay_seconds=0,
            raw_payload=payload,
        )
    return JSONResponse({"ok": True, "inserted": inserted, "external_id": external_id})


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
                   EXISTS(SELECT 1 FROM activation_codes c WHERE c.admin_id=a.id AND c.expires_at>?) AS has_activation_code,
                   COALESCE((SELECT CAST(s.value AS INTEGER) FROM module_settings s
                             WHERE s.key='admin_amo_task_enabled:'||a.id),1) AS amo_task_enabled,
                   (SELECT s.value FROM module_settings s
                    WHERE s.key='admin_amo_task_sources:'||a.id) AS amo_task_sources_json
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
        {
            **{key: value for key, value in dict(row).items() if key != "amo_task_sources_json"},
            "enabled": bool(row["enabled"]),
            "amo_task_enabled": bool(row["amo_task_enabled"]),
            "amo_task_sources": _parse_amo_task_sources(row["amo_task_sources_json"]),
            "bindings": by_admin.get(int(row["id"]), []),
        }
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


@router.get("/notification-routing")
async def notification_routing(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        admins = await (await db.execute(
            "SELECT id,name,enabled FROM admins WHERE enabled=1 ORDER BY name,id"
        )).fetchall()
        policies = await (await db.execute(
            "SELECT source_admin_id FROM notification_route_policies WHERE configured=1"
        )).fetchall()
        routes = await (await db.execute(
            "SELECT source_admin_id,recipient_admin_id FROM notification_routes ORDER BY source_admin_id,recipient_admin_id"
        )).fetchall()
        amo_sources = await (await db.execute(
            """SELECT DISTINCT a.id,a.name,a.enabled FROM admins a
               JOIN manager_bindings b ON b.admin_id=a.id AND b.platform='amocrm'
               WHERE a.enabled=1 ORDER BY a.name,a.id"""
        )).fetchall()
        chat_preferences = await (await db.execute(
            "SELECT admin_id,course_chats FROM notification_preferences WHERE course_chats=1"
        )).fetchall()
        destination_rows = await (await db.execute(
            """SELECT admin_id,provider FROM notification_destinations
               WHERE enabled=1 UNION ALL
               SELECT admin_id,'browser' FROM browser_notification_subscriptions WHERE enabled=1"""
        )).fetchall()
    finally:
        await db.close()
    configured = {int(row["source_admin_id"]) for row in policies}
    mapped: dict[int, list[int]] = {}
    for row in routes:
        mapped.setdefault(int(row["source_admin_id"]), []).append(int(row["recipient_admin_id"]))
    destination_map: dict[int, list[str]] = {}
    for row in destination_rows:
        values = destination_map.setdefault(int(row["admin_id"]), [])
        provider = _clean(row["provider"], 40)
        if provider and provider not in values:
            values.append(provider)
    return {
        "ok": True,
        "admins": [dict(row) | {
            "enabled": bool(row["enabled"]),
            "notification_channels": destination_map.get(int(row["id"]), []),
        } for row in admins],
        "amo_sources": [dict(row) | {"enabled": bool(row["enabled"])} for row in amo_sources],
        "course_chat_admin_ids": [int(row["admin_id"]) for row in chat_preferences if row["course_chats"]],
        "routes": [{
            "source_admin_id": int(row["id"]),
            "configured": int(row["id"]) in configured,
            "recipient_admin_ids": mapped.get(int(row["id"]), [int(row["id"])] if int(row["id"]) not in configured else []),
        } for row in admins],
    }


@router.put("/notification-routing/course-chats/{admin_id}")
async def save_course_chat_notifications(admin_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-course-chat-routing", limit=240, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    if not isinstance(data.get("enabled"), bool):
        raise HTTPException(400, "Укажите, включены ли уведомления учебных чатов")
    db = await _connect()
    try:
        admin = await (await db.execute(
            "SELECT id,name FROM admins WHERE id=? AND enabled=1", (admin_id,),
        )).fetchone()
        if not admin:
            raise HTTPException(404, "Сотрудник не найден")
        now = _iso()
        await db.execute(
            """INSERT INTO notification_preferences(admin_id,fallback_unassigned,course_chats,updated_at)
               VALUES(?,0,?,?) ON CONFLICT(admin_id) DO UPDATE SET
               course_chats=excluded.course_chats,updated_at=excluded.updated_at""",
            (admin_id, 1 if data["enabled"] else 0, now),
        )
        await db.commit()
    finally:
        await db.close()
    return {"ok": True, "admin_id": admin_id, "enabled": data["enabled"]}


@router.put("/notification-routing/{source_admin_id}")
async def save_notification_routing(source_admin_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-notification-routing", limit=240, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    reset = bool(data.get("reset"))
    values = data.get("recipient_admin_ids", [])
    if not isinstance(values, list) or len(values) > 200:
        raise HTTPException(400, "Некорректные получатели")
    try:
        recipients = sorted({int(value) for value in values})
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, "Некорректные получатели") from exc
    now = _iso()
    db = await _connect()
    try:
        valid = await (await db.execute(
            "SELECT id FROM admins WHERE enabled=1 AND (id=? OR id IN (SELECT value FROM json_each(?)))",
            (source_admin_id, json.dumps(recipients)),
        )).fetchall()
        valid_ids = {int(row["id"]) for row in valid}
        if source_admin_id not in valid_ids or (set(recipients) - valid_ids):
            raise HTTPException(404, "Сотрудник не найден")
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("DELETE FROM notification_routes WHERE source_admin_id=?", (source_admin_id,))
        await db.execute("DELETE FROM notification_route_policies WHERE source_admin_id=?", (source_admin_id,))
        if not reset:
            await db.execute(
                "INSERT INTO notification_route_policies(source_admin_id,configured,updated_at) VALUES(?,1,?)",
                (source_admin_id, now),
            )
            await db.executemany(
                "INSERT INTO notification_routes(source_admin_id,recipient_admin_id,created_at,updated_at) VALUES(?,?,?,?)",
                ((source_admin_id, recipient_id, now, now) for recipient_id in recipients),
            )
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()
    return {"ok": True, "source_admin_id": source_admin_id, "configured": not reset,
            "recipient_admin_ids": recipients if not reset else [source_admin_id]}


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


async def _template_owner_admin(admin_id: int) -> dict[str, Any]:
    """Return the exact employee whose private templates are being managed."""
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT id,name,enabled FROM admins WHERE id=?", (admin_id,)
        )).fetchone()
    finally:
        await db.close()
    if not row:
        raise HTTPException(404, "Сотрудник не найден")
    return dict(row)


@router.get("/admins/{admin_id}/templates")
async def list_admin_templates(admin_id: int, request: Request) -> dict[str, Any]:
    """Admin-only view of one employee's private templates and shared favourites."""
    await _require_admin(request)
    admin = await _template_owner_admin(admin_id)
    return {
        "ok": True,
        "admin": admin,
        "templates": await _template_rows(admin_id, include_disabled=True, can_edit_shared=False),
        "variables": TEMPLATE_VARIABLES,
    }


@router.post("/admins/{admin_id}/templates")
async def create_admin_template(admin_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-admin-template", limit=240, window_seconds=3600, subject=user["username"])
    await _template_owner_admin(admin_id)
    return {"ok": True, "template": await _save_template(await _read_json(request), admin_id)}


@router.patch("/admins/{admin_id}/templates/{template_id}")
async def update_admin_template(admin_id: int, template_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-admin-template", limit=240, window_seconds=3600, subject=user["username"])
    await _template_owner_admin(admin_id)
    return {"ok": True, "template": await _save_template(await _read_json(request), admin_id, template_id)}


@router.delete("/admins/{admin_id}/templates/{template_id}")
async def delete_admin_template(admin_id: int, template_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-admin-template", limit=240, window_seconds=3600, subject=user["username"])
    await _template_owner_admin(admin_id)
    await _delete_template(template_id, admin_id)
    return {"ok": True}


@router.put("/admins/{admin_id}/templates/{template_id}/favorite")
async def set_admin_template_favorite(admin_id: int, template_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-admin-template", limit=240, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    if not isinstance(data.get("favorite"), bool):
        raise HTTPException(400, "Некорректное избранное")
    await _template_owner_admin(admin_id)
    favorite = await _set_template_favorite(admin_id, template_id, data["favorite"])
    return {"ok": True, "id": template_id, "favorite": favorite}


@router.put("/admins/{admin_id}/template-order")
async def set_admin_template_order(admin_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-admin-template-order", limit=240, window_seconds=3600, subject=user["username"])
    await _template_owner_admin(admin_id)
    data = await _read_json(request)
    return {"ok": True, "template_ids": await _set_template_order(admin_id, data.get("template_ids"))}


@router.patch("/admins/{admin_id}")
async def update_admin(admin_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _read_json(request)
    if not {"enabled", "role", "amo_task_enabled", "amo_task_sources"}.intersection(data):
        raise HTTPException(400, "Нет изменений")
    role = _clean(data.get("role"), 20).lower()
    if "role" in data and role not in {"admin", "employee"}:
        raise HTTPException(400, "Неизвестная роль")
    if "amo_task_enabled" in data and not isinstance(data["amo_task_enabled"], bool):
        raise HTTPException(400, "Настройка задач amoCRM должна быть включена или выключена")
    amo_task_sources: list[str] | None = None
    if "amo_task_sources" in data:
        raw_sources = data["amo_task_sources"]
        if not isinstance(raw_sources, list) or any(not isinstance(source, str) for source in raw_sources):
            raise HTTPException(400, "Каналы задач amoCRM должны быть списком")
        unknown = sorted(set(raw_sources).difference(AMO_TASK_SOURCES))
        if unknown:
            raise HTTPException(400, "Неизвестные каналы задач amoCRM: " + ", ".join(unknown))
        amo_task_sources = [source for source in AMO_TASK_SOURCES if source in set(raw_sources)]
        if data.get("amo_task_enabled", True) and not amo_task_sources:
            raise HTTPException(400, "Выберите хотя бы один канал или выключите постановку задач")
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
        if "amo_task_enabled" in data:
            await db.execute(
                """INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                (
                    _admin_amo_task_setting_key(admin_id),
                    "1" if data["amo_task_enabled"] else "0",
                    _iso(),
                ),
            )
        if amo_task_sources is not None:
            await db.execute(
                """INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                (
                    _admin_amo_task_sources_setting_key(admin_id),
                    json.dumps(amo_task_sources, ensure_ascii=False),
                    _iso(),
                ),
            )
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


def _communication_provider_filter(provider: str) -> tuple[str, list[str]]:
    """Translate panel labels into the durable provider/transport storage shape."""

    value = _clean(provider, 40).lower()
    if value == "max":
        return "c.provider=? AND c.chat_type IN (?,?)", ["wazzup", "max", "maxgroup"]
    return "c.provider=?", [value]


@router.get("/communications")
async def list_communications(
    request: Request, limit: int = Query(100, ge=1, le=200), before_id: int = Query(0, ge=0),
    days: int = Query(30, ge=1, le=730), provider: str = "", direction: str = "",
    admin_id: int = Query(0, ge=0), query: str = "",
) -> dict[str, Any]:
    await _require_admin(request)
    clauses = ["c.sent_at>=?"]
    values: list[Any] = [_iso(_now_dt() - timedelta(days=days))]
    if before_id:
        clauses.append("c.id<?")
        values.append(before_id)
    if provider := _clean(provider, 40):
        clause, provider_values = _communication_provider_filter(provider)
        clauses.append(clause)
        values.extend(provider_values)
    if direction in {"incoming", "outgoing"}:
        clauses.append("c.direction=?")
        values.append(direction)
    if admin_id:
        clauses.append("c.admin_id=?")
        values.append(admin_id)
    if query := _clean(query, 200):
        clauses.append("(c.client_name LIKE ? OR c.text LIKE ? OR c.amo_lead_id LIKE ?)")
        pattern = f"%{query}%"
        values.extend((pattern, pattern, pattern))
    values.append(limit + 1)
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"""SELECT c.*,a.name AS admin_name FROM communication_messages c
                  LEFT JOIN admins a ON a.id=c.admin_id WHERE {' AND '.join(clauses)}
                  ORDER BY c.id DESC LIMIT ?""", values,
        )).fetchall()
    finally:
        await db.close()
    items = [dict(row) for row in rows[:limit]]
    return {
        "ok": True, "communications": items,
        "next_before_id": int(items[-1]["id"]) if len(rows) > limit and items else 0,
    }


@router.get("/communication-metrics")
async def communication_metrics(request: Request, days: int = Query(7, ge=1, le=90)) -> dict[str, Any]:
    await _require_admin(request)
    since = _iso(_now_dt() - timedelta(days=days))
    db = await _connect()
    try:
        communications = await (await db.execute(
            """SELECT provider,direction,status,COUNT(*) AS count FROM communication_messages
               WHERE created_at>=? GROUP BY provider,direction,status ORDER BY provider,direction,status""",
            (since,),
        )).fetchall()
        outbound = await (await db.execute(
            """SELECT provider,status,COUNT(*) AS count,
                      COALESCE(SUM(CASE WHEN attempts>1 THEN attempts-1 ELSE 0 END),0) AS retries,
                      ROUND(AVG(CASE WHEN status='sent' THEN latency_ms END)) AS avg_latency_ms
               FROM outbound_jobs WHERE created_at>=? GROUP BY provider,status ORDER BY provider,status""",
            (since,),
        )).fetchall()
        amo_tasks = await (await db.execute(
            """SELECT status,COUNT(*) AS count,
                      COALESCE(SUM(CASE WHEN attempts>1 THEN attempts-1 ELSE 0 END),0) AS retries
               FROM amo_task_jobs WHERE created_at>=? GROUP BY status ORDER BY status""", (since,),
        )).fetchall()
        queue = await (await db.execute(
            """SELECT
                 (SELECT COUNT(*) FROM outbound_jobs WHERE status IN ('pending','processing','retry')) AS outbound,
                 (SELECT COUNT(*) FROM amo_task_jobs WHERE status IN ('pending','processing','retry')) AS amo_tasks,
                 (SELECT COUNT(*) FROM notification_events WHERE status IN ('pending','processing','retry')) AS notifications,
                 (SELECT MIN(created_at) FROM outbound_jobs WHERE status IN ('pending','processing','retry')) AS outbound_oldest_at,
                 (SELECT MIN(created_at) FROM amo_task_jobs WHERE status IN ('pending','processing','retry')) AS amo_tasks_oldest_at,
                 (SELECT MIN(created_at) FROM notification_events WHERE status IN ('pending','processing','retry')) AS notifications_oldest_at""",
        )).fetchone()
        latencies = await (await db.execute(
            "SELECT latency_ms FROM outbound_jobs WHERE created_at>=? AND status='sent' AND latency_ms IS NOT NULL ORDER BY latency_ms LIMIT 10000",
            (since,),
        )).fetchall()
    finally:
        await db.close()
    samples = [int(row["latency_ms"]) for row in latencies]
    p50 = samples[min(len(samples) - 1, max(0, int(len(samples) * .50)))] if samples else None
    p95 = samples[min(len(samples) - 1, max(0, int(len(samples) * .95)))] if samples else None
    queue_data = dict(queue) if queue else {}
    for key in ("outbound", "amo_tasks", "notifications"):
        stamp = _parse_iso(queue_data.get(f"{key}_oldest_at"))
        queue_data[f"{key}_oldest_age_seconds"] = (
            max(0, int((_now_dt() - stamp).total_seconds())) if stamp else 0
        )
    return {
        "ok": True, "days": days, "generated_at": _iso(),
        "communications": [dict(row) for row in communications],
        "outbound": [dict(row) for row in outbound], "amo_tasks": [dict(row) for row in amo_tasks],
        "queue": queue_data, "p50_latency_ms": p50, "p95_latency_ms": p95,
    }


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


@router.get("/template-settings")
async def get_template_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    domains, tail = await asyncio.gather(_setting("auto_markup_domains"), _setting("auto_markup_tail"))
    return {"ok": True, "auto_markup_domains": domains, "auto_markup_tail": tail}


@router.put("/template-settings")
async def put_template_settings(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "messenger-widget-template-settings", limit=60, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    domains = _clean(data.get("auto_markup_domains"), 2000)
    tail = _auto_markup_tail(data.get("auto_markup_tail"))
    if domains and not _auto_markup_domains(domains):
        raise HTTPException(400, "Укажите домены через точку с запятой")
    await asyncio.gather(_set_setting("auto_markup_domains", domains), _set_setting("auto_markup_tail", tail))
    return {"ok": True, "auto_markup_domains": domains, "auto_markup_tail": tail}


@router.get("/identity/status")
async def identity_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "index": dict(_identity_index_status) if _identity_index is not None else {"status": "unavailable"}}


@router.post("/identity/rebuild")
async def identity_rebuild(request: Request) -> dict[str, Any]:
    global _identity_index_status
    await _require_admin(request)
    if _identity_index is None:
        raise HTTPException(503, "Индекс недоступен")
    result = await asyncio.to_thread(_identity_index.build_if_changed, force=True)
    _identity_index_status = dict(result)
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


@router.post("/widget/logout")
async def widget_logout(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": True})
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        db = await _connect()
        try:
            await db.execute(
                "UPDATE devices SET revoked_at=? WHERE id=? AND revoked_at=''",
                (_iso(), int(device["id"])),
            )
            await db.commit()
        finally:
            await db.close()
        await _audit("logout", "ok", admin_id=device["admin_id"], device_id=device["id"])
        return _widget_response(request, {"ok": True})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger widget logout failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось выйти из виджета"}, 500)


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


@router.post("/widget/profile-links")
async def widget_profile_links(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(request, "messenger-widget-profile-links", limit=240, window_seconds=3600, subject=str(device["id"]))
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        links, pending = await _cached_widget_profile_links(data, mode, device)
        response_links = list(links)
        if pending and not any(
            row.get("kind") == TELEGRAM_PROVIDER and row.get("verification") == "pending"
            for row in response_links
        ):
            # Keeps already-open iframe versions polling too; current clients
            # additionally use the explicit top-level ``pending`` flag.
            response_links.append({
                "kind": TELEGRAM_PROVIDER, "label": "", "url": "", "verification": "pending",
            })
        return _widget_response(request, {"ok": True, "links": response_links, "pending": pending})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger profile link resolution failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось найти профили"}, 500)


@router.post("/widget/getcourse-card")
async def widget_getcourse_card(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enforce_rate_limit(request, "messenger-widget-getcourse-card", limit=180, window_seconds=3600, subject=str(device["id"]))
        return _widget_response(
            request,
            await _widget_getcourse_card_data(
                data, mode, device, include_access=False, summary_only=True,
            ),
        )
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "Messenger GetCourse card failed")
        return _widget_response(
            request,
            {"ok": False, "error": "GetCourse временно обновляет данные. Повторите через несколько секунд."},
            503,
        )


@router.post("/widget/getcourse-lessons")
async def widget_getcourse_lessons(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enforce_rate_limit(request, "messenger-widget-getcourse-lessons", limit=240, window_seconds=3600, subject=str(device["id"]))
        card = await _widget_getcourse_card_data(
            data, mode, device, include_access=False, summary_only=True,
        )
        enrollment_id = _clean(data.get("enrollment_id"), 100)
        if not card.get("found") or enrollment_id != _clean((card.get("item") or {}).get("enrollment_id"), 100):
            raise HTTPException(404, "Данные ученика не найдены")
        service = _module_service("student-transfer", "service_widget_lessons")
        return _widget_response(request, await service(enrollment_id=enrollment_id))
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "Messenger GetCourse lessons failed")
        return _widget_response(
            request,
            {"ok": False, "error": "Данные обучения временно обновляются. Повторите через несколько секунд."},
            503,
        )


@router.post("/widget/getcourse-access")
async def widget_getcourse_access(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enrollment_id = _clean(data.get("enrollment_id"), 100)
        if not enrollment_id:
            raise HTTPException(404, "Доступы ученика не найдены")
        action = _clean(data.get("action"), 30).lower() or "read"
        requester = f"messenger:{int(device['admin_id'])}"
        if action == "read":
            service = _module_service("student-transfer", "service_widget_access")
            return _widget_response(request, {"ok": True, "access": await service(enrollment_id=enrollment_id, live=bool(data.get("live")))})
        if action == "preview":
            changes = data.get("changes")
            if not isinstance(changes, list) or not 1 <= len(changes) <= 100:
                raise HTTPException(400, "Выберите изменения доступов")
            normalized = []
            for row in changes:
                if not isinstance(row, dict) or not _clean(row.get("group_id"), 100) or not isinstance(row.get("enabled"), bool):
                    raise HTTPException(400, "Некорректное изменение доступа")
                normalized.append({"group_id": _clean(row["group_id"], 100), "enabled": row["enabled"]})
            service = _module_service("student-transfer", "service_widget_access_preview")
            return _widget_response(request, await service(enrollment_id=enrollment_id, changes=normalized, requester_user_id=requester))
        if action == "apply":
            request_id = _clean(data.get("request_id"), 200)
            if not request_id:
                raise HTTPException(400, "Проверка изменений не найдена")
            service = _module_service("student-transfer", "service_widget_access_apply")
            result = await service(
                enrollment_id=enrollment_id, request_id=request_id,
                requester_user_id=requester,
            )
            _schedule_widget_operation(
                action="widget_getcourse_access", device=device,
                context=_widget_context(data, mode, device), enrollment_id=enrollment_id,
                details={"request_id": request_id, "changes": data.get("changes") or []},
            )
            return _widget_response(request, {**result, "operation_queued": True})
        raise HTTPException(400, "Неизвестное действие")
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "Messenger GetCourse access failed")
        return _widget_response(
            request,
            {"ok": False, "error": "Не удалось принять команду. Nexus занят обновлением данных — повторите ещё раз."},
            503,
        )


@router.post("/widget/getcourse-test-period")
async def widget_getcourse_test_period(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enforce_rate_limit(
            request, "messenger-widget-getcourse-test-period", limit=30,
            window_seconds=3600, subject=str(device["id"]),
        )
        enrollment_id = _clean(data.get("enrollment_id"), 100)
        if not enrollment_id:
            raise HTTPException(404, "Пользователь GetCourse не найден")
        action = _clean(data.get("action"), 20).lower() or "status"
        days = int(data.get("days") or 1)
        courses = data.get("courses") if isinstance(data.get("courses"), list) else []
        if action in {"create", "repeat"} and (not 1 <= days <= 90 or not 1 <= len(courses) <= 2):
            raise HTTPException(400, "Укажите 1–90 дней и хотя бы один курс")
        if action not in {"status", "create", "repeat", "revoke"}:
            raise HTTPException(400, "Неизвестное действие")
        service = _module_service("student-transfer", "service_widget_test_period")
        result = await service(
            enrollment_id=enrollment_id, action=action, days=days, courses=courses,
            requester_user_id=f"messenger:{int(device['admin_id'])}",
        )
        response = {"ok": True, "test_period": result}
        if action in {"create", "repeat", "revoke"}:
            _schedule_widget_operation(
                action="widget_trial_issue" if action in {"create", "repeat"} else "widget_trial_revoke",
                device=device, context=_widget_context(data, mode, device),
                enrollment_id=enrollment_id,
                details={
                    "period_id": _clean(result.get("id"), 64),
                    "days": days if action == "create" else 0,
                    "courses": courses if action == "create" else result.get("courses") or [],
                    "expires_at": _clean(result.get("expires_at"), 60),
                },
            )
            response["operation_queued"] = True
        return _widget_response(request, response)
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "Messenger GetCourse test period failed")
        return _widget_response(
            request,
            {"ok": False, "error": "Не удалось принять команду. Nexus занят обновлением данных — повторите ещё раз."},
            503,
        )


@router.post("/widget/operations")
async def widget_operations(request: Request) -> JSONResponse:
    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(
                request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401,
            )
        data = await _read_json(request)
        _validate_device_context(device, data, mode)
        enforce_rate_limit(
            request, "messenger-widget-operations", limit=180,
            window_seconds=3600, subject=str(device["id"]),
        )
        admin_only = _clean(device.get("admin_role"), 20) != "admin"
        db = await _connect()
        try:
            event_params: list[Any] = [*WIDGET_OPERATION_ACTIONS]
            event_admin = ""
            if admin_only:
                event_admin = " AND e.admin_id=?"
                event_params.append(int(device["admin_id"]))
            event_params.append(100)
            placeholders = ",".join("?" for _ in WIDGET_OPERATION_ACTIONS)
            events = await (await db.execute(
                f"""SELECT e.*,a.name AS admin_name FROM events e
                    LEFT JOIN admins a ON a.id=e.admin_id
                    WHERE e.action IN ({placeholders}){event_admin}
                    ORDER BY e.id DESC LIMIT ?""",
                event_params,
            )).fetchall()
            message_params: list[Any] = []
            message_admin = ""
            if admin_only:
                message_admin = " AND c.admin_id=?"
                message_params.append(int(device["admin_id"]))
            message_params.append(100)
            messages = await (await db.execute(
                f"""SELECT c.*,a.name AS admin_name FROM communication_messages c
                    LEFT JOIN admins a ON a.id=c.admin_id
                    WHERE c.direction='outgoing' AND c.dedupe_key NOT LIKE 'outbound:%'{message_admin}
                    ORDER BY c.id DESC LIMIT ?""",
                message_params,
            )).fetchall()
            job_params: list[Any] = []
            job_admin = ""
            if admin_only:
                job_admin = " WHERE j.admin_id=?"
                job_params.append(int(device["admin_id"]))
            job_params.append(100)
            jobs = await (await db.execute(
                f"""SELECT j.*,a.name AS admin_name FROM outbound_jobs j
                    LEFT JOIN admins a ON a.id=j.admin_id{job_admin}
                    ORDER BY j.id DESC LIMIT ?""",
                job_params,
            )).fetchall()
        finally:
            await db.close()
        items: list[dict[str, Any]] = []
        for row in events:
            details = _operation_payload(row["error"])
            event_error = details.get("last_error") or details.get("error") or (
                details.get("result") if row["status"] == "failed" else ""
            )
            items.append({
                "id": f"operation:{row['id']}", "kind": "operation",
                "action": row["action"], "title": _operation_title(row["action"]),
                "status": row["status"], "created_at": row["created_at"],
                "admin_name": row["admin_name"] or details.get("manager_name") or "—",
                "client_name": details.get("client_name") or details.get("email") or "—",
                "result": details.get("result") or (
                    "Выполняется в GetCourse" if row["status"] == "pending" else ""
                ),
                "expires_at": details.get("expires_at") or "",
                "note_status": details.get("note_status") or "",
                "entity_url": details.get("entity_url") or "",
                "error": _friendly_operation_error(event_error) if row["status"] == "failed" else "",
            })
        for row in messages:
            items.append({
                "id": f"message:{row['id']}", "kind": "message",
                "action": "message", "title": "Сообщение · " + _clean(row["provider"], 40),
                "status": row["status"], "created_at": row["sent_at"],
                "admin_name": row["admin_name"] or row["manager_name"] or "—",
                "client_name": row["client_name"] or "—", "result": _clean(row["text"], 500),
                "expires_at": "", "note_status": "", "entity_url": row["entity_url"] or "",
                "error": _friendly_operation_error(row["error"], row["provider"])
                if row["status"] in {"failed", "dead"} else "",
            })
        for row in jobs:
            status = _clean(row["status"], 40)
            active = status in {"pending", "processing", "retry"}
            if status == "retry":
                result = "Канал временно не ответил. Nexus повторит доставку автоматически."
            elif status == "processing":
                result = "Отправляем сообщение в канал…"
            elif status == "pending":
                result = "Принято в очередь. Отправка начнётся автоматически."
            else:
                result = _clean(row["text"], 500)
            if active and row["text"]:
                result += "\n\n" + _clean(row["text"], 500)
            items.append({
                "id": f"outbound:{row['id']}", "kind": "message", "action": "message",
                "title": "Сообщение · " + _clean(row["provider"], 40),
                "status": status, "created_at": row["created_at"],
                "admin_name": row["admin_name"] or "—", "client_name": row["client_name"] or "—",
                "result": result, "expires_at": "", "note_status": "",
                "entity_url": row["entity_url"] or "",
                "attempts": int(row["attempts"] or 0),
                "next_attempt_at": row["next_attempt_at"] if active else "",
                "error": _friendly_operation_error(row["error"], row["provider"])
                if status in {"failed", "dead"} else "",
            })
        items.sort(key=lambda item: (_clean(item.get("created_at"), 60), item["id"]), reverse=True)
        return _widget_response(request, {"ok": True, "items": items[:100]})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "Messenger operations journal failed")
        return _widget_response(
            request, {"ok": False, "error": "Не удалось загрузить операции. Повторите через несколько секунд."}, 503,
        )


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
        if action == "list":
            return _widget_response(request, {
                "ok": True,
                "can_manage_shared": _clean(device.get("admin_role"), 20) == "admin",
                "templates": await _template_rows(admin_id, can_edit_shared=_clean(device.get("admin_role"), 20) == "admin"),
                "variables": TEMPLATE_VARIABLES,
            })
        if action == "favorite":
            template_id = int(data.get("id") or 0)
            if not template_id or not isinstance(data.get("favorite"), bool):
                raise HTTPException(400, "Некорректное избранное")
            favorite = await _set_template_favorite(admin_id, template_id, data["favorite"])
            return _widget_response(request, {"ok": True, "id": template_id, "favorite": favorite})
        if action == "reorder":
            ordered = await _set_template_order(admin_id, data.get("template_ids"))
            return _widget_response(request, {"ok": True, "template_ids": ordered})
        shared = _clean(data.get("scope"), 20).lower() == "shared"
        if shared and _clean(device.get("admin_role"), 20) != "admin":
            raise HTTPException(403, "Общие шаблоны может менять администратор")
        owner_id = None if shared else admin_id
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


def _widget_image_type(content: bytes) -> tuple[str, str]:
    """Return a safe extension and MIME type from image bytes, never a filename."""

    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return ".jpg", "image/jpeg"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return ".gif", "image/gif"
    if len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP":
        return ".webp", "image/webp"
    raise HTTPException(415, "Можно прикрепить только JPG, PNG, GIF или WebP")


async def _widget_media_path(url: str) -> tuple[Path, str]:
    prefix = PUBLIC_API_BASE + "/widget/media/"
    if not _clean(url, 4000).startswith(prefix):
        raise ValueError("Используйте изображение, загруженное через Nexus")
    suffix = url[len(prefix):].split("?", 1)[0]
    token, separator, filename = suffix.partition("/")
    token, filename = unquote(token), unquote(filename)
    if not separator or not token or not filename:
        raise ValueError("Изображение Nexus не найдено")
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT stored_name,mime_type FROM widget_media WHERE token=?", (token,),
        )).fetchone()
    finally:
        await db.close()
    if not row or filename != row["stored_name"]:
        raise ValueError("Изображение Nexus не найдено")
    path = _must_db().parent / "widget-media" / row["stored_name"]
    if not path.is_file():
        raise ValueError("Изображение Nexus не найдено")
    return path, _clean(row["mime_type"], 100)


async def _vk_upload_widget_image(peer_id: str, attachment_url: str) -> str:
    path, mime_type = await _widget_media_path(attachment_url)
    content = await asyncio.to_thread(path.read_bytes)
    last_error: Exception | None = None
    # VK upload hosts occasionally answer with a transient 5xx or a JSON body
    # without one of the save fields. Refreshing the upload URL once is much
    # faster than spending a full durable-queue attempt on that host.
    for upload_attempt in range(2):
        try:
            upload = await _vk_request("photos.getMessagesUploadServer", {
                "peer_id": peer_id, "group_id": _vk_group_id(),
            })
            upload_url = _clean((upload or {}).get("upload_url") if isinstance(upload, dict) else "", 4000)
            if not upload_url:
                raise HTTPException(502, "VK временно не подготовил загрузку изображения")
            async with httpx.AsyncClient(timeout=45, trust_env=False) as client:
                response = await client.post(upload_url, files={"photo": (path.name, content, mime_type)})
                response.raise_for_status()
                uploaded = response.json()
            server = uploaded.get("server") if isinstance(uploaded, dict) else None
            photo_payload = uploaded.get("photo") if isinstance(uploaded, dict) else None
            upload_hash = uploaded.get("hash") if isinstance(uploaded, dict) else None
            if (
                server in {None, ""}
                or not isinstance(photo_payload, str)
                or photo_payload.strip() in {"", "[]"}
                or not isinstance(upload_hash, str)
                or not upload_hash.strip()
            ):
                raise HTTPException(502, "VK временно не принял изображение")
            saved = await _vk_request("photos.saveMessagesPhoto", {
                "server": server, "photo": photo_payload, "hash": upload_hash,
            })
            if not isinstance(saved, list) or not saved:
                raise HTTPException(502, "VK временно не сохранил изображение")
            photo = saved[0]
            if not isinstance(photo, dict) or photo.get("owner_id") is None or photo.get("id") is None:
                raise HTTPException(502, "VK вернул неполные данные изображения")
            result = f"photo{photo['owner_id']}_{photo['id']}"
            if photo.get("access_key"):
                result += f"_{photo['access_key']}"
            return result
        except Exception as exc:
            last_error = exc
            if upload_attempt == 0 and _delivery_error_is_transient(exc):
                await asyncio.sleep(0.2)
                continue
            raise
    assert last_error is not None
    raise last_error


@router.post("/widget/image-upload")
async def widget_image_upload(request: Request) -> JSONResponse:
    """Store a small public image for asynchronous messenger delivery."""

    mode = await _widget_request_mode(request)
    if not mode:
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(
                request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401,
            )
        enforce_rate_limit(
            request, "messenger-widget-image-upload", limit=60,
            window_seconds=3600, subject=str(device["id"]),
        )
        content_length = request.headers.get("content-length", "")
        if content_length:
            try:
                if int(content_length) > MAX_WIDGET_IMAGE_BYTES:
                    raise HTTPException(413, "Изображение должно быть не больше 8 МБ")
            except ValueError:
                pass
        content = await request.body()
        if not content:
            raise HTTPException(400, "Выберите изображение")
        if len(content) > MAX_WIDGET_IMAGE_BYTES:
            raise HTTPException(413, "Изображение должно быть не больше 8 МБ")
        extension, mime_type = _widget_image_type(content)
        token = secrets.token_urlsafe(32)
        stored_name = secrets.token_hex(24) + extension
        media_dir = _must_db().parent / "widget-media"
        media_dir.mkdir(parents=True, exist_ok=True)
        target = media_dir / stored_name
        await asyncio.to_thread(target.write_bytes, content)
        try:
            db = await _connect()
            try:
                await db.execute(
                    "INSERT INTO widget_media(token,admin_id,stored_name,mime_type,size,created_at) VALUES(?,?,?,?,?,?)",
                    (token, int(device["admin_id"]), stored_name, mime_type, len(content), _iso()),
                )
                await db.commit()
            finally:
                await db.close()
        except Exception:
            target.unlink(missing_ok=True)
            raise
        url = f"{PUBLIC_API_BASE}/widget/media/{quote(token, safe='')}/{quote(stored_name, safe='')}"
        return _widget_response(request, {
            "ok": True, "url": url, "attachment_type": "image",
            "mime_type": mime_type, "size": len(content),
        })
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except ClientDisconnect:
        return _widget_response(request, {"ok": False, "error": "Загрузка изображения прервана"}, 499)
    except Exception:
        _log("exception", "Messenger image upload failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось загрузить изображение"}, 500)


@router.get("/widget/media/{token}/{filename}")
async def widget_media(token: str, filename: str) -> Response:
    clean_token = _clean(token, 100)
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT stored_name,mime_type FROM widget_media WHERE token=?", (clean_token,),
        )).fetchone()
    finally:
        await db.close()
    if not row or filename != row["stored_name"]:
        raise HTTPException(404, "Изображение не найдено")
    path = _must_db().parent / "widget-media" / row["stored_name"]
    if not path.is_file():
        raise HTTPException(404, "Изображение не найдено")
    return FileResponse(
        str(path), media_type=row["mime_type"],
        headers={"Cache-Control": "public, max-age=2592000, immutable", "X-Content-Type-Options": "nosniff"},
    )


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
        variables = resolved.get("variables", {})
        rendered = render_message_template(body, variables)
        rendered["text"] = await _auto_markup_for_send(rendered["text"], variables)
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
    channel_db: aiosqlite.Connection | None = None
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
        card_context = _widget_context(data, mode, device)
        if _telegram_dialog_hidden({
            **card_context,
            "phone": phone or card_context.get("phone"),
            "telegram_id": _identity_field_value(card_context, "telegram_id", "tg_id"),
            "telegram_username": _identity_field_value(
                card_context, "telegram_username", "tg_username",
            ),
        }):
            channels = [
                channel for channel in channels
                if channel.get("provider") != TELEGRAM_PROVIDER
            ]
        # A single card previously fanned its short indexed lookups out over a
        # separate aiosqlite thread/connection per direct channel.  Reuse one
        # card-local connection instead: employees still run concurrently,
        # while each request keeps only one WAL reader alive.
        channel_db = await _connect()
        try:
            conversation_presence = await asyncio.wait_for(
                _conversation_presence(channels, phone, db=channel_db), timeout=0.35,
            )
        except (TimeoutError, RuntimeError, aiosqlite.Error):
            conversation_presence = set()
        async def resolve_channel(channel: dict[str, Any]) -> tuple[dict[str, Any], tuple[str, str] | None]:
            provider = channel.get("provider", "wazzup")
            peer_id = ""
            has_chat = False
            can_send = False
            reason = ""
            link: dict[str, Any] = {}
            direct_link: tuple[str, str] | None = None
            direct_label = ""
            verification_pending = False
            if provider == EMAIL_PROVIDER:
                try:
                    email_service = _module_service("email-channel", "service_conversation")
                    email_result = await email_service(context=card_context, offset=0, limit=1)
                    email_channel = email_result.get("channel") if isinstance(email_result.get("channel"), dict) else {}
                    return ({
                        **channel, **email_channel,
                        "available": bool(email_channel.get("available")),
                        "can_send": bool(email_result.get("can_send")),
                        "has_chat": bool(email_result.get("has_chat")),
                        "confirmed_chat": bool(email_result.get("confirmed_chat")),
                        "chat_id": _clean(email_result.get("thread_id"), 250),
                        "send_reason": _clean(email_result.get("send_reason"), 500),
                        "requires_subject": bool(email_result.get("requires_subject")),
                        "email_guidelines_required": email_result.get("email_guidelines_required") is not False,
                    }, None)
                except Exception:
                    return ({
                        **channel, "available": False, "can_send": False,
                        "has_chat": False, "send_reason": "Email-канал временно недоступен",
                    }, None)
            if provider == "wazzup":
                has_chat = (channel["channel_id"], channel["transport"]) in conversation_presence
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
                    db=channel_db,
                ) if same_provider else {}
                context = card_context
                exact_amo_provider = (
                    context["platform"] == "amocrm"
                    and provider in {"vk", SALEBOT_PROVIDER}
                )
                if not link and exact_amo_provider:
                    # Exact Customer DB/provider discovery is already running
                    # in the shared profile-enrichment task.  Never repeat it
                    # synchronously here: the stored entity link is sufficient
                    # proof, otherwise the channel stays pending until that
                    # one background lookup finishes.
                    link = await _entity_external_link(
                        context["platform"], context["entity_type"], context["entity_id"], provider,
                        db=channel_db,
                    )
                    verification_pending = (
                        not link
                        and _widget_profile_kind_state(data, mode, device, provider) == "pending"
                    )
                if not link and not exact_amo_provider:
                    exact_link = await _entity_external_link(
                        context["platform"], context["entity_type"], context["entity_id"], provider,
                        db=channel_db,
                    )
                    if exact_link and _card_link_matches_context(
                        exact_link, context, gc_id,
                    ):
                        link = exact_link
                if (
                    not link
                    and provider == TELEGRAM_PROVIDER
                    and context["platform"] == "amocrm"
                ):
                    link = await _successful_card_delivery_link(context, provider, db=channel_db)
                if not link and (thread or context["platform"] != "amocrm"):
                    link = await _external_link_for_identity(
                        provider, phone=phone, gc_id=gc_id, db=channel_db,
                    )
                # Initial widget paint must use only identities already proven
                # for this card or an explicit provider field.  A live Customer
                # DB/Telegram lookup can take tens of seconds and is handled by
                # the independent profile search instead of blocking channels.
                if not link:
                    explicit_id = ""
                    if provider == "vk":
                        explicit_id = _identity_field_value(
                            context, "vk_id", "vkontakte_id", "senler_id",
                        )
                    elif provider == SALEBOT_PROVIDER and context["platform"] != "amocrm":
                        explicit_id = _identity_field_value(
                            context, "salebot_id", "salebot_client_id", "sb_id",
                        )
                    if explicit_id:
                        link = {"external_user_id": explicit_id}
                peer_id = _clean(link.get("external_user_id"), 200)
                provider_profile_name = ""
                if peer_id:
                    has_chat = await _has_exact_conversation(
                        channel["channel_id"], channel["transport"], peer_id,
                        db=channel_db,
                    )
                    direct_link = (provider, peer_id)
                    provider_profile_name = _clean(link.get("name"), 200)
                    if not provider_profile_name:
                        try:
                            provider_profile_name = await asyncio.wait_for(
                                _provider_profile_name(provider, peer_id, db=channel_db), timeout=0.2,
                            )
                        except TimeoutError:
                            provider_profile_name = ""
                if provider == "vk":
                    utm_candidates = parse_utm_term(
                        _identity_field_value(context, "utm_term")
                    ) if context["platform"] != "amocrm" else []
                    can_attempt = bool(utm_candidates)
                    can_send = bool(peer_id or can_attempt or (
                        context["platform"] != "amocrm"
                        and _identity_field_value(
                            context, "vk_id", "vkontakte_id", "senler_id", "platform_id",
                        )
                    ))
                    if peer_id:
                        reason = ""
                    elif can_attempt:
                        direct_label = "VK · найти по utm_term"
                        reason = "Нажмите — Nexus проверит VK из utm_term"
                    else:
                        reason = "VK клиента не найден"
                elif provider == SALEBOT_PROVIDER:
                    explicit_id = ""
                    if context["platform"] != "amocrm":
                        explicit_id = _identity_field_value(
                            context, "salebot_id", "salebot_client_id", "sb_id",
                        )
                        if not explicit_id:
                            explicit_id = next((
                                value for kind, value in parse_utm_term(
                                    _identity_field_value(context, "utm_term")
                                ) if kind == "salebot"
                            ), "")
                    can_send = bool(peer_id or explicit_id)
                    reason = "" if can_send else "SaleBot клиента не найден"
                else:
                    attemptable = bool(phone)
                    state = _card_link_state(
                        _card_link_cache_key(context, device, TELEGRAM_PROVIDER, "")
                    ) if attemptable and not peer_id else "verified"
                    verification_pending = bool(
                        attemptable and not peer_id and state in {"unknown", "pending"}
                    )
                    can_send = bool(peer_id or attemptable)
                    if peer_id:
                        reason = ""
                    elif attemptable:
                        direct_label = (
                            "TG Personal" if verification_pending
                            else "TG Personal · найти по номеру"
                        )
                        reason = "Нажмите — Nexus попробует найти пользователя Telegram"
                    else:
                        reason = "Пользователь Telegram не найден" if phone else "Телефон клиента не найден"
            return ({
                **channel,
                **({"label": direct_label} if direct_label else
                   {"label": f"{('TG Personal' if provider == TELEGRAM_PROVIDER else 'SaleBot' if provider == SALEBOT_PROVIDER else 'VK')}: {provider_profile_name}"}
                   if provider in {"vk", TELEGRAM_PROVIDER, SALEBOT_PROVIDER} and provider_profile_name else {}),
                "available": can_send,
                "can_send": can_send,
                "has_chat": has_chat,
                "send_reason": reason,
                **({"chat_id": peer_id} if peer_id else {}),
                **({"pending": True} if (
                    link.get("pending") or verification_pending
                ) else {}),
            }, direct_link)

        async def resolve_channel_safely(channel: dict[str, Any]) -> tuple[dict[str, Any], tuple[str, str] | None]:
            try:
                return await asyncio.wait_for(resolve_channel(channel), timeout=0.45)
            except TimeoutError:
                provider = channel.get("provider", "wazzup")
                can_send = False
                reason = "Проверка канала продолжается в фоне"
                if provider == "wazzup":
                    can_send, reason = _channel_send_state(channel, False)
                    if not phone:
                        can_send, reason = False, "Телефон не найден"
                elif provider == TELEGRAM_PROVIDER and phone:
                    can_send = True
                    reason = "Нажмите — Nexus попробует найти пользователя Telegram"
                elif provider == "vk" and card_context.get("platform") != "amocrm" and parse_utm_term(
                    _identity_field_value(card_context, "utm_term")
                ):
                    can_send = True
                    reason = "Нажмите — Nexus проверит VK из utm_term"
                return ({
                    **channel, "available": can_send, "can_send": can_send, "has_chat": False,
                    "send_reason": reason, "pending": True,
                }, None)
            except (HTTPException, aiosqlite.Error) as exc:
                _log(
                    "warning", "Widget channel resolution deferred: provider=%s channel=%s error=%s",
                    channel.get("provider", "wazzup"), _clean(channel.get("channel_id"), 200),
                    type(exc).__name__,
                )
                return ({
                    **channel, "available": False, "can_send": False, "has_chat": False,
                    "send_reason": "Канал временно недоступен. Повторяем проверку…", "pending": True,
                }, None)
            except Exception as exc:
                _log(
                    "exception", "Widget channel resolution failed: provider=%s channel=%s error=%s",
                    channel.get("provider", "wazzup"), _clean(channel.get("channel_id"), 200),
                    type(exc).__name__,
                )
                return ({
                    **channel, "available": False, "can_send": False, "has_chat": False,
                    "send_reason": "Канал временно недоступен. Повторяем проверку…", "pending": True,
                }, None)

        resolved_channels = await asyncio.gather(*(resolve_channel_safely(channel) for channel in channels))
        views = _prioritize_channels([view for view, _ in resolved_channels])
        direct_links = [link for _, link in resolved_channels if link]
        if not thread:
            owner_id = await _responsible_admin_id(data, mode, device, db=channel_db)
            await _assign_client_threads(
                owner_id, phone=phone, direct_links=direct_links, db=channel_db,
            )
        return _widget_response(request, {"ok": True, "channels": views})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup channel list failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось получить каналы Wazzup"}, 500)
    finally:
        if channel_db is not None:
            await channel_db.close()


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


def _schedule_wazzup_history(
    device: dict[str, Any],
    channel: dict[str, str],
    phone: str,
    *,
    name: str = "",
    identity: dict[str, str] | None = None,
    offset: int = 0,
    known_chat_id: str = "",
) -> None:
    normalized_phone = _normalize_phone(phone)
    key = (_clean(channel.get("channel_id"), 200), _phone_hash(normalized_phone), max(0, offset))
    if not key[0] or not normalized_phone or key in _wazzup_history_inflight:
        return
    _wazzup_history_inflight.add(key)

    async def run() -> None:
        try:
            await _record_history_sync(channel["channel_id"], normalized_phone, "syncing", 0, success=False)
            await _import_wazzup_history(
                device, channel, normalized_phone, name=name, identity=identity,
                offset=offset, known_chat_id=known_chat_id,
            )
        except Exception:
            await _record_history_sync(channel["channel_id"], normalized_phone, "error", 0, success=False)
            _log("warning", "Wazzup history refresh failed channel=%s", channel["channel_id"])
        finally:
            _wazzup_history_inflight.discard(key)

    asyncio.create_task(run())


async def _conversation_rows(
    channel_id: str,
    transport: str,
    phone: str,
    limit: int = 150,
    *,
    exact_chat_id: str = "",
    offset: int = 0,
) -> tuple[str, bool, list[dict[str, Any]]]:
    if transport == "telegram" and _telegram_dialog_hidden(
        peer_id=exact_chat_id, phone=phone,
    ):
        return "", False, []
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


async def _conversation_presence(
    channels: list[dict[str, Any]], phone: str, *,
    db: aiosqlite.Connection | None = None,
) -> set[tuple[str, str]]:
    """Read conversation availability for every Wazzup channel in one query."""

    phone_hash = _phone_hash(phone)
    digits = phone[1:] if phone else ""
    pairs = {
        (_clean(row.get("channel_id"), 200), _clean(row.get("transport"), 40))
        for row in channels if row.get("provider", "wazzup") == "wazzup"
    }
    pairs.discard(("", ""))
    if not pairs or (not phone_hash and not digits):
        return set()
    owns_db = db is None
    if db is None:
        db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT channel_id,chat_type FROM wazzup_chats
               WHERE phone_hash=? OR chat_id=?""",
            (phone_hash, digits),
        )).fetchall()
    finally:
        if owns_db:
            await db.close()
    return {
        (_clean(row["channel_id"], 200), _clean(row["chat_type"], 40))
        for row in rows
        if (_clean(row["channel_id"], 200), _clean(row["chat_type"], 40)) in pairs
    }


async def _has_exact_conversation(
    channel_id: str, transport: str, chat_id: str, *,
    db: aiosqlite.Connection | None = None,
) -> bool:
    if not channel_id or not transport or not chat_id:
        return False
    if transport == "telegram" and _telegram_dialog_hidden(peer_id=chat_id):
        return False
    owns_db = db is None
    if db is None:
        db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT 1 FROM wazzup_chats
               WHERE channel_id=? AND chat_type=? AND chat_id=? LIMIT 1""",
            (channel_id, transport, chat_id),
        )).fetchone()
        return bool(row)
    finally:
        if owns_db:
            await db.close()


async def _inbox_thread_context(
    channel_id: str,
    chat_type: str,
    chat_id: str,
    channels: list[dict[str, str]],
    device: dict[str, Any],
) -> dict[str, str]:
    if (
        channel_id.startswith("telegram-personal:")
        and _telegram_dialog_hidden(peer_id=chat_id)
    ):
        raise HTTPException(404, "Диалог не найден")
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


def _salebot_attachment_items(row: dict[str, Any]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    raw_attachments = row.get("attachments")
    if isinstance(raw_attachments, dict):
        raw_attachments = list(raw_attachments.values()) if raw_attachments else []
    if not isinstance(raw_attachments, list):
        raw_attachments = []
    for attachment in raw_attachments:
        if isinstance(attachment, str):
            url, content_type, filename = attachment, "", ""
        elif isinstance(attachment, dict):
            url = attachment.get("attachment_url") or attachment.get("url") or attachment.get("link") or attachment.get("src") or attachment.get("file")
            content_type = attachment.get("attachment_type") or attachment.get("type") or attachment.get("mime") or ""
            filename = attachment.get("filename") or attachment.get("name") or ""
        else:
            continue
        items.append({
            "url": _clean(url, 4000),
            "content_type": _clean(content_type, 200).lower(),
            "filename": _clean(filename, 500),
        })
    if items:
        return items
    for key in ("attachment_url", "attachment", "file", "media", "image", "photo", "video", "audio", "voice", "document"):
        value = row.get(key)
        url = value if isinstance(value, str) else value.get("url") if isinstance(value, dict) else ""
        if url or key in {"audio", "voice"} and value:
            return [{
                "url": _clean(url, 4000),
                "content_type": _clean(row.get("attachment_type") or key, 200).lower(),
                "filename": "",
            }]
    return []


def _salebot_attachment_type(content_type: str, url: str) -> str:
    clean_type = _clean(content_type, 200).lower()
    if clean_type:
        return "audio" if clean_type == "voice" else clean_type
    path = urlsplit(url).path.lower()
    if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")):
        return "image"
    if path.endswith((".mp3", ".m4a", ".ogg", ".opus", ".wav")):
        return "audio"
    if path.endswith((".mp4", ".webm", ".mov")):
        return "video"
    return "document"


def _salebot_attachment_token(client_id: str, message_id: str, attachment_index: int) -> str:
    expires = int(time.time()) + SALEBOT_ATTACHMENT_TTL_SECONDS
    payload = json.dumps(
        {"c": _clean(client_id, 200), "m": _clean(message_id, 200), "i": int(attachment_index), "e": expires},
        separators=(",", ":"),
    ).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    signature = hmac.new(_salebot_key().encode(), encoded.encode(), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"


def _salebot_attachment_claims(token: str) -> tuple[str, str, int]:
    try:
        encoded, signature = token.rsplit(".", 1)
        expected = hmac.new(_salebot_key().encode(), encoded.encode(), hashlib.sha256).hexdigest()
        if not _salebot_key() or not hmac.compare_digest(signature, expected):
            raise ValueError("signature")
        padding = "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded + padding))
        if int(payload.get("e") or 0) < int(time.time()):
            raise ValueError("expired")
        client_id = _clean(payload.get("c"), 200)
        message_id = _clean(payload.get("m"), 200)
        index = int(payload.get("i"))
        if not client_id or not message_id or index < 0 or index > 20:
            raise ValueError("claims")
        return client_id, message_id, index
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(404, "Вложение недоступно") from exc


def _salebot_remote_attachment_url(url: str) -> str:
    try:
        parsed = urlsplit(_clean(url, 4000))
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            return ""
        if parsed.port not in (None, 443):
            return ""
        host = parsed.hostname.rstrip(".").lower()
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if host == "localhost" or "." not in host or not re.fullmatch(r"[a-z0-9.-]{1,253}", host):
                return ""
        else:
            if not address.is_global:
                return ""
        return urlunsplit(("https", parsed.netloc, parsed.path or "/", parsed.query, ""))
    except (TypeError, ValueError):
        return ""


async def _require_public_attachment_host(url: str) -> str:
    safe_url = _salebot_remote_attachment_url(url)
    if not safe_url:
        raise HTTPException(404, "Вложение недоступно")
    host = urlsplit(safe_url).hostname or ""
    if host == "api.telegram.org":
        return safe_url
    try:
        addresses = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM),
        )
    except socket.gaierror as exc:
        raise HTTPException(404, "Вложение недоступно") from exc
    resolved = {item[4][0] for item in addresses if item and len(item) > 4 and item[4]}
    if not resolved:
        raise HTTPException(404, "Вложение недоступно")
    try:
        if any(not ipaddress.ip_address(address).is_global for address in resolved):
            raise HTTPException(404, "Вложение недоступно")
    except ValueError as exc:
        raise HTTPException(404, "Вложение недоступно") from exc
    return safe_url


def _salebot_safe_attachment_url(client_id: str, message_id: str, index: int, url: str) -> str:
    if not _salebot_remote_attachment_url(url):
        return ""
    token = _salebot_attachment_token(client_id, message_id, index)
    return f"{PUBLIC_API_BASE}/streams/salebot-attachment/{token}"


def _salebot_messages(payload: Any, client_id: str = "") -> list[dict[str, Any]]:
    rows = payload
    if isinstance(payload, dict):
        rows = payload.get("result") or payload.get("messages") or payload.get("history") or payload.get("data") or []
    if not isinstance(rows, list):
        return []
    result: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        outside_raw = row.get("message_from_outside")
        try:
            outside = int(outside_raw)
        except (TypeError, ValueError):
            continue
        # SaleBot uses the other values for CRM comments, callbacks, telephony
        # and other service events.  The sales dialog needs only ordinary and
        # API messages.
        if outside not in {0, 1}:
            continue
        text = _clean(row.get("text") or row.get("message"), 20_000)
        replica = row.get("client_replica")
        if isinstance(replica, str):
            replica = replica.strip().lower() in {"1", "true", "yes"}
        # message_from_outside separates dialog traffic from CRM/service
        # events. Both the client and the bot use value 0 in real histories;
        # client_replica is the speaker flag used by the amoCRM widget.
        incoming = bool(replica)
        if incoming and row.get("answered") is False and text.casefold() in SALEBOT_CALLBACK_MARKERS:
            continue
        attachments: list[dict[str, Any]] = []
        message_id = _clean(row.get("id") or row.get("message_id") or index, 200)
        for attachment_index, attachment in enumerate(_salebot_attachment_items(row)):
            clean_url = _salebot_safe_attachment_url(
                client_id, message_id, attachment_index, attachment["url"],
            ) if client_id else attachment["url"]
            clean_type = _salebot_attachment_type(attachment["content_type"], attachment["url"])
            if clean_url.startswith("https://") or clean_type in {"audio", "voice"}:
                attachments.append({
                    "content_uri": clean_url if clean_url.startswith("https://") else "",
                    "content_type": clean_type,
                    "filename": attachment["filename"],
                    "unavailable": not clean_url.startswith("https://"),
                })
        result.append({
            "external_id": f"salebot:{message_id}",
            "direction": "incoming" if incoming else "outgoing",
            "status": "delivered" if row.get("delivered", True) else "sent",
            "text": text,
            "content_uri": attachments[0]["content_uri"] if attachments else "",
            "attachments": attachments,
            "author_name": _clean(row.get("name") or row.get("author_name"), 200),
            "sent_at": _message_time(row.get("created_at") or row.get("date") or row.get("time")),
        })
    return sorted(result, key=lambda item: item["sent_at"])


async def _salebot_history(client_id: str) -> list[dict[str, Any]]:
    cached = _salebot_history_cache.get(client_id)
    if cached and cached[0] > time.monotonic():
        return await _apply_manager_attribution(list(cached[1]), SALEBOT_PROVIDER, client_id)
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
    messages = _salebot_messages(payload, client_id)
    _salebot_history_cache[client_id] = (time.monotonic() + SALEBOT_HISTORY_CACHE_SECONDS, messages)
    while len(_salebot_history_cache) > DIRECT_HISTORY_CACHE_LIMIT:
        _salebot_history_cache.pop(next(iter(_salebot_history_cache)))
    return await _apply_manager_attribution(list(messages), SALEBOT_PROVIDER, client_id)


async def _apply_manager_attribution(
    messages: list[dict[str, Any]], provider: str, chat_id: str,
) -> list[dict[str, Any]]:
    """Overlay the Nexus manager on provider histories that only expose a bot/project author."""

    if not messages:
        return messages
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT external_id,text,manager_name,sent_at FROM communication_messages
               WHERE provider=? AND chat_id=? AND direction='outgoing' AND manager_name<>''
               ORDER BY id DESC LIMIT 500""",
            (_clean(provider, 40), _clean(chat_id, 250)),
        )).fetchall()
    finally:
        await db.close()
    candidates = [dict(row) for row in rows]
    used: set[int] = set()
    for message in messages:
        if message.get("direction") != "outgoing":
            continue
        message_time = _parse_iso(message.get("sent_at"))
        best_index = -1
        best_distance = 601.0
        for index, row in enumerate(candidates):
            if index in used or _clean(row.get("text"), 4000) != _clean(message.get("text"), 4000):
                continue
            candidate_time = _parse_iso(row.get("sent_at"))
            distance = abs((message_time - candidate_time).total_seconds()) if message_time and candidate_time else 0
            if distance <= 600 and distance < best_distance:
                best_index, best_distance = index, distance
        if best_index >= 0:
            used.add(best_index)
            message["author_name"] = candidates[best_index]["manager_name"]
    return messages


async def _salebot_raw_attachment(client_id: str, message_id: str, attachment_index: int) -> dict[str, str]:
    key = _salebot_key()
    if not key:
        raise HTTPException(503, "SaleBot не настроен")
    async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
        response = await client.get(
            f"{SALEBOT_API_BASE}/{key}/get_history",
            params={"client_id": client_id, "limit": 2000},
        )
    if response.status_code >= 400:
        raise HTTPException(502, "SaleBot не отдал вложение")
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(502, "SaleBot вернул некорректное вложение") from exc
    rows = (
        payload.get("result") or payload.get("messages") or payload.get("history") or payload.get("data") or []
    ) if isinstance(payload, dict) else []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or _clean(row.get("id") or row.get("message_id"), 200) != message_id:
            continue
        attachments = _salebot_attachment_items(row)
        if attachment_index >= len(attachments):
            break
        attachment = attachments[attachment_index]
        if not _salebot_remote_attachment_url(attachment["url"]):
            break
        return attachment
    raise HTTPException(404, "Вложение недоступно")


@router.get("/streams/salebot-attachment/{token}")
async def streams_salebot_attachment(token: str, request: Request) -> Response:
    # The short-lived HMAC token is the capability credential. Browser media
    # requests from the standalone Streams app do not carry a Nexus admin cookie.
    enforce_rate_limit(request, "messenger-salebot-attachment", limit=120, window_seconds=300)
    client_id, message_id, attachment_index = _salebot_attachment_claims(token)
    attachment = await _salebot_raw_attachment(client_id, message_id, attachment_index)
    attachment_url = await _require_public_attachment_host(attachment["url"])

    def unavailable_image() -> Response:
        svg = '''<svg xmlns="http://www.w3.org/2000/svg" width="720" height="180" viewBox="0 0 720 180"><rect width="720" height="180" fill="#111316"/><rect x="1" y="1" width="718" height="178" fill="none" stroke="#383c43"/><path d="M55 119l42-47 35 38 22-23 49 52H55z" fill="#383c43"/><circle cx="166" cy="55" r="14" fill="#727780"/><text x="235" y="84" fill="#eceef1" font-family="Arial,sans-serif" font-size="20">Вложение больше недоступно</text><text x="235" y="116" fill="#a7abb3" font-family="Arial,sans-serif" font-size="15">Остальные файлы истории продолжают отображаться.</text></svg>'''.encode("utf-8")
        return Response(
            svg,
            media_type="image/svg+xml",
            headers={"Cache-Control": "private, max-age=60", "X-Content-Type-Options": "nosniff"},
        )

    try:
        parsed = urlsplit(attachment_url)
        client_kwargs = httpx_client_kwargs(timeout=httpx.Timeout(30, connect=15)) if parsed.hostname == "api.telegram.org" else {
            "timeout": httpx.Timeout(30, connect=15), "trust_env": False,
        }
        async with httpx.AsyncClient(follow_redirects=False, **client_kwargs) as client:
            response = await client.get(attachment_url)
    except httpx.HTTPError:
        return unavailable_image()
    if response.status_code != 200:
        return unavailable_image()
    try:
        declared_size = int(response.headers.get("content-length") or 0)
    except ValueError:
        declared_size = 0
    if declared_size > SALEBOT_ATTACHMENT_MAX_BYTES:
        raise HTTPException(413, "Вложение слишком большое")
    content = response.content
    if len(content) > SALEBOT_ATTACHMENT_MAX_BYTES:
        raise HTTPException(413, "Вложение слишком большое")
    declared_type = _clean(response.headers.get("content-type"), 200).split(";", 1)[0].lower()
    media_type = declared_type if declared_type in SALEBOT_SAFE_MEDIA_TYPES else "application/octet-stream"
    headers = {"Cache-Control": "private, max-age=300", "X-Content-Type-Options": "nosniff"}
    if media_type == "application/octet-stream":
        safe_name = re.sub(r"[^A-Za-zА-Яа-я0-9._ -]+", "_", _clean(attachment.get("filename"), 180)) or "attachment"
        headers["Content-Disposition"] = f'attachment; filename="{quote(safe_name)}"'
    return Response(
        content,
        media_type=media_type,
        headers=headers,
    )


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
        # Although SaleBot documents ``message`` as optional for a file-only
        # send, its Telegram transport can otherwise fall back to a plain URL.
        # A non-printing caption keeps the request on the native media path.
        if not text:
            body["message"] = "\u2060"
        body["attachment_url"] = attachment_url
        requested_type = _clean(attachment_type, 100).lower()
        if requested_type.startswith("image") or re.search(r"\.(?:jpe?g|png|gif|webp)(?:[?#]|$)", attachment_url, re.IGNORECASE):
            body["attachment_type"] = "image"
        elif requested_type.startswith("video"):
            body["attachment_type"] = "video"
        elif requested_type.startswith("audio") or requested_type == "voice":
            body["attachment_type"] = "audio"
        else:
            body["attachment_type"] = "file"
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
        if provider == EMAIL_PROVIDER:
            await _requested_channel(channel_id, transport, provider)
            email_service = _module_service("email-channel", "service_conversation")
            context = await _resolve_widget_context(data, mode, device)
            result = await email_service(context=context, offset=offset, limit=CONVERSATION_PAGE_SIZE)
            result["provider"] = EMAIL_PROVIDER
            result["offset"] = offset
            return _widget_response(request, result)
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
            if (
                not peer_id
                or not link
                or _telegram_dialog_hidden(
                    link, peer_id=peer_id, phone=data.get("phone"),
                    username=data.get("telegram_username"),
                )
            ):
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
                _schedule_wazzup_history(
                    device,
                    channel,
                    phone,
                    name=_clean((thread or {}).get("name") or data.get("name"), 200),
                    identity=identity,
                    offset=offset,
                    known_chat_id=chat_id,
                )
                history = {"status": "syncing", "imported": 0, "complete": False}
        if has_chat:
            await _mark_thread_read(int(device["id"]), channel_id, transport, chat_id)
            owner_id = await _responsible_admin_id(data, mode, device) if not thread else None
            await _assign_client_threads(owner_id, phone=phone)
            if not thread and _notification_source(channel_id, transport, provider) == "max":
                await _remember_notification_context(
                    _widget_context(data, mode, device), "max", chat_id, owner_id,
                )
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
    outbound_job: dict[str, Any] = {}
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
        request_key = _clean(data.get("request_id"), 300) or secrets.token_urlsafe(24)
        if not message_text and not attachment_url:
            raise HTTPException(400, "Введите сообщение")
        resolved_context = await _resolve_widget_context(data, mode, device)
        variables = resolved_context.get("variables") if isinstance(resolved_context.get("variables"), dict) else {}
        # The API is the final safety boundary: never rely on the browser having
        # called template-preview first. This also protects old tabs and direct
        # API callers from sending literal {{utm.*}} markers to a client.
        message_text = render_message_template(message_text, variables)["text"]
        if provider == EMAIL_PROVIDER:
            message_text, email_signature_url = await _auto_markup_values_for_send(
                [message_text, "https://sobakovod.pro/"], variables,
            )
        else:
            message_text = await _auto_markup_for_send(message_text, variables)
            email_signature_url = ""
        if len(message_text) > 4000:
            raise HTTPException(400, "После авторазметки сообщение длиннее 4000 символов")
        if re.search(r"\{\{\s*[a-zA-Z0-9_.:-]+\s*\}\}", message_text):
            raise HTTPException(400, "Подставьте все переменные шаблона")
        expected_client_name = _resolved_contact_name(resolved_context, data)
        mismatch = _salutation_name_mismatch(message_text, expected_client_name)
        if mismatch:
            raise HTTPException(
                409,
                f"В тексте обращение «{mismatch[0]}», а выбран клиент «{mismatch[1]}». Проверьте имя перед отправкой.",
            )
        if not channel_id or transport not in CHAT_TRANSPORTS:
            raise HTTPException(400, "Канал не указан")

        if provider == EMAIL_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            email_service = _module_service("email-channel", "service_send")
            try:
                result = await email_service(
                    context=resolved_context, text=message_text,
                    subject=_clean(data.get("subject"), 300),
                    manager_id=str(device["admin_id"]),
                    manager_name=_clean(device.get("admin_name"), 200),
                    from_name=_clean(device.get("admin_name"), 200),
                    idempotency_key=request_key,
                    attachment_url=attachment_url, attachment_type=attachment_type,
                    attachment_name=_clean(data.get("attachment_name"), 500),
                    signature_url=email_signature_url,
                    email_guidelines_confirmed=data.get("email_guidelines_confirmed") is True,
                    email_guidelines_version=_clean(data.get("email_guidelines_version"), 40),
                )
            except ValueError as exc:
                as_dict = getattr(exc, "as_dict", None)
                if callable(as_dict):
                    return _widget_response(request, as_dict(), int(getattr(exc, "status_code", 400)))
                raise HTTPException(409 if "несколькими" in str(exc) else 400, str(exc)) from exc
            return _widget_response(request, {"ok": True, "channel": channel, **result})

        async def start_delivery_job(chat_id: str, client_phone: str, client_name: str) -> dict[str, Any] | None:
            nonlocal outbound_job
            context = await _widget_delivery_context(
                data, mode, device, provider, chat_id, channel_id, transport,
            )
            outbound_job = await _start_outbound_job(
                request_key=request_key, device=device, provider=provider,
                channel_id=channel_id, chat_type=transport, chat_id=chat_id,
                phone=client_phone, client_name=client_name,
                email=_clean(data.get("email"), 320),
                getcourse_user_id=_clean(data.get("getcourse_user_id"), 200),
                text=message_text, attachment_url=attachment_url,
                attachment_type=attachment_type, context=context,
            )
            return _outbound_duplicate_payload(outbound_job)

        active_channels = await _all_channels()
        thread_fields = (
            _clean(data.get("thread_channel_id"), 200),
            _clean(data.get("thread_chat_type"), 40).lower(),
            _clean(data.get("thread_chat_id"), 250),
        )
        thread = None
        if any(thread_fields):
            thread = await _inbox_thread_context(*thread_fields, active_channels, device)

        async def trusted_provider_link(direct_provider: str) -> dict[str, Any]:
            candidate = _clean(data.get("provider_chat_id"), 200)
            if not candidate:
                return {}
            context = _widget_context(data, mode, device)
            entity_link = await _entity_external_link(
                context["platform"], context["entity_type"], context["entity_id"], direct_provider,
            )
            if direct_provider == TELEGRAM_PROVIDER and _telegram_dialog_hidden(
                entity_link, peer_id=candidate, phone=context.get("phone"),
            ):
                return {}
            if _clean(entity_link.get("external_user_id"), 200) == candidate:
                return entity_link
            link = await _external_link(peer_id=candidate, provider=direct_provider)
            return link if _card_link_matches_context(
                link, context, _clean(data.get("getcourse_user_id"), 200),
            ) else {}

        if provider == SALEBOT_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            link = await trusted_provider_link(provider)
            if not link:
                link = await _provider_card_link(data, mode, device, provider)
            client_id = _clean(link.get("external_user_id"), 200)
            if not client_id:
                raise HTTPException(404, "SaleBot ID не найден. Нужен salebot_id в карточке или utm_term.")
            duplicate = await start_delivery_job(
                client_id, _clean(link.get("phone") or data.get("phone"), 40),
                _clean(link.get("name") or data.get("name"), 200),
            )
            if duplicate:
                duplicate["channel"] = channel
                return _widget_response(request, duplicate)
            return await _queued_outbound_response(
                request, outbound_job, channel, device,
                phone=_clean(link.get("phone") or data.get("phone"), 40), entity_id=client_id,
            )
        if provider == "vk":
            channel = await _requested_channel(channel_id, transport, provider)
            page_kind, entity_id = _page_context(source_url) if not thread else ("inbox", thread["chat_id"])
            peer_id = _clean((thread or {}).get("chat_id"), 200)
            card_link: dict[str, Any] = await trusted_provider_link("vk") if not thread else {}
            peer_id = peer_id or _clean(card_link.get("external_user_id"), 200)
            peer_id = peer_id or _clean(data.get("vk_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, "vk")
                peer_id = _clean(card_link.get("external_user_id"), 200)
            stored_link = await _external_link(peer_id=peer_id)
            link = stored_link or card_link
            if not peer_id or not link:
                raise HTTPException(404, "Диалог VK не найден")
            duplicate = await start_delivery_job(
                peer_id, _clean(link.get("phone") or data.get("phone"), 40),
                _clean(link.get("name") or data.get("name"), 200),
            )
            if duplicate:
                duplicate["channel"] = channel
                return _widget_response(request, duplicate)
            return await _queued_outbound_response(
                request, outbound_job, channel, device,
                phone=_clean(link.get("phone") or data.get("phone"), 40),
                page_kind=page_kind, entity_id=entity_id,
            )
        if provider == TELEGRAM_PROVIDER:
            channel = await _requested_channel(channel_id, transport, provider)
            page_kind, entity_id = _page_context(source_url) if not thread else ("inbox", thread["chat_id"])
            peer_id = _clean((thread or {}).get("chat_id"), 200)
            card_link: dict[str, Any] = await trusted_provider_link(TELEGRAM_PROVIDER) if not thread else {}
            peer_id = peer_id or _clean(card_link.get("external_user_id"), 200)
            peer_id = peer_id or _clean(data.get("telegram_id"), 200)
            if not peer_id and not thread:
                card_link = await _provider_card_link(data, mode, device, TELEGRAM_PROVIDER, allow_phone_import=True)
                peer_id = _clean(card_link.get("external_user_id"), 200)
            if card_link.get("pending"):
                raise HTTPException(504, "Telegram не ответил. Повторите.")
            stored_link = await _external_link(peer_id=peer_id, provider=TELEGRAM_PROVIDER)
            link = stored_link or card_link
            if (
                not peer_id
                or not link
                or _telegram_dialog_hidden(
                    link, peer_id=peer_id, phone=data.get("phone"),
                    username=data.get("telegram_username"),
                )
            ):
                raise HTTPException(404, "Диалог Telegram не найден")
            duplicate = await start_delivery_job(
                peer_id, _clean(link.get("phone") or data.get("phone"), 40),
                _clean(link.get("name") or data.get("name"), 200),
            )
            if duplicate:
                duplicate["channel"] = channel
                return _widget_response(request, duplicate)
            return await _queued_outbound_response(
                request, outbound_job, channel, device,
                phone=_clean(link.get("phone") or data.get("phone"), 40),
                page_kind=page_kind, entity_id=entity_id,
            )
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
        if exact_thread:
            chat_id, has_chat = thread["chat_id"], True
        else:
            if not phone:
                raise HTTPException(409, "Для этого канала нужен телефон клиента")
            chat_id, has_chat, _ = await _conversation_rows(channel_id, transport, phone, 1)
        duplicate = await start_delivery_job(
            chat_id, phone, _clean((thread or {}).get("name") or data.get("name"), 200),
        )
        if duplicate:
            duplicate["channel"] = channel
            return _widget_response(request, duplicate)
        return await _queued_outbound_response(
            request, outbound_job, channel, device,
            phone=phone, page_kind=page_kind, entity_id=entity_id,
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
