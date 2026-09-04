from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import sys
from datetime import datetime, timedelta, timezone
from email.utils import formataddr, parseaddr
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, unquote_plus, urlencode, urlsplit

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from orchestrator.auth import can_access_module, enforce_rate_limit, verify_token_from_request

router = APIRouter()
MODULE_ID = "email-channel"
STAFF_REGISTRY_MODULE_NAME = "_nexus_mod_staff-registry"
STAFF_REGISTRY_PANEL_PATH = "/nexus/staff-registry/panel/"


def _ensure_local_staff_mutation_allowed() -> None:
    if sys.modules.get(STAFF_REGISTRY_MODULE_NAME) is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Сотрудники управляются в едином реестре: {STAFF_REGISTRY_PANEL_PATH}",
        )


API_KEY_ENV = "NEXUS_EMAIL_DASHAMAIL_API_KEY"
EVENT_KEY_ENV = "NEXUS_EMAIL_DASHAMAIL_EVENT_WEBHOOK_KEY"
ROUTER_KEY_ENV = "NEXUS_EMAIL_DASHAMAIL_ROUTER_SIGNING_KEY"
ROUTER_PREVIOUS_KEY_ENV = "NEXUS_EMAIL_DASHAMAIL_ROUTER_PREVIOUS_SIGNING_KEY"
API_BASE_ENV = "NEXUS_EMAIL_DASHAMAIL_API_BASE"
DEFAULT_API_BASE = "https://api.dashamail.com/"
PUBLIC_API_BASE = "https://junior.sobakovod.pro/nexus/email-channel/api"
DEFAULT_SIGNATURE_URL = "https://sobakovod.pro/"
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024
MAX_MESSAGE_CHARS = 100_000
ROUTER_CLOCK_SKEW_SECONDS = 600
EMAIL_RE = re.compile(r"^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$")
TOKEN_RE = re.compile(r"^case\+([A-Za-z0-9_-]{20,80})@", re.I)
URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
BARE_LINK_RE = re.compile(
    r"(?<![/:@.\w-])(?:[a-z0-9](?:[a-z0-9-]{0,62})\.)+[a-z]{2,}(?::\d{1,5})?(?:/[^\s<>\"']*)?",
    re.I,
)
EMBEDDED_EMAIL_RE = re.compile(r"(?<![\w.+-])[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", re.I)
PHONE_LIKE_RE = re.compile(r"(?<!\d)(?:\+?\d[\s().-]*){10,15}(?!\d)")
LOCAL_PART_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
RESERVED_LOCAL_PARTS = {"abuse", "admin", "case", "info", "mailer-daemon", "postmaster", "root", "support"}
CYRILLIC_LATIN = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
})
SHORT_SENDER_OVERRIDES = {
    "никита попов": "nikita.p",
    "татьяна воробьева": "tatiana.v",
    "татьяна истратова": "titiana.i",
}
INVALID_RECIPIENT_RE = re.compile(
    r"(?:\b5\.1\.1\b|\b5\.1\.0\b|user unknown|unknown user|no such (?:user|mailbox)|"
    r"mailbox (?:does not exist|not found)|recipient (?:does not exist|not found)|"
    r"invalid recipient|address (?:does not exist|not found)|bad destination mailbox address|"
    r"reason\s*[:=]?\s*hard)",
    re.I,
)
SIGNATURE_LINK_DOMAIN = "sobakovod.pro"
ALLOWED_LINK_DOMAINS = (
    "sobakovod.pro",
    "salebot.pro",
    "getcourse.ru",
    "vk.ru",
)
SHORT_LINK_DOMAINS = {
    "bit.ly", "clck.ru", "cutt.ly", "goo.gl", "is.gd", "lnkd.in", "rebrand.ly",
    "shorturl.at", "t.co", "tiny.cc", "tinyurl.com", "vk.cc",
}
ATTRIBUTION_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "param1", "param2", "ym_uid", "conversation_id",
}
EMAIL_GUIDELINES_CHECKLIST = [
    {
        "code": "contact_source_confirmed",
        "label": "Я понимаю, откуда получен контакт, и клиент ожидает это письмо.",
    },
    {
        "code": "honest_subject_confirmed",
        "label": "Тема письма соответствует содержанию, без обмана и кликбейта.",
    },
    {
        "code": "light_message_confirmed",
        "label": "В письме обычный текст без вложений и тяжёлых файлов.",
    },
]
EMAIL_GUIDELINES_VERSION = "2026-09-01"
DEFAULT_SETTINGS = {
    "enabled": "0", "pilot_mode": "1", "from_email": "info@support.sobakovod.pro",
    "reply_domain": "support.sobakovod.pro", "inbound_task_mode": "shadow",
    # Personal manager correspondence should not carry an invisible tracking
    # pixel by default. Delivery and replies remain observable via webhooks.
    "track_opens": "0", "track_clicks": "0", "max_attempts": "5",
    "request_timeout_seconds": "20",
}

_db_path: Path | None = None
_logger: Any = None
_wakeup = asyncio.Event()


class EmailGuardError(ValueError):
    """A machine-readable outbound policy rejection for every Nexus caller."""

    def __init__(
        self, code: str, message: str, *, status_code: int = 400,
        confirmation_required: bool = False, details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.confirmation_required = confirmation_required
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": False,
            "error": str(self),
            "code": self.code,
            "email_guard": True,
        }
        if self.confirmation_required:
            result.update({
                "confirmation_required": True,
                "checklist_version": EMAIL_GUIDELINES_VERSION,
                "checklist": EMAIL_GUIDELINES_CHECKLIST,
            })
        if self.details:
            result["details"] = self.details
        return result


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _now()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _bool(value: Any) -> bool:
    return _clean(value, 20).lower() in {"1", "true", "yes", "on"}


def _email(value: Any) -> str:
    address = parseaddr(_clean(value, 500))[1].casefold()
    return address if EMAIL_RE.fullmatch(address) else ""


def _url_body(value: str) -> str:
    """Remove sentence punctuation without changing the URL being validated."""
    result = value
    while result and result[-1] in ".,;:!?)]}":
        result = result[:-1]
    return result


def _message_urls(value: Any) -> list[str]:
    return [url for match in URL_RE.finditer(_clean(value, MAX_MESSAGE_CHARS)) if (url := _url_body(match.group(0)))]


def _allowed_message_link_host(host: Any) -> bool:
    normalized = _clean(host, 500).casefold().rstrip(".")
    return any(
        normalized == domain or normalized.endswith(f".{domain}")
        for domain in ALLOWED_LINK_DOMAINS
    )


def _validate_no_bare_links(body: str) -> None:
    # Validate bare domains only outside already parsed absolute URLs. Nested
    # redirect targets are decoded and checked separately below.
    scrubbed = list(body)
    for match in URL_RE.finditer(body):
        scrubbed[match.start():match.end()] = " " * (match.end() - match.start())
    for position, match in enumerate(BARE_LINK_RE.finditer("".join(scrubbed)), start=1):
        value = _url_body(match.group(0))
        host = value.split("/", 1)[0].split(":", 1)[0].casefold().rstrip(".")
        if host in SHORT_LINK_DOMAINS:
            raise EmailGuardError(
                "email_short_link_not_allowed",
                "Сокращённые ссылки в email запрещены. Вставьте прямую https-ссылку на сайт клуба.",
                details={"link_number": position, "domain": host},
            )
        raise EmailGuardError(
            "email_link_scheme_required",
            "Ссылка указана без https://. Вставьте полную разрешённую ссылку.",
            details={"link_number": position, "domain": host},
        )


def _digits(value: Any) -> str:
    return re.sub(r"\D", "", _clean(value, 2000))


def _decoded_url_parts(url: str) -> tuple[str, list[tuple[str, str]], str]:
    parsed = urlsplit(url)
    path = unquote_plus(parsed.path)
    pairs = [(unquote_plus(key), unquote_plus(value)) for key, value in parse_qsl(parsed.query, keep_blank_values=True)]
    return path, pairs, unquote_plus(parsed.fragment)


def _url_personal_data(url: str, context: dict[str, Any]) -> str:
    """Return the PII kind found in URL path/query while preserving tracking IDs."""
    path, pairs, fragment = _decoded_url_parts(url)
    pieces = [path, *(part for pair in pairs for part in pair), fragment]
    decoded = " ".join(pieces)
    if EMBEDDED_EMAIL_RE.search(decoded):
        return "email"

    contact_email = _email(context.get("email"))
    if contact_email and contact_email.casefold() in decoded.casefold():
        return "email"

    contact_phone = _digits(context.get("phone"))
    if len(contact_phone) >= 10 and any(contact_phone in _digits(piece) for piece in pieces):
        return "phone"

    # Numeric analytics identifiers generated by Nexus are valid in these
    # exact fields. Exact contact phone detection above still wins, so a phone
    # cannot be hidden in utm_term/param1/param2.
    if any(10 <= len(_digits(match.group(0))) <= 15 for match in PHONE_LIKE_RE.finditer(f"{path} {fragment}")):
        return "phone"
    for key, value in pairs:
        if key.casefold() in ATTRIBUTION_QUERY_KEYS:
            continue
        if any(10 <= len(_digits(match.group(0))) <= 15 for match in PHONE_LIKE_RE.finditer(value)):
            return "phone"

    # Names are blocked only on an exact full-name match. A single word is too
    # ambiguous (for example a course slug may legitimately contain it).
    full_name = " ".join(_clean(context.get("name"), 300).casefold().split())
    if len(full_name.split()) >= 2:
        normalized_url = " ".join(re.sub(r"[/_.-]+", " ", decoded.casefold()).split())
        if full_name in normalized_url:
            return "name"
    return ""


def _nested_link_host(url: str) -> str:
    """Find a URL/domain hidden in redirect parameters or a fragment."""
    _path, pairs, fragment = _decoded_url_parts(url)
    for value in [*(value for _key, value in pairs), fragment]:
        candidates = _message_urls(value)
        if value.startswith("//"):
            candidates.append(f"https:{value}")
        for candidate in candidates:
            try:
                if host := (urlsplit(candidate).hostname or "").casefold().rstrip("."):
                    return host
            except ValueError:
                return "invalid"
        if match := BARE_LINK_RE.search(value):
            return _url_body(match.group(0)).split("/", 1)[0].split(":", 1)[0].casefold().rstrip(".")
    return ""


def _validate_message_links(body: str, context: dict[str, Any]) -> None:
    _validate_no_bare_links(body)
    for position, url in enumerate(_message_urls(body), start=1):
        try:
            parsed = urlsplit(url)
            host = (parsed.hostname or "").casefold().rstrip(".")
        except ValueError as exc:
            raise EmailGuardError(
                "email_link_invalid", "В письме найдена некорректная ссылка.",
                details={"link_number": position},
            ) from exc
        if parsed.username is not None or parsed.password is not None:
            raise EmailGuardError(
                "email_link_credentials_not_allowed",
                "В ссылке нельзя указывать логин или пароль.",
                details={"link_number": position},
            )
        if host in SHORT_LINK_DOMAINS:
            raise EmailGuardError(
                "email_short_link_not_allowed",
                "Сокращённые ссылки в email запрещены. Вставьте прямую ссылку на сайт клуба.",
                details={"link_number": position, "domain": host},
            )
        if not _allowed_message_link_host(host):
            raise EmailGuardError(
                "email_link_domain_not_allowed",
                "В email разрешены только ссылки на sobakovod.pro, salebot.pro, getcourse.ru, vk.ru и их поддомены.",
                details={"link_number": position, "domain": host},
            )
        if parsed.scheme.casefold() != "https":
            raise EmailGuardError(
                "email_link_https_required",
                "Ссылка в email должна начинаться с https://.",
                details={"link_number": position, "domain": host},
            )
        nested_host = _nested_link_host(url)
        if nested_host and not _allowed_message_link_host(nested_host):
            if nested_host in SHORT_LINK_DOMAINS:
                code = "email_short_link_not_allowed"
                message = "Ссылка содержит скрытый сокращённый адрес. Вставьте прямую ссылку на сайт клуба."
            else:
                code = "email_nested_link_not_allowed"
                message = "Ссылка содержит скрытый переход на другой домен. Используйте прямую разрешённую ссылку."
            raise EmailGuardError(
                code, message,
                details={"link_number": position, "domain": nested_host},
            )
        pii_kind = _url_personal_data(url, context)
        if pii_kind:
            labels = {"email": "email клиента", "phone": "телефон клиента", "name": "имя и фамилия клиента"}
            raise EmailGuardError(
                "email_url_contains_personal_data",
                f"Отправка остановлена: ссылка содержит {labels[pii_kind]}. Уберите персональные данные из URL.",
                details={"link_number": position, "personal_data": pii_kind},
            )


def _sender_domain(settings: dict[str, str]) -> str:
    return _email(settings.get("from_email")).partition("@")[2]


def _latin_piece(value: Any) -> str:
    text = _clean(value, 200).casefold().translate(CYRILLIC_LATIN)
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def _legacy_local_part(manager_name: Any, manager_id: Any = "") -> str:
    parts = [_latin_piece(part) for part in re.split(r"\s+", _clean(manager_name, 200))]
    parts = [part for part in parts if part]
    value = ".".join((parts[0], parts[-1])) if len(parts) > 1 else (parts[0] if parts else "")
    if not value:
        suffix = re.sub(r"[^a-z0-9]", "", _clean(manager_id, 80).casefold())[:16]
        value = f"manager-{suffix}" if suffix else "manager"
    if value in RESERVED_LOCAL_PARTS:
        value = f"manager-{value}"
    return value[:64].rstrip("._-") or "manager"


def _suggest_local_part(manager_name: Any, manager_id: Any = "") -> str:
    clean_name = " ".join(_clean(manager_name, 200).casefold().split())
    if clean_name in SHORT_SENDER_OVERRIDES:
        return SHORT_SENDER_OVERRIDES[clean_name]
    parts = [_latin_piece(part) for part in clean_name.split()]
    parts = [part for part in parts if part]
    value = f"{parts[0]}.{parts[-1][0]}" if len(parts) > 1 else (parts[0] if parts else "")
    if not value:
        suffix = re.sub(r"[^a-z0-9]", "", _clean(manager_id, 80).casefold())[:16]
        value = f"manager-{suffix}" if suffix else "manager"
    if value in RESERVED_LOCAL_PARTS:
        value = f"manager-{value}"
    return value[:64].rstrip("._-") or "manager"


def _first_name(manager_name: Any) -> str:
    parts = _clean(manager_name, 200).split()
    return parts[0] if parts else ""


def _unsubscribe_key() -> bytes:
    source = os.environ.get(ROUTER_KEY_ENV, "")
    if not source:
        return b""
    return hmac.new(source.encode(), b"nexus-email-unsubscribe-v1", hashlib.sha256).digest()


def _unsubscribe_query(email_address: str) -> str:
    key = _unsubscribe_key()
    if not key:
        return ""
    encoded = base64.urlsafe_b64encode(email_address.encode()).decode().rstrip("=")
    signature = hmac.new(key, encoded.encode(), hashlib.sha256).hexdigest()
    return urlencode({"e": encoded, "s": signature})


def _unsubscribe_email(encoded: Any, signature: Any) -> str:
    value, candidate, key = _clean(encoded, 1000), _clean(signature, 200).casefold(), _unsubscribe_key()
    expected = hmac.new(key, value.encode(), hashlib.sha256).hexdigest() if key and value else ""
    if not expected or not candidate or not hmac.compare_digest(candidate, expected):
        return ""
    try:
        padding = "=" * (-len(value) % 4)
        return _email(base64.urlsafe_b64decode(value + padding).decode())
    except (ValueError, UnicodeDecodeError):
        return ""


def _visible_unsubscribe_url(email_address: str) -> str:
    query = _unsubscribe_query(email_address)
    return f"{PUBLIC_API_BASE}/unsubscribe?{query}" if query else ""


def _one_click_unsubscribe_url(email_address: str) -> str:
    query = _unsubscribe_query(email_address)
    return f"{PUBLIC_API_BASE}/unsubscribe/one-click?{query}" if query else ""


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("email-channel is not initialized")
    return _db_path


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


async def _connect() -> aiosqlite.Connection:
    db = await aiosqlite.connect(_must_db(), timeout=30)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA busy_timeout=30000")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA synchronous=NORMAL")
    return db


def _suppression_error(reason: Any) -> EmailGuardError:
    clean_reason = _clean(reason, 200).casefold()
    if clean_reason == "unsubscribe":
        return EmailGuardError(
            "email_unsubscribed",
            "Клиент отписался от ручных писем. Отправка на этот адрес запрещена.",
            status_code=403,
        )
    if clean_reason in {"spam", "complained"}:
        return EmailGuardError(
            "email_spam_complaint",
            "Клиент пожаловался на спам. Повторная отправка на этот адрес запрещена.",
            status_code=403,
        )
    return EmailGuardError(
        "email_recipient_suppressed",
        "Отправка на этот адрес запрещена после ошибки доставки.",
        status_code=403,
        details={"reason": clean_reason or "suppressed"},
    )


async def _active_suppression(address: str) -> str:
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT reason FROM suppressions WHERE email=? AND active=1", (address,),
        )).fetchone()
        return _clean(row["reason"], 200) if row else ""
    finally:
        await db.close()


async def _cancel_queued_for_suppression(db: aiosqlite.Connection, address: str, reason: str, now: str) -> None:
    error = str(_suppression_error(reason))
    await db.execute(
        """UPDATE outbound_jobs SET status='failed',next_attempt_at='',error=?,updated_at=?
           WHERE status IN ('pending','retry','processing') AND message_id IN
             (SELECT id FROM email_messages WHERE to_email=?)""",
        (error, now, address),
    )
    await db.execute(
        """UPDATE email_messages SET status='failed',error=?,updated_at=?
           WHERE to_email=? AND status IN ('queued','pending','processing','retry')""",
        (error, now, address),
    )


async def _init_db() -> None:
    db = await _connect()
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS mailboxes(
          id INTEGER PRIMARY KEY,address TEXT NOT NULL UNIQUE,provider TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS email_threads(
          id INTEGER PRIMARY KEY,public_token TEXT NOT NULL UNIQUE,mailbox_id INTEGER NOT NULL REFERENCES mailboxes(id),
          client_email TEXT NOT NULL,client_name TEXT NOT NULL DEFAULT '',subject TEXT NOT NULL DEFAULT '',
          state TEXT NOT NULL DEFAULT 'active',last_rfc_message_id TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS email_thread_links(
          thread_id INTEGER NOT NULL REFERENCES email_threads(id) ON DELETE CASCADE,platform TEXT NOT NULL,
          entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
          PRIMARY KEY(platform,entity_type,entity_id));
        CREATE TABLE IF NOT EXISTS email_messages(
          id INTEGER PRIMARY KEY,thread_id INTEGER NOT NULL REFERENCES email_threads(id) ON DELETE CASCADE,
          nexus_message_id TEXT NOT NULL UNIQUE,provider_message_id TEXT UNIQUE,rfc_message_id TEXT,
          direction TEXT NOT NULL,status TEXT NOT NULL,opened INTEGER NOT NULL DEFAULT 0,replied INTEGER NOT NULL DEFAULT 0,
          from_email TEXT NOT NULL,to_email TEXT NOT NULL,subject TEXT NOT NULL,text_body TEXT NOT NULL DEFAULT '',
          html_body TEXT NOT NULL DEFAULT '',in_reply_to TEXT NOT NULL DEFAULT '',references_json TEXT NOT NULL DEFAULT '[]',
          manager_id TEXT NOT NULL DEFAULT '',manager_name TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
          sent_at TEXT NOT NULL DEFAULT '',delivered_at TEXT NOT NULL DEFAULT '',opened_at TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS outbound_jobs(
          id INTEGER PRIMARY KEY,idempotency_key TEXT NOT NULL UNIQUE,
          message_id INTEGER NOT NULL UNIQUE REFERENCES email_messages(id) ON DELETE CASCADE,
          status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at TEXT NOT NULL DEFAULT '',
          claimed_at TEXT NOT NULL DEFAULT '',provider_response TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS provider_events(
          id INTEGER PRIMARY KEY,event_hash TEXT NOT NULL UNIQUE,provider_event_id TEXT NOT NULL DEFAULT '',
          nexus_message_id TEXT NOT NULL DEFAULT '',provider_message_id TEXT NOT NULL DEFAULT '',event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,received_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS inbound_events(
          id INTEGER PRIMARY KEY,event_hash TEXT NOT NULL UNIQUE,rfc_message_id TEXT NOT NULL DEFAULT '',
          recipient TEXT NOT NULL,sender TEXT NOT NULL,event_type TEXT NOT NULL,status TEXT NOT NULL,
          thread_id INTEGER REFERENCES email_threads(id) ON DELETE SET NULL,
          message_id INTEGER REFERENCES email_messages(id) ON DELETE SET NULL,payload_json TEXT NOT NULL,
          error TEXT NOT NULL DEFAULT '',received_at TEXT NOT NULL,processed_at TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS suppressions(
          email TEXT PRIMARY KEY,reason TEXT NOT NULL,source_event_hash TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sender_profiles(
          manager_id TEXT PRIMARY KEY,manager_name TEXT NOT NULL,local_part TEXT NOT NULL UNIQUE,
          enabled INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ix_email_threads_email ON email_threads(client_email,updated_at DESC);
        CREATE INDEX IF NOT EXISTS ix_email_links_thread ON email_thread_links(thread_id);
        CREATE INDEX IF NOT EXISTS ix_email_messages_thread ON email_messages(thread_id,id DESC);
        CREATE INDEX IF NOT EXISTS ix_email_messages_provider ON email_messages(provider_message_id);
        CREATE INDEX IF NOT EXISTS ix_email_outbound_due ON outbound_jobs(status,next_attempt_at,id);
        CREATE INDEX IF NOT EXISTS ix_email_inbound_thread ON inbound_events(thread_id,id DESC);
        CREATE INDEX IF NOT EXISTS ix_email_sender_profiles_name ON sender_profiles(manager_name);
        """)
        now = _iso()
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)", (key, value, now))
        await db.execute(
            "INSERT OR IGNORE INTO mailboxes(address,provider,enabled,created_at,updated_at) VALUES(?,'dashamail',1,?,?)",
            (DEFAULT_SETTINGS["from_email"], now, now),
        )
        await db.execute("UPDATE outbound_jobs SET status='pending',claimed_at='',updated_at=? WHERE status='processing'", (now,))
        await db.commit()
    finally:
        await db.close()


async def setup(ctx) -> None:
    global _db_path, _logger
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    lifecycle = getattr(ctx, "lifecycle", None)
    if lifecycle is not None:
        lifecycle.create_task(_delivery_loop(), name="email-channel-delivery")


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _settings() -> dict[str, str]:
    db = await _connect()
    try:
        rows = await (await db.execute("SELECT key,value FROM settings")).fetchall()
        return {row["key"]: row["value"] for row in rows}
    finally:
        await db.close()


async def _messenger_staff() -> list[dict[str, str]]:
    module = sys.modules.get("_nexus_mod_messenger-widget")
    callback = getattr(module, "service_email_staff", None) if module else None
    if not callable(callback):
        return []
    try:
        rows = await callback()
    except Exception as exc:
        _log("warning", "Messenger staff list unavailable: %s", type(exc).__name__)
        return []
    return [
        {"manager_id": _clean(row.get("id"), 200), "manager_name": _clean(row.get("name"), 200)}
        for row in rows if isinstance(row, dict) and _clean(row.get("id"), 200) and _clean(row.get("name"), 200)
    ]


async def _ensure_sender_profile(manager_id: Any, manager_name: Any) -> dict[str, Any] | None:
    clean_id, clean_name = _clean(manager_id, 200), _clean(manager_name, 200)
    if not clean_id or not clean_name:
        return None
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT * FROM sender_profiles WHERE manager_id=?", (clean_id,),
        )).fetchone()
        now = _iso()
        if row:
            updates: dict[str, Any] = {}
            if row["manager_name"] != clean_name:
                updates["manager_name"] = clean_name
            # Shorten only aliases generated by Nexus 0.2.0/0.2.1. An address
            # edited by an administrator remains untouched.
            legacy = _legacy_local_part(row["manager_name"], clean_id)
            suggested = _suggest_local_part(clean_name, clean_id)
            if row["local_part"] == legacy and suggested != legacy:
                candidate, suffix = suggested, 1
                while await (await db.execute(
                    "SELECT 1 FROM sender_profiles WHERE local_part=? AND manager_id<>?",
                    (candidate, clean_id),
                )).fetchone():
                    suffix += 1
                    candidate = f"{suggested[:max(1, 63 - len(str(suffix)))]}-{suffix}"
                updates["local_part"] = candidate
            if updates:
                await db.execute(
                    "UPDATE sender_profiles SET manager_name=?,local_part=?,updated_at=? WHERE manager_id=?",
                    (updates.get("manager_name", row["manager_name"]),
                     updates.get("local_part", row["local_part"]), now, clean_id),
                )
                await db.commit()
                row = await (await db.execute(
                    "SELECT * FROM sender_profiles WHERE manager_id=?", (clean_id,),
                )).fetchone()
            return dict(row)
        base, local_part, suffix = _suggest_local_part(clean_name, clean_id), "", 1
        while not local_part:
            candidate = base if suffix == 1 else f"{base[:max(1, 63 - len(str(suffix)))]}-{suffix}"
            exists = await (await db.execute(
                "SELECT 1 FROM sender_profiles WHERE local_part=?", (candidate,),
            )).fetchone()
            if not exists:
                local_part = candidate
                break
            suffix += 1
        await db.execute(
            """INSERT INTO sender_profiles(manager_id,manager_name,local_part,created_at,updated_at)
               VALUES(?,?,?,?,?)""",
            (clean_id, clean_name, local_part, now, now),
        )
        await db.commit()
        row = await (await db.execute(
            "SELECT * FROM sender_profiles WHERE manager_id=?", (clean_id,),
        )).fetchone()
        return dict(row)
    finally:
        await db.close()


async def _sync_sender_profiles() -> list[dict[str, Any]]:
    for staff in await _messenger_staff():
        await _ensure_sender_profile(staff["manager_id"], staff["manager_name"])
    settings, db = await _settings(), await _connect()
    try:
        rows = await (await db.execute(
            "SELECT * FROM sender_profiles ORDER BY manager_name,manager_id",
        )).fetchall()
    finally:
        await db.close()
    domain = _sender_domain(settings)
    return [{**dict(row), "email": f"{row['local_part']}@{domain}"} for row in rows]


def service_staff_connector() -> dict[str, Any]:
    """Describe the email sender settings managed by the staff registry."""
    return {
        "module_id": MODULE_ID,
        "version": 1,
        "label": "Email-канал",
        "dependencies": ["messenger-widget"],
        "capabilities": ["list", "snapshot", "upsert", "deactivate"],
        "config_schema": {
            "local_part": {"type": "string", "label": "Персональный email", "max_length": 64},
            "enabled": {"type": "boolean", "default": True},
            "messenger_admin_id": {"type": "source_link", "module_id": "messenger-widget"},
        },
    }


def _staff_link(employee: dict[str, Any], module_id: str) -> str:
    links = employee.get("source_links")
    if not isinstance(links, dict):
        return ""
    value = links.get(module_id)
    if value is None:
        value = links.get(module_id.replace("-", "_"))
    if isinstance(value, dict):
        value = value.get("local_id")
    return _clean(value, 200)


def _staff_messenger_manager_id(employee: dict[str, Any], config: dict[str, Any] | None = None) -> str:
    """Resolve a sender only from an explicit Messenger link/identity/config."""
    linked = _staff_link(employee, "messenger-widget")
    if linked:
        return linked
    for identity in employee.get("identities", []) if isinstance(employee.get("identities"), list) else []:
        if not isinstance(identity, dict):
            continue
        provider = _clean(identity.get("provider"), 40).casefold().replace("_", "-")
        if provider in {"messenger-widget", "messenger-widget-local"}:
            external_id = _clean(identity.get("external_id") or identity.get("id"), 200)
            if external_id:
                return external_id
    values = config if isinstance(config, dict) else {}
    return _clean(values.get("messenger_admin_id") or values.get("manager_id"), 200)


async def _staff_sender_snapshot(manager_id: str) -> dict[str, Any]:
    clean_id = _clean(manager_id, 200)
    db = await _connect()
    try:
        row = await (await db.execute(
            "SELECT * FROM sender_profiles WHERE manager_id=?", (clean_id,),
        )).fetchone() if clean_id else None
    finally:
        await db.close()
    if not row:
        return {
            "ok": True, "module_id": MODULE_ID, "found": False, "status": "unlinked", "local_id": clean_id,
        }
    settings = await _settings()
    value = dict(row)
    config = {"local_part": value["local_part"], "enabled": bool(value["enabled"]), "manager_id": clean_id}
    return {
        "ok": True,
        "module_id": MODULE_ID,
        "found": True,
        "status": "active" if value["enabled"] else "inactive",
        "local_id": clean_id,
        "display_name": value["manager_name"],
        "active": bool(value["enabled"]),
        "email": f"{value['local_part']}@{_sender_domain(settings)}",
        "config": config,
    }


async def service_staff_list() -> list[dict[str, Any]]:
    """Export sender profiles without messages, credentials, or delivery data."""
    db = await _connect()
    try:
        profiles = [dict(row) for row in await (await db.execute(
            "SELECT * FROM sender_profiles ORDER BY manager_name,manager_id",
        )).fetchall()]
    finally:
        await db.close()
    return [{
        "module_id": MODULE_ID,
        "local_id": _clean(row["manager_id"], 200),
        "full_name": _clean(row["manager_name"], 200),
        "display_name": _clean(row["manager_name"], 200),
        "identities": [{"provider": "messenger-widget", "external_id": _clean(row["manager_id"], 200)}],
        "config": {
            "local_part": _clean(row["local_part"], 64),
            "enabled": bool(row["enabled"]),
            "manager_id": _clean(row["manager_id"], 200),
        },
        "active": bool(row["enabled"]),
    } for row in profiles]


async def service_staff_snapshot(*, employee: dict[str, Any]) -> dict[str, Any]:
    manager_id = _staff_messenger_manager_id(employee) or _staff_link(employee, MODULE_ID)
    result = await _staff_sender_snapshot(manager_id)
    if not _staff_messenger_manager_id(employee):
        result["warnings"] = ["Email sender is not linked to a Messenger employee"]
    return result


async def service_staff_apply(
    *, employee: dict[str, Any], config: dict[str, Any] | None, operation: str,
    idempotency_key: str = "",
) -> dict[str, Any]:
    desired = dict(config or {})
    action = _clean(operation, 40).casefold().replace("_", "-")
    if action not in {"upsert", "create", "update", "activate", "deactivate", "disable", "remove"}:
        raise ValueError(f"Unsupported Email staff operation: {operation}")
    deactivate = action in {"deactivate", "disable", "remove"}
    messenger_id = _staff_messenger_manager_id(employee, desired)
    existing_email_id = _staff_link(employee, MODULE_ID)
    manager_id = messenger_id or (existing_email_id if deactivate else "")
    if not manager_id:
        if deactivate:
            return {
                "ok": True, "module_id": MODULE_ID, "operation": "deactivate", "local_id": "",
                "changed": False, "snapshot": {"found": False, "status": "unlinked"},
                "idempotency_key": _clean(idempotency_key, 300),
            }
        raise ValueError("Email sender requires an explicit Messenger employee link")
    if existing_email_id and messenger_id and existing_email_id != messenger_id:
        raise ValueError("Email and Messenger source links point to different employees")

    before = await _staff_sender_snapshot(manager_id)
    if deactivate:
        if not before.get("found"):
            return {
                "ok": True, "module_id": MODULE_ID, "operation": "deactivate", "local_id": manager_id,
                "changed": False, "snapshot": before, "idempotency_key": _clean(idempotency_key, 300),
            }
        db = await _connect()
        try:
            await db.execute(
                "UPDATE sender_profiles SET enabled=0,updated_at=? WHERE manager_id=? AND enabled=1",
                (_iso(), manager_id),
            )
            await db.commit()
        finally:
            await db.close()
        snapshot = await _staff_sender_snapshot(manager_id)
        return {
            "ok": True, "module_id": MODULE_ID, "operation": "deactivate", "local_id": manager_id,
            "changed": bool(before.get("active")), "config": snapshot.get("config", {}), "snapshot": snapshot,
            "idempotency_key": _clean(idempotency_key, 300),
        }

    name = _clean(employee.get("display_name") or employee.get("full_name"), 200)
    if not name:
        raise ValueError("Email sender name is required")
    profile = await _ensure_sender_profile(manager_id, name)
    if not profile:
        raise ValueError("Email sender profile could not be created")
    local_part = _clean(desired.get("local_part") or desired.get("alias") or profile["local_part"], 100).casefold()
    if not LOCAL_PART_RE.fullmatch(local_part) or len(local_part) > 64 or local_part in RESERVED_LOCAL_PARTS:
        raise ValueError("Email alias may contain latin letters, digits, dots, dashes, and underscores")
    employee_status = _clean(employee.get("status"), 40).casefold()
    enabled = bool(desired.get("enabled", employee_status not in {"disabled", "inactive", "dismissed", "terminated", "fired"}))
    changed = (
        not before.get("found") or before.get("display_name") != name
        or before.get("config", {}).get("local_part") != local_part
        or bool(before.get("active")) != enabled
    )
    db = await _connect()
    try:
        await db.execute(
            "UPDATE sender_profiles SET manager_name=?,local_part=?,enabled=?,updated_at=? WHERE manager_id=?",
            (name, local_part, int(enabled), _iso(), manager_id),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise ValueError("Email alias is already assigned to another employee") from exc
    finally:
        await db.close()
    snapshot = await _staff_sender_snapshot(manager_id)
    return {
        "ok": True, "module_id": MODULE_ID, "operation": "upsert", "local_id": manager_id,
        "changed": changed, "config": snapshot.get("config", {}), "snapshot": snapshot,
        "idempotency_key": _clean(idempotency_key, 300),
    }


async def _sender_for_message(settings: dict[str, str], manager_id: Any, manager_name: Any) -> str:
    profile = await _ensure_sender_profile(manager_id, manager_name)
    domain = _sender_domain(settings)
    if profile and profile["enabled"] and domain:
        return f"{profile['local_part']}@{domain}"
    return _email(settings.get("from_email"))


def _context_parts(context: dict[str, Any]) -> tuple[str, str, str]:
    platform = _clean(context.get("platform") or context.get("service"), 40).lower()
    entity_type = _clean(context.get("entity_type") or ("lead" if platform == "amocrm" else "user"), 40).lower()
    entity_id = _clean(context.get("entity_id") or context.get("lead_id") or context.get("getcourse_user_id"), 200)
    return platform, entity_type, entity_id


async def _resolve_thread(context: dict[str, Any], recipient: str, *, attach_unique: bool = False) -> tuple[dict[str, Any] | None, bool]:
    platform, entity_type, entity_id = _context_parts(context)
    db = await _connect()
    try:
        if platform and entity_type and entity_id:
            row = await (await db.execute(
                "SELECT t.* FROM email_thread_links l JOIN email_threads t ON t.id=l.thread_id WHERE l.platform=? AND l.entity_type=? AND l.entity_id=?",
                (platform, entity_type, entity_id),
            )).fetchone()
            if row:
                return (None, True) if recipient and row["client_email"] != recipient else (dict(row), False)
        rows = await (await db.execute(
            "SELECT * FROM email_threads WHERE client_email=? AND state='active' ORDER BY updated_at DESC LIMIT 3", (recipient,),
        )).fetchall() if recipient else []
        if len(rows) != 1:
            return None, len(rows) > 1
        row = dict(rows[0])
        if attach_unique and platform and entity_type and entity_id:
            # A shared email must never make one thread authoritative for two
            # different cards of the same kind (especially two amoCRM leads).
            # Cross-system links are allowed only while each system has a
            # single exact entity bound to this thread.
            links = await (await db.execute(
                "SELECT platform,entity_type,entity_id FROM email_thread_links WHERE thread_id=?",
                (row["id"],),
            )).fetchall()
            if any(link["platform"] == platform and link["entity_type"] == entity_type
                   and link["entity_id"] != entity_id for link in links):
                return None, True
            now = _iso()
            await db.execute(
                "INSERT OR IGNORE INTO email_thread_links(thread_id,platform,entity_type,entity_id,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (row["id"], platform, entity_type, entity_id, now, now),
            )
            await db.commit()
        return row, False
    finally:
        await db.close()


async def _create_thread(context: dict[str, Any], recipient: str, client_name: str, subject: str) -> dict[str, Any]:
    settings = await _settings()
    platform, entity_type, entity_id = _context_parts(context)
    if not platform or not entity_type or not entity_id:
        raise ValueError("Для нового email-диалога нужна точная карточка клиента")
    # Email addresses are normalized case-insensitively by many systems. Keep
    # the opaque route token lowercase so case folding cannot break replies.
    now, token = _iso(), secrets.token_urlsafe(24).lower()
    db = await _connect()
    try:
        mailbox = await (await db.execute("SELECT id FROM mailboxes WHERE address=?", (settings["from_email"].casefold(),))).fetchone()
        if mailbox:
            mailbox_id = mailbox["id"]
        else:
            cursor = await db.execute(
                "INSERT INTO mailboxes(address,provider,enabled,created_at,updated_at) VALUES(?,'dashamail',1,?,?)",
                (settings["from_email"].casefold(), now, now),
            )
            mailbox_id = cursor.lastrowid
        cursor = await db.execute(
            "INSERT INTO email_threads(public_token,mailbox_id,client_email,client_name,subject,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (token, mailbox_id, recipient, _clean(client_name, 200), subject, now, now),
        )
        thread_id = cursor.lastrowid
        await db.execute(
            "INSERT INTO email_thread_links(thread_id,platform,entity_type,entity_id,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            (thread_id, platform, entity_type, entity_id, now, now),
        )
        await db.commit()
        row = await (await db.execute("SELECT * FROM email_threads WHERE id=?", (thread_id,))).fetchone()
        return dict(row)
    finally:
        await db.close()


async def service_channel(*, context: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = await _settings()
    recipient = _email((context or {}).get("email"))
    ready = _bool(settings.get("enabled")) and bool(os.environ.get(API_KEY_ENV))
    return {
        "channel_id": "email:info", "transport": "email", "channel_transport": "email", "provider": "email",
        "name": "Email", "label": "Email", "available": ready,
        "can_send": ready and bool(recipient), "recipient": recipient, "send_all_allowed": True,
        "send_reason": "" if ready and recipient else ("Email клиента не найден" if ready else "Email-канал ещё не включён"),
    }


async def service_conversation(*, context: dict[str, Any], offset: int = 0, limit: int = 50) -> dict[str, Any]:
    recipient = _email(context.get("email"))
    thread, ambiguous = await _resolve_thread(context, recipient)
    channel = await service_channel(context=context)
    if not thread:
        reason = "Один email связан с несколькими цепочками — требуется ручное сопоставление" if ambiguous else channel["send_reason"]
        return {"ok": True, "channel": channel, "thread_id": "", "chat_id": "", "subject": "", "messages": [],
                "has_chat": False, "confirmed_chat": False, "ambiguous": ambiguous,
                "can_send": channel["can_send"] and not ambiguous,
                "send_reason": reason, "requires_subject": True, "email_guidelines_required": True,
                "history_complete": True, "has_more": False}
    db = await _connect()
    try:
        rows = await (await db.execute(
            """SELECT nexus_message_id AS external_id,direction,status,text_body AS text,html_body,
                      manager_name AS author_name,created_at AS sent_at,opened,replied,error,subject
               FROM email_messages WHERE thread_id=? ORDER BY id DESC LIMIT ? OFFSET ?""",
            (thread["id"], max(1, min(int(limit), 100)), max(0, int(offset))),
        )).fetchall()
        state = await (await db.execute(
            """SELECT
                 EXISTS(SELECT 1 FROM email_messages WHERE thread_id=? AND direction='outgoing') AS has_outgoing,
                 EXISTS(SELECT 1 FROM email_messages WHERE thread_id=? AND (
                   direction='incoming' OR (direction='outgoing' AND status IN ('accepted','sent','delivered','opened'))
                 )) AS confirmed_chat""",
            (thread["id"], thread["id"]),
        )).fetchone()
    finally:
        await db.close()
    messages = [dict(row) for row in reversed(rows)]
    for message in messages:
        if message["opened"]:
            message["status"] = "opened"
    return {"ok": True, "channel": channel, "thread_id": thread["public_token"], "chat_id": thread["public_token"],
            "subject": thread["subject"], "messages": messages, "has_chat": bool(messages), "ambiguous": False,
            "confirmed_chat": bool(state["confirmed_chat"]),
            "can_send": channel["can_send"], "send_reason": channel["send_reason"], "requires_subject": False,
            "email_guidelines_required": not bool(state["has_outgoing"]),
            "history_complete": len(rows) < limit, "next_offset": offset + len(rows), "has_more": len(rows) >= limit}


async def service_send(*, context: dict[str, Any], text: str, idempotency_key: str, subject: str = "",
                       manager_id: str = "", manager_name: str = "", from_name: str = "",
                       attachment_url: str = "", attachment_type: str = "", attachment_name: str = "",
                       attachments: list[dict[str, Any]] | None = None,
                       signature_url: str = "",
                       email_guidelines_confirmed: bool = False,
                       email_guidelines_version: str = "") -> dict[str, Any]:
    settings = await _settings()
    if not _bool(settings.get("enabled")):
        raise ValueError("Email-канал выключен")
    if not os.environ.get(API_KEY_ENV):
        raise ValueError("API-ключ DashaMail не настроен")
    recipient, body, request_key = _email(context.get("email")), _clean(text, MAX_MESSAGE_CHARS), _clean(idempotency_key, 300)
    if not recipient:
        raise ValueError("Email клиента не найден или заполнен неверно")
    if not body:
        raise ValueError("Введите текст письма")
    if not request_key:
        raise ValueError("idempotency_key обязателен")
    db = await _connect()
    try:
        blocked = await (await db.execute("SELECT reason FROM suppressions WHERE email=? AND active=1", (recipient,))).fetchone()
        if blocked:
            raise _suppression_error(blocked["reason"])
        existing = await (await db.execute(
            """SELECT j.status,j.error,m.nexus_message_id,t.public_token,t.subject FROM outbound_jobs j
               JOIN email_messages m ON m.id=j.message_id JOIN email_threads t ON t.id=m.thread_id WHERE j.idempotency_key=?""",
            (request_key,),
        )).fetchone()
        if existing:
            result = dict(existing)
            return {"ok": True, "queued": result["status"] in {"pending", "processing", "retry"}, **result}
    finally:
        await db.close()
    thread, ambiguous = await _resolve_thread(context, recipient, attach_unique=True)
    if ambiguous:
        raise ValueError("Этот email связан с несколькими переписками. Требуется ручное сопоставление.")
    _validate_message_links(body, context)
    final_subject = _clean(subject, 300) or _clean((thread or {}).get("subject"), 300)
    if not final_subject:
        raise ValueError("Для первого письма укажите тему")
    has_attachment = bool(
        _clean(attachment_url, 4000)
        or _clean(attachment_type, 200)
        or _clean(attachment_name, 500)
        or [item for item in (attachments or []) if isinstance(item, dict)]
    )
    first_outgoing = thread is None
    guidelines_required = thread is None
    if thread is not None:
        db = await _connect()
        try:
            outgoing_state = await (await db.execute(
                """SELECT
                     EXISTS(SELECT 1 FROM email_messages WHERE thread_id=? AND direction='outgoing') AS has_any,
                     EXISTS(SELECT 1 FROM email_messages WHERE thread_id=? AND direction='outgoing'
                            AND status IN ('accepted','sent','delivered')) AS has_success""",
                (thread["id"], thread["id"]),
            )).fetchone()
            guidelines_required = not bool(outgoing_state["has_any"])
            first_outgoing = not bool(outgoing_state["has_success"])
        finally:
            await db.close()
    if first_outgoing and len(_message_urls(body)) > 1:
        raise EmailGuardError(
            "email_first_message_too_many_links",
            "В первом письме разрешена одна основная ссылка. Уберите лишние ссылки и отправьте снова.",
            details={"links": len(_message_urls(body)), "maximum": 1},
        )
    if has_attachment:
        if first_outgoing:
            raise EmailGuardError(
                "email_first_message_attachment_not_allowed",
                "Первое письмо отправляется без файлов и архивов. Сначала установите контакт обычным текстовым письмом.",
            )
        # DashaMail attachments are intentionally not accepted until storage,
        # MIME type and provider delivery are verified end-to-end.
        raise EmailGuardError(
            "email_attachment_not_supported",
            "Email-вложения пока не поддерживаются безопасно. Отправьте письмо без файла.",
        )
    # Fail closed at the shared service boundary. A versioned acknowledgement
    # prevents old browser tabs from silently accepting a changed checklist.
    if guidelines_required and (
        email_guidelines_confirmed is not True
        or _clean(email_guidelines_version, 40) != EMAIL_GUIDELINES_VERSION
    ):
        raise EmailGuardError(
            "email_guidelines_confirmation_required",
            "Перед отправкой подтвердите рекомендации для хорошей доставляемости.",
            status_code=409,
            confirmation_required=True,
        )
    if thread is None:
        thread = await _create_thread(context, recipient, _clean(context.get("name"), 200), final_subject)
    sender_name = _clean(from_name or manager_name, 200)
    from_address = await _sender_for_message(settings, manager_id, sender_name)
    raw_signature_url = _clean(signature_url, 4000)
    personalized_signature_url = _safe_signature_url(raw_signature_url) if raw_signature_url else DEFAULT_SIGNATURE_URL
    if raw_signature_url and not personalized_signature_url:
        raise ValueError("Некорректная ссылка сайта в подписи Email")
    _validate_message_links(personalized_signature_url, context)
    rendered_html = _render_email_html(
        body, sender_name, _visible_unsubscribe_url(recipient), personalized_signature_url,
    )
    now, nexus_id = _iso(), secrets.token_urlsafe(24)
    db = await _connect()
    try:
        cursor = await db.execute(
            """INSERT INTO email_messages(thread_id,nexus_message_id,direction,status,from_email,to_email,subject,text_body,html_body,
               manager_id,manager_name,created_at,updated_at) VALUES(?,?,'outgoing','queued',?,?,?,?,?,?,?,?,?)""",
            (thread["id"], nexus_id, from_address, recipient, final_subject, body, rendered_html,
             _clean(manager_id, 200), sender_name, now, now),
        )
        await db.execute(
            "INSERT INTO outbound_jobs(idempotency_key,message_id,status,created_at,updated_at) VALUES(?,?,'pending',?,?)",
            (request_key, cursor.lastrowid, now, now),
        )
        await db.execute("UPDATE email_threads SET subject=?,updated_at=? WHERE id=?", (final_subject, now, thread["id"]))
        await db.commit()
    finally:
        await db.close()
    _wakeup.set()
    return {"ok": True, "queued": True, "status": "queued", "nexus_message_id": nexus_id,
            "thread_id": thread["public_token"], "subject": final_subject, "from_email": from_address}


class _Retryable(Exception):
    def __init__(self, message: str, retry_after: int = 0):
        super().__init__(message)
        self.retry_after = retry_after


class _Ambiguous(Exception):
    pass


async def _claim_job() -> dict[str, Any] | None:
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        row = await (await db.execute(
            """SELECT j.*,m.nexus_message_id,m.from_email,m.to_email,m.subject,m.text_body,m.html_body,m.manager_name,
                      t.public_token,t.last_rfc_message_id FROM outbound_jobs j
               JOIN email_messages m ON m.id=j.message_id JOIN email_threads t ON t.id=m.thread_id
               WHERE j.status IN ('pending','retry') AND (j.next_attempt_at='' OR j.next_attempt_at<=?) ORDER BY j.id LIMIT 1""",
            (_iso(),),
        )).fetchone()
        if not row:
            await db.rollback()
            return None
        now = _iso()
        cursor = await db.execute(
            "UPDATE outbound_jobs SET status='processing',attempts=attempts+1,claimed_at=?,updated_at=? WHERE id=? AND status IN ('pending','retry')",
            (now, now, row["id"]),
        )
        if cursor.rowcount != 1:
            await db.rollback()
            return None
        await db.commit()
        result = dict(row); result["attempts"] = int(result["attempts"]) + 1
        return result
    finally:
        await db.close()


def _safe_link(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return parsed.scheme.casefold() in {"http", "https"} and bool(parsed.netloc)


def _email_text_html(value: Any) -> str:
    source, chunks, cursor = _clean(value, MAX_MESSAGE_CHARS), [], 0
    for match in URL_RE.finditer(source):
        raw = match.group(0)
        suffix = ""
        while raw and raw[-1] in ".,;:!?)]}":
            suffix = raw[-1] + suffix
            raw = raw[:-1]
        chunks.append(html.escape(source[cursor:match.start()]))
        if raw and _safe_link(raw):
            escaped = html.escape(raw, quote=True)
            chunks.append(
                f'<a href="{escaped}" style="color:#2787cf;text-decoration:underline;word-break:break-word">{escaped}</a>'
            )
        else:
            chunks.append(html.escape(raw))
        chunks.append(html.escape(suffix))
        cursor = match.end()
    chunks.append(html.escape(source[cursor:]))
    return "".join(chunks).replace("\n", "<br>")


def _safe_signature_url(value: Any) -> str:
    candidate = _clean(value, 4000)
    if not candidate:
        return ""
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return ""
    host = (parsed.hostname or "").casefold().rstrip(".")
    if parsed.scheme.casefold() != "https" or not (
        host == SIGNATURE_LINK_DOMAIN or host.endswith(f".{SIGNATURE_LINK_DOMAIN}")
    ):
        return ""
    return candidate


def _signature_url_from_html(value: Any) -> str:
    match = re.search(
        r'data-nexus-signature-link="1"\s+href="([^"]+)"',
        _clean(value, MAX_MESSAGE_CHARS * 2),
    )
    return (_safe_signature_url(html.unescape(match.group(1))) if match else "") or DEFAULT_SIGNATURE_URL


def _signature_text(manager_name: Any, signature_url: Any = DEFAULT_SIGNATURE_URL) -> str:
    name = _clean(manager_name, 200) or "Служба заботы"
    safe_url = _safe_signature_url(signature_url) or DEFAULT_SIGNATURE_URL
    return f"{name}\nСлужба заботы клуба «Современный собаковод»\n{safe_url}"


def _render_plain_text(
    body: Any, manager_name: Any, unsubscribe_url: str,
    signature_url: Any = DEFAULT_SIGNATURE_URL,
) -> str:
    parts = [_clean(body, MAX_MESSAGE_CHARS), _signature_text(manager_name, signature_url)]
    if unsubscribe_url:
        parts.append(f"Не хотите получать письма от нашей команды? Отписаться от писем: {unsubscribe_url}")
    return "\n\n".join(part for part in parts if part)


def _render_email_html(
    body: Any, manager_name: Any, unsubscribe_url: str,
    signature_url: Any = DEFAULT_SIGNATURE_URL,
) -> str:
    name = html.escape(_clean(manager_name, 200) or "Служба заботы")
    safe_signature_url = html.escape(_safe_signature_url(signature_url) or DEFAULT_SIGNATURE_URL, quote=True)
    unsubscribe = (
        '<a href="{}" style="color:#68747b;text-decoration:underline">Отписаться от писем</a>'
        .format(html.escape(unsubscribe_url, quote=True))
        if unsubscribe_url else ""
    )
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f6f8fa;color:#313131;font-family:Arial,Helvetica,sans-serif">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f6f8fa">
<tr><td align="center" style="padding:24px 12px">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="max-width:640px;background:#ffffff;border:1px solid #d6edff">
<tr><td style="height:6px;background:#5aacec;font-size:0;line-height:0">&nbsp;</td></tr>
<tr><td style="padding:24px 28px 16px">
<div style="font-size:13px;line-height:18px;letter-spacing:.04em;color:#2787cf;font-weight:700;text-transform:uppercase">Современный собаковод</div>
<div style="margin-top:5px;font-size:13px;line-height:18px;color:#68747b">Клуб для владельцев собак</div>
</td></tr>
<tr><td style="padding:8px 28px 26px;font-size:16px;line-height:25px;color:#313131">{_email_text_html(body)}</td></tr>
<tr><td style="padding:20px 28px;background:#fbfaf8;border-top:1px solid #e6ecef;font-size:14px;line-height:21px;color:#4d4d4d">
<strong style="color:#313131">{name}</strong><br>Служба заботы клуба «Современный собаковод»<br>
<a data-nexus-signature-link="1" href="{safe_signature_url}" style="color:#2787cf;text-decoration:none">sobakovod.pro</a>
</td></tr>
<tr><td style="padding:15px 28px 22px;font-size:11px;line-height:17px;color:#8da1ad">{unsubscribe}</td></tr>
</table>
</td></tr></table>
</body></html>"""


async def _submit(job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    # Final provider-boundary check: a recipient may unsubscribe or bounce
    # after the job was enqueued or while it waited for a retry.
    if reason := await _active_suppression(_email(job.get("to_email"))):
        raise _suppression_error(reason)
    settings = await _settings()
    reply_to = f"case+{job['public_token']}@{settings['reply_domain']}"
    unsubscribe_url = _visible_unsubscribe_url(job["to_email"])
    one_click_url = _one_click_unsubscribe_url(job["to_email"])
    headers = {"Reply-To": formataddr(("Современный собаковод", reply_to), charset="utf-8"), "X-Dashamail-Variables": json.dumps(
        {"thread": job["public_token"], "message": job["nexus_message_id"]}, separators=(",", ":"))}
    if one_click_url:
        headers.update({
            "List-Unsubscribe": f"<{one_click_url}>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        })
    if job.get("last_rfc_message_id"):
        headers.update({"In-Reply-To": job["last_rfc_message_id"], "References": job["last_rfc_message_id"]})
    signature_url = _signature_url_from_html(job.get("html_body"))
    payload = {
        "method": "transactional.send", "api_key": os.environ[API_KEY_ENV], "to": job["to_email"],
        "from_email": job["from_email"],
        "from_name": f"{_first_name(job['manager_name'])} · Современный собаковод" if _first_name(job["manager_name"]) else "Современный собаковод",
        "subject": job["subject"],
        "message": job.get("html_body") or _render_email_html(
            job["text_body"], job["manager_name"], unsubscribe_url, signature_url,
        ),
        "plain_text": _render_plain_text(
            job["text_body"], job["manager_name"], unsubscribe_url, signature_url,
        ),
        "message_id": job["nexus_message_id"],
        "headers": json.dumps(headers, ensure_ascii=False),
        "no_track_opens": 0 if _bool(settings.get("track_opens")) else 1,
        "no_track_clicks": 0 if _bool(settings.get("track_clicks")) else 1,
    }
    timeout = max(5, min(int(settings.get("request_timeout_seconds") or 20), 60))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(_clean(os.environ.get(API_BASE_ENV), 1000) or DEFAULT_API_BASE, json=payload)
    try:
        data = response.json()
    except ValueError:
        data = {"raw": _clean(response.text, 2000)}
    if response.status_code == 429:
        retry = response.headers.get("Retry-After", "")
        raise _Retryable("DashaMail rate limit", int(retry) if retry.isdigit() else 60)
    if response.status_code >= 500:
        raise _Ambiguous(f"DashaMail HTTP {response.status_code}")
    if response.status_code >= 400:
        raise ValueError(_clean(data.get("error") if isinstance(data, dict) else data, 1000) or f"DashaMail HTTP {response.status_code}")
    transaction_id = _clean(data.get("transaction_id") if isinstance(data, dict) else "", 300)
    if not transaction_id and isinstance(data, dict):
        transaction_id = _clean(((data.get("response") or {}).get("data") or {}).get("transaction_id"), 300)
    response_block = data.get("response") if isinstance(data, dict) else None
    response_message = response_block.get("msg") if isinstance(response_block, dict) else None
    error_code = response_message.get("err_code") if isinstance(response_message, dict) else None
    if error_code not in (None, 0, "0"):
        raise ValueError(_clean(response_message.get("text"), 1000) or f"DashaMail API error {error_code}")
    if not transaction_id:
        raise ValueError("DashaMail не вернул идентификатор отправки")
    return transaction_id, data if isinstance(data, dict) else {"response": data}


async def _finish_job(job: dict[str, Any], status: str, *, provider_id: str = "", response: dict[str, Any] | None = None,
                      error: str = "", next_at: str = "") -> None:
    now = _iso()
    db = await _connect()
    try:
        cursor = await db.execute(
            """UPDATE outbound_jobs SET status=?,next_attempt_at=?,provider_response=?,error=?,updated_at=?
               WHERE id=? AND status='processing'""",
            (status, next_at, json.dumps(response or {}, ensure_ascii=False)[:20_000], error, now, job["id"]),
        )
        if cursor.rowcount != 1:
            # A suppression webhook may have cancelled the job while it was
            # being processed. Never overwrite that terminal decision.
            await db.commit()
            return
        message_status = "sent" if status == "accepted" else status
        await db.execute(
            """UPDATE email_messages SET status=?,provider_message_id=CASE WHEN ?<>'' THEN ? ELSE provider_message_id END,
               error=?,sent_at=CASE WHEN ?='accepted' THEN ? ELSE sent_at END,updated_at=? WHERE id=?""",
            (message_status, provider_id, provider_id, error, status, now, now, job["message_id"]),
        )
        await db.commit()
    finally:
        await db.close()


async def _process_job(job: dict[str, Any]) -> None:
    try:
        transaction_id, response = await _submit(job)
    except (httpx.ConnectError, _Retryable) as exc:
        settings, attempts = await _settings(), int(job["attempts"])
        dead = attempts >= max(1, min(int(settings.get("max_attempts") or 5), 10))
        delay = getattr(exc, "retry_after", 0) or min(3600, 30 * (2 ** max(0, attempts - 1)))
        await _finish_job(job, "failed" if dead else "retry", error=_clean(exc, 1000),
                          next_at="" if dead else _iso(_now() + timedelta(seconds=delay)))
    except (httpx.ReadTimeout, httpx.RemoteProtocolError, _Ambiguous) as exc:
        # DashaMail does not document an idempotency key. An ambiguous retry
        # can duplicate a client letter, therefore this state requires reconcile.
        await _finish_job(job, "unknown", error=f"Неизвестен результат отправки: {_clean(exc, 800)}")
    except Exception as exc:
        await _finish_job(job, "failed", error=_clean(exc, 1000))
    else:
        await _finish_job(job, "accepted", provider_id=transaction_id, response=response)


async def _delivery_loop() -> None:
    while True:
        try:
            job = await _claim_job()
            if job:
                await _process_job(job)
                continue
            _wakeup.clear()
            try:
                await asyncio.wait_for(_wakeup.wait(), timeout=3)
            except TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _log("exception", "email delivery loop failed")
            await asyncio.sleep(2)


def _event_status(event: str) -> tuple[str, bool, bool]:
    value = event.casefold()
    if value in {"open", "opened"}: return "delivered", True, False
    if value == "delivered": return "delivered", False, False
    # Complaints and unsubscribes affect future permission to send, not the
    # delivery result of a message which may already have been delivered.
    if value in {"hard", "bounced", "dropped"}: return "failed", False, False
    if value in {"send", "sent"}: return "sent", False, False
    return "", False, False


def _provider_suppression_reason(event: str, fields: dict[str, str]) -> str:
    """Suppress people, not failures of the provider's sending infrastructure."""
    value = event.casefold()
    if value in {"unsub", "unsubscribe", "unsubscribed"}:
        return "unsubscribe"
    if value in {"spam", "complained"}:
        return value
    if value == "hard":
        return "hard"
    category = _clean(
        fields.get("bounce_category") or fields.get("bounce_type") or fields.get("category"), 100,
    ).casefold()
    if value in {"bounced", "dropped"} and category == "hard":
        return "hard"
    if category in {"soft", "spam", "spam_blocked", "blk"}:
        return ""
    detail = " ".join((
        _clean(fields.get("reason"), 2000),
        _clean(fields.get("description"), 2000),
        _clean(fields.get("bounce_reason"), 2000),
        _clean(fields.get("bounce_code"), 200),
    ))
    if value in {"bounced", "dropped"} and (
        detail.strip().casefold() == "hard" or INVALID_RECIPIENT_RE.search(detail)
    ):
        return "hard"
    return ""


async def _store_provider_event(fields: dict[str, str]) -> bool:
    event = _clean(fields.get("event"), 80).casefold()
    nexus_id, provider_id = _clean(fields.get("message_id"), 300), _clean(fields.get("transaction_id"), 300)
    address = _email(fields.get("email"))
    canonical = json.dumps(fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    # Signatures and delivery timestamps may change on a provider retry. The
    # stable business identity keeps the webhook idempotent across retries.
    stable_event = {
        "event": event, "message_id": nexus_id, "transaction_id": provider_id,
        "email": _email(fields.get("email")), "reason": _clean(fields.get("reason"), 1000),
        "provider_event_id": _clean(fields.get("id"), 300),
    }
    event_hash, now = hashlib.sha256(json.dumps(stable_event, sort_keys=True, separators=(",", ":")).encode()).hexdigest(), _iso()
    status, opened, _ = _event_status(event)
    suppression_reason = _provider_suppression_reason(event, fields)
    db = await _connect()
    try:
        cursor = await db.execute(
            "INSERT OR IGNORE INTO provider_events(event_hash,provider_event_id,nexus_message_id,provider_message_id,event_type,payload_json,received_at) VALUES(?,?,?,?,?,?,?)",
            (event_hash, _clean(fields.get("id"), 300), nexus_id, provider_id, event or "unknown", canonical[:50_000], now),
        )
        inserted = cursor.rowcount > 0
        if inserted and nexus_id:
            row = await (await db.execute(
                "SELECT id,status,to_email FROM email_messages WHERE nexus_message_id=?", (nexus_id,),
            )).fetchone()
            if row and status:
                if not address:
                    address = _email(row["to_email"])
                current = _clean(row["status"], 40)
                ranks = {"queued": 0, "pending": 0, "processing": 0, "retry": 0, "sent": 1, "accepted": 1, "delivered": 2}
                next_status = status
                if current == "failed" or (status != "failed" and ranks.get(current, 0) > ranks.get(status, 0)):
                    next_status = current
                await db.execute(
                    """UPDATE email_messages SET status=?,opened=MAX(opened,?),
                       opened_at=CASE WHEN ?=1 THEN ? ELSE opened_at END,
                       delivered_at=CASE WHEN ?='delivered' AND delivered_at='' THEN ? ELSE delivered_at END,
                       error=CASE WHEN ?='failed' THEN ? ELSE error END,updated_at=? WHERE id=?""",
                    (next_status, int(opened), int(opened), now, next_status, now, next_status,
                     _clean(fields.get("reason") or fields.get("description"), 1000), now, row["id"]),
                )
                if next_status == "failed":
                    reason = _clean(fields.get("reason") or fields.get("description"), 1000)
                    await db.execute(
                        """UPDATE outbound_jobs SET status='failed',next_attempt_at='',error=?,updated_at=?
                           WHERE message_id=? AND status NOT IN ('failed','cancelled')""",
                        (reason, now, row["id"]),
                    )
        if inserted and suppression_reason and address:
            await db.execute(
                """INSERT INTO suppressions(email,reason,source_event_hash,active,created_at,updated_at) VALUES(?,?,?,1,?,?)
                   ON CONFLICT(email) DO UPDATE SET reason=excluded.reason,source_event_hash=excluded.source_event_hash,active=1,updated_at=excluded.updated_at""",
                (address, suppression_reason, event_hash, now, now),
            )
            await _cancel_queued_for_suppression(db, address, suppression_reason, now)
        await db.commit()
        return inserted
    finally:
        await db.close()


async def _bounded_body(request: Request, limit: int = MAX_WEBHOOK_BYTES) -> bytes:
    length = request.headers.get("content-length", "")
    if length.isdigit() and int(length) > limit:
        raise HTTPException(413, "payload too large")
    body = await request.body()
    if len(body) > limit:
        raise HTTPException(413, "payload too large")
    return body


def _form_fields(body: bytes) -> dict[str, str]:
    parsed = parse_qs(body.decode("utf-8", "replace"), keep_blank_values=True)
    return {key: _clean(values[-1], MAX_MESSAGE_CHARS) for key, values in parsed.items() if values}


async def _webhook_fields(request: Request) -> dict[str, str]:
    body = await _bounded_body(request)
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().casefold()
    if content_type == "multipart/form-data":
        try:
            form = await request.form()
        except Exception:
            raise HTTPException(400, "invalid multipart form") from None
        return {
            _clean(key, 200): _clean(value, MAX_MESSAGE_CHARS)
            for key, value in form.multi_items()
            if _clean(key, 200) and isinstance(value, str)
        }
    if content_type != "application/json":
        return _form_fields(body)
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        raise HTTPException(400, "invalid JSON") from None
    if not isinstance(payload, dict):
        raise HTTPException(400, "JSON object required")
    fields: dict[str, str] = {}
    for key, value in payload.items():
        name = _clean(key, 200)
        if not name:
            continue
        fields[name] = (
            json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            if isinstance(value, (dict, list)) else _clean(value, MAX_MESSAGE_CHARS)
        )
    return fields


@router.post("/webhooks/dashamail/events")
async def dashamail_events(request: Request) -> JSONResponse:
    fields = await _webhook_fields(request)
    key = os.environ.get(EVENT_KEY_ENV, "")
    # Email Transport callbacks use ``secret`` with MD5.  Dasha.Router is a
    # separate contract and uses ``signature`` with HMAC-SHA256.
    candidate = _clean(fields.get("secret"), 200).lower()
    expected = hashlib.md5((_clean(fields.get("email"), 500) + _clean(fields.get("message_id"), 300) + key).encode()).hexdigest() if key else ""
    if not expected or not candidate or not hmac.compare_digest(candidate, expected):
        _log(
            "warning",
            "DashaMail event rejected: content_type=%s fields=%s secret=%s email=%s message_id=%s key_length=%d",
            request.headers.get("content-type", "").split(";", 1)[0].strip().casefold(),
            ",".join(sorted(fields)), bool(candidate), bool(_clean(fields.get("email"), 500)),
            bool(_clean(fields.get("message_id"), 300)), len(key),
        )
        return JSONResponse({"ok": False}, status_code=401)
    return JSONResponse({"ok": True, "inserted": await _store_provider_event(fields)})


def _header_map(fields: dict[str, str]) -> dict[str, str]:
    try:
        raw = json.loads(fields.get("message-headers") or "{}")
    except (TypeError, ValueError):
        return {}
    if isinstance(raw, dict):
        return {_clean(k, 100).casefold(): _clean(v, 2000) for k, v in raw.items()}
    if isinstance(raw, list):
        return {_clean(row[0], 100).casefold(): _clean(row[1], 2000) for row in raw if isinstance(row, list) and len(row) >= 2}
    return {}


def _classify_inbound(fields: dict[str, str]) -> str:
    headers = _header_map(fields)
    auto, precedence = headers.get("auto-submitted", "").casefold(), headers.get("precedence", "").casefold()
    sender = _email(fields.get("sender") or fields.get("from"))
    if auto and auto != "no": return "auto_reply"
    if precedence in {"bulk", "list", "junk"} or headers.get("list-unsubscribe"): return "list_mail"
    if sender.startswith(("mailer-daemon@", "postmaster@")): return "bounce"
    return "reply"


async def _store_inbound(fields: dict[str, str]) -> tuple[bool, str, int | None]:
    recipient, sender = _email(fields.get("recipient")), _email(fields.get("sender") or fields.get("from"))
    headers = _header_map(fields)
    rfc_id = _clean(fields.get("Message-Id") or fields.get("message-id") or headers.get("message-id"), 500)
    token_match = TOKEN_RE.match(recipient); token = token_match.group(1) if token_match else ""
    canonical = json.dumps(fields, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    # Router retries may use a fresh timestamp/token/signature. Prefer the RFC
    # Message-ID; for malformed mail without one, hash only message content.
    content_identity = rfc_id or hashlib.sha256((
        _clean(fields.get("subject"), 300) + "\0" +
        _clean(fields.get("stripped-text") or fields.get("body-plain"), MAX_MESSAGE_CHARS) + "\0" +
        _clean(fields.get("stripped-html") or fields.get("body-html"), MAX_MESSAGE_CHARS)
    ).encode()).hexdigest()
    event_hash = hashlib.sha256((content_identity + "\0" + recipient + "\0" + sender).encode()).hexdigest()
    kind, now = _classify_inbound(fields), _iso()
    db = await _connect()
    try:
        thread = await (await db.execute("SELECT * FROM email_threads WHERE public_token=?", (token,))).fetchone() if token else None
        state = "matched" if thread else "unmatched"
        cursor = await db.execute(
            "INSERT OR IGNORE INTO inbound_events(event_hash,rfc_message_id,recipient,sender,event_type,status,thread_id,payload_json,received_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (event_hash, rfc_id, recipient, sender, kind, state, thread["id"] if thread else None, canonical[:100_000], now),
        )
        inserted, message_id = cursor.rowcount > 0, None
        notify_context: dict[str, str] = {}
        if inserted and thread and kind == "reply":
            message = await db.execute(
                """INSERT INTO email_messages(thread_id,nexus_message_id,rfc_message_id,direction,status,from_email,to_email,subject,
                   text_body,html_body,in_reply_to,references_json,created_at,updated_at)
                   VALUES(?,?,?,'incoming','delivered',?,?,?,?,?,?,?,?,?)""",
                (thread["id"], f"in:{event_hash[:32]}", rfc_id or None, sender, recipient,
                 _clean(fields.get("subject"), 300) or thread["subject"],
                 _clean(fields.get("stripped-text") or fields.get("body-plain"), MAX_MESSAGE_CHARS),
                 _clean(fields.get("stripped-html") or fields.get("body-html"), MAX_MESSAGE_CHARS),
                 _clean(headers.get("in-reply-to"), 500), json.dumps(headers.get("references", "").split()), now, now),
            )
            message_id = message.lastrowid
            await db.execute("UPDATE email_threads SET last_rfc_message_id=?,updated_at=? WHERE id=?", (rfc_id, now, thread["id"]))
            await db.execute("UPDATE email_messages SET replied=1,updated_at=? WHERE thread_id=? AND direction='outgoing'", (now, thread["id"]))
            link = await (await db.execute(
                """SELECT platform,entity_type,entity_id FROM email_thread_links
                   WHERE thread_id=? AND platform='amocrm' AND entity_type='lead' ORDER BY updated_at DESC LIMIT 1""",
                (thread["id"],),
            )).fetchone()
            if link:
                notify_context = dict(link)
        if inserted:
            await db.execute("UPDATE inbound_events SET message_id=?,processed_at=? WHERE event_hash=?", (message_id, now, event_hash))
        await db.commit()
        settings = await _settings()
        if inserted and message_id and notify_context and settings.get("inbound_task_mode") == "enabled":
            messenger = sys.modules.get("_nexus_mod_messenger-widget")
            callback = getattr(messenger, "service_email_inbound", None) if messenger else None
            if callback:
                try:
                    await callback(
                        external_id=f"email:{event_hash}", thread_token=thread["public_token"],
                        client_name=thread["client_name"] or sender,
                        text=_clean(fields.get("stripped-text") or fields.get("body-plain"), 3500),
                        sent_at=now, context=notify_context,
                    )
                except Exception as exc:
                    _log("warning", "Messenger email notification deferred: %s", type(exc).__name__)
        return inserted, state if kind == "reply" else kind, message_id
    finally:
        await db.close()


@router.post("/webhooks/dashamail/inbound")
async def dashamail_inbound(request: Request) -> JSONResponse:
    fields = await _webhook_fields(request)
    timestamp, token = _clean(fields.get("timestamp"), 30), _clean(fields.get("token"), 300)
    candidate = _clean(fields.get("signature"), 200).lower()
    try:
        stamp = int(timestamp)
    except ValueError:
        return JSONResponse({"ok": False, "error": "invalid timestamp"}, status_code=400)
    if abs(int(_now().timestamp()) - stamp) > ROUTER_CLOCK_SKEW_SECONDS:
        return JSONResponse({"ok": False, "error": "stale webhook"}, status_code=401)
    keys = [os.environ.get(ROUTER_KEY_ENV, ""), os.environ.get(ROUTER_PREVIOUS_KEY_ENV, "")]
    valid = any(key and hmac.compare_digest(candidate, hmac.new(key.encode(), (timestamp + token).encode(), hashlib.sha256).hexdigest()) for key in keys)
    if not valid:
        return JSONResponse({"ok": False}, status_code=401)
    inserted, state, message_id = await _store_inbound(fields)
    return JSONResponse({"ok": True, "inserted": inserted, "status": state, "message_id": message_id})


def _unsubscribe_address(request: Request) -> str:
    return _unsubscribe_email(request.query_params.get("e"), request.query_params.get("s"))


def _mask_email(address: str) -> str:
    local, _, domain = address.partition("@")
    visible = local[:2] if len(local) > 2 else local[:1]
    return f"{visible}{'•' * max(3, min(8, len(local) - len(visible)))}@{domain}"


def _unsubscribe_page(*, address: str = "", complete: bool = False) -> HTMLResponse:
    title = "Вы отписаны" if complete else "Подтвердите отписку"
    body = (
        "Новые ручные письма сотрудников Nexus на этот адрес больше не отправятся. "
        "Письма о покупках, доступах и восстановлении аккаунта GetCourse продолжат приходить."
        if complete else
        f"Мы прекратим ручные письма сотрудников на адрес {html.escape(_mask_email(address))}. "
        "Системные письма GetCourse это не отключит."
    )
    action = "" if complete else """
<form method="post" onsubmit="const b=this.querySelector('button');b.disabled=true;b.innerHTML='<i></i>Отписываем…'">
<button type="submit">Отписаться от писем</button></form>"""
    response = HTMLResponse(f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow">
<title>{title} · Современный собаковод</title><style>
*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;display:grid;place-items:center;padding:20px;background:#f6f8fa;color:#313131;font:16px/1.55 Arial,sans-serif}}
main{{width:min(100%,560px);background:#fff;border:1px solid #d6edff;border-top:6px solid #5aacec;padding:30px}}
.brand{{color:#2787cf;font-size:13px;font-weight:700;letter-spacing:.04em;text-transform:uppercase}}h1{{font-size:25px;line-height:1.2;margin:18px 0 10px}}p{{margin:0 0 22px;color:#4d4d4d}}button{{min-height:42px;padding:0 18px;border:0;background:#313131;color:#fff;font-weight:700;cursor:pointer}}button:disabled{{opacity:.65;cursor:wait}}button i{{display:inline-block;width:15px;height:15px;margin-right:9px;vertical-align:-3px;border:2px solid #888;border-top-color:#fff;border-radius:50%;animation:spin .75s linear infinite}}@keyframes spin{{to{{transform:rotate(360deg)}}}}
a{{color:#2787cf}}</style></head><body><main><div class="brand">Современный собаковод</div><h1>{title}</h1><p>{body}</p>{action}<p style="margin-top:24px;font-size:13px"><a href="https://sobakovod.pro/">Перейти на sobakovod.pro</a></p></main></body></html>""")
    response.headers.update({"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Robots-Tag": "noindex"})
    return response


async def _suppress_unsubscribed(address: str) -> None:
    now = _iso()
    source_hash = hashlib.sha256(f"unsubscribe:{address}".encode()).hexdigest()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO suppressions(email,reason,source_event_hash,active,created_at,updated_at)
               VALUES(?,'unsubscribe',?,1,?,?) ON CONFLICT(email) DO UPDATE SET
               reason='unsubscribe',source_event_hash=excluded.source_event_hash,active=1,updated_at=excluded.updated_at""",
            (address, source_hash, now, now),
        )
        await _cancel_queued_for_suppression(db, address, "unsubscribe", now)
        await db.commit()
    finally:
        await db.close()


@router.get("/unsubscribe")
async def unsubscribe_confirmation(request: Request) -> HTMLResponse:
    address = _unsubscribe_address(request)
    if not address:
        raise HTTPException(404, "Ссылка недействительна")
    return _unsubscribe_page(address=address)


@router.post("/unsubscribe")
async def unsubscribe_submit(request: Request) -> HTMLResponse:
    enforce_rate_limit(request, "email-unsubscribe", limit=30, window_seconds=3600)
    address = _unsubscribe_address(request)
    if not address:
        raise HTTPException(404, "Ссылка недействительна")
    await _suppress_unsubscribed(address)
    return _unsubscribe_page(address=address, complete=True)


@router.post("/unsubscribe/one-click")
async def unsubscribe_one_click(request: Request) -> Response:
    enforce_rate_limit(request, "email-unsubscribe-one-click", limit=60, window_seconds=3600)
    address = _unsubscribe_address(request)
    if not address:
        raise HTTPException(404, "Ссылка недействительна")
    body = await _bounded_body(request, 4096)
    if b"List-Unsubscribe=One-Click" not in body:
        raise HTTPException(400, "Некорректный запрос отписки")
    await _suppress_unsubscribed(address)
    return Response(status_code=204, headers={"Cache-Control": "no-store"})


@router.get("/health")
async def health() -> dict[str, Any]:
    settings = await _settings()
    return {"ok": True, "enabled": _bool(settings["enabled"]), "configured": bool(os.environ.get(API_KEY_ENV)), "provider": "dashamail"}


@router.get("/status")
async def status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings(); db = await _connect()
    try:
        counts = await (await db.execute("""SELECT
          (SELECT COUNT(*) FROM outbound_jobs WHERE status IN ('pending','processing','retry')) queued,
          (SELECT COUNT(*) FROM outbound_jobs WHERE status IN ('failed','unknown')) problems,
          (SELECT COUNT(*) FROM email_threads) threads,
          (SELECT COUNT(*) FROM inbound_events WHERE status='unmatched') unmatched,
          (SELECT COUNT(*) FROM suppressions WHERE active=1) suppressed""")).fetchone()
    finally:
        await db.close()
    return {"ok": True, "enabled": _bool(settings["enabled"]), "pilot_mode": _bool(settings["pilot_mode"]),
            "from_email": settings["from_email"], "reply_domain": settings["reply_domain"],
            "inbound_task_mode": settings["inbound_task_mode"], "credentials": {
                "api_key": bool(os.environ.get(API_KEY_ENV)), "event_webhook_key": bool(os.environ.get(EVENT_KEY_ENV)),
                "router_signing_key": bool(os.environ.get(ROUTER_KEY_ENV))}, "counts": dict(counts)}


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, "settings": await _settings()}


@router.put("/settings")
async def put_settings(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(
        request,
        "email-channel-settings",
        limit=60,
        window_seconds=3600,
        subject=_clean(user.get("username"), 200),
    )
    data = await request.json()
    allowed = {"enabled", "pilot_mode", "from_email", "reply_domain", "inbound_task_mode", "track_opens", "track_clicks"}
    values: dict[str, str] = {}
    for key in allowed:
        if key in data:
            values[key] = "1" if key in {"enabled", "pilot_mode", "track_opens", "track_clicks"} and _bool(data[key]) else (
                "0" if key in {"enabled", "pilot_mode", "track_opens", "track_clicks"} else _clean(data[key], 500))
    if "from_email" in values and not _email(values["from_email"]): raise HTTPException(400, "Некорректный адрес отправителя")
    if values.get("inbound_task_mode") not in {None, "shadow", "enabled", "disabled"}: raise HTTPException(400, "Некорректный режим задач")
    if "reply_domain" in values and not re.fullmatch(r"[A-Za-z0-9.-]+", values["reply_domain"]): raise HTTPException(400, "Некорректный домен ответов")
    db = await _connect()
    try:
        now = _iso()
        for key, value in values.items():
            await db.execute("INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", (key, value, now))
        await db.commit()
    finally:
        await db.close()
    return {"ok": True, "settings": await _settings()}


@router.get("/senders")
async def list_senders(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    return {
        "ok": True,
        "domain": _sender_domain(settings),
        "fallback": _email(settings.get("from_email")),
        "senders": await _sync_sender_profiles(),
    }


@router.put("/senders/{manager_id}")
async def update_sender(manager_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    _ensure_local_staff_mutation_allowed()
    enforce_rate_limit(
        request, "email-sender-update", limit=100, window_seconds=3600,
        subject=_clean(user.get("username"), 200),
    )
    data = await request.json()
    profiles = await _sync_sender_profiles()
    current = next((row for row in profiles if row["manager_id"] == _clean(manager_id, 200)), None)
    if not current:
        raise HTTPException(404, "Сотрудник не найден")
    local_part = _clean(data.get("local_part", current["local_part"]), 100).casefold()
    if not LOCAL_PART_RE.fullmatch(local_part) or len(local_part) > 64 or local_part in RESERVED_LOCAL_PARTS:
        raise HTTPException(400, "Используйте латинские буквы, цифры, точку, дефис или подчёркивание")
    enabled = _bool(data.get("enabled", current["enabled"]))
    db = await _connect()
    try:
        await db.execute(
            "UPDATE sender_profiles SET local_part=?,enabled=?,updated_at=? WHERE manager_id=?",
            (local_part, int(enabled), _iso(), current["manager_id"]),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise HTTPException(409, "Такой адрес уже назначен другому сотруднику") from exc
    finally:
        await db.close()
    settings = await _settings()
    profile = next(row for row in await _sync_sender_profiles() if row["manager_id"] == current["manager_id"])
    return {"ok": True, "domain": _sender_domain(settings), "sender": profile}


@router.get("/threads")
async def list_threads(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    await _require_admin(request); db = await _connect()
    try:
        rows = await (await db.execute("""SELECT t.id,t.client_email,t.client_name,t.subject,t.state,t.updated_at,COUNT(m.id) messages
          FROM email_threads t LEFT JOIN email_messages m ON m.thread_id=t.id GROUP BY t.id ORDER BY t.updated_at DESC LIMIT ?""", (limit,))).fetchall()
    finally:
        await db.close()
    return {"ok": True, "threads": [dict(row) for row in rows]}
