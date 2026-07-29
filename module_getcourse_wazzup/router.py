from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from orchestrator.auth import (
    _read_env_values,
    _write_env_values,
    can_access_module,
    enforce_rate_limit,
    require_admin,
    verify_token_from_request,
)


router = APIRouter()

MODULE_ID = "getcourse-wazzup"
ENV_KEY = "NEXUS_GETCOURSE_WAZZUP_API_KEY"
WAZZUP_API = "https://api.wazzup24.com/v3"
WAZZUP_APP_API = "https://app.wazzup24.com/api/v2"
ALLOWED_ORIGIN = "https://club.sobakovod.pro"
DEVICE_TTL_DAYS = 180
ACTIVATION_TTL_MINUTES = 15
AUDIT_RETENTION_DAYS = 30
MAX_BODY_BYTES = 32 * 1024
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024
HISTORY_SYNC_TTL_MINUTES = 10
HISTORY_PAGE_SIZE = 50
HISTORY_MAX_CHATS = 500
HISTORY_MAX_MESSAGES = 500
CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
CHAT_TRANSPORTS = {
    "max", "maxgroup", "whatsapp", "whatsgroup", "telegram", "telegroup",
    "vk", "viber", "instagram", "avito", "cian",
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

_db_path: Path | None = None
_logger = None


async def setup(ctx) -> None:
    global _db_path, _logger
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    await _cleanup()


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("getcourse-wazzup is not initialized")
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
    return _clean(os.environ.get(ENV_KEY) or _read_env_values().get(ENV_KEY), 4000)


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
    if origin != ALLOWED_ORIGIN:
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
    if request.headers.get("origin", "") == ALLOWED_ORIGIN:
        return "getcourse"
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
            CREATE INDEX IF NOT EXISTS ix_gcw_devices_token ON devices(token_hash);
            CREATE INDEX IF NOT EXISTS ix_gcw_codes_hash ON activation_codes(code_hash);
            CREATE INDEX IF NOT EXISTS ix_gcw_events_created ON events(created_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_contacts_synced ON contacts(synced_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_chats_phone ON wazzup_chats(phone_hash,updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_messages_chat ON wazzup_messages(channel_id,chat_type,chat_id,sent_at,id);
            CREATE INDEX IF NOT EXISTS ix_gcw_messages_phone ON wazzup_messages(phone_hash,sent_at,id);
            CREATE INDEX IF NOT EXISTS ix_gcw_webhook_received ON webhook_events(received_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_gcw_history_sync_attempt ON history_sync_state(last_attempt_at DESC);
            """
        )
        columns = {row[1] for row in await (await db.execute("PRAGMA table_info(admins)")).fetchall()}
        if "phone" not in columns:
            await db.execute("ALTER TABLE admins ADD COLUMN phone TEXT NOT NULL DEFAULT ''")
        now = _iso()
        await db.execute(
            "INSERT OR IGNORE INTO module_settings(key,value,updated_at) VALUES('webhook_secret',?,?)",
            (secrets.token_urlsafe(32), now),
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


async def _wazzup_request(method: str, path: str, payload: Any = None) -> Any:
    key = _api_key()
    if not key:
        raise HTTPException(503, "Wazzup API key не настроен")
    headers = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=False) as client:
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
        channels.append(
            {
                "channel_id": channel_id,
                "transport": transport,
                "channel_transport": channel_transport,
                "name": _clean(row.get("name"), 120) or transport.upper(),
                "plain_id": _clean(row.get("plainId"), 120),
            }
        )
    return channels


async def _setting(key: str) -> str:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT value FROM module_settings WHERE key=?", (key,))).fetchone()
        return _clean(row["value"], 1000) if row else ""
    finally:
        await db.close()


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


async def _ingest_webhook(payload: dict[str, Any]) -> dict[str, int]:
    messages, statuses = _webhook_messages(payload)
    now = _iso()
    db = await _connect()
    inserted = updated = 0
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
            content_uri = _clean(row.get("contentUri"), 4000)
            contact = row.get("contact") if isinstance(row.get("contact"), dict) else {}
            author_name = _clean(row.get("authorName") or row.get("contactName") or contact.get("name"), 200)
            contact_phone = _normalize_phone(contact.get("phone"))
            phone_hash = _phone_hash(contact_phone or chat_id)
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
        for row in statuses:
            external_id = _clean(row.get("messageId") or row.get("id"), 250)
            status = _clean(row.get("status"), 80)
            if external_id and status:
                cursor = await db.execute("UPDATE wazzup_messages SET status=? WHERE external_id=?", (status, external_id))
                updated += max(0, cursor.rowcount)
        await db.commit()
    finally:
        await db.close()
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
                """SELECT d.*,a.wazzup_user_id,a.name AS admin_name,a.enabled
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
    return {"ok": True, "module": MODULE_ID, "api_key_configured": bool(_api_key()), "admins": admins, "devices": devices, "contacts": contacts, "chats": chats, "messages": messages}


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {
        "ok": True,
        "api_key_configured": bool(_api_key()),
        "api_key_hint": f"задан, длина {len(_api_key())}" if _api_key() else "не задан",
        "allowed_origin": ALLOWED_ORIGIN,
        "activation_ttl_minutes": ACTIVATION_TTL_MINUTES,
        "device_ttl_days": DEVICE_TTL_DAYS,
    }


@router.put("/settings")
async def put_settings(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-settings", limit=20, window_seconds=3600, subject=user["username"])
    data = await _read_json(request)
    key = _clean(data.get("api_key"), 4000)
    if key:
        if len(key) < 20:
            raise HTTPException(400, "API key слишком короткий")
        values = _read_env_values()
        values[ENV_KEY] = key
        _write_env_values(values)
        os.environ[ENV_KEY] = key
        _log("info", "Wazzup API key updated by Nexus admin=%s", user.get("username"))
    return await get_settings(request)


@router.post("/settings/test")
async def test_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    channels = await _wazzup_request("GET", "/channels")
    rows = channels if isinstance(channels, list) else []
    active = sum(1 for row in rows if isinstance(row, dict) and _clean(row.get("state"), 40).lower() == "active")
    return {"ok": True, "channels": len(rows), "active_channels": active, "message": f"Wazzup доступен: {active} активных каналов из {len(rows)}"}


@router.get("/webhook/status")
async def webhook_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _wazzup_request("GET", "/webhooks")
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
    base = str(request.base_url).rstrip("/")
    static_url = f"{base}/{MODULE_ID}/static/widget.js?v=3.1.0"
    api_url = f"{base}/{MODULE_ID}/api/widget"
    code = f'<script src="{static_url}" data-nexus-wazzup-api="{api_url}" async></script>'
    return {"ok": True, "snippet": code, "static_url": static_url, "api_url": api_url}


@router.post("/admins/sync")
async def sync_admins(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    enforce_rate_limit(request, "getcourse-wazzup-sync", limit=20, window_seconds=3600, subject=user["username"])
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
    return {"ok": True, "synced": len(users)}


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
                   COALESCE(SUM(CASE WHEN d.revoked_at='' AND d.expires_at>? THEN 1 ELSE 0 END),0) AS active_devices
                   FROM admins a LEFT JOIN devices d ON d.admin_id=a.id GROUP BY a.id ORDER BY a.name""",
                (_iso(),),
            )
        ).fetchall()
    finally:
        await db.close()
    return {"ok": True, "admins": [{**dict(row), "enabled": bool(row["enabled"])} for row in rows]}


@router.patch("/admins/{admin_id}")
async def update_admin(admin_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await _read_json(request)
    if "enabled" not in data:
        raise HTTPException(400, "Нет изменений")
    db = await _connect()
    try:
        cur = await db.execute("UPDATE admins SET enabled=?,updated_at=? WHERE id=?", (1 if data["enabled"] else 0, _iso(), admin_id))
        if not cur.rowcount:
            raise HTTPException(404, "Администратор не найден")
        if not data["enabled"]:
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
    expires = _iso(_now_dt() + timedelta(minutes=ACTIVATION_TTL_MINUTES))
    db = await _connect()
    try:
        admin = await (await db.execute("SELECT id,name,enabled FROM admins WHERE id=?", (admin_id,))).fetchone()
        if not admin:
            raise HTTPException(404, "Администратор не найден")
        if not admin["enabled"]:
            raise HTTPException(409, "Администратор выключен")
        await db.execute("DELETE FROM activation_codes WHERE admin_id=? AND used_at=''", (admin_id,))
        await db.execute(
            "INSERT INTO activation_codes(admin_id,code_hash,expires_at,created_at) VALUES(?,?,?,?)",
            (admin_id, _hash(_normalize_code(code)), expires, _iso()),
        )
        await db.commit()
    finally:
        await db.close()
    return {"ok": True, "code": code, "expires_at": expires, "admin_name": admin["name"], "one_time": True}


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


@router.options("/widget/{path:path}")
async def widget_options(path: str, request: Request) -> Response:
    origin = request.headers.get("origin", "")
    if origin != ALLOWED_ORIGIN:
        return Response(status_code=403)
    return Response(status_code=204, headers=_cors_headers(origin))


@router.post("/widget/activate")
async def widget_activate(request: Request) -> JSONResponse:
    if not await _widget_request_mode(request):
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        enforce_rate_limit(request, "getcourse-wazzup-activate", limit=12, window_seconds=3600)
        data = await _read_json(request)
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
            if not row or row["used_at"] or row["expires_at"] <= now or not row["enabled"]:
                await db.rollback()
                return _widget_response(request, {"ok": False, "error": "Код недействителен или уже использован"}, 401)
            token = secrets.token_urlsafe(40)
            expires = _iso(_now_dt() + timedelta(days=DEVICE_TTL_DAYS))
            cur = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (row["admin_id"], _hash(token), f"••••{token[-4:]}", now, now, expires),
            )
            device_id = int(cur.lastrowid)
            await db.execute("UPDATE activation_codes SET used_at=? WHERE id=?", (now, row["id"]))
            await db.commit()
        finally:
            await db.close()
        await _audit("activate", "ok", admin_id=row["admin_id"], device_id=device_id)
        return _widget_response(
            request,
            {"ok": True, "device_token": token, "admin": {"id": row["admin_id"], "name": row["name"]}, "expires_at": expires},
        )
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception as exc:
        _log("exception", "GetCourse Wazzup activation failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось активировать устройство"}, 500)


@router.post("/widget/iframe-link")
async def widget_iframe_link(request: Request) -> JSONResponse:
    if not await _widget_request_mode(request):
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
        source_url = _clean(data.get("source_url"), 2000)
        page_kind, entity_id = _page_context(source_url)
        if not page_kind or not source_url.startswith(ALLOWED_ORIGIN + "/"):
            return _widget_response(request, {"ok": False, "error": "Откройте карточку пользователя или заказа GetCourse"}, 400)
        phone = _normalize_phone(data.get("phone"))
        name = _clean(data.get("name"), 200) or f"GetCourse {page_kind} #{entity_id}"
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
                    "Wazzup ещё не вернул ID этого чата. Начните диалог в окне Nexus; после первого сообщения кнопка откроет его и в Wazzup.",
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
    if not await _widget_request_mode(request):
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(request, "getcourse-wazzup-channels", limit=120, window_seconds=3600, subject=str(device["id"]))
        channels = _active_chat_channels(await _wazzup_request("GET", "/channels"))
        return _widget_response(request, {"ok": True, "channels": channels})
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup channel list failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось получить каналы Wazzup"}, 500)


async def _requested_channel(channel_id: str, transport: str) -> dict[str, str]:
    channels = _active_chat_channels(await _wazzup_request("GET", "/channels"))
    for channel in channels:
        if channel["channel_id"] == channel_id and channel["transport"] == transport:
            return channel
    raise HTTPException(409, "Канал Wazzup отключён или недоступен")


def _history_chat_candidate(
    rows: Any,
    channel_id: str,
    transport: str,
    phone: str,
) -> dict[str, str] | None:
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict) or _normalize_phone(row.get("userPhone")) != phone:
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
        if channel_match:
            return {
                "channel_id": channel_id,
                "chat_type": transport,
                "chat_id": chat_id,
                "contact_name": _clean(row.get("contactName"), 200),
            }
    return None


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
    text_value = _clean(row.get("text"), 20_000)
    if not text_value and _clean(row.get("filename"), 500):
        text_value = f"[Вложение: {_clean(row.get('filename'), 500)}]"
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
        "content_uri": _clean(row.get("contentUri"), 4000),
        "author_name": _clean(row.get("authorName") or row.get("displayAuthorName"), 200),
        "sent_at": _message_time(row.get("datetime") or row.get("dateTime") or row.get("timestampMsg")),
        "raw_json": json.dumps(row, ensure_ascii=False, separators=(",", ":"))[:50_000],
    }


async def _history_sync_due(channel_id: str, phone: str) -> bool:
    cutoff = _iso(_now_dt() - timedelta(minutes=HISTORY_SYNC_TTL_MINUTES))
    db = await _connect()
    try:
        row = await (
            await db.execute(
                "SELECT last_attempt_at FROM history_sync_state WHERE channel_id=? AND phone_hash=?",
                (channel_id, _phone_hash(phone)),
            )
        ).fetchone()
        return not row or _clean(row["last_attempt_at"], 80) < cutoff
    finally:
        await db.close()


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
    candidate: dict[str, str] | None = None
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        for offset in range(0, HISTORY_MAX_CHATS, HISTORY_PAGE_SIZE):
            response = await client.get(
                f"{WAZZUP_APP_API}/chats",
                headers=headers,
                params={"limit": HISTORY_PAGE_SIZE, "offset": offset, "filterChannels": ""},
            )
            if response.status_code != 200:
                raise RuntimeError(f"history chats HTTP {response.status_code}")
            body = response.json()
            rows = body.get("data") if isinstance(body, dict) else []
            candidate = _history_chat_candidate(rows, channel["channel_id"], channel["transport"], phone)
            if candidate or not isinstance(rows, list) or len(rows) < HISTORY_PAGE_SIZE:
                break
        if not candidate:
            await _record_history_sync(channel["channel_id"], phone, "not_found", 0, success=True)
            return {"status": "not_found", "imported": 0, "complete": False}
        messages: list[dict[str, Any]] = []
        complete = False
        for offset in range(0, HISTORY_MAX_MESSAGES, HISTORY_PAGE_SIZE):
            response = await client.get(
                f"{WAZZUP_APP_API}/messages",
                headers=headers,
                params={
                    "limit": HISTORY_PAGE_SIZE,
                    "offset": offset,
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
            messages.extend(row for row in page_rows if isinstance(row, dict))
            if len(page_rows) < HISTORY_PAGE_SIZE:
                complete = True
                break
        imported = await _store_history(candidate, phone, messages)
        await _record_history_sync(channel["channel_id"], phone, "imported", imported, success=True)
        return {"status": "imported", "imported": imported, "complete": complete}


async def _conversation_rows(
    channel_id: str,
    transport: str,
    phone: str,
    limit: int = 150,
) -> tuple[str, bool, list[dict[str, Any]]]:
    phone_hash = _phone_hash(phone)
    digits = phone[1:]
    db = await _connect()
    try:
        chat = await (
            await db.execute(
                """SELECT chat_id FROM wazzup_chats WHERE channel_id=? AND chat_type=?
                   AND (phone_hash=? OR chat_id=?) ORDER BY updated_at DESC,id DESC LIMIT 1""",
                (channel_id, transport, phone_hash, digits),
            )
        ).fetchone()
        chat_id = _clean(chat["chat_id"], 250) if chat else ""
        rows = await (
            await db.execute(
                """SELECT external_id,direction,status,text,content_uri,author_name,sent_at
                   FROM wazzup_messages WHERE channel_id=? AND chat_type=?
                   AND (chat_id=? OR phone_hash=?) ORDER BY sent_at DESC,id DESC LIMIT ?""",
                (channel_id, transport, chat_id, phone_hash, limit),
            )
        ).fetchall()
    finally:
        await db.close()
    return chat_id, bool(chat), [dict(row) for row in reversed(rows)]


@router.post("/widget/conversation")
async def widget_conversation(request: Request) -> JSONResponse:
    if not await _widget_request_mode(request):
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
        phone = _normalize_phone(data.get("phone"))
        channel_id = _clean(data.get("channel_id"), 200)
        transport = _clean(data.get("transport"), 40).lower()
        if not phone or not channel_id or transport not in CHAT_TRANSPORTS:
            raise HTTPException(400, "Не указан телефон или канал")
        channel = await _requested_channel(channel_id, transport)
        history: dict[str, Any] = await _history_sync_info(channel_id, phone)
        if await _history_sync_due(channel_id, phone):
            try:
                history = await _import_wazzup_history(device, channel, phone)
            except Exception:
                await _record_history_sync(channel_id, phone, "error", 0, success=False)
                history = {"status": "error", "imported": 0, "complete": False}
                _log("warning", "Wazzup history read failed channel=%s", channel_id)
        chat_id, has_chat, messages = await _conversation_rows(channel_id, transport, phone)
        return _widget_response(
            request,
            {
                "ok": True,
                "channel": channel,
                "chat_id": chat_id,
                "has_chat": has_chat,
                "phone": phone,
                "messages": messages,
                "history_complete": history.get("status") == "imported",
                "history_status": history.get("status", "pending"),
            },
        )
    except HTTPException as exc:
        return _widget_response(request, {"ok": False, "error": str(exc.detail)}, exc.status_code)
    except Exception:
        _log("exception", "GetCourse Wazzup conversation load failed")
        return _widget_response(request, {"ok": False, "error": "Не удалось загрузить переписку"}, 500)


@router.post("/widget/send")
async def widget_send(request: Request) -> JSONResponse:
    if not await _widget_request_mode(request):
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    device: dict[str, Any] | None = None
    phone = ""
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        enforce_rate_limit(request, "getcourse-wazzup-send", limit=120, window_seconds=3600, subject=str(device["id"]))
        data = await _read_json(request)
        phone = _normalize_phone(data.get("phone"))
        channel_id = _clean(data.get("channel_id"), 200)
        transport = _clean(data.get("transport"), 40).lower()
        message_text = _clean(data.get("text"), 4000)
        source_url = _clean(data.get("source_url"), 2000)
        page_kind, entity_id = _page_context(source_url)
        if not phone or not page_kind or not source_url.startswith(ALLOWED_ORIGIN + "/"):
            raise HTTPException(400, "Откройте карточку GetCourse с заполненным телефоном")
        if not message_text:
            raise HTTPException(400, "Введите сообщение")
        channel = await _requested_channel(channel_id, transport)
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
        elif transport in {"whatsapp", "viber"}:
            message_payload["chatId"] = phone[1:]
        elif transport in {"max", "telegram"}:
            message_payload["phone"] = phone[1:]
        else:
            raise HTTPException(
                409,
                "Для этого канала Wazzup нужен ID чата из входящего сообщения; по одному номеру начать диалог нельзя.",
            )
        result = await _wazzup_request(
            "POST",
            "/message",
            message_payload,
        )
        external_id = ""
        if isinstance(result, dict):
            external_id = _clean(result.get("messageId") or result.get("id"), 250)
            if not external_id and isinstance(result.get("data"), dict):
                external_id = _clean(result["data"].get("messageId") or result["data"].get("id"), 250)
        external_id = external_id or f"local-{crm_message_id}"
        response_chat_id = _clean(result.get("chatId"), 250) if isinstance(result, dict) else ""
        if not response_chat_id and isinstance(result, dict) and isinstance(result.get("data"), dict):
            response_chat_id = _clean(result["data"].get("chatId"), 250)
        chat_id = response_chat_id or chat_id or phone[1:]
        now = _iso()
        db = await _connect()
        try:
            await db.execute(
                """INSERT OR IGNORE INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,content_uri,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,?,?,?,?,'outgoing','accepted',?,'',?,?,?,?)""",
                (external_id, channel_id, transport, chat_id, _phone_hash(phone), message_text, device["admin_name"], now, "", now),
            )
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,last_message_preview,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,chat_type,chat_id) DO UPDATE SET
                   phone_hash=excluded.phone_hash,last_message_at=excluded.last_message_at,
                   last_message_preview=excluded.last_message_preview,updated_at=excluded.updated_at""",
                (channel_id, transport, chat_id, _phone_hash(phone), _clean(data.get("name"), 200), now, message_text[:500], now, now),
            )
            await db.commit()
        finally:
            await db.close()
        await _audit("send_message", "ok", admin_id=device["admin_id"], device_id=device["id"], page_kind=page_kind, entity_id=entity_id, phone=phone)
        return _widget_response(
            request,
            {
                "ok": True,
                "message": {"external_id": external_id, "direction": "outgoing", "status": "accepted", "text": message_text, "content_uri": "", "author_name": device["admin_name"], "sent_at": now},
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
    if not await _widget_request_mode(request):
        return _widget_response(request, {"ok": False, "error": "origin not allowed"}, 403)
    try:
        device = await _device(request)
        if not device:
            return _widget_response(request, {"ok": False, "error": "Требуется повторная активация", "reauth": True}, 401)
        data = await _read_json(request)
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
