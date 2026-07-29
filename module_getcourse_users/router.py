from __future__ import annotations

import asyncio
import logging
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request

from orchestrator.auth import _read_env_values, can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "getcourse-users"
TABLE_NAME = "getcourse_users"
UTC = timezone.utc

_db_path: Path | None = None
_logger: logging.Logger | None = None
_write_lock = asyncio.Lock()


def setup(ctx) -> None:
    global _db_path, _logger
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-users"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())
    _log("info", "GetCourse users receiver ready")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("module context is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    if value is None:
        return ""
    return str(value).strip()[:limit]


async def _init_db() -> None:
    path = _must_db()
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(path, timeout=30) as db:
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at     TEXT NOT NULL DEFAULT '',
                gc_user_id      TEXT NOT NULL DEFAULT '',
                result          TEXT NOT NULL DEFAULT '',
                customer_db_id  INTEGER,
                fields_received INTEGER NOT NULL DEFAULT 0,
                error           TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_gc_user_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_gc_user_events_user ON events(gc_user_id);
            """
        )
        cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
        row = await cur.fetchone()
        if not row or not _clean(row[0]):
            await db.execute(
                "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES('webhook_secret',?,?)",
                (secrets.token_urlsafe(32), _now()),
            )
        await db.commit()


async def _setting(key: str) -> str:
    await _init_db()
    async with aiosqlite.connect(_must_db(), timeout=20) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
    return _clean(row[0] if row else "", 4000)


async def _require_panel_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _customer_db_token() -> str:
    token = os.environ.get("NEXUS_CUSTOMER_DB_API_TOKEN", "").strip()
    if token:
        return token
    try:
        values = _read_env_values()
    except Exception:
        values = {}
    token = _clean(values.get("NEXUS_CUSTOMER_DB_API_TOKEN"), 1000)
    if token:
        os.environ["NEXUS_CUSTOMER_DB_API_TOKEN"] = token
    return token


def _customer_db_url() -> str:
    base = os.environ.get("NEXUS_INTERNAL_BASE", "http://127.0.0.1:8080").rstrip("/")
    return f"{base}/customer-db/api/tables/{TABLE_NAME}/records/upsert"


async def _payload(request: Request) -> dict[str, Any]:
    data: dict[str, Any] = dict(request.query_params)
    if request.method == "GET":
        return data
    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(400, "Некорректный JSON") from exc
        if not isinstance(body, dict):
            raise HTTPException(400, "JSON должен быть объектом")
        data.update(body)
        return data
    try:
        form = await request.form()
        data.update({str(key): value for key, value in form.multi_items()})
    except Exception as exc:
        raise HTTPException(400, "Некорректное тело callback") from exc
    return data


def _first(data: dict[str, Any], *keys: str, limit: int = 2000) -> str:
    for key in keys:
        value = _clean(data.get(key), limit)
        if value:
            return value
    return ""


FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "email": ("email", "user_email"),
    "phone": ("phone", "user_phone"),
    "first_name": ("first_name", "firstname"),
    "last_name": ("last_name", "lastname", "second_name"),
    "name": ("name", "full_name"),
    "city": ("city",),
    "created_at": ("created_at", "created", "registration_at"),
    "registration_type": ("registration_type", "register_type"),
    "source": ("source", "registration_source"),
    "utm_source": ("utm_source",),
    "utm_medium": ("utm_medium",),
    "utm_campaign": ("utm_campaign",),
    "utm_content": ("utm_content",),
    "utm_term": ("utm_term",),
    "utm_group": ("utm_group",),
    "salebot_id": ("salebot_id", "sb_id", "salebot_user_id"),
    "vka_id": ("vka_id", "platform_id_vk"),
    "vk_id": ("vk_id", "vk-id", "vkid"),
    "telegram_id": ("telegram_id", "tg_id"),
    "ym_uid": ("ym_uid", "ym_user_id"),
    "yclid": ("yclid",),
}


def _normalize_user(data: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    gc_user_id = _first(data, "gc_user_id", "user_id", "id", "object_id", limit=120)
    if not gc_user_id:
        raise ValueError("gc_user_id обязателен")
    fields: dict[str, Any] = {"gc_user_id": gc_user_id}
    for target, aliases in FIELD_ALIASES.items():
        value = _first(data, *aliases)
        if value:
            fields[target] = value
    if fields.get("vka_id"):
        fields["vk_platform_id"] = fields["vka_id"]
    if not fields.get("name"):
        name = " ".join(part for part in (fields.get("first_name", ""), fields.get("last_name", "")) if part).strip()
        if name:
            fields["name"] = name
    fields["webhook_received_at"] = _now()
    fields["sync_source"] = "getcourse_user_process"
    return gc_user_id, fields


async def _record_event(
    gc_user_id: str,
    result: str,
    *,
    customer_db_id: int | None = None,
    fields_received: int = 0,
    error: str = "",
) -> None:
    async with aiosqlite.connect(_must_db(), timeout=20) as db:
        await db.execute(
            "INSERT INTO events(received_at,gc_user_id,result,customer_db_id,fields_received,error) VALUES(?,?,?,?,?,?)",
            (_now(), _clean(gc_user_id, 120), _clean(result, 40), customer_db_id, fields_received, _clean(error, 1000)),
        )
        await db.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 10000)")
        await db.commit()


async def _upsert_user(gc_user_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    token = _customer_db_token()
    if not token:
        raise RuntimeError("NEXUS_CUSTOMER_DB_API_TOKEN не настроен")
    async with httpx.AsyncClient(timeout=8) as client:
        response = await client.post(
            _customer_db_url(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"platform_id": gc_user_id, "custom_fields": fields},
        )
    if response.status_code >= 400:
        raise RuntimeError(f"Customer DB HTTP {response.status_code}")
    try:
        body = response.json()
    except Exception as exc:
        raise RuntimeError("Customer DB вернула некорректный JSON") from exc
    if not body.get("ok"):
        raise RuntimeError(_clean(body.get("reason") or body.get("error") or "Customer DB отклонила запись", 1000))
    return body


async def _receive(request: Request) -> dict[str, Any]:
    expected = await _setting("webhook_secret")
    candidate = _clean(request.query_params.get("secret"), 200)
    if not expected or not candidate or not secrets.compare_digest(candidate, expected):
        raise HTTPException(404, "not found")
    data = await _payload(request)
    gc_user_id = _first(data, "gc_user_id", "user_id", "id", "object_id", limit=120)
    try:
        gc_user_id, fields = _normalize_user(data)
        async with _write_lock:
            stored = await _upsert_user(gc_user_id, fields)
        result = _clean(stored.get("status") or "updated", 40)
        await _record_event(
            gc_user_id,
            result,
            customer_db_id=int(stored.get("id") or 0) or None,
            fields_received=len(fields),
        )
        _log("info", "GetCourse user %s %s", gc_user_id, result)
        return {"ok": True, "stored": True, "gc_user_id": gc_user_id, "status": result}
    except ValueError as exc:
        await _record_event(gc_user_id, "rejected", error=str(exc))
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        await _record_event(gc_user_id, "error", error=str(exc))
        _log("warning", "GetCourse user callback failed for %s: %s", gc_user_id or "missing", exc)
        raise HTTPException(503, "Nexus временно не сохранил пользователя; повторите callback") from exc


def _callback_body() -> str:
    return "&".join(
        (
            "gc_user_id={object.id}",
            "email={object.email}",
            "phone={object.phone}",
            "first_name={object.first_name}",
            "last_name={object.last_name}",
            "city={object.city}",
            "created_at={object.created_at}",
            "utm_source={object.create_session.utm_source}",
            "utm_medium={object.create_session.utm_medium}",
            "utm_campaign={object.create_session.utm_campaign}",
            "utm_content={object.create_session.utm_content}",
            "utm_term={object.create_session.utm_term}",
            "utm_group={object.create_session.utm_group}",
            "salebot_id={object.sb_id}",
            "vka_id={object.vka_id}",
            "vk_id={object.VK-ID}",
            "ym_uid={object.ym_uid}",
            "yclid={object.yclid}",
        )
    )


def _callback_urls(request: Request, secret: str) -> tuple[str, str]:
    root_path = _clean(request.scope.get("root_path"), 500).rstrip("/")
    relative = f"{root_path}/{MODULE_ID}/api/webhook?secret={secret}&{_callback_body()}"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or "junior.sobakovod.pro"
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme or "https"
    return relative, f"{scheme}://{host}{relative}"


async def _stats() -> dict[str, Any]:
    await _init_db()
    async with aiosqlite.connect(_must_db(), timeout=20) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT COUNT(*) total,
                   SUM(CASE WHEN result IN ('created','updated') THEN 1 ELSE 0 END) successful,
                   SUM(CASE WHEN result='error' THEN 1 ELSE 0 END) errors,
                   SUM(CASE WHEN datetime(received_at)>=datetime('now','-24 hours') THEN 1 ELSE 0 END) last_24h
            FROM events
            """
        )
        aggregate = dict(await cur.fetchone() or {})
        cur = await db.execute(
            "SELECT received_at,gc_user_id,result,fields_received,error FROM events ORDER BY id DESC LIMIT 1"
        )
        last = dict(await cur.fetchone() or {})
    return {
        "total": int(aggregate.get("total") or 0),
        "successful": int(aggregate.get("successful") or 0),
        "errors": int(aggregate.get("errors") or 0),
        "last_24h": int(aggregate.get("last_24h") or 0),
        "last": last,
    }


@router.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "customer_db_configured": bool(_customer_db_token()), "stats": await _stats()}


@router.get("/config")
async def config(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    secret = await _setting("webhook_secret")
    relative, full = _callback_urls(request, secret)
    return {
        "ok": True,
        "configured": bool(_customer_db_token()),
        "callback_url": full,
        "callback_path": relative,
        "method": "GET",
        "stats": await _stats(),
    }


@router.get("/webhook")
async def webhook(request: Request) -> dict[str, Any]:
    return await _receive(request)
