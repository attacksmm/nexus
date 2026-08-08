"""
senler v1.0.0
Трекинг посещений → добавление VK ID в списки Senler.

Трекинг-скрипт ставится на сайт, при посещении присылает URL + параметры.
Модуль сохраняет страницу (без параметров), ищет активные связки и добавляет в Senler.
"""
import asyncio
import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()
_db_path = None
_logger: logging.Logger = None
MODULE_ID = "senler"
CHANNELS_SETTING_KEY = "channels"
MAX_CHANNEL_ID_LEN = 80
MAX_CHANNEL_NAME_LEN = 120

SENLER_API = "https://senler.ru/api"
SENLER_V = "2"

# правильные endpoints (через /): subscribers/add, subscribers/get
_EP_ADD = f"{SENLER_API}/subscribers/add"
_EP_GET = f"{SENLER_API}/subscribers/get"
_EP_STAT_SUBSCRIBE = f"{SENLER_API}/subscribers/statSubscribe"
_EP_VAR_SET = f"{SENLER_API}/vars/set"
METRIKA_COLLECT_URL = "https://mc.yandex.ru/collect"
DEFAULT_METRIKA_COUNTER_ID = "96682515"
DEFAULT_METRIKA_GOAL_ID = "subscribe"
ATTRIBUTION_TTL_DAYS = 30
ATTRIBUTION_TOKEN_BYTES = 12
ATTRIBUTION_FIELDS = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")
ATTRIBUTION_RECONCILE_SECONDS = 60
ATTRIBUTION_RECONCILE_OVERLAP_MINUTES = 5
_reconcile_task: asyncio.Task | None = None


def _get_credentials() -> tuple[str, str]:
    """Читает токен и fallback channel/group_id из ENV без перезапуска."""
    return (
        os.environ.get("SENLER_ACCESS_TOKEN", ""),
        os.environ.get("SENLER_GROUP_ID", ""),
    )


class ChannelIn(BaseModel):
    id: str
    name: str = ""
    api_key: str = ""


class ChannelCheckIn(BaseModel):
    user_id: str


def _db_connect(path):
    return aiosqlite.connect(path, timeout=30)


def setup(ctx):
    global _db_path, _logger, _reconcile_task
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.senler"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        lifecycle = getattr(ctx, "lifecycle", None)
        if lifecycle is not None:
            lifecycle.create_task(_attribution_reconcile_loop(), name="senler-telegram-attribution")
        else:
            _reconcile_task = loop.create_task(
                _attribution_reconcile_loop(), name="senler-telegram-attribution"
            )
    else:
        loop.run_until_complete(_init_db())


async def shutdown():
    global _reconcile_task
    if _reconcile_task and not _reconcile_task.done():
        _reconcile_task.cancel()
        try:
            await _reconcile_task
        except asyncio.CancelledError:
            pass
    _reconcile_task = None


async def _init_db():
    async with _db_connect(_db_path) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS pages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT UNIQUE NOT NULL,
                first_seen  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                visit_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS bindings (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                page_url       TEXT NOT NULL,
                channel_id     TEXT NOT NULL DEFAULT '',
                subscription_id TEXT NOT NULL,
                visit_subscription_id TEXT NOT NULL DEFAULT '',
                vk_id_param    TEXT NOT NULL DEFAULT 'vk_id',
                note           TEXT NOT NULL DEFAULT '',
                active         INTEGER NOT NULL DEFAULT 1,
                created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS visits (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                page_url    TEXT NOT NULL,
                vk_id       TEXT NOT NULL DEFAULT '',
                ip          TEXT NOT NULL DEFAULT '',
                visited_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                binding_id  INTEGER,
                success     INTEGER NOT NULL DEFAULT 0,
                error       TEXT NOT NULL DEFAULT '',
                details     TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_visits_page   ON visits(page_url);
            CREATE INDEX IF NOT EXISTS idx_bindings_page ON bindings(page_url);
            CREATE TABLE IF NOT EXISTS telegram_attributions (
                token           TEXT PRIMARY KEY,
                channel_id      TEXT NOT NULL,
                subscription_id TEXT NOT NULL,
                utm_source      TEXT NOT NULL DEFAULT '',
                utm_medium      TEXT NOT NULL DEFAULT '',
                utm_campaign    TEXT NOT NULL DEFAULT '',
                utm_content     TEXT NOT NULL DEFAULT '',
                utm_term        TEXT NOT NULL DEFAULT '',
                ym_client_id    TEXT NOT NULL DEFAULT '',
                yclid           TEXT NOT NULL DEFAULT '',
                landing_url     TEXT NOT NULL DEFAULT '',
                referrer        TEXT NOT NULL DEFAULT '',
                url_params      TEXT NOT NULL DEFAULT '',
                tg_user_id      TEXT NOT NULL DEFAULT '',
                status          TEXT NOT NULL DEFAULT 'pending',
                error           TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                expires_at      TEXT NOT NULL,
                applied_at      TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_tg_attr_expires ON telegram_attributions(expires_at);
            CREATE INDEX IF NOT EXISTS idx_tg_attr_status ON telegram_attributions(status, created_at);
            CREATE TABLE IF NOT EXISTS telegram_attribution_claims (
                token           TEXT PRIMARY KEY,
                tg_user_id      TEXT NOT NULL,
                subscription_id TEXT NOT NULL DEFAULT '',
                status          TEXT NOT NULL DEFAULT 'pending',
                error           TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tg_claim_status ON telegram_attribution_claims(status, created_at);
        """)
        # migration
        try:
            await db.execute("ALTER TABLE visits ADD COLUMN details TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE bindings ADD COLUMN channel_id TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE bindings ADD COLUMN visit_subscription_id TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass
        for column in ("ym_client_id", "yclid", "landing_url", "referrer", "url_params"):
            try:
                await db.execute(f"ALTER TABLE telegram_attributions ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass
        default_token, default_channel_id = _get_credentials()
        if default_channel_id:
            await db.execute("UPDATE bindings SET channel_id=? WHERE channel_id=''", (default_channel_id,))
            cur = await db.execute("SELECT value FROM settings WHERE key=?", (CHANNELS_SETTING_KEY,))
            row = await cur.fetchone()
            if not row:
                await db.execute(
                    "INSERT INTO settings(key, value) VALUES(?, ?)",
                    (CHANNELS_SETTING_KEY, json.dumps([{"id": default_channel_id, "name": "Основной канал", "api_key": default_token}], ensure_ascii=False)),
                )
        await db.commit()
    _logger.info("senul DB initialized")


def _clean_url(raw: str) -> str:
    """URL без query и fragment."""
    try:
        p = urlparse(raw)
        return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    except Exception:
        return raw.split("?")[0].split("#")[0].rstrip("/")


def _page_sort_key(page: dict) -> tuple[str, str, str]:
    """Группирует страницы по домену, внутри домена сортирует по пути."""
    url = str(page.get("url") or "")
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "/").rstrip("/") or "/"
        return (host or "~~~", path.casefold(), url.casefold())
    except Exception:
        return ("~~~", url.casefold(), url.casefold())


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _clean_channel_id(value: str) -> str:
    clean = str(value or "").strip()
    if not clean:
        raise HTTPException(400, "ID канала обязателен")
    if len(clean) > MAX_CHANNEL_ID_LEN:
        raise HTTPException(400, f"ID канала длиннее {MAX_CHANNEL_ID_LEN} символов")
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", clean):
        raise HTTPException(400, "Недопустимый ID канала")
    return clean


def _clean_channel_name(value: str, fallback: str) -> str:
    clean = str(value or "").strip()[:MAX_CHANNEL_NAME_LEN]
    return clean or fallback


async def _get_setting(db: aiosqlite.Connection, key: str) -> str:
    cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = await cur.fetchone()
    return row[0] if row else ""


async def _set_setting(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _parse_channels(raw: str) -> list[dict[str, str]]:
    try:
        data = json.loads(raw or "[]")
    except Exception:
        data = []
    result = []
    seen = set()
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        channel_id = str(item.get("id") or "").strip()
        if not channel_id or channel_id in seen:
            continue
        seen.add(channel_id)
        result.append({
            "id": channel_id,
            "name": str(item.get("name") or channel_id).strip() or channel_id,
            "api_key": str(item.get("api_key") or item.get("token") or "").strip(),
        })
    return result


async def _load_channels(db: aiosqlite.Connection, *, include_secrets: bool = False) -> list[dict[str, str]]:
    raw = await _get_setting(db, CHANNELS_SETTING_KEY)
    channels = _parse_channels(raw)
    default_token, default_channel_id = _get_credentials()
    if default_channel_id and all(ch["id"] != default_channel_id for ch in channels):
        channels.insert(0, {"id": default_channel_id, "name": "Основной канал", "api_key": default_token})
    if include_secrets:
        return channels
    return [{"id": ch["id"], "name": ch["name"], "has_api_key": bool(ch.get("api_key") or (ch["id"] == default_channel_id and default_token))} for ch in channels]


async def _channel_credentials(db: aiosqlite.Connection, channel_id: str) -> tuple[str, str]:
    fallback_token, fallback_channel_id = _get_credentials()
    channels = await _load_channels(db, include_secrets=True)
    for channel in channels:
        if channel["id"] == channel_id:
            return channel.get("api_key") or fallback_token, channel_id
    return fallback_token, channel_id or fallback_channel_id
    return channels


# ── ENV status ────────────────────────────────────────────────────────────────


@router.get("/env-status")
async def env_status(request: Request):
    """Показывает наличие переменных ENV (без значений)."""
    await _require_panel_user(request)
    token, group_id = _get_credentials()
    async with _db_connect(_db_path) as db:
        channels = await _load_channels(db)
    has_channel_token = any(ch.get("has_api_key") for ch in channels)
    return {
        "SENLER_ACCESS_TOKEN": bool(token),
        "SENLER_GROUP_ID": bool(group_id),
        "channels": len(channels),
        "channel_tokens": has_channel_token,
        "ready": bool(channels and has_channel_token),
    }


@router.get("/channels")
async def list_channels(request: Request):
    await _require_panel_user(request)
    _, default_channel_id = _get_credentials()
    async with _db_connect(_db_path) as db:
        channels = await _load_channels(db)
    return {"items": channels, "default_channel_id": default_channel_id}


@router.post("/channels", status_code=201)
async def upsert_channel(data: ChannelIn, request: Request):
    await _require_panel_user(request)
    channel_id = _clean_channel_id(data.id)
    name = _clean_channel_name(data.name, channel_id)
    api_key = str(data.api_key or "").strip()
    async with _db_connect(_db_path) as db:
        channels = await _load_channels(db, include_secrets=True)
        updated = False
        for channel in channels:
            if channel["id"] == channel_id:
                channel["name"] = name
                if api_key:
                    channel["api_key"] = api_key
                updated = True
                break
        if not updated:
            channels.append({"id": channel_id, "name": name, "api_key": api_key})
        await _set_setting(db, CHANNELS_SETTING_KEY, json.dumps(channels, ensure_ascii=False))
        await db.commit()
    return {"ok": True, "id": channel_id, "name": name, "has_api_key": bool(api_key)}


@router.delete("/channels/{channel_id}")
async def delete_channel(channel_id: str, request: Request):
    await _require_panel_user(request)
    channel_id = _clean_channel_id(channel_id)
    _, default_channel_id = _get_credentials()
    if channel_id == default_channel_id:
        raise HTTPException(400, "Канал из SENLER_GROUP_ID удаляется через ENV")
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT COUNT(*) FROM bindings WHERE channel_id=?", (channel_id,))
        used = (await cur.fetchone())[0]
        if used:
            raise HTTPException(409, "Канал используется в связках")
        channels = [ch for ch in await _load_channels(db, include_secrets=True) if ch["id"] != channel_id]
        await _set_setting(db, CHANNELS_SETTING_KEY, json.dumps(channels, ensure_ascii=False))
        await db.commit()
    return {"ok": True}


@router.post("/channels/{channel_id}/check")
async def check_channel(channel_id: str, data: ChannelCheckIn, request: Request):
    await _require_panel_user(request)
    channel_id = _clean_channel_id(channel_id)
    user_id = str(data.user_id or "").strip()
    if not user_id:
        raise HTTPException(400, "ID пользователя обязателен")
    async with _db_connect(_db_path) as db:
        access_token, effective_channel_id = await _channel_credentials(db, channel_id)
    if not access_token:
        raise HTTPException(400, "API ключ канала не задан")
    found, raw = await _senler_check(access_token, effective_channel_id, "", user_id)
    try:
        body = json.loads(raw)
    except Exception:
        body = {"raw": raw}
    items = body.get("items") if isinstance(body, dict) else []
    item = items[0] if isinstance(items, list) and items else {}
    return {
        "ok": True,
        "channel_id": effective_channel_id,
        "found": bool(items),
        "user": {
            "first_name": item.get("first_name", ""),
            "last_name": item.get("last_name", ""),
            "status": item.get("status"),
            "tg_user_id": item.get("tg_user_id"),
            "subscriptions_count": len(item.get("subscriptions") or []),
        } if item else None,
        "senler_success": bool(body.get("success")) if isinstance(body, dict) else False,
        "error": body.get("error") if isinstance(body, dict) else None,
    }


# ── Pages ─────────────────────────────────────────────────────────────────────

@router.get("/pages")
async def list_pages(request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM pages")
        pages = [dict(r) for r in await cur.fetchall()]
        return sorted(pages, key=_page_sort_key)


@router.delete("/pages/{page_id}")
async def delete_page(page_id: int, request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        cur = await db.execute("SELECT url FROM pages WHERE id=?", (page_id,))
        row = await cur.fetchone()
        if row:
            await db.execute("DELETE FROM bindings WHERE page_url=?", (row[0],))
        await db.execute("DELETE FROM pages WHERE id=?", (page_id,))
        await db.commit()
    return {"ok": True}


# ── Bindings ──────────────────────────────────────────────────────────────────

@router.get("/bindings")
async def list_bindings(request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bindings ORDER BY id DESC")
        return [dict(r) for r in await cur.fetchall()]


class BindingIn:
    def __init__(
        self,
        page_url: str,
        subscription_id: str,
        visit_subscription_id: str = "",
        channel_id: str = "",
        vk_id_param: str = "vk_id",
        note: str = "",
    ):
        self.page_url = _clean_url(page_url)
        if not str(channel_id or "").strip():
            _, channel_id = _get_credentials()
        self.channel_id = _clean_channel_id(channel_id)
        self.subscription_id = str(subscription_id or "").strip()
        self.visit_subscription_id = str(visit_subscription_id or "").strip()
        self.vk_id_param = vk_id_param
        self.note = note


@router.post("/bindings", status_code=201)
async def create_binding(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    b = BindingIn(
        page_url=data.get("page_url", ""),
        channel_id=data.get("channel_id", ""),
        subscription_id=data.get("subscription_id", ""),
        visit_subscription_id=data.get("visit_subscription_id", ""),
        vk_id_param=data.get("vk_id_param", "vk_id"),
        note=data.get("note", ""),
    )
    if not b.page_url or not b.subscription_id or not b.channel_id:
        return JSONResponse({"error": "page_url, channel_id и subscription_id обязательны"}, status_code=400)
    async with _db_connect(_db_path) as db:
        channels = await _load_channels(db)
        if channels and all(ch["id"] != b.channel_id for ch in channels):
            return JSONResponse({"error": "Канал не найден в настройках"}, status_code=400)
        cur = await db.execute(
            "INSERT INTO bindings(page_url, channel_id, subscription_id, visit_subscription_id, vk_id_param, note) VALUES(?,?,?,?,?,?)",
            (b.page_url, b.channel_id, b.subscription_id, b.visit_subscription_id, b.vk_id_param, b.note),
        )
        await db.commit()
        bid = cur.lastrowid
    _logger.info(f"binding created: {b.page_url} → channel={b.channel_id} list={b.subscription_id} visit_list={b.visit_subscription_id}")
    return {
        "id": bid,
        "page_url": b.page_url,
        "channel_id": b.channel_id,
        "subscription_id": b.subscription_id,
        "visit_subscription_id": b.visit_subscription_id,
    }


@router.put("/bindings/{bid}/toggle")
async def toggle_binding(bid: int, request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        await db.execute("UPDATE bindings SET active = 1-active WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


@router.delete("/bindings/{bid}")
async def delete_binding(bid: int, request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        await db.execute("DELETE FROM bindings WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


# ── Visits ────────────────────────────────────────────────────────────────────

@router.get("/visits")
async def list_visits(request: Request, limit: int = 200):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM visits ORDER BY id DESC LIMIT ?", (min(limit, 500),)
        )
        return [dict(r) for r in await cur.fetchall()]


@router.get("/visits/{vid}")
async def get_visit(vid: int, request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM visits WHERE id=?", (vid,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Не найдено")
    r = dict(row)
    try:
        r["details"] = json.loads(r["details"]) if r["details"] else {}
    except Exception:
        r["details"] = {"raw": r["details"]}
    return r


@router.get("/stats")
async def stats(request: Request):
    await _require_panel_user(request)
    async with _db_connect(_db_path) as db:
        (pages,)    = (await (await db.execute("SELECT COUNT(*) FROM pages")).fetchone())
        (bindings,) = (await (await db.execute("SELECT COUNT(*) FROM bindings WHERE active=1")).fetchone())
        (visits,)   = (await (await db.execute("SELECT COUNT(*) FROM visits")).fetchone())
        (success,)  = (await (await db.execute("SELECT COUNT(*) FROM visits WHERE success=1")).fetchone())
    return {"pages": pages, "bindings": bindings, "visits": visits, "success": success}


# ── Pixel / track endpoint ────────────────────────────────────────────────────

@router.post("/track")
@router.get("/track")
async def track(request: Request):
    """
    Принимает данные от JS-скрипта на сайте.
    Тело JSON: { url, params: {key: value, ...} }
    Или GET параметры: ?url=...&vk_id=...
    Ответ всегда 200 (скрипт не-cors).
    """
    # CORS заголовки — track доступен с любого сайта
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }

    if request.method == "OPTIONS":
        return JSONResponse({}, headers=headers)

    try:
        if request.method == "POST":
            body = await request.body()
            data = json.loads(body) if body else {}
        else:
            data = dict(request.query_params)

        raw_url = data.get("url", "")
        params  = data.get("params", {})
        if not params and request.method == "GET":
            params = dict(request.query_params)

        if not raw_url:
            return JSONResponse({"ok": False, "error": "url required"}, headers=headers)

        page_url = _clean_url(raw_url)
        ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else "")

        # Сохраняем/обновляем страницу
        async with _db_connect(_db_path) as db:
            await db.execute(
                "INSERT INTO pages(url, visit_count) VALUES(?,1)"
                " ON CONFLICT(url) DO UPDATE SET visit_count=visit_count+1",
                (page_url,),
            )
            await db.commit()

            # Ищем активные связки
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM bindings WHERE page_url=? AND active=1", (page_url,)
            )
            bindings = [dict(r) for r in await cur.fetchall()]

        if not bindings:
            return JSONResponse({"ok": True, "action": "page_registered", "url": page_url}, headers=headers)

        _, default_channel_id = _get_credentials()

        results = []
        for binding in bindings:
            channel_id = str(binding.get("channel_id") or default_channel_id or "").strip()
            vk_id = str(params.get(binding["vk_id_param"], "")).strip()
            if not vk_id:
                _logger.warning(f"track: no vk_id in param '{binding['vk_id_param']}' for {page_url}")
                details = json.dumps({"reason": f"параметр '{binding['vk_id_param']}' не найден в URL"}, ensure_ascii=False)
                async with _db_connect(_db_path) as db:
                    await db.execute(
                        "INSERT INTO visits(page_url,vk_id,ip,binding_id,success,error,details) VALUES(?,?,?,?,0,?,?)",
                        (page_url, "", ip, binding["id"], f"no param {binding['vk_id_param']}", details),
                    )
                    await db.commit()
                continue

            async with _db_connect(_db_path) as db:
                access_token, effective_channel_id = await _channel_credentials(db, channel_id)
            success, error, details = await _senler_add_binding_lists(access_token, effective_channel_id, binding, vk_id)
            async with _db_connect(_db_path) as db:
                await db.execute(
                    "INSERT INTO visits(page_url,vk_id,ip,binding_id,success,error,details) VALUES(?,?,?,?,?,?,?)",
                    (page_url, vk_id, ip, binding["id"], int(success), error, details),
                )
                await db.commit()
            results.append({
                "binding_id": binding["id"],
                "channel_id": effective_channel_id,
                "vk_id": vk_id,
                "success": success,
                "error": error,
                "subscription_id": binding["subscription_id"],
                "visit_subscription_id": binding.get("visit_subscription_id") or "",
            })

        return JSONResponse({"ok": True, "results": results}, headers=headers)

    except Exception as e:
        _logger.error(f"track error: {e}", exc_info=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200, headers=headers)


@router.options("/track")
async def track_options():
    return JSONResponse({}, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })


# ── Telegram attribution ─────────────────────────────────────────────────────

def _attribution_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": "no-store",
    }


def _attribution_value(value, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def _utc_iso(value: datetime | None = None) -> str:
    return (value or datetime.now(timezone.utc)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def _telegram_channel(db: aiosqlite.Connection, channel_id: str) -> dict[str, str] | None:
    for channel in await _load_channels(db, include_secrets=True):
        if channel["id"] == channel_id and channel.get("api_key"):
            return channel
    return None


@router.options("/telegram-attribution")
async def telegram_attribution_options():
    return JSONResponse({}, headers=_attribution_headers())


@router.post("/telegram-attribution")
async def create_telegram_attribution(request: Request):
    headers = _attribution_headers()
    try:
        if "application/json" in request.headers.get("content-type", ""):
            data = await request.json()
        else:
            parsed = parse_qs((await request.body()).decode("utf-8", errors="replace"), keep_blank_values=True)
            data = {key: values[-1] if values else "" for key, values in parsed.items()}
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid payload"}, status_code=400, headers=headers)
    channel_id = _attribution_value(data.get("channel_id"), MAX_CHANNEL_ID_LEN)
    subscription_id = _attribution_value(data.get("subscription_id"), 24)
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", channel_id) or not re.fullmatch(r"\d{1,24}", subscription_id):
        return JSONResponse({"ok": False, "error": "invalid channel or subscription"}, status_code=400, headers=headers)
    values = {field: _attribution_value(data.get(field)) for field in ATTRIBUTION_FIELDS}
    raw_url_params = data.get("url_params") or []
    if isinstance(raw_url_params, str):
        try:
            raw_url_params = json.loads(raw_url_params)
        except Exception:
            raw_url_params = [["raw", raw_url_params]]
    extras = {
        "ym_client_id": _attribution_value(data.get("ym_client_id"), 32),
        "yclid": _attribution_value(data.get("yclid"), 200),
        "landing_url": _attribution_value(data.get("landing_url"), 2000),
        "referrer": _attribution_value(data.get("referrer"), 2000),
        "url_params": _attribution_value(json.dumps(raw_url_params, ensure_ascii=False), 8000),
    }
    now = datetime.now(timezone.utc)
    supplied_token = _attribution_value(data.get("token"), 80)
    if supplied_token and not re.fullmatch(r"[A-Za-z0-9_-]{12,30}", supplied_token):
        return JSONResponse({"ok": False, "error": "invalid token"}, status_code=400, headers=headers)
    token = supplied_token or secrets.token_urlsafe(ATTRIBUTION_TOKEN_BYTES)
    async with _db_connect(_db_path) as db:
        if not await _telegram_channel(db, channel_id):
            return JSONResponse({"ok": False, "error": "channel is not configured"}, status_code=400, headers=headers)
        await db.execute("DELETE FROM telegram_attributions WHERE expires_at<?", (_utc_iso(now),))
        await db.execute(
            """
            INSERT OR IGNORE INTO telegram_attributions(
                token,channel_id,subscription_id,utm_source,utm_medium,utm_campaign,utm_content,utm_term,
                ym_client_id,yclid,landing_url,referrer,url_params,created_at,expires_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                token, channel_id, subscription_id,
                *(values[field] for field in ATTRIBUTION_FIELDS),
                *(extras[field] for field in ("ym_client_id", "yclid", "landing_url", "referrer", "url_params")),
                _utc_iso(now), _utc_iso(now + timedelta(days=ATTRIBUTION_TTL_DAYS)),
            ),
        )
        await db.execute(
            """
            UPDATE telegram_attributions SET
                utm_source=CASE WHEN utm_source='' THEN ? ELSE utm_source END,
                utm_medium=CASE WHEN utm_medium='' THEN ? ELSE utm_medium END,
                utm_campaign=CASE WHEN utm_campaign='' THEN ? ELSE utm_campaign END,
                utm_content=CASE WHEN utm_content='' THEN ? ELSE utm_content END,
                utm_term=CASE WHEN utm_term='' THEN ? ELSE utm_term END,
                ym_client_id=CASE WHEN ym_client_id='' THEN ? ELSE ym_client_id END,
                yclid=CASE WHEN yclid='' THEN ? ELSE yclid END,
                landing_url=CASE WHEN landing_url='' THEN ? ELSE landing_url END,
                referrer=CASE WHEN referrer='' THEN ? ELSE referrer END,
                url_params=CASE WHEN url_params='' OR url_params='[]' THEN ? ELSE url_params END
            WHERE token=? AND channel_id=? AND subscription_id=?
            """,
            (
                *(values[field] for field in ATTRIBUTION_FIELDS),
                *(extras[field] for field in ("ym_client_id", "yclid", "landing_url", "referrer", "url_params")),
                token, channel_id, subscription_id,
            ),
        )
        pending = await (
            await db.execute(
                "SELECT tg_user_id,subscription_id FROM telegram_attribution_claims WHERE token=? AND status='pending'",
                (token,),
            )
        ).fetchone()
        await db.commit()
    if pending:
        body, status_code = await _claim_telegram_attribution({
            "token": token,
            "tg_user_id": pending[0],
            "subscription_id": pending[1] or subscription_id,
        })
        return JSONResponse({"ok": True, "token": token, "claim": body.get("status", "pending")}, headers=headers)
    return JSONResponse({"ok": True, "token": token, "expires_in": ATTRIBUTION_TTL_DAYS * 86400}, headers=headers)


async def _apply_telegram_attribution(row: dict) -> tuple[bool, str]:
    async with _db_connect(_db_path) as db:
        channel = await _telegram_channel(db, row["channel_id"])
    if not channel:
        return False, "Senler Telegram channel is not configured"
    data = {
        "access_token": channel["api_key"],
        "group_id": row["channel_id"],
        "subscription_id": row["subscription_id"],
        "tg_user_id": row["tg_user_id"],
        "v": SENLER_V,
        **{field: row.get(field, "") for field in ATTRIBUTION_FIELDS if row.get(field)},
    }
    last_error = ""
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(_EP_ADD, data=data)
            body = response.json()
            if response.is_success and body.get("success"):
                break
            error = body.get("error") if isinstance(body, dict) else body
            last_error = json.dumps(error, ensure_ascii=False)[:500]
        except Exception as exc:
            last_error = str(exc)[:500]
        if attempt < 2:
            await asyncio.sleep(0.25 * (attempt + 1))
    else:
        return False, last_error or "Senler API request failed"
    variables = {
        "nexus.source": row.get("utm_source", ""),
        "nexus.medium": row.get("utm_medium", ""),
        "nexus.campaign": row.get("utm_campaign", ""),
        "nexus.content": row.get("utm_content", ""),
        "nexus.term": row.get("utm_term", ""),
        "nexus.ym_uid": row.get("ym_client_id", ""),
        "nexus.yclid": row.get("yclid", ""),
        "landing_url": row.get("landing_url", ""),
        "referrer": row.get("referrer", ""),
        "url_params": row.get("url_params", ""),
    }
    for name, value in variables.items():
        if not value:
            continue
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    response = await client.post(_EP_VAR_SET, data={
                        "access_token": channel["api_key"], "group_id": row["channel_id"],
                        "tg_user_id": row["tg_user_id"], "name": name, "value": value, "v": SENLER_V,
                    })
                body = response.json()
                if response.is_success and body.get("success"):
                    break
                last_error = json.dumps(body.get("error", body), ensure_ascii=False)[:500]
            except Exception as exc:
                last_error = str(exc)[:500]
            if attempt < 2:
                await asyncio.sleep(0.25 * (attempt + 1))
        else:
            return False, f"vars/set {name}: {last_error or 'Senler API request failed'}"
    metrika_status, metrika_error = await _send_metrika_subscribe_goal(row)
    if metrika_status == "failed":
        _logger.warning(
            "Yandex Metrika subscribe goal failed: tg_user_id=%s error=%s",
            row.get("tg_user_id", ""), metrika_error,
        )
    return True, ""


def _metrika_config() -> tuple[str, str, str]:
    counter_id = os.environ.get("YANDEX_METRIKA_COUNTER_ID", DEFAULT_METRIKA_COUNTER_ID).strip()
    goal_id = os.environ.get("YANDEX_METRIKA_SUBSCRIBE_GOAL", DEFAULT_METRIKA_GOAL_ID).strip()
    secret = os.environ.get("YANDEX_METRIKA_MEASUREMENT_TOKEN", "").strip()
    if not re.fullmatch(r"\d{1,20}", counter_id):
        counter_id = ""
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,255}", goal_id):
        goal_id = ""
    if len(secret) > 512:
        secret = ""
    return counter_id, goal_id, secret


async def _send_metrika_subscribe_goal(row: dict) -> tuple[str, str]:
    """At-most-once goal delivery: ambiguous failures are never retried automatically."""
    client_id = _attribution_value(row.get("ym_client_id"), 32)
    tg_user_id = _attribution_value(row.get("tg_user_id"), 32)
    if not re.fullmatch(r"\d{6,32}", client_id) or not re.fullmatch(r"\d{1,24}", tg_user_id):
        return "skipped", ""
    counter_id, goal_id, secret = _metrika_config()
    if not counter_id or not goal_id or not secret:
        return "not_configured", ""
    now = _utc_iso()
    delivery_key = f"metrika_goal:{counter_id}:{goal_id}:{tg_user_id}"
    async with _db_connect(_db_path) as db:
        inserted = await db.execute(
            "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
            (delivery_key, json.dumps({
                "status": "reserved", "ym_client_id": client_id,
                "token": row.get("token", ""), "attempted_at": now,
            }, ensure_ascii=False)),
        )
        await db.commit()
        if inserted.rowcount != 1:
            existing = await (
                await db.execute("SELECT value FROM settings WHERE key=?", (delivery_key,))
            ).fetchone()
            try:
                delivery = json.loads(existing[0]) if existing else {}
            except Exception:
                delivery = {}
            return delivery.get("status", "reserved"), delivery.get("error", "")
    payload = {
        "tid": counter_id,
        "cid": client_id,
        "t": "event",
        "ea": goal_id,
        "et": str(int(datetime.now(timezone.utc).timestamp())),
        "ms": secret,
    }
    landing_url = _attribution_value(row.get("landing_url"), 4000)
    if landing_url.startswith(("https://", "http://")):
        payload["dl"] = landing_url
    status, error = "failed", ""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(METRIKA_COLLECT_URL, data=payload)
        if response.is_success:
            status = "sent"
        else:
            error = f"HTTP {response.status_code}: {response.text[:300]}"
    except Exception as exc:
        error = str(exc)[:500]
    async with _db_connect(_db_path) as db:
        await db.execute(
            "UPDATE settings SET value=? WHERE key=?",
            (json.dumps({
                "status": status, "error": error, "ym_client_id": client_id,
                "token": row.get("token", ""), "attempted_at": now,
                "sent_at": _utc_iso() if status == "sent" else "",
            }, ensure_ascii=False), delivery_key),
        )
        await db.commit()
    return status, error


async def _claim_telegram_attribution(data: dict) -> tuple[dict, int]:
    token = _attribution_value(data.get("token"), 80)
    client_id = _attribution_value(data.get("client_id"), 32)
    tg_user_id = _attribution_value(data.get("tg_user_id"), 32)
    subscription_id = _attribution_value(data.get("subscription_id"), 24)
    token_valid = bool(re.fullmatch(r"[A-Za-z0-9_-]{12,80}", token))
    client_valid = bool(re.fullmatch(r"\d{6,32}", client_id))
    if not re.fullmatch(r"\d{1,24}", tg_user_id) or not (token_valid or client_valid):
        raise HTTPException(400, "invalid attribution claim")
    if client_valid and not re.fullmatch(r"\d{1,24}", subscription_id):
        raise HTTPException(400, "subscription is required for ClientID claim")
    now = _utc_iso()
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        if token_valid:
            row = await (
                await db.execute("SELECT * FROM telegram_attributions WHERE token=?", (token,))
            ).fetchone()
        else:
            row = await (
                await db.execute(
                    """
                    SELECT * FROM telegram_attributions
                    WHERE subscription_id=? AND (ym_client_id=? OR utm_term=?) AND tg_user_id=?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (subscription_id, client_id, client_id, tg_user_id),
                )
            ).fetchone()
            if not row:
                row = await (
                    await db.execute(
                        """
                        SELECT * FROM telegram_attributions
                        WHERE subscription_id=? AND (ym_client_id=? OR utm_term=?) AND tg_user_id=''
                          AND expires_at>=? AND status IN ('pending','failed')
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (subscription_id, client_id, client_id, now),
                    )
                ).fetchone()
        if not row:
            if client_valid:
                return {"ok": True, "status": "unmatched", "duplicate": False}, 200
            await db.execute(
                """
                INSERT INTO telegram_attribution_claims(token,tg_user_id,subscription_id,created_at,updated_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(token) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    subscription_id=CASE WHEN telegram_attribution_claims.subscription_id='' THEN excluded.subscription_id ELSE telegram_attribution_claims.subscription_id END
                WHERE telegram_attribution_claims.tg_user_id=excluded.tg_user_id
                """,
                (token, tg_user_id, subscription_id, now, now),
            )
            await db.commit()
            return {"ok": True, "status": "pending", "duplicate": False}, 200
        item = dict(row)
        if item["expires_at"] < now:
            raise HTTPException(410, "attribution expired")
        if subscription_id and item["subscription_id"] != subscription_id:
            raise HTTPException(409, "subscription mismatch")
        if item["tg_user_id"] and item["tg_user_id"] != tg_user_id:
            raise HTTPException(409, "attribution already claimed")
        if item["status"] == "applied" and item["tg_user_id"] == tg_user_id:
            await _send_metrika_subscribe_goal(item)
            return {"ok": True, "status": "applied", "duplicate": True}, 200
        changed = await db.execute(
            """
            UPDATE telegram_attributions SET tg_user_id=?,status='applying',error=''
            WHERE token=? AND (tg_user_id='' OR tg_user_id=?)
            """,
            (tg_user_id, item["token"], tg_user_id),
        )
        if changed.rowcount != 1:
            raise HTTPException(409, "attribution already claimed")
        await db.commit()
        item["tg_user_id"] = tg_user_id
    success, error = await _apply_telegram_attribution(item)
    async with _db_connect(_db_path) as db:
        await db.execute(
            "UPDATE telegram_attributions SET status=?,error=?,applied_at=? WHERE token=?",
            ("applied" if success else "failed", error, _utc_iso() if success else "", item["token"]),
        )
        await db.execute(
            "UPDATE telegram_attribution_claims SET status=?,error=?,updated_at=? WHERE token=? AND tg_user_id=?",
            ("applied" if success else "failed", error, _utc_iso(), item["token"], tg_user_id),
        )
        await db.commit()
    if success:
        _logger.info("telegram attribution applied: tg_user_id=%s subscription=%s", tg_user_id, item["subscription_id"])
        return {"ok": True, "status": "applied", "duplicate": False}, 200
    _logger.warning("telegram attribution failed: tg_user_id=%s subscription=%s error=%s", tg_user_id, item["subscription_id"], error)
    return {"ok": False, "status": "failed", "error": error}, 502


@router.post("/telegram-attribution/claim")
async def claim_telegram_attribution(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "invalid json")
    body, status_code = await _claim_telegram_attribution(data)
    return body if status_code == 200 else JSONResponse(body, status_code=status_code)


def _senler_date(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M:%S")


def _event_attribution(item: dict) -> dict[str, str]:
    value = _attribution_value(item.get("utm_term"), 32)
    if re.fullmatch(r"\d{6,32}", value):
        return {"client_id": value}
    if re.fullmatch(r"n_[A-Za-z0-9_-]{12,30}", value):
        return {"token": value[2:]}
    for utm in item.get("utms") if isinstance(item.get("utms"), list) else []:
        value = _attribution_value(utm.get("utm_term") if isinstance(utm, dict) else "", 32)
        if re.fullmatch(r"\d{6,32}", value):
            return {"client_id": value}
        if re.fullmatch(r"n_[A-Za-z0-9_-]{12,30}", value):
            return {"token": value[2:]}
    return {}


async def _senler_subscription_events(
    channel: dict[str, str], subscription_ids: list[str], date_from: datetime, date_to: datetime
) -> list[dict]:
    items: list[dict] = []
    offset = 0
    offset_id = ""
    for _ in range(20):
        payload = {
            "access_token": channel["api_key"],
            "group_id": channel["id"],
            "v": SENLER_V,
            "date_from": _senler_date(date_from),
            "date_to": _senler_date(date_to),
            "subscription_id": [int(value) for value in subscription_ids],
            "action": 1,
            "count": 1000,
        }
        if offset_id:
            payload["offset_id"] = offset_id
        else:
            payload["offset"] = offset
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(_EP_STAT_SUBSCRIBE, json=payload)
        body = response.json()
        if not response.is_success or not body.get("success"):
            raise RuntimeError(json.dumps(body.get("error") or body, ensure_ascii=False)[:500])
        page = body.get("items") if isinstance(body.get("items"), list) else []
        items.extend(item for item in page if isinstance(item, dict))
        if len(page) < 1000:
            return items
        offset_id = str(body.get("offset_id") or "").strip()
        if not offset_id:
            offset += len(page)
    raise RuntimeError("Senler subscription event pagination limit reached")


async def _retry_bound_attributions() -> int:
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                """
                SELECT * FROM telegram_attributions
                WHERE status IN ('failed','applying') AND tg_user_id<>'' AND expires_at>=?
                ORDER BY created_at LIMIT 100
                """,
                (_utc_iso(),),
            )
        ).fetchall()
    applied = 0
    for row in rows:
        item = dict(row)
        success, error = await _apply_telegram_attribution(item)
        async with _db_connect(_db_path) as db:
            await db.execute(
                "UPDATE telegram_attributions SET status=?,error=?,applied_at=? WHERE token=?",
                ("applied" if success else "failed", error, _utc_iso() if success else "", item["token"]),
            )
            await db.commit()
        applied += int(success)
    return applied


async def _reconcile_pending_attributions() -> int:
    now = datetime.now(timezone.utc)
    async with _db_connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                """
                SELECT DISTINCT channel_id,subscription_id
                FROM telegram_attributions
                WHERE status IN ('pending','failed') AND tg_user_id='' AND expires_at>=?
                  AND (ym_client_id GLOB '[0-9]*' OR utm_term GLOB '[0-9]*' OR token<>'')
                """,
                (_utc_iso(now),),
            )
        ).fetchall()
        channels = {item["id"]: item for item in await _load_channels(db, include_secrets=True)}
    grouped: dict[str, set[str]] = {}
    for row in rows:
        grouped.setdefault(row["channel_id"], set()).add(row["subscription_id"])
    applied = await _retry_bound_attributions()
    for channel_id, subscriptions in grouped.items():
        channel = channels.get(channel_id)
        if not channel or not channel.get("api_key"):
            continue
        setting_key = f"telegram_attribution_reconcile:{channel_id}"
        async with _db_connect(_db_path) as db:
            raw_since = await _get_setting(db, setting_key)
        try:
            since = datetime.fromisoformat(raw_since.replace("Z", "+00:00"))
        except Exception:
            since = now - timedelta(hours=1)
        since = max(since - timedelta(minutes=ATTRIBUTION_RECONCILE_OVERLAP_MINUTES), now - timedelta(days=ATTRIBUTION_TTL_DAYS))
        events = await _senler_subscription_events(channel, sorted(subscriptions), since, now)
        for event in events:
            attribution = _event_attribution(event)
            tg_user_id = _attribution_value(event.get("tg_user_id"), 32)
            subscription_id = _attribution_value(event.get("subscription_id"), 24)
            if not attribution or not re.fullmatch(r"\d{1,24}", tg_user_id) or subscription_id not in subscriptions:
                continue
            body, _ = await _claim_telegram_attribution({
                **attribution,
                "tg_user_id": tg_user_id,
                "subscription_id": subscription_id,
            })
            applied += int(body.get("status") == "applied" and not body.get("duplicate"))
        async with _db_connect(_db_path) as db:
            await _set_setting(db, setting_key, _utc_iso(now))
            await db.commit()
    return applied


async def _attribution_reconcile_loop() -> None:
    await _init_db()
    while True:
        try:
            applied = await _reconcile_pending_attributions()
            if applied:
                _logger.info("telegram attribution reconciliation applied=%s", applied)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _logger.warning("telegram attribution reconciliation failed: %s", exc)
        await asyncio.sleep(ATTRIBUTION_RECONCILE_SECONDS)


# ── Senler API call ───────────────────────────────────────────────────────────

def _json_loads_or_raw(value: str) -> dict | str:
    try:
        return json.loads(value) if value else {}
    except Exception:
        return value


async def _senler_add_binding_lists(access_token: str, channel_id: str, binding: dict, vk_id: str) -> tuple[bool, str, str]:
    targets = [
        ("primary", str(binding.get("subscription_id") or "").strip()),
        ("visit", str(binding.get("visit_subscription_id") or "").strip()),
    ]
    results = []
    errors = []
    for kind, subscription_id in targets:
        if not subscription_id:
            continue
        ok, error, details = await _senler_add(access_token, channel_id, subscription_id, vk_id)
        results.append({
            "kind": kind,
            "subscription_id": subscription_id,
            "success": ok,
            "error": error,
            "details": _json_loads_or_raw(details),
        })
        if not ok:
            errors.append(f"{kind}:{subscription_id}: {error}")

    if not results:
        details = json.dumps({"reason": "у связки не задан ни один список Senler"}, ensure_ascii=False)
        return False, "списки Senler не настроены", details

    details = json.dumps({
        "channel_id": channel_id,
        "vk_id": vk_id,
        "binding_id": binding.get("id"),
        "adds": results,
    }, ensure_ascii=False)
    return not errors, "; ".join(errors), details

async def _senler_check(access_token: str, channel_id: str, subscription_id: str, vk_id: str) -> tuple[bool | None, str]:
    """Проверяет подписан ли vk_id в список subscription_id.
    Возвращает (True=подписан, False=нет, None=ошибка проверки), raw_response.
    """
    try:
        data = {
            "access_token": access_token,
            "group_id": channel_id,
            "vk_user_id": vk_id,
            "v": SENLER_V,
        }
        if subscription_id:
            data["subscription_id"] = subscription_id
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(_EP_GET, data=data)
        raw = resp.text[:2000]
        try:
            body = resp.json()
        except Exception:
            return None, raw
        # если есть пользователь в ответе — он подписан
        items = body.get("items") or body.get("users") or body.get("response") or []
        if isinstance(items, list) and len(items) > 0:
            return True, raw
        return False, raw
    except Exception as e:
        return None, str(e)


async def _senler_add(access_token: str, channel_id: str, subscription_id: str, vk_id: str) -> tuple[bool, str, str]:
    """Возвращает (success, error_msg, details_json)."""
    params = {
        "access_token": "***",
        "group_id": channel_id,
        "subscription_id": subscription_id,
        "vk_user_id": vk_id,
        "v": SENLER_V,
    }
    if not access_token or not channel_id:
        details = json.dumps({"reason": "access_token или channel_id не настроены", "params": params}, ensure_ascii=False)
        return False, "токен или channel_id не настроены", details

    # проверяем — возможно уже подписан (пришёл из Salebot или другой системы)
    already, check_raw = await _senler_check(access_token, channel_id, subscription_id, vk_id)
    if already is True:
        _logger.info(f"senler: vk_id={vk_id} уже в списке {subscription_id}, пропускаем")
        details = json.dumps({
            "skipped": True,
            "reason": "пользователь уже подписан на список",
            "check_response": check_raw,
            "params": params,
        }, ensure_ascii=False)
        return True, "", details

    raw_body = ""
    status_code = 0
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _EP_ADD,
                data={
                    "access_token": access_token,
                    "group_id": channel_id,
                    "subscription_id": subscription_id,
                    "vk_user_id": vk_id,
                    "v": SENLER_V,
                },
            )
        status_code = resp.status_code
        raw_body = resp.text[:2000]

        try:
            body = resp.json()
        except Exception:
            details = json.dumps({
                "http_status": status_code,
                "response": raw_body,
                "params": params,
            }, ensure_ascii=False)
            _logger.warning(f"senler: не JSON ответ [{status_code}]: {raw_body[:200]}")
            return False, f"ответ не JSON (HTTP {status_code})", details

        details = json.dumps({
            "http_status": status_code,
            "response": body,
            "params": params,
            "check_before_add": check_raw,
        }, ensure_ascii=False)

        if body.get("success"):
            _logger.info(f"senler: vk_id={vk_id} → list={subscription_id} OK")
            return True, "", details

        err = body.get("error", {})
        msg = err.get("error_msg", str(body)) if isinstance(err, dict) else str(err)
        _logger.warning(f"senler: vk_id={vk_id} → list={subscription_id} FAIL: {msg}")
        return False, msg, details

    except Exception as e:
        details = json.dumps({
            "http_status": status_code,
            "response": raw_body,
            "exception": str(e),
            "params": params,
        }, ensure_ascii=False)
        _logger.error(f"senler API error: {e}")
        return False, str(e), details
