"""
senler v1.0.0
Трекинг посещений → добавление VK ID в списки Senler.

Трекинг-скрипт ставится на сайт, при посещении присылает URL + параметры.
Модуль сохраняет страницу (без параметров), ищет активные связки и добавляет в Senler.
"""
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

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


def setup(ctx):
    global _db_path, _logger
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.senler"))
    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        channels = await _load_channels(db)
    return {"items": channels, "default_channel_id": default_channel_id}


@router.post("/channels", status_code=201)
async def upsert_channel(data: ChannelIn, request: Request):
    await _require_panel_user(request)
    channel_id = _clean_channel_id(data.id)
    name = _clean_channel_name(data.name, channel_id)
    api_key = str(data.api_key or "").strip()
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM pages")
        pages = [dict(r) for r in await cur.fetchall()]
        return sorted(pages, key=_page_sort_key)


@router.delete("/pages/{page_id}")
async def delete_page(page_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("UPDATE bindings SET active = 1-active WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


@router.delete("/bindings/{bid}")
async def delete_binding(bid: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM bindings WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


# ── Visits ────────────────────────────────────────────────────────────────────

@router.get("/visits")
async def list_visits(request: Request, limit: int = 200):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM visits ORDER BY id DESC LIMIT ?", (min(limit, 500),)
        )
        return [dict(r) for r in await cur.fetchall()]


@router.get("/visits/{vid}")
async def get_visit(vid: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
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
    async with aiosqlite.connect(_db_path) as db:
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
        async with aiosqlite.connect(_db_path) as db:
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
                async with aiosqlite.connect(_db_path) as db:
                    await db.execute(
                        "INSERT INTO visits(page_url,vk_id,ip,binding_id,success,error,details) VALUES(?,?,?,?,0,?,?)",
                        (page_url, "", ip, binding["id"], f"no param {binding['vk_id_param']}", details),
                    )
                    await db.commit()
                continue

            async with aiosqlite.connect(_db_path) as db:
                access_token, effective_channel_id = await _channel_credentials(db, channel_id)
            success, error, details = await _senler_add_binding_lists(access_token, effective_channel_id, binding, vk_id)
            async with aiosqlite.connect(_db_path) as db:
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
