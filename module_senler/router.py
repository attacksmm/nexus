"""
senler v1.0.0
Трекинг посещений → добавление VK ID в списки Senler.

Трекинг-скрипт ставится на сайт, при посещении присылает URL + параметры.
Модуль сохраняет страницу (без параметров), ищет активные связки и добавляет в Senler.
"""
import json
import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import aiosqlite
import httpx
from fastapi import APIRouter, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

router = APIRouter()
_db_path = None
_logger: logging.Logger = None

SENLER_API = "https://senler.ru/api"
SENLER_V = "2"


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
                subscription_id TEXT NOT NULL,
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
                error       TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_visits_page   ON visits(page_url);
            CREATE INDEX IF NOT EXISTS idx_bindings_page ON bindings(page_url);
        """)
        await db.commit()
    _logger.info("senler DB initialized")


def _clean_url(raw: str) -> str:
    """URL без query и fragment."""
    try:
        p = urlparse(raw)
        return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    except Exception:
        return raw.split("?")[0].split("#")[0].rstrip("/")


# ── Settings ──────────────────────────────────────────────────────────────────

@router.get("/settings")
async def get_settings():
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute("SELECT key, value FROM settings")
        rows = dict(await cur.fetchall())
    return {
        "access_token": rows.get("access_token", ""),
        "group_id": rows.get("group_id", ""),
    }


@router.post("/settings")
async def save_settings(request: Request):
    data = await request.json()
    async with aiosqlite.connect(_db_path) as db:
        for k in ("access_token", "group_id"):
            if k in data:
                await db.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (k, str(data[k]).strip()),
                )
        await db.commit()
    _logger.info("senler settings updated")
    return {"ok": True}


# ── Pages ─────────────────────────────────────────────────────────────────────

@router.get("/pages")
async def list_pages():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM pages ORDER BY visit_count DESC, id DESC")
        return [dict(r) for r in await cur.fetchall()]


@router.delete("/pages/{page_id}")
async def delete_page(page_id: int):
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
async def list_bindings():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bindings ORDER BY id DESC")
        return [dict(r) for r in await cur.fetchall()]


class BindingIn:
    def __init__(self, page_url: str, subscription_id: str, vk_id_param: str = "vk_id", note: str = ""):
        self.page_url = _clean_url(page_url)
        self.subscription_id = subscription_id
        self.vk_id_param = vk_id_param
        self.note = note


@router.post("/bindings", status_code=201)
async def create_binding(request: Request):
    data = await request.json()
    b = BindingIn(
        page_url=data.get("page_url", ""),
        subscription_id=data.get("subscription_id", ""),
        vk_id_param=data.get("vk_id_param", "vk_id"),
        note=data.get("note", ""),
    )
    if not b.page_url or not b.subscription_id:
        return JSONResponse({"error": "page_url и subscription_id обязательны"}, status_code=400)
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            "INSERT INTO bindings(page_url, subscription_id, vk_id_param, note) VALUES(?,?,?,?)",
            (b.page_url, b.subscription_id, b.vk_id_param, b.note),
        )
        await db.commit()
        bid = cur.lastrowid
    _logger.info(f"binding created: {b.page_url} → {b.subscription_id}")
    return {"id": bid, "page_url": b.page_url, "subscription_id": b.subscription_id}


@router.put("/bindings/{bid}/toggle")
async def toggle_binding(bid: int):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("UPDATE bindings SET active = 1-active WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


@router.delete("/bindings/{bid}")
async def delete_binding(bid: int):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM bindings WHERE id=?", (bid,))
        await db.commit()
    return {"ok": True}


# ── Visits ────────────────────────────────────────────────────────────────────

@router.get("/visits")
async def list_visits(limit: int = 200):
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM visits ORDER BY id DESC LIMIT ?", (min(limit, 500),)
        )
        return [dict(r) for r in await cur.fetchall()]


@router.get("/stats")
async def stats():
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

            # Настройки Senler
            s_cur = await db.execute("SELECT key, value FROM settings")
            cfg = dict(await s_cur.fetchall())

        if not bindings:
            return JSONResponse({"ok": True, "action": "page_registered", "url": page_url}, headers=headers)

        access_token = cfg.get("access_token", "")
        group_id = cfg.get("group_id", "")

        results = []
        for binding in bindings:
            vk_id = str(params.get(binding["vk_id_param"], "")).strip()
            if not vk_id:
                _logger.warning(f"track: no vk_id in param '{binding['vk_id_param']}' for {page_url}")
                async with aiosqlite.connect(_db_path) as db:
                    await db.execute(
                        "INSERT INTO visits(page_url,vk_id,ip,binding_id,success,error) VALUES(?,?,?,?,0,?)",
                        (page_url, "", ip, binding["id"], f"no param {binding['vk_id_param']}"),
                    )
                    await db.commit()
                continue

            success, error = await _senler_add(access_token, group_id, binding["subscription_id"], vk_id)
            async with aiosqlite.connect(_db_path) as db:
                await db.execute(
                    "INSERT INTO visits(page_url,vk_id,ip,binding_id,success,error) VALUES(?,?,?,?,?,?)",
                    (page_url, vk_id, ip, binding["id"], int(success), error),
                )
                await db.commit()
            results.append({"binding_id": binding["id"], "vk_id": vk_id, "success": success, "error": error})

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

async def _senler_add(access_token: str, group_id: str, subscription_id: str, vk_id: str) -> tuple[bool, str]:
    if not access_token or not group_id:
        return False, "access_token или group_id не настроены"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{SENLER_API}/subscribers.add",
                data={
                    "access_token": access_token,
                    "group_id": group_id,
                    "subscription_id": subscription_id,
                    "vk_user_ids": vk_id,
                    "v": SENLER_V,
                },
            )
        body = resp.json()
        if body.get("success"):
            _logger.info(f"senler: vk_id={vk_id} → list={subscription_id} OK")
            return True, ""
        err = body.get("error", {})
        msg = err.get("error_msg", str(body)) if isinstance(err, dict) else str(err)
        _logger.warning(f"senler: vk_id={vk_id} → list={subscription_id} FAIL: {msg}")
        return False, msg
    except Exception as e:
        _logger.error(f"senler API error: {e}")
        return False, str(e)
