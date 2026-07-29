from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiosqlite
import httpx
from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, HTTPException, Request, Response

from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
from orchestrator.telegram_proxy import httpx_client_kwargs, telegram_bot_api_base


router = APIRouter()

MODULE_ID = "telegram-salebot-proxy"
DEFAULT_PUBLIC_BASE = "https://junior.sobakovod.pro/nexus"
SALEBOT_API_BASE = "https://chatter.salebot.pro/api"
MAX_UPDATE_BYTES = 5 * 1024 * 1024
SALEBOT_HOST_SUFFIX = ".salebot.pro"
WATCH_INTERVAL_SECONDS = 5

_db_path: Path | None = None
_logger: logging.Logger | None = None
_watcher_task: asyncio.Task | None = None
_stop_event = asyncio.Event()
_bot_locks: dict[int, asyncio.Lock] = {}


async def setup(ctx) -> None:
    global _db_path, _logger, _watcher_task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.telegram-salebot-proxy"))
    _stop_event.clear()
    await _init_db()
    if _watcher_task is None or _watcher_task.done():
        _watcher_task = asyncio.create_task(_watcher_loop(), name="telegram-salebot-proxy-watcher")


async def shutdown() -> None:
    global _watcher_task
    _stop_event.set()
    if _watcher_task and not _watcher_task.done():
        _watcher_task.cancel()
        try:
            await _watcher_task
        except asyncio.CancelledError:
            pass
    _watcher_task = None


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("telegram-salebot-proxy module is not initialized")
    return _db_path


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _truthy(value: Any) -> bool:
    return _clean(value, 20).lower() in {"1", "true", "yes", "on", "да"}


def _public_base(value: Any = "") -> str:
    candidate = _clean(value, 1000) or _clean(os.environ.get("TG_SALEBOT_PROXY_PUBLIC_BASE"), 1000) or DEFAULT_PUBLIC_BASE
    candidate = candidate.rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Нужен публичный HTTPS-адрес Nexus без query и fragment")
    return candidate


def _is_salebot_url(value: Any) -> bool:
    try:
        parsed = urlparse(_clean(value, 4000))
    except Exception:
        return False
    host = (parsed.hostname or "").lower().rstrip(".")
    return bool(
        parsed.scheme == "https"
        and host
        and (host == "salebot.pro" or host.endswith(SALEBOT_HOST_SUFFIX))
        and not parsed.username
        and not parsed.password
    )


def _mask_url(value: Any) -> str:
    try:
        parsed = urlparse(_clean(value, 4000))
    except Exception:
        return ""
    if not parsed.hostname:
        return ""
    path = parsed.path or "/"
    parts = [part for part in path.split("/") if part]
    masked_path = "/" + "/".join((parts[:1] + ["••••"] if parts else []))
    return f"{parsed.scheme}://{parsed.hostname}{masked_path}"


def _crypto() -> Fernet:
    secret = _clean(os.environ.get("NEXUS_SECRET"), 10000)
    if not secret:
        raise RuntimeError("NEXUS_SECRET не задан: безопасное хранение токена недоступно")
    digest = hashlib.sha256(("nexus:telegram-salebot-proxy:v1:" + secret).encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _encrypt(value: str) -> str:
    return _crypto().encrypt(value.encode("utf-8")).decode("ascii")


def _decrypt(value: Any) -> str:
    if not value:
        return ""
    try:
        return _crypto().decrypt(str(value).encode("ascii")).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("Секрет Nexus изменился; введите bot token заново") from exc


def _token_hint(token: str) -> str:
    prefix = token.split(":", 1)[0] if ":" in token else ""
    return f"{prefix}:••••••••" if prefix else "••••••••"


def _webhook_url(bot: dict[str, Any], public_base: str) -> str:
    return f"{public_base}/{MODULE_ID}/api/webhook/{bot['telegram_id']}/{bot['path_secret']}"


def _event_meta(payload: dict[str, Any]) -> tuple[str, str, str]:
    keys = (
        "message", "edited_message", "channel_post", "edited_channel_post",
        "callback_query", "inline_query", "chosen_inline_result", "shipping_query",
        "pre_checkout_query", "poll", "poll_answer", "my_chat_member", "chat_member",
        "chat_join_request", "message_reaction", "message_reaction_count",
        "business_connection", "business_message", "edited_business_message",
        "deleted_business_messages", "purchased_paid_media",
    )
    event_type = next((key for key in keys if key in payload), "unknown")
    node = payload.get(event_type) if isinstance(payload.get(event_type), dict) else {}
    if event_type == "callback_query":
        message = node.get("message") if isinstance(node.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        actor = node.get("from") if isinstance(node.get("from"), dict) else {}
    else:
        chat = node.get("chat") if isinstance(node.get("chat"), dict) else {}
        actor = node.get("from") if isinstance(node.get("from"), dict) else {}
    return event_type, _clean(chat.get("id"), 80), _clean(actor.get("id"), 80)


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user) or not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "Доступ разрешён только администратору")
    return user


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                token_enc TEXT NOT NULL,
                token_hint TEXT NOT NULL DEFAULT '',
                upstream_url_enc TEXT NOT NULL DEFAULT '',
                upstream_masked TEXT NOT NULL DEFAULT '',
                salebot_api_key_enc TEXT NOT NULL DEFAULT '',
                salebot_project_id TEXT NOT NULL DEFAULT '',
                observed_url_enc TEXT NOT NULL DEFAULT '',
                observed_masked TEXT NOT NULL DEFAULT '',
                observed_count INTEGER NOT NULL DEFAULT 0,
                path_secret TEXT NOT NULL UNIQUE,
                telegram_secret TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'draft',
                transport TEXT NOT NULL DEFAULT 'webhook',
                poll_offset INTEGER NOT NULL DEFAULT 0,
                auto_activate INTEGER NOT NULL DEFAULT 1,
                auto_recover INTEGER NOT NULL DEFAULT 1,
                current_webhook_masked TEXT NOT NULL DEFAULT '',
                pending_updates INTEGER NOT NULL DEFAULT 0,
                last_check_at TEXT NOT NULL DEFAULT '',
                last_check_error TEXT NOT NULL DEFAULT '',
                last_update_at TEXT NOT NULL DEFAULT '',
                last_delivery_at TEXT NOT NULL DEFAULT '',
                last_delivery_error TEXT NOT NULL DEFAULT '',
                received_count INTEGER NOT NULL DEFAULT 0,
                delivered_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER NOT NULL,
                update_id TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                actor_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'received',
                attempts INTEGER NOT NULL DEFAULT 0,
                response_code INTEGER,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                raw_payload TEXT NOT NULL DEFAULT '',
                received_at TEXT NOT NULL DEFAULT '',
                delivered_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(bot_id) REFERENCES bots(id) ON DELETE CASCADE,
                UNIQUE(bot_id, update_id)
            );
            CREATE INDEX IF NOT EXISTS idx_proxy_events_bot_id ON events(bot_id,id DESC);
            CREATE INDEX IF NOT EXISTS idx_proxy_events_status ON events(status,id DESC);
            """
        )
        cur = await db.execute("PRAGMA table_info(bots)")
        columns = {str(row[1]) for row in await cur.fetchall()}
        for name, ddl in (
            ("transport", "TEXT NOT NULL DEFAULT 'webhook'"),
            ("poll_offset", "INTEGER NOT NULL DEFAULT 0"),
            ("salebot_api_key_enc", "TEXT NOT NULL DEFAULT ''"),
            ("salebot_project_id", "TEXT NOT NULL DEFAULT ''"),
        ):
            if name not in columns:
                await db.execute(f"ALTER TABLE bots ADD COLUMN {name} {ddl}")
        defaults = {
            "public_base": _clean(os.environ.get("TG_SALEBOT_PROXY_PUBLIC_BASE"), 1000) or DEFAULT_PUBLIC_BASE,
            "retention_days": "14",
        }
        for key, value in defaults.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, value, _now()),
            )
        await db.commit()
    _log("info", "telegram-salebot-proxy initialized")


async def _settings() -> dict[str, str]:
    result = {"public_base": DEFAULT_PUBLIC_BASE, "retention_days": "14"}
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        result.update({str(key): str(value or "") for key, value in await cur.fetchall()})
    if _clean(os.environ.get("TG_SALEBOT_PROXY_PUBLIC_BASE"), 1000):
        result["public_base"] = _clean(os.environ.get("TG_SALEBOT_PROXY_PUBLIC_BASE"), 1000)
    result["public_base"] = _public_base(result["public_base"])
    return result


async def _bot_row(bot_id: int) -> dict[str, Any] | None:
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bots WHERE id=?", (bot_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


async def _bot_by_telegram_id(telegram_id: str) -> dict[str, Any] | None:
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bots WHERE telegram_id=?", (telegram_id,))
        row = await cur.fetchone()
    return dict(row) if row else None


def _public_bot(row: dict[str, Any], public_base: str) -> dict[str, Any]:
    data = {key: value for key, value in row.items() if key not in {"token_enc", "upstream_url_enc", "observed_url_enc", "telegram_secret", "path_secret", "salebot_api_key_enc"}}
    data.update(
        {
            "proxy_url": _webhook_url(row, public_base),
            "has_upstream": bool(row.get("upstream_url_enc")),
            "salebot_api_connected": bool(row.get("salebot_api_key_enc")),
            "encrypted": True,
        }
    )
    return data


async def _tg_call(token: str, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(**httpx_client_kwargs(timeout=15), follow_redirects=False) as client:
            response = await client.post(
                f"{telegram_bot_api_base()}/bot{token}/{method}",
                json=payload or {},
            )
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Telegram недоступен: {type(exc).__name__}") from exc
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Telegram вернул HTTP {response.status_code} без JSON") from exc
    if not response.is_success or not isinstance(body, dict) or not body.get("ok"):
        description = _clean(body.get("description") if isinstance(body, dict) else "", 300)
        raise RuntimeError(description or f"Telegram API HTTP {response.status_code}")
    return body.get("result") if isinstance(body.get("result"), dict) else {"value": body.get("result")}


async def _telegram_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return await _tg_call(_decrypt(row["token_enc"]), "getWebhookInfo")


async def _set_proxy_webhook(row: dict[str, Any], public_base: str) -> None:
    url = _webhook_url(row, public_base)
    await _tg_call(
        _decrypt(row["token_enc"]),
        "setWebhook",
        {
            "url": url,
            "secret_token": row["telegram_secret"],
            "drop_pending_updates": False,
            "max_connections": 40,
        },
    )


async def _set_upstream_webhook(row: dict[str, Any]) -> None:
    target = _decrypt(row.get("upstream_url_enc"))
    if not _is_salebot_url(target):
        raise RuntimeError("Безопасный URL SaleBot не сохранён")
    await _tg_call(
        _decrypt(row["token_enc"]),
        "setWebhook",
        {"url": target, "drop_pending_updates": False, "max_connections": 40},
    )


async def _validate_salebot_api_key(api_key: str, bot_username: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=False, trust_env=False) as client:
            response = await client.get(f"{SALEBOT_API_BASE}/{api_key}/connected_channels")
    except httpx.HTTPError as exc:
        raise RuntimeError(f"SaleBot API недоступен: {type(exc).__name__}") from exc
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(f"SaleBot API вернул HTTP {response.status_code} без JSON") from exc
    if not response.is_success or not isinstance(body, dict):
        raise RuntimeError(f"SaleBot API отклонил ключ: HTTP {response.status_code}")
    expected = _clean(bot_username, 200).lstrip("@").casefold()
    channels = body.get("telegram") if isinstance(body.get("telegram"), list) else []
    match = next(
        (
            item for item in channels if isinstance(item, dict) and any(
                _clean(item.get(key), 200).lstrip("@").casefold() == expected
                for key in ("group_id", "short_name")
            )
        ),
        None,
    )
    if not match:
        raise RuntimeError(f"В этом SaleBot-проекте бот @{bot_username} не подключён")
    return {"project_id": _clean(body.get("project_id"), 100), "channel_id": _clean(match.get("id"), 100)}


async def _save_snapshot(bot_id: int, snapshot: dict[str, Any], *, error: str = "") -> None:
    current = _clean(snapshot.get("url"), 4000)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            UPDATE bots SET current_webhook_masked=?,pending_updates=?,last_check_at=?,last_check_error=?,updated_at=?
            WHERE id=?
            """,
            (
                _mask_url(current),
                int(snapshot.get("pending_update_count") or 0),
                _now(),
                _clean(error, 500),
                _now(),
                bot_id,
            ),
        )
        await db.commit()


async def _set_bot_error(bot_id: int, error: str, *, state: str | None = None) -> None:
    fields = "last_check_at=?,last_check_error=?,updated_at=?"
    values: list[Any] = [_now(), _clean(error, 500), _now()]
    if state:
        fields += ",state=?"
        values.append(state)
    values.append(bot_id)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(f"UPDATE bots SET {fields} WHERE id=?", tuple(values))
        await db.commit()


async def _capture_upstream(bot_id: int, url: str) -> None:
    if not _is_salebot_url(url):
        raise RuntimeError("Обнаруженный webhook не принадлежит SaleBot")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            UPDATE bots SET upstream_url_enc=?,upstream_masked=?,observed_url_enc='',observed_masked='',
            observed_count=0,updated_at=? WHERE id=?
            """,
            (_encrypt(url), _mask_url(url), _now(), bot_id),
        )
        await db.commit()


async def _observe_once(bot_id: int, *, force_activation: bool = False) -> dict[str, Any]:
    row = await _bot_row(bot_id)
    if not row:
        raise RuntimeError("Бот не найден")
    if row.get("transport") == "polling":
        return await _poll_once(row)
    settings = await _settings()
    own_url = _webhook_url(row, settings["public_base"])
    try:
        snapshot = await _telegram_snapshot(row)
    except Exception as exc:
        await _set_bot_error(bot_id, str(exc))
        raise
    current = _clean(snapshot.get("url"), 4000)
    await _save_snapshot(bot_id, snapshot)

    if current == own_url:
        error_message = _clean(snapshot.get("last_error_message"), 500).lower()
        pending = int(snapshot.get("pending_update_count") or 0)
        webhook_unreachable = pending > 0 and any(
            marker in error_message for marker in ("timed out", "timeout", "connection refused", "connection reset")
        )
        if webhook_unreachable and row.get("upstream_url_enc"):
            await _switch_to_polling(row, reason=_clean(snapshot.get("last_error_message"), 300))
            fresh = await _bot_row(bot_id)
            assert fresh
            result = await _poll_once(fresh)
            result["fallback"] = True
            return result
        if row.get("upstream_url_enc"):
            async with aiosqlite.connect(_must_db()) as db:
                await db.execute(
                    "UPDATE bots SET state='active',observed_count=0,observed_url_enc='',observed_masked='',last_check_error='',updated_at=? WHERE id=?",
                    (_now(), bot_id),
                )
                await db.commit()
        return {"action": "proxy_active", "snapshot": snapshot}

    if current and _is_salebot_url(current):
        observed = ""
        try:
            observed = _decrypt(row.get("observed_url_enc"))
        except Exception:
            observed = ""
        count = int(row.get("observed_count") or 0) + 1 if observed == current else 1
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "UPDATE bots SET observed_url_enc=?,observed_masked=?,observed_count=?,updated_at=? WHERE id=?",
                (_encrypt(current), _mask_url(current), count, _now(), bot_id),
            )
            await db.commit()
        should_activate = force_activation or (bool(row.get("auto_activate")) and row.get("state") == "waiting" and count >= 2)
        should_recover = bool(row.get("auto_recover")) and row.get("state") == "active" and count >= 2
        if should_activate or should_recover:
            await _capture_upstream(bot_id, current)
            fresh = await _bot_row(bot_id)
            assert fresh
            await _set_proxy_webhook(fresh, settings["public_base"])
            async with aiosqlite.connect(_must_db()) as db:
                await db.execute(
                    "UPDATE bots SET state='active',transport='webhook',poll_offset=0,current_webhook_masked=?,last_check_error='',updated_at=? WHERE id=?",
                    (_mask_url(own_url), _now(), bot_id),
                )
                await db.commit()
            _log("info", "proxy activated bot_id=%s username=%s", bot_id, row.get("username"))
            return {"action": "captured_and_activated", "snapshot": snapshot}
        return {"action": "salebot_observed", "stable_count": count, "snapshot": snapshot}

    if not current and row.get("state") == "active" and row.get("auto_recover") and row.get("upstream_url_enc"):
        await _set_proxy_webhook(row, settings["public_base"])
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "UPDATE bots SET transport='webhook',poll_offset=0,current_webhook_masked=?,updated_at=? WHERE id=?",
                (_mask_url(own_url), _now(), bot_id),
            )
            await db.commit()
        return {"action": "proxy_recovered", "snapshot": snapshot}

    if current and current != own_url:
        await _set_bot_error(bot_id, "Webhook занят другим сервисом; автоматическое переключение остановлено", state="conflict")
        return {"action": "conflict", "snapshot": snapshot}
    return {"action": "waiting", "snapshot": snapshot}


async def _cleanup_events() -> None:
    settings = await _settings()
    try:
        days = max(1, min(90, int(settings.get("retention_days") or 14)))
    except ValueError:
        days = 14
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM events WHERE received_at < ?", (cutoff,))
        await db.commit()


async def _watcher_loop() -> None:
    cleanup_tick = 0
    while not _stop_event.is_set():
        try:
            async with aiosqlite.connect(_must_db()) as db:
                cur = await db.execute("SELECT id FROM bots WHERE state IN ('waiting','active') ORDER BY id")
                bot_ids = [int(row[0]) for row in await cur.fetchall()]
            for bot_id in bot_ids:
                if _stop_event.is_set():
                    break
                try:
                    await _observe_once(bot_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log("warning", "watch check failed bot_id=%s error=%s", bot_id, _clean(exc, 300))
            cleanup_tick += 1
            if cleanup_tick >= 720:
                await _cleanup_events()
                cleanup_tick = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "watcher loop error: %s", _clean(exc, 300))
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=WATCH_INTERVAL_SECONDS)
        except TimeoutError:
            pass


@router.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "module": MODULE_ID,
        "encryption_ready": bool(_clean(os.environ.get("NEXUS_SECRET"), 10000)),
        "watcher_running": bool(_watcher_task and not _watcher_task.done()),
    }


@router.get("/status")
async def status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bots ORDER BY id DESC")
        rows = [dict(row) for row in await cur.fetchall()]
        cur = await db.execute(
            "SELECT COUNT(*),SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END),SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) FROM events"
        )
        totals = await cur.fetchone()
    return {
        "ok": True,
        "encryption_ready": bool(_clean(os.environ.get("NEXUS_SECRET"), 10000)),
        "watcher_running": bool(_watcher_task and not _watcher_task.done()),
        "settings": settings,
        "bots": [_public_bot(row, settings["public_base"]) for row in rows],
        "totals": {
            "events": int(totals[0] or 0),
            "delivered": int(totals[1] or 0),
            "failed": int(totals[2] or 0),
        },
    }


@router.post("/settings")
async def save_settings(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await request.json()
    public_base = _public_base(data.get("public_base"))
    try:
        retention_days = max(1, min(90, int(data.get("retention_days") or 14)))
    except (TypeError, ValueError):
        raise HTTPException(400, "Срок хранения должен быть числом от 1 до 90")
    active_exists = False
    for bot_id in await _all_bot_ids():
        active_exists = active_exists or (await _bot_row(bot_id) or {}).get("state") == "active"
    if active_exists:
        current = (await _settings())["public_base"]
        if public_base != current:
            raise HTTPException(409, "Сначала восстановите прямые webhook активных ботов")
    async with aiosqlite.connect(_must_db()) as db:
        for key, value in {"public_base": public_base, "retention_days": str(retention_days)}.items():
            await db.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, value, _now()),
            )
        await db.commit()
    _log("info", "settings updated by=%s", user.get("username"))
    return {"ok": True, "settings": await _settings()}


async def _all_bot_ids() -> list[int]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT id FROM bots")
        return [int(row[0]) for row in await cur.fetchall()]


@router.post("/bots")
async def add_bot(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await request.json()
    token = _clean(data.get("token"), 500)
    if not token or ":" not in token:
        raise HTTPException(400, "Введите bot token из BotFather")
    if not _clean(os.environ.get("NEXUS_SECRET"), 10000):
        raise HTTPException(503, "Сначала задайте NEXUS_SECRET")
    try:
        me = await _tg_call(token, "getMe")
        snapshot = await _tg_call(token, "getWebhookInfo")
    except Exception as exc:
        raise HTTPException(400, _clean(exc, 500))
    telegram_id = _clean(me.get("id"), 80)
    if not telegram_id:
        raise HTTPException(400, "Telegram не вернул ID бота")
    username = _clean(me.get("username"), 200)
    display_name = " ".join(part for part in (_clean(me.get("first_name"), 200), _clean(me.get("last_name"), 200)) if part)
    current = _clean(snapshot.get("url"), 4000)
    upstream_enc = _encrypt(current) if _is_salebot_url(current) else ""
    upstream_masked = _mask_url(current) if upstream_enc else ""
    existing = await _bot_by_telegram_id(telegram_id)
    async with aiosqlite.connect(_must_db()) as db:
        if existing:
            await db.execute(
                """
                UPDATE bots SET username=?,display_name=?,token_enc=?,token_hint=?,
                upstream_url_enc=CASE WHEN ?<>'' THEN ? ELSE upstream_url_enc END,
                upstream_masked=CASE WHEN ?<>'' THEN ? ELSE upstream_masked END,
                current_webhook_masked=?,pending_updates=?,last_check_at=?,last_check_error='',updated_at=?
                WHERE id=?
                """,
                (
                    username, display_name, _encrypt(token), _token_hint(token),
                    upstream_enc, upstream_enc, upstream_masked, upstream_masked,
                    _mask_url(current), int(snapshot.get("pending_update_count") or 0),
                    _now(), _now(), existing["id"],
                ),
            )
            bot_id = int(existing["id"])
        else:
            cur = await db.execute(
                """
                INSERT INTO bots(
                    telegram_id,username,display_name,token_enc,token_hint,upstream_url_enc,upstream_masked,
                    path_secret,telegram_secret,state,current_webhook_masked,pending_updates,last_check_at,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,'draft',?,?,?,?,?)
                """,
                (
                    telegram_id, username, display_name, _encrypt(token), _token_hint(token),
                    upstream_enc, upstream_masked, secrets.token_urlsafe(24), secrets.token_urlsafe(24),
                    _mask_url(current), int(snapshot.get("pending_update_count") or 0), _now(), _now(), _now(),
                ),
            )
            bot_id = int(cur.lastrowid)
        await db.commit()
    _log("info", "bot saved by=%s bot_id=%s username=%s upstream=%s", user.get("username"), bot_id, username, bool(upstream_enc))
    row = await _bot_row(bot_id)
    assert row
    settings = await _settings()
    return {"ok": True, "bot": _public_bot(row, settings["public_base"]), "captured": bool(upstream_enc)}


@router.post("/bots/{bot_id}/wait")
async def wait_for_salebot(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE bots SET state='waiting',auto_activate=1,last_check_error='',observed_count=0,observed_url_enc='',observed_masked='',updated_at=? WHERE id=?",
            (_now(), bot_id),
        )
        await db.commit()
    _log("info", "waiting enabled by=%s bot_id=%s", user.get("username"), bot_id)
    try:
        result = await _observe_once(bot_id)
    except Exception as exc:
        result = {"action": "error", "error": _clean(exc, 500)}
    return {"ok": True, "result": result}


@router.post("/bots/{bot_id}/activate")
async def activate(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    if not row.get("upstream_url_enc"):
        try:
            result = await _observe_once(bot_id, force_activation=True)
        except Exception as exc:
            raise HTTPException(409, _clean(exc, 500))
        row = await _bot_row(bot_id)
        if not row or not row.get("upstream_url_enc"):
            raise HTTPException(409, f"Сначала подключите бота в SaleBot: {result.get('action')}")
    settings = await _settings()
    try:
        await _set_proxy_webhook(row, settings["public_base"])
        snapshot = await _telegram_snapshot(row)
    except Exception as exc:
        await _set_bot_error(bot_id, str(exc), state="error")
        raise HTTPException(502, _clean(exc, 500))
    if _clean(snapshot.get("url"), 4000) != _webhook_url(row, settings["public_base"]):
        raise HTTPException(502, "Telegram не подтвердил webhook Nexus")
    await _save_snapshot(bot_id, snapshot)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE bots SET state='active',transport='webhook',poll_offset=0,last_check_error='',updated_at=? WHERE id=?",
            (_now(), bot_id),
        )
        await db.commit()
    _log("info", "proxy activated manually by=%s bot_id=%s", user.get("username"), bot_id)
    return {"ok": True, "state": "active"}


@router.post("/bots/{bot_id}/check")
async def check(bot_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    if not await _bot_row(bot_id):
        raise HTTPException(404, "Бот не найден")
    try:
        result = await _observe_once(bot_id)
    except Exception as exc:
        raise HTTPException(502, _clean(exc, 500))
    row = await _bot_row(bot_id)
    assert row
    settings = await _settings()
    return {"ok": True, "result": result, "bot": _public_bot(row, settings["public_base"])}


@router.post("/bots/{bot_id}/salebot")
async def connect_salebot_api(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    data = await request.json()
    api_key = _clean(data.get("api_key"), 1000)
    if not api_key:
        raise HTTPException(400, "Введите API-ключ SaleBot-проекта")
    try:
        matched = await _validate_salebot_api_key(api_key, row.get("username"))
    except RuntimeError as exc:
        raise HTTPException(400, str(exc)) from exc
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE bots SET salebot_api_key_enc=?,salebot_project_id=?,state='active',last_delivery_error='',updated_at=? WHERE id=?",
            (_encrypt(api_key), matched["project_id"], _now(), bot_id),
        )
        await db.commit()
    fresh = await _bot_row(bot_id)
    assert fresh
    try:
        if fresh.get("transport") != "polling":
            await _switch_to_polling(fresh, reason="SaleBot API подключён")
            fresh = await _bot_row(bot_id)
            assert fresh
        result = await _poll_once(fresh)
    except Exception as exc:
        await _set_bot_error(bot_id, str(exc), state="error")
        raise HTTPException(502, _clean(exc, 500)) from exc
    _log("info", "salebot api connected by=%s bot_id=%s project=%s", user.get("username"), bot_id, matched["project_id"])
    updated = await _bot_row(bot_id)
    assert updated
    settings = await _settings()
    return {"ok": True, "match": matched, "result": result, "bot": _public_bot(updated, settings["public_base"])}


@router.post("/bots/{bot_id}/restore")
async def restore(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    if not row.get("upstream_url_enc"):
        raise HTTPException(409, "URL SaleBot не сохранён; автоматический откат невозможен")
    try:
        await _set_upstream_webhook(row)
        snapshot = await _telegram_snapshot(row)
    except Exception as exc:
        await _set_bot_error(bot_id, str(exc), state="error")
        raise HTTPException(502, _clean(exc, 500))
    await _save_snapshot(bot_id, snapshot)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE bots SET state='restored',transport='webhook',poll_offset=0,auto_activate=0,observed_count=0,observed_url_enc='',observed_masked='',last_check_error='',updated_at=? WHERE id=?",
            (_now(), bot_id),
        )
        await db.commit()
    _log("info", "direct salebot webhook restored by=%s bot_id=%s", user.get("username"), bot_id)
    return {"ok": True, "state": "restored", "current_webhook": _mask_url(snapshot.get("url"))}


@router.patch("/bots/{bot_id}")
async def update_bot(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    data = await request.json()
    auto_recover = 1 if _truthy(data.get("auto_recover")) else 0
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("UPDATE bots SET auto_recover=?,updated_at=? WHERE id=?", (auto_recover, _now(), bot_id))
        await db.commit()
    _log("info", "bot settings updated by=%s bot_id=%s", user.get("username"), bot_id)
    fresh = await _bot_row(bot_id)
    assert fresh
    settings = await _settings()
    return {"ok": True, "bot": _public_bot(fresh, settings["public_base"])}


@router.delete("/bots/{bot_id}")
async def delete_bot(bot_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    row = await _bot_row(bot_id)
    if not row:
        raise HTTPException(404, "Бот не найден")
    if row.get("state") == "active":
        raise HTTPException(409, "Сначала выполните откат на SaleBot")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM bots WHERE id=?", (bot_id,))
        await db.commit()
    _bot_locks.pop(bot_id, None)
    _log("info", "bot deleted by=%s bot_id=%s", user.get("username"), bot_id)
    return {"ok": True}


@router.get("/events")
async def events(request: Request, bot_id: int = 0, limit: int = 50) -> dict[str, Any]:
    await _require_admin(request)
    limit = max(1, min(200, int(limit or 50)))
    where = "WHERE e.bot_id=?" if bot_id else ""
    params: tuple[Any, ...] = (bot_id, limit) if bot_id else (limit,)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT e.id,e.bot_id,e.update_id,e.event_type,e.chat_id,e.actor_id,e.status,e.attempts,
                   e.response_code,e.duration_ms,e.error,e.received_at,e.delivered_at,e.updated_at,
                   b.username,b.display_name
            FROM events e JOIN bots b ON b.id=e.bot_id
            {where} ORDER BY e.id DESC LIMIT ?
            """,
            params,
        )
        rows = [dict(row) for row in await cur.fetchall()]
    return {"ok": True, "events": rows}


def _salebot_callback_payload(row: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    event_type, chat_id, actor_id = _event_meta(payload)
    message = payload.get("message") or payload.get("edited_message") or payload.get("business_message")
    if not isinstance(message, dict):
        callback = payload.get("callback_query")
        message = callback.get("message") if isinstance(callback, dict) and isinstance(callback.get("message"), dict) else {}
    text = _clean(message.get("text") or message.get("caption"), 4000)
    callback = payload.get("callback_query")
    if not text and isinstance(callback, dict):
        text = _clean(callback.get("data"), 4000)
    return {
        "message": text or f"telegram_{event_type}",
        "user_id": actor_id or chat_id,
        "group_id": _clean(row.get("username"), 200).lstrip("@"),
        "resume_bot": True,
        "telegram_update_id": _clean(payload.get("update_id"), 80),
        "telegram_event_type": event_type,
        "tg_request": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    }


async def _forward_event(row: dict[str, Any], event_id: int, body: bytes) -> Response:
    api_key = _decrypt(row.get("salebot_api_key_enc")) if row.get("salebot_api_key_enc") else ""
    target = _decrypt(row.get("upstream_url_enc")) if row.get("upstream_url_enc") else ""
    if not api_key and not _is_salebot_url(target):
        raise RuntimeError("SaleBot API или безопасный webhook не настроен")
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=25, follow_redirects=False, trust_env=False) as client:
            if api_key:
                payload = json.loads(body)
                upstream = await client.post(
                    f"{SALEBOT_API_BASE}/{api_key}/tg_callback",
                    json=_salebot_callback_payload(row, payload),
                )
            else:
                upstream = await client.post(
                    target,
                    content=body,
                    headers={"Content-Type": "application/json", "User-Agent": "Nexus-Telegram-SaleBot-Proxy/1.0"},
                )
        duration_ms = int((time.monotonic() - started) * 1000)
        ok = 200 <= upstream.status_code < 300
        if api_key and ok:
            try:
                response_body = upstream.json()
            except ValueError:
                response_body = None
            if isinstance(response_body, dict) and (
                response_body.get("success") is False or response_body.get("status") == "error" or response_body.get("error")
            ):
                ok = False
        needs_key = not api_key and upstream.status_code in {401, 403}
        error = "" if ok else ("Подключите API-ключ этого SaleBot-проекта" if needs_key else f"SaleBot HTTP {upstream.status_code}")
        response_status = upstream.status_code if ok or upstream.status_code >= 400 else 502
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                """
                UPDATE events SET status=?,attempts=attempts+1,response_code=?,duration_ms=?,error=?,
                raw_payload=CASE WHEN ? THEN '' ELSE raw_payload END,
                delivered_at=CASE WHEN ? THEN ? ELSE delivered_at END,updated_at=? WHERE id=?
                """,
                ("delivered" if ok else "failed", upstream.status_code, duration_ms, error, ok, ok, _now(), _now(), event_id),
            )
            await db.execute(
                """
                UPDATE bots SET delivered_count=delivered_count+?,failed_count=failed_count+?,last_delivery_at=?,
                last_delivery_error=?,state=CASE WHEN ? THEN 'needs_salebot_key' ELSE state END,updated_at=? WHERE id=?
                """,
                (1 if ok else 0, 0 if ok else 1, _now() if ok else row.get("last_delivery_at", ""), error, needs_key, _now(), row["id"]),
            )
            await db.commit()
        content_type = upstream.headers.get("content-type", "application/json").split(";", 1)[0]
        return Response(content=upstream.content, status_code=response_status, media_type=content_type)
    except httpx.HTTPError as exc:
        duration_ms = int((time.monotonic() - started) * 1000)
        error = f"SaleBot недоступен: {type(exc).__name__}"
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "UPDATE events SET status='failed',attempts=attempts+1,duration_ms=?,error=?,updated_at=? WHERE id=?",
                (duration_ms, error, _now(), event_id),
            )
            await db.execute(
                "UPDATE bots SET failed_count=failed_count+1,last_delivery_error=?,updated_at=? WHERE id=?",
                (error, _now(), row["id"]),
            )
            await db.commit()
        return Response(content=json.dumps({"ok": False, "error": error}, ensure_ascii=False), status_code=502, media_type="application/json")


async def _process_update(row: dict[str, Any], payload: dict[str, Any], body: bytes | None = None) -> Response:
    if row.get("state") != "active" or not row.get("upstream_url_enc"):
        return Response(content='{"ok":false,"error":"proxy inactive"}', status_code=503, media_type="application/json")
    body = body if body is not None else json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_UPDATE_BYTES:
        return Response(status_code=413)
    update_id = _clean(payload.get("update_id"), 80)
    if not update_id:
        return Response(status_code=400)
    event_type, chat_id, actor_id = _event_meta(payload)
    lock = _bot_locks.setdefault(int(row["id"]), asyncio.Lock())
    async with lock:
        async with aiosqlite.connect(_must_db()) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM events WHERE bot_id=? AND update_id=?", (row["id"], update_id))
            existing = await cur.fetchone()
            if existing and existing["status"] == "delivered":
                return Response(content='{"ok":true,"duplicate":true}', status_code=200, media_type="application/json")
            if existing:
                event_id = int(existing["id"])
                await db.execute(
                    "UPDATE events SET status='received',raw_payload=?,error='',updated_at=? WHERE id=?",
                    (body.decode("utf-8", errors="replace"), _now(), event_id),
                )
            else:
                cur = await db.execute(
                    """
                    INSERT INTO events(bot_id,update_id,event_type,chat_id,actor_id,status,raw_payload,received_at,updated_at)
                    VALUES(?,?,?,?,?,'received',?,?,?)
                    """,
                    (row["id"], update_id, event_type, chat_id, actor_id, body.decode("utf-8", errors="replace"), _now(), _now()),
                )
                event_id = int(cur.lastrowid)
                await db.execute(
                    "UPDATE bots SET received_count=received_count+1,last_update_at=?,updated_at=? WHERE id=?",
                    (_now(), _now(), row["id"]),
                )
            await db.commit()
        fresh = await _bot_row(int(row["id"]))
        assert fresh
        return await _forward_event(fresh, event_id, body)


async def _switch_to_polling(row: dict[str, Any], *, reason: str) -> None:
    await _tg_call(_decrypt(row["token_enc"]), "deleteWebhook", {"drop_pending_updates": False})
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            UPDATE bots SET transport='polling',current_webhook_masked='Long polling · Bot API proxy',
            last_check_error='',updated_at=? WHERE id=?
            """,
            (_now(), row["id"]),
        )
        await db.commit()
    _log("warning", "webhook unavailable; polling enabled bot_id=%s reason=%s", row["id"], _clean(reason, 200))


async def _poll_once(row: dict[str, Any]) -> dict[str, Any]:
    result = await _tg_call(
        _decrypt(row["token_enc"]),
        "getUpdates",
        {"offset": int(row.get("poll_offset") or 0), "limit": 100, "timeout": 0},
    )
    updates = result.get("value") if isinstance(result, dict) else None
    if not isinstance(updates, list):
        raise RuntimeError("Telegram getUpdates вернул некорректный ответ")
    delivered = 0
    next_offset = int(row.get("poll_offset") or 0)
    for payload in updates:
        if not isinstance(payload, dict):
            continue
        try:
            update_id = int(payload.get("update_id"))
        except (TypeError, ValueError):
            continue
        response = await _process_update(row, payload)
        if not 200 <= response.status_code < 300:
            break
        delivered += 1
        next_offset = max(next_offset, update_id + 1)
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "UPDATE bots SET poll_offset=?,pending_updates=CASE WHEN pending_updates>0 THEN pending_updates-1 ELSE 0 END,updated_at=? WHERE id=?",
                (next_offset, _now(), row["id"]),
            )
            await db.commit()
    return {"action": "polling_active", "received": len(updates), "delivered": delivered, "offset": next_offset}


@router.post("/webhook/{telegram_id}/{path_secret}")
async def telegram_webhook(telegram_id: str, path_secret: str, request: Request) -> Response:
    row = await _bot_by_telegram_id(_clean(telegram_id, 80))
    if not row or not hmac.compare_digest(str(row.get("path_secret") or ""), _clean(path_secret, 200)):
        return Response(status_code=404)
    header_secret = _clean(request.headers.get("x-telegram-bot-api-secret-token"), 300)
    if not hmac.compare_digest(str(row.get("telegram_secret") or ""), header_secret):
        return Response(status_code=403)
    body = await request.body()
    if len(body) > MAX_UPDATE_BYTES:
        return Response(status_code=413)
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return Response(status_code=400)
    if not isinstance(payload, dict):
        return Response(status_code=400)
    return await _process_update(row, payload, body)


@router.post("/events/{event_id}/replay")
async def replay_event(event_id: int, request: Request) -> Response:
    user = await _require_admin(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT e.*,b.* FROM events e JOIN bots b ON b.id=e.bot_id WHERE e.id=?",
            (event_id,),
        )
        joined = await cur.fetchone()
    if not joined:
        raise HTTPException(404, "Событие не найдено")
    data = dict(joined)
    body = str(data.get("raw_payload") or "").encode("utf-8")
    if not body:
        raise HTTPException(409, "Успешное событие уже очищено и не требует повтора")
    bot = await _bot_row(int(data["bot_id"]))
    if not bot:
        raise HTTPException(404, "Бот не найден")
    _log("info", "event replay requested by=%s event_id=%s", user.get("username"), event_id)
    return await _forward_event(bot, event_id, body)
