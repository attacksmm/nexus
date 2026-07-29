from __future__ import annotations

import asyncio
import os
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx


DEFAULT_BOT_API_BASE = "https://api.telegram.org"

BOT_API_BASE_KEYS = (
    "TELEGRAM_BOT_API_BASE",
    "SBKVD_LETTER_TELEGRAM_API_BASE",
)
BOT_API_PROXY_KEYS = (
    "TELEGRAM_BOT_API_PROXY_URL",
    "TELEGRAM_HTTPS_PROXY_URL",
    "SBKVD_LETTER_TELEGRAM_PROXY_URL",
)
MTPROTO_PROXY_KEYS = (
    "TELEGRAM_MTPROTO_PROXY_URL",
    "TELEGRAM_MTPROTO_PROXY",
    "TELEGRAM_PROXY_URL",
)


def _clean(value: Any, limit: int = 4000) -> str:
    return str(value or "").strip()[:limit]


def _first_env(keys: tuple[str, ...]) -> str:
    for key in keys:
        value = _clean(os.environ.get(key))
        if value:
            return value
    return ""


def telegram_bot_api_base(*fallbacks: Any) -> str:
    value = _first_env(BOT_API_BASE_KEYS)
    if not value:
        value = next((_clean(item) for item in fallbacks if _clean(item)), "")
    return (value or DEFAULT_BOT_API_BASE).rstrip("/")


def telegram_bot_api_proxy_url(*fallbacks: Any) -> str:
    # Canonical Nexus value always wins over module-local legacy settings.
    canonical = _clean(os.environ.get(BOT_API_PROXY_KEYS[0]))
    if canonical:
        return canonical
    value = next((_clean(item) for item in fallbacks if _clean(item)), "")
    return value or _first_env(BOT_API_PROXY_KEYS[1:])


def telegram_mtproto_proxy_url(*fallbacks: Any) -> str:
    canonical = _clean(os.environ.get(MTPROTO_PROXY_KEYS[0]))
    if canonical:
        return canonical
    value = next((_clean(item) for item in fallbacks if _clean(item)), "")
    return value or _first_env(MTPROTO_PROXY_KEYS[1:])


def validate_bot_api_base(value: Any) -> str:
    raw = _clean(value) or DEFAULT_BOT_API_BASE
    parsed = urlparse(raw)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Bot API base должен быть HTTPS-адресом без логина, query и fragment")
    return raw.rstrip("/")


def validate_bot_api_proxy(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https", "socks5", "socks5h"} or not parsed.hostname or not parsed.port:
        raise ValueError("Bot API proxy: используйте http(s)://host:port или socks5(h)://host:port")
    return raw


def mtproto_proxy_parts(value: Any) -> tuple[str, int, str]:
    raw = _clean(value)
    if not raw:
        raise ValueError("MTProto proxy не задан")
    parsed = urlparse(raw)
    query = parse_qs(parsed.query)
    if parsed.scheme in {"http", "https", "tg"} and (
        (parsed.hostname or "").lower() == "t.me" or parsed.scheme == "tg"
    ):
        server = _clean((query.get("server") or [""])[0], 300)
        port_raw = _clean((query.get("port") or [""])[0], 20)
        secret = _clean((query.get("secret") or [""])[0], 300)
    else:
        server = _clean(parsed.hostname, 300)
        port_raw = str(parsed.port or "")
        secret = _clean((query.get("secret") or [""])[0] or parsed.password, 300)
    if not server or not port_raw or not secret:
        raise ValueError("MTProto proxy: нужны server, port и secret")
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError("MTProto proxy: некорректный port") from exc
    if not 1 <= port <= 65535:
        raise ValueError("MTProto proxy: port вне диапазона 1–65535")
    return server, port, secret


def validate_mtproto_proxy(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    mtproto_proxy_parts(raw)
    return raw


def masked_proxy(value: Any, *, kind: str) -> str:
    raw = _clean(value)
    if not raw:
        return "не задан"
    if kind == "mtproto":
        try:
            server, port, _ = mtproto_proxy_parts(raw)
            return f"{server}:{port} · secret ••••"
        except ValueError:
            return "задан, формат не распознан"
    parsed = urlparse(raw)
    if not parsed.hostname:
        return "задан, формат не распознан"
    auth = " · auth ••••" if parsed.username or parsed.password else ""
    return f"{parsed.scheme}://{parsed.hostname}:{parsed.port or '—'}{auth}"


def httpx_client_kwargs(*, timeout: Any = 15, proxy_url: str | None = None) -> dict[str, Any]:
    # An explicit value is used by the settings preflight. Runtime callers omit
    # it and receive the canonical Nexus route (with legacy fallbacks).
    proxy = validate_bot_api_proxy(proxy_url) if proxy_url is not None else telegram_bot_api_proxy_url()
    kwargs: dict[str, Any] = {"timeout": timeout, "trust_env": False}
    if proxy:
        kwargs["proxy"] = proxy
    return kwargs


def telethon_proxy_config(value: str | None = None) -> tuple[Any | None, tuple[str, int, str] | None]:
    # Keep an explicit candidate ahead of ENV so Settings can test a new proxy
    # before making it global.
    raw = validate_mtproto_proxy(value) if value is not None else telegram_mtproto_proxy_url()
    if not raw:
        return None, None
    server, port, secret = mtproto_proxy_parts(raw)
    from telethon import connection

    return connection.ConnectionTcpMTProxyRandomizedIntermediate, (server, port, secret)


async def test_bot_api_route(*, base: str = "", proxy: str = "", token: str = "") -> dict[str, Any]:
    api_base = validate_bot_api_base(base or telegram_bot_api_base())
    proxy_url = validate_bot_api_proxy(proxy or telegram_bot_api_proxy_url())
    url = f"{api_base}/bot{token}/getMe" if token else api_base
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(**httpx_client_kwargs(timeout=httpx.Timeout(15, connect=10), proxy_url=proxy_url)) as client:
            response = await client.get(url)
        ok = response.status_code < 500
        return {
            "ok": ok,
            "status_code": response.status_code,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "message": "Telegram Bot API доступен" if ok else f"Telegram HTTP {response.status_code}",
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "message": f"{type(exc).__name__}: {_clean(exc, 300)}",
        }


async def test_mtproto_route(*, proxy: str = "", api_id: str = "", api_hash: str = "") -> dict[str, Any]:
    proxy_url = validate_mtproto_proxy(proxy or telegram_mtproto_proxy_url())
    if not proxy_url:
        return {"ok": False, "duration_ms": 0, "message": "MTProto proxy не задан"}
    api_id = _clean(api_id or os.environ.get("TELEGRAM_API_ID"), 30)
    api_hash = _clean(api_hash or os.environ.get("TELEGRAM_API_HASH"), 200)
    if not api_id or not api_hash:
        return {"ok": False, "duration_ms": 0, "message": "TELEGRAM_API_ID/TELEGRAM_API_HASH не заданы"}
    started = time.monotonic()
    client = None
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        conn, parsed_proxy = telethon_proxy_config(proxy_url)
        client = TelegramClient(
            StringSession(),
            int(api_id),
            api_hash,
            connection=conn,
            proxy=parsed_proxy,
            timeout=8,
            connection_retries=1,
            request_retries=1,
        )
        await asyncio.wait_for(client.connect(), timeout=20)
        ok = bool(client.is_connected())
        return {
            "ok": ok,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "message": "Telegram MTProto доступен" if ok else "Telethon не подтвердил соединение",
        }
    except Exception as exc:
        return {
            "ok": False,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "message": f"{type(exc).__name__}: {_clean(exc, 300)}",
        }
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
