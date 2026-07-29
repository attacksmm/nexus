from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from orchestrator.auth import (
    ENV_PATH,
    _read_env_values,
    _write_env_values,
    can_access_module,
    enforce_rate_limit,
    require_admin,
    verify_token_from_request,
)
from orchestrator.credentials import (
    ENV_KEY_RE,
    clean,
    current_value,
    inventory,
    now,
    provider_for_key,
    save_value,
    save_values,
    status_item,
    validate_known,
)


router = APIRouter()
MODULE_ID = "token-vault"
VK_ID_AUTHORIZE_URL = "https://id.vk.ru/authorize"
VK_ID_TOKEN_URL = "https://id.vk.ru/oauth2/auth"
VK_ID_DEFAULT_SCOPES = "vkid.personal_info"
VK_ID_REFRESH_SECONDS = 180 * 24 * 60 * 60
VK_ID_REFRESH_MARGIN_SECONDS = 10 * 60
VK_ID_ATTEMPT_TTL_SECONDS = 10 * 60

_module_dir: Path | None = None
_logger = None
_restart_modules_for_env = None
_lifecycle = None
_refresh_lock = asyncio.Lock()
_oauth_attempts: dict[str, tuple[str, float]] = {}
_last_error = ""


def setup(ctx):
    global _module_dir, _logger, _restart_modules_for_env, _lifecycle
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", None)
    _restart_modules_for_env = getattr(ctx, "restart_modules_for_env", None)
    _lifecycle = getattr(ctx, "lifecycle", None)
    if _lifecycle:
        _lifecycle.create_task(_refresh_loop(), name="token-vault-vk-id-refresh")


def _base_dir() -> Path:
    if _module_dir is None:
        return Path(__file__).resolve().parents[1]
    if _module_dir.parent.name == "modules":
        return _module_dir.parent.parent
    return _module_dir.parent


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user) or not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "admin only")
    return user


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


_clean = clean
_now = now
_provider_for_key = provider_for_key


def _normalize_scopes(value: Any) -> str:
    parts = []
    for raw in str(value or VK_ID_DEFAULT_SCOPES).replace(",", " ").split():
        if not raw.replace("_", "").replace("-", "").replace(".", "").isalnum():
            raise ValueError("Некорректный scope")
        if raw not in parts:
            parts.append(raw)
    if not parts or len(parts) > 20:
        raise ValueError("Некорректный список scope")
    return " ".join(parts)


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _redirect_uri(request: Request) -> str:
    configured = _current_value("VK_ID_REDIRECT_URI")
    if configured:
        return configured
    root_path = request.scope.get("root_path", "").rstrip("/")
    return f"{request.url.scheme}://{request.url.netloc}{root_path}/{MODULE_ID}/api/vk/callback"


def _settings_url(request: Request, result: str) -> str:
    root_path = request.scope.get("root_path", "").rstrip("/")
    return f"{root_path}/settings?tab=credentials&vk_id={result}"


def _callback_values(request: Request) -> dict[str, str]:
    values = {key: str(value) for key, value in request.query_params.items()}
    payload = values.get("payload", "")
    if payload:
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = {}
        if isinstance(decoded, dict):
            values.update({str(key): str(value) for key, value in decoded.items() if value is not None})
    return values


def _timestamp(value: str) -> int:
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


def _iso(timestamp: int) -> str:
    if not timestamp:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def _vk_id_status(request: Request) -> dict[str, Any]:
    now_epoch = int(time.time())
    access_expires = _timestamp(_current_value("VK_ID_ACCESS_EXPIRES_AT"))
    refresh_expires = _timestamp(_current_value("VK_ID_REFRESH_EXPIRES_AT"))
    access_present = bool(_current_value("VK_USER_TOKEN"))
    refresh_present = bool(_current_value("VK_ID_REFRESH_TOKEN"))
    blocked = _current_value("VK_ID_REFRESH_BLOCKED") == "1"
    scopes = _current_value("VK_ID_SCOPES") or VK_ID_DEFAULT_SCOPES
    granted_scopes = _current_value("VK_ID_GRANTED_SCOPES")
    return {
        "ok": True,
        "configured": bool(_current_value("VK_ID_APP_ID")),
        "app_id": _current_value("VK_ID_APP_ID"),
        "client_secret_present": bool(_current_value("VK_ID_CLIENT_SECRET")),
        "service_token_present": bool(_current_value("VK_ID_SERVICE_TOKEN")),
        "scopes": scopes,
        "granted_scopes": granted_scopes,
        "callback_url": _redirect_uri(request),
        "connected": access_present and refresh_present and not blocked,
        "user_id": _current_value("VK_ID_USER_ID"),
        "access_present": access_present,
        "access_expires_at": _iso(access_expires),
        "access_expires_in": access_expires - now_epoch if access_expires else 0,
        "refresh_present": refresh_present,
        "refresh_expires_at": _iso(refresh_expires),
        "refresh_expires_in": refresh_expires - now_epoch if refresh_expires else 0,
        "last_refresh_at": _current_value("VK_ID_LAST_REFRESH_AT"),
        "last_error": _last_error or _current_value("VK_ID_LAST_ERROR"),
        "needs_reauthorization": blocked or (access_present and not refresh_present) or bool(
            refresh_present and refresh_expires and refresh_expires <= now_epoch
        ),
        "messages_requested": "messages" in scopes.split(),
        "messages_granted": "messages" in granted_scopes.split(),
    }


def _oauth_error(body: Any, status_code: int = 0) -> str:
    if isinstance(body, dict):
        code = clean(body.get("error"), 80)
        description = clean(body.get("error_description"), 260)
        if code or description:
            return ": ".join(part for part in (code, description) if part)
    return f"VK ID HTTP {status_code}" if status_code else "Некорректный ответ VK ID"


async def _token_request(data: dict[str, str]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10), trust_env=True) as client:
        response = await client.post(VK_ID_TOKEN_URL, data=data)
    try:
        body = response.json()
    except ValueError as exc:
        raise RuntimeError(f"VK ID HTTP {response.status_code}: некорректный JSON") from exc
    if response.status_code >= 400 or not isinstance(body, dict) or body.get("error"):
        raise RuntimeError(_oauth_error(body, response.status_code))
    return body


def _token_updates(body: dict[str, Any], *, device_id: str, expected_state: str) -> dict[str, str]:
    if not secrets.compare_digest(str(body.get("state") or ""), expected_state):
        raise RuntimeError("VK ID вернул другой state")
    access_token = str(body.get("access_token") or "").strip()
    refresh_token = str(body.get("refresh_token") or "").strip()
    try:
        expires_in = int(body.get("expires_in") or 0)
    except (TypeError, ValueError):
        expires_in = 0
    if not access_token or not refresh_token or expires_in <= 0:
        raise RuntimeError("VK ID не вернул полную пару токенов")
    issued_at = int(time.time())
    return {
        "VK_USER_TOKEN": access_token,
        "VK_ID_REFRESH_TOKEN": refresh_token,
        "VK_ID_DEVICE_ID": device_id,
        "VK_ID_ACCESS_EXPIRES_AT": str(issued_at + expires_in),
        "VK_ID_REFRESH_EXPIRES_AT": str(issued_at + VK_ID_REFRESH_SECONDS),
        "VK_ID_USER_ID": str(body.get("user_id") or ""),
        "VK_ID_GRANTED_SCOPES": _normalize_scopes(body.get("scope") or VK_ID_DEFAULT_SCOPES),
        "VK_ID_LAST_REFRESH_AT": _iso(issued_at),
        "VK_ID_REFRESH_BLOCKED": "0",
        "VK_ID_LAST_ERROR": "",
    }


async def _save_token_updates(updates: dict[str, str]) -> dict[str, Any]:
    return await save_values(
        updates,
        read_env=_read_env_values,
        write_env=_write_env_values,
        restart=_restart_modules_for_env if callable(_restart_modules_for_env) else None,
        restart_key="VK_USER_TOKEN",
    )


async def _block_refresh(message: str) -> None:
    global _last_error
    _last_error = clean(message, 300)
    await save_values(
        {"VK_ID_REFRESH_BLOCKED": "1", "VK_ID_LAST_ERROR": _last_error},
        read_env=_read_env_values,
        write_env=_write_env_values,
    )


async def _refresh_vk_id(*, force: bool = False) -> dict[str, Any]:
    global _last_error
    async with _refresh_lock:
        if _current_value("VK_ID_REFRESH_BLOCKED") == "1":
            return {"ok": False, "refreshed": False, "reauthorize": True, "message": "Нужна повторная авторизация"}
        app_id = _current_value("VK_ID_APP_ID")
        refresh_token = _current_value("VK_ID_REFRESH_TOKEN")
        device_id = _current_value("VK_ID_DEVICE_ID")
        expires_at = _timestamp(_current_value("VK_ID_ACCESS_EXPIRES_AT"))
        if not app_id or not refresh_token or not device_id:
            return {"ok": False, "refreshed": False, "message": "VK ID не подключён"}
        if not force and expires_at - int(time.time()) > VK_ID_REFRESH_MARGIN_SECONDS:
            return {"ok": True, "refreshed": False, "message": "Токен ещё действует"}
        state = secrets.token_urlsafe(32)
        request_data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": app_id,
            "device_id": device_id,
            "state": state,
        }
        service_token = _current_value("VK_ID_SERVICE_TOKEN")
        if service_token:
            request_data["service_token"] = service_token
        try:
            body = await _token_request(request_data)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            message = "Ответ VK ID потерян; Refresh token нельзя использовать повторно"
            await _block_refresh(message)
            _log("error", "vk id refresh transport uncertainty error=%s", type(exc).__name__)
            return {"ok": False, "refreshed": False, "reauthorize": True, "message": message}
        except Exception as exc:
            _last_error = clean(exc, 300)
            permanent = any(marker in _last_error for marker in ("invalid_token", "access_denied", "invalid_client"))
            if permanent:
                await _block_refresh(_last_error)
            _log("warning", "vk id refresh rejected error=%s", _last_error)
            return {"ok": False, "refreshed": False, "reauthorize": permanent, "message": _last_error}
        try:
            updates = _token_updates(body, device_id=device_id, expected_state=state)
        except Exception as exc:
            await _block_refresh(str(exc))
            return {"ok": False, "refreshed": False, "reauthorize": True, "message": clean(exc, 300)}
        restart = await _save_token_updates(updates)
        _last_error = ""
        _log(
            "info",
            "vk id token refreshed user_id=%s scope=%s restarted=%s failed=%s",
            updates["VK_ID_USER_ID"],
            updates["VK_ID_GRANTED_SCOPES"],
            restart.get("restarted", 0),
            restart.get("failed", 0),
        )
        return {"ok": bool(restart.get("ok", False)), "refreshed": True, "restart": restart}


async def _refresh_loop() -> None:
    while True:
        try:
            await _refresh_vk_id(force=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "vk id refresh loop error=%s", clean(exc, 300))
        await asyncio.sleep(60)


def _module_rows() -> list[dict[str, Any]]:
    db_path = _base_dir() / "data" / "nexus.db"
    if not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT id,name,status,manifest_json FROM modules ORDER BY name"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def _inventory() -> list[dict[str, Any]]:
    return inventory(_module_rows(), read_env=_read_env_values)


def _current_value(key: str) -> str:
    return current_value(key, read_env=_read_env_values)


async def _validate_known(key: str, value: str) -> dict[str, Any]:
    return await validate_known(
        key,
        value,
        base_dir=_base_dir(),
        read_env=_read_env_values,
    )


async def _status_item(
    item: dict[str, Any], *, validate: bool = False
) -> dict[str, Any]:
    return await status_item(
        item,
        base_dir=_base_dir(),
        validate=validate,
        read_env=_read_env_values,
    )


@router.get("/status")
async def status(request: Request, validate: int = 0):
    await _require_admin(request)
    items = [await _status_item(item, validate=bool(validate)) for item in _inventory()]
    return {"ok": True, "items": items, "env_path": str(ENV_PATH), "updated_at": _now()}


@router.get("/vk-id/status")
async def vk_id_status(request: Request):
    await _require_admin(request)
    return _vk_id_status(request)


@router.post("/vk-id/config")
async def vk_id_config(request: Request):
    user = await _require_admin(request)
    enforce_rate_limit(
        request,
        "token-vault-vk-id-config",
        limit=20,
        window_seconds=600,
        subject=user.get("username", ""),
    )
    data = await request.json()
    app_id = clean(data.get("app_id"), 40) or _current_value("VK_ID_APP_ID")
    if not app_id.isdigit():
        raise HTTPException(400, "ID приложения должен быть числом")
    try:
        scopes = _normalize_scopes(data.get("scopes") or _current_value("VK_ID_SCOPES"))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    values = {"VK_ID_APP_ID": app_id, "VK_ID_SCOPES": scopes}
    client_secret = str(data.get("client_secret") or "").strip()
    service_token = str(data.get("service_token") or "").strip()
    if client_secret:
        values["VK_ID_CLIENT_SECRET"] = client_secret[:2000]
    if service_token:
        values["VK_ID_SERVICE_TOKEN"] = service_token[:4000]
    await save_values(values, read_env=_read_env_values, write_env=_write_env_values)
    _log("info", "vk id config updated by=%s app_id=%s scopes=%s", user.get("username"), app_id, scopes)
    return _vk_id_status(request)


@router.post("/vk-id/start")
async def vk_id_start(request: Request):
    user = await _require_admin(request)
    enforce_rate_limit(
        request,
        "token-vault-vk-id-start",
        limit=10,
        window_seconds=600,
        subject=user.get("username", ""),
    )
    app_id = _current_value("VK_ID_APP_ID")
    if not app_id:
        raise HTTPException(400, "Сначала сохраните ID приложения")
    if not _current_value("VK_ID_SERVICE_TOKEN"):
        raise HTTPException(400, "Для серверного обмена нужен сервисный ключ")
    now_monotonic = time.monotonic()
    for key, (_, expires_at) in list(_oauth_attempts.items()):
        if expires_at <= now_monotonic:
            _oauth_attempts.pop(key, None)
    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(48)
    _oauth_attempts[state] = (verifier, now_monotonic + VK_ID_ATTEMPT_TTL_SECONDS)
    if len(_oauth_attempts) > 20:
        oldest = min(_oauth_attempts, key=lambda key: _oauth_attempts[key][1])
        _oauth_attempts.pop(oldest, None)
    query = urlencode(
        {
            "response_type": "code",
            "client_id": app_id,
            "redirect_uri": _redirect_uri(request),
            "state": state,
            "code_challenge": _pkce_challenge(verifier),
            "code_challenge_method": "s256",
            "scope": _current_value("VK_ID_SCOPES") or VK_ID_DEFAULT_SCOPES,
            "prompt": "consent",
            "lang_id": "0",
            "scheme": "dark",
        }
    )
    _log("info", "vk id authorization started by=%s", user.get("username"))
    return {"ok": True, "authorization_url": f"{VK_ID_AUTHORIZE_URL}?{query}"}


@router.get("/vk/callback")
@router.get("/vk-id/callback")
async def vk_id_callback(request: Request):
    global _last_error
    values = _callback_values(request)
    state = clean(values.get("state"), 200)
    attempt = _oauth_attempts.pop(state, None) if state else None
    if not attempt or attempt[1] <= time.monotonic():
        return PlainTextResponse("VK ID state истёк или не совпал", status_code=400)
    verifier = attempt[0]
    if values.get("error"):
        _last_error = _oauth_error(values)
        _log("warning", "vk id authorization rejected error=%s", _last_error)
        return RedirectResponse(_settings_url(request, "error"), status_code=303)
    code = clean(values.get("code"), 2000)
    device_id = clean(values.get("device_id"), 1000)
    if not code or not device_id:
        _last_error = "VK ID не вернул code или device_id"
        return RedirectResponse(_settings_url(request, "error"), status_code=303)
    request_data = {
        "grant_type": "authorization_code",
        "code_verifier": verifier,
        "redirect_uri": _redirect_uri(request),
        "code": code,
        "client_id": _current_value("VK_ID_APP_ID"),
        "device_id": device_id,
        "state": state,
        "service_token": _current_value("VK_ID_SERVICE_TOKEN"),
    }
    try:
        body = await _token_request(request_data)
        updates = _token_updates(body, device_id=device_id, expected_state=state)
        restart = await _save_token_updates(updates)
    except Exception as exc:
        _last_error = clean(exc, 300)
        _log("error", "vk id authorization exchange failed error=%s", _last_error)
        return RedirectResponse(_settings_url(request, "error"), status_code=303)
    _last_error = ""
    _log(
        "info",
        "vk id connected user_id=%s scope=%s restarted=%s failed=%s",
        updates["VK_ID_USER_ID"],
        updates["VK_ID_GRANTED_SCOPES"],
        restart.get("restarted", 0),
        restart.get("failed", 0),
    )
    return RedirectResponse(_settings_url(request, "connected"), status_code=303)


@router.post("/vk-id/refresh")
async def vk_id_refresh(request: Request):
    user = await _require_admin(request)
    enforce_rate_limit(
        request,
        "token-vault-vk-id-refresh",
        limit=10,
        window_seconds=600,
        subject=user.get("username", ""),
    )
    return await _refresh_vk_id(force=True)


@router.post("/validate")
async def validate(request: Request):
    await _require_admin(request)
    data = await request.json()
    key = _clean(data.get("key"), 120)
    if not ENV_KEY_RE.fullmatch(key):
        raise HTTPException(400, "Некорректный ENV ключ")
    value = str(data.get("value") or "").strip() or _current_value(key)
    return {"ok": True, "key": key, "validation": await _validate_known(key, value)}


@router.post("/env")
async def save_env(request: Request):
    user = await _require_admin(request)
    data = await request.json()
    key = _clean(data.get("key"), 120)
    value = str(data.get("value") or "").strip()
    if not ENV_KEY_RE.fullmatch(key):
        raise HTTPException(400, "Некорректный ENV ключ")
    if not value:
        raise HTTPException(400, "Значение пустое")
    validation = (
        await _validate_known(key, value)
        if data.get("validate", True)
        else {"status": "unchecked", "message": "проверка пропущена"}
    )
    if validation["status"] in {"invalid", "error"} and not data.get("force"):
        return {"ok": False, "key": key, "validation": validation, "saved": False}

    result = await save_value(
        key,
        value,
        validation=validation,
        read_env=_read_env_values,
        write_env=_write_env_values,
        restart=_restart_modules_for_env if callable(_restart_modules_for_env) else None,
    )
    restart = result["restart"]
    _log(
        "info",
        "env token updated by=%s key=%s validation=%s restarted=%s failed=%s",
        user.get("username"),
        key,
        validation["status"],
        restart.get("restarted", 0),
        restart.get("failed", 0),
    )
    result["item"] = await _status_item(
        {
            "key": key,
            "description": "",
            "required": False,
            "modules": [],
            "provider": _provider_for_key(key),
        }
    )
    return result
