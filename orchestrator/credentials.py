from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from orchestrator.auth import ENV_PATH, _read_env_values
from orchestrator.telegram_proxy import telegram_bot_api_base, telegram_bot_api_proxy_url


ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_WRITE_LOCK = asyncio.Lock()


def clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def mask(value: str) -> str:
    return f"задано, длина {len(value)}" if value else ""


def provider_for_key(key: str) -> str:
    if key in {"VK_USER_TOKEN", "VK_TEST_USER_TOKEN"}:
        return "vk_user"
    if key in {"VK_GROUP_TOKEN", "SBKVD_LETTER_VK_TOKEN"}:
        return "vk_messages"
    if "TELEGRAM_BOT_TOKEN" in key:
        return "telegram_bot"
    if key == "OPENROUTER_API_KEY":
        return "openrouter"
    if key == "SENLER_ACCESS_TOKEN":
        return "senler"
    if key in {
        "TILDA_CHAT_LINKS_GOOGLE_CREDENTIALS_FILE",
        "GETCOURSE_CHAT_FIELDS_GOOGLE_CREDENTIALS_FILE",
        "GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
    } or key.endswith("_GOOGLE_CREDENTIALS_FILE"):
        return "google_service_account_file"
    return "unknown"


def inventory(
    module_rows: list[dict[str, Any]],
    *,
    read_env: Callable[[], dict[str, str]] = _read_env_values,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    env_values = read_env()
    for row in module_rows:
        try:
            manifest = json.loads(row.get("manifest_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            manifest = {}
        env_vars = manifest.get("env_vars") or {}
        env_required = manifest.get("env_required")
        required = set(env_required) if isinstance(env_required, list) else set(env_vars)
        for raw_key, description in env_vars.items():
            key = str(raw_key)
            if not ENV_KEY_RE.fullmatch(key):
                continue
            item = by_key.setdefault(
                key,
                {
                    "key": key,
                    "description": clean(description, 500),
                    "required": False,
                    "modules": [],
                    "provider": provider_for_key(key),
                },
            )
            item["required"] = bool(item["required"] or key in required)
            item["modules"].append(
                {"id": row["id"], "name": row["name"], "status": row["status"]}
            )
    for key in sorted(set(env_values) | set(os.environ)):
        if key.startswith("_") or not ENV_KEY_RE.fullmatch(key):
            continue
        if key in by_key or any(part in key for part in ("TOKEN", "SECRET", "KEY", "PASSWORD")):
            by_key.setdefault(
                key,
                {
                    "key": key,
                    "description": "",
                    "required": False,
                    "modules": [],
                    "provider": provider_for_key(key),
                },
            )
    return sorted(by_key.values(), key=lambda item: (not item["required"], item["key"]))


def current_value(
    key: str,
    *,
    read_env: Callable[[], dict[str, str]] = _read_env_values,
) -> str:
    return str(os.environ.get(key) or read_env().get(key) or "").strip()


def _proxy_value(*keys: str, read_env: Callable[[], dict[str, str]]) -> str:
    for key in keys:
        value = current_value(key, read_env=read_env)
        if value:
            return value
    return ""


def _httpx_kwargs(timeout: float, proxy: str = "") -> dict[str, Any]:
    kwargs: dict[str, Any] = {"timeout": timeout}
    if proxy:
        kwargs.update(proxy=proxy, trust_env=False)
    return kwargs


async def _validate_vk(method: str, token: str, params: dict[str, Any]) -> dict[str, Any]:
    payload = {**params, "access_token": token, "v": "5.199"}
    async with httpx.AsyncClient(timeout=12) as client:
        response = await client.post(f"https://api.vk.com/method/{method}", data=payload)
    body = response.json()
    if isinstance(body, dict) and body.get("error"):
        error = body["error"]
        return {
            "status": "invalid",
            "message": f"VK {error.get('error_code')}: {error.get('error_msg')}",
        }
    return {"status": "ok", "message": method}


async def validate_known(
    key: str,
    value: str,
    *,
    base_dir: Path,
    read_env: Callable[[], dict[str, str]] = _read_env_values,
) -> dict[str, Any]:
    if not value:
        return {"status": "missing", "message": "значение не задано"}
    provider = provider_for_key(key)
    try:
        if provider == "vk_user":
            return await _validate_vk("users.get", value, {})
        if provider == "vk_messages":
            return await _validate_vk("messages.getConversations", value, {"count": 1, "filter": "all"})
        if provider == "telegram_bot":
            base = telegram_bot_api_base(current_value("SBKVD_LETTER_TELEGRAM_API_BASE", read_env=read_env))
            proxy = telegram_bot_api_proxy_url(current_value("SBKVD_LETTER_TELEGRAM_PROXY_URL", read_env=read_env))
            async with httpx.AsyncClient(**_httpx_kwargs(12, proxy)) as client:
                response = await client.get(f"{base}/bot{value}/getMe")
            body = response.json()
            return (
                {"status": "ok", "message": "getMe"}
                if body.get("ok")
                else {
                    "status": "invalid",
                    "message": clean(body.get("description"), 300) or "Telegram rejected token",
                }
            )
        if provider == "openrouter":
            proxy = _proxy_value("OPENROUTER_HTTPS_PROXY", "OPENROUTER_HTTP_PROXY", read_env=read_env)
            headers = {
                "Authorization": f"Bearer {value}",
                "HTTP-Referer": "https://junior.sobakovod.pro/nexus/",
                "X-Title": "Nexus",
            }
            async with httpx.AsyncClient(**_httpx_kwargs(12, proxy)) as client:
                response = await client.get("https://openrouter.ai/api/v1/models", headers=headers)
            if response.status_code < 400:
                return {"status": "ok", "message": "models"}
            if response.status_code == 403 and "security policy" in response.text[:500].lower() and not proxy:
                return {
                    "status": "unchecked",
                    "message": "OpenRouter заблокировал прямую проверку; задайте OPENROUTER_HTTPS_PROXY",
                }
            return {"status": "invalid", "message": f"OpenRouter HTTP {response.status_code}"}
        if provider == "senler":
            group_id = current_value("SENLER_GROUP_ID", read_env=read_env)
            if not group_id:
                return {"status": "unchecked", "message": "для проверки нужен SENLER_GROUP_ID"}
            async with httpx.AsyncClient(timeout=12) as client:
                response = await client.post(
                    "https://senler.ru/api/subscribers/get",
                    data={
                        "access_token": value,
                        "group_id": group_id,
                        "vk_user_id": "1105209997",
                        "v": "2",
                    },
                )
            body = response.json()
            if isinstance(body, dict) and body.get("success") is False:
                return {
                    "status": "invalid",
                    "message": clean(body.get("error") or body.get("error_message") or body, 300),
                }
            return {"status": "ok", "message": "Senler API ответил"}
        if provider == "google_service_account_file":
            path = Path(value).expanduser()
            if not path.is_absolute():
                path = (base_dir / path).resolve()
            if not path.is_file():
                return {"status": "invalid", "message": "файл service account не найден"}
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("type") != "service_account":
                return {"status": "invalid", "message": "JSON не похож на service account"}
            missing = [name for name in ("client_email", "private_key", "token_uri") if not data.get(name)]
            if missing:
                return {"status": "invalid", "message": "в JSON не хватает полей: " + ", ".join(missing)}
            return {"status": "ok", "message": "service account JSON читается"}
    except Exception as exc:
        message = f"{type(exc).__name__}: {clean(exc, 240)}"
        if provider in {"telegram_bot", "openrouter"} and any(
            part in message.lower() for part in ("network is unreachable", "connecterror", "timeout", "proxy")
        ):
            return {"status": "unchecked", "message": message}
        return {"status": "error", "message": message}
    return {"status": "unchecked", "message": "автоматическая проверка недоступна"}


async def status_item(
    item: dict[str, Any],
    *,
    base_dir: Path,
    validate: bool = False,
    read_env: Callable[[], dict[str, str]] = _read_env_values,
) -> dict[str, Any]:
    key = item["key"]
    value = current_value(key, read_env=read_env)
    result = {
        **item,
        "present": bool(value),
        "masked": mask(value),
        "env_path": str(ENV_PATH),
        "validation": {
            "status": "present" if value else "missing",
            "message": "проверка не запускалась",
        },
    }
    if validate:
        result["validation"] = (
            await validate_known(key, value, base_dir=base_dir, read_env=read_env)
            if value or item.get("required")
            else {"status": "optional", "message": "не задано, необязательно"}
        )
    return result


RestartCallback = Callable[[str], Awaitable[dict[str, Any]]]


async def save_values(
    values: dict[str, str],
    *,
    read_env: Callable[[], dict[str, str]],
    write_env: Callable[[dict[str, str]], None],
    restart: RestartCallback | None = None,
    restart_key: str = "",
) -> dict[str, Any]:
    cleaned = {str(key): str(value) for key, value in values.items()}
    if not cleaned or any(not ENV_KEY_RE.fullmatch(key) for key in cleaned):
        raise ValueError("Некорректный ENV ключ")
    async with _ENV_WRITE_LOCK:
        env = read_env()
        env.update(cleaned)
        write_env(env)
        os.environ.update(cleaned)
    result = {"ok": True, "key": restart_key, "modules": [], "restarted": 0, "failed": 0}
    if restart and restart_key:
        try:
            result = await restart(restart_key)
        except Exception as exc:
            result = {
                "ok": False,
                "key": restart_key,
                "modules": [],
                "restarted": 0,
                "failed": 1,
                "error": f"{type(exc).__name__}: {clean(exc, 300)}",
            }
    return result


async def save_value(
    key: str,
    value: str,
    *,
    validation: dict[str, Any],
    read_env: Callable[[], dict[str, str]],
    write_env: Callable[[dict[str, str]], None],
    restart: RestartCallback | None,
) -> dict[str, Any]:
    restart_result = await save_values(
        {key: value},
        read_env=read_env,
        write_env=write_env,
        restart=restart,
        restart_key=key,
    )
    return {
        "ok": bool(restart_result.get("ok", False)),
        "key": key,
        "saved": True,
        "validation": validation,
        "restart": restart_result,
    }
