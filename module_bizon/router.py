from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "bizon"
DEFAULT_BASE_URL = "https://start.bizon365.ru/room/97242"
DEFAULT_SEC_KEY = "vJm1416BlJXMV0Pj3QewbkZ"
DEFAULT_ROOMS = [
    "puppy",
    "master-klass",
    "lay",
    "134b75a8bd9f",
    "progulka",
    "podbor",
    "agresiya",
    "perevozbujdenie",
    "podziv",
    "ukusi",
    "pelenki",
    "razrushitel",
    "ulitsa",
    "nelza",
    "socializaciya",
]
DEFAULT_SILENCE_WINDOWS = [{"start": "12:00", "end": "13:00"}, {"start": "19:00", "end": "20:00"}]
SENSITIVE_KEYS = {"login", "password", "telegram_bot_token", "telegram_chat_id"}
PUBLIC_NEXUS_BASE = os.environ.get("NEXUS_PUBLIC_BASE", "https://junior.sobakovod.pro/nexus").rstrip("/")
INTERNAL_NEXUS_BASE = os.environ.get("NEXUS_INTERNAL_BASE", "http://127.0.0.1:8080").rstrip("/")
DEFAULT_SCRIPT_DELAY = {
    "reply_delay_mode": "hybrid",
    "reply_delay_fixed_ms": 60000,
    "reply_delay_base_ms": 8000,
    "reply_delay_per_char_ms": 35,
    "reply_delay_per_word_ms": 450,
    "reply_delay_jitter_ms": 8000,
    "reply_delay_multiplier": 1,
    "reply_delay_min_ms": 60000,
    "reply_delay_max_ms": 180000,
}

_db_path: Path | None = None
_module_dir: Path | None = None
_data_dir: Path | None = None
_log_file: Path | None = None
_logger = None
_generation_slots = asyncio.Semaphore(15)


class SettingsIn(BaseModel):
    base_url: str = DEFAULT_BASE_URL
    sec_key: str = ""
    rooms: list[str] | str = DEFAULT_ROOMS
    login: str = ""
    password: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    scheduled_restart_time: str = "03:30"
    silence_windows: list[dict[str, str]] | str = DEFAULT_SILENCE_WINDOWS
    silence_threshold_minutes: int = 10


class ScriptRoomIn(BaseModel):
    room: str
    title: str = ""
    enabled: bool = True
    prompt_path: str = ""
    alt_prompt_path: str = ""
    context: int = 4
    individual_chat: bool = True
    reply_delay_mode: str = DEFAULT_SCRIPT_DELAY["reply_delay_mode"]
    reply_delay_fixed_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_fixed_ms"]
    reply_delay_base_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_base_ms"]
    reply_delay_per_char_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_per_char_ms"]
    reply_delay_per_word_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_per_word_ms"]
    reply_delay_jitter_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_jitter_ms"]
    reply_delay_multiplier: float = DEFAULT_SCRIPT_DELAY["reply_delay_multiplier"]
    reply_delay_min_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_min_ms"]
    reply_delay_max_ms: int = DEFAULT_SCRIPT_DELAY["reply_delay_max_ms"]


class ScriptRoomsIn(BaseModel):
    rooms: list[ScriptRoomIn]


class ProcessIn(BaseModel):
    userId: str = ""
    clientId: str = ""
    assistant_id: str = ""
    thread_id: str = ""
    room_key: str = ""


class PublicChatIn(BaseModel):
    user_id: str = ""
    thread_id: str | None = None
    prompt_file: str = ""
    context: int = 4
    message: str = ""
    client_name: str | None = None
    room_key: str = ""
    sec_key: str = ""


def setup(ctx):
    global _db_path, _module_dir, _data_dir, _log_file, _logger
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _data_dir = ctx.data_dir
    _log_file = ctx.data_dir / "logs" / "module.log"
    _log_file.parent.mkdir(parents=True, exist_ok=True)
    _logger = getattr(ctx, "logger", None)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        loop.create_task(_auto_install_deps_if_needed())
        loop.create_task(_auto_resume_runner_if_needed())
    else:
        loop.run_until_complete(_init_db())
        loop.run_until_complete(_auto_install_deps_if_needed())
        loop.run_until_complete(_auto_resume_runner_if_needed())


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("bizon module is not initialized")
    return _db_path


def _must_data() -> Path:
    if _data_dir is None:
        raise RuntimeError("bizon module is not initialized")
    return _data_dir


def _must_module() -> Path:
    if _module_dir is None:
        raise RuntimeError("bizon module is not initialized")
    return _module_dir


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS user_mappings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT NOT NULL,
                client_id   TEXT NOT NULL,
                prompt_path TEXT NOT NULL DEFAULT '',
                thread_id   TEXT NOT NULL DEFAULT '',
                room_key    TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL DEFAULT '',
                updated_at  TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_mappings_user_room
                ON user_mappings(user_id, room_key);
            CREATE TABLE IF NOT EXISTS script_rooms (
                room        TEXT PRIMARY KEY,
                config_json TEXT NOT NULL DEFAULT '{}',
                updated_at  TEXT NOT NULL DEFAULT ''
            );
        """)
        defaults = {
            "base_url": DEFAULT_BASE_URL,
            "sec_key": DEFAULT_SEC_KEY,
            "rooms": json.dumps(DEFAULT_ROOMS, ensure_ascii=False),
            "scheduled_restart_time": "03:30",
            "silence_windows": json.dumps(DEFAULT_SILENCE_WINDOWS, ensure_ascii=False),
            "silence_threshold_minutes": "10",
        }
        for key, value in defaults.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)", (key, value, _now()))
        await db.commit()
    _log("info", "bizon DB initialized")


def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400",
    }


def _cors_json(data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(data, status_code=status_code, headers=_cors_headers())


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _env_value(key: str) -> str:
    return os.environ.get(key, "").strip()


async def _settings_raw() -> dict[str, str]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        return {key: value for key, value in await cur.fetchall()}


def _parse_json_list(value: Any, fallback: list[Any]) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                data = json.loads(text)
                return data if isinstance(data, list) else fallback
            except Exception:
                pass
        return [part.strip() for part in text.replace("\r", "\n").replace(",", "\n").split("\n") if part.strip()]
    return fallback


def _settings_public(raw: dict[str, str]) -> dict[str, Any]:
    rooms = _parse_json_list(raw.get("rooms", ""), DEFAULT_ROOMS)
    windows = _parse_json_list(raw.get("silence_windows", ""), DEFAULT_SILENCE_WINDOWS)
    return {
        "base_url": raw.get("base_url") or DEFAULT_BASE_URL,
        "sec_key": raw.get("sec_key") or "",
        "rooms": rooms,
        "rooms_text": "\n".join(str(room) for room in rooms),
        "scheduled_restart_time": raw.get("scheduled_restart_time") or "03:30",
        "silence_windows": windows,
        "silence_windows_text": "\n".join(f"{w.get('start','')}-{w.get('end','')}" for w in windows if isinstance(w, dict)),
        "silence_threshold_minutes": int(raw.get("silence_threshold_minutes") or "10"),
        "has_login": bool(raw.get("login") or _env_value("BIZON365_LOGIN")),
        "has_password": bool(raw.get("password") or _env_value("BIZON365_PASS")),
        "has_telegram_bot_token": bool(raw.get("telegram_bot_token") or _env_value("TELEGRAM_BOT_TOKEN") or _env_value("TELEGRAM_BOT_TOKEN_ERROR_ALERT")),
        "has_telegram_chat_id": bool(raw.get("telegram_chat_id") or _env_value("TELEGRAM_CHAT_ID_ERROR_ALERT")),
        "env_login": bool(_env_value("BIZON365_LOGIN")),
        "env_password": bool(_env_value("BIZON365_PASS")),
        "env_telegram": bool(_env_value("TELEGRAM_BOT_TOKEN") or _env_value("TELEGRAM_BOT_TOKEN_ERROR_ALERT")),
    }


def _parse_windows(value: Any) -> list[dict[str, str]]:
    if isinstance(value, list):
        result = []
        for item in value:
            if isinstance(item, dict) and item.get("start") and item.get("end"):
                result.append({"start": str(item["start"]).strip(), "end": str(item["end"]).strip()})
        return result
    windows = []
    for raw in str(value or "").replace("\r", "\n").replace(",", "\n").split("\n"):
        text = raw.strip()
        if not text:
            continue
        if "-" not in text:
            continue
        start, end = text.split("-", 1)
        windows.append({"start": start.strip(), "end": end.strip()})
    return windows


def _validate_hhmm(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        raise HTTPException(400, f"{label}: формат HH:MM")
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except Exception:
        raise HTTPException(400, f"{label}: формат HH:MM")
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise HTTPException(400, f"{label}: некорректное время")
    return f"{hour:02d}:{minute:02d}"


def _clean_rooms(value: Any) -> list[str]:
    rooms = [str(item).strip().strip("/") for item in _parse_json_list(value, []) if str(item).strip()]
    clean = []
    for room in rooms:
        if "?" in room or "/" in room or "\\" in room:
            raise HTTPException(400, f"Некорректный ключ комнаты: {room}")
        clean.append(room)
    if not clean:
        raise HTTPException(400, "Нужно указать хотя бы одну комнату")
    return clean


async def _save_settings(data: SettingsIn) -> dict[str, Any]:
    raw = await _settings_raw()
    rooms = _clean_rooms(data.rooms)
    windows = []
    for window in _parse_windows(data.silence_windows):
        windows.append({
            "start": _validate_hhmm(window["start"], label="Окно тишины"),
            "end": _validate_hhmm(window["end"], label="Окно тишины"),
        })
    values = {
        "base_url": str(data.base_url or "").strip().rstrip("/"),
        "sec_key": str(data.sec_key or "").strip(),
        "rooms": json.dumps(rooms, ensure_ascii=False),
        "scheduled_restart_time": _validate_hhmm(data.scheduled_restart_time, label="Плановый рестарт"),
        "silence_windows": json.dumps(windows, ensure_ascii=False),
        "silence_threshold_minutes": str(max(1, min(120, int(data.silence_threshold_minutes or 10)))),
    }
    if data.login.strip():
        values["login"] = data.login.strip()
    elif "login" not in raw:
        values["login"] = ""
    if data.password.strip():
        values["password"] = data.password.strip()
    elif "password" not in raw:
        values["password"] = ""
    if data.telegram_bot_token.strip():
        values["telegram_bot_token"] = data.telegram_bot_token.strip()
    elif "telegram_bot_token" not in raw:
        values["telegram_bot_token"] = ""
    if data.telegram_chat_id.strip():
        values["telegram_chat_id"] = data.telegram_chat_id.strip()
    elif "telegram_chat_id" not in raw:
        values["telegram_chat_id"] = ""

    if not values["base_url"]:
        raise HTTPException(400, "base_url обязателен")
    if not values["sec_key"]:
        raise HTTPException(400, "sec_key обязателен")

    async with aiosqlite.connect(_must_db()) as db:
        for key, value in values.items():
            await db.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, value, _now()),
            )
        await db.commit()
    return _settings_public(await _settings_raw())


def _venv_dir() -> Path:
    return _must_data() / "runner-venv"


def _venv_python() -> Path:
    return _venv_dir() / "bin" / "python"


def _pid_file() -> Path:
    return _must_data() / "bizon.pid"


def _started_file() -> Path:
    return _must_data() / "bizon.started_at"


def _config_file() -> Path:
    return _must_data() / "runner_config.json"


def _install_pid_file() -> Path:
    return _must_data() / "deps-install.pid"


def _deps_log_file() -> Path:
    return _must_data() / "logs" / "deps-install.log"


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    stat_path = Path(f"/proc/{pid}/stat")
    if stat_path.exists():
        try:
            text = stat_path.read_text(encoding="utf-8", errors="replace")
            tail = text.rsplit(")", 1)[1].strip()
            if tail.split(" ", 1)[0] == "Z":
                return False
        except Exception:
            pass
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _process_info(path: Path) -> dict[str, Any]:
    pid = _read_pid(path)
    running = _pid_alive(pid)
    if not running and path.exists():
        path.unlink(missing_ok=True)
    return {"pid": pid if running else None, "running": running}


async def _command_ok(cmd: list[str], timeout: int = 10) -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode == 0
    except Exception:
        return False


def _browser_cache_ready() -> bool:
    browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if browsers_path:
        root = Path(browsers_path)
    else:
        root = Path.home() / ".cache" / "ms-playwright"
    try:
        return root.exists() and any(path.name.startswith("chromium") for path in root.iterdir())
    except Exception:
        return False


def _browser_executable() -> Path | None:
    browsers_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    root = Path(browsers_path) if browsers_path else Path.home() / ".cache" / "ms-playwright"
    candidates = [
        *root.glob("chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell"),
        *root.glob("chromium-*/chrome-linux/chrome"),
    ]
    return next((path for path in candidates if path.exists()), None)


def _missing_browser_libraries() -> list[str]:
    exe = _browser_executable()
    if not exe:
        return []
    try:
        proc = subprocess.run(["ldd", str(exe)], capture_output=True, text=True, timeout=10)
    except Exception:
        return []
    missing = []
    for line in (proc.stdout + "\n" + proc.stderr).splitlines():
        if "not found" in line:
            missing.append(line.strip())
    return missing


async def _deps_status() -> dict[str, Any]:
    python_path = _venv_python()
    install_info = _process_info(_install_pid_file())
    ready = python_path.exists() and await _command_ok([str(python_path), "-c", "import playwright"], timeout=10)
    browser_cache_ready = ready and _browser_cache_ready()
    missing_browser_libraries = _missing_browser_libraries() if browser_cache_ready else []
    browser_system_ready = browser_cache_ready and not missing_browser_libraries
    return {
        "venv_dir": str(_venv_dir()),
        "python": str(python_path),
        "ready": ready,
        "browser_cache_ready": browser_cache_ready,
        "browser_system_ready": browser_system_ready,
        "browser_ready": browser_system_ready,
        "missing_browser_libraries": missing_browser_libraries[:20],
        "install_running": install_info["running"],
        "install_pid": install_info["pid"],
        "install_log_size": _deps_log_file().stat().st_size if _deps_log_file().exists() else 0,
    }


def _start_deps_install(source: str = "manual") -> dict[str, Any]:
    install_info = _process_info(_install_pid_file())
    if install_info["running"]:
        return {"ok": True, "running": True, "pid": install_info["pid"]}
    _deps_log_file().parent.mkdir(parents=True, exist_ok=True)
    cmd = (
        f"echo \"$(date -Is) bizon deps install start source={source}\" && "
        f"{sys.executable} -m venv {str(_venv_dir())!r} && "
        f"{str(_venv_python())!r} -m pip install --upgrade pip -q && "
        f"{str(_venv_python())!r} -m pip install 'playwright>=1.40.0' -q && "
        f"(sudo -n true 2>/dev/null && sudo -n {str(_venv_python())!r} -m playwright install-deps chromium || "
        f"echo \"$(date -Is) playwright system deps skipped: sudo unavailable\") && "
        f"{str(_venv_python())!r} -m playwright install chromium && "
        f"echo \"$(date -Is) bizon deps install done source={source}\""
    )
    log_handle = _deps_log_file().open("ab")
    try:
        proc = subprocess.Popen(
            ["/bin/bash", "-lc", cmd],
            cwd=str(_must_data()),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log_handle.close()
    _install_pid_file().write_text(str(proc.pid), encoding="utf-8")
    _log("info", "bizon deps install started source=%s pid=%s", source, proc.pid)
    return {"ok": True, "running": True, "pid": proc.pid}


async def _auto_install_deps_if_needed() -> None:
    try:
        await asyncio.sleep(0.5)
        deps = await _deps_status()
        if not deps["ready"] or not deps["browser_ready"]:
            _start_deps_install("auto")
    except Exception as exc:
        _log("warning", "bizon deps auto-install check failed: %s", exc)


def _start_runner_process(config: dict[str, Any], source: str) -> dict[str, Any]:
    _config_file().write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_log(f"starting runner from {source}")
    log_handle = (_log_file or (_must_data() / "logs" / "module.log")).open("ab")
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    try:
        proc_obj = subprocess.Popen(
            [str(_venv_python()), str(_must_module() / "runner.py"), str(_config_file())],
            cwd=str(_must_module()),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
    finally:
        log_handle.close()
    _pid_file().write_text(str(proc_obj.pid), encoding="utf-8")
    _started_file().write_text(_now(), encoding="utf-8")
    _log("info", "bizon runner started source=%s pid=%s", source, proc_obj.pid)
    return {"ok": True, "running": True, "pid": proc_obj.pid}


async def _auto_resume_runner_if_needed() -> None:
    try:
        await asyncio.sleep(2)
        if not _started_file().exists():
            return
        proc = _process_info(_pid_file())
        if proc["running"]:
            return
        deps = await _deps_status()
        if not deps["ready"] or not deps["browser_ready"]:
            _log("warning", "bizon runner auto-resume skipped: dependencies are not ready")
            return
        raw = await _settings_raw()
        config = _runtime_config(raw)
        if not config["login"] or not config["password"] or not config["base_url"] or not config["sec_key"] or not config["rooms"]:
            _log("warning", "bizon runner auto-resume skipped: runtime settings are incomplete")
            return
        _start_runner_process(config, "auto-resume after Nexus restart")
    except Exception as exc:
        _log("warning", "bizon runner auto-resume failed: %s", exc, exc_info=True)


def _tail(path: Path, lines: int = 120) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return "\n".join(text.splitlines()[-max(1, min(lines, 1000)):])


def _runtime_config(raw: dict[str, str]) -> dict[str, Any]:
    login = raw.get("login") or _env_value("BIZON365_LOGIN")
    password = raw.get("password") or _env_value("BIZON365_PASS")
    telegram_token = raw.get("telegram_bot_token") or _env_value("TELEGRAM_BOT_TOKEN") or _env_value("TELEGRAM_BOT_TOKEN_ERROR_ALERT")
    telegram_chat = raw.get("telegram_chat_id") or _env_value("TELEGRAM_CHAT_ID_ERROR_ALERT")
    return {
        "base_url": raw.get("base_url") or DEFAULT_BASE_URL,
        "sec_key": raw.get("sec_key") or DEFAULT_SEC_KEY,
        "rooms": _parse_json_list(raw.get("rooms", ""), DEFAULT_ROOMS),
        "login": login,
        "password": password,
        "telegram_bot_token": telegram_token,
        "telegram_chat_id": telegram_chat,
        "profile_dir": str(_must_data() / "profile"),
        "log_file": str(_log_file or (_must_data() / "logs" / "module.log")),
        "supervisor": {
            "scheduled_restart_time": raw.get("scheduled_restart_time") or "03:30",
            "silence_windows": _parse_json_list(raw.get("silence_windows", ""), DEFAULT_SILENCE_WINDOWS),
            "silence_threshold_minutes": int(raw.get("silence_threshold_minutes") or "10"),
        },
    }


def _safe_json(value: str) -> dict[str, Any]:
    try:
        data = json.loads(value or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_prompt_path(value: str, fallback: str = "") -> str:
    text = str(value or fallback or "").strip().strip("/")
    if not text:
        return ""
    if "/" not in text:
        text = f"prompts/{text}"
    if not text.endswith(".txt"):
        text = f"{text}.txt"
    parts = [part for part in text.split("/") if part]
    if any(part in {".", ".."} or "\\" in part for part in parts):
        raise HTTPException(400, "invalid prompt path")
    return "/".join(parts)


def _room_kind(room: str) -> str:
    text = str(room or "").strip().lower()
    if "puppy" in text or "щен" in text:
        return "puppy"
    return "dog"


def _default_script_room(room: str) -> dict[str, Any]:
    kind = _room_kind(room)
    title = "Щенки" if kind == "puppy" else "Собаки"
    prompt_base = "puppy_gpt4" if kind == "puppy" else "dog_gpt4"
    return {
        "room": room,
        "title": title,
        "enabled": True,
        "prompt_path": f"prompts/{prompt_base}.txt",
        "alt_prompt_path": f"prompts/{prompt_base}-2.txt",
        "context": 4,
        "individual_chat": True,
        **DEFAULT_SCRIPT_DELAY,
    }


def _coerce_script_config(room: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    base = _default_script_room(room)
    merged = {**base, **(config or {})}
    merged.pop("model", None)
    merged.pop("telegram_topic", None)
    merged["room"] = str(room or merged.get("room") or "").strip()
    merged["title"] = str(merged.get("title") or base["title"]).strip()
    merged["enabled"] = bool(merged.get("enabled", True))
    merged["prompt_path"] = _normalize_prompt_path(str(merged.get("prompt_path") or ""), base["prompt_path"])
    merged["alt_prompt_path"] = _normalize_prompt_path(str(merged.get("alt_prompt_path") or ""), base["alt_prompt_path"])
    try:
        merged["context"] = max(0, min(4, int(float(merged.get("context", base["context"]) or base["context"]))))
    except Exception:
        merged["context"] = base["context"]
    merged["individual_chat"] = bool(merged.get("individual_chat", True))
    for key in [
        "reply_delay_fixed_ms",
        "reply_delay_base_ms",
        "reply_delay_per_char_ms",
        "reply_delay_per_word_ms",
        "reply_delay_jitter_ms",
        "reply_delay_min_ms",
        "reply_delay_max_ms",
    ]:
        merged[key] = max(0, int(float(merged.get(key, DEFAULT_SCRIPT_DELAY[key]) or 0)))
    merged["reply_delay_mode"] = str(merged.get("reply_delay_mode") or "hybrid").strip().lower()
    if merged["reply_delay_mode"] not in {"off", "fixed", "chars", "words", "hybrid", "range", "random"}:
        merged["reply_delay_mode"] = "hybrid"
    try:
        merged["reply_delay_multiplier"] = max(0, float(merged.get("reply_delay_multiplier") or 1))
    except Exception:
        merged["reply_delay_multiplier"] = 1
    return merged


async def _script_room_rows() -> dict[str, dict[str, Any]]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT room, config_json FROM script_rooms")
        rows = await cur.fetchall()
    return {row[0]: _safe_json(row[1]) for row in rows}


def _script_urls() -> dict[str, str]:
    public_api = f"{PUBLIC_NEXUS_BASE}/{MODULE_ID}/api/public"
    script_src = f"{PUBLIC_NEXUS_BASE}/{MODULE_ID}/static/moderator_openrouter_pm.js?v=1.1.21"
    return {"public_api": public_api, "script_src": script_src}


def _script_snippet(config: dict[str, Any], sec_key: str) -> str:
    urls = _script_urls()
    bot_config = _public_bot_config(config, sec_key)
    body = json.dumps(bot_config, ensure_ascii=False, indent=8)
    return (
        "<script>\n"
        "(function() {\n"
        "    window.BOT_CONFIG = Object.assign({}, window.BOT_CONFIG || {}, "
        f"{body});\n"
        "})();\n"
        "</script>\n"
        f"<script defer src=\"{urls['script_src']}\"></script>"
    )


def _public_bot_config(config: dict[str, Any], sec_key: str) -> dict[str, Any]:
    urls = _script_urls()
    return {
        "API_BASE": urls["public_api"],
        "PROMPT_FILE": config["prompt_path"],
        "ALT_PROMPT_FILE": config["alt_prompt_path"],
        "CONTEXT": config["context"],
        "SEC_KEY": sec_key,
        "INDIVIDUAL_CHAT": config["individual_chat"],
        "REPLY_DELAY_MODE": config["reply_delay_mode"],
        "REPLY_DELAY_FIXED_MS": config["reply_delay_fixed_ms"],
        "REPLY_DELAY_BASE_MS": config["reply_delay_base_ms"],
        "REPLY_DELAY_PER_CHAR_MS": config["reply_delay_per_char_ms"],
        "REPLY_DELAY_PER_WORD_MS": config["reply_delay_per_word_ms"],
        "REPLY_DELAY_JITTER_MS": config["reply_delay_jitter_ms"],
        "REPLY_DELAY_MULTIPLIER": config["reply_delay_multiplier"],
        "REPLY_DELAY_MIN_MS": config["reply_delay_min_ms"],
        "REPLY_DELAY_MAX_MS": config["reply_delay_max_ms"],
    }


async def _scripts_payload() -> dict[str, Any]:
    raw = await _settings_raw()
    settings = _settings_public(raw)
    stored = await _script_room_rows()
    rooms = []
    for room in settings["rooms"]:
        config = _coerce_script_config(str(room), stored.get(str(room)))
        config["snippet"] = _script_snippet(config, settings["sec_key"])
        rooms.append(config)
    return {"rooms": rooms, "urls": _script_urls()}


async def _save_script_rooms(data: ScriptRoomsIn) -> dict[str, Any]:
    valid_rooms = {str(room) for room in _settings_public(await _settings_raw())["rooms"]}
    seen = set()
    async with aiosqlite.connect(_must_db()) as db:
        for item in data.rooms:
            room = str(item.room or "").strip()
            if not room or room not in valid_rooms or room in seen:
                continue
            seen.add(room)
            config = _coerce_script_config(room, item.model_dump())
            payload = dict(config)
            payload.pop("snippet", None)
            await db.execute(
                "INSERT INTO script_rooms(room,config_json,updated_at) VALUES(?,?,?) ON CONFLICT(room) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at",
                (room, json.dumps(payload, ensure_ascii=False), _now()),
            )
        await db.commit()
    return await _scripts_payload()


def _resolve_room_key(explicit: str, request: Request) -> str:
    text = str(explicit or "").strip()
    if text:
        return text[:255]
    referer = str(request.headers.get("referer") or "").strip()
    if not referer:
        return ""
    try:
        parsed = urlparse(referer)
        return f"{parsed.netloc}{parsed.path}".strip()[:255]
    except Exception:
        return ""


async def _openrouter_token() -> str:
    db_path = _openrouter_db_path()
    if not db_path.exists():
        raise HTTPException(503, "openrouter module DB not found")
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key='module_api_token'")
        row = await cur.fetchone()
    token = str(row[0] if row else "").strip()
    if not token:
        raise HTTPException(503, "openrouter module token is not configured")
    return token


def _openrouter_db_path() -> Path:
    candidates = [
        _must_module().parent / "openrouter" / "data" / "openrouter.db",
        _must_module().parent.parent / "modules" / "openrouter" / "data" / "openrouter.db",
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


async def _ensure_openrouter_conversation(
    platform_id: str,
    prompt_path: str,
    requested_conversation_id: str | None = None,
) -> str:
    db_path = _openrouter_db_path()
    if not db_path.exists():
        raise HTTPException(503, "openrouter module DB not found")
    now = _now()
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            """
            SELECT conversation_id
            FROM conversations
            WHERE platform_id=? AND active=1
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (platform_id,),
        )
        row = await cur.fetchone()
        if row:
            return str(row[0])
        if requested_conversation_id:
            cur = await db.execute(
                "SELECT conversation_id FROM conversations WHERE conversation_id=? AND platform_id=?",
                (requested_conversation_id, platform_id),
            )
            row = await cur.fetchone()
            if row:
                return str(row[0])
        cur = await db.execute(
            "SELECT conversation_id FROM conversations WHERE platform_id=? ORDER BY updated_at DESC LIMIT 1",
            (platform_id,),
        )
        row = await cur.fetchone()
        if row:
            return str(row[0])
        conversation_id = f"or_conv_{uuid.uuid4().hex}"
        await db.execute(
            """
            INSERT INTO users(platform_id, created_at, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(platform_id) DO UPDATE SET updated_at=excluded.updated_at
            """,
            (platform_id, now, now),
        )
        await db.execute(
            """
            INSERT INTO conversations(conversation_id, platform_id, active, prompt_path, model, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (conversation_id, platform_id, 0, prompt_path, "", now, now),
        )
        await db.commit()
    return conversation_id


async def _is_openrouter_bizon_conversation(conversation_id: str, platform_id: str) -> bool:
    db_path = _openrouter_db_path()
    if not db_path.exists() or not conversation_id:
        return False
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("SELECT platform_id FROM conversations WHERE conversation_id=?", (conversation_id,))
        row = await cur.fetchone()
    return bool(row and row[0] == platform_id)


async def _call_openrouter_chat(data: PublicChatIn, conversation_id: str | None) -> dict[str, Any]:
    prompt = _normalize_prompt_path(data.prompt_file)
    platform_id = str(data.user_id or "").strip()
    resolved_conversation_id = conversation_id if conversation_id and conversation_id.startswith("or_conv_") else None
    if resolved_conversation_id and not await _is_openrouter_bizon_conversation(resolved_conversation_id, platform_id):
        resolved_conversation_id = None
    resolved_conversation_id = await _ensure_openrouter_conversation(platform_id, prompt, resolved_conversation_id)
    payload = {
        "platform_id": platform_id,
        "conversation_id": resolved_conversation_id,
        "prompt": prompt,
        "message": str(data.message or "").strip(),
        "context": max(0, min(4, int(data.context if data.context is not None else 4))),
        "summary_only": True,
    }
    token = await _openrouter_token()
    queued_at = time.monotonic()
    async with _generation_slots:
        queue_wait = time.monotonic() - queued_at
        if queue_wait >= 0.05:
            _log("info", "bizon generation dequeued platform_id=%s wait_seconds=%.3f", platform_id, queue_wait)
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.post(
                f"{INTERNAL_NEXUS_BASE}/openrouter/api/chat",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )
    try:
        result = resp.json()
    except Exception:
        result = {"detail": resp.text}
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, result.get("detail") or result.get("error") or "openrouter error")
    return result


def _append_log(message: str) -> None:
    path = _log_file or (_must_data() / "logs" / "module.log")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [bizon] {message}\n")


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    return _settings_public(await _settings_raw())


@router.post("/settings")
async def post_settings(data: SettingsIn, request: Request):
    await _require_panel_user(request)
    result = await _save_settings(data)
    _log("info", "bizon settings updated")
    return result


@router.get("/scripts/rooms")
async def get_script_rooms(request: Request):
    await _require_panel_user(request)
    return await _scripts_payload()


@router.put("/scripts/rooms")
async def put_script_rooms(data: ScriptRoomsIn, request: Request):
    await _require_panel_user(request)
    result = await _save_script_rooms(data)
    _log("info", "bizon script rooms updated")
    return result


@router.get("/status")
async def status(request: Request):
    await _require_panel_user(request)
    proc = _process_info(_pid_file())
    deps = await _deps_status()
    raw = await _settings_raw()
    started_at = _started_file().read_text(encoding="utf-8").strip() if _started_file().exists() else ""
    return {
        "process": {
            **proc,
            "started_at": started_at if proc["running"] else "",
            "log_size": (_log_file.stat().st_size if _log_file and _log_file.exists() else 0),
        },
        "deps": deps,
        "settings": _settings_public(raw),
    }


@router.get("/logs")
async def logs(request: Request, lines: int = 200):
    await _require_panel_user(request)
    return PlainTextResponse(_tail(_log_file or (_must_data() / "logs" / "module.log"), lines))


@router.get("/deps/logs")
async def deps_logs(request: Request, lines: int = 200):
    await _require_panel_user(request)
    return PlainTextResponse(_tail(_deps_log_file(), lines))


@router.options("/public/{path:path}")
async def public_options(path: str):
    return PlainTextResponse("", headers=_cors_headers())


@router.post("/public/process2")
async def public_process2(data: ProcessIn, request: Request):
    user_id = str(data.userId or "").strip()
    client_id = str(data.clientId or "").strip()
    prompt_path = _normalize_prompt_path(data.assistant_id)
    thread_id = str(data.thread_id or "").strip()
    room_key = _resolve_room_key(data.room_key, request)
    if not user_id or not client_id or not prompt_path:
        return _cors_json({"detail": "Invalid input data"}, status_code=400)
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO user_mappings(user_id,client_id,prompt_path,thread_id,room_key,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(user_id,room_key) DO UPDATE SET
                client_id=excluded.client_id,
                prompt_path=excluded.prompt_path,
                thread_id=CASE
                    WHEN user_mappings.client_id<>excluded.client_id THEN excluded.thread_id
                    WHEN excluded.thread_id<>'' THEN excluded.thread_id
                    ELSE user_mappings.thread_id
                END,
                updated_at=excluded.updated_at
            """,
            (user_id, client_id, prompt_path, thread_id, room_key, now, now),
        )
        await db.commit()
    return _cors_json({"message": "Data saved successfully"})


async def _recover_mapping_from_message(user_id: str, room_key: str, message: str) -> tuple | None:
    clean_message = str(message or "").strip()
    if len(clean_message) < 80 or not room_key:
        return None
    openrouter_db = _openrouter_db_path()
    if not openrouter_db.exists():
        return None
    async with aiosqlite.connect(openrouter_db) as db:
        cur = await db.execute(
            """
            SELECT DISTINCT platform_id
            FROM messages
            WHERE role IN ('user','manual_user')
              AND content=?
              AND platform_id NOT LIKE 'start.bizon365.ru/%'
            ORDER BY id DESC
            LIMIT 5
            """,
            (clean_message,),
        )
        candidates = [str(row[0]) for row in await cur.fetchall() if str(row[0] or "").strip()]
    if not candidates:
        return None
    placeholders = ",".join("?" for _ in candidates)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            f"""
            SELECT client_id,thread_id,prompt_path,room_key
            FROM user_mappings
            WHERE room_key=? AND client_id IN ({placeholders})
            ORDER BY updated_at DESC,id DESC
            """,
            (room_key, *candidates),
        )
        rows = await cur.fetchall()
        unique = {}
        for row in rows:
            unique.setdefault(str(row[0]), row)
        if len(unique) != 1:
            return None
        row = next(iter(unique.values()))
        now = _now()
        await db.execute(
            """
            INSERT INTO user_mappings(user_id,client_id,prompt_path,thread_id,room_key,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(user_id,room_key) DO UPDATE SET
                client_id=excluded.client_id,
                prompt_path=excluded.prompt_path,
                thread_id=excluded.thread_id,
                updated_at=excluded.updated_at
            """,
            (user_id, row[0], row[2], row[1], room_key, now, now),
        )
        await db.commit()
    _log("warning", "bizon mapping recovered from unique long message user_id=%s client_id=%s room=%s", user_id, row[0], room_key)
    return row


@router.get("/public/user_lookup")
async def public_user_lookup(
    user_id: str,
    request: Request,
    room_key: str | None = None,
    message: str | None = None,
    sec_key: str | None = None,
):
    clean_user_id = str(user_id or "").strip()
    if not clean_user_id:
        return _cors_json({"error": "user_id is required"}, status_code=400)
    resolved_room_key = _resolve_room_key(room_key or "", request)
    row = None
    async with aiosqlite.connect(_must_db()) as db:
        if resolved_room_key:
            cur = await db.execute(
                """
                SELECT client_id, thread_id, prompt_path, room_key
                FROM user_mappings
                WHERE user_id=? AND room_key=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_user_id, resolved_room_key),
            )
            row = await cur.fetchone()
        if row is None:
            cur = await db.execute(
                """
                SELECT client_id, thread_id, prompt_path, room_key
                FROM user_mappings
                WHERE user_id=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (clean_user_id,),
            )
            row = await cur.fetchone()
    if row is None and message:
        expected_key = (await _settings_raw()).get("sec_key") or DEFAULT_SEC_KEY
        if sec_key and str(sec_key).strip() == expected_key:
            row = await _recover_mapping_from_message(clean_user_id, resolved_room_key, message)
    if row:
        return _cors_json({
            "client_id": row[0],
            "thread_id": row[1],
            "assistant_id": row[2],
            "room_key": row[3],
        })
    return _cors_json({
        "client_id": None,
        "thread_id": None,
        "assistant_id": None,
        "room_key": resolved_room_key or None,
    })


@router.get("/public/room-config")
async def public_room_config(room: str, request: Request):
    clean_room = str(room or "").strip().strip("/")
    settings = _settings_public(await _settings_raw())
    valid_rooms = {str(item) for item in settings["rooms"]}
    if not clean_room or clean_room not in valid_rooms:
        return _cors_json({"error": "room not configured"}, 404)
    stored = await _script_room_rows()
    config = _coerce_script_config(clean_room, stored.get(clean_room))
    if not config["enabled"]:
        return _cors_json({"error": "room disabled"}, 404)
    return _cors_json({
        "room": clean_room,
        "config": _public_bot_config(config, settings["sec_key"]),
        "script_src": _script_urls()["script_src"],
    })


@router.get("/public/room_mappings")
async def public_room_mappings(request: Request, room_key: str | None = None, sec_key: str | None = None):
    expected_key = (await _settings_raw()).get("sec_key") or DEFAULT_SEC_KEY
    if not sec_key or str(sec_key).strip() != expected_key:
        return _cors_json({"detail": "unauthorized"}, status_code=401)
    resolved_room_key = _resolve_room_key(room_key or "", request)
    if not resolved_room_key:
        return _cors_json({"mappings": [], "room_key": None})
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            """
            SELECT user_id, client_id, thread_id, prompt_path, room_key, updated_at
            FROM user_mappings
            WHERE room_key=?
            ORDER BY updated_at DESC, id DESC
            LIMIT 50
            """,
            (resolved_room_key,),
        )
        rows = await cur.fetchall()
    return _cors_json({
        "room_key": resolved_room_key,
        "mappings": [
            {
                "user_id": row[0],
                "client_id": row[1],
                "platform_id": row[1],
                "thread_id": row[2],
                "conversation_id": row[2],
                "assistant_id": row[3],
                "room_key": row[4],
                "updated_at": row[5],
            }
            for row in rows
        ],
    })


@router.post("/public/chat")
async def public_chat(data: PublicChatIn, request: Request):
    expected_key = (await _settings_raw()).get("sec_key") or DEFAULT_SEC_KEY
    if not data.sec_key or str(data.sec_key).strip() != expected_key:
        return _cors_json({"detail": "unauthorized"}, status_code=401)
    user_id = str(data.user_id or "").strip()
    message = str(data.message or "").strip()
    if not user_id or not message:
        return _cors_json({"detail": "user_id and message are required"}, status_code=400)
    mapping_thread_id = str(data.thread_id or "").strip()
    try:
        result = await _call_openrouter_chat(data, mapping_thread_id)
        conversation_id = str(result.get("conversation_id") or "").strip()
        room_key = _resolve_room_key(data.room_key, request)
        if conversation_id and room_key:
            async with aiosqlite.connect(_must_db()) as db:
                await db.execute(
                    """
                    UPDATE user_mappings
                    SET thread_id=?, prompt_path=?, updated_at=?
                    WHERE client_id=? AND room_key=?
                    """,
                    (conversation_id, _normalize_prompt_path(data.prompt_file), _now(), user_id, room_key),
                )
                await db.commit()
        return _cors_json({
            "message": result.get("answer") or result.get("text") or "",
            "thread_id": conversation_id,
            "conversation_id": conversation_id,
            "model": result.get("model") or "",
        })
    except HTTPException as exc:
        return _cors_json({"detail": exc.detail}, status_code=exc.status_code)
    except Exception as exc:
        _log("error", "bizon public chat crashed: %s", exc, exc_info=True)
        return _cors_json({"detail": "chat error"}, status_code=500)


@router.post("/deps/install")
async def install_deps(request: Request):
    await _require_panel_user(request)
    return _start_deps_install("manual")


@router.post("/start")
async def start_runner(request: Request):
    await _require_panel_user(request)
    proc = _process_info(_pid_file())
    if proc["running"]:
        return {"ok": True, "running": True, "pid": proc["pid"]}
    deps = await _deps_status()
    if not deps["ready"] or not deps["browser_ready"]:
        if deps.get("missing_browser_libraries"):
            raise HTTPException(409, "Системные зависимости Chromium не установлены. Нажмите «Установить зависимости».")
        raise HTTPException(409, "Runner-зависимости не установлены. Нажмите «Установить зависимости».")
    raw = await _settings_raw()
    config = _runtime_config(raw)
    if not config["login"] or not config["password"]:
        raise HTTPException(400, "Bizon365 логин и пароль не настроены")
    if not config["base_url"] or not config["sec_key"] or not config["rooms"]:
        raise HTTPException(400, "base_url, sec_key и rooms обязательны")
    return _start_runner_process(config, "Nexus panel")


@router.post("/stop")
async def stop_runner(request: Request):
    await _require_panel_user(request)
    pid = _read_pid(_pid_file())
    if not _pid_alive(pid):
        _pid_file().unlink(missing_ok=True)
        return {"ok": True, "running": False}
    assert pid is not None
    _append_log(f"stopping runner pid={pid}")
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    deadline = time.time() + 15
    while time.time() < deadline:
        if not _pid_alive(pid):
            break
        await asyncio.sleep(0.5)
    if _pid_alive(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except Exception:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
    _pid_file().unlink(missing_ok=True)
    _started_file().unlink(missing_ok=True)
    _log("info", "bizon runner stopped pid=%s", pid)
    return {"ok": True, "running": False}


@router.post("/restart")
async def restart_runner(request: Request):
    await stop_runner(request)
    return await start_runner(request)
