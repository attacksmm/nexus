from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
import secrets
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request

from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
from orchestrator.db import (
    create_user,
    get_all_users,
    update_user,
    update_user_password,
)


MODULE_ID = "staff-registry"
CONNECTOR_VERSION = 1
MAX_BODY_BYTES = 256_000
SYNC_MAX_ATTEMPTS = 6

router = APIRouter()
_db_path: Path | None = None
_logger = None
_worker_task: asyncio.Task | None = None
_worker_wakeup = asyncio.Event()
_write_lock = asyncio.Lock()


MODULE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "nexus-core": {
        "label": "Nexus",
        "description": "Вход в Nexus, роль и доступные панели.",
        "fields": [
            {"key": "username", "label": "Логин", "type": "text"},
            {"key": "role", "label": "Роль", "type": "select", "options": ["viewer", "editor", "admin"]},
            {"key": "module_access", "label": "Доступные модули", "type": "list"},
        ],
    },
    "messenger-widget": {
        "label": "Messenger",
        "description": "Диалоги, задачи amoCRM, уведомления и внешние привязки.",
        "fields": [
            {"key": "role", "label": "Роль", "type": "select", "options": ["employee", "admin"]},
            {"key": "amo_task_enabled", "label": "Создавать задачи amoCRM", "type": "bool"},
            {"key": "amo_task_sources", "label": "Каналы задач", "type": "list"},
            {"key": "course_chat_notifications", "label": "Уведомления учебных чатов", "type": "bool"},
            {"key": "notification_recipient_ids", "label": "Кому отправлять уведомления", "type": "multiselect", "options": []},
        ],
    },
    "course-chat-creator": {
        "label": "Учебные чаты",
        "description": "Роль в чатах, VK, Telegram и настройки куратора.",
        "fields": [
            {"key": "kind", "label": "Роль", "type": "select", "options": ["admin", "kurator", "author", "tech"]},
            {"key": "vk_id", "label": "VK ID или ссылка", "type": "text"},
            {"key": "tg_ref", "label": "Telegram username", "type": "text"},
            {"key": "offer_id", "label": "GetCourse offer ID", "type": "number"},
            {"key": "parity", "label": "Потоки", "type": "select", "options": ["any", "even", "odd"]},
            {"key": "note", "label": "Заметка", "type": "text"},
        ],
    },
    "student-transfer": {
        "label": "Streams",
        "description": "Аккаунт оператора управления потоками.",
        "fields": [
            {"key": "login", "label": "Логин", "type": "text"},
            {"key": "display_name", "label": "Имя в интерфейсе", "type": "text"},
        ],
    },
    "sales-chats": {
        "label": "Чаты продаж",
        "description": "Аккаунт в рабочем чате отдела продаж.",
        "fields": [
            {"key": "login", "label": "Логин", "type": "text"},
            {"key": "display_name", "label": "Имя в интерфейсе", "type": "text"},
        ],
    },
    "sbkvd-gpt": {
        "label": "SBKVD GPT",
        "description": "Аккаунт, модели и промпты сотрудника.",
        "fields": [
            {"key": "login", "label": "Логин", "type": "text"},
            {"key": "display_name", "label": "Имя в интерфейсе", "type": "text"},
            {"key": "default_prompt", "label": "Промпт по умолчанию", "type": "text"},
            {"key": "default_model", "label": "Модель по умолчанию", "type": "text"},
            {"key": "prompt_paths", "label": "Доступные промпты", "type": "list"},
            {"key": "models", "label": "Доступные модели", "type": "list"},
        ],
    },
    "email-channel": {
        "label": "Email",
        "description": "Персональный адрес отправителя.",
        "fields": [
            {"key": "local_part", "label": "Адрес до @", "type": "text"},
        ],
    },
    "admin-handoff": {
        "label": "Передача диалогов",
        "description": "VK-администраторы, которым разрешено защищать и передавать рабочие диалоги.",
        "fields": [
            {"key": "protect_dialogs", "label": "Разрешить работу с диалогами", "type": "bool"},
        ],
    },
    "chat-moderator": {
        "label": "Модератор чатов",
        "description": "Исключения модерации и доверенные отправители по точному VK ID.",
        "fields": [
            {"key": "allowed_admin", "label": "Администратор модерации", "type": "bool"},
            {"key": "trusted_sender", "label": "Доверенный отправитель", "type": "bool"},
            {"key": "telegram_allowed_adder", "label": "Может добавлять Telegram-бота", "type": "bool"},
        ],
    },
    "chat-moderators": {
        "label": "Модераторы чатов",
        "description": "Исключения модерации, доверенные отправители и защита VK-администратора.",
        "fields": [
            {"key": "allowed_admin", "label": "Администратор модерации", "type": "bool"},
            {"key": "trusted_sender", "label": "Доверенный отправитель", "type": "bool"},
            {"key": "telegram_allowed_adder", "label": "Может добавлять Telegram-бота", "type": "bool"},
        ],
    },
    "bizon-amocrm": {
        "label": "Bizon → amoCRM",
        "description": "Участие сотрудника в очередях ответственных по привязкам вебинаров.",
        "fields": [
            {"key": "responsible_binding_ids", "label": "Очереди привязок", "type": "multiselect", "options": []},
        ],
    },
    "getcourse-amocrm": {
        "label": "GetCourse → amoCRM",
        "description": "Общая очередь и назначения ответственного за сделки и задачи.",
        "fields": [
            {"key": "round_robin_enabled", "label": "Участвует в общей очереди", "type": "bool"},
            {"key": "deal_binding_ids", "label": "Ответственный за сделки", "type": "multiselect", "options": []},
            {"key": "task_binding_ids", "label": "Ответственный за задачи", "type": "multiselect", "options": []},
        ],
    },
    "getcourse-chat-fields": {
        "label": "Поля чатов GetCourse",
        "description": "Соответствие имён кураторов из рабочих таблиц системным значениям GetCourse.",
        "fields": [
            {"key": "name_markers", "label": "Варианты имени в таблицах", "type": "list"},
            {"key": "curator_value", "label": "Значение поля куратора", "type": "text"},
        ],
    },
}


PROFILE_PRESETS: dict[str, dict[str, Any]] = {
    "sales_manager": {
        "label": "Менеджер продаж",
        "roles": ["sales_manager"],
        "modules": {
            "nexus-core": {"role": "editor", "module_access": ["messenger-widget", "sales-chats", "sbkvd-gpt", "email-channel"]},
            "messenger-widget": {"role": "employee", "amo_task_enabled": True, "amo_task_sources": ["wazzup", "vk", "telegram", "salebot", "email"]},
            "sales-chats": {},
            "sbkvd-gpt": {},
            "email-channel": {},
        },
    },
    "curator": {
        "label": "Куратор",
        "roles": ["curator"],
        "modules": {
            "nexus-core": {"role": "editor", "module_access": ["student-transfer", "messenger-widget", "course-chat-creator"]},
            "messenger-widget": {"role": "employee", "course_chat_notifications": True, "amo_task_enabled": False},
            "course-chat-creator": {"kind": "kurator", "parity": "any"},
            "student-transfer": {},
            "email-channel": {},
            "chat-moderator": {"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
            "chat-moderators": {"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
        },
    },
    "tech_support": {
        "label": "Техподдержка",
        "roles": ["tech_support"],
        "modules": {
            "nexus-core": {"role": "editor", "module_access": ["messenger-widget", "student-transfer", "course-chat-creator", "chat-moderator", "chat-moderators", "scanner", "admin-handoff"]},
            "messenger-widget": {"role": "employee", "amo_task_enabled": True},
            "course-chat-creator": {"kind": "tech", "parity": "any"},
            "student-transfer": {},
            "email-channel": {},
            "admin-handoff": {"protect_dialogs": True},
            "chat-moderator": {"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
            "chat-moderators": {"allowed_admin": True, "trusted_sender": True, "telegram_allowed_adder": True},
        },
    },
    "nexus_admin": {
        "label": "Администратор Nexus",
        "roles": ["nexus_admin"],
        "modules": {
            "nexus-core": {"role": "admin", "module_access": []},
            "messenger-widget": {"role": "admin", "amo_task_enabled": True},
            "sbkvd-gpt": {},
            "email-channel": {},
        },
    },
    "custom": {"label": "Настроить вручную", "roles": [], "modules": {}},
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _json(value: Any, fallback: Any) -> Any:
    if isinstance(value, type(fallback)):
        return value
    try:
        parsed = json.loads(str(value or ""))
        return parsed if isinstance(parsed, type(fallback)) else fallback
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def _normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value, 100))
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return "+" + digits if 8 <= len(digits) <= 15 else ""


def _normalize_email(value: Any) -> str:
    email = _clean(value, 320).casefold()
    return email if re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email) else ""


def _slug(value: Any) -> str:
    text = _clean(value, 200).casefold()
    translit = str.maketrans({
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
        "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    })
    text = text.translate(translit)
    return re.sub(r"[^a-z0-9]+", ".", text).strip(".")[:80] or "employee"


def _safe_config(value: Any) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    blocked = {"password", "password_hash", "token", "secret", "api_key", "session"}
    result: dict[str, Any] = {}
    for key, item in data.items():
        clean_key = _clean(key, 100)
        if clean_key.casefold() in blocked or any(part in clean_key.casefold() for part in ("password", "secret", "token")):
            continue
        if isinstance(item, dict):
            result[clean_key] = _safe_config(item)
        elif isinstance(item, list):
            result[clean_key] = item[:500]
        elif isinstance(item, (str, int, float, bool)) or item is None:
            result[clean_key] = item
    return result


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("••••" if any(part in str(key).casefold() for part in ("password", "secret", "token", "hash")) else _redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value[:500]]
    return value


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("staff-registry is not initialized")
    return _db_path


async def _connect() -> aiosqlite.Connection:
    db = await aiosqlite.connect(_must_db(), timeout=30)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA busy_timeout=30000")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA synchronous=NORMAL")
    return db


async def _init_db() -> None:
    db = await _connect()
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS employees(
              id TEXT PRIMARY KEY,full_name TEXT NOT NULL,display_name TEXT NOT NULL DEFAULT '',
              job_profile TEXT NOT NULL DEFAULT 'custom',status TEXT NOT NULL DEFAULT 'active',
              email TEXT NOT NULL DEFAULT '',phone TEXT NOT NULL DEFAULT '',note TEXT NOT NULL DEFAULT '',
              version INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_staff_employees_status_name ON employees(status,full_name);
            CREATE TABLE IF NOT EXISTS employee_roles(
              employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
              role TEXT NOT NULL,scope TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,
              PRIMARY KEY(employee_id,role,scope));
            CREATE TABLE IF NOT EXISTS employee_identities(
              id INTEGER PRIMARY KEY AUTOINCREMENT,employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
              provider TEXT NOT NULL,external_id TEXT NOT NULL,username TEXT NOT NULL DEFAULT '',
              email TEXT NOT NULL DEFAULT '',phone TEXT NOT NULL DEFAULT '',verified INTEGER NOT NULL DEFAULT 0,
              source TEXT NOT NULL DEFAULT 'manual',metadata_json TEXT NOT NULL DEFAULT '{}',updated_at TEXT NOT NULL,
              UNIQUE(provider,external_id));
            CREATE INDEX IF NOT EXISTS idx_staff_identity_employee ON employee_identities(employee_id,provider);
            CREATE TABLE IF NOT EXISTS module_memberships(
              employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,module_id TEXT NOT NULL,
              desired_enabled INTEGER NOT NULL DEFAULT 0,config_json TEXT NOT NULL DEFAULT '{}',local_id TEXT NOT NULL DEFAULT '',
              sync_status TEXT NOT NULL DEFAULT 'pending',last_error TEXT NOT NULL DEFAULT '',
              applied_hash TEXT NOT NULL DEFAULT '',last_synced_at TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,
              PRIMARY KEY(employee_id,module_id));
            CREATE TABLE IF NOT EXISTS sync_jobs(
              id TEXT PRIMARY KEY,employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
              module_id TEXT NOT NULL,operation TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'queued',
              attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at REAL NOT NULL DEFAULT 0,
              idempotency_key TEXT NOT NULL UNIQUE,error TEXT NOT NULL DEFAULT '',result_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL,updated_at TEXT NOT NULL,finished_at TEXT NOT NULL DEFAULT '');
            CREATE INDEX IF NOT EXISTS idx_staff_jobs_due ON sync_jobs(status,next_attempt_at,created_at);
            CREATE TABLE IF NOT EXISTS audit_log(
              id INTEGER PRIMARY KEY AUTOINCREMENT,employee_id TEXT NOT NULL DEFAULT '',actor TEXT NOT NULL DEFAULT '',
              action TEXT NOT NULL,details_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_staff_audit_employee ON audit_log(employee_id,id DESC);
            CREATE TABLE IF NOT EXISTS discovery_candidates(
              candidate_key TEXT PRIMARY KEY,module_id TEXT NOT NULL,local_id TEXT NOT NULL,payload_json TEXT NOT NULL,
              match_employee_id TEXT NOT NULL DEFAULT '',match_reason TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'new',updated_at TEXT NOT NULL);
            """
        )
        await db.execute("UPDATE sync_jobs SET status='queued',next_attempt_at=0,updated_at=? WHERE status='processing'", (_now(),))
        await db.commit()
    finally:
        await db.close()


async def setup(ctx) -> None:
    global _db_path, _logger, _worker_task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", None)
    await _init_db()
    lifecycle = getattr(ctx, "lifecycle", None)
    create_task = lifecycle.create_task if lifecycle is not None else asyncio.create_task
    _worker_task = create_task(_sync_worker(), name="staff-registry-sync")


async def _require_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user):
        raise HTTPException(403, "Только администратор Nexus может менять сотрудников")
    return user


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        raise HTTPException(413, "Слишком большой запрос")
    try:
        data = json.loads(raw or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(400, "Некорректный JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(400, "Ожидается JSON-объект")
    return data


async def _audit(employee_id: str, actor: str, action: str, details: Any = None) -> None:
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO audit_log(employee_id,actor,action,details_json,created_at) VALUES(?,?,?,?,?)",
            (employee_id, _clean(actor, 200), action, json.dumps(_redact(details or {}), ensure_ascii=False), _now()),
        )
        await db.commit()
    finally:
        await db.close()


async def _employee(employee_id: str) -> dict[str, Any] | None:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM employees WHERE id=?", (employee_id,))).fetchone()
        if not row:
            return None
        roles = await (await db.execute(
            "SELECT role,scope FROM employee_roles WHERE employee_id=? ORDER BY role,scope", (employee_id,)
        )).fetchall()
        identities = await (await db.execute(
            "SELECT provider,external_id,username,email,phone,verified,source,metadata_json FROM employee_identities WHERE employee_id=? ORDER BY provider,external_id",
            (employee_id,),
        )).fetchall()
        memberships = await (await db.execute(
            "SELECT * FROM module_memberships WHERE employee_id=? ORDER BY module_id", (employee_id,)
        )).fetchall()
    finally:
        await db.close()
    item = dict(row)
    item["roles"] = [{"role": r["role"], "scope": r["scope"]} for r in roles]
    item["identities"] = [
        {**{key: value for key, value in dict(r).items() if key != "metadata_json"}, "verified": bool(r["verified"]), "metadata": _json(r["metadata_json"], {})}
        for r in identities
    ]
    item["memberships"] = [
        {**{key: value for key, value in dict(r).items() if key != "config_json"}, "desired_enabled": bool(r["desired_enabled"]), "config": _json(r["config_json"], {})}
        for r in memberships
    ]
    item["source_links"] = {
        row["module_id"]: {"local_id": row["local_id"]}
        for row in memberships if _clean(row["local_id"], 200)
    }
    return item


def _preset_modules(profile: str, full_name: str) -> dict[str, dict[str, Any]]:
    base = PROFILE_PRESETS.get(profile, PROFILE_PRESETS["custom"])["modules"]
    result = {module_id: dict(config) for module_id, config in base.items()}
    login = _slug(full_name)
    for module_id, config in result.items():
        if module_id in {"nexus-core", "sales-chats", "sbkvd-gpt", "student-transfer"}:
            config.setdefault("username" if module_id == "nexus-core" else "login", login)
        if module_id in {"sales-chats", "sbkvd-gpt", "student-transfer"}:
            config.setdefault("display_name", full_name)
    return result


async def _save_employee(data: dict[str, Any], actor: str, employee_id: str = "") -> tuple[dict[str, Any], list[str]]:
    full_name = _clean(data.get("full_name"), 200)
    if len(full_name) < 2:
        raise HTTPException(400, "Укажите имя сотрудника")
    status = _clean(data.get("status") or "active", 30)
    if status not in {"active", "suspended", "offboarded"}:
        raise HTTPException(400, "Некорректный статус сотрудника")
    profile = _clean(data.get("job_profile") or "custom", 50)
    if profile not in PROFILE_PRESETS:
        raise HTTPException(400, "Неизвестный профиль должности")
    employee_id = employee_id or str(uuid.uuid4())
    now = _now()
    roles_raw = data.get("roles")
    if roles_raw is None:
        roles_raw = PROFILE_PRESETS[profile]["roles"]
    roles: list[tuple[str, str]] = []
    for raw in roles_raw if isinstance(roles_raw, list) else []:
        if isinstance(raw, dict):
            role, scope = _clean(raw.get("role"), 80), _clean(raw.get("scope"), 120)
        else:
            role, scope = _clean(raw, 80), ""
        if role:
            roles.append((role, scope))
    identities: list[dict[str, Any]] = []
    for raw in data.get("identities") if isinstance(data.get("identities"), list) else []:
        if not isinstance(raw, dict):
            continue
        provider, external_id = _clean(raw.get("provider"), 80).casefold(), _clean(raw.get("external_id"), 300)
        if provider and external_id:
            identities.append({
                "provider": provider, "external_id": external_id,
                "username": _clean(raw.get("username"), 200), "email": _normalize_email(raw.get("email")),
                "phone": _normalize_phone(raw.get("phone")), "verified": bool(raw.get("verified")),
                "source": _clean(raw.get("source") or "manual", 80), "metadata": _safe_config(raw.get("metadata")),
            })
    raw_modules = data.get("modules")
    modules = _preset_modules(profile, full_name) if not isinstance(raw_modules, dict) else raw_modules
    normalized_modules: dict[str, tuple[bool, dict[str, Any]]] = {}
    for module_id, raw in modules.items():
        module_id = _clean(module_id, 100)
        if not module_id:
            continue
        if isinstance(raw, bool):
            enabled, config = raw, {}
        elif isinstance(raw, dict):
            enabled = bool(raw.get("enabled", True))
            config = _safe_config(raw.get("config") if isinstance(raw.get("config"), dict) else {k: v for k, v in raw.items() if k != "enabled"})
        else:
            continue
        normalized_modules[module_id] = (enabled, config)

    async with _write_lock:
        db = await _connect()
        try:
            existing = await (await db.execute("SELECT version FROM employees WHERE id=?", (employee_id,))).fetchone()
            expected = data.get("version")
            if existing and expected is not None and int(expected) != int(existing["version"]):
                raise HTTPException(409, "Карточка уже изменена в другом окне. Обновите данные")
            if existing:
                await db.execute(
                    "UPDATE employees SET full_name=?,display_name=?,job_profile=?,status=?,email=?,phone=?,note=?,version=version+1,updated_at=? WHERE id=?",
                    (full_name, _clean(data.get("display_name") or full_name, 200), profile, status,
                     _normalize_email(data.get("email")), _normalize_phone(data.get("phone")), _clean(data.get("note"), 3000), now, employee_id),
                )
            else:
                await db.execute(
                    "INSERT INTO employees(id,full_name,display_name,job_profile,status,email,phone,note,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (employee_id, full_name, _clean(data.get("display_name") or full_name, 200), profile, status,
                     _normalize_email(data.get("email")), _normalize_phone(data.get("phone")), _clean(data.get("note"), 3000), now, now),
                )
            await db.execute("DELETE FROM employee_roles WHERE employee_id=?", (employee_id,))
            await db.executemany(
                "INSERT INTO employee_roles(employee_id,role,scope,created_at) VALUES(?,?,?,?)",
                [(employee_id, role, scope, now) for role, scope in sorted(set(roles))],
            )
            if "identities" in data:
                await db.execute("DELETE FROM employee_identities WHERE employee_id=?", (employee_id,))
                for identity in identities:
                    try:
                        await db.execute(
                            "INSERT INTO employee_identities(employee_id,provider,external_id,username,email,phone,verified,source,metadata_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (employee_id, identity["provider"], identity["external_id"], identity["username"], identity["email"], identity["phone"],
                             int(identity["verified"]), identity["source"], json.dumps(identity["metadata"], ensure_ascii=False), now),
                        )
                    except aiosqlite.IntegrityError as exc:
                        raise HTTPException(409, f"{identity['provider']} ID уже связан с другим сотрудником") from exc
            current_rows = await (await db.execute(
                "SELECT module_id,desired_enabled,config_json FROM module_memberships WHERE employee_id=?", (employee_id,)
            )).fetchall()
            current = {row["module_id"]: row for row in current_rows}
            if isinstance(raw_modules, dict):
                for old_module in set(current).difference(normalized_modules):
                    normalized_modules[old_module] = (False, _json(current[old_module]["config_json"], {}))
            for module_id, (enabled, config) in normalized_modules.items():
                await db.execute(
                    """INSERT INTO module_memberships(employee_id,module_id,desired_enabled,config_json,sync_status,updated_at)
                       VALUES(?,?,?,?, 'pending',?) ON CONFLICT(employee_id,module_id) DO UPDATE SET
                       desired_enabled=excluded.desired_enabled,config_json=excluded.config_json,sync_status='pending',last_error='',updated_at=excluded.updated_at""",
                    (employee_id, module_id, int(enabled), json.dumps(config, ensure_ascii=False, separators=(",", ":")), now),
                )
            await db.commit()
        finally:
            await db.close()
    jobs = await _queue_employee(employee_id, actor=actor)
    await _audit(employee_id, actor, "employee_updated" if existing else "employee_created", {"profile": profile, "modules": list(normalized_modules)})
    item = await _employee(employee_id)
    assert item is not None
    return item, jobs


async def _queue_job(employee_id: str, module_id: str, operation: str, *, force: bool = False) -> str:
    now = _now()
    db = await _connect()
    try:
        desired = await (await db.execute(
            """SELECT e.version,e.status,m.desired_enabled,m.config_json
               FROM employees e JOIN module_memberships m ON m.employee_id=e.id
               WHERE e.id=? AND m.module_id=?""",
            (employee_id, module_id),
        )).fetchone()
        if not desired:
            raise HTTPException(404, "Настройка модуля для сотрудника не найдена")
        fingerprint = secrets.token_hex(16) if force else json.dumps(dict(desired), ensure_ascii=False, sort_keys=True)
        idempotency = hashlib.sha256(f"{employee_id}:{module_id}:{operation}:{fingerprint}".encode()).hexdigest()
        job_id = str(uuid.uuid4())
        try:
            await db.execute(
                "INSERT INTO sync_jobs(id,employee_id,module_id,operation,status,next_attempt_at,idempotency_key,created_at,updated_at) VALUES(?,?,?,?, 'queued',0,?,?,?)",
                (job_id, employee_id, module_id, operation, idempotency, now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            row = await (await db.execute("SELECT id FROM sync_jobs WHERE idempotency_key=?", (idempotency,))).fetchone()
            job_id = str(row["id"])
    finally:
        await db.close()
    _worker_wakeup.set()
    return job_id


async def _queue_employee(employee_id: str, *, actor: str = "", force: bool = False) -> list[str]:
    employee = await _employee(employee_id)
    if not employee:
        raise HTTPException(404, "Сотрудник не найден")
    jobs: list[str] = []
    active = employee["status"] == "active"
    for membership in employee["memberships"]:
        operation = "upsert" if active and membership["desired_enabled"] else "deactivate"
        jobs.append(await _queue_job(employee_id, membership["module_id"], operation, force=force))
    if actor:
        await _audit(employee_id, actor, "sync_queued", {"jobs": jobs})
    return jobs


def _module_service(module_id: str, name: str):
    module = sys.modules.get(f"_nexus_mod_{module_id}")
    return getattr(module, name, None) if module is not None else None


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _nexus_apply(employee: dict[str, Any], config: dict[str, Any], operation: str, password: str = "") -> dict[str, Any]:
    users = await get_all_users()
    source = employee.get("source_links", {}).get("nexus-core") or {}
    local_id = _clean(source.get("local_id") if isinstance(source, dict) else source, 40)
    username = _clean(config.get("username"), 100)
    row = next((item for item in users if local_id and str(item["id"]) == local_id), None)
    if row is None and username:
        row = next((item for item in users if item["username"].casefold() == username.casefold()), None)
    if operation == "deactivate":
        if not row:
            return {"ok": True, "changed": False, "local_id": "", "status": "not_linked"}
        await update_user(int(row["id"]), row["role"], row["module_access"], 0)
        return {"ok": True, "changed": bool(row["active"]), "local_id": str(row["id"]), "status": "disabled"}
    role = _clean(config.get("role") or "viewer", 20)
    if role not in {"viewer", "editor", "admin"}:
        raise ValueError("Некорректная роль Nexus")
    module_access = config.get("module_access") if isinstance(config.get("module_access"), list) else []
    module_access = [_clean(value, 100) for value in module_access[:100] if _clean(value, 100)]
    if row:
        await update_user(int(row["id"]), role, json.dumps(module_access, ensure_ascii=False), 1)
        if password:
            from orchestrator.auth import pwd_ctx
            await update_user_password(int(row["id"]), pwd_ctx.hash(password))
        return {"ok": True, "changed": True, "local_id": str(row["id"]), "status": "active"}
    if not username:
        raise ValueError("Укажите логин Nexus")
    if len(password) < 8:
        return {"ok": False, "needs_input": "password", "error": "Для нового аккаунта Nexus нужен пароль не короче 8 символов"}
    from orchestrator.auth import pwd_ctx
    user_id = await create_user(username, pwd_ctx.hash(password), role, json.dumps(module_access, ensure_ascii=False))
    return {"ok": True, "changed": True, "local_id": str(user_id), "status": "active"}


async def _apply_job(row: dict[str, Any]) -> None:
    job_id, employee_id, module_id = row["id"], row["employee_id"], row["module_id"]
    attempts = int(row["attempts"] or 0) + 1
    db = await _connect()
    try:
        await db.execute("UPDATE sync_jobs SET status='processing',attempts=?,updated_at=? WHERE id=?", (attempts, _now(), job_id))
        await db.commit()
    finally:
        await db.close()
    employee = await _employee(employee_id)
    if not employee:
        return
    membership = next((item for item in employee["memberships"] if item["module_id"] == module_id), None)
    if not membership:
        return
    operation = row["operation"]
    desired_operation = "upsert" if employee["status"] == "active" and membership["desired_enabled"] else "deactivate"
    if operation != desired_operation:
        await _finish_job(
            row,
            result={"ok": True, "superseded": True, "desired_operation": desired_operation},
            status="done",
            membership_status="pending",
        )
        return
    try:
        if module_id == "nexus-core":
            result = await _nexus_apply(employee, membership["config"], operation)
        else:
            service = _module_service(module_id, "service_staff_apply")
            if not callable(service):
                raise LookupError("Модуль не установлен или ещё не поддерживает единый реестр")
            result = await _call(service(
                employee=employee, config=membership["config"], operation=operation,
                idempotency_key=row["idempotency_key"],
            ))
        if not isinstance(result, dict):
            raise RuntimeError("Коннектор вернул некорректный ответ")
        if not result.get("ok"):
            if result.get("needs_input"):
                await _finish_job(row, result=result, status="needs_input", membership_status="needs_input", error=_clean(result.get("error"), 1000))
                return
            raise RuntimeError(_clean(result.get("error") or "Модуль не применил изменение", 1000))
        local_id = _clean(result.get("local_id") or (result.get("snapshot") or {}).get("local_id") or membership.get("local_id"), 200)
        applied = hashlib.sha256(json.dumps({"operation": operation, "config": membership["config"]}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        await _finish_job(row, result=result, status="done", membership_status="applied", local_id=local_id, applied_hash=applied)
    except LookupError as exc:
        await _finish_job(row, result={}, status="failed", membership_status="unsupported", error=str(exc))
    except Exception as exc:
        error = _clean(str(exc) or type(exc).__name__, 1000)
        if attempts < SYNC_MAX_ATTEMPTS:
            delay = min(300, 2 ** attempts)
            db = await _connect()
            try:
                await db.execute(
                    "UPDATE sync_jobs SET status='retry',next_attempt_at=?,error=?,updated_at=? WHERE id=?",
                    (time.time() + delay, error, _now(), job_id),
                )
                await db.execute(
                    "UPDATE module_memberships SET sync_status='retry',last_error=?,updated_at=? WHERE employee_id=? AND module_id=?",
                    (error, _now(), employee_id, module_id),
                )
                await db.commit()
            finally:
                await db.close()
        else:
            await _finish_job(row, result={}, status="failed", membership_status="error", error=error)


async def _finish_job(
    row: dict[str, Any], *, result: dict[str, Any], status: str, membership_status: str,
    error: str = "", local_id: str = "", applied_hash: str = "",
) -> None:
    now = _now()
    db = await _connect()
    try:
        await db.execute(
            "UPDATE sync_jobs SET status=?,error=?,result_json=?,updated_at=?,finished_at=? WHERE id=?",
            (status, error, json.dumps(_redact(result), ensure_ascii=False), now, now, row["id"]),
        )
        await db.execute(
            """UPDATE module_memberships SET sync_status=?,last_error=?,local_id=CASE WHEN ?!='' THEN ? ELSE local_id END,
               applied_hash=CASE WHEN ?!='' THEN ? ELSE applied_hash END,last_synced_at=?,updated_at=?
               WHERE employee_id=? AND module_id=?""",
            (membership_status, error, local_id, local_id, applied_hash, applied_hash, now, now, row["employee_id"], row["module_id"]),
        )
        await db.commit()
    finally:
        await db.close()
    await _audit(row["employee_id"], "staff-registry", f"sync_{status}", {"module_id": row["module_id"], "error": error})


async def _resolve_input_jobs(employee_id: str, module_id: str, local_id: str) -> None:
    now = _now()
    db = await _connect()
    try:
        await db.execute(
            """UPDATE sync_jobs SET status='done',error='',result_json=?,updated_at=?,finished_at=?
               WHERE employee_id=? AND module_id=? AND status='needs_input'""",
            (json.dumps({"ok": True, "input_resolved": True, "local_id": local_id}), now, now, employee_id, module_id),
        )
        await db.commit()
    finally:
        await db.close()


async def _sync_worker() -> None:
    while True:
        try:
            db = await _connect()
            try:
                row = await (await db.execute(
                    "SELECT * FROM sync_jobs WHERE status IN ('queued','retry') AND next_attempt_at<=? ORDER BY created_at,rowid LIMIT 1",
                    (time.time(),),
                )).fetchone()
            finally:
                await db.close()
            if row:
                await _apply_job(dict(row))
                continue
            _worker_wakeup.clear()
            try:
                await asyncio.wait_for(_worker_wakeup.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.exception("staff registry worker failed: %s", exc)
            await asyncio.sleep(2)


async def _connector_catalog() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for module_id, built_in in MODULE_DEFINITIONS.items():
        item = {"module_id": module_id, **built_in, "available": module_id == "nexus-core", "connector_version": 0}
        if module_id != "nexus-core":
            service = _module_service(module_id, "service_staff_connector")
            if callable(service):
                try:
                    remote = await _call(service())
                    if isinstance(remote, dict):
                        item.update(_redact(remote))
                    item["available"] = True
                except Exception as exc:
                    item["error"] = _clean(exc, 500)
        result.append(item)
    return result


async def _discover_candidates(actor: str) -> dict[str, Any]:
    sources: list[tuple[str, list[dict[str, Any]]]] = []
    users = await get_all_users()
    sources.append(("nexus-core", [
        {"local_id": str(row["id"]), "full_name": row["username"], "display_name": row["username"],
         "active": bool(row["active"]), "identities": [{"provider": "nexus", "external_id": row["username"], "username": row["username"]}],
         "config": {"username": row["username"], "role": row["role"], "module_access": _json(row["module_access"], [])}}
        for row in users
    ]))
    errors: list[dict[str, str]] = []
    for module_id in MODULE_DEFINITIONS:
        if module_id == "nexus-core":
            continue
        service = _module_service(module_id, "service_staff_list")
        if not callable(service):
            continue
        try:
            rows = await _call(service())
            if isinstance(rows, dict):
                rows = rows.get("items") or []
            sources.append((module_id, [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []))
        except Exception as exc:
            errors.append({"module_id": module_id, "error": _clean(exc, 500)})
    db = await _connect()
    discovered = 0
    try:
        for module_id, rows in sources:
            for raw in rows[:1000]:
                local_id = _clean(raw.get("local_id") or raw.get("id"), 200)
                if not local_id:
                    continue
                identities = raw.get("identities") if isinstance(raw.get("identities"), list) else []
                match_id, reason = "", ""
                link = await (await db.execute(
                    "SELECT employee_id FROM module_memberships WHERE module_id=? AND local_id=?", (module_id, local_id)
                )).fetchone()
                if link:
                    match_id, reason = link["employee_id"], "source_link"
                if not match_id:
                    for identity in identities:
                        provider, external_id = _clean(identity.get("provider"), 80).casefold(), _clean(identity.get("external_id"), 300)
                        if not provider or not external_id:
                            continue
                        linked = await (await db.execute(
                            "SELECT employee_id FROM employee_identities WHERE provider=? AND external_id=?", (provider, external_id)
                        )).fetchone()
                        if linked:
                            match_id, reason = linked["employee_id"], f"identity:{provider}"
                            break
                payload = _redact({**raw, "module_id": module_id, "local_id": local_id})
                candidate_key = hashlib.sha256(f"{module_id}:{local_id}".encode()).hexdigest()
                await db.execute(
                    """INSERT INTO discovery_candidates(candidate_key,module_id,local_id,payload_json,match_employee_id,match_reason,status,updated_at)
                       VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(candidate_key) DO UPDATE SET payload_json=excluded.payload_json,
                       match_employee_id=excluded.match_employee_id,match_reason=excluded.match_reason,
                       status=CASE WHEN discovery_candidates.status='ignored' THEN 'ignored' ELSE excluded.status END,updated_at=excluded.updated_at""",
                    (candidate_key, module_id, local_id, json.dumps(payload, ensure_ascii=False), match_id, reason,
                     "linked" if match_id else "new", _now()),
                )
                discovered += 1
        await db.commit()
    finally:
        await db.close()
    await _audit("", actor, "discovery_completed", {"discovered": discovered, "errors": errors})
    return {"ok": True, "discovered": discovered, "sources": [{"module_id": m, "count": len(r)} for m, r in sources], "errors": errors}


@router.get("/health")
async def health() -> dict[str, Any]:
    db = await _connect()
    try:
        employees = int((await (await db.execute("SELECT COUNT(*) FROM employees")).fetchone())[0])
        pending = int((await (await db.execute("SELECT COUNT(*) FROM sync_jobs WHERE status IN ('queued','retry','processing')")).fetchone())[0])
    finally:
        await db.close()
    return {"ok": True, "employees": employees, "pending_jobs": pending, "worker": bool(_worker_task and not _worker_task.done())}


@router.get("/capabilities")
async def capabilities(request: Request) -> dict[str, Any]:
    user = await _require_user(request)
    return {
        "ok": True,
        "can_manage": require_admin(user),
        "profiles": PROFILE_PRESETS,
        "modules": await _connector_catalog(),
    }


@router.get("/employees")
async def employees(request: Request, q: str = "", status: str = "", limit: int = Query(500, ge=1, le=1000)) -> dict[str, Any]:
    await _require_user(request)
    where, params = ["1=1"], []
    if status:
        where.append("status=?"); params.append(_clean(status, 30))
    if q:
        where.append("(full_name LIKE ? OR display_name LIKE ? OR email LIKE ? OR phone LIKE ?)")
        needle = f"%{_clean(q, 200)}%"; params.extend([needle] * 4)
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"""SELECT e.*,
            (SELECT COUNT(*) FROM module_memberships m WHERE m.employee_id=e.id AND m.desired_enabled=1) module_count,
            (SELECT COUNT(*) FROM module_memberships m WHERE m.employee_id=e.id AND m.sync_status IN ('error','retry','unsupported','needs_input')) problem_count
            FROM employees e WHERE {' AND '.join(where)} ORDER BY CASE e.status WHEN 'active' THEN 0 WHEN 'suspended' THEN 1 ELSE 2 END,e.full_name LIMIT ?""",
            (*params, limit),
        )).fetchall()
    finally:
        await db.close()
    return {"ok": True, "items": [dict(row) for row in rows]}


@router.get("/employees/{employee_id}")
async def employee_detail(employee_id: str, request: Request) -> dict[str, Any]:
    await _require_user(request)
    item = await _employee(_clean(employee_id, 50))
    if not item:
        raise HTTPException(404, "Сотрудник не найден")
    return {"ok": True, "employee": item}


@router.post("/employees")
async def create_employee(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    item, jobs = await _save_employee(await _body(request), user["username"])
    return {"ok": True, "employee": item, "jobs": jobs}


@router.put("/employees/{employee_id}")
async def update_employee(employee_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    if not await _employee(_clean(employee_id, 50)):
        raise HTTPException(404, "Сотрудник не найден")
    item, jobs = await _save_employee(await _body(request), user["username"], _clean(employee_id, 50))
    return {"ok": True, "employee": item, "jobs": jobs}


@router.post("/employees/{employee_id}/status")
async def employee_status(employee_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await _body(request)
    status = _clean(data.get("status"), 30)
    if status not in {"active", "suspended", "offboarded"}:
        raise HTTPException(400, "Некорректный статус")
    db = await _connect()
    try:
        cur = await db.execute("UPDATE employees SET status=?,version=version+1,updated_at=? WHERE id=?", (status, _now(), employee_id))
        await db.commit()
    finally:
        await db.close()
    if not cur.rowcount:
        raise HTTPException(404, "Сотрудник не найден")
    jobs = await _queue_employee(employee_id, actor=user["username"], force=True)
    await _audit(employee_id, user["username"], "status_changed", {"status": status})
    return {"ok": True, "status": status, "jobs": jobs}


@router.post("/employees/{employee_id}/sync")
async def sync_employee(employee_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    jobs = await _queue_employee(employee_id, actor=user["username"], force=True)
    return {"ok": True, "jobs": jobs}


@router.post("/employees/{employee_id}/nexus-password")
async def nexus_password(employee_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await _body(request)
    password = str(data.get("password") or "")
    if len(password) < 8:
        raise HTTPException(400, "Пароль должен быть не короче 8 символов")
    employee = await _employee(employee_id)
    if not employee:
        raise HTTPException(404, "Сотрудник не найден")
    membership = next((item for item in employee["memberships"] if item["module_id"] == "nexus-core"), None)
    if not membership or not membership["desired_enabled"]:
        raise HTTPException(409, "Сначала включите Nexus в карточке сотрудника")
    result = await _nexus_apply(employee, membership["config"], "upsert", password=password)
    if not result.get("ok"):
        raise HTTPException(409, _clean(result.get("error") or "Не удалось создать аккаунт Nexus", 1000))
    db = await _connect()
    try:
        await db.execute(
            "UPDATE module_memberships SET local_id=?,sync_status='applied',last_error='',last_synced_at=?,updated_at=? WHERE employee_id=? AND module_id='nexus-core'",
            (_clean(result.get("local_id"), 100), _now(), _now(), employee_id),
        )
        await db.commit()
    finally:
        await db.close()
    await _resolve_input_jobs(employee_id, "nexus-core", _clean(result.get("local_id"), 100))
    await _audit(employee_id, user["username"], "nexus_password_set", {"local_id": result.get("local_id")})
    return {"ok": True, "local_id": result.get("local_id")}


@router.post("/employees/{employee_id}/module-password")
async def module_password(employee_id: str, request: Request) -> dict[str, Any]:
    """Apply a password once without ever persisting it in the registry database."""
    user = await _require_admin(request)
    data = await _body(request)
    module_id = _clean(data.get("module_id"), 100)
    password = str(data.get("password") or "")
    if module_id == "nexus-core":
        return await nexus_password(employee_id, request)
    if module_id != "student-transfer":
        raise HTTPException(400, "Этот модуль не поддерживает установку пароля из реестра")
    if len(password) < 8 or len(password) > 200:
        raise HTTPException(400, "Пароль должен содержать от 8 до 200 символов")
    employee = await _employee(_clean(employee_id, 50))
    if not employee:
        raise HTTPException(404, "Сотрудник не найден")
    membership = next((item for item in employee["memberships"] if item["module_id"] == module_id), None)
    if not membership or not membership["desired_enabled"]:
        raise HTTPException(409, "Сначала включите модуль в карточке сотрудника")
    service = _module_service(module_id, "service_staff_apply")
    if not callable(service):
        raise HTTPException(409, "Модуль не установлен или не поддерживает единый реестр")
    transient_config = {**membership["config"], "password": password}
    result = await _call(service(
        employee=employee, config=transient_config, operation="upsert",
        idempotency_key=secrets.token_hex(24),
    ))
    if not isinstance(result, dict) or not result.get("ok"):
        message = result.get("error") if isinstance(result, dict) else ""
        raise HTTPException(409, _clean(message or "Не удалось установить пароль", 1000))
    local_id = _clean(result.get("local_id") or membership.get("local_id"), 200)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE module_memberships SET local_id=?,sync_status='applied',last_error='',last_synced_at=?,updated_at=? WHERE employee_id=? AND module_id=?",
            (local_id, _now(), _now(), employee_id, module_id),
        )
        await db.commit()
    finally:
        await db.close()
    await _resolve_input_jobs(employee_id, module_id, local_id)
    await _audit(employee_id, user["username"], "module_password_set", {"module_id": module_id, "local_id": local_id})
    return {"ok": True, "module_id": module_id, "local_id": local_id}


async def _messenger_employee_service(employee_id: str, service_name: str) -> tuple[dict[str, Any], Any]:
    employee = await _employee(_clean(employee_id, 50))
    if not employee:
        raise HTTPException(404, "Сотрудник не найден")
    membership = next((item for item in employee["memberships"] if item["module_id"] == "messenger-widget"), None)
    if not membership or not membership["desired_enabled"] or not membership.get("local_id"):
        raise HTTPException(409, "Сначала свяжите активный аккаунт Messenger с сотрудником")
    service = _module_service("messenger-widget", service_name)
    if not callable(service):
        raise HTTPException(409, "Messenger не поддерживает управление доступом из реестра")
    return employee, service


@router.get("/employees/{employee_id}/messenger-access")
async def messenger_access(employee_id: str, request: Request) -> dict[str, Any]:
    await _require_user(request)
    employee, service = await _messenger_employee_service(employee_id, "service_staff_access")
    try:
        result = await _call(service(employee=employee))
    except ValueError as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc
    return result if isinstance(result, dict) else {"ok": False, "devices": []}


@router.post("/employees/{employee_id}/messenger-activation-code")
async def messenger_activation_code(employee_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    employee, service = await _messenger_employee_service(employee_id, "service_staff_issue_activation_code")
    try:
        result = await _call(service(employee=employee))
    except ValueError as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc
    if not isinstance(result, dict) or not result.get("ok"):
        raise HTTPException(409, "Не удалось выдать код Messenger")
    await _audit(employee_id, user["username"], "messenger_activation_code_issued", {
        "reissued": bool(result.get("reissued")), "revoked_devices": int(result.get("revoked_devices") or 0),
    })
    return result


@router.delete("/employees/{employee_id}/messenger-devices/{device_id}")
async def messenger_revoke_device(employee_id: str, device_id: int, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    employee, service = await _messenger_employee_service(employee_id, "service_staff_revoke_device")
    try:
        result = await _call(service(employee=employee, device_id=int(device_id)))
    except ValueError as exc:
        raise HTTPException(409, _clean(exc, 1000)) from exc
    await _audit(employee_id, user["username"], "messenger_device_revoked", {"device_id": int(device_id)})
    return result if isinstance(result, dict) else {"ok": True, "device_id": int(device_id)}


@router.get("/jobs")
async def jobs(request: Request, employee_id: str = "", limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    await _require_user(request)
    where, params = ["1=1"], []
    if employee_id:
        where.append("employee_id=?"); params.append(_clean(employee_id, 50))
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"SELECT * FROM sync_jobs WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT ?", (*params, limit)
        )).fetchall()
    finally:
        await db.close()
    return {"ok": True, "items": [{**{k: v for k, v in dict(row).items() if k != "result_json"}, "result": _json(row["result_json"], {})} for row in rows]}


@router.post("/jobs/{job_id}/retry")
async def retry_job(job_id: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    db = await _connect()
    try:
        row = await (await db.execute(
            """SELECT j.employee_id,j.module_id,e.status,m.desired_enabled
               FROM sync_jobs j JOIN employees e ON e.id=j.employee_id
               JOIN module_memberships m ON m.employee_id=j.employee_id AND m.module_id=j.module_id
               WHERE j.id=?""",
            (job_id,),
        )).fetchone()
    finally:
        await db.close()
    if not row:
        raise HTTPException(404, "Задача не найдена")
    operation = "upsert" if row["status"] == "active" and bool(row["desired_enabled"]) else "deactivate"
    new_id = await _queue_job(row["employee_id"], row["module_id"], operation, force=True)
    await _audit(row["employee_id"], user["username"], "job_retried", {"old_job_id": job_id, "new_job_id": new_id})
    return {"ok": True, "job_id": new_id}


@router.post("/discovery/run")
async def discovery_run(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    return await _discover_candidates(user["username"])


@router.get("/discovery")
async def discovery(request: Request, status: str = "", limit: int = Query(500, ge=1, le=2000)) -> dict[str, Any]:
    await _require_user(request)
    where, params = ["1=1"], []
    if status:
        where.append("status=?"); params.append(_clean(status, 30))
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"SELECT * FROM discovery_candidates WHERE {' AND '.join(where)} ORDER BY module_id,local_id LIMIT ?", (*params, limit)
        )).fetchall()
    finally:
        await db.close()
    return {"ok": True, "items": [
        {**{k: v for k, v in dict(row).items() if k != "payload_json"}, "payload": _json(row["payload_json"], {})}
        for row in rows
    ]}


@router.post("/discovery/{candidate_key}/link")
async def discovery_link(candidate_key: str, request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    data = await _body(request)
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM discovery_candidates WHERE candidate_key=?", (candidate_key,))).fetchone()
    finally:
        await db.close()
    if not row:
        raise HTTPException(404, "Найденная запись не существует")
    if data.get("ignore"):
        db = await _connect()
        try:
            await db.execute("UPDATE discovery_candidates SET status='ignored',updated_at=? WHERE candidate_key=?", (_now(), candidate_key))
            await db.commit()
        finally:
            await db.close()
        return {"ok": True, "status": "ignored"}
    employee_id = _clean(data.get("employee_id"), 50)
    payload = _json(row["payload_json"], {})
    if not employee_id and data.get("create"):
        item, _jobs = await _save_employee({
            "full_name": payload.get("full_name") or payload.get("display_name") or payload.get("login") or "Новый сотрудник",
            "display_name": payload.get("display_name") or payload.get("full_name"),
            "job_profile": "custom", "status": "active" if payload.get("active", True) else "suspended",
            "email": payload.get("email"), "phone": payload.get("phone"),
            "identities": payload.get("identities") or [],
            # Link the already existing local account below before any sync is queued.
            "modules": {},
        }, user["username"])
        employee_id = item["id"]
    if not employee_id or not await _employee(employee_id):
        raise HTTPException(400, "Выберите сотрудника или создайте новую карточку")
    now = _now()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO module_memberships(employee_id,module_id,desired_enabled,config_json,local_id,sync_status,last_synced_at,updated_at)
               VALUES(?,?,?,?,?,'applied',?,?) ON CONFLICT(employee_id,module_id) DO UPDATE SET
               local_id=excluded.local_id,config_json=excluded.config_json,sync_status='applied',last_error='',last_synced_at=excluded.last_synced_at,updated_at=excluded.updated_at""",
            (employee_id, row["module_id"], int(payload.get("active", True)), json.dumps(_safe_config(payload.get("config")), ensure_ascii=False), row["local_id"], now, now),
        )
        for identity in payload.get("identities") or []:
            provider, external_id = _clean(identity.get("provider"), 80).casefold(), _clean(identity.get("external_id"), 300)
            if provider and external_id:
                try:
                    await db.execute(
                        "INSERT INTO employee_identities(employee_id,provider,external_id,username,email,phone,verified,source,metadata_json,updated_at) VALUES(?,?,?,?,?,?,1,?,'{}',?)",
                        (employee_id, provider, external_id, _clean(identity.get("username"), 200), _normalize_email(identity.get("email")), _normalize_phone(identity.get("phone")), row["module_id"], now),
                    )
                except aiosqlite.IntegrityError as exc:
                    owner = await (await db.execute(
                        "SELECT employee_id FROM employee_identities WHERE provider=? AND external_id=?",
                        (provider, external_id),
                    )).fetchone()
                    if not owner or owner["employee_id"] != employee_id:
                        raise HTTPException(409, f"{provider} ID уже связан с другим сотрудником") from exc
        await db.execute(
            "UPDATE discovery_candidates SET match_employee_id=?,match_reason='manual',status='linked',updated_at=? WHERE candidate_key=?",
            (employee_id, now, candidate_key),
        )
        await db.commit()
    finally:
        await db.close()
    await _audit(employee_id, user["username"], "discovery_linked", {"module_id": row["module_id"], "local_id": row["local_id"]})
    return {"ok": True, "employee_id": employee_id}


@router.get("/audit")
async def audit(request: Request, employee_id: str = "", limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    await _require_user(request)
    where, params = ["1=1"], []
    if employee_id:
        where.append("employee_id=?"); params.append(_clean(employee_id, 50))
    db = await _connect()
    try:
        rows = await (await db.execute(
            f"SELECT * FROM audit_log WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ?", (*params, limit)
        )).fetchall()
    finally:
        await db.close()
    return {"ok": True, "items": [
        {**{k: v for k, v in dict(row).items() if k != "details_json"}, "details": _json(row["details_json"], {})}
        for row in rows
    ]}
