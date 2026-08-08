from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "bizon-amocrm"
DEFAULT_SETTINGS = {
    "feed_url": "http://127.0.0.1:8080/bizon-reports/api/attendance-feed",
    "feed_token": "",
    "dry_run": "1",
    "poll_enabled": "1",
    "poll_seconds": "15",
    "request_timeout": "20",
    "feed_cursor": "0",
    "sample_preset_json": "{}",
}
DEFAULT_DUPLICATE_RULES = [
    {"entity": "contacts", "field_code": "PHONE", "source": "phone"},
    {"entity": "contacts", "field_code": "EMAIL", "source": "email"},
]
EXCLUDE_OPERATORS = {
    "equals",
    "not_equals",
    "contains",
    "not_contains",
    "starts_with",
    "ends_with",
    "is_empty",
    "is_not_empty",
}
DEFAULT_LEAD_NAME_TEMPLATE = "{webinar_date} | {webinar_time} | {watch_minutes_round}м | {username}"
BIZON_FIELD_DEFINITIONS = [
    ("username", "Имя участника", "text", "Контакт", "Имя из анкеты/чата Bizon"),
    ("phone", "Телефон", "phone", "Контакт", "Нормализованный телефон участника"),
    ("email", "Email", "email", "Контакт", "Email участника в нижнем регистре"),
    ("city", "Город", "text", "Контакт", "Город по данным отчёта"),
    ("country", "Страна", "text", "Контакт", "Страна участника"),
    ("country_code", "Код страны", "text", "Контакт", "Двухбуквенный код страны"),
    ("webinarId", "ID вебинара", "text", "Вебинар", "Полный идентификатор отчёта Bizon"),
    ("roomid", "Комната", "text", "Вебинар", "Идентификатор комнаты Bizon"),
    ("room_slug", "Код комнаты", "text", "Вебинар", "Часть идентификатора комнаты после двоеточия: 97242:lay → lay"),
    ("room_title", "Название комнаты", "text", "Вебинар", "Человекочитаемое название комнаты"),
    ("webinar_at", "Дата и время вебинара", "date_time", "Вебинар", "Время, извлечённое из webinarId"),
    ("webinar_date", "Дата вебинара ДД.ММ.ГГГГ", "text", "Вебинар", "Вычисленная дата по Москве для шаблонов"),
    ("webinar_time", "Время вебинара ЧЧ:ММ", "text", "Вебинар", "Вычисленное время по Москве для шаблонов"),
    ("created", "Дата создания отчёта", "date_time", "Вебинар", "Дата из метаданных Bizon"),
    ("type", "Тип вебинара", "text", "Вебинар", "Тип отчёта/вебинара"),
    ("watch_minutes", "Минут на вебинаре", "number", "Посещение", "Сумма объединённых интервалов просмотра"),
    ("watch_minutes_round", "Минуты, округлённые", "number", "Посещение", "Целое число минут для названия сделки"),
    ("watch_seconds", "Секунд на вебинаре", "number", "Посещение", "Точная длительность просмотра"),
    ("watch_valid", "Длительность валидна", "boolean", "Посещение", "Проверка корректности длительности"),
    ("watch_source", "Источник длительности", "text", "Посещение", "vi или диапазон view/viewTill"),
    ("watch_error", "Ошибка длительности", "text", "Посещение", "Причина невалидной длительности"),
    ("finished", "Досмотрел до конца", "boolean", "Посещение", "Флаг finished из Bizon"),
    ("clicked_button", "Нажал кнопку", "boolean", "Посещение", "Есть хотя бы один клик по кнопке"),
    ("clicked_banner", "Нажал баннер", "boolean", "Посещение", "Есть хотя бы один клик по баннеру"),
    ("buttons", "Клики по кнопкам", "array", "Посещение", "Исходный список кликов по кнопкам"),
    ("banners", "Клики по баннерам", "array", "Посещение", "Исходный список кликов по баннерам"),
    ("view", "Начало просмотра", "number", "Посещение", "Исходное время первого входа"),
    ("viewTill", "Конец просмотра", "number", "Посещение", "Исходное время последнего выхода"),
    ("profile_count", "Число объединённых входов", "number", "Посещение", "Сколько профилей Bizon объединено в участника"),
    ("chat_message_count", "Количество сообщений в чате", "number", "Чат", "Количество реплик участника в полном отчёте Bizon"),
    ("chat_messages_text", "Сообщения и ответы в чате", "text", "Чат", "Реплики участника с таймкодами; Bizon не разделяет комментарии и ответы ИИ"),
    ("attendance_key", "Ключ посещения", "text", "Служебное", "Стабильный ключ участник + вебинар"),
    ("person_key", "Ключ участника", "text", "Служебное", "Телефон/email/Bizon identity"),
    ("utm_source", "utm_source", "text", "UTM", "Источник трафика"),
    ("utm_medium", "utm_medium", "text", "UTM", "Канал трафика"),
    ("utm_campaign", "utm_campaign", "text", "UTM", "Кампания"),
    ("utm_content", "utm_content", "text", "UTM", "Контент объявления"),
    ("utm_term", "utm_term", "text", "UTM", "Ключ/идентификатор пользователя"),
    ("messenger_type", "Мессенджер из Customer DB", "text", "Nexus", "telegram или vk при однозначном совпадении utm_term в базе клиентов Nexus"),
    ("dialog_salebot_url", "Ссылка на диалог SaleBot", "url", "Nexus", "Заполняется только для utm_term, найденного в таблице Telegram"),
    ("dialog_vk_url", "Ссылка на диалог ВКонтакте", "url", "Nexus", "Заполняется только для utm_term, найденного в таблице ВКонтакте"),
    ("source_user_id", "ID участника Bizon", "text", "Идентификаторы", "chatUserId; резерв: uid, sid или person_key"),
    ("_ym_uid", "_ym_uid", "text", "Идентификаторы", "_ym_uid; резерв: ym_uid, param1 или p1"),
    ("p1", "Параметр p1", "text", "Метки", "Пользовательский параметр Bizon"),
    ("p2", "Параметр p2", "text", "Метки", "Пользовательский параметр Bizon"),
    ("p3", "Параметр p3", "text", "Метки", "Пользовательский параметр Bizon"),
    ("sup", "Параметр sup", "text", "Метки", "Пользовательский параметр Bizon"),
    ("cu1", "Параметр cu1", "text", "Метки", "Пользовательский параметр Bizon"),
    ("c1", "Параметр c1", "text", "Метки", "Пользовательский параметр Bizon"),
    ("cv", "Параметр cv", "text", "Метки", "Пользовательский параметр Bizon"),
    ("page", "Страница входа", "text", "Источник", "Страница, с которой пришёл участник"),
    ("partner", "Партнёр", "text", "Источник", "Партнёрская метка"),
    ("referer", "Referer", "text", "Источник", "HTTP referer из отчёта"),
    ("url", "URL", "text", "Источник", "URL из профиля Bizon"),
    ("comment", "Комментарий", "text", "Анкета", "Комментарий участника"),
    ("ticket", "Билет", "text", "Анкета", "Билет/идентификатор регистрации"),
    ("vizitForm", "Форма посещения", "object", "Анкета", "Исходные ответы формы"),
    ("newOrder", "Новый заказ", "object", "Заказы", "Данные заказа из Bizon"),
    ("orderDetails", "Детали заказа", "object", "Заказы", "Подробности заказа"),
    ("chatUserId", "Bizon chatUserId", "text", "Служебное", "ID пользователя чата"),
    ("uid", "Bizon uid", "text", "Служебное", "UID участника"),
    ("sid", "Bizon sid", "text", "Служебное", "SID участника"),
    ("ip", "IP", "text", "Служебное", "IP из отчёта Bizon"),
]

_db_path: Path | None = None
_logger: logging.Logger | None = None
_poll_task: asyncio.Task | None = None
_process_lock = asyncio.Lock()
_field_cache: dict[str, list[dict[str, Any]]] = {}
_pipeline_cache: list[dict[str, Any]] = []


class SettingsIn(BaseModel):
    feed_url: str | None = None
    feed_token: str | None = None
    dry_run: bool | None = None
    poll_enabled: bool | None = None
    poll_seconds: int | None = None
    request_timeout: int | None = None


class BindingIn(BaseModel):
    id: int | None = None
    name: str = ""
    match_type: str = "room"
    match_value: str = ""
    priority: int = 100
    threshold_minutes: float = 60
    min_minutes: float | None = None
    max_minutes: float | None = None
    pipeline_id: str = ""
    status_id: str = ""
    click_status_id: str = ""
    pipeline_scope: list[str] = []
    status_scope: list[str] = []
    duplicate_action: str = "note_only"
    duplicate_rules: list[dict[str, Any]] = []
    exclude_conditions: list[dict[str, Any]] = []
    note_only_status_ids: list[str] = []
    responsible_user_ids: list[str] = []
    tags: list[str] = []
    field_mappings: list[dict[str, Any]] = []
    lead_name_template: str = DEFAULT_LEAD_NAME_TEMPLATE
    note_template: str = ""
    active: bool = True


class RetryIn(BaseModel):
    event_id: int


class PresetIn(BaseModel):
    lead_ids: list[int]


def setup(ctx):
    global _db_path, _logger, _poll_task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.bizon-amocrm"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        if _poll_task is None or _poll_task.done():
            _poll_task = loop.create_task(_poll_loop())
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _poll_task
    task, _poll_task = _poll_task, None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("bizon-amocrm is not initialized")
    return _db_path


def _customer_db_path() -> Path:
    env_path = os.environ.get("BIZON_AMOCRM_CUSTOMER_DB_PATH", "").strip()
    if env_path:
        return Path(env_path)
    module_db = _must_db()
    module_dir = module_db.parent.parent
    candidates = [
        module_dir.parent / "customer-db" / "data" / "customer-db.db",
        module_dir.parent / "module_customer_db" / "data" / "customer-db.db",
        module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


async def _messenger_for_utm_term(utm_term: Any) -> str:
    """Return a platform only when Customer DB has one unambiguous exact match."""
    platform_id = _clean(utm_term, 500)
    if not platform_id:
        return ""
    path = _customer_db_path()
    if not path.exists():
        _log("warning", "customer DB is unavailable for messenger lookup: %s", path)
        return ""
    try:
        async with aiosqlite.connect(path) as db:
            matches: list[str] = []
            for platform, table in (("telegram", "cdb_telegram_clients"), ("vk", "cdb_vk_clients")):
                cur = await db.execute(
                    f"SELECT 1 FROM {table} WHERE platform_id=? LIMIT 1",
                    (platform_id,),
                )
                if await cur.fetchone():
                    matches.append(platform)
    except Exception as exc:
        _log("warning", "customer DB messenger lookup failed: %s", exc)
        return ""
    return matches[0] if len(matches) == 1 else ""


async def _with_messenger_fields(attendance: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(attendance)
    utm_term = _clean(_source_value(attendance, "utm_term"), 500)
    messenger = await _messenger_for_utm_term(utm_term)
    enriched["messenger_type"] = messenger
    enriched["dialog_salebot_url"] = ""
    enriched["dialog_vk_url"] = ""
    encoded = quote(utm_term, safe="")
    if messenger == "telegram":
        enriched["dialog_salebot_url"] = f"https://salebot.pro/projects/397724/clients/{encoded}"
    elif messenger == "vk":
        enriched["dialog_vk_url"] = f"https://vk.com/gim225075265/convo/{encoded}"
    return enriched


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _json(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return fallback
    return parsed if isinstance(parsed, type(fallback)) else fallback


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "да"}


def _int(value: Any) -> int | None:
    try:
        return int(str(value or "").strip())
    except Exception:
        return None


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL DEFAULT '',
                match_type TEXT NOT NULL DEFAULT 'room',
                match_value TEXT NOT NULL DEFAULT '',
                priority INTEGER NOT NULL DEFAULT 100,
                threshold_minutes REAL NOT NULL DEFAULT 60,
                min_minutes REAL NOT NULL DEFAULT 0,
                max_minutes REAL,
                pipeline_id TEXT NOT NULL DEFAULT '',
                status_id TEXT NOT NULL DEFAULT '',
                click_status_id TEXT NOT NULL DEFAULT '',
                pipeline_scope_json TEXT NOT NULL DEFAULT '[]',
                status_scope_json TEXT NOT NULL DEFAULT '[]',
                duplicate_action TEXT NOT NULL DEFAULT 'merge_empty',
                duplicate_rules_json TEXT NOT NULL DEFAULT '[]',
                exclude_conditions_json TEXT NOT NULL DEFAULT '[]',
                note_only_status_ids_json TEXT NOT NULL DEFAULT '[]',
                responsible_user_ids_json TEXT NOT NULL DEFAULT '[]',
                tags_json TEXT NOT NULL DEFAULT '[]',
                field_mappings_json TEXT NOT NULL DEFAULT '[]',
                lead_name_template TEXT NOT NULL DEFAULT '',
                note_template TEXT NOT NULL DEFAULT '',
                cursor INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_id INTEGER NOT NULL DEFAULT 0,
                attendance_key TEXT NOT NULL DEFAULT '',
                source_hash TEXT NOT NULL DEFAULT '',
                binding_id INTEGER,
                status TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '',
                lead_id TEXT NOT NULL DEFAULT '',
                responsible_user_id TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                details_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bizon_amo_change ON events(change_id);
            CREATE INDEX IF NOT EXISTS idx_bizon_amo_status ON events(status,id);
            CREATE TABLE IF NOT EXISTS round_robin_cursors (
                pool_key TEXT PRIMARY KEY,
                cursor INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
        cur = await db.execute("PRAGMA table_info(bindings)")
        columns = {str(row[1]) for row in await cur.fetchall()}
        if "min_minutes" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN min_minutes REAL")
            await db.execute("UPDATE bindings SET min_minutes=threshold_minutes WHERE min_minutes IS NULL")
        if "max_minutes" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN max_minutes REAL")
        if "click_status_id" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN click_status_id TEXT NOT NULL DEFAULT ''")
        if "note_only_status_ids_json" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN note_only_status_ids_json TEXT NOT NULL DEFAULT '[]'")
        if "exclude_conditions_json" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN exclude_conditions_json TEXT NOT NULL DEFAULT '[]'")
        if "lead_name_template" not in columns:
            await db.execute("ALTER TABLE bindings ADD COLUMN lead_name_template TEXT NOT NULL DEFAULT ''")
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        await db.commit()


async def _require_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _settings() -> dict[str, str]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        data = {str(row[0]): str(row[1]) for row in await cur.fetchall()}
    return {**DEFAULT_SETTINGS, **data}


async def _set_settings(values: dict[str, Any]) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        for key, value in values.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
        await db.commit()


def _env_status() -> dict[str, Any]:
    return {
        "amo_base_url": bool(os.environ.get("AMO_BASE_URL", "").strip()),
        "amo_access_token": bool(os.environ.get("AMO_ACCESS_TOKEN", "").strip()),
        "feed_token_env": bool(os.environ.get("NEXUS_BIZON_FEED_TOKEN", "").strip()),
    }


async def _amo_request(method: str, path: str, payload: Any = None) -> tuple[Any, str, int]:
    base = os.environ.get("AMO_BASE_URL", "").strip().rstrip("/")
    token = os.environ.get("AMO_ACCESS_TOKEN", "").strip()
    if not base or not token:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы", 0
    settings = await _settings()
    timeout = max(5, min(60, int(settings.get("request_timeout") or 20)))
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, base + path, headers=headers, json=payload)
        if response.status_code >= 400:
            return None, f"amoCRM HTTP {response.status_code}: {response.text[:1000]}", response.status_code
        return (response.json() if response.text else {}), "", response.status_code
    except Exception as exc:
        return None, str(exc), 0


async def _catalog(entity: str) -> tuple[list[dict[str, Any]], str]:
    if entity in _field_cache:
        return _field_cache[entity], ""
    body, error, _ = await _amo_request("GET", f"/api/v4/{entity}/custom_fields?limit=250")
    rows = (((body or {}).get("_embedded") or {}).get("custom_fields") or []) if not error else []
    result = [row for row in rows if isinstance(row, dict)]
    if not error:
        _field_cache[entity] = result
    return result, error


async def _pipeline_catalog(force: bool = False) -> tuple[list[dict[str, Any]], str]:
    global _pipeline_cache
    if _pipeline_cache and not force:
        return _pipeline_cache, ""
    body, error, _ = await _amo_request("GET", "/api/v4/leads/pipelines")
    rows = (((body or {}).get("_embedded") or {}).get("pipelines") or []) if not error else []
    result = [row for row in rows if isinstance(row, dict)]
    if not error:
        _pipeline_cache = result
    return result, error


def _lead_unsorted_from_pipelines(
    lead: dict[str, Any], pipelines: list[dict[str, Any]]
) -> bool | None:
    pair = (_clean(lead.get("pipeline_id"), 64), _clean(lead.get("status_id"), 64))
    for pipeline in pipelines:
        pipeline_id = _clean(pipeline.get("id"), 64)
        statuses = (((pipeline.get("_embedded") or {}).get("statuses")) or [])
        for status in statuses:
            if pair == (pipeline_id, _clean(status.get("id"), 64)):
                return _int(status.get("type")) == 1
    return None


async def _lead_unsorted_state(lead: dict[str, Any]) -> tuple[bool | None, str]:
    pipelines, error = await _pipeline_catalog()
    if error:
        return None, error
    state = _lead_unsorted_from_pipelines(lead, pipelines)
    if state is not None:
        return state, ""
    pipelines, error = await _pipeline_catalog(force=True)
    if error:
        return None, error
    state = _lead_unsorted_from_pipelines(lead, pipelines)
    if state is None:
        return None, "Текущий статус сделки не найден в каталоге amoCRM"
    return state, ""


def _binding_dict(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for source, target in (
        ("pipeline_scope_json", "pipeline_scope"),
        ("status_scope_json", "status_scope"),
        ("duplicate_rules_json", "duplicate_rules"),
        ("exclude_conditions_json", "exclude_conditions"),
        ("note_only_status_ids_json", "note_only_status_ids"),
        ("responsible_user_ids_json", "responsible_user_ids"),
        ("tags_json", "tags"),
        ("field_mappings_json", "field_mappings"),
    ):
        result[target] = _json(result.get(source), [])
    result["active"] = bool(result.get("active"))
    if result.get("min_minutes") is None:
        result["min_minutes"] = float(result.get("threshold_minutes") or 0)
    result["lead_name_template"] = _clean(result.get("lead_name_template"), 1000) or DEFAULT_LEAD_NAME_TEMPLATE
    return result


async def _bindings() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM bindings ORDER BY priority ASC,id ASC")
        return [_binding_dict(dict(row)) for row in await cur.fetchall()]


def _binding_matches(binding: dict[str, Any], attendance: dict[str, Any]) -> bool:
    value = _clean(binding.get("match_value"), 1000)
    match_type = _clean(binding.get("match_type"), 50)
    webinar = _clean(attendance.get("webinarId"), 1000)
    room = _clean(attendance.get("roomid") or attendance.get("room_id"), 1000)
    title = _clean(attendance.get("room_title") or attendance.get("name"), 1000)
    if match_type == "all":
        return True
    if match_type == "webinar":
        return webinar == value
    if match_type == "room":
        return room == value or webinar.split("*", 1)[0] == value
    if match_type == "contains":
        return bool(value and value.casefold() in f"{room} {title} {webinar}".casefold())
    if match_type == "regex":
        try:
            return bool(value and re.search(value, f"{room} {title} {webinar}", re.IGNORECASE))
        except re.error:
            return False
    return False


async def _binding_for(attendance: dict[str, Any]) -> dict[str, Any] | None:
    for row in await _bindings():
        if row["active"] and _binding_matches(row, attendance) and _binding_time_matches(row, attendance):
            return row
    return None


def _binding_time_matches(binding: dict[str, Any], attendance: dict[str, Any]) -> bool:
    minutes = attendance.get("watch_minutes")
    if not attendance.get("watch_valid") or not isinstance(minutes, (int, float)):
        return True
    minimum = float(binding.get("min_minutes") if binding.get("min_minutes") is not None else binding.get("threshold_minutes") or 0)
    maximum = binding.get("max_minutes")
    above_minimum = float(minutes) >= minimum
    if _has_webinar_click(attendance) and _int(binding.get("click_status_id")):
        above_minimum = True
    return above_minimum and (maximum in (None, "") or float(minutes) < float(maximum))


def _qualification_reason(
    attendance: dict[str, Any],
    minimum: float,
    maximum: float | None = None,
    click_status_id: Any = None,
) -> str:
    minutes = attendance.get("watch_minutes")
    if not attendance.get("watch_valid") or not isinstance(minutes, (int, float)):
        return "invalid_duration"
    if float(minutes) < float(minimum):
        if _has_webinar_click(attendance) and _int(click_status_id):
            return "eligible"
        return "below_minimum"
    if maximum is not None and float(minutes) >= float(maximum):
        return "at_or_above_maximum"
    if not _clean(attendance.get("phone")) and not _clean(attendance.get("email")):
        return "missing_contact"
    return "eligible"


def _source_value(attendance: dict[str, Any], source: str) -> Any:
    aliases = {
        "name": "username",
        "minutes": "watch_minutes",
        "webinar_id": "webinarId",
        "room_id": "roomid",
    }
    key = aliases.get(source, source)
    profiles = attendance.get("profiles") if isinstance(attendance.get("profiles"), list) else []

    def first_value(*keys: str) -> Any:
        for container in (attendance, *[item for item in profiles if isinstance(item, dict)]):
            for candidate in keys:
                value = container.get(candidate)
                if value not in (None, ""):
                    return value
        return ""

    if key == "source_user_id":
        value = first_value("chatUserId", "uid", "sid", "bizon_user_id")
        if value not in (None, ""):
            return value
        person_key = _clean(attendance.get("person_key"), 1000)
        return person_key.split(":", 1)[1] if ":" in person_key else person_key
    if key in {"_ym_uid", "ym_uid"}:
        return first_value("_ym_uid", "ym_uid", "param1", "p1")
    if key in {"webinar_date", "webinar_time", "watch_minutes_round"}:
        return _template_values(attendance).get(key, "")
    return attendance.get(key, "")


def _condition_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return str(value).strip()


def _exclude_condition_matches(attendance: dict[str, Any], condition: dict[str, Any]) -> bool:
    source = _clean(condition.get("source"), 100)
    operator = _clean(condition.get("operator"), 30)
    if not source or operator not in EXCLUDE_OPERATORS:
        return False
    actual = _condition_text(_source_value(attendance, source)).casefold()
    expected = _condition_text(condition.get("value")).casefold()
    if operator == "is_empty":
        return not actual
    if operator == "is_not_empty":
        return bool(actual)
    if operator == "equals":
        return actual == expected
    if operator == "not_equals":
        return actual != expected
    if operator == "contains":
        return expected in actual
    if operator == "not_contains":
        return expected not in actual
    if operator == "starts_with":
        return actual.startswith(expected)
    if operator == "ends_with":
        return actual.endswith(expected)
    return False


def _matching_exclude_conditions(binding: dict[str, Any], attendance: dict[str, Any]) -> list[dict[str, str]]:
    conditions = binding.get("exclude_conditions") or []
    if not conditions or not all(
        isinstance(condition, dict) and _exclude_condition_matches(attendance, condition)
        for condition in conditions
    ):
        return []
    return [
        {
            "source": _clean(condition.get("source"), 100),
            "operator": _clean(condition.get("operator"), 30),
            "value": _clean(condition.get("value"), 1000),
        }
        for condition in conditions
    ]


def _coerce_amo_field_value(value: Any, field_type: str) -> Any:
    kind = _clean(field_type, 50).casefold()
    if kind == "checkbox":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        return _clean(value, 100).casefold() in {"1", "true", "yes", "on", "да"}
    if kind == "numeric":
        try:
            number = float(value)
        except (TypeError, ValueError):
            return value
        return int(number) if number.is_integer() else number
    if kind in {"date", "date_time"}:
        try:
            number = float(value)
        except (TypeError, ValueError):
            number = 0
        if number > 0:
            return int(number / 1000 if number > 10_000_000_000 else number)
        text = _clean(value, 100)
        if not text:
            return ""
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("Europe/Moscow"))
            return int(parsed.timestamp())
        except (ValueError, TypeError):
            return value
    return value


def _entity_values(entity: dict[str, Any], rule: dict[str, Any]) -> list[str]:
    field_id = _clean(rule.get("field_id"), 50)
    field_code = _clean(rule.get("field_code"), 100).upper()
    field_name = _clean(rule.get("field"), 300).casefold()
    values: list[str] = []
    for field in entity.get("custom_fields_values") or []:
        if field_id and _clean(field.get("field_id"), 50) != field_id:
            continue
        if field_code and _clean(field.get("field_code"), 100).upper() != field_code:
            continue
        if field_name and _clean(field.get("field_name"), 300).casefold() != field_name:
            continue
        values.extend(_clean(item.get("value"), 1000) for item in field.get("values") or [])
    if field_name in {"name", "название"}:
        values.append(_clean(entity.get("name"), 1000))
    return [value for value in values if value]


def _same(left: Any, right: Any) -> bool:
    a, b = _clean(left, 1000).casefold(), _clean(right, 1000).casefold()
    ad, bd = re.sub(r"\D", "", a), re.sub(r"\D", "", b)
    if len(ad) == 11 and ad.startswith("8"):
        ad = "7" + ad[1:]
    if len(bd) == 11 and bd.startswith("8"):
        bd = "7" + bd[1:]
    if len(ad) == 10:
        ad = "7" + ad
    if len(bd) == 10:
        bd = "7" + bd
    return bool(a and b and (a == b or (len(ad) >= 7 and ad == bd)))


def _phone_identity(value: Any) -> str:
    digits = re.sub(r"\D", "", _clean(value, 1000))
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return digits if len(digits) >= 7 else ""


def _phone_text(value: Any) -> str:
    """Return an amoCRM-friendly phone while keeping identity matching digit-based."""
    digits = _phone_identity(value)
    return "+" + digits if digits else ""


def _email_identity(value: Any) -> str:
    return _clean(value, 1000).casefold()


async def _customer_db_deal_ids_for_attendance(attendance: dict[str, Any]) -> list[int]:
    """Find deals across duplicate amoCRM contacts using Nexus' normalized amo snapshot."""
    phone = _phone_identity(attendance.get("phone"))
    email = _email_identity(attendance.get("email"))
    if not phone and not email:
        return []
    path = _customer_db_path()
    if not path.exists():
        return []
    candidates: dict[int, dict[str, Any]] = {}
    try:
        async with aiosqlite.connect(path) as db:
            for needle in [item for item in (phone, email) if item]:
                cur = await db.execute(
                    "SELECT platform_id,custom_fields FROM cdb_amo_deals "
                    "WHERE custom_fields LIKE ? ORDER BY id DESC LIMIT 100",
                    (f'%"{needle}"%',),
                )
                for platform_id, raw_fields in await cur.fetchall():
                    fields = _json(raw_fields, {})
                    phones = {_phone_identity(item) for item in fields.get("phones") or []}
                    emails = {_email_identity(item) for item in fields.get("emails") or []}
                    if (phone and phone in phones) or (email and email in emails):
                        lead_id = _int(fields.get("deal_id") or platform_id)
                        if lead_id:
                            candidates[lead_id] = fields
    except Exception as exc:
        _log("warning", "customer DB amo duplicate lookup failed: %s", exc)
        return []
    return list(candidates)


def _sort_existing_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer an active deal, then the most recently updated deal."""
    return sorted(
        leads,
        key=lambda lead: (
            1 if str(lead.get("status_id") or "") in {"142", "143"} else 0,
            -int(lead.get("updated_at") or 0),
            -int(lead.get("id") or 0),
        ),
    )


async def _lead_ids_for_contact(contact: dict[str, Any]) -> list[int]:
    ids = [_int(item.get("id")) for item in ((contact.get("_embedded") or {}).get("leads") or [])]
    ids = [item for item in ids if item]
    if ids:
        return ids
    contact_id = _int(contact.get("id"))
    if not contact_id:
        return []
    body, error, _ = await _amo_request("GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads")
    if error:
        return []
    return [int(item["to_entity_id"]) for item in (((body or {}).get("_embedded") or {}).get("links") or []) if _int(item.get("to_entity_id"))]


def _lead_allowed(lead: dict[str, Any], binding: dict[str, Any]) -> bool:
    pipeline_scope = [str(value) for value in binding.get("pipeline_scope") or []]
    status_scope = [str(value) for value in binding.get("status_scope") or []]
    if pipeline_scope and str(lead.get("pipeline_id")) not in pipeline_scope:
        return False
    status = str(lead.get("status_id") or "")
    if status_scope and status not in status_scope:
        return False
    return True


def _duplicate_plan(existing: dict[str, Any] | None, binding: dict[str, Any]) -> str:
    if not existing:
        return "create"
    action = _clean(binding.get("duplicate_action"), 30) or "merge_empty"
    note_only_statuses = {str(value) for value in binding.get("note_only_status_ids") or []}
    if str(existing.get("status_id") or "") in note_only_statuses:
        return "note_only"
    if action in {"update", "merge_empty"}:
        return "merge_empty"
    if action == "note_only":
        return "note_only"
    return "create"


async def _find_existing(attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    candidates: dict[int, dict[str, Any]] = {}
    rules = binding.get("duplicate_rules") or DEFAULT_DUPLICATE_RULES
    for rule in rules:
        entity_name = "contacts" if _clean(rule.get("entity"), 20) == "contacts" else "leads"
        query = _clean(_source_value(attendance, _clean(rule.get("source"), 100)), 1000)
        if not query:
            continue
        body, error, _ = await _amo_request("GET", f"/api/v4/{entity_name}?query={quote(query)}&with=leads")
        if error:
            return None, error
        for entity in (((body or {}).get("_embedded") or {}).get(entity_name) or []):
            if not any(_same(value, query) for value in _entity_values(entity, rule)):
                continue
            lead_ids = await _lead_ids_for_contact(entity) if entity_name == "contacts" else [_int(entity.get("id"))]
            for lead_id in [item for item in lead_ids if item]:
                lead_body, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}")
                if lead_error:
                    continue
                if isinstance(lead_body, dict) and _lead_allowed(lead_body, binding):
                    candidates[int(lead_id)] = lead_body
        if candidates:
            break
    if not candidates:
        return None, ""
    pipeline_order = {str(value): index for index, value in enumerate(binding.get("pipeline_scope") or [])}
    rows = sorted(
        candidates.values(),
        key=lambda lead: (
            1 if str(lead.get("status_id") or "") in {"142", "143"} else 0,
            pipeline_order.get(str(lead.get("pipeline_id")), 9999),
            -int(lead.get("updated_at") or 0),
            -int(lead.get("id") or 0),
        ),
    )
    return rows[0], ""


async def _find_all_contact_leads(attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """Find deals across exact amo contacts and Nexus' cross-contact identity snapshot."""
    candidates: dict[int, dict[str, Any]] = {}
    rules = binding.get("duplicate_rules") or DEFAULT_DUPLICATE_RULES
    for rule in rules:
        if _clean(rule.get("entity"), 20) != "contacts":
            continue
        query = _clean(_source_value(attendance, _clean(rule.get("source"), 100)), 1000)
        if not query:
            continue
        body, error, _ = await _amo_request("GET", f"/api/v4/contacts?query={quote(query)}&with=leads&limit=50")
        if error:
            return [], error
        for contact in (((body or {}).get("_embedded") or {}).get("contacts") or []):
            if not any(_same(value, query) for value in _entity_values(contact, rule)):
                continue
            for lead_id in await _lead_ids_for_contact(contact):
                lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}")
                if lead_error:
                    return [], lead_error
                if isinstance(lead, dict) and _int(lead.get("id")):
                    candidates[int(lead["id"])] = lead
    for lead_id in await _customer_db_deal_ids_for_attendance(attendance):
        if lead_id in candidates:
            continue
        lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}")
        if lead_error:
            continue
        if isinstance(lead, dict) and _int(lead.get("id")):
            candidates[int(lead["id"])] = lead
    return _sort_existing_leads(list(candidates.values())), ""


def _scalar_for_amo(value: Any, field_type: str) -> Any:
    value = _coerce_amo_field_value(value, field_type)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return value


async def _mapped_field_values(
    attendance: dict[str, Any],
    binding: dict[str, Any],
    entity: str,
    catalog: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    catalog = catalog if catalog is not None else (await _catalog(entity))[0]
    fields = {int(item["id"]): item for item in catalog if _int(item.get("id"))}
    mapped: dict[int, dict[str, Any]] = {}
    for mapping in binding.get("field_mappings") or []:
        mapping_entity = _clean(mapping.get("entity") or "leads", 20)
        if mapping_entity not in {"leads", "contacts"}:
            mapping_entity = "leads"
        if mapping_entity != entity or _clean(mapping.get("target"), 30) == "name":
            continue
        field_id = _int(mapping.get("field_id"))
        field = fields.get(field_id or 0)
        value = _source_value(attendance, _clean(mapping.get("source"), 100))
        if not field or value in (None, ""):
            continue
        item: dict[str, Any] = {
            "value": _scalar_for_amo(value, _clean(field.get("type"), 50)),
        }
        field_code = _clean(field.get("code"), 50).upper()
        if field_code == "PHONE":
            item["value"] = _phone_text(value)
        if field_code in {"PHONE", "EMAIL"}:
            item["enum_code"] = "WORK"
        mapped[int(field["id"])] = {"field_id": int(field["id"]), "values": [item]}
    return list(mapped.values())


def _mapped_entity_name(attendance: dict[str, Any], binding: dict[str, Any], entity: str) -> str:
    for mapping in binding.get("field_mappings") or []:
        mapping_entity = _clean(mapping.get("entity") or "leads", 20)
        if mapping_entity == entity and _clean(mapping.get("target"), 30) == "name":
            value = _source_value(attendance, _clean(mapping.get("source"), 100))
            if value not in (None, ""):
                return _clean(_scalar_for_amo(value, "text"), 500)
    return ""


def _ensure_contact_identity_fields(
    attendance: dict[str, Any],
    fields: list[dict[str, Any]],
    mapped: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    result = {int(item["field_id"]): item for item in mapped if _int(item.get("field_id"))}
    for code, source in (("PHONE", "phone"), ("EMAIL", "email")):
        field = next((item for item in fields if _clean(item.get("code"), 50).upper() == code), None)
        value = _clean(attendance.get(source), 500)
        if field and value and int(field["id"]) not in result:
            if code == "PHONE":
                value = _phone_text(value)
            result[int(field["id"])] = {
                "field_id": int(field["id"]),
                "values": [{"value": value, "enum_code": "WORK"}],
            }
    return list(result.values())


def _template_values(attendance: dict[str, Any]) -> dict[str, str]:
    values = {key: _clean(value, 1000) for key, value in attendance.items() if not isinstance(value, (dict, list))}
    values.setdefault("name", values.get("username", ""))
    values["room_slug"] = _room_slug(attendance)
    minutes = attendance.get("watch_minutes")
    if isinstance(minutes, (int, float)):
        values["watch_minutes_round"] = str(int(round(float(minutes))))
    values["source_user_id"] = _clean(_source_value(attendance, "source_user_id"), 1000)
    values["_ym_uid"] = _clean(_source_value(attendance, "_ym_uid"), 1000)
    values["clicked_button_text"] = "да" if bool(attendance.get("clicked_button")) else "нет"
    values["clicked_banner_text"] = "да" if bool(attendance.get("clicked_banner")) else "нет"
    values["finished_text"] = "да" if bool(attendance.get("finished")) else "нет"

    def moscow_datetime(value: Any) -> str:
        try:
            if isinstance(value, (int, float)) or str(value or "").strip().isdigit():
                number = float(value)
                parsed_value = datetime.fromtimestamp(
                    number / 1000 if number > 10_000_000_000 else number,
                    timezone.utc,
                ).astimezone(ZoneInfo("Europe/Moscow"))
            else:
                parsed_value = datetime.fromisoformat(_clean(value, 100).replace("Z", "+00:00"))
                if parsed_value.tzinfo is None:
                    parsed_value = parsed_value.replace(tzinfo=ZoneInfo("Europe/Moscow"))
                parsed_value = parsed_value.astimezone(ZoneInfo("Europe/Moscow"))
            return parsed_value.strftime("%d.%m.%Y %H:%M")
        except (ValueError, TypeError, OSError):
            return ""

    values["view_from_text"] = moscow_datetime(_source_value(attendance, "view"))
    values["view_till_text"] = moscow_datetime(_source_value(attendance, "viewTill"))
    stamp = attendance.get("webinar_at")
    parsed: datetime | None = None
    try:
        if isinstance(stamp, (int, float)) or str(stamp or "").strip().isdigit():
            number = float(stamp)
            parsed = datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number, timezone.utc).astimezone(ZoneInfo("Europe/Moscow"))
        elif _clean(stamp, 100):
            parsed = datetime.fromisoformat(_clean(stamp, 100).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("Europe/Moscow"))
            parsed = parsed.astimezone(ZoneInfo("Europe/Moscow"))
    except (ValueError, TypeError, OSError):
        parsed = None
    if parsed:
        values["webinar_date"] = parsed.strftime("%d.%m.%Y")
        values["webinar_time"] = parsed.strftime("%H:%M")
    return values


def _room_slug(attendance: dict[str, Any]) -> str:
    """Return the Bizon room code without the account/group prefix."""
    candidates = (
        attendance.get("roomid"),
        attendance.get("room_id"),
        str(attendance.get("webinarId") or "").split("*", 1)[0],
    )
    for candidate in candidates:
        room_id = _clean(candidate, 1000)
        if ":" not in room_id:
            continue
        slug = _clean(room_id.rsplit(":", 1)[1], 200)
        if slug:
            return slug
    return ""


def _lead_tags(attendance: dict[str, Any], binding: dict[str, Any]) -> list[dict[str, str]]:
    """Resolve tag templates and discard empty, unresolved, or duplicate tags."""
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for template in binding.get("tags") or []:
        tag = _clean(_format(str(template or ""), attendance), 200)
        if not tag or re.search(r"\{[A-Za-z0-9_]+\}", tag):
            continue
        normalized = tag.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append({"name": tag})
    return result


def _format(template: str, attendance: dict[str, Any]) -> str:
    values = _template_values(attendance)
    text = template
    for key, value in values.items():
        text = text.replace("{" + key + "}", value)
    return _clean(text, 5000)


def _format_note(template: str, attendance: dict[str, Any]) -> str:
    template = str(template or "").replace("\\n", "\n")
    values = _template_values(attendance)
    lines: list[str] = []
    for line in template.splitlines():
        placeholders = re.findall(r"\{([A-Za-z0-9_]+)\}", line)
        resolved = {
            key: values[key] if key in values else _clean(_source_value(attendance, key), 1000)
            for key in placeholders
        }
        if placeholders and not any(value for value in resolved.values()):
            continue
        for key, value in resolved.items():
            line = line.replace("{" + key + "}", value or "—")
        lines.append(line.rstrip())
    return _clean("\n".join(lines).strip(), 5000)


def _lead_name(attendance: dict[str, Any], binding: dict[str, Any] | None = None) -> str:
    template = _clean((binding or {}).get("lead_name_template"), 1000) or DEFAULT_LEAD_NAME_TEMPLATE
    formatted = _format(template, attendance)
    if formatted and "{" not in formatted:
        return _clean(formatted, 500)
    name = _clean(attendance.get("username") or "Без имени", 300)
    room = _clean(attendance.get("room_title") or attendance.get("roomid") or attendance.get("webinarId"), 500)
    return _clean(f"ВЕБИНАР | {name} | {room}", 500)


def _select_responsible(users: list[str], cursor: int, active: set[str] | None = None) -> str:
    users = [str(value) for value in users if _int(value)]
    if active is not None:
        users = [user_id for user_id in users if user_id in active]
    if not users:
        return ""

    return users[int(cursor or 0) % len(users)]


def _active_amo_user_ids(users: list[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for item in users:
        rights = item.get("rights") if isinstance(item.get("rights"), dict) else {}
        is_active = rights.get("is_active", item.get("is_active", True))
        if is_active and _int(item.get("id")):
            result.add(str(item["id"]))
    return result


async def _responsible(binding: dict[str, Any]) -> str:
    users = [str(value) for value in binding.get("responsible_user_ids") or [] if _int(value)]
    if not users:
        return ""
    body, error, _ = await _amo_request("GET", "/api/v4/users?limit=250")
    active: set[str] | None = None
    if not error:
        active = _active_amo_user_ids(
            (((body or {}).get("_embedded") or {}).get("users") or [])
        )
    cursor = await _round_robin_cursor(binding)
    return _select_responsible(users, cursor, active)


def _round_robin_pool_key(binding: dict[str, Any]) -> str:
    return json.dumps([str(value) for value in binding.get("responsible_user_ids") or [] if _int(value)], separators=(",", ":"))


async def _round_robin_cursor(binding: dict[str, Any]) -> int:
    pool_key = _round_robin_pool_key(binding)
    if not pool_key or pool_key == "[]":
        return 0
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT cursor FROM round_robin_cursors WHERE pool_key=?", (pool_key,))
        row = await cur.fetchone()
        if row:
            return int(row[0] or 0)
        responsible_json = json.dumps(binding.get("responsible_user_ids") or [], ensure_ascii=False)
        cur = await db.execute("SELECT COALESCE(MAX(cursor),0) FROM bindings WHERE responsible_user_ids_json=?", (responsible_json,))
        initial = int((await cur.fetchone())[0] or 0)
        await db.execute("INSERT OR IGNORE INTO round_robin_cursors(pool_key,cursor,updated_at) VALUES(?,?,?)", (pool_key, initial, _now()))
        await db.commit()
        return initial


async def _advance_cursor(binding: dict[str, Any]) -> None:
    pool_key = _round_robin_pool_key(binding)
    responsible_json = json.dumps(binding.get("responsible_user_ids") or [], ensure_ascii=False)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "INSERT INTO round_robin_cursors(pool_key,cursor,updated_at) VALUES(?,1,?) ON CONFLICT(pool_key) DO UPDATE SET cursor=cursor+1,updated_at=excluded.updated_at",
            (pool_key, _now()),
        )
        await db.execute("UPDATE bindings SET cursor=cursor+1,updated_at=? WHERE responsible_user_ids_json=?", (_now(), responsible_json))
        await db.commit()


def _has_webinar_click(attendance: dict[str, Any]) -> bool:
    return bool(attendance.get("clicked_button") or attendance.get("clicked_banner"))


def _lead_route(attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[int | None, int | None]:
    pipeline_id = _int(binding.get("pipeline_id"))
    status_id = _int(binding.get("status_id"))
    click_status_id = _int(binding.get("click_status_id"))
    if _has_webinar_click(attendance) and click_status_id:
        status_id = click_status_id
    return pipeline_id, status_id


async def _preserve_existing_lead_route(
    existing: dict[str, Any], attendance: dict[str, Any], binding: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    """Record click routing without ever moving an existing amoCRM deal."""
    return {
        "preserved": True,
        "reason": "existing lead keeps pipeline and status",
        "click_detected": _has_webinar_click(attendance),
        "configured_click_status_id": _clean(binding.get("click_status_id"), 50),
        "pipeline_id": existing.get("pipeline_id"),
        "status_id": existing.get("status_id"),
        "responsible_user_id": existing.get("responsible_user_id"),
    }, ""


async def _create_lead(attendance: dict[str, Any], binding: dict[str, Any], responsible: str) -> tuple[dict[str, Any], str]:
    contact_catalog, error = await _catalog("contacts")
    if error:
        return {}, error
    contact = {
        "name": _mapped_entity_name(attendance, binding, "contacts")
        or _clean(attendance.get("username") or attendance.get("email") or attendance.get("phone"), 500)
    }
    contact_custom = _ensure_contact_identity_fields(
        attendance,
        contact_catalog,
        await _mapped_field_values(attendance, binding, "contacts", contact_catalog),
    )
    if contact_custom:
        contact["custom_fields_values"] = contact_custom
    pipeline_id, status_id = _lead_route(attendance, binding)
    lead = {
        "name": _mapped_entity_name(attendance, binding, "leads") or _lead_name(attendance, binding),
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "responsible_user_id": _int(responsible),
        "custom_fields_values": await _mapped_field_values(attendance, binding, "leads"),
        "_embedded": {"contacts": [contact], "tags": _lead_tags(attendance, binding)},
    }
    lead = {key: value for key, value in lead.items() if value not in (None, "", [])}
    body, error, _ = await _amo_request("POST", "/api/v4/leads/complex", [lead])
    item = body[0] if isinstance(body, list) and body else body
    return {"lead_id": _clean((item or {}).get("id"), 50), "request": lead, "response": body}, error


async def _update_lead(lead_id: str, attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[dict[str, Any], str]:
    payload = {
        "name": _mapped_entity_name(attendance, binding, "leads") or _lead_name(attendance, binding),
        "custom_fields_values": await _mapped_field_values(attendance, binding, "leads"),
        "_embedded": {"tags": _lead_tags(attendance, binding)},
    }
    payload = {key: value for key, value in payload.items() if value not in (None, "", [])}
    body, error, _ = await _amo_request("PATCH", f"/api/v4/leads/{lead_id}", payload)
    result: dict[str, Any] = {"lead_id": lead_id, "request": payload, "response": body}
    if error:
        return result, error

    contact_catalog, catalog_error = await _catalog("contacts")
    if catalog_error:
        return result, catalog_error
    contact_values = await _mapped_field_values(attendance, binding, "contacts", contact_catalog)
    contact_name = _mapped_entity_name(attendance, binding, "contacts")
    if not contact_values and not contact_name:
        return result, ""

    lead, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}?with=contacts")
    if lead_error:
        return result, lead_error
    contacts = (((lead or {}).get("_embedded") or {}).get("contacts") or [])
    contact = next((item for item in contacts if item.get("is_main")), contacts[0] if contacts else None)
    contact_id = _int((contact or {}).get("id"))
    if not contact_id:
        return result, "У активной сделки не найден связанный контакт для маппинга"

    contact_payload: dict[str, Any] = {}
    if contact_name:
        contact_payload["name"] = contact_name
    if contact_values:
        contact_payload["custom_fields_values"] = contact_values
    contact_body, contact_error, _ = await _amo_request("PATCH", f"/api/v4/contacts/{contact_id}", contact_payload)
    result["contact"] = {"contact_id": contact_id, "request": contact_payload, "response": contact_body}
    return result, contact_error


def _field_has_value(field: dict[str, Any]) -> bool:
    for item in field.get("values") or []:
        value = item.get("value")
        if value not in (None, "", [], {}):
            return True
    return False


def _only_empty_custom_fields(existing: dict[str, Any], proposed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    occupied = {
        int(field["field_id"])
        for field in existing.get("custom_fields_values") or []
        if _int(field.get("field_id")) and _field_has_value(field)
    }
    return [field for field in proposed if _int(field.get("field_id")) not in occupied]


async def _merge_empty_lead(existing: dict[str, Any], attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Fill only blank lead/contact fields. Ownership, pipeline, status and names are preserved."""
    lead_id = _clean(existing.get("id"), 50)
    result: dict[str, Any] = {
        "lead_id": lead_id,
        "preserved": {
            "responsible_user_id": existing.get("responsible_user_id"),
            "pipeline_id": existing.get("pipeline_id"),
            "status_id": existing.get("status_id"),
            "name": existing.get("name"),
        },
    }
    lead_values = _only_empty_custom_fields(
        existing,
        await _mapped_field_values(attendance, binding, "leads"),
    )
    if lead_values:
        lead_payload = {"custom_fields_values": lead_values}
        lead_body, lead_error, _ = await _amo_request("PATCH", f"/api/v4/leads/{lead_id}", lead_payload)
        result["lead"] = {"request": lead_payload, "response": lead_body}
        if lead_error:
            return result, lead_error

    lead_with_contacts, lead_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}?with=contacts")
    if lead_error:
        return result, lead_error
    contacts = (((lead_with_contacts or {}).get("_embedded") or {}).get("contacts") or [])
    contact_ref = next((item for item in contacts if item.get("is_main")), contacts[0] if contacts else None)
    contact_id = _int((contact_ref or {}).get("id"))
    if not contact_id:
        return result, ""

    contact, contact_error, _ = await _amo_request("GET", f"/api/v4/contacts/{contact_id}")
    if contact_error:
        return result, contact_error
    contact_catalog, catalog_error = await _catalog("contacts")
    if catalog_error:
        return result, catalog_error
    proposed_contact = _ensure_contact_identity_fields(
        attendance,
        contact_catalog,
        await _mapped_field_values(attendance, binding, "contacts", contact_catalog),
    )
    contact_values = _only_empty_custom_fields(contact or {}, proposed_contact)
    contact_payload: dict[str, Any] = {}
    if contact_values:
        contact_payload["custom_fields_values"] = contact_values
    contact_name = _mapped_entity_name(attendance, binding, "contacts") or _clean(attendance.get("username"), 500)
    if not _clean((contact or {}).get("name"), 500) and contact_name:
        contact_payload["name"] = contact_name
    if contact_payload:
        contact_body, contact_patch_error, _ = await _amo_request("PATCH", f"/api/v4/contacts/{contact_id}", contact_payload)
        result["contact"] = {"contact_id": contact_id, "request": contact_payload, "response": contact_body}
        if contact_patch_error:
            return result, contact_patch_error
    return result, ""


def _note_texts(attendance: dict[str, Any], binding: dict[str, Any]) -> list[str]:
    template = binding.get("note_template") or (
        "Посещение Bizon365\nВебинар: {webinarId}\nКомната: {roomid}\n"
        "Время: {watch_minutes} мин.\nТелефон: {phone}\nEmail: {email}\n"
        "Сообщения и ответы в чате: {chat_messages_text}"
    )
    main_template = "\n".join(
        line for line in str(template).replace("\\n", "\n").splitlines()
        if "{chat_messages_text}" not in line and "{chat_messages}" not in line
    )
    texts = [_format_note(main_template, attendance)]
    messages = attendance.get("chat_messages")
    rendered: list[str] = []
    if isinstance(messages, list):
        for item in messages[:100]:
            if not isinstance(item, dict):
                continue
            message = _clean(item.get("text"), 3000)
            if not message:
                continue
            timestamp = _clean(item.get("time"), 50)
            rendered.append(f"[{timestamp}] {message}" if timestamp else message)
    if not rendered:
        rendered = [
            _clean(line, 3000)
            for line in str(attendance.get("chat_messages_text") or "").splitlines()[:100]
            if _clean(line, 3000)
        ]
    if rendered:
        texts.append("Bizon365 · комментарии с вебинара:\n" + "\n".join(rendered))
    return list(dict.fromkeys(text for text in texts if text))


async def _add_note(lead_id: str, attendance: dict[str, Any], binding: dict[str, Any]) -> tuple[Any, str]:
    texts = _note_texts(attendance, binding)
    existing, existing_error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}/notes?limit=250")
    existing_texts: set[str] = set()
    if not existing_error:
        for note in (((existing or {}).get("_embedded") or {}).get("notes") or []):
            if note.get("note_type") == "common":
                existing_texts.add(_clean((note.get("params") or {}).get("text"), 10000))
    missing = [text for text in texts if text not in existing_texts]
    if not missing:
        return {"skipped": True, "reason": "all notes already exist"}, ""
    payload = [{"note_type": "common", "params": {"text": text}} for text in missing]
    return (await _amo_request("POST", f"/api/v4/leads/{lead_id}/notes", payload))[:2]


async def _insert_event(change: dict[str, Any]) -> int:
    attendance = change.get("attendance") if isinstance(change.get("attendance"), dict) else {}
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            """
            INSERT OR IGNORE INTO events(change_id,attendance_key,source_hash,status,payload_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                int(change.get("id") or 0), _clean(change.get("attendance_key"), 300),
                _clean(change.get("source_hash"), 100), "received",
                json.dumps(attendance, ensure_ascii=False, default=str), _now(), _now(),
            ),
        )
        await db.commit()
        if cur.lastrowid:
            return int(cur.lastrowid)
        return 0


async def _update_event(event_id: int, **values: Any) -> None:
    allowed = {"binding_id", "status", "action", "lead_id", "responsible_user_id", "error", "details_json", "attempts"}
    pairs = [(key, value) for key, value in values.items() if key in allowed]
    pairs.append(("updated_at", _now()))
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE events SET " + ",".join(f"{key}=?" for key, _ in pairs) + " WHERE id=?",
            [value for _, value in pairs] + [event_id],
        )
        await db.commit()


async def _process_event(event_id: int) -> dict[str, Any]:
    async with _process_lock:
        async with aiosqlite.connect(_must_db()) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
            row = await cur.fetchone()
        if not row:
            return {"ok": False, "error": "event not found"}
        event = dict(row)
        attendance = _json(event.get("payload_json"), {})
        binding = await _binding_for(attendance)
        attempts = int(event.get("attempts") or 0) + 1
        if not binding:
            await _update_event(event_id, status="pending_binding", action="hold", error="Связка вебинара не настроена", attempts=attempts)
            return {"ok": True, "status": "pending_binding"}
        excluded_by = _matching_exclude_conditions(binding, attendance)
        if excluded_by:
            await _update_event(
                event_id,
                binding_id=binding["id"],
                status="skipped",
                action="excluded",
                error="",
                attempts=attempts,
                details_json=json.dumps({"exclude_conditions": excluded_by}, ensure_ascii=False),
            )
            return {"ok": True, "status": "skipped", "reason": "excluded"}
        minimum = float(binding.get("min_minutes") if binding.get("min_minutes") is not None else binding.get("threshold_minutes") or 0)
        maximum = float(binding["max_minutes"]) if binding.get("max_minutes") not in (None, "") else None
        minutes = attendance.get("watch_minutes")
        qualification = _qualification_reason(
            attendance, minimum, maximum, binding.get("click_status_id")
        )
        if qualification == "invalid_duration":
            await _update_event(event_id, binding_id=binding["id"], status="skipped", action="invalid_duration", error=_clean(attendance.get("watch_error")), attempts=attempts)
            return {"ok": True, "status": "skipped", "reason": "invalid_duration"}
        if qualification in {"below_minimum", "at_or_above_maximum"}:
            await _update_event(event_id, binding_id=binding["id"], status="skipped", action=qualification, error="", attempts=attempts)
            return {"ok": True, "status": "skipped", "reason": qualification}
        if qualification == "missing_contact":
            await _update_event(event_id, binding_id=binding["id"], status="skipped", action="missing_contact", error="", attempts=attempts)
            return {"ok": True, "status": "skipped", "reason": "missing_contact"}
        attendance = await _with_messenger_fields(attendance)
        settings = await _settings()
        contact_leads: list[dict[str, Any]] = []
        waiting_lead_id = _clean(event.get("lead_id"), 50) if event.get("status") == "waiting_unsorted" else ""
        if waiting_lead_id:
            existing, search_error, _ = await _amo_request("GET", f"/api/v4/leads/{waiting_lead_id}")
            if search_error or not isinstance(existing, dict):
                error = search_error or "Ожидающая сделка не найдена"
                await _update_event(
                    event_id, binding_id=binding["id"], status="waiting_unsorted", action="hold",
                    lead_id=waiting_lead_id, error=error, attempts=attempts,
                )
                return {"ok": True, "status": "waiting_unsorted", "error": error}
            contact_leads = [existing]
        else:
            contact_leads, search_error = await _find_all_contact_leads(attendance, binding)
            existing = contact_leads[0] if contact_leads else None
            if not search_error and not existing:
                existing, search_error = await _find_existing(attendance, binding)
        if search_error and not _bool(settings.get("dry_run")):
            await _update_event(event_id, binding_id=binding["id"], status="failed", action="search", error=search_error, attempts=attempts)
            return {"ok": False, "error": search_error}
        # Webinar attendance is enrichment, not a new sales intent. Any exact
        # existing deal wins regardless of pipeline/status/contact duplication.
        # Only a person with no deal at all receives a newly created deal.
        planned = _duplicate_plan(existing, binding)
        responsible = _clean(existing.get("responsible_user_id"), 50) if planned in {"merge_empty", "note_only", "note_all_contact_deals"} else await _responsible(binding)
        if existing:
            is_unsorted, unsorted_error = await _lead_unsorted_state(existing)
            if is_unsorted is not False:
                lead_id = _clean(existing.get("id"), 50)
                details = {
                    "reason": "unsorted" if is_unsorted else "unsorted_check_failed",
                    "preserved": {
                        "responsible_user_id": existing.get("responsible_user_id"),
                        "pipeline_id": existing.get("pipeline_id"),
                        "status_id": existing.get("status_id"),
                        "name": existing.get("name"),
                    },
                }
                await _update_event(
                    event_id, binding_id=binding["id"], status="waiting_unsorted", action="hold",
                    lead_id=lead_id, responsible_user_id=responsible,
                    error=unsorted_error, attempts=attempts,
                    details_json=json.dumps(details, ensure_ascii=False, default=str),
                )
                return {"ok": True, "status": "waiting_unsorted", "lead_id": lead_id}
        if planned == "create" and binding.get("responsible_user_ids") and not responsible:
            error = "В связке нет активного ответственного amoCRM"
            await _update_event(event_id, binding_id=binding["id"], status="failed", action=planned, error=error, attempts=attempts)
            return {"ok": False, "error": error}
        if _bool(settings.get("dry_run")):
            await _update_event(
                event_id, binding_id=binding["id"], status="shadow", action=planned,
                lead_id=_clean((existing or {}).get("id")), responsible_user_id=responsible,
                error="", attempts=attempts,
                details_json=json.dumps({"min_minutes": minimum, "max_minutes": maximum, "minutes": minutes, "duplicate": bool(existing), "responsible_preserved": bool(existing), "note_lead_ids": [_clean(item.get("id"), 50) for item in contact_leads]}, ensure_ascii=False),
            )
            return {"ok": True, "status": "shadow", "action": planned}
        notes_already_added = False
        if planned == "merge_empty":
            result, error = await _merge_empty_lead(existing, attendance, binding)
        elif planned == "note_only":
            result, error = {"lead_id": _clean(existing.get("id")), "preserved": {"responsible_user_id": existing.get("responsible_user_id"), "pipeline_id": existing.get("pipeline_id"), "status_id": existing.get("status_id"), "name": existing.get("name")}}, ""
        else:
            result, error = await _create_lead(attendance, binding, responsible)
        lead_id = _clean(result.get("lead_id"), 50)
        if not error and lead_id and planned != "create":
            route_preservation, route_error = await _preserve_existing_lead_route(
                existing, attendance, binding
            )
            result["route_preservation"] = route_preservation
            error = route_error
        created_successfully = planned == "create" and not error and bool(lead_id)
        if created_successfully:
            await _advance_cursor(binding)
        if not error and lead_id and not notes_already_added:
            _, note_error = await _add_note(lead_id, attendance, binding)
            error = note_error
        if error:
            await _update_event(event_id, binding_id=binding["id"], status="failed", action=planned, lead_id=lead_id, responsible_user_id=responsible, error=error, attempts=attempts, details_json=json.dumps(result, ensure_ascii=False, default=str))
            return {"ok": False, "error": error}
        await _update_event(event_id, binding_id=binding["id"], status="success", action=planned, lead_id=lead_id, responsible_user_id=responsible, error="", attempts=attempts, details_json=json.dumps(result, ensure_ascii=False, default=str))
        return {"ok": True, "status": "success", "action": planned, "lead_id": lead_id}


async def _resume_waiting_unsorted(limit: int = 250) -> dict[str, Any]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "SELECT id,lead_id FROM events WHERE status='waiting_unsorted' ORDER BY id ASC LIMIT ?",
            (max(1, min(1000, limit)),),
        )
        rows = [(int(row[0]), _clean(row[1], 50)) for row in await cur.fetchall()]
    resumed = 0
    errors: list[str] = []
    for event_id, lead_id in rows:
        if not lead_id:
            errors.append(f"event {event_id}: lead_id пустой")
            continue
        lead, error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}")
        if error or not isinstance(lead, dict):
            message = error or "Сделка не найдена"
            await _update_event(event_id, error=message)
            errors.append(f"{lead_id}: {message}")
            continue
        is_unsorted, catalog_error = await _lead_unsorted_state(lead)
        if catalog_error:
            await _update_event(event_id, error=catalog_error)
            errors.append(f"{lead_id}: {catalog_error}")
            continue
        if is_unsorted is not False:
            continue
        result = await _process_event(event_id)
        resumed += int(result.get("status") != "waiting_unsorted")
    return {"waiting": len(rows), "resumed": resumed, "errors": errors[:5]}


async def _poll_once(limit: int = 200) -> dict[str, Any]:
    waiting = await _resume_waiting_unsorted()
    settings = await _settings()
    feed_url = _clean(settings.get("feed_url"), 2000)
    token = os.environ.get("NEXUS_BIZON_FEED_TOKEN", "").strip() or _clean(settings.get("feed_token"), 1000)
    if not feed_url or not token:
        return {"ok": False, "error": "feed_url или feed_token не настроены", "processed": 0, **waiting}
    cursor = int(settings.get("feed_cursor") or 0)
    timeout = max(5, min(60, int(settings.get("request_timeout") or 20)))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(feed_url, params={"after": cursor, "limit": min(500, limit)}, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    data = response.json()
    processed = 0
    for change in data.get("items") or []:
        event_id = await _insert_event(change)
        if event_id:
            await _process_event(event_id)
            processed += 1
        cursor = max(cursor, int(change.get("id") or 0))
        await _set_settings({"feed_cursor": cursor})
    return {"ok": True, "processed": processed, "cursor": cursor, "has_more": bool(data.get("has_more")), **waiting}


async def _poll_loop() -> None:
    await asyncio.sleep(5)
    while True:
        wait = 15
        try:
            settings = await _settings()
            wait = max(5, min(300, int(settings.get("poll_seconds") or 15)))
            if _bool(settings.get("poll_enabled")):
                for _ in range(10):
                    result = await _poll_once()
                    if not result.get("has_more"):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "bizon-amocrm poll failed: %s", exc)
        await asyncio.sleep(wait)


@router.get("/health")
async def health():
    settings = await _settings()
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT COUNT(*) FROM events WHERE status='waiting_unsorted'")
        waiting_unsorted = int((await cur.fetchone())[0])
    return {"ok": True, "module": MODULE_ID, "dry_run": _bool(settings.get("dry_run")), "cursor": int(settings.get("feed_cursor") or 0), "waiting_unsorted": waiting_unsorted, **_env_status()}


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    settings = await _settings()
    return {**settings, "feed_token": os.environ.get("NEXUS_BIZON_FEED_TOKEN", "").strip() or settings.get("feed_token", ""), "dry_run": _bool(settings.get("dry_run")), "poll_enabled": _bool(settings.get("poll_enabled")), **_env_status()}


@router.put("/settings")
async def put_settings(data: SettingsIn, request: Request):
    await _require_user(request)
    current = await _settings()
    values: dict[str, Any] = {}
    for key in ("feed_url", "feed_token"):
        value = getattr(data, key)
        if value is not None:
            values[key] = _clean(value, 2000)
    for key in ("dry_run", "poll_enabled"):
        value = getattr(data, key)
        if value is not None:
            values[key] = "1" if value else "0"
    if data.poll_seconds is not None:
        values["poll_seconds"] = max(5, min(300, data.poll_seconds))
    if data.request_timeout is not None:
        values["request_timeout"] = max(5, min(60, data.request_timeout))
    await _set_settings(values)
    return {"ok": True, "changed": sorted(values), "previous_dry_run": _bool(current.get("dry_run"))}


@router.get("/bindings")
async def list_bindings(request: Request):
    await _require_user(request)
    return {"items": await _bindings()}


@router.post("/bindings")
async def save_binding(data: BindingIn, request: Request):
    await _require_user(request)
    if data.match_type not in {"all", "webinar", "room", "contains", "regex"}:
        raise HTTPException(400, "invalid match_type")
    if data.match_type != "all" and not data.match_value:
        raise HTTPException(400, "match_value обязателен")
    if data.match_type == "regex":
        try:
            re.compile(data.match_value)
        except re.error as exc:
            raise HTTPException(400, f"Некорректное регулярное выражение: {exc}") from exc
    min_minutes = float(data.min_minutes if data.min_minutes is not None else data.threshold_minutes)
    max_minutes = float(data.max_minutes) if data.max_minutes is not None else None
    if not 0 <= min_minutes <= 24 * 60:
        raise HTTPException(400, "min_minutes вне диапазона")
    if max_minutes is not None and not min_minutes < max_minutes <= 24 * 60:
        raise HTTPException(400, "max_minutes должен быть больше min_minutes")
    if not _int(data.pipeline_id) or not _int(data.status_id):
        raise HTTPException(400, "Воронка и статус новой сделки обязательны")
    if data.click_status_id and not _int(data.click_status_id):
        raise HTTPException(400, "Некорректный статус для клика")
    if not data.pipeline_scope:
        raise HTTPException(400, "Выберите хотя бы одну воронку поиска дублей")
    if not data.responsible_user_ids:
        raise HTTPException(400, "Выберите ответственных для round-robin")
    duplicate_action = _clean(data.duplicate_action, 30) or "note_only"
    if duplicate_action not in {"update", "merge_empty", "note_only", "skip", "create"}:
        raise HTTPException(400, "Неизвестное действие для найденной сделки")
    for index, rule in enumerate(data.duplicate_rules or DEFAULT_DUPLICATE_RULES):
        if not isinstance(rule, dict):
            raise HTTPException(400, f"Правило дублей #{index + 1} должно быть объектом")
        if _clean(rule.get("entity"), 20) not in {"contacts", "leads"}:
            raise HTTPException(400, f"Правило дублей #{index + 1}: неизвестная сущность amoCRM")
        if not _clean(rule.get("source"), 100):
            raise HTTPException(400, f"Правило дублей #{index + 1}: выберите поле Bizon")
        if not (_int(rule.get("field_id")) or _clean(rule.get("field_code"), 100) or _clean(rule.get("field"), 300)):
            raise HTTPException(400, f"Правило дублей #{index + 1}: выберите поле amoCRM")
    normalized_exclude_conditions: list[dict[str, str]] = []
    if len(data.exclude_conditions) > 20:
        raise HTTPException(400, "Можно настроить не более 20 условий исключения")
    for index, condition in enumerate(data.exclude_conditions):
        if not isinstance(condition, dict):
            raise HTTPException(400, f"Условие исключения #{index + 1} должно быть объектом")
        source = _clean(condition.get("source"), 100)
        operator = _clean(condition.get("operator"), 30)
        value = _clean(condition.get("value"), 1000)
        if not source:
            raise HTTPException(400, f"Условие исключения #{index + 1}: выберите поле Bizon")
        if operator not in EXCLUDE_OPERATORS:
            raise HTTPException(400, f"Условие исключения #{index + 1}: неизвестный оператор")
        if operator not in {"is_empty", "is_not_empty"} and not value:
            raise HTTPException(400, f"Условие исключения #{index + 1}: укажите значение")
        normalized_exclude_conditions.append({"source": source, "operator": operator, "value": value})
    for index, mapping in enumerate(data.field_mappings):
        if not isinstance(mapping, dict):
            raise HTTPException(400, f"Маппинг #{index + 1} должен быть объектом")
        entity = _clean(mapping.get("entity") or "leads", 20)
        if entity not in {"leads", "contacts"}:
            raise HTTPException(400, f"Маппинг #{index + 1}: неизвестная сущность amoCRM")
        if not _clean(mapping.get("source"), 100):
            raise HTTPException(400, f"Маппинг #{index + 1}: выберите поле Bizon")
        if _clean(mapping.get("target"), 30) != "name" and not _int(mapping.get("field_id")):
            raise HTTPException(400, f"Маппинг #{index + 1}: выберите поле amoCRM")
    values = (
        _clean(data.name, 300), data.match_type, _clean(data.match_value, 1000), data.priority,
        min_minutes, min_minutes, max_minutes, _clean(data.pipeline_id, 50), _clean(data.status_id, 50),
        _clean(data.click_status_id, 50),
        json.dumps(data.pipeline_scope, ensure_ascii=False), json.dumps(data.status_scope, ensure_ascii=False),
        duplicate_action, json.dumps(data.duplicate_rules or DEFAULT_DUPLICATE_RULES, ensure_ascii=False),
        json.dumps(normalized_exclude_conditions, ensure_ascii=False),
        json.dumps(data.note_only_status_ids, ensure_ascii=False),
        json.dumps(data.responsible_user_ids, ensure_ascii=False), json.dumps(data.tags, ensure_ascii=False),
        json.dumps(data.field_mappings, ensure_ascii=False), _clean(data.lead_name_template, 1000) or DEFAULT_LEAD_NAME_TEMPLATE,
        _clean(data.note_template, 5000),
        1 if data.active else 0, _now(),
    )
    async with aiosqlite.connect(_must_db()) as db:
        if data.id:
            await db.execute(
                """UPDATE bindings SET name=?,match_type=?,match_value=?,priority=?,threshold_minutes=?,min_minutes=?,max_minutes=?,pipeline_id=?,status_id=?,click_status_id=?,pipeline_scope_json=?,status_scope_json=?,duplicate_action=?,duplicate_rules_json=?,exclude_conditions_json=?,note_only_status_ids_json=?,responsible_user_ids_json=?,tags_json=?,field_mappings_json=?,lead_name_template=?,note_template=?,active=?,updated_at=? WHERE id=?""",
                values + (data.id,),
            )
            binding_id = data.id
        else:
            cur = await db.execute(
                """INSERT INTO bindings(name,match_type,match_value,priority,threshold_minutes,min_minutes,max_minutes,pipeline_id,status_id,click_status_id,pipeline_scope_json,status_scope_json,duplicate_action,duplicate_rules_json,exclude_conditions_json,note_only_status_ids_json,responsible_user_ids_json,tags_json,field_mappings_json,lead_name_template,note_template,active,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
            binding_id = int(cur.lastrowid)
        await db.commit()
    return {"ok": True, "id": binding_id}


@router.delete("/bindings/{binding_id}")
async def delete_binding(binding_id: int, request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM bindings WHERE id=?", (binding_id,))
        await db.commit()
    return {"ok": True}


def _slim_amo_field(field: dict[str, Any]) -> dict[str, Any]:
    enums = []
    for item in field.get("enums") or []:
        if not isinstance(item, dict):
            continue
        enums.append({
            "id": item.get("id"),
            "value": item.get("value"),
            "code": item.get("code") or item.get("enum_code"),
        })
    return {
        "id": field.get("id"),
        "name": field.get("name"),
        "type": field.get("type"),
        "code": field.get("code"),
        "group_id": field.get("group_id"),
        "sort": field.get("sort"),
        "enums": enums,
    }


def _bizon_value_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "text"


async def _bizon_field_catalog() -> list[dict[str, Any]]:
    definitions = {
        code: {"code": code, "label": label, "type": kind, "group": group, "description": description}
        for code, label, kind, group, description in BIZON_FIELD_DEFINITIONS
    }
    seen: dict[str, dict[str, Any]] = {}
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT payload_json FROM events ORDER BY id DESC LIMIT 200")
        rows = await cur.fetchall()
    for row in rows:
        attendance = _json(row[0], {})
        if not isinstance(attendance, dict):
            continue
        for code, value in attendance.items():
            if code in {"profiles", "identity_tokens", "watch_intervals"}:
                continue
            item = seen.setdefault(str(code), {"count": 0, "type": _bizon_value_type(value)})
            item["count"] += 1
    for code, item in seen.items():
        if code not in definitions:
            definitions[code] = {
                "code": code,
                "label": code,
                "type": item["type"],
                "group": "Получено дополнительно",
                "description": "Поле обнаружено в реальном attendance-feed",
            }
    for code, item in definitions.items():
        item["seen_count"] = int((seen.get(code) or {}).get("count") or 0)
    return sorted(definitions.values(), key=lambda item: (item["group"], item["label"].casefold(), item["code"]))


@router.get("/bizon/fields")
async def bizon_fields(request: Request):
    await _require_user(request)
    items = await _bizon_field_catalog()
    return {"items": items, "observed_fields": sum(1 for item in items if item.get("seen_count"))}


@router.get("/amo/catalog")
async def amo_catalog(request: Request):
    await _require_user(request)
    pipelines, error, _ = await _amo_request("GET", "/api/v4/leads/pipelines")
    if error:
        raise HTTPException(502, error)
    users, error, _ = await _amo_request("GET", "/api/v4/users?limit=250")
    if error:
        raise HTTPException(502, error)
    lead_fields, _ = await _catalog("leads")
    contact_fields, _ = await _catalog("contacts")
    pipeline_rows = []
    for pipeline in (((pipelines or {}).get("_embedded") or {}).get("pipelines") or []):
        statuses = []
        for status in (((pipeline.get("_embedded") or {}).get("statuses")) or []):
            statuses.append({
                "id": status.get("id"), "name": status.get("name"),
                "sort": status.get("sort"), "type": status.get("type"),
                "color": status.get("color"),
            })
        pipeline_rows.append({
            "id": pipeline.get("id"), "name": pipeline.get("name"),
            "sort": pipeline.get("sort"), "is_main": pipeline.get("is_main"),
            "_embedded": {"statuses": statuses},
        })
    user_rows = []
    for user in (((users or {}).get("_embedded") or {}).get("users") or []):
        rights = user.get("rights") if isinstance(user.get("rights"), dict) else {}
        user_rows.append({
            "id": user.get("id"), "name": user.get("name"),
            "is_active": bool(rights.get("is_active", user.get("is_active", True))),
        })
    return {
        "pipelines": pipeline_rows,
        "users": user_rows,
        "lead_fields": [_slim_amo_field(field) for field in lead_fields],
        "contact_fields": [_slim_amo_field(field) for field in contact_fields],
    }


@router.post("/presets/from-leads")
async def preset_from_leads(data: PresetIn, request: Request):
    await _require_user(request)
    if not data.lead_ids or len(data.lead_ids) > 10:
        raise HTTPException(400, "Укажите от 1 до 10 lead_ids")
    samples = []
    for lead_id in data.lead_ids:
        lead, error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}?with=contacts")
        if error:
            samples.append({"lead_id": lead_id, "error": error})
            continue
        samples.append({
            "lead_id": lead_id,
            "pipeline_id": lead.get("pipeline_id"),
            "status_id": lead.get("status_id"),
            "responsible_user_id": lead.get("responsible_user_id"),
            "tag_names": [item.get("name") for item in ((lead.get("_embedded") or {}).get("tags") or [])],
            "custom_field_ids": [item.get("field_id") for item in lead.get("custom_fields_values") or []],
        })
    lead_fields, _ = await _catalog("leads")
    field_by_id = {str(item.get("id")): item for item in lead_fields}
    source_hints = (
        ("utm_source", "utm_source"), ("utm_medium", "utm_medium"),
        ("utm_campaign", "utm_campaign"), ("utm_content", "utm_content"),
        ("utm_term", "utm_term"), ("минут", "watch_minutes"),
        ("время на веб", "watch_minutes"), ("webinar", "webinarId"),
        ("вебинар", "webinarId"), ("комнат", "roomid"),
    )
    used_ids = {str(field_id) for sample in samples for field_id in sample.get("custom_field_ids", [])}
    mappings = []
    for field_id in used_ids:
        field = field_by_id.get(field_id) or {}
        name = _clean(field.get("name"), 300).casefold()
        source = next((source for needle, source in source_hints if needle in name), "")
        if source:
            mappings.append({"field_id": int(field_id), "field_name": field.get("name"), "source": source})
    first = next((sample for sample in samples if not sample.get("error")), {})
    preset = {
        "source_lead_ids": data.lead_ids,
        "samples": samples,
        "suggested_binding": {
            "pipeline_id": first.get("pipeline_id"),
            "status_id": first.get("status_id"),
            "responsible_user_ids": [str(first.get("responsible_user_id"))] if first.get("responsible_user_id") else [],
            "tags": first.get("tag_names") or [],
            "field_mappings": mappings,
        },
        "created_at": _now(),
    }
    await _set_settings({"sample_preset_json": json.dumps(preset, ensure_ascii=False)})
    return preset


@router.get("/events")
async def events(request: Request, limit: int = Query(100, ge=1, le=500), status: str = ""):
    await _require_user(request)
    where, params = ("WHERE status=?", [status]) if status else ("", [])
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", params + [limit])
        return {"items": [dict(row) for row in await cur.fetchall()]}


@router.post("/events/retry")
async def retry_event(data: RetryIn, request: Request):
    await _require_user(request)
    return await _process_event(data.event_id)


@router.post("/events/retry-pending")
async def retry_pending(request: Request, limit: int = Query(100, ge=1, le=500)):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT id FROM events WHERE status IN ('pending_binding','failed') ORDER BY id ASC LIMIT ?", (limit,))
        ids = [int(row[0]) for row in await cur.fetchall()]
    results = [await _process_event(event_id) for event_id in ids]
    return {"ok": True, "processed": len(results), "failed": sum(1 for item in results if not item.get("ok"))}


@router.post("/sync/run")
async def sync_run(request: Request):
    await _require_user(request)
    return await _poll_once()
