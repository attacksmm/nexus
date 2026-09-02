from __future__ import annotations

import asyncio
import base64
import html
import json
import os
import random
import re
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import uuid

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from orchestrator.telegram_proxy import telegram_bot_api_proxy_url as global_telegram_bot_api_proxy_url
from orchestrator.vk_group_poll import (
    VkGroupPollSubscription,
    adapt_group_message_event,
    shared_vk_group_poll_hub,
)

try:
    from orchestrator.auth import can_access_module, verify_token_from_request
except Exception:  # pragma: no cover - isolated local checks
    can_access_module = None
    verify_token_from_request = None


router = APIRouter()

MODULE_ID = "chat-moderators"
VK_API_VERSION = "5.131"
VK_MESSAGE_CHUNK_LIMIT = 3500
VK_CHAT_PEER_BASE = 2_000_000_000
VK_CHAT_DISCOVERY_BATCH_SIZE = 25
VK_CHAT_DISCOVERY_EMPTY_TAIL = 100
VK_CHAT_DISCOVERY_HARD_LIMIT = 2_000
SENLER_API_BASE = "https://senler.ru/api"
SENLER_COURSE_CHAT_SUBSCRIPTION_ID = "3801272"
GETCOURSE_ACCESS_CHAT_ID = 2000000090
GETCOURSE_ACCESS_CHAT_TITLES = {"ник, открой", "ник, помоги", "тех.спец || агент"}
GETCOURSE_NICK_RE = re.compile(r"(?<![\wа-яё])ник(?![\wа-яё])", re.IGNORECASE)
GETCOURSE_ACTION_RE = re.compile(
    r"\b(?:откро\w*|закро\w*|убер\w*|удал\w*|выда\w*|дай|добав\w*|сним\w*|забер\w*|нуж\w*|остав\w*)\b",
    re.IGNORECASE,
)
GETCOURSE_STATE_RE = re.compile(
    r"\b(?:(?:сколько|какие)\s+(?:модул\w*|групп\w*|доступ\w*)\s+(?:открыт\w*|выдан\w*|есть)|что\s+открыт\w*)\b",
    re.IGNORECASE,
)
GETCOURSE_PENDING_TIMEOUT_SECONDS = 60
YES_VALUES = {"да", "yes", "y", "+"}
NO_VALUES = {"нет", "no", "n", "-"}
VK_REACTION_IN_PROGRESS_ID = int(os.getenv("VK_REACTION_IN_PROGRESS_ID", "12"))
VK_REACTION_SUCCESS_ID = int(os.getenv("VK_REACTION_SUCCESS_ID", "4"))
VK_REACTION_FAILURE_ID = int(os.getenv("VK_REACTION_FAILURE_ID", "9"))

TG_ALLOWED_ADDERS_DEFAULT = "5601500901,5447488280"
VK_ALLOWED_ADMINS_DEFAULT = "1105209997"
VK_TRUSTED_SENDERS_DEFAULT = "765938,1105209997"

VK_CREATED_CHAT_PATTERNS = [
    re.compile(r"^\d+\.\s*\d{2}\.\d{2}\.\d{4}\s*-\s*Курс Щенок\. Современный Собаковод$", re.IGNORECASE),
    re.compile(r"^\d+\.\s*\d{2}\.\d{2}\.\d{4}\s*-\s*Современный Собаковод - закрытый чат$", re.IGNORECASE),
]
TRUSTED_MODERATOR_RESOURCE_RE = re.compile(
    r"(?i)(?:https?://)?(?:m\.)?(?:vk\.com|vk\.ru)/(?:id765938|timofeevapodbordog|id1105209997|tehpod_sobakovodpro)\b"
    r"|(?:https?://)?vk\.me/(?:id1105209997|tehpod_sobakovodpro)\b"
    r"|@(?:tehpod_sobakovodpro|timofeevapodbordog)\b"
    r"|\[id(?:765938|1105209997)\|"
)
URL_PATTERN = re.compile(r"(?i)\b(?:https?://|www\.)[^\s<>()]+")
TRUSTED_HOST_SUFFIXES = (
    "sobakovod.pro",
    "wildberries.ru",
    "wb.ru",
    "ozon.ru",
    "yandex.ru",
    "yandex.by",
    "yandex.kz",
    "yandex.com",
    "yandex.market",
    "market.yandex.ru",
    "disk.yandex.ru",
    "mail.ru",
    "sharing.mail.ru",
    "cloud.mail.ru",
)
TRUSTED_URL_IDENTITIES = {
    "vk.com/id765938",
    "vk.com/timofeevapodbordog",
    "vk.ru/id765938",
    "vk.ru/timofeevapodbordog",
    "vk.com/id1105209997",
    "vk.com/tehpod_sobakovodpro",
    "vk.ru/id1105209997",
    "vk.ru/tehpod_sobakovodpro",
    "vk.me/id1105209997",
    "vk.me/tehpod_sobakovodpro",
}
PROFANITY_RE = re.compile(
    r"(?i)(?:"
    r"\bбл(?:я|ять|ядь|ин)\b|"
    r"\bсу(?:ка|ки|чар|чк)\w*\b|"
    r"\b(?:хуй|хуя|хуе|хуё|нахуй|похуй|оху)\w*\b|"
    r"\b(?:пизд|пизж|пзд)\w*\b|"
    r"\b(?:еба|ёба|ебн|ёбн|ебу|ёбу|ебл|ёбл|ебан|ёбан|заеб|заёб|уеб|уёб)\w*\b|"
    r"\bпид[оа]р\w*\b|"
    r"\bмраз[ьи]\w*\b|"
    r"\bгандон\w*\b"
    r")"
)
MILD_INTERJECTION_RE = re.compile(r"(?i)\bблин\b")
CANINE_SEX_TERM_RE = re.compile(r"(?i)\b(?:сук(?:а|и|е|у|ой|ою|ами?|ах)?|сучк(?:а|и|е|у|ой|ою|ами?|ах)?)\b")
CANINE_CONTEXT_RE = re.compile(
    r"(?i)\b(?:"
    r"собак\w*|пес|пёс|пса|псу|псом|псы|псов|щен\w*|кобел\w*|"
    r"йорк\w*|корги|овчарк\w*|немк\w*|девочк[аи]\s*-\s*собачк\w*|"
    r"кинолог\w*|заводчиц\w*|вольер\w*|манеж\w*|веран[дт]\w*|стерилизован\w*|"
    r"броса\w*|ла[её]т|рычит|куса\w*|гуля\w*|подъезд\w*|"
    r"осторожн\w*|недоверчив\w*|доверя\w*|страх\w*|пуглив\w*|боится|боят\w*|"
    r"ручк\w*|пузик\w*|целова\w*|рождени\w*"
    r")\b"
)
DIRECT_ABUSE_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:ты|вы|тебя|тебе|вам|вас|админ\w*|куратор\w*|команд\w*|кинолог\w*|участник\w*|люди\s+(?:тут|здесь|в\s+чат[еау]))\b"
    r".{0,80}\b(?:зл[её]йш\w*|туп\w*|идиот\w*|урод\w*|мраз\w*|ненавиж\w*|бесит\w*)\b|"
    r"\b(?:зл[её]йш\w*|туп\w*|идиот\w*|урод\w*|мраз\w*|ненавиж\w*|бесит\w*)\b"
    r".{0,80}\b(?:ты|вы|админ\w*|куратор\w*|команд\w*|кинолог\w*|участник\w*|чат)\b"
    r")"
)
GENERAL_VENT_RE = re.compile(
    r"(?i)^\s*(?:люди|человек|человечество)\s+(?:вообще\s+)?(?:зл[её]йш\w+|странн\w+|жесток\w+|непонятн\w+)(?:\s+\w+){0,4}\s*[.!?…]*\s*$"
)
NEGATIVE_LANGUAGE_MENTION_RE = re.compile(
    r"(?i)\b(?:запрет\w*|правил\w*|говор\w*|обсужд\w*|информац\w*)\b"
    r".{0,100}\b(?:руган\w*|мат(?:а|ом|е)?|оскорблен\w*|агресси\w*)\b"
)
BENIGN_CONSISTENCY_RE = re.compile(
    r"(?i)^\s*(?:да|нет|ну)?\s*(?:жидковат\w*|густоват\w*|мягковат\w*|суховат\w*|твердоват\w*)\s*[.!?…)]*\s*$"
)
REFUND_RE = re.compile(
    r"(?i)(?:"
    r"\bверн(?:ите|уть|и|ул[аи]?|ули)\s+(?:мне\s+)?(?:деньги|средства|оплат[уы])\b|"
    r"\b(?:деньги|средства|оплат[ау])\s+(?:назад|верн(?:ите|уть|и|ул[аи]?|ули))\b|"
    r"\bвозврат(?:\s+(?:денег|средств|оплаты))?\b|"
    r"\bотказ(?:аться)?\s+от\s+(?:курса|обучения|подписки)\b|"
    r"\bрасторг(?:нуть|аю|аем|ли)\s+договор\b|"
    r"\bотмен(?:а|ить|яю)\s+(?:подписк[уи]|оплат[уы]|курс)\b"
    r")"
)
TECH_SUPPORT_RE = re.compile(
    r"(?i)(?:"
    r"\bтех\s*под(?:держк[аиу])?\b|"
    r"\bтехподдержк[аиу]?\b|"
    r"\b(?:не\s+)?(?:открыва(?:ется|ются)|работа(?:ет|ют)|загружа(?:ется|ются))\s+(?:урок|видео|сайт|кабинет|платформ|доступ|материал)\w*\b|"
    r"\b(?:нет|не\s+вижу|пропал)\s+доступ\b|"
    r"\b(?:ошибка|проблем[аы])\s+(?:входа|авторизац|доступа|оплат[уы]|на\s+сайте)\b"
    r")"
)
TRAINING_CHAT_TRANSFER_RE = re.compile(
    r"(?i)(?=.*\b(?:чат|бесед)\w*\b)"
    r"(?=.*\b(?:щенк|собак|курс|обучен|поток|владельц|возраст|месяц|мес|6\\+|до\\s*6|старш|младш)\w*\b)"
    r"(?:"
    r"\b(?:перейт|попасть|вступить|зайти|добав(?:ьте|ить|или)|присла(?:ли|ть)|скин(?:ьте|уть|ули)|ссылк)\w*\b"
    r"|6\\+"
    r")"
)
GENERIC_CHAT_REDIRECT_RE = re.compile(
    r"(?i)(?=.*\b(?:чат|бесед|канал|групп)\w*\b)"
    r"(?:"
    r"\b(?:перейт|переход|переезжа|уходим|вступ(?:ай|ить)|заход(?:и|ите)|добавля(?:й|йтесь)|подписыва(?:й|йтесь))\w*\b"
    r"|срочно\\s+(?:перейт|заход|вступ)"
    r")"
)

TG_SYSTEM_PROMPT = """Ты модератор Telegram-чата. Твоя задача — определить, является ли сообщение:
- негативом (оскорбления, агрессия, токсичность, угрозы),
- скамом (реклама сторонних услуг, ссылки на подозрительные ресурсы, мошеннические предложения, "заработать деньги", казино, ставки, крипто-схемы, продажа аккаунтов и т.п.).

Слова с негативной окраской сами по себе не означают нарушение. Описания поведения или состояния собаки, пересказ правил и упоминание ругани, короткие бытовые оценки без адресной агрессии — это "ок". Если сомневаешься, выбирай "ок".

Отвечай СТРОГО одним словом: "негатив", "скам" или "ок"."""

VK_SYSTEM_PROMPT = """Ты модератор учебного чата ВК. Твоя задача — проанализировать сообщение и отнести его к одной из 4 категорий:
1. "возврат" — вопросы про возврат денег, отмену подписки, рассрочку, финансовые вопросы.
2. "техпод" — технические проблемы (не открывается урок, не работает сайт, проблемы с доступом).
3. "негатив" — реклама сторонних услуг, мошенничество, спам, агрессия, подозрительные ссылки, попытки увести людей в другие чаты.
4. "нейтрально" — всё остальное (обычное общение, вопросы про поведение и воспитание собак, домашка).

ВАЖНО: вопросы про поведение и воспитание собак — это категория "нейтрально", а не "техпод".
Важное правило по ссылкам: wildberries, ozon, обычные ссылки yandex и mail.ru, домены sobakovod.pro и обычные ссылки на товары/файлы не считать негативом сами по себе. Если сомневаешься, выбирай "нейтрально".

Ответь ТОЛЬКО одним словом из списка: возврат, техпод, негатив, нейтрально.
Никаких объяснений, только одно слово."""

WELCOME_TEMPLATE = (
    "{user_mention}, Здравствуйте и добро пожаловать на наш курс 🥰 "
    "заглядывайте в закрепленное сообщение, будем ждать Вашу визитку ❤️"
)
VK_REFUND_TEMPLATE = "{user_mention}, по всем вопросам касающихся оплат и возвратов обращаться ВКонтакте - @id11335495"
TG_UNAUTHORIZED_ADD_TEMPLATE = "У вас нет прав для использования этого бота."
TG_WELCOME_TEMPLATE = (
    "{user_mention}, здравствуйте и добро пожаловать на наш курс 🥰 "
    "заглядывайте в закрепленное сообщение, будем ждать Вашу визитку ❤️"
)
TG_REFUND_TEMPLATE = "{user_mention}, по вопросам оплаты и возвратов напишите в поддержку: @tech_sobakovod_pro"
TG_TECH_SUPPORT_TEMPLATE = "{user_mention}, по техническим вопросам напишите в поддержку: @tech_sobakovod_pro"

DEFAULT_SETTINGS = {
    "runtime_enabled": "false",
    "tg_enabled": "false",
    "vk_enabled": "false",
    "vk_tech_agent_enabled": "false",
    "dry_run": "true",
    "tg_send_responses": "false",
    "vk_send_responses": "false",
    "history_retention_days": "90",
    "tg_allowed_adders": TG_ALLOWED_ADDERS_DEFAULT,
    "telegram_log_chat_id": "-1002852064172",
    "chat_title_contains": "Курс Щенок. Современный Собаковод;Современный Собаковод - закрытый чат",
    "vk_allowed_admins": VK_ALLOWED_ADMINS_DEFAULT,
    "vk_trusted_senders": VK_TRUSTED_SENDERS_DEFAULT,
    "vk_training_title_allowlist": "",
    "vk_ai_prompt_path": "prompts/kuratorbot_promt.txt",
    "telegram_bot_api_proxy_url": "",
}

SECRET_SPECS = {
    "telegram_bot_token_moderator": {
        "env": "TELEGRAM_BOT_TOKEN_MODERATOR",
        "label": "Telegram токен бота-модератора",
        "kind": "token",
        "default": "",
    },
    "vk_group_token": {
        "env": "VK_GROUP_TOKEN",
        "label": "VK токен сообщества",
        "kind": "token",
        "default": "",
    },
    "vk_group_id": {
        "env": "VK_GROUP_ID",
        "label": "VK ID сообщества",
        "kind": "text",
        "default": "",
    },
    "vk_log_chat_id": {
        "env": "VK_LOG_CHAT_ID",
        "label": "VK peer_id чата логов",
        "kind": "text",
        "default": "2000000041",
    },
    "openrouter_api_key": {
        "env": "OPENROUTER_API_KEY",
        "label": "OpenRouter API key",
        "kind": "token",
        "default": "",
    },
    "getcourse_account_name": {
        "env": "GETCOURSE_ACCOUNT_NAME",
        "label": "GetCourse имя аккаунта",
        "kind": "text",
        "default": "",
        "visible": False,
    },
    "getcourse_api_token": {
        "env": "GETCOURSE_API_TOKEN",
        "label": "GetCourse API токен",
        "kind": "token",
        "default": "",
        "visible": False,
    },
    "public_base_url": {
        "env": "SBKVD_SERVER_PUBLIC_BASE_URL",
        "label": "Публичный base URL",
        "kind": "url",
        "default": "https://junior.sobakovod.pro",
        "visible": False,
    },
}

DEFAULT_TEMPLATES = {
    "vk_welcome": {
        "title": "VK приветствие участника",
        "body": WELCOME_TEMPLATE,
        "help": "Доступна переменная {user_mention}.",
        "enabled": True,
    },
    "vk_refund": {
        "title": "VK ответ про оплату/возврат",
        "body": VK_REFUND_TEMPLATE,
        "help": "Доступна переменная {user_mention}.",
        "enabled": False,
    },
    "tg_unauthorized_add": {
        "title": "Telegram ответ при неразрешённом добавлении",
        "body": TG_UNAUTHORIZED_ADD_TEMPLATE,
        "help": "Отправляется перед выходом из чата, если dry-run выключен.",
        "enabled": True,
    },
    "tg_welcome": {
        "title": "Telegram приветствие участника",
        "body": TG_WELCOME_TEMPLATE,
        "help": "Доступны переменные {user_mention}, {user_name}.",
        "enabled": True,
    },
    "tg_refund": {
        "title": "Telegram ответ про оплату/возврат",
        "body": TG_REFUND_TEMPLATE,
        "help": "Доступны переменные {user_mention}, {user_name}.",
        "enabled": False,
    },
    "tg_tech_support": {
        "title": "Telegram ответ про техподдержку",
        "body": TG_TECH_SUPPORT_TEMPLATE,
        "help": "Доступны переменные {user_mention}, {user_name}.",
        "enabled": False,
    },
}

_ctx = None
_logger = None
_runtime: RuntimeManager | None = None
_prompt_cache: dict[str, Any] = {}
_openrouter_model_cache: dict[str, Any] = {}
_vk_staff_registry_cache: dict[str, Any] = {}
MODERATION_DEGRADED_CATEGORY = "не проверено"


async def setup(ctx):
    global _ctx, _logger, _runtime
    _ctx = ctx
    _logger = getattr(ctx, "logger", None)
    _init_db()
    _runtime = RuntimeManager()
    corrected = _sync_runtime_state_with_live_snapshot(
        reason="Nexus restarted; runtime is not auto-started"
    )
    _log(
        "info",
        "chat-moderators setup completed; runtime is not auto-started; corrected_stale=%s",
        ",".join(corrected) if corrected else "-",
    )
    settings = _settings()
    if _env_forces_false("NEXUS_CHAT_MODERATORS_RUNTIME_ENABLED"):
        _log("info", "chat-moderators runtime auto-start skipped: env runtime disabled")
    elif _truthy(settings.get("runtime_enabled")):
        try:
            result = await _runtime.start("all")
            _log("info", "chat-moderators runtime auto-start completed: %s", result)
        except Exception as error:
            _log("error", "chat-moderators runtime auto-start failed: %s", error)


async def shutdown() -> None:
    global _runtime
    runtime, _runtime = _runtime, None
    if runtime is None:
        return

    async def stop_platform(name: str, stop) -> None:
        try:
            await asyncio.wait_for(stop(), timeout=2.0)
        except TimeoutError:
            _log("warning", "chat-moderators %s shutdown timed out; forcing lifecycle cancellation", name)
        except Exception as error:
            _log("warning", "chat-moderators %s shutdown failed: %s", name, error)

    await asyncio.gather(
        stop_platform("telegram", runtime.telegram.stop),
        stop_platform("vk", runtime.vk.stop),
    )


async def on_telegram_proxy_changed() -> dict[str, Any]:
    runtime = _runtime
    if runtime is None or not runtime.telegram.running:
        return {"reconnected": False, "reason": "telegram runtime is not running"}
    await runtime.stop("telegram")
    result = await runtime.start("telegram")
    _log("info", "telegram runtime reconnected after global proxy change")
    return {"reconnected": True, "result": result}


def _log(level: str, message: str, *args: Any, **kwargs: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


def _db_path() -> Path:
    if _ctx is not None:
        return _ctx.db_path
    return Path(__file__).parent / "data" / f"{MODULE_ID}.db"


def _module_dir() -> Path:
    if _ctx is not None and getattr(_ctx, "module_dir", None):
        return Path(_ctx.module_dir)
    return Path(__file__).parent


def _data_dir() -> Path:
    if _ctx is not None and getattr(_ctx, "data_dir", None):
        return Path(_ctx.data_dir)
    return Path(__file__).parent / "data"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "да", "y"}


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _safe_prompt_parts(prompt_path: str) -> list[str]:
    parts = [part for part in str(prompt_path or "").strip("/").split("/") if part]
    if not parts:
        raise ValueError("prompt path is empty")
    for part in parts:
        if part in {".", ".."} or "/" in part or "\\" in part:
            raise ValueError("invalid prompt path")
    return parts


def _file_storage_locations() -> tuple[Path, Path]:
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "file-storage",
        module_dir.parent / "module_file_storage",
        module_dir.parent.parent / "modules" / "file-storage",
        module_dir.parent.parent / "module_file_storage",
    ]
    for candidate in candidates:
        db_path = candidate / "data" / "file-storage.db"
        blob_dir = candidate / "data" / "blobs"
        if db_path.exists() and blob_dir.exists():
            return db_path, blob_dir
    return candidates[0] / "data" / "file-storage.db", candidates[0] / "data" / "blobs"


def _openrouter_db_path() -> Path:
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "openrouter" / "data" / "openrouter.db",
        module_dir.parent / "module_openrouter" / "data" / "openrouter.db",
        module_dir.parent.parent / "modules" / "openrouter" / "data" / "openrouter.db",
        module_dir.parent.parent / "module_openrouter" / "data" / "openrouter.db",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _course_chat_creator_db_path() -> Path:
    module_dir = _module_dir()
    candidates = [
        module_dir.parent / "course-chat-creator" / "data" / "course-chat-creator.db",
        module_dir.parent / "module_course_chat_creator" / "data" / "course-chat-creator.db",
        module_dir.parent.parent / "modules" / "course-chat-creator" / "data" / "course-chat-creator.db",
        module_dir.parent.parent / "module_course_chat_creator" / "data" / "course-chat-creator.db",
    ]
    return next((path for path in candidates if path.exists()), candidates[0])


def _trusted_vk_staff_ids() -> set[int]:
    now = time.monotonic()
    cached = _vk_staff_registry_cache
    if cached and now - float(cached.get("checked_at") or 0) < 30:
        return set(cached.get("user_ids") or set())
    user_ids: set[int] = set()
    path = _course_chat_creator_db_path()
    try:
        if path.exists():
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2) as db:
                row = db.execute("SELECT value FROM meta WHERE key='vk_staff_registry_v1'").fetchone()
            payload = json.loads(row[0]) if row and row[0] else {}
            user_ids = {
                int(value) for value in (payload.get("user_ids") or [])
                if str(value).isdigit() and int(value) > 0
            }
    except Exception as error:
        _log("warning", "VK staff registry lookup failed: %s", error)
    _vk_staff_registry_cache.clear()
    _vk_staff_registry_cache.update({"checked_at": now, "user_ids": user_ids})
    return set(user_ids)


def _openrouter_model_for_prompt(prompt_path: str) -> dict[str, str]:
    normalized = "/".join(_safe_prompt_parts(prompt_path)) if str(prompt_path or "").strip() else ""
    cache_key = normalized or "__default__"
    now = time.monotonic()
    cached = _openrouter_model_cache.get(cache_key)
    if cached and now - float(cached.get("checked_at") or 0) < 30:
        return dict(cached.get("payload") or {})
    fallback = "deepseek/deepseek-chat"
    payload = {"model": fallback, "source": "module-default"}
    db_path = _openrouter_db_path()
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as db:
                if normalized:
                    row = db.execute("SELECT model FROM prompt_models WHERE prompt_path=?", (normalized,)).fetchone()
                    if row and str(row[0] or "").strip():
                        payload = {"model": str(row[0]).strip(), "source": "openrouter-prompt"}
                    else:
                        row = db.execute("SELECT value FROM settings WHERE key='default_model'").fetchone()
                        if row and str(row[0] or "").strip():
                            payload = {"model": str(row[0]).strip(), "source": "openrouter-default"}
                else:
                    row = db.execute("SELECT value FROM settings WHERE key='default_model'").fetchone()
                    if row and str(row[0] or "").strip():
                        payload = {"model": str(row[0]).strip(), "source": "openrouter-default"}
        except Exception as error:
            _log("warning", "OpenRouter model lookup failed path=%s error=%s", normalized, error)
    _openrouter_model_cache[cache_key] = {"checked_at": now, "payload": payload}
    return payload


def _resolve_file_storage_text(prompt_path: str) -> str:
    parts = _safe_prompt_parts(prompt_path)
    normalized = "/".join(parts)
    now = time.monotonic()
    cached = _prompt_cache.get(normalized)
    if cached and now - float(cached.get("checked_at") or 0) < 30:
        return str(cached.get("text") or "")
    db_path, blob_dir = _file_storage_locations()
    if not db_path.exists():
        raise FileNotFoundError("file-storage DB not found")
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        current_id = 1
        item = None
        for idx, name in enumerate(parts):
            item = db.execute("SELECT * FROM items WHERE parent_id=? AND name=?", (current_id, name)).fetchone()
            if item is None:
                raise FileNotFoundError(f"prompt not found: {normalized}")
            if idx < len(parts) - 1 and item["kind"] != "folder":
                raise FileNotFoundError(f"prompt not found: {normalized}")
            current_id = int(item["id"])
    if item is None or item["kind"] != "file" or item["ext"] != "txt":
        raise ValueError("prompt must be a .txt file")
    blob_path = blob_dir / str(item["stored_name"] or "")
    if not blob_path.exists():
        raise FileNotFoundError("prompt blob not found")
    text = blob_path.read_text(encoding="utf-8").strip().lstrip("\ufeff")
    if not text:
        raise ValueError("prompt file is empty")
    _prompt_cache[normalized] = {"checked_at": now, "text": text, "size": len(text)}
    return text


def _vk_system_prompt() -> str:
    prompt_path = (_settings().get("vk_ai_prompt_path") or "").strip()
    if not prompt_path:
        return VK_SYSTEM_PROMPT
    try:
        return _resolve_file_storage_text(prompt_path)
    except Exception as error:
        _log("warning", "VK prompt from file-storage unavailable path=%s error=%s", prompt_path, error)
        return VK_SYSTEM_PROMPT


def _prompt_status() -> dict[str, Any]:
    prompt_path = (_settings().get("vk_ai_prompt_path") or "").strip()
    model_info = _openrouter_model_for_prompt(prompt_path) if prompt_path else _openrouter_model_for_prompt("")
    if not prompt_path:
        return {"vk_ai_prompt_path": "", "ready": False, "source": "fallback", "error": "path is empty", "model": model_info.get("model"), "model_source": model_info.get("source")}
    try:
        text = _resolve_file_storage_text(prompt_path)
        return {"vk_ai_prompt_path": prompt_path, "ready": True, "source": "file-storage", "size": len(text), "model": model_info.get("model"), "model_source": model_info.get("source")}
    except Exception as error:
        return {"vk_ai_prompt_path": prompt_path, "ready": False, "source": "fallback", "error": str(error), "model": model_info.get("model"), "model_source": model_info.get("source")}


def _getcourse_access_db_status() -> dict[str, Any]:
    path = _data_dir() / "getcourse_access.db"
    if not path.exists():
        return {"ready": False, "path": str(path), "error": "getcourse_access.db not found"}
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
            quick_check = str(db.execute("PRAGMA quick_check").fetchone()[0])
            tables = {
                "groups": int(db.execute("SELECT COUNT(*) FROM gc_groups_catalog").fetchone()[0]),
                "snapshots": int(db.execute("SELECT COUNT(*) FROM gc_user_snapshots").fetchone()[0]),
                "backups": int(db.execute("SELECT COUNT(*) FROM gc_group_backups").fetchone()[0]),
                "requests": int(db.execute("SELECT COUNT(*) FROM gc_access_requests").fetchone()[0]),
                "temporary_jobs": int(db.execute("SELECT COUNT(*) FROM gc_temporary_access_jobs").fetchone()[0]),
            }
        return {"ready": quick_check == "ok", "path": str(path), "quick_check": quick_check, "tables": tables}
    except Exception as error:
        return {"ready": False, "path": str(path), "error": str(error)}


class GetCourseAccessError(RuntimeError):
    pass


@dataclass
class ParsedAccessCommand:
    identifier: str
    course_keys: list[str]
    package_key: str | None
    add_modules: list[int]
    remove_modules: list[int]
    all_modules: bool
    replace_managed_groups: bool
    keep_only_modules: bool
    raw_text: str


def _gc_db_path() -> Path:
    return _data_dir() / "getcourse_access.db"


@contextmanager
def _gc_db():
    path = _gc_db_path()
    if not path.exists():
        raise GetCourseAccessError("getcourse_access.db не найден в data модуля")
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _gc_json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _gc_json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _phone_digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _normalize_phone(value: Any) -> str:
    digits = _phone_digits(value)
    if len(digits) == 10:
        digits = f"7{digits}"
    elif len(digits) == 11 and digits.startswith("8"):
        digits = f"7{digits[1:]}"
    return f"+{digits}" if len(digits) == 11 else ""


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold()).replace("ё", "е")


def _preview_log_text(value: Any, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text if len(text) <= limit else f"{text[:limit]}..."


def _configured_title_fragments(settings: dict[str, str] | None = None) -> list[str]:
    values = settings or _settings()
    raw = str(values.get("chat_title_contains") or "")
    return [_norm_text(item) for item in raw.split(";") if _norm_text(item)]


def _title_matches_config(title: Any, settings: dict[str, str] | None = None) -> bool:
    value = _norm_text(title)
    if not value:
        return False
    return any(fragment in value for fragment in _configured_title_fragments(settings))


def _gc_create_group_backup(*, gc_user_id: str, source_text: str, groups: list[dict[str, Any]]) -> int:
    with _gc_db() as db:
        cur = db.execute(
            "INSERT INTO gc_group_backups(gc_user_id,source_text,groups_json,created_at) VALUES(?,?,?,?)",
            (str(gc_user_id), source_text, _gc_json_dumps(groups), time.time()),
        )
        return int(cur.lastrowid)


def _gc_create_access_request(
    *,
    request_id: str,
    requester_chat_id: str | None,
    requester_user_id: str | None,
    command_text: str,
    identifier: str,
    gc_user_id: str,
    parsed_payload: dict[str, Any],
    current_groups: list[dict[str, Any]],
    target_groups: list[dict[str, Any]],
    backup_id: int | None,
    preview_text: str,
) -> None:
    with _gc_db() as db:
        db.execute(
            """
            INSERT INTO gc_access_requests(
                request_id,requester_chat_id,requester_user_id,command_text,identifier,
                gc_user_id,parsed_json,current_groups_json,target_groups_json,backup_id,
                status,preview_text,created_at,applied_at,apply_result_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?, 'pending', ?, ?, NULL, NULL)
            """,
            (
                request_id,
                requester_chat_id,
                requester_user_id,
                command_text,
                identifier,
                gc_user_id,
                _gc_json_dumps(parsed_payload),
                _gc_json_dumps(current_groups),
                _gc_json_dumps(target_groups),
                backup_id,
                preview_text,
                time.time(),
            ),
        )


def _gc_get_access_request(request_id: str) -> dict[str, Any] | None:
    with _gc_db() as db:
        row = db.execute("SELECT * FROM gc_access_requests WHERE request_id=? LIMIT 1", (str(request_id),)).fetchone()
    if row is None:
        return None
    return {
        "request_id": str(row["request_id"]),
        "requester_chat_id": row["requester_chat_id"],
        "requester_user_id": row["requester_user_id"],
        "command_text": str(row["command_text"]),
        "identifier": str(row["identifier"]),
        "gc_user_id": str(row["gc_user_id"]),
        "parsed_payload": _gc_json_loads(row["parsed_json"], {}),
        "current_groups": _gc_json_loads(row["current_groups_json"], []),
        "target_groups": _gc_json_loads(row["target_groups_json"], []),
        "backup_id": row["backup_id"],
        "status": str(row["status"]),
        "preview_text": row["preview_text"] or "",
        "created_at": float(row["created_at"]),
        "applied_at": float(row["applied_at"]) if row["applied_at"] is not None else None,
        "apply_result": _gc_json_loads(row["apply_result_json"], {}),
    }


def _gc_mark_request_status(request_id: str, *, status: str, apply_result: dict[str, Any] | None = None) -> None:
    applied_at = time.time() if status in {"applied", "failed", "cancelled"} else None
    with _gc_db() as db:
        db.execute(
            "UPDATE gc_access_requests SET status=?, applied_at=?, apply_result_json=? WHERE request_id=?",
            (status, applied_at, _gc_json_dumps(apply_result or {}), str(request_id)),
        )


def _gc_update_access_result(request_id: str, result: dict[str, Any]) -> None:
    with _gc_db() as db:
        db.execute(
            "UPDATE gc_access_requests SET apply_result_json=? WHERE request_id=?",
            (_gc_json_dumps(result), str(request_id)),
        )


class NexusGetCourseAccessService:
    EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
    PHONE_RE = re.compile(r"\+?\d[\d\s()\-]{8,}\d")
    GC_USER_URL_RE = re.compile(r"https?://[^\s/]+/(?:user/control/user/update|teach/control/stat/user)/id/(\d+)", re.IGNORECASE)
    BARE_ID_RE = re.compile(r"\b\d{6,}\b")
    COURSE_ALIASES = {
        "puppy": ("щ",),
        "dog": (),
        "mini_leash": ("поводок",),
        "mini_obedience": ("послушание",),
    }
    PACKAGE_ALIASES = {
        "standard": ("стандарт", "standard"),
        "premium": ("премиум", "premium"),
        "vip": ("vip", "вип"),
        "mentorship": ("наставничество", "личное наставничество"),
        "module_standard": ("помодульно",),
    }
    MODULE_RE = re.compile(r"(\d{1,2})(?:\s*[-–]?\s*(?:го|й|ый|ой))?\s*модул", re.IGNORECASE)
    RANGE_RE = re.compile(r"(?:с|от)\s*(\d{1,2})(?:-?го|-?й)?\s*(?:до|по)\s*(\d{1,2})(?:-?го|-?й)?", re.IGNORECASE)
    OPEN_RE = re.compile(r"\b(?:открой|открыть|добавь|добавить|дай|выдай|выдать|нужен|нужна|нужно|докин\w*)\b", re.IGNORECASE)
    REMOVE_RE = re.compile(r"\b(?:убери|убрать|закрой|закрыть|удали|удалить|сними|снять|забери|забрать)\b", re.IGNORECASE)

    def _catalog(self) -> list[dict[str, Any]]:
        with _gc_db() as db:
            rows = db.execute("SELECT raw_payload FROM gc_groups_catalog ORDER BY name").fetchall()
        return [_gc_json_loads(str(row["raw_payload"]), {}) for row in rows]

    def _extract_identifier(self, text: str) -> str:
        match = self.GC_USER_URL_RE.search(text or "")
        if match:
            return match.group(1)
        match = self.EMAIL_RE.search(text or "")
        if match:
            return match.group(0).lower()
        for phone_match in self.PHONE_RE.finditer(text or ""):
            phone = _normalize_phone(phone_match.group(0))
            if phone:
                return phone
        match = self.BARE_ID_RE.search(text or "")
        if match:
            return match.group(0)
        raise GetCourseAccessError("Не найден email, телефон или GetCourse ID в команде")

    def _detect_courses(self, lowered: str) -> list[str]:
        result: list[str] = []
        both = bool(re.search(r"\bщ\s*\+\s*с\b|\bс\s*\+\s*щ\b|щен\w*\s+(?:и|\+)\s+собак\w*|собак\w*\s+(?:и|\+)\s+щен\w*", lowered))
        if both:
            return ["puppy", "dog"]
        if re.search(r"\b(?:щен|шен)\w*\b", lowered):
            result.append("puppy")
        if re.search(r"\bсобак\w*\b", lowered):
            result.append("dog")
        for key, aliases in self.COURSE_ALIASES.items():
            if any(re.search(rf"(?<![\wа-яё]){re.escape(alias)}(?![\wа-яё])", lowered) for alias in aliases):
                if key not in result:
                    result.append(key)
        return result

    def _detect_package(self, lowered: str) -> str | None:
        for key, aliases in self.PACKAGE_ALIASES.items():
            if any(alias in lowered for alias in aliases):
                return key
        return None

    def _detect_modules(self, lowered: str, *, removing: bool) -> tuple[list[int], list[int], bool, bool]:
        all_modules = bool(re.search(r"\b(?:все|весь|полностью)\s+модул|\bполный\b|\bполностью\b", lowered))
        keep_only = bool(re.search(r"\b(?:оставь|оставить|только)\b", lowered))
        modules: set[int] = set()
        for match in self.RANGE_RE.finditer(lowered):
            start, end = int(match.group(1)), int(match.group(2))
            modules.update(range(min(start, end), max(start, end) + 1))
        for match in self.MODULE_RE.finditer(lowered):
            modules.add(int(match.group(1)))
        if re.search(r"\bстартов(?:ые|ой)\b", lowered):
            modules.update({0, 1, 2})
            keep_only = True
        cleaned = sorted(module for module in modules if 0 <= module <= 9)
        if removing:
            return [], cleaned, all_modules, keep_only
        return cleaned, [], all_modules, keep_only

    def parse_command(self, text: str) -> ParsedAccessCommand:
        lowered = _norm_text(text)
        identifier = self._extract_identifier(text)
        removing = bool(self.REMOVE_RE.search(lowered)) and not bool(self.OPEN_RE.search(lowered))
        add_modules, remove_modules, all_modules, keep_only = self._detect_modules(lowered, removing=removing)
        return ParsedAccessCommand(
            identifier=identifier,
            course_keys=self._detect_courses(lowered),
            package_key=self._detect_package(lowered),
            add_modules=add_modules,
            remove_modules=remove_modules,
            all_modules=all_modules,
            replace_managed_groups=bool(removing and not remove_modules),
            keep_only_modules=keep_only,
            raw_text=text.strip(),
        )

    def _snapshot_by_identifier(self, identifier: str) -> dict[str, Any] | None:
        phone = _normalize_phone(identifier)
        email = identifier.lower() if "@" in identifier else ""
        with _gc_db() as db:
            if identifier.isdigit():
                row = db.execute("SELECT * FROM gc_user_snapshots WHERE gc_user_id=? LIMIT 1", (identifier,)).fetchone()
                if row:
                    return self._snapshot_from_row(row)
            if email:
                row = db.execute("SELECT * FROM gc_user_snapshots WHERE lower(email)=? LIMIT 1", (email,)).fetchone()
                if row:
                    return self._snapshot_from_row(row)
            if phone:
                candidates = {phone, phone.replace("+", ""), "8" + phone[2:]}
                rows = db.execute("SELECT * FROM gc_user_snapshots").fetchall()
                matches = []
                for row in rows:
                    if _normalize_phone(row["phone"]) in candidates or _phone_digits(row["phone"]) in {_phone_digits(item) for item in candidates}:
                        matches.append(row)
                if len(matches) == 1:
                    return self._snapshot_from_row(matches[0])
        return None

    def _snapshot_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "gc_user_id": str(row["gc_user_id"]),
            "email": row["email"],
            "phone": row["phone"],
            "full_name": row["full_name"],
            "groups": _gc_json_loads(row["groups_json"], []),
            "utms": _gc_json_loads(row["utms_json"], {}),
            "raw_user": _gc_json_loads(row["raw_user_json"], {}),
        }

    def _resolve_user(self, identifier: str) -> dict[str, Any]:
        snapshot = self._snapshot_by_identifier(identifier)
        if snapshot:
            return snapshot
        raise GetCourseAccessError("Пользователя в локальной базе GetCourse не нашел. Нужен live lookup GetCourse или другой идентификатор.")

    def _infer_courses(self, current_groups: list[dict[str, Any]], explicit: list[str]) -> list[str]:
        if explicit:
            return explicit
        found = []
        for group in current_groups:
            key = str(group.get("course_key") or "").strip()
            if key in {"puppy", "dog"} and key not in found:
                found.append(key)
        if len(found) == 1:
            return found
        if not found:
            raise GetCourseAccessError("Не удалось определить курс. Укажи Щенок, Собака или щ+с.")
        raise GetCourseAccessError("Вижу несколько курсов. Укажи Щенок, Собака или щ+с.")

    def _find_group(self, catalog: list[dict[str, Any]], *, course_key: str, kind: str, package_key: str | None = None, module_index: int | None = None) -> dict[str, Any] | None:
        for group in catalog:
            if str(group.get("course_key") or "").strip() != course_key:
                continue
            if str(group.get("group_kind") or "").strip() != kind:
                continue
            if package_key is not None and str(group.get("package_key") or "").strip() != package_key:
                continue
            if module_index is not None and int(group.get("module_index") if group.get("module_index") is not None else -1) != int(module_index):
                continue
            return dict(group)
        return None

    def _build_target_groups(self, *, current_groups: list[dict[str, Any]], parsed: ParsedAccessCommand, catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
        target = [dict(group) for group in current_groups if isinstance(group, dict)]
        target_by_name = {str(group.get("name") or "").strip(): dict(group) for group in target if str(group.get("name") or "").strip()}
        courses = self._infer_courses(target, parsed.course_keys)
        if parsed.replace_managed_groups or parsed.keep_only_modules:
            remove_courses = set(courses)
            target_by_name = {
                name: group
                for name, group in target_by_name.items()
                if not (group.get("managed") and str(group.get("course_key") or "") in remove_courses)
            }
        if parsed.remove_modules:
            remove_modules = set(parsed.remove_modules)
            target_by_name = {
                name: group
                for name, group in target_by_name.items()
                if not (
                    str(group.get("course_key") or "") in courses
                    and str(group.get("group_kind") or "") == "module"
                    and int(group.get("module_index") if group.get("module_index") is not None else -1) in remove_modules
                )
            }
        for course_key in courses:
            for kind in ("root", "bridge"):
                group = self._find_group(catalog, course_key=course_key, kind=kind)
                if group:
                    target_by_name[str(group["name"])] = group
            if parsed.package_key:
                group = self._find_group(catalog, course_key=course_key, kind="package", package_key=parsed.package_key)
                if group:
                    target_by_name[str(group["name"])] = group
                else:
                    raise GetCourseAccessError(f"Не нашел пакет {parsed.package_key} для курса {course_key}")
            modules = parsed.add_modules
            if parsed.all_modules:
                modules = sorted(
                    int(group["module_index"])
                    for group in catalog
                    if str(group.get("course_key") or "") == course_key
                    and str(group.get("group_kind") or "") == "module"
                    and group.get("module_index") is not None
                )
            for module_index in modules:
                group = self._find_group(catalog, course_key=course_key, kind="module", module_index=int(module_index))
                if not group:
                    raise GetCourseAccessError(f"Не нашел {module_index} модуль для курса {course_key}")
                target_by_name[str(group["name"])] = group
        return list(target_by_name.values())

    def _format_preview(self, *, user: dict[str, Any], target_groups: list[dict[str, Any]]) -> str:
        current_names = [str(group.get("name") or "").strip() for group in user.get("groups") or [] if str(group.get("name") or "").strip()]
        target_names = [str(group.get("name") or "").strip() for group in target_groups if str(group.get("name") or "").strip()]
        before, after = set(current_names), set(target_names)
        added = [name for name in target_names if name not in before]
        removed = [name for name in current_names if name not in after]
        def bullets(values: list[str]) -> list[str]:
            return [f"• {value}" for value in values[:24]] or ["• ничего"]
        return "\n".join(
            [
                "Нашел клиента и собрал изменения.",
                "",
                f"Имя: {user.get('full_name') or 'Без имени'}",
                f"Почта: {user.get('email') or '—'}",
                f"Телефон: {user.get('phone') or '—'}",
                f"Профиль: https://club.sobakovod.pro/user/control/user/update/id/{user.get('gc_user_id')}",
                "",
                "Добавить:",
                *bullets(added),
                "",
                "Убрать:",
                *bullets(removed),
            ]
        )

    def prepare_access_request(self, *, command_text: str, requester_chat_id: str | None = None, requester_user_id: str | None = None) -> dict[str, Any]:
        parsed = self.parse_command(command_text)
        user = self._resolve_user(parsed.identifier)
        catalog = self._catalog()
        target_groups = self._build_target_groups(current_groups=user.get("groups") or [], parsed=parsed, catalog=catalog)
        request_id = uuid.uuid4().hex[:12]
        backup_id = _gc_create_group_backup(gc_user_id=user["gc_user_id"], source_text=command_text, groups=user.get("groups") or [])
        preview_text = self._format_preview(user=user, target_groups=target_groups)
        parsed_payload = {
            "identifier": parsed.identifier,
            "course_keys": parsed.course_keys,
            "package_key": parsed.package_key,
            "add_modules": parsed.add_modules,
            "remove_modules": parsed.remove_modules,
            "all_modules": parsed.all_modules,
            "replace_managed_groups": parsed.replace_managed_groups,
            "keep_only_modules": parsed.keep_only_modules,
            "parser": "nexus-regex",
        }
        _gc_create_access_request(
            request_id=request_id,
            requester_chat_id=requester_chat_id,
            requester_user_id=requester_user_id,
            command_text=command_text,
            identifier=parsed.identifier,
            gc_user_id=user["gc_user_id"],
            parsed_payload=parsed_payload,
            current_groups=user.get("groups") or [],
            target_groups=target_groups,
            backup_id=backup_id,
            preview_text=preview_text,
        )
        return {
            "request_id": request_id,
            "preview_text": preview_text,
            "gc_user_id": user["gc_user_id"],
            "current_groups": user.get("groups") or [],
            "target_groups": target_groups,
            "user": user,
            "parsed_payload": parsed_payload,
        }

    def describe_access_state(self, *, command_text: str) -> dict[str, Any]:
        identifier = self._extract_identifier(command_text)
        user = self._resolve_user(identifier)
        names = [
            str(group.get("name") or "").strip()
            for group in user.get("groups") or []
            if group.get("managed") and str(group.get("name") or "").strip()
        ]
        summary = "Открытые управляемые группы:\n" + "\n".join(f"• {name}" for name in names) if names else "У пользователя нет управляемых групп Щенка или Собаки."
        return {"gc_user_id": user.get("gc_user_id"), "user": user, "summary_text": summary}

    def _update_user_groups_live(self, *, gc_user_id: str, group_names: list[str]) -> dict[str, Any]:
        account = _secret_value("getcourse_account_name")
        token = _secret_value("getcourse_api_token")
        if not account or not token:
            raise GetCourseAccessError("GetCourse credentials are not configured")
        payload = {
            "action": "update",
            "key": token,
            "params": base64.b64encode(
                json.dumps({"user": {"id": str(gc_user_id), "group_name": group_names}}, ensure_ascii=False).encode("utf-8")
            ).decode("ascii"),
        }
        with httpx.Client(timeout=30) as client:
            response = client.post(f"https://{account}.getcourse.ru/pl/api/users", data=payload)
        if response.status_code == 429:
            raise GetCourseAccessError("Слишком много запросов")
        response.raise_for_status()
        data = response.json()
        result = data.get("result") if isinstance(data.get("result"), dict) else {}
        if not data.get("success") or result.get("error") or result.get("success") is False:
            raise GetCourseAccessError(str(result.get("error_message") or data.get("error_message") or "GetCourse update error"))
        return data

    def apply_access_request(self, request_id: str) -> dict[str, Any]:
        request = _gc_get_access_request(request_id)
        if request is None:
            raise GetCourseAccessError("Запрос не найден")
        if request["status"] != "pending":
            raise GetCourseAccessError(f"Запрос уже обработан: {request['status']}")
        is_test_period = (request.get("parsed_payload") or {}).get("source") == "student-transfer-test-period"
        if request.get("command_text") == "streams-access" and not is_test_period:
            catalog = self._catalog()
            roots = {
                str(group.get("name") or "").strip(): str(group.get("course_key") or "")
                for group in catalog
                if group.get("managed") and group.get("group_kind") == "root"
            }
            by_id = {str(group.get("group_id") or ""): group for group in catalog}
            current_roots = {str(group.get("name") or "").strip() for group in request["current_groups"]} & set(roots)
            target_roots = {str(group.get("name") or "").strip() for group in request["target_groups"]} & set(roots)
            changed_courses = {roots[name] for name in current_roots ^ target_roots}
            if changed_courses:
                _gc_mark_request_status(request_id, status="cancelled", apply_result={"error": "obsolete-root-change"})
                raise GetCourseAccessError("Проверка устарела. Выберите доступы заново")
        group_names = [str(group.get("name") or "").strip() for group in request["target_groups"] if str(group.get("name") or "").strip()]
        if _truthy(_settings().get("dry_run")):
            result = {"dry_run": True, "group_names": group_names}
        else:
            result = self._update_user_groups_live(gc_user_id=request["gc_user_id"], group_names=group_names)
        _gc_mark_request_status(request_id, status="applied", apply_result=result)
        return result

    def cancel_access_request(self, request_id: str) -> None:
        request = _gc_get_access_request(request_id)
        if request is None:
            raise GetCourseAccessError("Запрос не найден")
        _gc_mark_request_status(request_id, status="cancelled", apply_result={"cancelled": True})


MINI_ACCESS_GROUPS: tuple[dict[str, Any], ...] = (
    {"group_id": 4842617, "name": "Мини-курс «Намордник». Тариф «Самостоятельно»", "course_key": "mini_muzzle", "group_kind": "mini", "managed": True},
    {"group_id": 4842619, "name": "Мини-курс «Намордник». Тариф «С проверкой и поддержкой»", "course_key": "mini_muzzle", "group_kind": "mini", "managed": True},
    {"group_id": 4119459, "name": "Мини-курс \"Поводок\"", "course_key": "mini_leash", "group_kind": "mini", "managed": True},
    {"group_id": 4217019, "name": "Мини-курс \"Послушание\"", "course_key": "mini_obedience", "group_kind": "mini", "managed": True},
    {"group_id": 4443745, "name": "Послушная собака за 15 минут в день", "course_key": "mini_15", "group_kind": "mini", "managed": True},
)
ACCESS_COURSE_KEYS = {"puppy", "dog", "mini_muzzle", "mini_leash", "mini_obedience", "mini_15"}
TEST_PERIOD_GROUP_NAMES = {
    "puppy": "Тест-драйв. Щенок",
    "dog": "Тест-драйв. Собака",
    "used": "Использовал тестовый период",
    **{f"module_{index}": f"{index} Модуль.Тестовый период" for index in range(1, 9)},
}


def _access_catalog_with_minis(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {str(item.get("group_id") or ""): dict(item) for item in items}
    for configured in MINI_ACCESS_GROUPS:
        group_id = str(configured["group_id"])
        merged[group_id] = {**merged.get(group_id, {}), **configured}
    return list(merged.values())


def service_access_catalog() -> dict[str, Any]:
    items = _access_catalog_with_minis(NexusGetCourseAccessService()._catalog())
    return {
        "ok": True,
        "items": [
            item
            for item in items
            if item.get("managed") and str(item.get("course_key") or "") in ACCESS_COURSE_KEYS
        ],
    }


def service_resolve_access_user(*, gc_user_id: str = "", email: str = "", phone: str = "") -> dict[str, Any]:
    """Resolve one exact GetCourse user from the maintained access snapshot index."""
    service = NexusGetCourseAccessService()
    for identifier in (str(gc_user_id or "").strip(), str(email or "").strip().lower(), str(phone or "").strip()):
        if not identifier:
            continue
        snapshot = service._snapshot_by_identifier(identifier)
        if snapshot:
            return {"ok": True, "found": True, **snapshot}
    return {"ok": True, "found": False}


def service_test_period_catalog() -> dict[str, Any]:
    """Resolve the small, explicit GetCourse allow-list used by Streams trials."""
    catalog = _access_catalog_with_minis(NexusGetCourseAccessService()._catalog())
    by_name = {str(item.get("name") or "").strip(): dict(item) for item in catalog}
    items: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for key, name in TEST_PERIOD_GROUP_NAMES.items():
        item = by_name.get(name)
        if not item or not str(item.get("group_id") or "").strip():
            missing.append(name)
            continue
        items[key] = {**item, "group_id": int(item["group_id"]), "name": name}
    return {"ok": not missing, "items": items, "missing": missing}


def service_prepare_test_period_change(
    *,
    gc_user_id: str,
    email: str,
    current_groups: list[dict[str, Any]],
    changes: list[dict[str, Any]],
    requester_user_id: str,
) -> dict[str, Any]:
    """Prepare an idempotent access change limited to the trial-only groups."""
    resolved = service_test_period_catalog()
    if not resolved["ok"]:
        raise GetCourseAccessError("Не найдены группы тестового периода: " + ", ".join(resolved["missing"]))
    allowed = {
        str(item["group_id"]): dict(item)
        for item in resolved["items"].values()
    }
    catalog = _access_catalog_with_minis(NexusGetCourseAccessService()._catalog())
    by_id = {str(item.get("group_id") or ""): dict(item) for item in catalog}
    current: list[dict[str, Any]] = []
    for raw in current_groups:
        group_id = str(raw.get("group_id") or "").strip()
        name = str(raw.get("name") or "").strip()
        if not group_id or not name:
            raise GetCourseAccessError("Группа GetCourse без ID или имени; изменение отменено")
        current.append({**dict(raw), **by_id.get(group_id, {}), "group_id": int(group_id), "name": name})
    target = {str(item["group_id"]): dict(item) for item in current}
    normalized_changes: list[dict[str, Any]] = []
    for raw in changes[:16]:
        group_id = str(raw.get("group_id") or "").strip()
        group = allowed.get(group_id)
        if not group:
            raise GetCourseAccessError("Недоступная группа тестового периода")
        enabled = bool(raw.get("enabled"))
        normalized_changes.append({"group_id": group_id, "enabled": enabled})
        if enabled:
            target[group_id] = dict(group)
        else:
            target.pop(group_id, None)
    if not normalized_changes:
        raise GetCourseAccessError("Нет изменений")
    current_names = {str(item.get("name") or "") for item in current}
    target_groups = list(target.values())
    target_names = {str(item.get("name") or "") for item in target_groups}
    added = sorted(target_names - current_names)
    removed = sorted(current_names - target_names)
    if not added and not removed:
        raise GetCourseAccessError("Доступы уже установлены")
    request_id = uuid.uuid4().hex[:12]
    backup_id = _gc_create_group_backup(gc_user_id=gc_user_id, source_text="streams-test-period", groups=current)
    _gc_create_access_request(
        request_id=request_id,
        requester_chat_id="streams",
        requester_user_id=requester_user_id,
        command_text="streams-access",
        identifier=email or gc_user_id,
        gc_user_id=gc_user_id,
        parsed_payload={"changes": normalized_changes, "source": "student-transfer-test-period"},
        current_groups=current,
        target_groups=target_groups,
        backup_id=backup_id,
        preview_text="",
    )
    return {
        "ok": True,
        "request_id": request_id,
        "added": added,
        "removed": removed,
        "current_groups": current,
        "target_groups": target_groups,
        "expires_in": GETCOURSE_PENDING_TIMEOUT_SECONDS,
    }


def service_prepare_access_change(
    *,
    gc_user_id: str,
    email: str,
    current_groups: list[dict[str, Any]],
    changes: list[dict[str, Any]],
    requester_user_id: str,
) -> dict[str, Any]:
    service = NexusGetCourseAccessService()
    catalog = _access_catalog_with_minis(service._catalog())
    by_id = {str(item.get("group_id") or ""): dict(item) for item in catalog}
    current: list[dict[str, Any]] = []
    for raw in current_groups:
        group_id = str(raw.get("group_id") or "").strip()
        name = str(raw.get("name") or "").strip()
        if not group_id or not name:
            raise GetCourseAccessError("Группа GetCourse без ID или имени; изменение отменено")
        current.append({**dict(raw), **by_id.get(group_id, {}), "group_id": int(group_id), "name": name})
    target = {str(item["group_id"]): dict(item) for item in current}

    normalized_changes: list[dict[str, Any]] = []
    touched_courses: set[str] = set()
    for raw in changes[:40]:
        group_id = str(raw.get("group_id") or "").strip()
        group = by_id.get(group_id)
        if not group or not group.get("managed") or group.get("course_key") not in ACCESS_COURSE_KEYS:
            raise GetCourseAccessError("Недоступная группа GetCourse")
        enabled = bool(raw.get("enabled"))
        course_key = str(group.get("course_key"))
        kind = str(group.get("group_kind") or "")
        if kind == "root":
            raise GetCourseAccessError("Вводная группа не управляется здесь")
        touched_courses.add(course_key)
        normalized_changes.append({"group_id": group_id, "enabled": enabled})
        if enabled:
            if kind == "package":
                target = {
                    key: value
                    for key, value in target.items()
                    if not (
                        value.get("managed")
                        and value.get("course_key") == course_key
                        and value.get("group_kind") == "package"
                    )
                }
            if kind == "mini":
                target = {
                    key: value
                    for key, value in target.items()
                    if not (
                        value.get("managed")
                        and value.get("course_key") == course_key
                        and value.get("group_kind") == "mini"
                    )
                }
            target[group_id] = dict(group)
        else:
            target.pop(group_id, None)

    for course_key in touched_courses:
        all_course_groups = [
            group for group in target.values()
            if group.get("course_key") == course_key
        ]
        course_groups = [
            group for group in target.values()
            if group.get("managed") and group.get("course_key") == course_key
        ]
        if not course_groups:
            continue
        entitlement_marker = any(
            (
                "частичн" in _norm_text(group.get("name"))
                and "оплат" in _norm_text(group.get("name"))
            )
            or "автооплат" in _norm_text(group.get("name"))
            for group in all_course_groups
        )
        if (
            course_key in {"puppy", "dog"}
            and not any(group.get("group_kind") == "package" for group in course_groups)
            and not entitlement_marker
        ):
            bridge = service._find_group(catalog, course_key=course_key, kind="bridge")
            if bridge:
                target[str(bridge["group_id"])] = dict(bridge)

    if not normalized_changes:
        raise GetCourseAccessError("Нет изменений")
    current_names = {str(item.get("name") or "") for item in current}
    target_groups = list(target.values())
    target_names = {str(item.get("name") or "") for item in target_groups}
    added = sorted(target_names - current_names)
    removed = sorted(current_names - target_names)
    if not added and not removed:
        raise GetCourseAccessError("Доступы уже установлены")
    request_id = uuid.uuid4().hex[:12]
    backup_id = _gc_create_group_backup(gc_user_id=gc_user_id, source_text="streams-access", groups=current)
    _gc_create_access_request(
        request_id=request_id,
        requester_chat_id="streams",
        requester_user_id=requester_user_id,
        command_text="streams-access",
        identifier=email or gc_user_id,
        gc_user_id=gc_user_id,
        parsed_payload={"changes": normalized_changes, "source": "student-transfer"},
        current_groups=current,
        target_groups=target_groups,
        backup_id=backup_id,
        preview_text="",
    )
    return {
        "ok": True,
        "request_id": request_id,
        "added": added,
        "removed": removed,
        "current_groups": current,
        "target_groups": target_groups,
        "expires_in": GETCOURSE_PENDING_TIMEOUT_SECONDS,
    }


def service_apply_access_change(*, request_id: str, requester_user_id: str) -> dict[str, Any]:
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    if str(request.get("requester_user_id") or "") != str(requester_user_id or ""):
        raise GetCourseAccessError("Запрос создан другим оператором")
    if (
        time.time() - float(request.get("created_at") or 0) > GETCOURSE_PENDING_TIMEOUT_SECONDS
        and not bool((request.get("apply_result") or {}).get("apply_queued"))
    ):
        _gc_mark_request_status(request_id, status="cancelled", apply_result={"error": "expired"})
        raise GetCourseAccessError("Проверка устарела")
    result = NexusGetCourseAccessService().apply_access_request(request_id)
    return {"ok": True, "result": result, "request": _gc_get_access_request(request_id)}


def service_schedule_access_apply(
    *, request_id: str, requester_user_id: str, delay_seconds: int = 2
) -> dict[str, Any]:
    """Persist an access write for the Streams worker and return immediately."""
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    if str(request.get("requester_user_id") or "") != str(requester_user_id or ""):
        raise GetCourseAccessError("Запрос создан другим оператором")
    if request["status"] != "pending":
        raise GetCourseAccessError(f"Запрос уже обработан: {request['status']}")
    if time.time() - float(request.get("created_at") or 0) > GETCOURSE_PENDING_TIMEOUT_SECONDS:
        _gc_mark_request_status(request_id, status="cancelled", apply_result={"error": "expired"})
        raise GetCourseAccessError("Проверка устарела")
    now = time.time()
    next_at = now + max(1, min(int(delay_seconds), 300))
    ready_by = max(next_at + 5 * 60, now + 5 * 60)
    previous = request.get("apply_result") or {}
    result = {
        **previous,
        "apply_queued": True,
        "apply_attempts": int(previous.get("apply_attempts") or 0),
        "apply_next_at": next_at,
        "apply_ready_by": ready_by,
        "apply_error": "",
    }
    _gc_update_access_result(request_id, result)
    return {
        "ok": True,
        "queued": True,
        "request_id": request_id,
        "target_groups": request.get("target_groups") or [],
        "next_check_at": datetime.fromtimestamp(next_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ready_by": datetime.fromtimestamp(ready_by, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def service_pending_access_applies(*, limit: int = 1) -> dict[str, Any]:
    now = time.time()
    bounded_limit = max(1, min(int(limit), 10))
    with _gc_db() as db:
        rows = db.execute(
            """
            SELECT request_id FROM gc_access_requests
            WHERE command_text='streams-access' AND status='pending'
              AND COALESCE(json_extract(apply_result_json,'$.apply_queued'),0)=1
              AND COALESCE(json_extract(apply_result_json,'$.apply_next_at'),0)<=?
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, bounded_limit),
        ).fetchall()
    return {
        "ok": True,
        "items": [
            item for row in rows
            if (item := _gc_get_access_request(str(row["request_id"]))) is not None
        ],
    }


def service_retry_access_apply(*, request_id: str, delay_seconds: int, error: str) -> dict[str, Any]:
    request = _gc_get_access_request(request_id)
    if request is None or request.get("status") != "pending":
        raise GetCourseAccessError("Запрос на изменение доступов уже закрыт")
    previous = request.get("apply_result") or {}
    attempts = int(previous.get("apply_attempts") or 0) + 1
    next_at = time.time() + max(15, min(int(delay_seconds), 6 * 3600))
    ready_by = next_at + 5 * 60
    _gc_update_access_result(request_id, {
        **previous,
        "apply_queued": True,
        "apply_attempts": attempts,
        "apply_next_at": next_at,
        "apply_ready_by": ready_by,
        "apply_error": str(error or "")[:500],
    })
    return {
        "ok": True,
        "attempts": attempts,
        "next_check_at": datetime.fromtimestamp(next_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ready_by": datetime.fromtimestamp(ready_by, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def service_fail_access_apply(*, request_id: str, error: str) -> dict[str, Any]:
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    result = {**(request.get("apply_result") or {}), "apply_queued": False, "apply_error": str(error or "")[:500]}
    _gc_mark_request_status(request_id, status="failed", apply_result=result)
    return {"ok": True, "request_id": request_id}


def service_access_change_request(*, request_id: str, requester_user_id: str) -> dict[str, Any]:
    """Return the identity bound to a prepared request before any write occurs."""
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    if str(request.get("requester_user_id") or "") != str(requester_user_id or ""):
        raise GetCourseAccessError("Запрос создан другим оператором")
    return {
        "ok": True,
        "request_id": str(request.get("request_id") or ""),
        "gc_user_id": str(request.get("gc_user_id") or ""),
        "identifier": str(request.get("identifier") or ""),
        "target_groups": request.get("target_groups") or [],
        "status": str(request.get("status") or ""),
        "created_at": float(request.get("created_at") or 0),
    }


def service_schedule_access_verification(*, request_id: str, delay_seconds: int = 60, error: str = "") -> dict[str, Any]:
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    if request["status"] != "applied":
        raise GetCourseAccessError("Изменение не применено")
    previous = request.get("apply_result") or {}
    attempts = int(previous.get("verification_attempts") or 0)
    if error:
        attempts += 1
    next_check_at = time.time() + max(15, min(int(delay_seconds), 6 * 3600))
    result = {
        **previous,
        "verification_pending": True,
        "verification_attempts": attempts,
        "verification_next_at": next_check_at,
        "verification_error": str(error or "")[:500],
    }
    _gc_update_access_result(request_id, result)
    return {
        "ok": True,
        "request_id": request_id,
        "attempts": attempts,
        "next_check_at": datetime.fromtimestamp(next_check_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def service_pending_access_verifications(*, limit: int = 1) -> dict[str, Any]:
    now = time.time()
    bounded_limit = max(1, min(int(limit), 10))
    with _gc_db() as db:
        rows = db.execute(
            """
            SELECT request_id FROM gc_access_requests
            WHERE command_text='streams-access' AND status='applied'
              AND COALESCE(json_extract(apply_result_json,'$.verification_pending'),0)=1
              AND COALESCE(json_extract(apply_result_json,'$.verification_next_at'),0)<=?
            ORDER BY applied_at ASC
            LIMIT ?
            """,
            (now, bounded_limit),
        ).fetchall()
    return {"ok": True, "items": [_gc_get_access_request(str(row["request_id"])) for row in rows]}


def service_access_verifications(*, limit: int = 50, request_id: str = "") -> dict[str, Any]:
    """Return queued and completed Streams access changes for the operations journal."""
    bounded_limit = max(1, min(int(limit), 200))
    params: list[Any] = []
    clauses = [
        "command_text='streams-access'",
        "(status IN ('applied','failed') OR (status='pending' AND COALESCE(json_extract(apply_result_json,'$.apply_queued'),0)=1))",
    ]
    if str(request_id or "").strip():
        clauses.append("request_id=?")
        params.append(str(request_id).strip())
    params.append(bounded_limit)
    with _gc_db() as db:
        rows = db.execute(
            f"""
            SELECT request_id FROM gc_access_requests
            WHERE {' AND '.join(clauses)}
            ORDER BY COALESCE(applied_at,created_at) DESC, created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return {
        "ok": True,
        "items": [
            item
            for row in rows
            if (item := _gc_get_access_request(str(row["request_id"]))) is not None
        ],
    }


def service_latest_access_verification(*, gc_user_id: str = "", email: str = "") -> dict[str, Any]:
    params: list[Any] = []
    clauses = [
        "command_text='streams-access'",
        "((status='applied' AND COALESCE(json_extract(apply_result_json,'$.verification_pending'),0)=1) "
        "OR (status='pending' AND COALESCE(json_extract(apply_result_json,'$.apply_queued'),0)=1))",
    ]
    if str(gc_user_id or "").strip():
        clauses.append("gc_user_id=?")
        params.append(str(gc_user_id).strip())
    elif str(email or "").strip():
        clauses.append("lower(identifier)=lower(?)")
        params.append(str(email).strip())
    else:
        return {"ok": True, "pending": False}
    with _gc_db() as db:
        row = db.execute(
            f"SELECT request_id FROM gc_access_requests WHERE {' AND '.join(clauses)} "
            "ORDER BY COALESCE(applied_at,created_at) DESC LIMIT 1",
            params,
        ).fetchone()
    request = _gc_get_access_request(str(row["request_id"])) if row else None
    result = (request or {}).get("apply_result") or {}
    if not request:
        return {"ok": True, "pending": False}
    applying = request.get("status") == "pending"
    next_at = float((result.get("apply_next_at") if applying else result.get("verification_next_at")) or 0)
    ready_by = float(result.get("apply_ready_by") or 0)
    return {
        "ok": True,
        "pending": True,
        "stage": "queued" if applying else "verifying",
        "request_id": request["request_id"],
        "target_groups": request["target_groups"],
        "attempts": int((result.get("apply_attempts") if applying else result.get("verification_attempts")) or 0),
        "next_check_at": datetime.fromtimestamp(next_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if next_at else "",
        "ready_by": datetime.fromtimestamp(ready_by, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ready_by else "",
    }


def service_record_access_verification(
    *, request_id: str, actual_groups: list[dict[str, Any]], defer_on_mismatch: bool = False
) -> dict[str, Any]:
    request = _gc_get_access_request(request_id)
    if request is None:
        raise GetCourseAccessError("Запрос не найден")
    expected = {str(item.get("name") or "").strip() for item in request["target_groups"] if str(item.get("name") or "").strip()}
    actual = {str(item.get("name") or "").strip() for item in actual_groups if str(item.get("name") or "").strip()}
    verification = {
        "verified": expected == actual,
        "missing": sorted(expected - actual),
        "unexpected": sorted(actual - expected),
        "actual_groups": actual_groups,
    }
    result = {
        **(request.get("apply_result") or {}),
        "verification": verification,
        "verification_pending": bool(defer_on_mismatch and not verification["verified"]),
        "verification_error": "",
    }
    if defer_on_mismatch and not verification["verified"]:
        _gc_update_access_result(request_id, result)
    else:
        _gc_mark_request_status(request_id, status="applied" if verification["verified"] else "failed", apply_result=result)
    return verification


def service_cancel_access_change(*, request_id: str) -> dict[str, Any]:
    NexusGetCourseAccessService().cancel_access_request(request_id)
    return {"ok": True}


def _int_set_csv(value: Any) -> set[int]:
    result: set[int] = set()
    for part in re.split(r"[\s,;]+", str(value or "")):
        if not part:
            continue
        try:
            result.add(int(part))
        except Exception:
            continue
    return result


def _vk_member_is_admin(member: dict[str, Any]) -> bool:
    role = _clean(member.get("role") or member.get("member_role"), 40).lower()
    return bool(
        member.get("is_admin")
        or member.get("is_owner")
        or member.get("rank") == 100
        or role in {"admin", "administrator", "creator", "owner"}
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return int(default)


async def service_refund_chat_membership(
    *, platform: str, user_id: str, dry_run: bool = True,
) -> dict[str, Any]:
    """Find/remove one exact numeric account in all managed school chats.

    Fuzzy identifiers are rejected and administrators are never removed.
    """
    normalized_platform = _clean(platform, 20).lower()
    normalized_user_id = _clean(user_id, 30)
    if normalized_platform not in {"vk", "telegram"}:
        raise ValueError("platform must be vk or telegram")
    if not normalized_user_id.isdigit() or int(normalized_user_id) <= 0:
        raise ValueError("user_id must be an exact positive numeric platform ID")
    with _db() as db:
        chats = [dict(row) for row in db.execute(
            """SELECT id,platform,chat_id,peer_id,title,zone FROM managed_chats
               WHERE platform=? AND enabled=1
                 AND zone IN ('training_stream','closed_club')
               ORDER BY id""",
            (normalized_platform,),
        ).fetchall()]
    result: dict[str, Any] = {
        "ok": True, "platform": normalized_platform, "user_id": normalized_user_id,
        "dry_run": bool(dry_run), "checked": 0, "found": [], "removed": [],
        "admin_skipped": [], "errors": [],
    }
    runtime = _runtime_or_error()
    if normalized_platform == "telegram":
        app = runtime.telegram.app
        if not runtime.telegram.running or app is None:
            return {**result, "ok": False, "status": "runtime_not_running"}
        bot = app.bot
        for chat in chats:
            chat_id = _safe_int(chat.get("chat_id"), 0)
            if not chat_id:
                continue
            result["checked"] += 1
            try:
                member = await bot.get_chat_member(chat_id=chat_id, user_id=int(normalized_user_id))
                status = _clean(getattr(member, "status", ""), 30).lower()
                if status in {"left", "kicked"}:
                    continue
                item = {"chat_id": str(chat_id), "title": _clean(chat.get("title"), 500), "zone": _clean(chat.get("zone"), 80)}
                if status in {"administrator", "creator", "owner"}:
                    result["admin_skipped"].append(item)
                    continue
                result["found"].append(item)
                if dry_run:
                    continue
                await bot.ban_chat_member(chat_id=chat_id, user_id=int(normalized_user_id))
                await bot.unban_chat_member(chat_id=chat_id, user_id=int(normalized_user_id), only_if_banned=True)
                verify = await bot.get_chat_member(chat_id=chat_id, user_id=int(normalized_user_id))
                if _clean(getattr(verify, "status", ""), 30).lower() in {"left", "kicked"}:
                    result["removed"].append(item)
                else:
                    result["errors"].append({**item, "error": "membership verification failed"})
            except Exception as exc:
                message = str(exc)
                if "not found" not in message.lower() and "participant_id_invalid" not in message.lower():
                    result["errors"].append({"chat_id": str(chat_id), "title": _clean(chat.get("title"), 500), "error": message[:500]})
    else:
        vk_runtime = runtime.vk
        if not vk_runtime.subscription or not vk_runtime.subscription.running:
            return {**result, "ok": False, "status": "runtime_not_running"}
        user_number = int(normalized_user_id)
        for chat in chats:
            peer_id = _safe_int(chat.get("peer_id"), 0)
            if peer_id <= VK_CHAT_PEER_BASE:
                continue
            result["checked"] += 1
            item = {"peer_id": str(peer_id), "title": _clean(chat.get("title"), 500), "zone": _clean(chat.get("zone"), 80)}
            try:
                members = await asyncio.to_thread(
                    vk_runtime._vk_for_peer(peer_id).messages.getConversationMembers,
                    peer_id=peer_id,
                )
                member = next((row for row in members.get("items", []) if _safe_int(row.get("member_id"), 0) == user_number), None)
                if member is None:
                    continue
                if _vk_member_is_admin(member):
                    result["admin_skipped"].append(item)
                    continue
                result["found"].append(item)
                if dry_run:
                    continue
                await asyncio.to_thread(vk_runtime._pace_mutating_action)
                await asyncio.to_thread(
                    vk_runtime._vk_for_peer(peer_id).messages.removeChatUser,
                    chat_id=peer_id - VK_CHAT_PEER_BASE,
                    member_id=user_number,
                )
                verify = await asyncio.to_thread(
                    vk_runtime._vk_for_peer(peer_id).messages.getConversationMembers,
                    peer_id=peer_id,
                )
                present = any(_safe_int(row.get("member_id"), 0) == user_number for row in verify.get("items", []))
                if present:
                    result["errors"].append({**item, "error": "membership verification failed"})
                else:
                    result["removed"].append(item)
            except Exception as exc:
                result["errors"].append({**item, "error": str(exc)[:500]})
    result["ok"] = not result["errors"]
    result["status"] = "preview" if dry_run else ("removed" if result["removed"] else "not_member")
    _record_action(
        platform=normalized_platform, action="refund_chat_removal",
        status="ok" if result["ok"] else "error", user_id=normalized_user_id,
        request_json={"dry_run": bool(dry_run)}, response_json={
            "checked": result["checked"], "found": len(result["found"]),
            "removed": len(result["removed"]), "admin_skipped": len(result["admin_skipped"]),
            "errors": result["errors"],
        },
    )
    return result


def _senler_course_chat_config() -> dict[str, str] | None:
    token = _clean(os.environ.get("SENLER_ACCESS_TOKEN"))
    group_id = _clean(os.environ.get("SENLER_GROUP_ID"))
    subscription_id = _clean(
        os.environ.get("SENLER_COURSE_CHAT_SUBSCRIPTION_ID") or SENLER_COURSE_CHAT_SUBSCRIPTION_ID
    )
    if not token or not group_id.isdigit() or not subscription_id.isdigit():
        return None
    return {"token": token, "group_id": group_id, "subscription_id": subscription_id}


def _senler_chat_is_eligible(zone: str, title: str) -> bool:
    if zone not in {"training_stream", "closed_club"}:
        return False
    value = str(title or "")
    stream = re.match(r"^\s*(\d+)\.", value)
    date = re.search(r"\b\d{2}\.\d{2}\.(\d{4})\b", value)
    return (stream is None or int(stream.group(1)) < 900) and (
        date is None or int(date.group(1)) >= 2020
    )


async def _senler_add_course_chat_users(vk_user_ids: list[int] | set[int]) -> dict[str, Any]:
    ids = sorted({int(value) for value in vk_user_ids if int(value) > 0})
    if not ids:
        return {"ok": True, "status": "empty", "users": 0, "verified": 0}
    config = _senler_course_chat_config()
    if config is None:
        return {"ok": False, "status": "not_configured", "users": len(ids), "verified": 0}
    verified: set[int] = set()
    errors: list[str] = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        for index in range(0, len(ids), 100):
            batch = ids[index : index + 100]
            try:
                response = await client.post(
                    f"{SENLER_API_BASE}/subscribers/add",
                    json={
                        "access_token": config["token"],
                        "group_id": config["group_id"],
                        "subscription_id": config["subscription_id"],
                        "vk_user_id": batch,
                        "v": "2",
                    },
                )
                body = response.json()
                if not response.is_success or not isinstance(body, dict) or not body.get("success"):
                    raise RuntimeError(_clean(body.get("error") if isinstance(body, dict) else "") or "subscribers/add failed")
                check = await client.post(
                    f"{SENLER_API_BASE}/subscribers/get",
                    json={
                        "access_token": config["token"],
                        "group_id": config["group_id"],
                        "subscription_id": [int(config["subscription_id"])],
                        "vk_user_id": batch,
                        "v": "2",
                        "count": 100,
                    },
                )
                check_body = check.json()
                if not check.is_success or not isinstance(check_body, dict) or not check_body.get("success"):
                    raise RuntimeError(_clean(check_body.get("error") if isinstance(check_body, dict) else "") or "subscribers/get failed")
                verified.update(
                    int(item.get("vk_user_id") or 0)
                    for item in check_body.get("items", [])
                    if int(item.get("vk_user_id") or 0) > 0
                )
            except Exception as error:
                errors.append(_clean(error, 500))
    missing = sorted(set(ids) - verified)
    return {
        "ok": not errors and not missing,
        "status": "synced" if not errors and not missing else "partial_error",
        "users": len(ids),
        "verified": len(verified),
        "unresolved_vk_ids": missing,
        "subscription_id": config["subscription_id"],
        "errors": errors,
    }


def _copy_telegram_moderation_log_to_vk(text: str, *, category: str, chat_id: Any, user_id: Any) -> None:
    manager = _runtime
    vk_runtime = manager.vk if manager is not None else None
    if vk_runtime is None or vk_runtime.subscription is None or not vk_runtime.subscription.running:
        _record_action(
            platform="telegram",
            action="forward_to_vk_log",
            category=category,
            status="error",
            chat_id=chat_id,
            user_id=user_id,
            error="VK moderator runtime is not running",
        )
        return
    try:
        vk_runtime._send_message(vk_runtime.log_chat_id, text)
        _record_action(
            platform="telegram",
            action="forward_to_vk_log",
            category=category,
            status="ok",
            chat_id=chat_id,
            peer_id=vk_runtime.log_chat_id,
            user_id=user_id,
            text=text,
        )
    except Exception as error:
        _record_action(
            platform="telegram",
            action="forward_to_vk_log",
            category=category,
            status="error",
            chat_id=chat_id,
            peer_id=vk_runtime.log_chat_id,
            user_id=user_id,
            error=str(error),
            text=text,
        )


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def _db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _init_db() -> None:
    with _db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS secret_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS templates (
                key TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                body TEXT NOT NULL DEFAULT '',
                help TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_state (
                platform TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'stopped',
                last_started_at TEXT NOT NULL DEFAULT '',
                last_stopped_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS managed_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL DEFAULT '',
                peer_id TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                zone TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                last_seen_at TEXT NOT NULL DEFAULT '',
                meta_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(platform, chat_id, peer_id)
            );
            CREATE TABLE IF NOT EXISTS vk_member_roster (
                peer_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                present INTEGER NOT NULL DEFAULT 1,
                welcome_status TEXT NOT NULL DEFAULT '',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY(peer_id, user_id)
            );
            CREATE INDEX IF NOT EXISTS idx_vk_member_roster_present
                ON vk_member_roster(peer_id, present);
            CREATE TABLE IF NOT EXISTS vk_roster_state (
                peer_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'baseline',
                initialized_at TEXT NOT NULL DEFAULT '',
                last_checked_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS vk_roster_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS moderation_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                platform TEXT NOT NULL,
                zone TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                peer_id TEXT NOT NULL DEFAULT '',
                message_id TEXT NOT NULL DEFAULT '',
                cmid TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '',
                user_name TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                text_preview TEXT NOT NULL DEFAULT '',
                request_json TEXT NOT NULL DEFAULT '{}',
                response_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_moderation_actions_ts ON moderation_actions(ts DESC);
            CREATE INDEX IF NOT EXISTS idx_moderation_actions_platform_ts ON moderation_actions(platform, ts DESC);
            CREATE INDEX IF NOT EXISTS idx_moderation_actions_action_ts ON moderation_actions(action, ts DESC);
            CREATE TABLE IF NOT EXISTS pending_getcourse_requests (
                key TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '',
                request_id TEXT NOT NULL DEFAULT '',
                preview_message_id TEXT NOT NULL DEFAULT '',
                preview_cmid TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS retry_jobs (
                key TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '',
                stage TEXT NOT NULL DEFAULT '',
                retry_at TEXT NOT NULL DEFAULT '',
                retry_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'queued',
                json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS vk_course_members (
                user_id INTEGER PRIMARY KEY,
                peer_id TEXT NOT NULL DEFAULT '',
                senler_status TEXT NOT NULL DEFAULT 'pending',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_attempt_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
        template_columns = {row["name"] for row in db.execute("PRAGMA table_info(templates)").fetchall()}
        if "enabled" not in template_columns:
            db.execute("ALTER TABLE templates ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
        for key, value in DEFAULT_SETTINGS.items():
            db.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, _env_default(key, value), _now()),
            )
        for key, item in DEFAULT_TEMPLATES.items():
            db.execute(
                "INSERT OR IGNORE INTO templates(key,title,body,help,enabled,updated_at) VALUES(?,?,?,?,?,?)",
                (key, item["title"], item["body"], item["help"], 1 if item.get("enabled", True) else 0, _now()),
            )
        for platform in ("telegram", "vk"):
            db.execute(
                "INSERT OR IGNORE INTO runtime_state(platform,status,updated_at) VALUES(?,?,?)",
                (platform, "stopped", _now()),
            )


def _remember_vk_course_member(user_id: int, peer_id: int) -> None:
    if int(user_id) <= 0:
        return
    now = _now()
    with _db() as db:
        db.execute(
            "INSERT INTO vk_course_members(user_id,peer_id,first_seen_at,last_seen_at) VALUES(?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET peer_id=excluded.peer_id,last_seen_at=excluded.last_seen_at",
            (int(user_id), str(int(peer_id)), now, now),
        )


def _vk_course_member_pending(user_id: int) -> bool:
    if int(user_id) <= 0:
        return False
    with _db() as db:
        row = db.execute(
            "SELECT senler_status FROM vk_course_members WHERE user_id=?", (int(user_id),)
        ).fetchone()
    return bool(row and row["senler_status"] != "verified")


def _set_vk_course_member_senler_status(user_id: int, verified: bool) -> None:
    with _db() as db:
        db.execute(
            "UPDATE vk_course_members SET senler_status=?,last_attempt_at=? WHERE user_id=?",
            ("verified" if verified else "pending", _now(), int(user_id)),
        )


def _env_default(key: str, fallback: str) -> str:
    env_key = {
        "runtime_enabled": "NEXUS_CHAT_MODERATORS_RUNTIME_ENABLED",
        "tg_enabled": "NEXUS_CHAT_MODERATORS_TG_ENABLED",
        "vk_enabled": "NEXUS_CHAT_MODERATORS_VK_ENABLED",
        "dry_run": "NEXUS_CHAT_MODERATORS_DRY_RUN",
        "tg_send_responses": "NEXUS_CHAT_MODERATORS_TG_SEND_RESPONSES",
        "vk_send_responses": "NEXUS_CHAT_MODERATORS_VK_SEND_RESPONSES",
        "telegram_log_chat_id": "TELEGRAM_LOG_CHAT_ID",
        "telegram_bot_api_proxy_url": "TELEGRAM_BOT_API_PROXY_URL",
    }.get(key, "")
    if env_key and os.getenv(env_key) is not None:
        return str(os.getenv(env_key) or "").strip()
    if key == "telegram_log_chat_id" and os.getenv("KURATOR_LOG_CHAT_ID") is not None:
        return str(os.getenv("KURATOR_LOG_CHAT_ID") or "").strip()
    return fallback


def _env_forces_false(env_key: str) -> bool:
    value = os.getenv(env_key)
    return value is not None and not _truthy(value)


def _setting_enabled(settings: dict[str, str], key: str, env_key: str) -> bool:
    if _env_forces_false(env_key):
        return False
    return _truthy(settings.get(key))


def _settings() -> dict[str, str]:
    with _db() as db:
        rows = db.execute("SELECT key,value FROM settings").fetchall()
    values = dict(DEFAULT_SETTINGS)
    values.update({row["key"]: row["value"] for row in rows})
    return values


def _save_settings(values: dict[str, Any]) -> dict[str, str]:
    allowed = set(DEFAULT_SETTINGS)
    now = _now()
    with _db() as db:
        for key, value in values.items():
            if key not in allowed:
                continue
            db.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, str(value if value is not None else "").strip(), now),
            )
    return _settings()


def _stored_secrets() -> dict[str, str]:
    try:
        with _db() as db:
            rows = db.execute("SELECT key,value FROM secret_settings").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).casefold():
            raise
        return {}
    return {row["key"]: row["value"] for row in rows}


def _secret_env_value(spec: dict[str, str]) -> str:
    env_key = spec.get("env") or ""
    if not env_key:
        return ""
    return str(os.getenv(env_key) or "").strip()


def _secret_value(key: str) -> str:
    spec = SECRET_SPECS.get(key)
    if not spec:
        return ""
    env_value = _secret_env_value(spec)
    if env_value:
        return env_value
    stored = _stored_secrets().get(key, "").strip()
    if stored:
        return stored
    return str(spec.get("default") or "").strip()


def _mask_secret(value: str, *, kind: str = "token") -> str:
    value = str(value or "")
    if not value:
        return ""
    if kind in {"text", "url"}:
        if len(value) <= 18:
            return value
        return f"{value[:10]}…{value[-6:]}"
    if len(value) <= 8:
        return "••••"
    return f"{value[:4]}…{value[-4:]}"


def _secret_validation(key: str) -> dict[str, str]:
    if key != "vk_group_token":
        return {"status": "unchecked", "error": ""}
    invalid_markers = ("invalid access_token", "User authorization failed")
    try:
        with _db() as db:
            state = db.execute(
                """
                SELECT status,last_error AS error
                FROM runtime_state
                WHERE platform='vk'
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            if state and state["status"] == "running" and not str(state["error"] or "").strip():
                return {"status": "ok", "error": ""}
            if state and any(marker in str(state["error"] or "") for marker in invalid_markers):
                return {"status": "invalid", "error": str(state["error"] or "")}
            row = db.execute(
                """
                SELECT action,status,error
                FROM moderation_actions
                WHERE platform='vk'
                  AND action IN ('runtime_start','runtime_loop_started','runtime_token_selected','runtime_loop_error')
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
    except Exception:
        return {"status": "unchecked", "error": ""}
    if not row:
        return {"status": "unchecked", "error": ""}
    error = str(row["error"] or "")
    if row["status"] == "ok":
        return {"status": "ok", "error": ""}
    if row["status"] == "error" and any(marker in error for marker in invalid_markers):
        return {"status": "invalid", "error": error}
    return {"status": "unchecked", "error": ""}


def _is_vk_auth_error(error: Any) -> bool:
    text = str(error or "").lower()
    markers = (
        "invalid access_token",
        "user authorization failed",
        "user is blocked",
        "access_token has expired",
        "authorization failed",
    )
    return any(marker in text for marker in markers)


def _secret_status() -> list[dict[str, Any]]:
    stored = _stored_secrets()
    items: list[dict[str, Any]] = []
    for key, spec in SECRET_SPECS.items():
        if spec.get("visible") is False:
            continue
        env_value = _secret_env_value(spec)
        stored_value = stored.get(key, "").strip()
        default_value = str(spec.get("default") or "").strip()
        if env_value:
            value = env_value
            source = "env"
        elif stored_value:
            value = stored_value
            source = "module"
        elif default_value:
            value = default_value
            source = "default"
        else:
            value = ""
            source = "missing"
        validation = _secret_validation(key) if value else {"status": "missing", "error": ""}
        ready = bool(value) and validation.get("status") != "invalid"
        items.append(
            {
                "key": key,
                "env": spec.get("env", ""),
                "label": spec.get("label", key),
                "kind": spec.get("kind", "token"),
                "ready": ready,
                "source": source,
                "env_present": bool(env_value),
                "module_present": bool(stored_value),
                "masked": _mask_secret(value, kind=str(spec.get("kind") or "token")),
                "env_wins": bool(env_value and stored_value),
                "validation_status": validation.get("status", "unchecked"),
                "validation_error": validation.get("error", ""),
            }
        )
    return items


def _save_secrets(values: dict[str, Any]) -> list[dict[str, Any]]:
    now = _now()
    with _db() as db:
        for key, raw_value in values.items():
            if key not in SECRET_SPECS:
                continue
            value = str(raw_value or "").strip()
            if not value:
                continue
            if value == "__clear__":
                db.execute("DELETE FROM secret_settings WHERE key=?", (key,))
                continue
            db.execute(
                "INSERT INTO secret_settings(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, value, now),
            )
    return _secret_status()


def _templates() -> list[dict[str, Any]]:
    with _db() as db:
        rows = db.execute("SELECT key,title,body,help,enabled,updated_at FROM templates ORDER BY key").fetchall()
    by_key = {row["key"]: dict(row) for row in rows}
    items: list[dict[str, Any]] = []
    for key, default in DEFAULT_TEMPLATES.items():
        item = by_key.get(key) or {
            "key": key,
            "title": default["title"],
            "body": default["body"],
            "help": default["help"],
            "enabled": 1 if default.get("enabled", True) else 0,
            "updated_at": "",
        }
        item["enabled"] = bool(item.get("enabled"))
        items.append(item)
    return items


def _template_enabled(key: str) -> bool:
    with _db() as db:
        row = db.execute("SELECT enabled FROM templates WHERE key=?", (key,)).fetchone()
    if row is not None:
        return bool(row["enabled"])
    return bool(DEFAULT_TEMPLATES.get(key, {}).get("enabled", True))


def _template_value(key: str) -> str:
    with _db() as db:
        row = db.execute("SELECT body FROM templates WHERE key=?", (key,)).fetchone()
    if row and str(row["body"]).strip():
        return str(row["body"])
    return DEFAULT_TEMPLATES.get(key, {}).get("body", "")


def _save_templates(values: dict[str, Any]) -> list[dict[str, Any]]:
    now = _now()
    with _db() as db:
        for key, raw_value in values.items():
            if key not in DEFAULT_TEMPLATES:
                continue
            enabled = DEFAULT_TEMPLATES[key].get("enabled", True)
            if isinstance(raw_value, dict):
                body = str(raw_value.get("body") if raw_value.get("body") is not None else "").strip()
                if "enabled" in raw_value:
                    enabled = _truthy(raw_value.get("enabled"))
            else:
                body = str(raw_value if raw_value is not None else "").strip()
            if not body:
                body = DEFAULT_TEMPLATES[key]["body"]
            db.execute(
                """INSERT INTO templates(key,title,body,help,enabled,updated_at) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(key) DO UPDATE SET body=excluded.body,enabled=excluded.enabled,updated_at=excluded.updated_at""",
                (key, DEFAULT_TEMPLATES[key]["title"], body, DEFAULT_TEMPLATES[key]["help"], 1 if enabled else 0, now),
            )
    return _templates()


def _runtime_state() -> list[dict[str, Any]]:
    with _db() as db:
        rows = db.execute("SELECT * FROM runtime_state ORDER BY platform").fetchall()
    return [dict(row) for row in rows]


def _sync_runtime_state_with_live_snapshot(
    snapshot: dict[str, Any] | None = None,
    *,
    reason: str,
) -> list[str]:
    snapshot = snapshot or (_runtime.snapshot() if _runtime is not None else {})
    live_by_platform = {
        "telegram": bool(snapshot.get("telegram_running")),
        "vk": bool(snapshot.get("vk_running")),
    }
    corrected: list[str] = []
    for row in _runtime_state():
        platform = str(row.get("platform") or "")
        if row.get("status") == "running" and platform in live_by_platform and not live_by_platform[platform]:
            _set_runtime_state(platform, "stopped", error=reason)
            _record_action(
                platform=platform,
                action="runtime_state_corrected",
                status="stopped",
                error=reason,
                request_json={"previous_status": "running", "live": False},
            )
            corrected.append(platform)
    if corrected:
        _log("warning", "corrected stale runtime_state for %s: %s", ",".join(corrected), reason)
    return corrected


def _set_runtime_state(platform: str, status: str, *, error: str = "") -> None:
    now = _now()
    fields = {
        "platform": platform,
        "status": status,
        "last_error": error,
        "updated_at": now,
        "last_started_at": now if status == "running" else "",
        "last_stopped_at": now if status in {"stopped", "error"} else "",
    }
    with _db() as db:
        current = db.execute("SELECT * FROM runtime_state WHERE platform=?", (platform,)).fetchone()
        last_started_at = fields["last_started_at"] or (current["last_started_at"] if current else "")
        last_stopped_at = fields["last_stopped_at"] or (current["last_stopped_at"] if current else "")
        db.execute(
            """INSERT INTO runtime_state(platform,status,last_started_at,last_stopped_at,last_error,updated_at)
               VALUES(:platform,:status,:last_started_at,:last_stopped_at,:last_error,:updated_at)
               ON CONFLICT(platform) DO UPDATE SET
                 status=excluded.status,
                 last_started_at=excluded.last_started_at,
                 last_stopped_at=excluded.last_stopped_at,
                 last_error=excluded.last_error,
                 updated_at=excluded.updated_at""",
            {**fields, "last_started_at": last_started_at, "last_stopped_at": last_stopped_at},
        )


def _record_action(
    *,
    platform: str,
    action: str,
    status: str,
    zone: str = "",
    chat_id: Any = "",
    peer_id: Any = "",
    message_id: Any = "",
    cmid: Any = "",
    user_id: Any = "",
    user_name: str = "",
    category: str = "",
    error: str = "",
    text: str = "",
    request_json: Any = None,
    response_json: Any = None,
) -> int:
    preview = re.sub(r"\s+", " ", str(text or "")).strip()[:500]
    with _db() as db:
        cur = db.execute(
            """INSERT INTO moderation_actions(
                   ts,platform,zone,chat_id,peer_id,message_id,cmid,user_id,user_name,
                   action,category,status,error,text_preview,request_json,response_json
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                _now(),
                platform,
                zone,
                _clean(chat_id, 120),
                _clean(peer_id, 120),
                _clean(message_id, 120),
                _clean(cmid, 120),
                _clean(user_id, 120),
                _clean(user_name, 200),
                action,
                category,
                status,
                _clean(error, 1000),
                preview,
                _json_dump(request_json),
                _json_dump(response_json),
            ),
        )
        action_id = int(cur.lastrowid)
    _log("info", "action id=%s platform=%s action=%s status=%s category=%s", action_id, platform, action, status, category)
    return action_id


def _upsert_chat(*, platform: str, chat_id: Any = "", peer_id: Any = "", title: str = "", zone: str = "", meta: Any = None) -> None:
    now = _now()
    if platform == "vk" and peer_id:
        chat_id = ""
    meta_json = _json_dump(meta)
    with _db() as db:
        existing = db.execute(
            "SELECT enabled, meta_json FROM managed_chats WHERE platform=? AND chat_id=? AND peer_id=?",
            (platform, _clean(chat_id, 120), _clean(peer_id, 120)),
        ).fetchone()
        enabled = int(existing["enabled"]) if existing else 1
        if existing and meta in (None, {}, ""):
            meta_json = str(existing["meta_json"] or "{}")
        db.execute(
            """INSERT INTO managed_chats(platform,chat_id,peer_id,title,zone,enabled,last_seen_at,meta_json)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(platform,chat_id,peer_id) DO UPDATE SET
                 title=excluded.title,
                 zone=excluded.zone,
                 last_seen_at=excluded.last_seen_at,
                 meta_json=excluded.meta_json""",
            (platform, _clean(chat_id, 120), _clean(peer_id, 120), _clean(title, 500), zone, enabled, now, meta_json),
        )


def _prepare_vk_roster_upgrade_baseline(zones: set[str]) -> None:
    """Mark pre-existing chats before the first roster scan so an upgrade never mass-greets them."""
    now = _now()
    with _db() as db:
        initialized = db.execute(
            "SELECT value FROM vk_roster_meta WHERE key='upgrade_baseline_prepared'"
        ).fetchone()
        if initialized:
            return
        placeholders = ",".join("?" for _ in zones)
        rows = db.execute(
            f"""SELECT DISTINCT peer_id FROM managed_chats
                WHERE platform='vk' AND enabled=1 AND peer_id!='' AND zone IN ({placeholders})""",
            sorted(zones),
        ).fetchall()
        for row in rows:
            db.execute(
                """INSERT OR IGNORE INTO vk_roster_state(peer_id,mode,initialized_at,last_checked_at,last_error)
                   VALUES(?, 'baseline', '', '', '')""",
                (str(row["peer_id"]),),
            )
        db.execute(
            "INSERT INTO vk_roster_meta(key,value,updated_at) VALUES('upgrade_baseline_prepared','1',?)",
            (now,),
        )


def _managed_vk_roster_chats(zones: set[str]) -> list[tuple[int, str]]:
    placeholders = ",".join("?" for _ in zones)
    with _db() as db:
        rows = db.execute(
            f"""SELECT mc.peer_id, mc.zone FROM managed_chats mc
                LEFT JOIN vk_roster_state rs ON rs.peer_id=mc.peer_id
                WHERE mc.platform='vk' AND mc.enabled=1 AND mc.peer_id!='' AND mc.zone IN ({placeholders})
                  AND (rs.last_error IS NULL OR rs.last_error='' OR
                       rs.last_checked_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now','-1 hour'))
                ORDER BY mc.last_seen_at DESC""",
            sorted(zones),
        ).fetchall()
    result: list[tuple[int, str]] = []
    seen: set[int] = set()
    for row in rows:
        peer_id = _safe_int(row["peer_id"], 0)
        if peer_id > 2000000000 and peer_id not in seen:
            seen.add(peer_id)
            result.append((peer_id, str(row["zone"] or "")))
    return result


def _reconcile_discovered_vk_chats(chats: list[dict[str, Any]]) -> dict[str, Any]:
    """Make community-side peer IDs authoritative without deleting historical rows."""
    by_title: dict[str, list[dict[str, Any]]] = {}
    for item in chats:
        title = _clean(item.get("title"), 500)
        peer_id = _safe_int(item.get("peer_id"), 0)
        zone = _clean(item.get("zone"), 80)
        if title and peer_id > VK_CHAT_PEER_BASE and zone in {"training_stream", "closed_club"}:
            by_title.setdefault(title, []).append({"peer_id": peer_id, "title": title, "zone": zone})
    authoritative = {
        title: items[0]
        for title, items in by_title.items()
        if len({int(item["peer_id"]) for item in items}) == 1
    }
    if not authoritative:
        return {"discovered": 0, "created": 0, "updated": 0, "stale_disabled": 0, "ambiguous": len(by_title)}

    now = _now()
    created = 0
    updated = 0
    stale_disabled = 0
    with _db() as db:
        before = [dict(row) for row in db.execute(
            "SELECT * FROM managed_chats WHERE platform='vk' AND zone IN ('training_stream','closed_club')"
        ).fetchall()]
        enabled_by_title: dict[str, int] = {}
        source_by_title: dict[str, dict[str, Any]] = {}
        for row in before:
            title = str(row.get("title") or "")
            enabled_by_title[title] = max(enabled_by_title.get(title, 0), int(row.get("enabled") or 0))
            current = source_by_title.get(title)
            if current is None or str(row.get("last_seen_at") or "") > str(current.get("last_seen_at") or ""):
                source_by_title[title] = row

        for title, item in authoritative.items():
            peer_id = str(item["peer_id"])
            zone = str(item["zone"])
            source = source_by_title.get(title) or {}
            desired_enabled = enabled_by_title.get(title, 1)
            existing = db.execute(
                "SELECT id,title,zone,enabled FROM managed_chats WHERE platform='vk' AND chat_id='' AND peer_id=?",
                (peer_id,),
            ).fetchone()
            if existing:
                changed = (
                    str(existing["title"] or "") != title
                    or str(existing["zone"] or "") != zone
                    or int(existing["enabled"] or 0) != desired_enabled
                )
                db.execute(
                    """UPDATE managed_chats SET title=?,zone=?,enabled=?,last_seen_at=?,meta_json=?
                       WHERE id=?""",
                    (
                        title,
                        zone,
                        desired_enabled,
                        max(str(source.get("last_seen_at") or ""), now),
                        str(source.get("meta_json") or "{}"),
                        int(existing["id"]),
                    ),
                )
                updated += int(changed)
            else:
                db.execute(
                    """INSERT INTO managed_chats(
                           platform,chat_id,peer_id,title,zone,enabled,last_seen_at,meta_json
                       ) VALUES('vk','',?,?,?,?,?,?)""",
                    (
                        peer_id,
                        title,
                        zone,
                        desired_enabled,
                        max(str(source.get("last_seen_at") or ""), now),
                        str(source.get("meta_json") or "{}"),
                    ),
                )
                created += 1
            db.execute(
                """INSERT OR IGNORE INTO vk_roster_state(
                       peer_id,mode,initialized_at,last_checked_at,last_error
                   ) VALUES(?,'baseline','','','')""",
                (peer_id,),
            )

        rows = db.execute(
            """SELECT id,peer_id,title,enabled FROM managed_chats
               WHERE platform='vk' AND zone IN ('training_stream','closed_club')"""
        ).fetchall()
        for row in rows:
            expected = authoritative.get(str(row["title"] or ""))
            if not expected or str(row["peer_id"] or "") == str(expected["peer_id"]):
                continue
            if int(row["enabled"] or 0):
                db.execute("UPDATE managed_chats SET enabled=0 WHERE id=?", (int(row["id"]),))
                stale_disabled += 1
    return {
        "discovered": len(authoritative),
        "created": created,
        "updated": updated,
        "stale_disabled": stale_disabled,
        "ambiguous": len(by_title) - len(authoritative),
    }


def _telegram_peer_id_from_chat_id(chat_id: Any) -> str:
    value = str(chat_id or "").strip()
    if value.startswith("-100") and len(value) > 4:
        return value[4:]
    return ""


def _chat_enabled(*, platform: str, chat_id: Any = "", peer_id: Any = "") -> bool:
    chat_id_clean = _clean(chat_id, 120)
    peer_id_clean = _clean(peer_id, 120)
    with _db() as db:
        if platform == "telegram":
            peer_from_chat = _telegram_peer_id_from_chat_id(chat_id_clean)
            ids = [value for value in {chat_id_clean, peer_id_clean, peer_from_chat} if value]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                rows = db.execute(
                    f"SELECT enabled FROM managed_chats WHERE platform=? AND (chat_id IN ({placeholders}) OR peer_id IN ({placeholders}))",
                    [platform, *ids, *ids],
                ).fetchall()
                if rows:
                    return all(bool(row["enabled"]) for row in rows)
        if platform == "vk" and peer_id_clean:
            row = db.execute(
                "SELECT enabled FROM managed_chats WHERE platform=? AND peer_id=? ORDER BY CASE WHEN chat_id='' THEN 0 ELSE 1 END, id DESC LIMIT 1",
                (platform, peer_id_clean),
            ).fetchone()
            if row is not None:
                return bool(row["enabled"])
        row = db.execute(
            "SELECT enabled FROM managed_chats WHERE platform=? AND chat_id=? AND peer_id=?",
            (platform, chat_id_clean, peer_id_clean),
        ).fetchone()
    return row is None or bool(row["enabled"])


def _managed_chat_meta(*, platform: str, chat_id: Any = "", peer_id: Any = "") -> dict[str, Any]:
    chat_id_clean = _clean(chat_id, 120)
    peer_id_clean = _clean(peer_id, 120)
    with _db() as db:
        rows = db.execute(
            """SELECT meta_json FROM managed_chats
               WHERE platform=? AND (
                 (chat_id=? AND peer_id=?) OR
                 (chat_id=? AND meta_json NOT IN ('', '{}')) OR
                 (peer_id=? AND meta_json NOT IN ('', '{}'))
               )
               ORDER BY CASE WHEN meta_json NOT IN ('', '{}') THEN 0 ELSE 1 END, id DESC
               LIMIT 5""",
            (platform, chat_id_clean, peer_id_clean, chat_id_clean, peer_id_clean),
        ).fetchall()
    for row in rows:
        try:
            meta = json.loads(row["meta_json"] or "{}")
        except Exception:
            continue
        if isinstance(meta, dict) and meta:
            return meta
    return {}


def _telegram_welcome_thread_id(chat_id: Any) -> int | None:
    meta = _managed_chat_meta(platform="telegram", chat_id=chat_id, peer_id=_telegram_peer_id_from_chat_id(chat_id))
    topic_ids = meta.get("topic_ids") if isinstance(meta, dict) else None
    if not isinstance(topic_ids, dict):
        return None
    for key in ("vizitka", "obuchenie", "boltalka"):
        try:
            topic_id = int(topic_ids.get(key) or 0)
        except Exception:
            topic_id = 0
        if topic_id > 0:
            return topic_id
    return None


def _telegram_welcome_thread_candidates(chat_id: Any) -> list[int | None]:
    """Prefer stored metadata, then the course-chat Vizitka convention, then General."""
    candidates: list[int | None] = []
    stored = _telegram_welcome_thread_id(chat_id)
    ordered = ([stored] if stored is not None else []) + [3, None]
    for thread_id in ordered:
        if thread_id not in candidates:
            candidates.append(thread_id)
    return candidates


def _telegram_topic_unavailable(error: Exception) -> bool:
    value = str(error or "").casefold().replace("_", " ")
    return "topic closed" in value or "message thread not found" in value


def _is_course_chat_title(title: str) -> bool:
    fragments = _configured_title_fragments()
    if fragments:
        return _title_matches_config(title)
    value = str(title or "").strip()
    if not value:
        return False
    if "чат клуба" in value.lower():
        return False
    return bool(re.search(r"^\d+\.\s*\d{2}\.\d{2}\.\d{4}\s*-\s*.*современный\s+собаковод", value, re.IGNORECASE))


def _is_closed_club_title(title: Any) -> bool:
    value = _norm_text(title)
    return "закрытый чат" in value or "закрытый клуб" in value


def _telegram_chat_zone(chat: Any) -> str | None:
    title = getattr(chat, "title", "") or ""
    if _is_course_chat_title(title):
        return "closed_club" if _is_closed_club_title(title) else "training_stream"
    meta = _managed_chat_meta(platform="telegram", chat_id=getattr(chat, "id", ""), peer_id=_telegram_peer_id_from_chat_id(getattr(chat, "id", "")))
    if not _configured_title_fragments() and str(meta.get("source") or "") == "course-chat-creator":
        return "training_stream"
    return None


async def _require_user(request: Request) -> dict[str, Any]:
    if verify_token_from_request is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    user = await verify_token_from_request(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if can_access_module and not can_access_module(user, MODULE_ID):
        raise HTTPException(status_code=403, detail="Forbidden")
    return user


def extract_urls(text: str) -> list[str]:
    return URL_PATTERN.findall(text or "")


def normalize_url_host(url: str) -> str:
    candidate = url.strip().rstrip(".,!?;:)\"]'")
    if not candidate:
        return ""
    if not re.match(r"(?i)^https?://", candidate):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    return (parsed.netloc or parsed.path).lower().lstrip(".")


def normalize_url_identity(url: str) -> str:
    candidate = url.strip().rstrip(".,!?;:)\"]'")
    if not candidate:
        return ""
    if not re.match(r"(?i)^https?://", candidate):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or "").lower().lstrip(".")
    if host.startswith("m."):
        host = host[2:]
    path = (parsed.path or "").strip("/").lower()
    first_segment = path.split("/", 1)[0]
    return f"{host}/{first_segment}" if host and first_segment else ""


def is_trusted_url(url: str) -> bool:
    identity = normalize_url_identity(url)
    if identity in TRUSTED_URL_IDENTITIES:
        return True
    host = normalize_url_host(url)
    return bool(host) and any(host == suffix or host.endswith(f".{suffix}") for suffix in TRUSTED_HOST_SUFFIXES)


def strip_urls(text: str) -> str:
    return re.sub(r"\s+", " ", URL_PATTERN.sub(" ", text or "")).strip()


def _has_canine_context(text: str) -> bool:
    return bool(CANINE_CONTEXT_RE.search(str(text or "")))


def _has_actionable_profanity(text: str) -> bool:
    value = str(text or "")
    matches = list(PROFANITY_RE.finditer(value))
    if not matches:
        return False
    has_canine_context = _has_canine_context(value)
    for match in matches:
        term = match.group(0)
        if MILD_INTERJECTION_RE.fullmatch(term):
            continue
        if CANINE_SEX_TERM_RE.fullmatch(term) and has_canine_context:
            continue
        return True
    return False


def _should_downgrade_vk_negative(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    urls = extract_urls(value)
    if urls and not all(is_trusted_url(url) for url in urls):
        return False
    if _has_actionable_profanity(value) or DIRECT_ABUSE_RE.search(value):
        return False
    if _has_canine_context(value):
        return True
    return bool(GENERAL_VENT_RE.match(value))


def _should_downgrade_tg_negative(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    urls = extract_urls(value)
    if urls and not all(is_trusted_url(url) for url in urls):
        return False
    if _has_actionable_profanity(value) or DIRECT_ABUSE_RE.search(value) or GENERIC_CHAT_REDIRECT_RE.search(value):
        return False
    return bool(
        _has_canine_context(value)
        or NEGATIVE_LANGUAGE_MENTION_RE.search(value)
        or BENIGN_CONSISTENCY_RE.match(value)
    )


def _should_downgrade_vk_refund(text: str) -> bool:
    value = str(text or "").strip()
    if not value or REFUND_RE.search(value):
        return False
    return _has_canine_context(value)


def _rule_based_category(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    if _has_actionable_profanity(value):
        return "негатив"
    if REFUND_RE.search(value):
        return "возврат"
    if TECH_SUPPORT_RE.search(value):
        return "техпод"
    if TRAINING_CHAT_TRANSFER_RE.search(value):
        urls = extract_urls(value)
        if not urls:
            return "нейтрально"
        if all(is_trusted_url(url) for url in urls):
            return "нейтрально"
    if GENERIC_CHAT_REDIRECT_RE.search(value):
        return "негатив"
    return ""


class ModerationAnalyzer:
    def __init__(self) -> None:
        self._timeout = 45.0

    async def _call_openrouter(self, *, system_prompt: str, text: str, max_tokens: int, temperature: float, prompt_path: str = "") -> str:
        api_key = _secret_value("openrouter_api_key")
        if not api_key:
            return ""
        max_tokens = max(16, int(max_tokens or 16))
        if prompt_path:
            model = _openrouter_model_for_prompt(prompt_path).get("model") or "deepseek/deepseek-chat"
        else:
            model = _openrouter_model_for_prompt("").get("model") or "deepseek/deepseek-chat"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://junior.sobakovod.pro/nexus/",
            "X-Title": "Nexus Chat Moderators",
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "reasoning": {"exclude": True, "effort": "none"},
        }
        client_kwargs: dict[str, Any] = {"timeout": self._timeout}
        proxy = (
            os.getenv("OPENROUTER_HTTPS_PROXY", "")
            or os.getenv("OPENROUTER_HTTP_PROXY", "")
        ).strip()
        if proxy:
            client_kwargs.update(proxy=proxy, trust_env=False)
        async with httpx.AsyncClient(**client_kwargs) as client:
            resp = await client.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
        if resp.status_code >= 400:
            _log("warning", "OpenRouter moderation HTTP %s model=%s proxy=%s body=%s", resp.status_code, model, bool(proxy), resp.text[:500])
            raise RuntimeError(f"OpenRouter moderation HTTP {resp.status_code}")
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError("OpenRouter moderation returned no choices")
        content = choices[0].get("message", {}).get("content", "")
        if isinstance(content, list):
            content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
        result = str(content or "").strip().lower()
        if not result:
            raise RuntimeError("OpenRouter moderation returned empty content")
        return result

    async def analyze_tg(self, text: str) -> str:
        if not text or len(text.strip()) < 2:
            return "ок"
        rule_category = _rule_based_category(text)
        if rule_category:
            return rule_category
        if not _secret_value("openrouter_api_key"):
            return MODERATION_DEGRADED_CATEGORY
        try:
            result = await self._call_openrouter(
                system_prompt=TG_SYSTEM_PROMPT,
                text=text[:2000],
                temperature=0,
                max_tokens=10,
            )
            for category in ("возврат", "техпод", "негатив", "скам", "удалить"):
                if category in result:
                    if category == "негатив" and _should_downgrade_tg_negative(text):
                        return "ок"
                    return category
            return "ок"
        except Exception as error:
            _log("error", "TG OpenRouter moderation failed: %s", error)
            return MODERATION_DEGRADED_CATEGORY

    async def analyze_vk(self, text: str) -> str:
        if not text or len(text.strip()) < 3:
            return "нейтрально"
        try:
            analysis_text = text.strip()
            urls = extract_urls(analysis_text)
            if urls and all(is_trusted_url(url) for url in urls):
                analysis_text = strip_urls(analysis_text)
                if len(analysis_text) < 3:
                    return "нейтрально"
            rule_category = _rule_based_category(analysis_text)
            if rule_category:
                return rule_category
            if not _secret_value("openrouter_api_key"):
                return MODERATION_DEGRADED_CATEGORY
            result = await self._call_openrouter(
                system_prompt=_vk_system_prompt(),
                text=analysis_text[:2000],
                temperature=0.1,
                max_tokens=5,
                prompt_path=(_settings().get("vk_ai_prompt_path") or "").strip(),
            )
            for category in ("возврат", "техпод", "негатив", "скам", "удалить"):
                if category in result:
                    if category == "возврат" and _should_downgrade_vk_refund(analysis_text):
                        return "нейтрально"
                    if category == "негатив" and _should_downgrade_vk_negative(analysis_text):
                        return "нейтрально"
                    return category
            return "нейтрально"
        except Exception as error:
            _log("error", "VK OpenRouter moderation failed: %s", error)
            return MODERATION_DEGRADED_CATEGORY


class TelegramModeratorRuntime:
    def __init__(self, analyzer: ModerationAnalyzer) -> None:
        self.analyzer = analyzer
        self.app: Any | None = None
        self.running = False
        self.settings: dict[str, str] = {}
        self.last_polling_error_signature = ""
        self.last_polling_error_at = 0.0

    async def start(self, settings: dict[str, str]) -> None:
        self.settings = settings
        if self.running:
            return
        token = _secret_value("telegram_bot_token_moderator")
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN_MODERATOR is not configured")
        try:
            from telegram.ext import ApplicationBuilder, ChatMemberHandler, ContextTypes, MessageHandler, filters
        except Exception as error:
            raise RuntimeError(f"python-telegram-bot is not installed: {error}") from error

        runtime = self
        dry_run = _truthy(settings.get("dry_run"))
        bot_api_proxy_url = global_telegram_bot_api_proxy_url(settings.get("telegram_bot_api_proxy_url"))
        telegram_log_chat_id = _safe_int(
            settings.get("telegram_log_chat_id")
            or os.environ.get("TELEGRAM_LOG_CHAT_ID")
            or os.environ.get("KURATOR_LOG_CHAT_ID"),
            2852064172,
        )

        def message_thread_kwargs(message: Any) -> dict[str, Any]:
            thread_id = getattr(message, "message_thread_id", None)
            return {"message_thread_id": int(thread_id)} if thread_id else {}

        async def send_log_notification(update: Any, context: ContextTypes.DEFAULT_TYPE, *, category: str, text: str) -> None:
            message = update.effective_message
            chat = update.effective_chat
            user = update.effective_user
            if not message or not chat:
                return
            chat_title = html.escape(str(getattr(chat, "title", "") or chat.id))
            user_name = html.escape(str(getattr(user, "full_name", "") or getattr(user, "first_name", "") or "Unknown"))
            user_id = getattr(user, "id", "Unknown")
            header = (
                f"<b>[TG MODERATOR]</b>\n"
                f"Type: {html.escape(str(category).upper())}\n"
                f"User: {user_name} (id{html.escape(str(user_id))})\n"
                f"Chat: {chat_title}\n"
                f"Message: {html.escape(str(text or '')[:900])}"
            )
            _copy_telegram_moderation_log_to_vk(
                (
                    f"🚨 [TG MODERATOR]\n"
                    f"Type: {str(category).upper()}\n"
                    f"User: {str(getattr(user, 'full_name', '') or getattr(user, 'first_name', '') or 'Unknown')} (id{user_id})\n"
                    f"Chat: {str(getattr(chat, 'title', '') or chat.id)}\n"
                    f"Message: {str(text or '')[:900]}"
                ),
                category=category,
                chat_id=getattr(chat, "id", ""),
                user_id=user_id,
            )
            if not telegram_log_chat_id:
                return
            try:
                await context.bot.send_message(chat_id=telegram_log_chat_id, text=header, parse_mode="HTML")
                try:
                    await context.bot.forward_message(
                        chat_id=telegram_log_chat_id,
                        from_chat_id=getattr(chat, "id", ""),
                        message_id=getattr(message, "message_id", ""),
                    )
                except Exception as forward_error:
                    _record_action(
                        platform="telegram",
                        action="forward_to_log",
                        category=category,
                        status="error",
                        chat_id=getattr(chat, "id", ""),
                        message_id=getattr(message, "message_id", ""),
                        user_id=user_id,
                        user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                        error=str(forward_error),
                        text=text,
                    )
                    return
                _record_action(
                    platform="telegram",
                    action="forward_to_log",
                    category=category,
                    status="ok",
                    chat_id=getattr(chat, "id", ""),
                    message_id=getattr(message, "message_id", ""),
                    user_id=user_id,
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    text=text,
                    request_json={"telegram_log_chat_id": telegram_log_chat_id},
                )
            except Exception as error:
                _record_action(
                    platform="telegram",
                    action="forward_to_log",
                    category=category,
                    status="error",
                    chat_id=getattr(chat, "id", ""),
                    message_id=getattr(message, "message_id", ""),
                    user_id=user_id,
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    error=str(error),
                    text=text,
                    request_json={"telegram_log_chat_id": telegram_log_chat_id},
                )

        async def should_ignore_admin_message(update: Any, context: ContextTypes.DEFAULT_TYPE) -> bool:
            message = update.effective_message
            if not message:
                return False
            if getattr(message, "sender_chat", None) is not None:
                _record_action(
                    platform="telegram",
                    action="skip_admin_or_service",
                    status="ok",
                    chat_id=getattr(update.effective_chat, "id", ""),
                    user_id=getattr(update.effective_user, "id", ""),
                    text=getattr(message, "text", ""),
                    request_json={"reason": "sender_chat"},
                )
                return True
            user = update.effective_user
            chat = update.effective_chat
            if not user or not chat:
                return False
            try:
                member = await context.bot.get_chat_member(chat.id, user.id)
                if member.status in {"creator", "administrator"}:
                    _record_action(
                        platform="telegram",
                        action="skip_admin_or_service",
                        status="ok",
                        chat_id=chat.id,
                        user_id=user.id,
                        user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                        text=getattr(message, "text", ""),
                        request_json={"reason": "admin", "member_status": member.status},
                    )
                    return True
            except Exception as error:
                _record_action(
                    platform="telegram",
                    action="resolve_admin_failed",
                    status="error",
                    chat_id=chat.id,
                    user_id=user.id,
                    error=str(error),
                )
                return True
            return False

        async def handle_message(update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
            if not update.effective_chat or not update.message or not update.message.text:
                return
            chat_id = update.effective_chat.id
            user = update.effective_user
            text = update.message.text
            chat_zone = _telegram_chat_zone(update.effective_chat)
            if not chat_zone:
                return
            _upsert_chat(platform="telegram", chat_id=chat_id, title=getattr(update.effective_chat, "title", ""), zone=chat_zone)
            if not _chat_enabled(platform="telegram", chat_id=chat_id):
                _record_action(platform="telegram", action="skip_chat_disabled", status="ok", chat_id=chat_id, text=text)
                return
            if await should_ignore_admin_message(update, context):
                return
            category = await runtime.analyzer.analyze_tg(text)
            _record_action(
                platform="telegram",
                action="analyze",
                category=category,
                status="ok",
                chat_id=chat_id,
                user_id=getattr(user, "id", ""),
                user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                text=text,
            )
            if category == "техпод":
                await send_log_notification(update, context, category=category, text=text)
                _record_action(
                    platform="telegram",
                    action="tech_support_no_delete",
                    category=category,
                    status="ok",
                    chat_id=chat_id,
                    user_id=getattr(user, "id", ""),
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    text=text,
                )
                return
            if category not in {"негатив", "скам", "удалить", "возврат"}:
                return
            user_name = getattr(user, "full_name", "") or getattr(user, "first_name", "") or "Участник"
            user_id = getattr(user, "id", "")
            user_mention = f'<a href="tg://user?id={user_id}">{user_name}</a>' if user_id else user_name
            reply_template_key = ""
            if category == "возврат":
                reply_template_key = "tg_refund"
            elif category == "техпод":
                reply_template_key = "tg_tech_support"
            if dry_run:
                _record_action(
                    platform="telegram",
                    action="would_delete",
                    category=category,
                    status="dry_run",
                    chat_id=chat_id,
                    message_id=getattr(update.message, "message_id", ""),
                    user_id=getattr(user, "id", ""),
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    text=text,
                )
                if reply_template_key and _template_enabled(reply_template_key):
                    reply_text = _template_value(reply_template_key).format(user_mention=user_mention, user_name=user_name)
                    _record_action(
                        platform="telegram",
                        action="would_send_message",
                        category=category,
                        status="dry_run",
                        chat_id=chat_id,
                        user_id=user_id,
                        user_name=user_name,
                        text=reply_text,
                    )
                return
            await send_log_notification(update, context, category=category, text=text)
            if reply_template_key and _template_enabled(reply_template_key):
                reply_text = _template_value(reply_template_key).format(user_mention=user_mention, user_name=user_name)
                try:
                    sent = await context.bot.send_message(
                        chat_id=chat_id,
                        text=reply_text,
                        parse_mode="HTML",
                        **message_thread_kwargs(update.message),
                    )
                    _record_action(
                        platform="telegram",
                        action="send_message",
                        category=category,
                        status="ok",
                        chat_id=chat_id,
                        message_id=getattr(sent, "message_id", ""),
                        user_id=user_id,
                        user_name=user_name,
                        text=reply_text,
                    )
                except Exception as error:
                    _record_action(
                        platform="telegram",
                        action="send_message",
                        category=category,
                        status="error",
                        chat_id=chat_id,
                        message_id=getattr(update.message, "message_id", ""),
                        user_id=user_id,
                        user_name=user_name,
                        error=str(error),
                        text=reply_text,
                    )
            try:
                await update.message.delete()
                _record_action(
                    platform="telegram",
                    action="delete",
                    category=category,
                    status="ok",
                    chat_id=chat_id,
                    message_id=getattr(update.message, "message_id", ""),
                    user_id=getattr(user, "id", ""),
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    text=text,
                )
            except Exception as error:
                _record_action(
                    platform="telegram",
                    action="delete",
                    category=category,
                    status="error",
                    chat_id=chat_id,
                    message_id=getattr(update.message, "message_id", ""),
                    user_id=getattr(user, "id", ""),
                    user_name=getattr(user, "full_name", "") or getattr(user, "first_name", ""),
                    error=str(error),
                    text=text,
                )

        async def handle_new_members(update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
            message = update.effective_message
            chat = update.effective_chat
            if not message or not chat:
                return
            chat_id = getattr(chat, "id", "")
            chat_zone = _telegram_chat_zone(chat)
            if not chat_zone:
                _log(
                    "info",
                    "chat-moderators ignored telegram new_member event: chat_id=%s title=%r reason=title_not_matched",
                    chat_id,
                    getattr(chat, "title", "") or "",
                )
                return
            _upsert_chat(platform="telegram", chat_id=chat_id, title=getattr(chat, "title", ""), zone=chat_zone)
            if not _chat_enabled(platform="telegram", chat_id=chat_id):
                _record_action(platform="telegram", action="skip_chat_disabled", status="ok", chat_id=chat_id, request_json={"event": "new_chat_members"})
                return
            if not _template_enabled("tg_welcome"):
                _record_action(platform="telegram", action="skip_template_disabled", status="ok", chat_id=chat_id, request_json={"template": "tg_welcome", "event": "new_chat_members"})
                return
            bot_id = getattr(context.bot, "id", None)
            for member in getattr(message, "new_chat_members", []) or []:
                user_id = getattr(member, "id", "")
                if bot_id and user_id == bot_id:
                    continue
                user_name = getattr(member, "full_name", "") or getattr(member, "first_name", "") or "Участник"
                mention = f'<a href="tg://user?id={user_id}">{user_name}</a>' if user_id else user_name
                text = _template_value("tg_welcome").format(user_mention=mention, user_name=user_name)
                _record_action(
                    platform="telegram",
                    action="new_member",
                    status="ok",
                    chat_id=chat_id,
                    message_id=getattr(message, "message_id", ""),
                    user_id=user_id,
                    user_name=user_name,
                    text=text,
                )
                if dry_run:
                    _record_action(platform="telegram", action="would_send_welcome", status="dry_run", chat_id=chat_id, user_id=user_id, user_name=user_name, text=text)
                    continue
                attempted_threads: list[int | None] = []
                last_error: Exception | None = None
                for welcome_thread_id in _telegram_welcome_thread_candidates(chat_id):
                    attempted_threads.append(welcome_thread_id)
                    try:
                        thread_kwargs = {"message_thread_id": welcome_thread_id} if welcome_thread_id else {}
                        sent = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", **thread_kwargs)
                        _record_action(
                            platform="telegram",
                            action="send_welcome",
                            status="ok",
                            chat_id=chat_id,
                            message_id=getattr(sent, "message_id", ""),
                            user_id=user_id,
                            user_name=user_name,
                            text=text,
                            request_json={"message_thread_id": welcome_thread_id, "attempted_threads": attempted_threads},
                        )
                        last_error = None
                        break
                    except Exception as error:
                        last_error = error
                        if not _telegram_topic_unavailable(error):
                            break
                if last_error is not None:
                    _record_action(
                        platform="telegram",
                        action="send_welcome",
                        status="error",
                        chat_id=chat_id,
                        user_id=user_id,
                        user_name=user_name,
                        error=str(last_error),
                        text=text,
                        request_json={"attempted_threads": attempted_threads},
                    )

        async def on_added_to_chat(update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
            result = update.my_chat_member
            if getattr(result.new_chat_member, "status", "") != "member":
                return
            chat = update.effective_chat
            adder_id = getattr(update.effective_user, "id", 0)
            chat_id = getattr(chat, "id", "") if chat else ""
            chat_zone = _telegram_chat_zone(chat) if chat else None
            if chat_zone:
                _upsert_chat(platform="telegram", chat_id=chat_id, title=getattr(chat, "title", ""), zone=chat_zone)
                _record_action(
                    platform="telegram",
                    action="authorized_add",
                    status="ok",
                    zone=chat_zone,
                    chat_id=chat_id,
                    user_id=adder_id,
                    request_json={"reason": "known_chat", "title": getattr(chat, "title", "")},
                )
                return
            if adder_id not in _int_set_csv(runtime.settings.get("tg_allowed_adders")):
                _record_action(
                    platform="telegram",
                    action="unauthorized_add",
                    status="blocked" if not dry_run else "dry_run",
                    chat_id=chat_id,
                    user_id=adder_id,
                    request_json={"dry_run": dry_run},
                )
                if not dry_run:
                    try:
                        if _template_enabled("tg_unauthorized_add"):
                            await context.bot.send_message(chat.id, _template_value("tg_unauthorized_add"))
                        await context.bot.leave_chat(chat.id)
                    except Exception as error:
                        _record_action(
                            platform="telegram",
                            action="leave_unauthorized_chat",
                            status="error",
                            chat_id=chat_id,
                            user_id=adder_id,
                            error=str(error),
                        )
            else:
                _record_action(
                    platform="telegram",
                    action="authorized_add",
                    status="ok",
                    chat_id=chat_id,
                    user_id=adder_id,
                    request_json={"title": getattr(chat, "title", "")},
                )

        builder = ApplicationBuilder().token(token)
        if bot_api_proxy_url:
            builder = builder.proxy_url(bot_api_proxy_url).get_updates_proxy_url(bot_api_proxy_url)
        self.app = builder.build()
        self.app.add_handler(ChatMemberHandler(on_added_to_chat, ChatMemberHandler.MY_CHAT_MEMBER))
        self.app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_members))
        self.app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
        await self.app.initialize()
        await self.app.start()
        def polling_error_callback(error: Exception) -> None:
            signature = f"{type(error).__name__}: {error}"
            now = time.monotonic()
            if signature == self.last_polling_error_signature and now - self.last_polling_error_at < 60.0:
                return
            self.last_polling_error_signature = signature
            self.last_polling_error_at = now
            _record_action(
                platform="telegram",
                action="polling_error",
                status="error",
                error=signature,
                request_json={"module": MODULE_ID},
            )

        await self.app.updater.start_polling(
            drop_pending_updates=False,
            error_callback=polling_error_callback,
        )
        self.running = True

    async def stop(self) -> None:
        app, self.app = self.app, None
        self.running = False
        if not app:
            return
        try:
            updater = getattr(app, "updater", None)
            if updater is not None and bool(getattr(updater, "running", False)):
                await updater.stop()
        finally:
            try:
                if bool(getattr(app, "running", False)):
                    await app.stop()
            finally:
                if bool(getattr(app, "_initialized", False)):
                    await app.shutdown()


class VKModeratorRuntime:
    def __init__(self, analyzer: ModerationAnalyzer) -> None:
        self.analyzer = analyzer
        self.vk: Any | None = None
        self.own_id = 0
        self.group_id = 0
        self.subscription: VkGroupPollSubscription | None = None
        self.action_pacing_lock = threading.Lock()
        self.last_mutating_action_at = 0.0
        self.log_chat_id = _safe_int(_secret_value("vk_log_chat_id"), 2000000041)
        self.chat_title_cache: dict[int, dict[str, Any]] = {}
        self.user_admin_cache: dict[tuple[int, int], dict[str, Any]] = {}
        self.community_admin_cache: dict[int, dict[str, Any]] = {}
        self.community_owner_cache: dict[int, dict[str, Any]] = {}
        self.join_greeting_cache: dict[tuple[int, int], float] = {}
        self.roster_reconcile_lock = asyncio.Lock()
        self.roster_last_checked: dict[int, float] = {}
        self.roster_task: asyncio.Task[Any] | None = None
        self.last_peer_discovery_at = 0.0
        self.senler_join_tasks: set[asyncio.Task[Any]] = set()
        self.settings: dict[str, str] = {}
        self.getcourse_service = NexusGetCourseAccessService()
        self.getcourse_pending: dict[tuple[int, int], dict[str, Any]] = {}
        self.getcourse_lock = threading.Lock()

    async def start(self, settings: dict[str, str]) -> None:
        if self.subscription and self.subscription.running:
            return
        self.settings = settings
        self.log_chat_id = _safe_int(_secret_value("vk_log_chat_id"), 2000000041)
        token = str(_secret_value("vk_group_token") or "").strip()
        group_id = _safe_int(_secret_value("vk_group_id"), 0)
        if not token:
            raise RuntimeError("VK_GROUP_TOKEN is not configured")
        if group_id <= 0:
            raise RuntimeError("VK_GROUP_ID is not configured")
        self.subscription = await shared_vk_group_poll_hub.subscribe(
            subscriber_id=MODULE_ID,
            token=token,
            group_id=group_id,
            on_event=self._handle_shared_event,
            on_error=self._handle_shared_error,
        )
        self.vk = self.subscription.vk
        self.own_id = self.subscription.own_id
        self.group_id = group_id
        self.subscription.activate()
        try:
            discovery = await asyncio.to_thread(self._discover_community_chat_peers_sync)
            self.last_peer_discovery_at = time.monotonic()
            _record_action(
                platform="vk",
                action="community_peer_discovery",
                status="ok",
                request_json={"group_id": group_id},
                response_json=discovery,
            )
        except Exception as error:
            _record_action(
                platform="vk",
                action="community_peer_discovery",
                status="error",
                error=str(error),
                request_json={"group_id": group_id},
            )
        _prepare_vk_roster_upgrade_baseline({"training_stream", "closed_club"})
        self.roster_task = asyncio.create_task(
            self._roster_reconcile_loop(), name=f"{MODULE_ID}-vk-roster"
        )
        _record_action(
            platform="vk",
            action="runtime_token_selected",
            status="ok",
            request_json={"source": "shared_vk_group_poll", "own_id": self.own_id, "group_id": group_id},
        )
        _record_action(
            platform="vk",
            action="runtime_loop_started",
            status="ok",
            request_json={"own_id": self.own_id, "group_id": group_id, "transport": "bots_long_poll"},
        )

    async def stop(self) -> None:
        roster_task, self.roster_task = self.roster_task, None
        if roster_task is not None:
            roster_task.cancel()
            await asyncio.gather(roster_task, return_exceptions=True)
        subscription, self.subscription = self.subscription, None
        if subscription is not None:
            await subscription.close()
        tasks = list(getattr(self, "senler_join_tasks", set()))
        if hasattr(self, "senler_join_tasks"):
            self.senler_join_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.vk = None
        self.own_id = 0
        self.group_id = 0

    def _zone_for_discovered_title(self, title: str) -> str:
        if self._is_closed_club_chat_title(title):
            return "closed_club"
        if self._is_training_stream_chat_title(title):
            return "training_stream"
        return ""

    def _discover_community_chat_peers_sync(self) -> dict[str, Any]:
        """Enumerate community-side chat IDs exactly as VK Support recommends."""
        if self.vk is None:
            raise RuntimeError("VK runtime is not connected")
        discovered: list[dict[str, Any]] = []
        highest_existing_local_id = 0
        scanned_until = 0
        for start in range(1, VK_CHAT_DISCOVERY_HARD_LIMIT + 1, VK_CHAT_DISCOVERY_BATCH_SIZE):
            local_ids = list(range(start, min(start + VK_CHAT_DISCOVERY_BATCH_SIZE, VK_CHAT_DISCOVERY_HARD_LIMIT + 1)))
            peer_ids = [VK_CHAT_PEER_BASE + local_id for local_id in local_ids]
            code = "return [" + ",".join(
                f'API.messages.getConversationsById({{"peer_ids":{peer_id}}})'
                for peer_id in peer_ids
            ) + "];"
            responses = self.vk.execute(code=code) or []
            for local_id, peer_id, response in zip(local_ids, peer_ids, responses):
                if not isinstance(response, dict):
                    continue
                items = response.get("items") or []
                if not items:
                    continue
                highest_existing_local_id = max(highest_existing_local_id, local_id)
                item = items[0]
                conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
                actual_peer_id = _safe_int((conversation.get("peer") or {}).get("id"), peer_id)
                title = _clean((conversation.get("chat_settings") or {}).get("title"), 500)
                zone = self._zone_for_discovered_title(title)
                if zone:
                    discovered.append({"peer_id": actual_peer_id, "title": title, "zone": zone})
            scanned_until = local_ids[-1]
            if highest_existing_local_id and scanned_until - highest_existing_local_id >= VK_CHAT_DISCOVERY_EMPTY_TAIL:
                break
            if not highest_existing_local_id and scanned_until >= 250:
                break
        result = _reconcile_discovered_vk_chats(discovered)
        return {
            **result,
            "scanned_until": scanned_until,
            "highest_existing_local_id": highest_existing_local_id,
        }

    async def _handle_shared_event(self, event: Any) -> None:
        try:
            message_event = adapt_group_message_event(event)
            payload = getattr(message_event, "message_payload", {}) if message_event is not None else {}
            has_chat_action = isinstance(payload, dict) and isinstance(payload.get("action"), dict)
            peer_id = int(getattr(message_event, "peer_id", 0) or 0) if message_event is not None else 0
            if peer_id > 2000000000:
                await self._maybe_reconcile_chat_members(peer_id, trigger="long_poll_event")
            if message_event is not None and (
                not bool(getattr(message_event, "from_me", False)) or has_chat_action
            ):
                await self.process_message(message_event)
        except Exception as error:
            _record_action(platform="vk", action="runtime_event_error", status="error", error=str(error))
            _log("error", "Shared VK event handler failed: %s", error, exc_info=True)

    async def _roster_reconcile_loop(self) -> None:
        try:
            while self.subscription is not None and self.subscription.running:
                if time.monotonic() - self.last_peer_discovery_at >= 6 * 3600:
                    try:
                        discovery = await asyncio.to_thread(self._discover_community_chat_peers_sync)
                        self.last_peer_discovery_at = time.monotonic()
                        _record_action(
                            platform="vk",
                            action="community_peer_discovery",
                            status="ok",
                            request_json={"trigger": "periodic"},
                            response_json=discovery,
                        )
                    except Exception as error:
                        self.last_peer_discovery_at = time.monotonic()
                        _record_action(
                            platform="vk",
                            action="community_peer_discovery",
                            status="error",
                            error=str(error),
                            request_json={"trigger": "periodic"},
                        )
                for peer_id, zone in _managed_vk_roster_chats({"training_stream", "closed_club"}):
                    if self.subscription is None or not self.subscription.running:
                        return
                    await self._maybe_reconcile_chat_members(
                        peer_id, zone=zone, trigger="periodic", force=True
                    )
                    await asyncio.sleep(0.75)
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _record_action(platform="vk", action="roster_loop_error", status="error", error=str(error))
            _log("warning", "VK roster reconciliation loop failed: %s", error)

    async def _maybe_reconcile_chat_members(
        self,
        peer_id: int,
        *,
        zone: str = "",
        trigger: str,
        force: bool = False,
    ) -> None:
        peer_id = int(peer_id or 0)
        if peer_id <= 2000000000 or self.vk is None:
            return
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self.roster_last_checked.get(peer_id, 0.0) < 15.0:
            return
        async with self.roster_reconcile_lock:
            now_monotonic = time.monotonic()
            if not force and now_monotonic - self.roster_last_checked.get(peer_id, 0.0) < 15.0:
                return
            self.roster_last_checked[peer_id] = now_monotonic
            title = self._get_chat_title(peer_id)
            zone = zone or self.get_chat_zone(peer_id) or ""
            if zone not in {"training_stream", "closed_club"}:
                return
            if trigger != "periodic" and title:
                _upsert_chat(platform="vk", peer_id=peer_id, title=title, zone=zone)
            if not _chat_enabled(platform="vk", peer_id=peer_id):
                return
            peer_key = str(peer_id)
            with _db() as db:
                state = db.execute(
                    "SELECT mode,initialized_at FROM vk_roster_state WHERE peer_id=?", (peer_key,)
                ).fetchone()
            if state is None:
                owner_id = 0
                try:
                    response = self._vk_for_peer(peer_id).messages.getConversationsById(peer_ids=[peer_id])
                    item = (response.get("items") or [{}])[0]
                    conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
                    owner_id = _safe_int((conversation.get("chat_settings") or {}).get("owner_id"), 0)
                except Exception as error:
                    _record_action(
                        platform="vk", zone=zone, action="roster_owner_lookup", status="error",
                        peer_id=peer_id, error=str(error), request_json={"trigger": trigger},
                    )
                mode = "active" if owner_id == -abs(int(self.group_id or 0)) and self.group_id else "baseline"
                with _db() as db:
                    db.execute(
                        """INSERT OR IGNORE INTO vk_roster_state(peer_id,mode,initialized_at,last_checked_at,last_error)
                           VALUES(?,?, '', '', '')""",
                        (peer_key, mode),
                    )
                initialized_at = ""
            else:
                mode = str(state["mode"] or "baseline")
                initialized_at = str(state["initialized_at"] or "")
            try:
                response = self._vk_for_peer(peer_id).messages.getConversationMembers(peer_id=peer_id)
                members = [
                    {
                        "user_id": int(item.get("member_id") or 0),
                        "is_admin": _vk_member_is_admin(item),
                    }
                    for item in (response.get("items") or [])
                    if int(item.get("member_id") or 0) > 0
                ]
            except Exception as error:
                missing_chat = "[927]" in str(error) or "chat does not exist" in str(error).casefold()
                with _db() as db:
                    db.execute(
                        "UPDATE vk_roster_state SET last_checked_at=?,last_error=? WHERE peer_id=?",
                        (_now(), _clean(error, 1000), peer_key),
                    )
                    if missing_chat:
                        db.execute(
                            "UPDATE managed_chats SET enabled=0 WHERE platform='vk' AND peer_id=?",
                            (peer_key,),
                        )
                _record_action(
                    platform="vk", zone=zone, action="roster_reconcile", status="error",
                    peer_id=peer_id, error=str(error),
                    request_json={"trigger": trigger, "stale_chat_disabled": missing_chat},
                )
                return
            trusted_staff = _trusted_vk_staff_ids()
            can_grant_staff_admin = self._community_owns_chat(peer_id)
            for member in members:
                user_id = int(member["user_id"])
                if user_id not in trusted_staff or member["is_admin"] or not can_grant_staff_admin:
                    continue
                try:
                    self._pace_mutating_action()
                    self._vk_for_peer(peer_id).messages.setMemberRole(
                        peer_id=peer_id, member_id=user_id, role="admin"
                    )
                    member["is_admin"] = True
                    self.user_admin_cache[(peer_id, user_id)] = {"is_admin": True, "ts": time.time()}
                    _record_action(
                        platform="vk", zone=zone, action="grant_staff_admin", status="ok",
                        peer_id=peer_id, user_id=user_id, request_json={"trigger": trigger},
                    )
                except Exception as error:
                    _record_action(
                        platform="vk", zone=zone, action="grant_staff_admin", status="error",
                        peer_id=peer_id, user_id=user_id, error=str(error), request_json={"trigger": trigger},
                    )
            now = _now()
            baseline_scan = mode == "baseline" and not initialized_at
            candidates: list[int] = []
            with _db() as db:
                previous = {
                    int(row["user_id"]): dict(row)
                    for row in db.execute(
                        "SELECT user_id,present,welcome_status FROM vk_member_roster WHERE peer_id=?",
                        (peer_key,),
                    ).fetchall()
                }
                db.execute("UPDATE vk_member_roster SET present=0 WHERE peer_id=?", (peer_key,))
                for member in members:
                    user_id = member["user_id"]
                    is_admin = bool(member["is_admin"])
                    prior = previous.get(user_id)
                    rejoined = prior is None or not bool(prior.get("present"))
                    prior_status = str((prior or {}).get("welcome_status") or "")
                    if baseline_scan:
                        welcome_status = "admin" if is_admin else "baseline"
                    elif is_admin:
                        welcome_status = "admin"
                    else:
                        welcome_status = prior_status
                        if rejoined or prior_status == "error":
                            candidates.append(user_id)
                            welcome_status = "pending"
                    db.execute(
                        """INSERT INTO vk_member_roster(
                               peer_id,user_id,is_admin,present,welcome_status,first_seen_at,last_seen_at
                           ) VALUES(?,?,?,?,?,?,?)
                           ON CONFLICT(peer_id,user_id) DO UPDATE SET
                             is_admin=excluded.is_admin,present=1,welcome_status=excluded.welcome_status,
                             last_seen_at=excluded.last_seen_at""",
                        (peer_key, str(user_id), int(is_admin), 1, welcome_status, now, now),
                    )
                db.execute(
                    """UPDATE vk_roster_state SET mode='active',initialized_at=CASE WHEN initialized_at='' THEN ? ELSE initialized_at END,
                       last_checked_at=?,last_error='' WHERE peer_id=?""",
                    (now, now, peer_key),
                )
            candidates = [user_id for user_id in candidates if user_id not in trusted_staff]
            for user_id in candidates:
                self._schedule_joined_user_senler_sync(peer_id, user_id, zone, title)
                if not _template_enabled("vk_welcome"):
                    welcome_status = "disabled"
                else:
                    sent = await self._send_welcome_message(peer_id, user_id, zone)
                    welcome_status = "sent" if sent else "error"
                with _db() as db:
                    db.execute(
                        "UPDATE vk_member_roster SET welcome_status=?,last_seen_at=? WHERE peer_id=? AND user_id=?",
                        (welcome_status, _now(), peer_key, str(user_id)),
                    )
            if candidates or baseline_scan:
                _record_action(
                    platform="vk", zone=zone, action="roster_reconcile", status="ok", peer_id=peer_id,
                    request_json={"trigger": trigger, "baseline": baseline_scan},
                    response_json={"members": len(members), "welcome_candidates": len(candidates)},
                )

    async def _handle_shared_error(self, error: Exception) -> None:
        _record_action(platform="vk", action="runtime_loop_error", status="error", error=str(error))
        if _is_vk_auth_error(error):
            _set_runtime_state("vk", "error", error=str(error))
            _log("error", "Shared VK longpoll authentication error: %s", error)
            return
        _log("warning", "Shared VK Bots Long Poll error: %s", error)

    def _vk_for_peer(self, peer_id: int) -> Any:
        if self.vk is None:
            raise RuntimeError("VK API client is not running")
        return self.vk

    def _pace_mutating_action(self) -> None:
        with self.action_pacing_lock:
            now = time.monotonic()
            delay = max(0.0, min(1.8, self.last_mutating_action_at + random.uniform(0.55, 1.75) - now))
            if delay:
                time.sleep(delay)
            self.last_mutating_action_at = time.monotonic()

    def _vk_event_type(self, name: str) -> Any:
        try:
            from vk_api.longpoll import VkEventType

            return getattr(VkEventType, name)
        except Exception:
            return name

    def _vk_chat_event_type(self, name: str) -> Any:
        try:
            from vk_api.longpoll import VkChatEventType

            return getattr(VkChatEventType, name)
        except Exception:
            return name

    def _get_chat_title(self, peer_id: int) -> str:
        now = time.time()
        cached = self.chat_title_cache.get(int(peer_id))
        if cached and now - float(cached["ts"]) < 300:
            return str(cached["title"])
        try:
            resp = self._vk_for_peer(peer_id).messages.getConversationsById(peer_ids=[int(peer_id)])
            items = resp.get("items") or []
            item = items[0] if items else {}
            conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
            title = str((conversation.get("chat_settings") or {}).get("title") or "").strip()
            self.chat_title_cache[int(peer_id)] = {"title": title, "ts": now}
            return title
        except Exception as error:
            _record_action(platform="vk", action="get_chat_title", status="error", peer_id=peer_id, error=str(error))
            return ""

    def _is_training_stream_chat_title(self, title: str) -> bool:
        fragments = _configured_title_fragments(self.settings)
        if fragments:
            return _title_matches_config(title, self.settings) and not _is_closed_club_title(title)
        return bool(VK_CREATED_CHAT_PATTERNS[0].match(title or ""))

    def _is_closed_club_chat_title(self, title: str) -> bool:
        if _configured_title_fragments(self.settings):
            return _title_matches_config(title, self.settings) and _is_closed_club_title(title)
        value = title or ""
        return bool(
            VK_CREATED_CHAT_PATTERNS[1].match(value)
            or "закрытый чат" in value.lower()
            or "закрытый клуб" in value.lower()
        )

    def _is_logs_chat(self, peer_id: int, title: str | None = None) -> bool:
        try:
            is_log_peer = int(peer_id) == int(self.log_chat_id)
        except Exception:
            is_log_peer = False
        return is_log_peer or "логи модератор" in (title or "").lower()

    def _is_getcourse_access_chat(self, peer_id: int, title: str | None = None) -> bool:
        try:
            if int(peer_id) == GETCOURSE_ACCESS_CHAT_ID:
                return True
        except Exception:
            pass
        return (title or self._get_chat_title(peer_id)).strip().lower() in GETCOURSE_ACCESS_CHAT_TITLES

    def get_chat_zone(self, peer_id: int) -> str | None:
        title = self._get_chat_title(peer_id)
        if self._is_logs_chat(peer_id, title):
            return "logs"
        if self._is_getcourse_access_chat(peer_id, title):
            return None
        if self._is_closed_club_chat_title(title):
            return "closed_club"
        allowlist = {line.strip() for line in str(self.settings.get("vk_training_title_allowlist") or "").splitlines() if line.strip()}
        if self._is_training_stream_chat_title(title) or (not _configured_title_fragments(self.settings) and title in allowlist):
            return "training_stream"
        return None

    async def is_chat_admin(self, peer_id: int, user_id: int) -> bool:
        if int(user_id) in (_int_set_csv(self.settings.get("vk_allowed_admins")) | _trusted_vk_staff_ids()):
            return True
        if int(peer_id) < 2000000000 or int(user_id) <= 0:
            return False
        cache_key = (int(peer_id), int(user_id))
        now = time.time()
        cached = self.user_admin_cache.get(cache_key)
        if cached and now - float(cached["ts"]) < (300 if cached.get("is_admin") else 30):
            return bool(cached["is_admin"])
        try:
            members = self._vk_for_peer(peer_id).messages.getConversationMembers(peer_id=int(peer_id))
            is_admin = False
            for member in members.get("items", []):
                if int(member.get("member_id") or 0) == int(user_id):
                    is_admin = _vk_member_is_admin(member)
                    break
            self.user_admin_cache[cache_key] = {"is_admin": is_admin, "ts": now}
            return is_admin
        except Exception as error:
            _record_action(platform="vk", action="resolve_admin_failed", status="error", peer_id=peer_id, user_id=user_id, error=str(error))
            return bool(cached["is_admin"]) if cached else True

    def _community_is_chat_admin(self, peer_id: int) -> bool:
        now = time.time()
        cache = getattr(self, "community_admin_cache", None)
        if cache is None:
            cache = {}
            self.community_admin_cache = cache
        cached = cache.get(int(peer_id))
        if cached and now - float(cached["ts"]) < (300 if cached.get("is_admin") else 30):
            return bool(cached["is_admin"])
        try:
            members = self._vk_for_peer(peer_id).messages.getConversationMembers(peer_id=int(peer_id))
            is_admin = any(
                int(member.get("member_id") or 0) == int(self.own_id) and _vk_member_is_admin(member)
                for member in members.get("items", [])
            )
            cache[int(peer_id)] = {"is_admin": is_admin, "ts": now}
            return is_admin
        except Exception as error:
            _record_action(
                platform="vk",
                action="resolve_community_admin_failed",
                status="error",
                peer_id=peer_id,
                user_id=self.own_id,
                error=str(error),
            )
            return False

    def _community_owns_chat(self, peer_id: int) -> bool:
        now = time.time()
        cached = self.community_owner_cache.get(int(peer_id))
        if cached and now - float(cached["ts"]) < 600:
            return bool(cached["is_owner"])
        try:
            response = self._vk_for_peer(peer_id).messages.getConversationsById(peer_ids=[int(peer_id)])
            item = (response.get("items") or [{}])[0]
            conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
            owner_id = _safe_int((conversation.get("chat_settings") or {}).get("owner_id"), 0)
            is_owner = bool(self.group_id and owner_id == -abs(int(self.group_id)))
            self.community_owner_cache[int(peer_id)] = {"is_owner": is_owner, "ts": now}
            return is_owner
        except Exception:
            return False

    async def _sync_joined_user_to_senler(self, peer_id: int, user_id: int, zone: str, title: str) -> None:
        if int(user_id) <= 0:
            return
        if not _senler_chat_is_eligible(zone, title):
            return
        if not self._community_is_chat_admin(peer_id):
            _record_action(
                platform="vk",
                zone=zone,
                action="senler_join_skip_no_community_admin",
                status="ok",
                peer_id=peer_id,
                user_id=user_id,
            )
            return
        _remember_vk_course_member(user_id, peer_id)
        result = await _senler_add_course_chat_users([int(user_id)])
        _set_vk_course_member_senler_status(user_id, bool(result.get("ok")))
        _record_action(
            platform="vk",
            zone=zone,
            action="senler_join_sync",
            status="ok" if result.get("ok") else "error",
            peer_id=peer_id,
            user_id=user_id,
            request_json={"subscription_id": result.get("subscription_id") or SENLER_COURSE_CHAT_SUBSCRIPTION_ID},
            response_json={
                "status": result.get("status"),
                "verified": result.get("verified", 0),
                "errors": result.get("errors") or [],
            },
        )

    def _schedule_joined_user_senler_sync(self, peer_id: int, user_id: int, zone: str, title: str) -> None:
        if int(user_id) <= 0:
            return
        task = asyncio.create_task(self._sync_joined_user_to_senler(peer_id, user_id, zone, title))
        tasks = getattr(self, "senler_join_tasks", None)
        if tasks is None:
            tasks = set()
            self.senler_join_tasks = tasks
        tasks.add(task)

        def done(completed: asyncio.Task[Any]) -> None:
            self.senler_join_tasks.discard(completed)
            if completed.cancelled():
                return
            error = completed.exception()
            if error is not None:
                _record_action(
                    platform="vk",
                    zone=zone,
                    action="senler_join_sync",
                    status="error",
                    peer_id=peer_id,
                    user_id=user_id,
                    error=str(error),
                )

        task.add_done_callback(done)

    def _is_trusted_sender(self, user_id: int) -> bool:
        trusted = _int_set_csv(self.settings.get("vk_trusted_senders"))
        trusted.update(_int_set_csv(self.settings.get("vk_allowed_admins")))
        trusted.update(_trusted_vk_staff_ids())
        return int(user_id) in trusted

    def _should_bypass_regular_moderation(self, *, from_id: int, text: str) -> bool:
        if not TRUSTED_MODERATOR_RESOURCE_RE.search(text or ""):
            return False
        return int(from_id) in _int_set_csv(self.settings.get("vk_trusted_senders"))

    def _extract_from_id(self, event: Any) -> int:
        direct = getattr(event, "user_id", None)
        if isinstance(direct, int):
            return direct
        extra_values = getattr(event, "extra_values", {}) or {}
        for key in ("from", "from_id", "user_id", "sender_id"):
            try:
                if extra_values.get(key) is not None:
                    return int(extra_values.get(key))
            except Exception:
                continue
        payload = self._get_message_payload(getattr(event, "message_id", None))
        try:
            return int(payload.get("from_id") or 0)
        except Exception:
            return 0

    def _extract_cmid(self, event: Any) -> int | None:
        extra_values = getattr(event, "extra_values", {}) or {}
        for key in ("conversation_message_id", "cmid"):
            try:
                if extra_values.get(key) is not None:
                    return int(extra_values.get(key))
            except Exception:
                continue
        payload = self._get_message_payload(getattr(event, "message_id", None))
        try:
            if payload.get("conversation_message_id") is not None:
                return int(payload.get("conversation_message_id"))
        except Exception:
            pass
        return None

    def _get_message_payload(self, message_id: Any) -> dict[str, Any]:
        if not message_id:
            return {}
        try:
            payload = self.vk.messages.getById(message_ids=[int(message_id)])
            items = payload.get("items") or []
            return items[0] if items else {}
        except Exception:
            return {}

    def _get_user_name(self, user_id: int) -> str:
        try:
            user_info = self.vk.users.get(user_ids=int(user_id))[0]
            return user_info.get("first_name") or "Коллега"
        except Exception:
            return "Коллега"

    def _should_send_join_greeting(self, peer_id: int, user_id: int, *, ttl_seconds: float = 60.0) -> bool:
        now = time.time()
        threshold = now - float(ttl_seconds)
        for key, ts in list(self.join_greeting_cache.items()):
            if ts <= threshold:
                self.join_greeting_cache.pop(key, None)
        cache_key = (int(peer_id), int(user_id))
        if cache_key in self.join_greeting_cache:
            return False
        self.join_greeting_cache[cache_key] = now
        return True

    def _split_message_chunks(self, message: str, limit: int = VK_MESSAGE_CHUNK_LIMIT) -> list[str]:
        text = str(message or "")
        if len(text) <= limit:
            return [text]
        chunks: list[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= limit:
                chunks.append(remaining)
                break
            split_at = remaining.rfind("\n", 0, limit + 1)
            if split_at < limit // 2:
                split_at = remaining.rfind(" ", 0, limit + 1)
            if split_at < limit // 2:
                split_at = limit
            chunks.append(remaining[:split_at].rstrip() or remaining[:limit])
            remaining = remaining[split_at:].lstrip()
        return chunks

    def _send_message(self, peer_id: int, message: str) -> dict[str, int | None]:
        if _truthy(self.settings.get("dry_run")):
            _record_action(platform="vk", action="would_send_message", status="dry_run", peer_id=peer_id, text=message)
            return {"message_id": None, "cmid": None}
        refs: list[dict[str, int | None]] = []
        for chunk in self._split_message_chunks(message):
            self._pace_mutating_action()
            response = self._vk_for_peer(peer_id).messages.send(peer_id=int(peer_id), message=chunk, random_id=0)
            message_id = int(response.get("message_id") or response.get("id")) if isinstance(response, dict) and (response.get("message_id") or response.get("id")) else None
            refs.append({"message_id": message_id, "cmid": None})
        return refs[0] if refs else {"message_id": None, "cmid": None}

    def _delete_chat_message(self, *, peer_id: int, message_id: Any = None, cmid: Any = None) -> None:
        if _truthy(self.settings.get("dry_run")):
            _record_action(platform="vk", action="would_delete", status="dry_run", peer_id=peer_id, message_id=message_id, cmid=cmid)
            return
        payload: dict[str, Any] = {"delete_for_all": 1, "peer_id": int(peer_id)}
        if cmid is not None:
            payload["cmids"] = [int(cmid)]
        elif message_id:
            payload["message_ids"] = [int(message_id)]
        else:
            return
        self._pace_mutating_action()
        self._vk_for_peer(peer_id).messages.delete(**payload)

    def _send_reaction(self, *, peer_id: int, cmid: int | None, reaction_id: int) -> None:
        if not cmid:
            return
        if _truthy(self.settings.get("dry_run")):
            _record_action(platform="vk", action="would_send_reaction", status="dry_run", peer_id=peer_id, cmid=cmid, request_json={"reaction_id": reaction_id})
            return
        self._pace_mutating_action()
        self._vk_for_peer(peer_id).messages.sendReaction(peer_id=int(peer_id), cmid=int(cmid), reaction_id=int(reaction_id))

    async def _send_welcome_message(self, peer_id: int, user_id: int, zone: str) -> bool:
        if int(user_id) <= 0:
            return False
        if not _template_enabled("vk_welcome"):
            _record_action(platform="vk", zone=zone, action="skip_template_disabled", status="ok", peer_id=peer_id, user_id=user_id, request_json={"template": "vk_welcome"})
            return False
        user_name = self._get_user_name(int(user_id))
        user_mention = f"[id{int(user_id)}|{user_name}]"
        message = _template_value("vk_welcome").format(user_mention=user_mention)
        if _truthy(self.settings.get("dry_run")):
            _record_action(platform="vk", zone=zone, action="would_send_welcome", status="dry_run", peer_id=peer_id, user_id=user_id, user_name=user_name, text=message)
            return False
        try:
            self._pace_mutating_action()
            self._vk_for_peer(peer_id).messages.send(peer_id=int(peer_id), message=message, random_id=0)
            _record_action(platform="vk", zone=zone, action="send_welcome", status="ok", peer_id=peer_id, user_id=user_id, user_name=user_name, text=message)
            return True
        except Exception as error:
            _record_action(platform="vk", zone=zone, action="send_welcome", status="error", peer_id=peer_id, user_id=user_id, user_name=user_name, error=str(error), text=message)
            return False

    async def forward_to_log(
        self, user_id: int, peer_id: int, category: str, message_id: Any, *, cmid: Any = None
    ) -> None:
        try:
            name = self._get_user_name(int(user_id))
            chat_name = self._get_chat_title(int(peer_id)) or f"Chat {peer_id}"
            msg_link = f"https://vk.com/im?sel=c{int(peer_id) - 2000000000}"
            log_text = (
                f"🚨 [VK MODERATOR]\n"
                f"Type: {category.upper()}\n"
                f"User: {name} (id{user_id})\n"
                f"Chat: {chat_name}\n"
                f"CMID: {cmid or '—'}\n"
                f"Link: {msg_link}"
            )
            if _truthy(self.settings.get("dry_run")):
                _record_action(platform="vk", action="would_forward_to_log", category=category, status="dry_run", peer_id=peer_id, user_id=user_id, user_name=name, text=log_text)
                return
            params: dict[str, Any] = {
                "peer_id": int(self.log_chat_id),
                "message": log_text,
                "random_id": 0,
            }
            if cmid:
                params["forward"] = json.dumps(
                    {"peer_id": int(peer_id), "conversation_message_ids": [int(cmid)]},
                    ensure_ascii=False,
                )
            elif message_id:
                params["forward_messages"] = [int(message_id)]
            self._pace_mutating_action()
            self._vk_for_peer(peer_id).messages.send(**params)
            _record_action(platform="vk", action="forward_to_log", category=category, status="ok", peer_id=peer_id, user_id=user_id, user_name=name, text=log_text)
        except Exception as error:
            primary_error = str(error)
            try:
                fallback_text = locals().get("log_text") or (
                    f"🚨 [VK MODERATOR]\nType: {category.upper()}\nUser: id{user_id}\n"
                    f"Chat: {peer_id}\nCMID: {cmid or '—'}"
                )
                self._pace_mutating_action()
                self._vk_for_peer(peer_id).messages.send(
                    peer_id=int(self.log_chat_id), message=fallback_text, random_id=0
                )
                _record_action(
                    platform="vk", action="forward_to_log", category=category, status="ok",
                    peer_id=peer_id, user_id=user_id, text=fallback_text,
                    response_json={"fallback_without_forward": True, "forward_error": primary_error},
                )
            except Exception as fallback_error:
                _record_action(
                    platform="vk", action="forward_to_log", category=category, status="error",
                    peer_id=peer_id, user_id=user_id,
                    error=f"forward={primary_error}; fallback={fallback_error}",
                )

    async def process_chat_update(self, event: Any) -> None:
        if getattr(event, "type", None) != self._vk_event_type("CHAT_UPDATE"):
            return
        if getattr(event, "update_type", None) != self._vk_chat_event_type("USER_JOINED"):
            return
        peer_id = int(getattr(event, "peer_id", 0) or 0)
        if not peer_id:
            return
        zone = self.get_chat_zone(peer_id)
        title = self._get_chat_title(peer_id)
        if zone not in {"training_stream", "closed_club"}:
            if zone != "logs":
                _log(
                    "info",
                    "chat-moderators ignored vk chat update: peer_id=%s title=%r reason=title_not_matched",
                    peer_id,
                    title,
                )
            return
        info = getattr(event, "info", {}) or {}
        try:
            user_id = int(info.get("user_id") or 0)
        except Exception:
            user_id = 0
        if user_id <= 0 or user_id == self.own_id:
            return
        _upsert_chat(platform="vk", peer_id=peer_id, title=title, zone=zone)
        if not _chat_enabled(platform="vk", peer_id=peer_id):
            _record_action(platform="vk", zone=zone, action="skip_chat_disabled", status="ok", peer_id=peer_id, user_id=user_id)
            return
        await self._maybe_reconcile_chat_members(
            peer_id, zone=zone, trigger="chat_update_user_joined", force=True
        )

    async def process_message(self, event: Any) -> None:
        peer_id = int(getattr(event, "peer_id", 0) or 0)
        message_id = getattr(event, "message_id", None)
        if not peer_id:
            return
        if peer_id < 2000000000:
            user_id = self._extract_from_id(event) or peer_id
            if _vk_course_member_pending(user_id):
                result = await _senler_add_course_chat_users([int(user_id)])
                if not result.get("ok"):
                    await asyncio.sleep(3)
                    result = await _senler_add_course_chat_users([int(user_id)])
                _set_vk_course_member_senler_status(user_id, bool(result.get("ok")))
                _record_action(
                    platform="vk",
                    action="senler_private_message_retry",
                    status="ok" if result.get("ok") else "error",
                    peer_id=peer_id,
                    user_id=user_id,
                    response_json={
                        "status": result.get("status"),
                        "verified": result.get("verified", 0),
                        "errors": result.get("errors") or [],
                    },
                )
            return
        full_msg = getattr(event, "message_payload", None) or self._get_message_payload(message_id)
        zone = self.get_chat_zone(peer_id)
        title = self._get_chat_title(peer_id)
        if zone not in {"training_stream", "closed_club"}:
            return
        _record_action(
            platform="vk",
            zone=zone,
            action="incoming_message_seen",
            status="ok",
            peer_id=peer_id,
            message_id=message_id,
            user_id=self._extract_from_id(event),
            text=str(getattr(event, "text", "") or ""),
            request_json={"zone": zone, "title": title},
        )
        _upsert_chat(platform="vk", peer_id=peer_id, title=title, zone=zone)
        if zone in {"training_stream", "closed_club"} and isinstance(full_msg.get("action"), dict):
            action = full_msg.get("action") or {}
            if action.get("type") in {"chat_invite_user_by_link", "chat_invite_user"}:
                await self._maybe_reconcile_chat_members(
                    peer_id, zone=zone, trigger=str(action.get("type")), force=True
                )
            return
        text = str(getattr(event, "text", "") or "").strip()
        if not text:
            return
        from_id = self._extract_from_id(event)
        cmid = self._extract_cmid(event)
        if from_id == self.own_id or from_id <= 0:
            _record_action(platform="vk", zone=zone or "", action="skip_sender", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        if self._is_trusted_sender(from_id):
            _record_action(platform="vk", zone=zone, action="skip_trusted_sender", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        if zone not in {"training_stream", "closed_club"}:
            return
        if not _chat_enabled(platform="vk", peer_id=peer_id):
            _record_action(platform="vk", zone=zone, action="skip_chat_disabled", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        is_admin = await self.is_chat_admin(peer_id, from_id)
        if is_admin:
            _record_action(platform="vk", zone=zone, action="skip_admin", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        await self._moderate_regular_member_message(from_id=from_id, peer_id=peer_id, zone=zone, text=text, message_id=message_id, cmid=cmid)

    def _is_getcourse_trigger(self, text: str) -> bool:
        value = text or ""
        if not GETCOURSE_ACTION_RE.search(value):
            return False
        return bool(GETCOURSE_NICK_RE.search(value) or NexusGetCourseAccessService.EMAIL_RE.search(value) or NexusGetCourseAccessService.PHONE_RE.search(value))

    def _is_getcourse_state_trigger(self, text: str) -> bool:
        value = text or ""
        if not GETCOURSE_STATE_RE.search(value):
            return False
        return bool(GETCOURSE_NICK_RE.search(value) or NexusGetCourseAccessService.EMAIL_RE.search(value) or NexusGetCourseAccessService.PHONE_RE.search(value))

    def _build_access_help_reply(self, user_name: str) -> str:
        return (
            f"{user_name}, не понял команду.\n\n"
            "Пиши так:\n"
            "• Ник, выдай mail@example.com премиум щенок\n"
            "• Ник, закрой mail@example.com щенок\n"
            "• Ник, выдай mail@example.com 5 модуль щенка\n"
            "• Ник, сколько модулей открыто +79990000000"
        )

    def _build_access_reply(self, user_name: str, prepared: dict[str, Any]) -> str:
        return (
            f"{user_name}, собрал изменения.\n\n"
            f"{prepared['preview_text']}\n\n"
            "Если все верно, ответь: да. Если нет, ответь: нет."
        )

    def _build_access_result_reply(self, user_name: str, request: dict[str, Any], *, applied: bool, cancelled: bool = False, error_text: str = "") -> str:
        if cancelled:
            return f"{user_name}, запрос отменил."
        if error_text:
            return f"{user_name}, ошибка: {error_text}"
        before = {str(group.get("name") or "").strip() for group in request.get("current_groups") or [] if str(group.get("name") or "").strip()}
        after = {str(group.get("name") or "").strip() for group in request.get("target_groups") or [] if str(group.get("name") or "").strip()}
        added = [name for name in sorted(after - before)]
        removed = [name for name in sorted(before - after)]
        def bullets(values: list[str]) -> str:
            return "\n".join(f"• {value}" for value in values[:24]) if values else "• ничего"
        status = "готово, доступы обновил." if applied else "изменения не применены."
        return "\n".join([f"{user_name}, {status}", "", "Добавил:", bullets(added), "", "Убрал:", bullets(removed)])

    async def _handle_tech_spec_agent(self, event: Any, *, from_id: int, peer_id: int, text: str, cmid: int | None) -> None:
        if not _truthy(self.settings.get("vk_tech_agent_enabled")):
            _record_action(
                platform="vk",
                zone="tech_spec_agent",
                action="tech_agent_skipped_disabled",
                status="ok",
                peer_id=peer_id,
                message_id=getattr(event, "message_id", ""),
                cmid=cmid,
                user_id=from_id,
                text=text,
                request_json={"note": "vk_tech_agent_enabled=false"},
            )
            return
        user_name = self._get_user_name(from_id)
        lowered = text.lower().strip()
        key = (int(peer_id), int(from_id))
        with self.getcourse_lock:
            pending = self.getcourse_pending.get(key)
        if pending and lowered in YES_VALUES:
            with self.getcourse_lock:
                self.getcourse_pending.pop(key, None)
            try:
                self._send_reaction(peer_id=peer_id, cmid=pending.get("cmid"), reaction_id=VK_REACTION_IN_PROGRESS_ID)
                result = self.getcourse_service.apply_access_request(pending["request_id"])
                request = _gc_get_access_request(pending["request_id"]) or {}
                self._send_reaction(peer_id=peer_id, cmid=pending.get("cmid"), reaction_id=VK_REACTION_SUCCESS_ID)
                self._send_message(peer_id, self._build_access_result_reply(user_name, request, applied=True))
                _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_apply", status="ok" if not result.get("dry_run") else "dry_run", peer_id=peer_id, cmid=cmid, user_id=from_id, text=text, request_json={"request_id": pending["request_id"]}, response_json=result)
            except Exception as error:
                self._send_reaction(peer_id=peer_id, cmid=pending.get("cmid"), reaction_id=VK_REACTION_FAILURE_ID)
                self._send_message(peer_id, self._build_access_result_reply(user_name, pending, applied=False, error_text=str(error)))
                _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_apply", status="error", peer_id=peer_id, cmid=cmid, user_id=from_id, error=str(error), text=text, request_json={"request_id": pending.get("request_id")})
            return
        if pending and lowered in NO_VALUES:
            with self.getcourse_lock:
                self.getcourse_pending.pop(key, None)
            try:
                self.getcourse_service.cancel_access_request(pending["request_id"])
            except Exception:
                pass
            self._send_reaction(peer_id=peer_id, cmid=pending.get("cmid"), reaction_id=VK_REACTION_FAILURE_ID)
            self._send_message(peer_id, self._build_access_result_reply(user_name, pending, applied=False, cancelled=True))
            _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_cancel", status="ok", peer_id=peer_id, cmid=cmid, user_id=from_id, text=text, request_json={"request_id": pending.get("request_id")})
            return
        if self._is_getcourse_state_trigger(text):
            self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_IN_PROGRESS_ID)
            try:
                state = self.getcourse_service.describe_access_state(command_text=text)
                self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_SUCCESS_ID)
                self._send_message(peer_id, f"{user_name}, нашел клиента.\n{state['summary_text']}")
                _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_state", status="ok", peer_id=peer_id, cmid=cmid, user_id=from_id, text=text, response_json={"gc_user_id": state.get("gc_user_id")})
            except Exception as error:
                self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_FAILURE_ID)
                self._send_message(peer_id, f"{user_name}, ошибка: {error}")
                _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_state", status="error", peer_id=peer_id, cmid=cmid, user_id=from_id, error=str(error), text=text)
            return
        if not self._is_getcourse_trigger(text):
            if GETCOURSE_NICK_RE.search(text):
                self._send_message(peer_id, self._build_access_help_reply(user_name))
                _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_help", status="ok", peer_id=peer_id, cmid=cmid, user_id=from_id, text=text)
            return
        self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_IN_PROGRESS_ID)
        try:
            prepared = self.getcourse_service.prepare_access_request(
                command_text=text,
                requester_chat_id=f"vk:{peer_id}",
                requester_user_id=f"vk:{from_id}",
            )
            self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_SUCCESS_ID)
            preview_ref = self._send_message(peer_id, self._build_access_reply(user_name, prepared))
            pending_payload = {
                "request_id": prepared["request_id"],
                "created_at": time.time(),
                "preview_text": prepared["preview_text"],
                "gc_user_id": prepared["gc_user_id"],
                "current_groups": prepared.get("current_groups") or [],
                "target_groups": prepared.get("target_groups") or [],
                "cmid": cmid,
                "preview_message_id": preview_ref.get("message_id"),
                "preview_cmid": preview_ref.get("cmid"),
            }
            with self.getcourse_lock:
                self.getcourse_pending[key] = pending_payload
            _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_prepare", status="ok", peer_id=peer_id, cmid=cmid, user_id=from_id, text=text, request_json={"request_id": prepared["request_id"]}, response_json={"gc_user_id": prepared.get("gc_user_id"), "parsed": prepared.get("parsed_payload")})
        except Exception as error:
            self._send_reaction(peer_id=peer_id, cmid=cmid, reaction_id=VK_REACTION_FAILURE_ID)
            self._send_message(peer_id, f"{user_name}, ошибка: {error}")
            _record_action(platform="vk", zone="tech_spec_agent", action="getcourse_prepare", status="error", peer_id=peer_id, cmid=cmid, user_id=from_id, error=str(error), text=text)

    async def _moderate_regular_member_message(
        self,
        *,
        from_id: int,
        peer_id: int,
        zone: str,
        text: str,
        message_id: Any,
        cmid: Any,
    ) -> None:
        if "Сообщение не поддерживается" in text or text.startswith("source_act"):
            _record_action(platform="vk", zone=zone, action="skip_unsupported", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        if self._should_bypass_regular_moderation(from_id=from_id, text=text):
            _record_action(platform="vk", zone=zone, action="skip_trusted", status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
            return
        category = await self.analyzer.analyze_vk(text)
        _record_action(platform="vk", zone=zone, action="analyze", category=category, status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, text=text)
        try:
            user_name = self._get_user_name(from_id)
            user_mention = f"[id{from_id}|{user_name}]"
        except Exception:
            user_name = "Участник"
            user_mention = "Участник"
        if category in {"негатив", "скам", "удалить", "возврат"}:
            # VK cannot forward an already deleted conversation message (error 969).
            # Preserve the original in the moderator log before deleting it for all.
            await self.forward_to_log(from_id, peer_id, category, message_id, cmid=cmid)
            try:
                self._delete_chat_message(peer_id=peer_id, message_id=message_id, cmid=cmid)
                _record_action(platform="vk", zone=zone, action="delete", category=category, status="ok" if not _truthy(self.settings.get("dry_run")) else "dry_run", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, user_name=user_name, text=text)
            except Exception as error:
                _record_action(platform="vk", zone=zone, action="delete", category=category, status="error", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, user_name=user_name, error=str(error), text=text)
            if category == "возврат" and _template_enabled("vk_refund"):
                self._send_message(peer_id, _template_value("vk_refund").format(user_mention=user_mention))
            return
        if category == "техпод":
            await self.forward_to_log(from_id, peer_id, category, message_id, cmid=cmid)
            _record_action(platform="vk", zone=zone, action="tech_support_forward_only", category=category, status="ok", peer_id=peer_id, message_id=message_id, cmid=cmid, user_id=from_id, user_name=user_name, text=text)


class RuntimeManager:
    def __init__(self) -> None:
        self.analyzer = ModerationAnalyzer()
        self.telegram = TelegramModeratorRuntime(self.analyzer)
        self.vk = VKModeratorRuntime(self.analyzer)
        self.lock = asyncio.Lock()

    async def start(self, platform: str) -> dict[str, Any]:
        settings = _settings()
        if not _truthy(settings.get("runtime_enabled")):
            raise HTTPException(status_code=409, detail="runtime_enabled=false. Включите runtime вручную перед стартом.")
        async with self.lock:
            result: dict[str, Any] = {"ok": True, "started": []}
            if platform in {"telegram", "all"}:
                if not _setting_enabled(settings, "tg_enabled", "NEXUS_CHAT_MODERATORS_TG_ENABLED"):
                    result.setdefault("skipped", []).append({"platform": "telegram", "reason": "tg_enabled=false"})
                else:
                    try:
                        await self.telegram.start(settings)
                        _set_runtime_state("telegram", "running")
                        _record_action(platform="telegram", action="runtime_start", status="ok", request_json={"dry_run": _truthy(settings.get("dry_run"))})
                        result["started"].append("telegram")
                    except Exception as error:
                        _set_runtime_state("telegram", "error", error=str(error))
                        _record_action(platform="telegram", action="runtime_start", status="error", error=str(error))
                        raise
            if platform in {"vk", "all"}:
                if not _setting_enabled(settings, "vk_enabled", "NEXUS_CHAT_MODERATORS_VK_ENABLED"):
                    result.setdefault("skipped", []).append({"platform": "vk", "reason": "vk_enabled=false"})
                else:
                    try:
                        await self.vk.start(settings)
                        _set_runtime_state("vk", "running")
                        _record_action(platform="vk", action="runtime_start", status="ok", request_json={"dry_run": _truthy(settings.get("dry_run"))})
                        result["started"].append("vk")
                    except Exception as error:
                        _set_runtime_state("vk", "error", error=str(error))
                        _record_action(platform="vk", action="runtime_start", status="error", error=str(error))
                        raise
            return result

    async def stop(self, platform: str) -> dict[str, Any]:
        async with self.lock:
            stopped: list[str] = []
            if platform in {"telegram", "all"}:
                await self.telegram.stop()
                _set_runtime_state("telegram", "stopped")
                _record_action(platform="telegram", action="runtime_stop", status="ok")
                stopped.append("telegram")
            if platform in {"vk", "all"}:
                await self.vk.stop()
                _set_runtime_state("vk", "stopped")
                _record_action(platform="vk", action="runtime_stop", status="ok")
                stopped.append("vk")
            return {"ok": True, "stopped": stopped}

    def snapshot(self) -> dict[str, Any]:
        return {
            "telegram_running": bool(self.telegram.running),
            "vk_running": bool(self.vk.subscription and self.vk.subscription.running),
        }


def _runtime_or_error() -> RuntimeManager:
    if _runtime is None:
        raise HTTPException(status_code=503, detail="Runtime manager is not initialized")
    return _runtime


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID, "runtime_auto_start": True}


@router.get("/status")
async def status(request: Request):
    await _require_user(request)
    settings = _settings()
    runtime_snapshot = _runtime_or_error().snapshot()
    _sync_runtime_state_with_live_snapshot(
        runtime_snapshot,
        reason="Runtime is not active in current Nexus process",
    )
    with _db() as db:
        total_actions = db.execute("SELECT COUNT(*) c FROM moderation_actions").fetchone()["c"]
        recent_errors = db.execute(
            "SELECT COUNT(*) c FROM moderation_actions WHERE status='error' AND ts >= strftime('%Y-%m-%dT%H:%M:%SZ','now','-24 hours')"
        ).fetchone()["c"]
        chats_count = db.execute("SELECT COUNT(*) c FROM managed_chats").fetchone()["c"]
    return {
        "ok": True,
        "settings": settings,
        "secrets": _secret_status(),
        "prompt": _prompt_status(),
        "runtime": runtime_snapshot,
        "vk_poll": shared_vk_group_poll_hub.snapshot(),
        "runtime_state": _runtime_state(),
        "templates": _templates(),
        "stats": {
            "actions": int(total_actions),
            "recent_errors_24h": int(recent_errors),
            "managed_chats": int(chats_count),
        },
        "safety": {
            "auto_start": False,
            "runtime_enabled": _truthy(settings.get("runtime_enabled")),
            "dry_run": _truthy(settings.get("dry_run")),
        },
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    return {"ok": True, "settings": _settings()}


@router.post("/settings")
async def save_settings(request: Request):
    await _require_user(request)
    data = await request.json()
    values = dict(data) if isinstance(data, dict) else {}
    if sys.modules.get("_nexus_mod_staff-registry") is not None:
        current = _settings()
        for key in ("vk_allowed_admins", "vk_trusted_senders", "tg_allowed_adders"):
            if key in values:
                values[key] = current[key]
    settings = _save_settings(values)
    _record_action(platform="system", action="settings_update", status="ok", request_json={key: settings.get(key) for key in DEFAULT_SETTINGS})
    return {"ok": True, "settings": settings}


@router.get("/secrets/status")
async def secrets_status(request: Request):
    await _require_user(request)
    return {"ok": True, "items": _secret_status()}


@router.post("/secrets")
async def save_secrets(request: Request):
    await _require_user(request)
    data = await request.json()
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="JSON object expected")
    before = {item["key"]: item for item in _secret_status()}
    items = _save_secrets(data)
    changed: list[str] = []
    for item in items:
        previous = before.get(item["key"], {})
        if (
            item.get("module_present") != previous.get("module_present")
            or item.get("source") != previous.get("source")
            or item.get("masked") != previous.get("masked")
        ):
            changed.append(str(item["key"]))
    _record_action(
        platform="system",
        action="secrets_update",
        status="ok",
        request_json={"changed": changed, "ignored_empty": [key for key, value in data.items() if key in SECRET_SPECS and not str(value or "").strip()]},
    )
    return {"ok": True, "items": items, "changed": changed}


@router.get("/templates")
async def get_templates(request: Request):
    await _require_user(request)
    return {"ok": True, "items": _templates()}


@router.post("/templates")
async def save_templates(request: Request):
    await _require_user(request)
    data = await request.json()
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="JSON object expected")
    items = _save_templates(data)
    _record_action(platform="system", action="templates_update", status="ok", request_json={"keys": sorted(key for key in data if key in DEFAULT_TEMPLATES)})
    return {"ok": True, "items": items}


@router.post("/tech-agent/preview")
async def tech_agent_preview(request: Request):
    await _require_user(request)
    data = await request.json()
    text = str((data or {}).get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    try:
        prepared = NexusGetCourseAccessService().prepare_access_request(
            command_text=text,
            requester_chat_id=str((data or {}).get("requester_chat_id") or "api"),
            requester_user_id=str((data or {}).get("requester_user_id") or "api"),
        )
        _record_action(
            platform="system",
            zone="tech_spec_agent",
            action="getcourse_preview",
            status="ok",
            text=text,
            request_json={"request_id": prepared["request_id"]},
            response_json={"gc_user_id": prepared.get("gc_user_id"), "parsed": prepared.get("parsed_payload")},
        )
        return {"ok": True, **prepared}
    except GetCourseAccessError as error:
        _record_action(platform="system", zone="tech_spec_agent", action="getcourse_preview", status="error", error=str(error), text=text)
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/tech-agent/apply")
async def tech_agent_apply(request: Request):
    await _require_user(request)
    data = await request.json()
    request_id = str((data or {}).get("request_id") or "").strip()
    if not request_id:
        raise HTTPException(status_code=400, detail="request_id is required")
    try:
        result = NexusGetCourseAccessService().apply_access_request(request_id)
        _record_action(
            platform="system",
            zone="tech_spec_agent",
            action="getcourse_apply_api",
            status="dry_run" if result.get("dry_run") else "ok",
            request_json={"request_id": request_id},
            response_json=result,
        )
        return {"ok": True, "result": result, "request": _gc_get_access_request(request_id)}
    except GetCourseAccessError as error:
        _record_action(platform="system", zone="tech_spec_agent", action="getcourse_apply_api", status="error", error=str(error), request_json={"request_id": request_id})
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/tech-agent/cancel")
async def tech_agent_cancel(request: Request):
    await _require_user(request)
    data = await request.json()
    request_id = str((data or {}).get("request_id") or "").strip()
    if not request_id:
        raise HTTPException(status_code=400, detail="request_id is required")
    try:
        NexusGetCourseAccessService().cancel_access_request(request_id)
        _record_action(platform="system", zone="tech_spec_agent", action="getcourse_cancel_api", status="ok", request_json={"request_id": request_id})
        return {"ok": True, "request": _gc_get_access_request(request_id)}
    except GetCourseAccessError as error:
        _record_action(platform="system", zone="tech_spec_agent", action="getcourse_cancel_api", status="error", error=str(error), request_json={"request_id": request_id})
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.post("/runtime/start")
async def runtime_start(request: Request):
    await _require_user(request)
    data = await request.json()
    platform = str((data or {}).get("platform") or "all").strip().lower()
    if platform not in {"telegram", "vk", "all"}:
        raise HTTPException(status_code=400, detail="platform must be telegram, vk or all")
    try:
        return await _runtime_or_error().start(platform)
    except HTTPException:
        raise
    except Exception as error:
        return JSONResponse({"ok": False, "error": str(error)}, status_code=500)


@router.post("/vk/join/reconcile")
async def reconcile_vk_join(request: Request):
    await _require_user(request)
    data = await request.json()
    peer_id = _safe_int((data or {}).get("peer_id"), 0)
    user_id = _safe_int((data or {}).get("user_id"), 0)
    if peer_id <= 2000000000 or user_id <= 0:
        raise HTTPException(status_code=400, detail="peer_id and user_id are invalid")
    runtime = _runtime_or_error().vk
    if not runtime.subscription or not runtime.subscription.running:
        raise HTTPException(status_code=409, detail="VK runtime is not running")
    zone = runtime.get_chat_zone(peer_id)
    title = runtime._get_chat_title(peer_id)
    if zone not in {"training_stream", "closed_club"}:
        raise HTTPException(status_code=400, detail="Chat is not a managed course chat")
    members = runtime._vk_for_peer(peer_id).messages.getConversationMembers(peer_id=peer_id)
    member = next(
        (item for item in members.get("items") or [] if int(item.get("member_id") or 0) == user_id),
        None,
    )
    if member is None:
        raise HTTPException(status_code=409, detail="User is not a chat member")
    if _vk_member_is_admin(member):
        raise HTTPException(status_code=409, detail="Chat administrators are not students")
    _upsert_chat(platform="vk", peer_id=peer_id, title=title, zone=zone)
    sent = await runtime._send_welcome_message(peer_id, user_id, zone)
    if not sent:
        raise HTTPException(status_code=409, detail="Welcome message was not sent")
    with _db() as db:
        now = _now()
        db.execute(
            """INSERT INTO vk_member_roster(peer_id,user_id,is_admin,present,welcome_status,first_seen_at,last_seen_at)
               VALUES(?,?,0,1,'sent',?,?)
               ON CONFLICT(peer_id,user_id) DO UPDATE SET
                 present=1,is_admin=0,welcome_status='sent',last_seen_at=excluded.last_seen_at""",
            (str(peer_id), str(user_id), now, now),
        )
    runtime._schedule_joined_user_senler_sync(peer_id, user_id, zone, title)
    _record_action(
        platform="vk", zone=zone, action="join_reconcile", status="ok",
        peer_id=peer_id, user_id=user_id, request_json={"title": title},
    )
    return {"ok": True, "peer_id": peer_id, "user_id": user_id, "zone": zone, "title": title, "sent": True}


@router.post("/runtime/stop")
async def runtime_stop(request: Request):
    await _require_user(request)
    data = await request.json()
    platform = str((data or {}).get("platform") or "all").strip().lower()
    if platform not in {"telegram", "vk", "all"}:
        raise HTTPException(status_code=400, detail="platform must be telegram, vk or all")
    return await _runtime_or_error().stop(platform)


@router.get("/actions")
async def list_actions(
    request: Request,
    platform: str = "",
    action: str = "",
    status_value: str = "",
    q: str = "",
    limit: int = 100,
    offset: int = 0,
):
    await _require_user(request)
    limit = max(1, min(int(limit or 100), 300))
    offset = max(0, int(offset or 0))
    where: list[str] = [
        """(
            platform='system'
            OR (platform='telegram' AND (chat_id='' OR EXISTS (
                SELECT 1 FROM managed_chats mc
                WHERE mc.platform='telegram' AND mc.chat_id=moderation_actions.chat_id
                  AND mc.zone IN ('training_stream','closed_club')
            )))
            OR (platform='vk' AND (peer_id='' OR zone IN ('training_stream','closed_club')))
        )"""
    ]
    params: list[Any] = []
    if platform:
        where.append("platform=?")
        params.append(platform)
    if action:
        where.append("action=?")
        params.append(action)
    if status_value:
        where.append("status=?")
        params.append(status_value)
    if q:
        where.append("(text_preview LIKE ? OR user_id LIKE ? OR user_name LIKE ? OR peer_id LIKE ? OR chat_id LIKE ?)")
        needle = f"%{q}%"
        params.extend([needle, needle, needle, needle, needle])
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with _db() as db:
        total = db.execute(f"SELECT COUNT(*) c FROM moderation_actions {where_sql}", params).fetchone()["c"]
        rows = db.execute(
            f"""SELECT id,ts,platform,zone,chat_id,peer_id,message_id,cmid,user_id,user_name,action,category,status,error,text_preview
                FROM moderation_actions {where_sql}
                ORDER BY id DESC LIMIT ? OFFSET ?""",
            [*params, limit, offset],
        ).fetchall()
    return {"ok": True, "total": int(total), "items": [dict(row) for row in rows], "limit": limit, "offset": offset}


@router.get("/actions/{action_id}")
async def get_action(action_id: int, request: Request):
    await _require_user(request)
    with _db() as db:
        row = db.execute("SELECT * FROM moderation_actions WHERE id=?", (int(action_id),)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="action not found")
    item = dict(row)
    for key in ("request_json", "response_json"):
        try:
            item[key] = json.loads(item.get(key) or "{}")
        except Exception:
            item[key] = {}
    return {"ok": True, "item": item}


@router.get("/chats")
async def list_chats(request: Request, platform: str = ""):
    await _require_user(request)
    params: list[Any] = []
    where_parts = ["zone IN ('training_stream','closed_club')"]
    if platform:
        where_parts.append("platform=?")
        params.append(platform)
    where = "WHERE " + " AND ".join(where_parts)
    with _db() as db:
        rows = db.execute(
            f"SELECT * FROM managed_chats {where} ORDER BY last_seen_at DESC, id DESC LIMIT 500",
            params,
        ).fetchall()
    return {"ok": True, "items": [dict(row) for row in rows]}


@router.post("/chats/{chat_id}/toggle")
async def toggle_chat(chat_id: int, request: Request):
    await _require_user(request)
    data = await request.json()
    enabled = 1 if bool((data or {}).get("enabled")) else 0
    with _db() as db:
        row = db.execute("SELECT * FROM managed_chats WHERE id=?", (int(chat_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="chat not found")
        db.execute("UPDATE managed_chats SET enabled=? WHERE id=?", (enabled, int(chat_id)))
    _record_action(platform=row["platform"], action="chat_toggle", status="ok", chat_id=row["chat_id"], peer_id=row["peer_id"], request_json={"enabled": bool(enabled)})
    return {"ok": True, "id": chat_id, "enabled": bool(enabled)}


@router.post("/maintenance/history-prune")
async def history_prune(request: Request):
    await _require_user(request)
    settings = _settings()
    try:
        days = max(1, int(settings.get("history_retention_days") or 90))
    except Exception:
        days = 90
    with _db() as db:
        cur = db.execute(
            "DELETE FROM moderation_actions WHERE ts < strftime('%Y-%m-%dT%H:%M:%SZ','now', ?)",
            (f"-{days} days",),
        )
        deleted = int(cur.rowcount or 0)
    _record_action(platform="system", action="history_prune", status="ok", request_json={"days": days, "deleted": deleted})
    return {"ok": True, "deleted": deleted, "days": days}


@router.post("/debug/analyze")
async def debug_analyze(request: Request):
    await _require_user(request)
    data = await request.json()
    platform = str((data or {}).get("platform") or "vk").strip().lower()
    text = str((data or {}).get("text") or "")
    analyzer = _runtime_or_error().analyzer
    if platform == "telegram":
        category = await analyzer.analyze_tg(text)
    else:
        category = await analyzer.analyze_vk(text)
    action_id = _record_action(platform=platform, action="debug_analyze", category=category, status="ok", text=text)
    return {"ok": True, "category": category, "action_id": action_id}


_STAFF_VK_SETTING_KEYS = ("vk_allowed_admins", "vk_trusted_senders")
_STAFF_TG_SETTING_KEY = "tg_allowed_adders"
_STAFF_VK_CONFIG_TO_SETTING = {
    "allowed_admin": "vk_allowed_admins",
    "trusted_sender": "vk_trusted_senders",
}
_STAFF_VK_PROVIDERS = {"vk", "vkontakte", "vk.com"}
_STAFF_TG_PROVIDERS = {"telegram", "tg", "telegram.org"}


def _staff_vk_source_id(employee: dict[str, Any]) -> int | None:
    links = employee.get("source_links") if isinstance(employee, dict) else None
    value: Any = links.get(MODULE_ID) if isinstance(links, dict) else None
    if value is None and isinstance(links, dict):
        value = links.get(MODULE_ID.replace("-", "_"))
    if isinstance(value, dict):
        value = value.get("local_id")
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if raw.startswith("tg:"):
        return None
    if not raw.removeprefix("vk:").isdigit() or int(raw.removeprefix("vk:")) <= 0:
        raise ValueError(f"invalid {MODULE_ID} source_links local_id")
    return int(raw.removeprefix("vk:"))


def _staff_tg_source_id(employee: dict[str, Any]) -> int | None:
    links = employee.get("source_links") if isinstance(employee, dict) else None
    value: Any = links.get(MODULE_ID) if isinstance(links, dict) else None
    if isinstance(value, dict):
        value = value.get("local_id")
    raw = str(value or "").strip()
    return int(raw[3:]) if raw.startswith("tg:") and raw[3:].isdigit() and int(raw[3:]) > 0 else None


def _staff_vk_identity_ids(employee: dict[str, Any]) -> set[int]:
    if not isinstance(employee, dict):
        raise ValueError("employee должен быть объектом")
    result: set[int] = set()
    identities = employee.get("identities")
    for identity in identities if isinstance(identities, list) else []:
        if not isinstance(identity, dict):
            continue
        provider = _clean(identity.get("provider"), 40).casefold().replace("_", ".")
        if provider not in _STAFF_VK_PROVIDERS:
            continue
        external_id = _clean(identity.get("external_id") or identity.get("id"), 100)
        if not external_id.isdigit() or int(external_id) <= 0:
            raise ValueError("VK identity должен содержать точный положительный numeric ID")
        result.add(int(external_id))
    if len(result) > 1:
        raise ValueError("Точные VK identities указывают на разные аккаунты")
    return result


def _staff_tg_identity_ids(employee: dict[str, Any]) -> set[int]:
    result: set[int] = set()
    identities = employee.get("identities") if isinstance(employee, dict) else []
    for identity in identities if isinstance(identities, list) else []:
        if not isinstance(identity, dict):
            continue
        provider = _clean(identity.get("provider"), 40).casefold().replace("_", ".")
        if provider not in _STAFF_TG_PROVIDERS:
            continue
        external_id = _clean(identity.get("external_id") or identity.get("id"), 100)
        if not external_id.isdigit() or int(external_id) <= 0:
            raise ValueError("Telegram identity должен содержать точный положительный numeric ID")
        result.add(int(external_id))
    if len(result) > 1:
        raise ValueError("Точные Telegram identities указывают на разные аккаунты")
    return result


def _staff_vk_bool(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"false", "0", "no", "off", "нет"}:
            return False
        if normalized in {"true", "1", "yes", "on", "да"}:
            return True
    return bool(value)


def _staff_vk_config(vk_id: int, settings: dict[str, str] | None = None) -> dict[str, bool]:
    current = settings or _settings()
    return {
        config_key: vk_id in _int_set_csv(current.get(setting_key))
        for config_key, setting_key in _STAFF_VK_CONFIG_TO_SETTING.items()
    }


def _staff_vk_view(vk_id: int, settings: dict[str, str] | None = None, tg_id: int | None = None) -> dict[str, Any]:
    config = _staff_vk_config(vk_id, settings)
    config["telegram_allowed_adder"] = bool(tg_id and tg_id in _int_set_csv((settings or _settings()).get(_STAFF_TG_SETTING_KEY)))
    active = any(config.values())
    display_name = f"VK ID {vk_id}"
    return {
        "module_id": MODULE_ID,
        "local_id": str(vk_id),
        "full_name": display_name,
        "display_name": display_name,
        "identities": ([{"provider": "vk", "external_id": str(vk_id)}] + ([{"provider": "telegram", "external_id": str(tg_id)}] if tg_id else [])),
        "config": config,
        "active": active,
    }


def service_staff_connector() -> dict[str, Any]:
    fields = [
        {
            "key": "allowed_admin", "type": "boolean", "default": True,
            "label": "Считать администратором VK",
        },
        {
            "key": "trusted_sender", "type": "boolean", "default": True,
            "label": "Доверенный отправитель VK",
        },
        {
            "key": "telegram_allowed_adder", "type": "boolean", "default": False,
            "label": "Может добавлять Telegram-бота",
        },
    ]
    return {
        "ok": True,
        "module_id": MODULE_ID,
        "title": "Куратор учебных чатов",
        "label": "Куратор учебных чатов",
        "version": 1,
        "entity": "vk_identity",
        "operations": ["upsert", "deactivate"],
        "capabilities": ["list", "snapshot", "upsert", "deactivate"],
        "supports_deactivate": True,
        "matching": ["source_link", "exact_vk_identity", "exact_telegram_identity"],
        "fields": fields,
        "config_fields": fields,
        "config_schema": {field["key"]: {key: value for key, value in field.items() if key != "key"} for field in fields},
        "unmanaged_capabilities": [{
            "key": "course_chat_creator_vk_staff",
            "source_module": "course-chat-creator",
            "managed": False,
            "read_only": True,
            "effects": ["auto_promote_vk_chat_admin", "protect_from_moderation"],
        }],
    }


def service_staff_list() -> list[dict[str, Any]]:
    settings = _settings()
    user_ids: set[int] = set()
    for key in _STAFF_VK_SETTING_KEYS:
        user_ids.update(_int_set_csv(settings.get(key)))
    items = [_staff_vk_view(vk_id, settings) for vk_id in sorted(user_ids) if vk_id > 0]
    for tg_id in sorted(_int_set_csv(settings.get(_STAFF_TG_SETTING_KEY))):
        items.append({
            "module_id": MODULE_ID, "local_id": f"tg:{tg_id}",
            "full_name": f"Telegram ID {tg_id}", "display_name": f"Telegram ID {tg_id}",
            "identities": [{"provider": "telegram", "external_id": str(tg_id)}],
            "config": {"allowed_admin": False, "trusted_sender": False, "telegram_allowed_adder": True},
            "active": True,
        })
    return items


def service_staff_snapshot(*, employee: dict[str, Any]) -> dict[str, Any]:
    linked_id = _staff_vk_source_id(employee)
    linked_tg_id = _staff_tg_source_id(employee)
    identity_ids = _staff_vk_identity_ids(employee)
    tg_identity_ids = _staff_tg_identity_ids(employee)
    vk_id = linked_id or (next(iter(identity_ids)) if identity_ids else None)
    tg_id = linked_tg_id or (next(iter(tg_identity_ids)) if tg_identity_ids else None)
    if vk_id is None and tg_id is None:
        return {"ok": True, "module_id": MODULE_ID, "found": False, "local_id": "", "snapshot": None}
    settings = _settings()
    snapshot = _staff_vk_view(vk_id, settings, tg_id) if vk_id is not None else {
        "module_id": MODULE_ID, "local_id": f"tg:{tg_id}", "full_name": f"Telegram ID {tg_id}",
        "identities": [{"provider": "telegram", "external_id": str(tg_id)}],
        "config": {"allowed_admin": False, "trusted_sender": False, "telegram_allowed_adder": tg_id in _int_set_csv(settings.get(_STAFF_TG_SETTING_KEY))},
        "active": tg_id in _int_set_csv(settings.get(_STAFF_TG_SETTING_KEY)),
    }
    if not snapshot["active"]:
        return {"ok": True, "module_id": MODULE_ID, "found": False, "local_id": str(vk_id) if vk_id is not None else f"tg:{tg_id}", "snapshot": None}
    return {
        "ok": True, "module_id": MODULE_ID, "found": True,
        "local_id": str(vk_id) if vk_id is not None else f"tg:{tg_id}", "snapshot": snapshot,
    }


def service_staff_apply(
    *, employee: dict[str, Any], config: dict[str, Any], operation: str, idempotency_key: str = "",
) -> dict[str, Any]:
    del idempotency_key  # Desired-state writes are naturally idempotent.
    if not isinstance(employee, dict) or not isinstance(config, dict):
        raise ValueError("employee и config должны быть объектами")
    action = _clean(operation, 40).casefold()
    if action not in {"upsert", "deactivate"}:
        raise ValueError(f"{MODULE_ID} поддерживает только upsert и deactivate")

    linked_id = _staff_vk_source_id(employee)
    linked_tg_id = _staff_tg_source_id(employee)
    identity_ids = _staff_vk_identity_ids(employee)
    tg_identity_ids = _staff_tg_identity_ids(employee)
    identity_id = next(iter(identity_ids)) if identity_ids else None
    tg_identity_id = next(iter(tg_identity_ids)) if tg_identity_ids else None
    if action == "upsert" and identity_id is None and linked_id is None and tg_identity_id is None and linked_tg_id is None:
        raise ValueError(f"Для {MODULE_ID} требуется точный VK или Telegram identity")
    target_id = identity_id or linked_id
    affected_ids = {value for value in (linked_id, identity_id) if value is not None}
    target_tg_id = tg_identity_id or linked_tg_id
    if action == "upsert" and target_id is None and any(
        _staff_vk_bool(config.get(key)) for key in _STAFF_VK_CONFIG_TO_SETTING if key in config
    ):
        raise ValueError(f"Для VK-доступа {MODULE_ID} требуется точный VK identity")
    if action == "upsert" and target_tg_id is None and _staff_vk_bool(config.get("telegram_allowed_adder")):
        raise ValueError(f"Для Telegram-доступа {MODULE_ID} требуется точный Telegram identity")
    affected_tg_ids = {value for value in (linked_tg_id, tg_identity_id) if value is not None}
    now = _now()

    with _db() as db:
        db.execute("BEGIN IMMEDIATE")
        all_staff_keys = (*_STAFF_VK_SETTING_KEYS, _STAFF_TG_SETTING_KEY)
        rows = db.execute(
            "SELECT key,value FROM settings WHERE key IN (?,?,?)",
            all_staff_keys,
        ).fetchall()
        stored = {row["key"]: row["value"] for row in rows}
        sets = {
            key: _int_set_csv(stored.get(key, DEFAULT_SETTINGS[key]))
            for key in _STAFF_VK_SETTING_KEYS
        }
        before = {key: set(values) for key, values in sets.items()}
        tg_set = _int_set_csv(stored.get(_STAFF_TG_SETTING_KEY, DEFAULT_SETTINGS[_STAFF_TG_SETTING_KEY]))
        before_tg = set(tg_set)

        if action == "deactivate":
            for values in sets.values():
                values.difference_update(affected_ids)
            tg_set.difference_update(affected_tg_ids)
        else:
            basis_id = (
                linked_id
                if linked_id is not None and any(linked_id in before[key] for key in _STAFF_VK_SETTING_KEYS)
                else target_id
            )
            basis_known = basis_id is not None and any(basis_id in before[key] for key in _STAFF_VK_SETTING_KEYS)
            desired = {
                setting_key: _staff_vk_bool(config[config_key]) if config_key in config else (
                    basis_id in before[setting_key] if basis_known else bool(target_id)
                )
                for config_key, setting_key in _STAFF_VK_CONFIG_TO_SETTING.items()
            }
            if linked_id is not None and linked_id != target_id:
                for values in sets.values():
                    values.discard(linked_id)
            if target_id is not None:
                for key, enabled in desired.items():
                    if enabled:
                        sets[key].add(target_id)
                    else:
                        sets[key].discard(target_id)
            if target_tg_id is not None:
                tg_enabled = _staff_vk_bool(config.get("telegram_allowed_adder", target_tg_id in before_tg))
                if tg_enabled:
                    tg_set.add(target_tg_id)
                else:
                    tg_set.discard(target_tg_id)

        changed = any(before[key] != sets[key] for key in _STAFF_VK_SETTING_KEYS) or before_tg != tg_set
        if changed:
            for key in _STAFF_VK_SETTING_KEYS:
                value = ",".join(str(item) for item in sorted(sets[key]) if item > 0)
                db.execute(
                    "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                    (key, value, now),
                )
            tg_value = ",".join(str(item) for item in sorted(tg_set) if item > 0)
            db.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (_STAFF_TG_SETTING_KEY, tg_value, now),
            )

    settings = _settings()
    if _runtime is not None:
        _runtime.vk.settings = settings
        _runtime.telegram.settings = settings
    local_id = target_id or linked_id
    snapshot = _staff_vk_view(local_id, settings, target_tg_id) if local_id is not None else ({
        "module_id": MODULE_ID, "local_id": f"tg:{target_tg_id}",
        "config": {"allowed_admin": False, "trusted_sender": False, "telegram_allowed_adder": target_tg_id in _int_set_csv(settings.get(_STAFF_TG_SETTING_KEY))},
        "active": target_tg_id in _int_set_csv(settings.get(_STAFF_TG_SETTING_KEY)),
    } if target_tg_id is not None else None)
    return {
        "ok": True,
        "module_id": MODULE_ID,
        "operation": action,
        "local_id": str(local_id) if local_id is not None else (f"tg:{target_tg_id}" if target_tg_id is not None else ""),
        "changed": changed,
        "config": snapshot["config"] if snapshot else {},
        "snapshot": snapshot,
    }
