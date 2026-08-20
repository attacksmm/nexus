from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, quote, unquote_plus, urlparse
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from orchestrator.auth import can_access_module, verify_token_from_request
from orchestrator.telegram_proxy import httpx_client_kwargs, telegram_bot_api_base


router = APIRouter()

MODULE_ID = "getcourse-onboarding"
BOT_TOKEN_ENV = "GETCOURSE_ONBOARDING_TELEGRAM_BOT_TOKEN"
MAX_MESSAGE = 3900
RESPONSE_DAYS = 30
WORKER_SECONDS = 30
DELIVERY_WORKER_SECONDS = 10
MAINTENANCE_SECONDS = 6 * 60 * 60
DELIVERY_CONCURRENCY = 8
EVENT_RETENTION_DAYS = 180
UPGRADE_WORKER_SECONDS = 20
EMAIL_WORKER_SECONDS = 15
MOSCOW = ZoneInfo("Europe/Moscow")

_ctx = None
_db_path: Path | None = None
_logger: logging.Logger | None = None
_worker_task: asyncio.Task | None = None
_db_ready: asyncio.Event | None = None
_init_error = ""
_last_guard_check = 0.0
_last_guard_result: dict[str, Any] = {"ok": False, "status": "unchecked", "error": ""}
_last_flow_result: dict[str, Any] = {
    "ok": False, "status": "unchecked", "source": "", "stale": False,
    "items": 0, "errors": [], "checked_at": "",
}
_browser_install_lock: asyncio.Lock | None = None
_browser_action_lock: asyncio.Lock | None = None
_worker_state: dict[str, Any] = {
    "sync_started_at": "", "sync_success_at": "", "sync_error": "", "sync_duration_ms": 0,
    "delivery_started_at": "", "delivery_success_at": "", "delivery_error": "", "delivery_duration_ms": 0,
    "delivery_processed": 0, "source_stored": 0,
    "upgrade_sync_at": "", "upgrade_error": "", "upgrade_processed": 0,
    "upgrade_browser_checked_at": "", "upgrade_browser_error": "",
}


class DeliveryUncertain(RuntimeError):
    """The provider may have accepted a message, so automatic resend is unsafe."""


class TelegramDeliveryError(RuntimeError):
    """Telegram rejected a request before accepting the message."""

    def __init__(self, message: str, *, definitive: bool = False):
        super().__init__(message)
        self.definitive = definitive

DEFAULT_SETTINGS = {
    "enabled": "0",
    "delivery_mode": "test",
    "initialized": "0",
    "cursor_id": "0",
    "cursor_updated_at": "",
    "reminder_hours": "12",
    "support_due_minutes": "60",
    "public_base": "https://junior.sobakovod.pro/nexus",
    "video_instruction_url": "https://sobakovod.pro/instruction",
    "text_instruction_url": "",
    "upgrade_url": "",
    "standard_upgrade_url": "https://club.sobakovod.pro/doplata_premium?utm_medium=perevodpismo",
    "standard_upgrade_puppy_url": "https://club.sobakovod.pro/doplata_premium_puppy?utm_medium=perevodpismo",
    "standard_upgrade_dog_url": "https://club.sobakovod.pro/doplata_premium_dog?utm_medium=perevodpismo",
    "premium_upgrade_url": "https://club.sobakovod.pro/doplata_vip?utm_medium=perevodpismo",
    "webhook_fingerprint": "",
    "webhook_host": "",
    "bot_id": "",
    "bot_username": "",
    "salebot_help_secret": "",
    "last_sync_at": "",
    "last_sync_success_at": "",
    "last_sync_error": "",
    "last_delivery_success_at": "",
    "last_delivery_error": "",
    "source_max_id": "0",
    "source_max_updated_at": "",
    "vk_test_callback_key": "",
    "vk_test_callback_secret": "",
    "vk_test_callback_server_id": "",
    "vk_test_confirmation_code": "",
    "upgrade_enabled": "0",
    "upgrade_mode": "test",
    "upgrade_auto_approve": "0",
    "upgrade_cursor_id": "0",
    "upgrade_surcharge_offer_ids_dog": "",
    "upgrade_surcharge_offer_ids_puppy": "",
    "upgrade_surcharge_offer_ids_combo": "",
    "upgrade_target_dog_manual": "7673858",
    "upgrade_target_dog_autopay": "8043443",
    "upgrade_target_puppy_manual": "7846896",
    "upgrade_target_puppy_autopay": "7846827",
    "upgrade_target_combo_manual": "7846898",
    "upgrade_target_combo_autopay": "7846828",
    "upgrade_process_confirmed": "0",
    "upgrade_command_field": "Nexus: смена тарифа",
    "upgrade_link_field": "Nexus: связанный заказ",
    "upgrade_operation_field": "Nexus: операция",
    "upgrade_browser_enabled": "0",
    "upgrade_browser_base": "https://club.sobakovod.pro",
    "upgrade_browser_probe_hours": "6",
    "email_enabled": "0",
    "email_mode": "paused",
    "email_process_confirmed": "0",
    "email_trigger_group_name": "Nexus email запуск",
    "email_trigger_group_template": "Nexus email {package_key}",
    "email_callback_timeout_minutes": "30",
    "email_baseline_at": "",
    "email_callback_secret": "",
}

UPGRADE_CHAT = """{first_name}, доплата получена — тариф курса «{course}» повышен до Premium ✅

Теперь Вам доступны проверка домашних заданий и поддержка кураторов.

Вступайте в чаты Вашего потока {stream}:
Telegram: {tg_link}
ВКонтакте: {vk_link}

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

UPGRADE_CHAT_COMBO = """{first_name}, доплата получена — тариф курсов «Щенок + Собака» повышен до Premium ✅

Теперь Вам доступны проверка домашних заданий и поддержка кураторов.

Чаты курса «Первые шаги к воспитанию», поток {puppy_stream}:
Telegram: {puppy_tg_link}
ВКонтакте: {puppy_vk_link}

Чаты курса «Послушная собака», поток {dog_stream}:
Telegram: {dog_tg_link}
ВКонтакте: {dog_vk_link}

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

FLOW_TRANSITION_EMAIL = """Для Вас открыт следующий этап обучения — курс «{course}» ✅

Вступайте в чаты Вашего потока {stream}:
Telegram: {tg_link}
ВКонтакте: {vk_link}

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

WELCOME_MANAGER = """Поздравляем Вас с началом обучения!

Вам на почту уже ушло письмо с приглашением на обучающую платформу GetCourse с поддержкой кураторов.

Что необходимо сделать сейчас:
1. Откройте почту и найдите письмо от GetCourse.
2. Перейдите по ссылке из письма.
3. Если письма нет во входящих, проверьте папки «Спам» и «Промоакции» — иногда письма попадают туда.
4. Для просмотра уроков с телефона скачайте приложение GetCourse и войдите по той же почте, которую указывали при оплате.
5. Откройте школу «Современный собаковод», выберите свой курс и начните с нулевого модуля.

Анна Тимофеева записала короткую видеоинструкцию и показала весь процесс на экране: как скачать приложение GetCourse, войти в аккаунт и найти материалы курса.
Смотреть инструкцию {video_instruction_url}

Вступайте в закрытый учебный чат. Здесь Вы можете задавать вопросы, отправлять видео и делиться своими достижениями.
Telegram:
{tg_link}
ВКонтакте:
{vk_link}

ПРАВИЛА ЗАКРЫТОГО ЧАТА:
— В чате мы общаемся по вопросам, связанным с предстоящим обучением.
— Все вопросы по рассрочкам и оплатам курса адресуются мне в личные сообщения.
— Ненормативная лексика запрещена.
— Куратор или я будем стараться отвечать на Ваши вопросы не дольше двух часов.
— В чате запрещены сторонние ссылки.
— Воскресенье у кураторов — день тишины и выходной. Ученики чата продолжают общаться, но на вопросы кураторы ответят уже в понедельник.

ВАЖНО: в пакет VIP входят 8 созвонов с кураторами, которые необходимо использовать в течение четырёх месяцев с момента покупки курса. Срок может быть продлён по договорённости и по уважительным причинам, возникшим у ученика: болезнь, отпуск и другие обстоятельства.
Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

WELCOME_AUTOPAY_VIP = WELCOME_MANAGER

WELCOME_STANDARD = """Поздравляем Вас с началом обучения!

Вам на почту уже ушло письмо с приглашением на обучающую платформу GetCourse.

Что необходимо сделать сейчас:
1. Откройте почту и найдите письмо от GetCourse.
2. Перейдите по ссылке из письма.
3. Если письма нет во входящих, проверьте папки «Спам» и «Промоакции» — иногда письма попадают туда.
4. Для просмотра уроков с телефона скачайте приложение GetCourse и войдите по той же почте, которую указывали при оплате.
5. Откройте школу «Современный собаковод», выберите свой курс и начните с нулевого модуля.

Анна Тимофеева записала короткую видеоинструкцию и показала весь процесс на экране.
Смотреть видеоинструкцию: {video_instruction_url}

ВАЖНО
В течение ближайших суток Вам позвонит менеджер службы заботы школы «Современный собаковод».
Он убедится, что Вы получили доступ к курсу и смогли открыть материалы, дополнительно расскажет, как будет проходить обучение, и ответит на Ваши вопросы.
Пожалуйста, возьмите трубку, даже если курс уже открылся и всё получилось.

Обратите внимание на формат обучения
Вы приобрели тариф «Стандарт». Он включает пошаговые видеоуроки и бессрочный доступ к материалам курса, но предполагает самостоятельное обучение без проверки домашних заданий и обратной связи от кураторов-кинологов.

При необходимости Вы можете перейти на тариф с обратной связью.

В каких ситуациях куратор рекомендует тариф с обратной связью:
— собака из-за страха вообще не выходит на улицу, отказывается гулять или забивается под машины;
— собака не просто плохо остаётся одна, а громит квартиру, воет по шесть часов, ходит в туалет дома или наносит себе травмы;
— собака проявляет выраженную агрессию к другим собакам, уже роняет Вас на прогулке, а отвлечь и переключить её не получается — особенно если это сильная и тяжёлая собака;
— собака постоянно находится в движении, плохо спит, непрерывно прыгает и скачет, а успокоить её не получается;
— это Ваша первая собака сложной породы, например малинуа;
— проблем настолько много, что Вы можете перечислить их длинным списком из десяти-пятнадцати пунктов.

Перейти на тариф выше можно, доплатив разницу по ссылке: {upgrade_url}

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

WELCOME_MANAGER_STANDARD = """Поздравляем Вас с началом обучения!

Вам на почту уже ушло письмо с приглашением на обучающую платформу GetCourse.

Что необходимо сделать сейчас:
1. Откройте почту и найдите письмо от GetCourse.
2. Перейдите по ссылке из письма.
3. Если письма нет во входящих, проверьте папки «Спам» и «Промоакции» — иногда письма попадают туда.
4. Для просмотра уроков с телефона скачайте приложение GetCourse и войдите по той же почте, которую указывали при оплате.
5. Откройте школу «Современный собаковод», выберите свой курс и начните с нулевого модуля.

Анна Тимофеева записала короткую видеоинструкцию и показала весь процесс на экране.
Смотреть видеоинструкцию: {video_instruction_url}

ВАЖНО
В течение ближайших суток Вам позвонит менеджер службы заботы школы «Современный собаковод».
Он убедится, что Вы получили доступ к курсу и смогли открыть материалы, дополнительно расскажет, как будет проходить обучение, и ответит на Ваши вопросы.
Пожалуйста, возьмите трубку, даже если курс уже открылся и всё получилось.

Обратите внимание на формат обучения
Вы приобрели тариф «Стандарт». Он включает пошаговые видеоуроки и бессрочный доступ к материалам курса, но предполагает самостоятельное обучение без проверки домашних заданий и обратной связи от кураторов-кинологов.

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

WELCOME_PREMIUM = """Поздравляем Вас с началом обучения!

Вам на почту уже ушло письмо с приглашением на обучающую платформу GetCourse с поддержкой кураторов.

Что необходимо сделать сейчас:
1. Откройте почту и найдите письмо от GetCourse.
2. Перейдите по ссылке из письма.
3. Если письма нет во входящих, проверьте папки «Спам» и «Промоакции» — иногда письма попадают туда.
4. Для просмотра уроков с телефона скачайте приложение GetCourse и войдите по той же почте, которую указывали при оплате.
5. Откройте школу «Современный собаковод», выберите свой курс и начните с нулевого модуля.

Анна Тимофеева записала короткую видеоинструкцию и показала весь процесс на экране: как скачать приложение GetCourse, войти в аккаунт и найти материалы курса.
Смотреть инструкцию {video_instruction_url}

Вступайте в закрытый учебный чат. Здесь Вы можете задавать вопросы, отправлять видео и делиться своими достижениями.
Telegram:
{tg_link}
ВКонтакте:
{vk_link}

ПРАВИЛА ЗАКРЫТОГО ЧАТА:
— В чате мы общаемся по вопросам, связанным с предстоящим обучением.
— Все вопросы по рассрочкам и оплатам курса адресуются мне в личные сообщения.
— Ненормативная лексика запрещена.
— Куратор или я будем стараться отвечать на Ваши вопросы не дольше двух часов.
— В чате запрещены сторонние ссылки.
— Воскресенье у кураторов — день тишины и выходной. Ученики чата продолжают общаться, но на вопросы кураторы ответят уже в понедельник.

ВАЖНО
В течение ближайших суток Вам позвонит менеджер службы заботы школы «Современный собаковод».
Он убедится, что Вы получили доступ к курсу и смогли открыть материалы, дополнительно расскажет, как будет проходить обучение, и ответит на Ваши вопросы.
Пожалуйста, возьмите трубку, даже если курс уже открылся и всё получилось.

Обратите внимание на формат обучения
У вас будет обратная связь с нашими кураторами в общем чате и проверка домашних заданий. Но есть ряд случаев в поведении питомца, когда мы рекомендуем перейти на тариф «VIP», который включает личное сопровождение нашего куратора на индивидуальных созвонах.

При необходимости Вы можете повысить свой тариф до уровня VIP.

В каких ситуациях Анна Тимофеева рекомендует тариф VIP:
— собака из-за страха вообще не выходит на улицу, отказывается гулять или забивается под машины;
— собака не просто плохо остаётся одна, а громит квартиру, воет по шесть часов, ходит в туалет дома или наносит себе травмы;
— собака проявляет выраженную агрессию к другим собакам, уже роняет Вас на прогулке, а отвлечь и переключить её не получается — особенно если это сильная и тяжёлая собака;
— собака постоянно находится в движении, плохо спит, непрерывно прыгает и скачет, а успокоить её не получается;
— это Ваша первая собака сложной породы, например малинуа;
— проблем настолько много, что Вы можете перечислить их длинным списком из десяти-пятнадцати пунктов.

Перейти на тариф выше можно, доплатив разницу по ссылке: {upgrade_url}

Лёгкого и продуктивного обучения Вам и Вашему питомцу!"""

DEFAULT_TEMPLATES = {
    "manager": ("Продажа менеджера · Премиум/ВИП", WELCOME_MANAGER),
    "manager_standard": ("Продажа менеджера · Стандарт", WELCOME_MANAGER_STANDARD),
    "autopay_vip": ("Автооплата · VIP", WELCOME_AUTOPAY_VIP),
    "premium": ("Автооплата · Премиум → VIP", WELCOME_PREMIUM),
    "standard": ("Автооплата · Стандарт → Премиум", WELCOME_STANDARD),
    "reminder": (
        "Проверка доступа",
        "{first_name}, получилось войти в GetCourse и открыть нулевой модуль курса «{course}»?\n"
        "Нажмите один из вариантов ниже — так мы поймём, нужна ли вам помощь.",
    ),
    "yes_reply": (
        "Доступ подтверждён",
        "Отлично, доступ подтверждён ✅\nНачните с нулевого модуля — в нём находится вся организационная информация по обучению.",
    ),
    "no_reply": (
        "Нужна помощь",
        "Поняли, поможем разобраться.\nМы передали информацию в службу заботы.\n"
        "В ближайшее рабочее время вам позвонит наш специалист, поможет открыть курс и ответит на ваши вопросы.\n"
        "Пожалуйста, возьмите трубку.",
    ),
    "upgrade_premium": ("Доплата · Standard → Premium", UPGRADE_CHAT),
    "upgrade_premium_combo": ("Доплата · Standard → Premium · Щ+С", UPGRADE_CHAT_COMBO),
    "flow_transition": ("Переход к следующему курсу и потоку", FLOW_TRANSITION_EMAIL),
}

TEMPLATE_REQUIRED_VARIABLES = {
    "manager": {"video_instruction_url", "tg_link", "vk_link"},
    "manager_standard": {"video_instruction_url"},
    "autopay_vip": {"video_instruction_url", "tg_link", "vk_link"},
    "premium": {"video_instruction_url", "tg_link", "vk_link", "upgrade_url"},
    "standard": {"video_instruction_url", "upgrade_url"},
    "reminder": {"first_name", "course"},
    "yes_reply": set(),
    "no_reply": set(),
    "upgrade_premium": {"first_name", "course", "stream", "vk_link", "tg_link"},
    "upgrade_premium_combo": {
        "first_name", "puppy_stream", "puppy_vk_link", "puppy_tg_link",
        "dog_stream", "dog_vk_link", "dog_tg_link",
    },
    "flow_transition": {"course", "stream", "vk_link", "tg_link"},
}
TEMPLATE_ALLOWED_VARIABLES = {
    "first_name", "name", "course", "stream", "vk_link", "tg_link",
    "video_instruction_url", "text_instruction_url", "upgrade_url",
    "puppy_stream", "puppy_vk_link", "puppy_tg_link",
    "dog_stream", "dog_vk_link", "dog_tg_link",
}


def setup(ctx) -> None:
    global _ctx, _db_path, _logger, _worker_task, _db_ready, _init_error
    global _browser_install_lock, _browser_action_lock
    _ctx = ctx
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-onboarding"))
    _db_ready = asyncio.Event()
    _browser_install_lock = asyncio.Lock()
    _browser_action_lock = asyncio.Lock()
    _init_error = ""
    loop = asyncio.get_event_loop()
    if loop.is_running():
        lifecycle = getattr(ctx, "lifecycle", None)
        if lifecycle is not None:
            _worker_task = lifecycle.create_task(_module_supervisor(), name="getcourse-onboarding-supervisor")
        else:
            _worker_task = loop.create_task(_module_supervisor(), name="getcourse-onboarding-supervisor")
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        await asyncio.gather(_worker_task, return_exceptions=True)
    _worker_task = None


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("getcourse-onboarding is not initialized")
    return _db_path


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _now_dt()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> datetime | None:
    text = _clean(value, 100)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _module(module_id: str, service: str):
    module = sys.modules.get(f"_nexus_mod_{module_id}")
    if module is None or not hasattr(module, service):
        raise RuntimeError(f"Модуль {module_id} недоступен")
    return module


async def _connect() -> aiosqlite.Connection:
    if _db_ready is not None and not _db_ready.is_set():
        await asyncio.wait_for(_db_ready.wait(), timeout=30)
    if _init_error:
        raise RuntimeError(f"getcourse-onboarding database initialization failed: {_init_error}")
    db = await aiosqlite.connect(_must_db(), timeout=30)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA busy_timeout=30000")
    await db.execute("PRAGMA synchronous=FULL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def _init_db() -> None:
    global _init_error
    db = await aiosqlite.connect(_must_db(), timeout=30)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=FULL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS templates(
                key TEXT PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL,updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_record_id INTEGER NOT NULL DEFAULT 0,
                order_id TEXT NOT NULL,
                deal_number TEXT NOT NULL DEFAULT '',
                gc_user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',email TEXT NOT NULL DEFAULT '',phone TEXT NOT NULL DEFAULT '',
                paid_at TEXT NOT NULL,course_key TEXT NOT NULL,course TEXT NOT NULL,tariff TEXT NOT NULL,
                autopayment TEXT NOT NULL DEFAULT '',autopayment_source TEXT NOT NULL DEFAULT '',
                branch TEXT NOT NULL DEFAULT '',utm_term TEXT NOT NULL DEFAULT '',stream TEXT NOT NULL DEFAULT '',
                vk_link TEXT NOT NULL DEFAULT '',tg_link TEXT NOT NULL DEFAULT '',
                target_platform_id TEXT NOT NULL DEFAULT '',target_source TEXT NOT NULL DEFAULT '',
                manual_vk_platform_id TEXT NOT NULL DEFAULT '',manual_telegram_platform_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',error TEXT NOT NULL DEFAULT '',
                welcome_due_at TEXT NOT NULL,welcome_sent_at TEXT NOT NULL DEFAULT '',
                reminder_due_at TEXT NOT NULL,reminder_sent_at TEXT NOT NULL DEFAULT '',
                response TEXT NOT NULL DEFAULT '',responded_at TEXT NOT NULL DEFAULT '',
                amo_lead_id TEXT NOT NULL DEFAULT '',amo_task_id TEXT NOT NULL DEFAULT '',amo_note_id TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(order_id,course_key)
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_due ON orders(status,next_attempt_at,welcome_due_at,reminder_due_at);
            CREATE INDEX IF NOT EXISTS idx_onboarding_source ON orders(source_record_id);
            CREATE TABLE IF NOT EXISTS deliveries(
                id INTEGER PRIMARY KEY AUTOINCREMENT,order_row_id INTEGER NOT NULL,stage TEXT NOT NULL,
                operation_id TEXT NOT NULL UNIQUE,status TEXT NOT NULL,error TEXT NOT NULL DEFAULT '',
                message_ids_json TEXT NOT NULL DEFAULT '[]',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(order_row_id,stage)
            );
            CREATE TABLE IF NOT EXISTS response_tokens(
                order_row_id INTEGER PRIMARY KEY,token_hash TEXT NOT NULL UNIQUE,expires_at TEXT NOT NULL,created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS test_runs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL UNIQUE,
                recipient_ref TEXT NOT NULL,recipient_id TEXT NOT NULL DEFAULT '',requested_by TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'sending',results_json TEXT NOT NULL DEFAULT '[]',
                error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS interaction_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,event_type TEXT NOT NULL,
                order_row_id INTEGER,recipient_id TEXT NOT NULL DEFAULT '',choice TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,payload_json TEXT NOT NULL DEFAULT '{}',result_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_interactions_updated
                ON interaction_events(updated_at DESC,id DESC);
            CREATE TABLE IF NOT EXISTS upgrade_jobs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_record_id INTEGER NOT NULL DEFAULT 0,
                upgrade_order_id TEXT NOT NULL,
                upgrade_deal_number TEXT NOT NULL DEFAULT '',
                origin_order_id TEXT NOT NULL DEFAULT '',
                origin_deal_number TEXT NOT NULL DEFAULT '',
                gc_user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',email TEXT NOT NULL DEFAULT '',phone TEXT NOT NULL DEFAULT '',
                course_key TEXT NOT NULL DEFAULT '',autopayment INTEGER NOT NULL DEFAULT 0,
                source_offer_id TEXT NOT NULL DEFAULT '',target_offer_id TEXT NOT NULL DEFAULT '',
                source_cost REAL NOT NULL DEFAULT 0,source_payed REAL NOT NULL DEFAULT 0,
                upgrade_cost REAL NOT NULL DEFAULT 0,upgrade_payed REAL NOT NULL DEFAULT 0,
                origin_paid_at TEXT NOT NULL DEFAULT '',upgrade_paid_at TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'preview',approved INTEGER NOT NULL DEFAULT 0,
                operation_id TEXT NOT NULL DEFAULT '',strategy TEXT NOT NULL DEFAULT '',
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                replacement_order_id TEXT NOT NULL DEFAULT '',replacement_deal_number TEXT NOT NULL DEFAULT '',
                payment_id TEXT NOT NULL DEFAULT '',browser_journal TEXT NOT NULL DEFAULT '',
                browser_artifact TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at TEXT NOT NULL DEFAULT '',
                chat_sent_at TEXT NOT NULL DEFAULT '',completed_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(upgrade_order_id,course_key)
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_upgrades_due
                ON upgrade_jobs(approved,status,next_attempt_at,updated_at);
            CREATE TABLE IF NOT EXISTS upgrade_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,job_id INTEGER NOT NULL,stage TEXT NOT NULL,
                status TEXT NOT NULL,details_json TEXT NOT NULL DEFAULT '{}',error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES upgrade_jobs(id)
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_upgrade_events_job
                ON upgrade_events(job_id,id DESC);
            CREATE TABLE IF NOT EXISTS upgrade_alerts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,job_id INTEGER NOT NULL DEFAULT 0,
                alert_key TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',
                message TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,updated_at TEXT NOT NULL,sent_at TEXT NOT NULL DEFAULT '',
                UNIQUE(job_id,alert_key)
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_upgrade_alerts_updated
                ON upgrade_alerts(updated_at DESC,id DESC);
            CREATE TABLE IF NOT EXISTS email_packages(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gc_user_id TEXT NOT NULL,email TEXT NOT NULL DEFAULT '',phone TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',package_key TEXT NOT NULL,channel TEXT NOT NULL DEFAULT 'email',
                source_kind TEXT NOT NULL,source_id TEXT NOT NULL DEFAULT '',source_order_id TEXT NOT NULL DEFAULT '',
                course_key TEXT NOT NULL DEFAULT '',tariff TEXT NOT NULL DEFAULT '',stream TEXT NOT NULL DEFAULT '',
                vk_link TEXT NOT NULL DEFAULT '',tg_link TEXT NOT NULL DEFAULT '',template_key TEXT NOT NULL,
                subject TEXT NOT NULL,body TEXT NOT NULL,operation_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'held',hold_reason TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at TEXT NOT NULL DEFAULT '',
                triggered_at TEXT NOT NULL DEFAULT '',callback_at TEXT NOT NULL DEFAULT '',sent_at TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(gc_user_id,package_key,channel)
            );
            CREATE INDEX IF NOT EXISTS idx_onboarding_email_due
                ON email_packages(status,next_attempt_at,updated_at);
            CREATE TABLE IF NOT EXISTS email_recipient_holds(
                gc_user_id TEXT PRIMARY KEY,reason TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL
            );
            """
        )
        order_columns = {
            str(row[1]) for row in await (await db.execute("PRAGMA table_info(orders)")).fetchall()
        }
        if "autopayment" not in order_columns:
            await db.execute("ALTER TABLE orders ADD COLUMN autopayment TEXT NOT NULL DEFAULT ''")
        if "autopayment_source" not in order_columns:
            await db.execute("ALTER TABLE orders ADD COLUMN autopayment_source TEXT NOT NULL DEFAULT ''")
        if "amo_note_id" not in order_columns:
            await db.execute("ALTER TABLE orders ADD COLUMN amo_note_id TEXT NOT NULL DEFAULT ''")
        if "manual_vk_platform_id" not in order_columns:
            await db.execute("ALTER TABLE orders ADD COLUMN manual_vk_platform_id TEXT NOT NULL DEFAULT ''")
        if "manual_telegram_platform_id" not in order_columns:
            await db.execute("ALTER TABLE orders ADD COLUMN manual_telegram_platform_id TEXT NOT NULL DEFAULT ''")
        upgrade_columns = {
            str(row[1]) for row in await (await db.execute("PRAGMA table_info(upgrade_jobs)")).fetchall()
        }
        for name in (
            "strategy",
            "replacement_order_id", "replacement_deal_number", "payment_id",
            "browser_journal", "browser_artifact",
        ):
            if name not in upgrade_columns:
                await db.execute(
                    f"ALTER TABLE upgrade_jobs ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )
        now = _iso()
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        secret_row = await (await db.execute("SELECT value FROM settings WHERE key='salebot_help_secret'")).fetchone()
        if not _clean(secret_row[0] if secret_row else "", 300):
            await db.execute(
                "INSERT INTO settings(key,value) VALUES('salebot_help_secret',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (secrets.token_urlsafe(32),),
            )
        email_secret = await (await db.execute("SELECT value FROM settings WHERE key='email_callback_secret'")).fetchone()
        if not _clean(email_secret[0] if email_secret else "", 300):
            await db.execute(
                "INSERT INTO settings(key,value) VALUES('email_callback_secret',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (secrets.token_urlsafe(32),),
            )
        for key, (title, body) in DEFAULT_TEMPLATES.items():
            await db.execute(
                "INSERT OR IGNORE INTO templates(key,title,body,updated_at) VALUES(?,?,?,?)",
                (key, title, body, now),
            )
        await db.commit()
        _init_error = ""
    except Exception as exc:
        _init_error = _clean(exc, 1000)
        raise
    finally:
        await db.close()
        if _db_ready is not None:
            _db_ready.set()


async def _settings() -> dict[str, str]:
    db = await _connect()
    try:
        rows = await (await db.execute("SELECT key,value FROM settings")).fetchall()
    finally:
        await db.close()
    result = dict(DEFAULT_SETTINGS)
    result.update({str(row["key"]): str(row["value"] or "") for row in rows})
    return result


async def _set_settings(values: dict[str, Any]) -> dict[str, str]:
    allowed = {
        "enabled", "delivery_mode", "reminder_hours", "support_due_minutes", "public_base",
        "video_instruction_url", "text_instruction_url", "upgrade_url",
        "standard_upgrade_url", "standard_upgrade_puppy_url", "standard_upgrade_dog_url",
        "premium_upgrade_url",
        "upgrade_enabled", "upgrade_mode", "upgrade_auto_approve",
        "upgrade_surcharge_offer_ids_dog", "upgrade_surcharge_offer_ids_puppy",
        "upgrade_surcharge_offer_ids_combo",
        "upgrade_target_dog_manual", "upgrade_target_dog_autopay",
        "upgrade_target_puppy_manual", "upgrade_target_puppy_autopay",
        "upgrade_target_combo_manual", "upgrade_target_combo_autopay",
        "upgrade_process_confirmed", "upgrade_command_field", "upgrade_link_field",
        "upgrade_operation_field", "upgrade_browser_enabled", "upgrade_browser_base",
        "upgrade_browser_probe_hours",
        "email_enabled", "email_mode", "email_process_confirmed",
        "email_trigger_group_name", "email_trigger_group_template", "email_callback_timeout_minutes",
    }
    db = await _connect()
    try:
        for key in allowed:
            if key not in values:
                continue
            value = _clean(values.get(key), 4000)
            if key in {
                "enabled", "upgrade_enabled", "upgrade_auto_approve", "upgrade_process_confirmed",
                "upgrade_browser_enabled",
                "email_enabled", "email_process_confirmed",
            }:
                value = "1" if str(value).lower() in {"1", "true", "yes", "on"} else "0"
            elif key in {"delivery_mode", "upgrade_mode"}:
                if value not in {"test", "live"}:
                    raise HTTPException(400, f"{key}: допустимы test или live")
            elif key == "email_mode":
                if value not in {"paused", "test", "live"}:
                    raise HTTPException(400, "email_mode: допустимы paused, test или live")
            elif key == "reminder_hours":
                value = str(max(1, min(72, int(float(value or 12)))))
            elif key == "support_due_minutes":
                value = str(max(5, min(43200, int(float(value or 60)))))
            elif key == "upgrade_browser_probe_hours":
                value = str(max(1, min(24, int(float(value or 6)))))
            elif key == "email_callback_timeout_minutes":
                value = str(max(5, min(1440, int(float(value or 30)))))
            elif key == "email_trigger_group_name":
                if not value.startswith("Nexus email "):
                    raise HTTPException(400, "Группа email-триггера должна начинаться с «Nexus email »")
            elif key == "email_trigger_group_template":
                if "{package_key}" not in value:
                    raise HTTPException(400, "Шаблон email-группы должен содержать {package_key}")
                if not _email_trigger_group("puppy:premium-entry", {key: value}).startswith("Nexus email "):
                    raise HTTPException(400, "Email-группы должны начинаться с «Nexus email »")
            elif key.endswith("_url") or key == "public_base":
                if value and not re.match(r"^https://", value, re.I):
                    raise HTTPException(400, f"{key}: требуется HTTPS URL")
                value = value.rstrip("/") if key == "public_base" else value
            elif key == "upgrade_browser_base":
                if value.rstrip("/") != "https://club.sobakovod.pro":
                    raise HTTPException(400, "Разрешён только https://club.sobakovod.pro")
                value = value.rstrip("/")
            elif key.startswith("upgrade_target_"):
                if not value.isdigit():
                    raise HTTPException(400, f"{key}: требуется числовой ID предложения")
            elif key.startswith("upgrade_surcharge_offer_ids_"):
                values_list = [item for item in re.split(r"[\s,;]+", value) if item]
                if any(not item.isdigit() for item in values_list):
                    raise HTTPException(400, f"{key}: укажите ID через запятую")
                value = ",".join(dict.fromkeys(values_list))
            elif key in {"upgrade_command_field", "upgrade_link_field", "upgrade_operation_field"}:
                if not value:
                    raise HTTPException(400, f"{key}: имя поля не может быть пустым")
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    finally:
        await db.close()
    return await _settings()


async def _setting_updates(values: dict[str, str]) -> None:
    db = await _connect()
    try:
        for key, value in values.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, _clean(value, 4000)),
            )
        await db.commit()
    finally:
        await db.close()


def _browser_root() -> Path:
    root = _must_db().parent / "getcourse-browser"
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    return root


def _browser_python() -> Path:
    return _browser_root() / "venv" / "bin" / "python"


def _browser_state_path() -> Path:
    return _browser_root() / "storage-state.json"


def _browser_artifacts_dir() -> Path:
    path = _browser_root() / "artifacts"
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def _browser_journal_path(operation_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", _clean(operation_id, 100)) or "operation"
    path = _browser_root() / "journals" / f"{safe}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    return path


def _repair_payment_checkpoint(job: dict[str, Any]) -> dict[str, Any]:
    """Read only an exact, private post-transfer checkpoint for this repair."""

    operation_id = f"repair-{_clean(job.get('operation_id'), 100)}"
    expected_path = _browser_journal_path(operation_id).resolve()
    saved_path = Path(_clean(job.get("browser_journal"), 4000) or expected_path)
    try:
        if saved_path.resolve() != expected_path or not expected_path.is_file():
            return {}
        payload = json.loads(expected_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict) or _clean(payload.get("step"), 40) not in {"payment_moved", "recalculated"}:
        return {}
    target_url = _clean(payload.get("target_order_url"), 1000)
    parsed = urlparse(target_url)
    target_match = re.fullmatch(r"/sales/control/deal/update/id/(\d+)", parsed.path)
    checks = (
        _clean(payload.get("operation_id"), 100) == operation_id,
        _clean(payload.get("source_order_id"), 100) == _clean(job.get("origin_order_id"), 100),
        _clean(payload.get("source_deal_number"), 100) == _clean(job.get("origin_deal_number"), 100),
        _clean(payload.get("target_deal_number"), 100) == _replacement_deal_number(job),
        parsed.scheme == "https" and parsed.netloc == "club.sobakovod.pro" and bool(target_match),
        _clean(payload.get("payment_id"), 100).isdigit(),
        _upgrade_money_matches(job.get("source_payed"), payload.get("expected_amount")),
    )
    return payload if all(checks) else {}


async def _command_result(command: list[str], *, timeout: int) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        return 124, "Команда превысила допустимое время"
    return int(process.returncode or 0), output.decode("utf-8", errors="replace")[-8000:]


async def _browser_status() -> dict[str, Any]:
    python = _browser_python()
    ready = False
    error = ""
    if python.is_file():
        code, output = await _command_result(
            [str(python), "-c", "import playwright; print('ok')"], timeout=20,
        )
        ready = code == 0 and "ok" in output
        if not ready:
            error = _clean(output, 1000)
    state = _browser_state_path()
    return {
        "ready": ready,
        "session_loaded": state.is_file(),
        "session_updated_at": _iso(datetime.fromtimestamp(state.stat().st_mtime, timezone.utc)) if state.is_file() else "",
        "error": error,
        "checked_at": _worker_state.get("upgrade_browser_checked_at", ""),
        "last_error": _worker_state.get("upgrade_browser_error", ""),
    }


async def _install_browser_runtime() -> dict[str, Any]:
    global _browser_install_lock
    if _browser_install_lock is None:
        _browser_install_lock = asyncio.Lock()
    async with _browser_install_lock:
        root = _browser_root()
        python = _browser_python()
        steps: list[dict[str, Any]] = []
        if not python.is_file():
            code, output = await _command_result(
                [sys.executable, "-m", "venv", str(root / "venv")], timeout=180,
            )
            steps.append({"step": "venv", "code": code, "output": _clean(output, 1000)})
            if code:
                raise RuntimeError("Не удалось создать изолированное окружение браузера")
        code, output = await _command_result(
            [str(python), "-m", "pip", "install", "playwright>=1.52,<2"], timeout=600,
        )
        steps.append({"step": "python", "code": code, "output": _clean(output, 1000)})
        if code:
            raise RuntimeError("Не удалось установить Playwright")
        code, output = await _command_result(
            [str(python), "-m", "playwright", "install", "chromium"], timeout=900,
        )
        steps.append({"step": "chromium", "code": code, "output": _clean(output, 1000)})
        if code:
            raise RuntimeError("Не удалось установить Chromium")
        return {"ok": True, "status": await _browser_status(), "steps": steps}


def _valid_browser_storage_state(value: Any) -> dict[str, Any]:
    state = value if isinstance(value, dict) else {}
    cookies = state.get("cookies") if isinstance(state.get("cookies"), list) else []
    origins = state.get("origins") if isinstance(state.get("origins"), list) else []
    if not cookies:
        raise HTTPException(400, "Файл не содержит cookies GetCourse")
    if len(cookies) > 500 or len(origins) > 100:
        raise HTTPException(400, "Файл сессии слишком большой")
    allowed_cookie = any(
        isinstance(item, dict)
        and _clean(item.get("domain"), 300).lstrip(".").endswith("sobakovod.pro")
        and _clean(item.get("name"), 300)
        and _clean(item.get("value"), 10000)
        for item in cookies
    )
    if not allowed_cookie:
        raise HTTPException(400, "В файле нет сессии club.sobakovod.pro")
    filtered_cookies = [
        item for item in cookies
        if isinstance(item, dict) and _clean(item.get("domain"), 300).lstrip(".").endswith("sobakovod.pro")
    ]
    filtered_origins = [
        item for item in origins
        if isinstance(item, dict) and _clean(item.get("origin"), 1000).rstrip("/") == "https://club.sobakovod.pro"
    ]
    return {"cookies": filtered_cookies, "origins": filtered_origins}


async def _run_browser_action(payload: dict[str, Any], *, timeout: int = 150) -> dict[str, Any]:
    global _browser_action_lock
    if _browser_action_lock is None:
        _browser_action_lock = asyncio.Lock()
    status = await _browser_status()
    if not status["ready"]:
        raise RuntimeError("Браузер GetCourse не установлен в модуле Nexus")
    if not status["session_loaded"]:
        raise RuntimeError("Сессия администратора GetCourse не загружена")
    operation_id = _clean(payload.get("operation_id"), 100) or "probe"
    job_file = _browser_root() / "jobs" / f"{re.sub(r'[^A-Za-z0-9_.-]+', '-', operation_id)}.json"
    job_file.parent.mkdir(parents=True, exist_ok=True)
    job_file.parent.chmod(0o700)
    complete_payload = {
        **payload,
        "base_url": "https://club.sobakovod.pro",
        "storage_state": str(_browser_state_path()),
        "journal_path": str(_browser_journal_path(operation_id)),
        "artifacts_dir": str(_browser_artifacts_dir()),
    }
    job_file.write_text(json.dumps(complete_payload, ensure_ascii=False), encoding="utf-8")
    job_file.chmod(0o600)
    runner = Path(__file__).with_name("gc_browser_action.py")
    try:
        async with _browser_action_lock:
            code, output = await _command_result(
                [str(_browser_python()), str(runner), "--payload", str(job_file)], timeout=timeout,
            )
    finally:
        try:
            job_file.unlink()
        except FileNotFoundError:
            pass
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Браузер вернул непонятный результат: {_clean(output, 500)}") from exc
    if not isinstance(result, dict):
        raise RuntimeError("Браузер вернул некорректный результат")
    if code and result.get("ok"):
        result["ok"] = False
        result["error"] = "Браузер завершился с ненулевым кодом"
    _worker_state["upgrade_browser_checked_at"] = _iso()
    _worker_state["upgrade_browser_error"] = _clean(result.get("error"), 1000) if not result.get("ok") else ""
    return result


async def service_getcourse_browser_access_snapshot(*, gc_user_id: str) -> dict[str, Any]:
    """Read current groups through the existing authenticated upgrade browser."""

    user_id = _clean(gc_user_id, 100)
    if not user_id.isdigit():
        return {"ok": False, "error": "GetCourse ID не найден", "groups": []}
    result = await _run_browser_action(
        {
            "action": "read_access",
            "operation_id": f"access-read-{user_id}-{int(time.time())}",
            "gc_user_id": user_id,
        },
        timeout=45,
    )
    if not result.get("ok"):
        return {
            "ok": False,
            "error": _clean(result.get("error"), 1000) or "Браузер GetCourse не прочитал доступы",
            "groups": [],
        }
    return {
        "ok": True,
        "gc_user_id": user_id,
        "groups": [
            {"group_id": _clean(item.get("group_id"), 30), "name": _clean(item.get("name"), 500)}
            for item in result.get("groups") or []
            if _clean(item.get("group_id"), 30).isdigit() and _clean(item.get("name"), 500)
        ],
        "source": "browser",
        "updated_at": _clean(result.get("checked_at"), 40) or _iso(),
    }


async def _send_upgrade_alert(job: dict[str, Any] | None, kind: str, message: str, *, repeat_hours: int = 6) -> None:
    job_id = int((job or {}).get("id") or 0)
    bucket = int(_now_dt().timestamp()) // max(3600, int(repeat_hours) * 3600)
    alert_key = f"{_clean(kind, 80)}:{bucket}"
    text = _clean(message, 3500)
    now = _iso()
    db = await _connect()
    try:
        try:
            await db.execute(
                "INSERT INTO upgrade_alerts(job_id,alert_key,status,message,created_at,updated_at) VALUES(?,?,'sending',?,?,?)",
                (job_id, alert_key, text, now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            return
    finally:
        await db.close()

    token = _clean(
        os.environ.get("TELEGRAM_BOT_TOKEN_ERROR_ALERT")
        or os.environ.get("TELEGRAM_BOT_TOKEN")
        or os.environ.get(BOT_TOKEN_ENV),
        500,
    )
    chat_id = _clean(os.environ.get("TELEGRAM_CHAT_ID_ERROR_ALERT"), 100)
    error = ""
    if not token or not chat_id:
        error = "Не настроены TELEGRAM_BOT_TOKEN_ERROR_ALERT/TELEGRAM_CHAT_ID_ERROR_ALERT"
    else:
        try:
            async with httpx.AsyncClient(**httpx_client_kwargs(timeout=httpx.Timeout(20, connect=10))) as client:
                response = await client.post(
                    f"{telegram_bot_api_base().rstrip('/')}/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
                )
            body = response.json() if response.content else {}
            if response.status_code >= 400 or not body.get("ok"):
                raise RuntimeError(f"Telegram HTTP {response.status_code}: {_clean(body.get('description'), 500)}")
        except Exception as exc:
            error = _clean(exc, 1000)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE upgrade_alerts SET status=?,error=?,sent_at=?,updated_at=? WHERE job_id=? AND alert_key=?",
            ("failed" if error else "sent", error, "" if error else _iso(), _iso(), job_id, alert_key),
        )
        await db.commit()
    finally:
        await db.close()
    if error and _logger:
        _logger.error("GetCourse upgrade alert delivery failed: %s", error)


async def service_system_alert(*, kind: str, message: str, repeat_hours: int = 6) -> dict[str, Any]:
    """Shared deduplicated operator alert for the GetCourse module family."""

    await _send_upgrade_alert(
        None,
        _clean(kind, 80) or "system",
        _clean(message, 3500),
        repeat_hours=max(1, min(24, int(repeat_hours or 6))),
    )
    return {"ok": True}


async def _template(key: str) -> str:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT body FROM templates WHERE key=?", (key,))).fetchone()
    finally:
        await db.close()
    body = _template_body(key, row["body"] if row else "")
    error = _template_validation_error(key, body)
    if error:
        raise RuntimeError(f"Шаблон «{DEFAULT_TEMPLATES.get(key, (key, ''))[0]}»: {error}")
    return body


def _template_body(key: str, body: Any) -> str:
    text = _clean(body, 30000)
    if key == "standard":
        text = re.sub(
            r"(?im)^[^\n]*смотреть\s+текстовую\s+инструкцию[^\n]*(?:\n|$)",
            "",
            text,
        )
        text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _template_validation_error(key: str, body: str) -> str:
    variables = set(re.findall(r"\{([a-z_][a-z0-9_]*)\}", body, flags=re.I))
    missing = sorted(TEMPLATE_REQUIRED_VARIABLES.get(key, set()) - variables)
    unknown = sorted(variables - TEMPLATE_ALLOWED_VARIABLES)
    problems = []
    if missing:
        problems.append("нет обязательных переменных: " + ", ".join("{" + item + "}" for item in missing))
    if unknown:
        problems.append("неизвестные переменные: " + ", ".join("{" + item + "}" for item in unknown))
    return "; ".join(problems)


def _token() -> str:
    return _clean(os.environ.get(BOT_TOKEN_ENV), 4000)


async def _tg_call(method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    token = _token()
    if not token:
        raise RuntimeError(f"{BOT_TOKEN_ENV} не настроен")
    async with httpx.AsyncClient(**httpx_client_kwargs(timeout=httpx.Timeout(30, connect=12))) as client:
        response = await client.post(
            f"{telegram_bot_api_base()}/bot{token}/{method}",
            json=payload or {},
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise TelegramDeliveryError(
            f"Telegram HTTP {response.status_code}: invalid JSON",
            definitive=response.status_code < 500,
        ) from exc
    if response.status_code >= 400 or not body.get("ok"):
        raise TelegramDeliveryError(
            f"Telegram {body.get('error_code') or response.status_code}: {_clean(body.get('description') or body, 500)}",
            definitive=True,
        )
    return body.get("result") if isinstance(body.get("result"), dict) else {"result": body.get("result")}


def _webhook_view(info: dict[str, Any]) -> dict[str, str]:
    url = _clean(info.get("url"), 4000)
    parsed = urlparse(url)
    return {
        "fingerprint": hashlib.sha256(url.encode()).hexdigest() if url else "",
        "host": parsed.hostname or "",
        "masked": f"https://{parsed.hostname}/••••" if parsed.hostname else "не установлен",
    }


async def _telegram_preflight() -> dict[str, Any]:
    me, webhook = await asyncio.gather(_tg_call("getMe"), _tg_call("getWebhookInfo"))
    view = _webhook_view(webhook)
    return {
        "ok": bool(me.get("id") and view["fingerprint"]),
        "bot_id": _clean(me.get("id"), 80),
        "bot_username": _clean(me.get("username"), 200),
        "webhook": view,
        "pending_update_count": int(webhook.get("pending_update_count") or 0),
        "last_error_message": _clean(webhook.get("last_error_message"), 500),
    }


async def _webhook_guard(force: bool = False) -> dict[str, Any]:
    global _last_guard_check, _last_guard_result
    loop = asyncio.get_running_loop()
    now = loop.time()
    if not force and now - _last_guard_check < 300:
        return dict(_last_guard_result)
    _last_guard_check = now
    try:
        current = await _telegram_preflight()
        settings = await _settings()
        expected = settings.get("webhook_fingerprint", "")
        actual = current["webhook"]["fingerprint"]
        if not expected:
            result = {**current, "ok": False, "status": "unconfirmed", "error": "Webhook Senler не подтверждён"}
        elif not secrets.compare_digest(expected, actual):
            result = {**current, "ok": False, "status": "changed", "error": "Webhook Telegram изменился"}
        else:
            result = {**current, "ok": True, "status": "ready", "error": ""}
    except Exception as exc:
        result = {"ok": False, "status": "error", "error": _clean(exc, 500)}
    _last_guard_result = result
    return dict(result)


def _truthy(value: Any) -> bool:
    return _clean(value, 40).casefold() in {"1", "true", "yes", "да", "on", "автооплата"}


def _scenario_branch(item: dict[str, Any]) -> str:
    tariff = _clean(item.get("tariff"), 40).casefold()
    sale = "autopay" if _truthy(item.get("autopayment")) else "manager"
    package = "standard" if tariff == "standard" else "premium"
    return f"{sale}_{package}"


def _template_key_for_order(row: dict[str, Any]) -> str:
    tariff = _clean(row.get("tariff"), 40).casefold()
    manager_sale = _clean(row.get("branch"), 40).startswith("manager_")
    if manager_sale:
        return "manager_standard" if tariff == "standard" else "manager"
    if tariff == "standard":
        return "standard"
    if tariff == "premium":
        return "premium"
    return "autopay_vip"


def _reminder_enabled(row: dict[str, Any]) -> bool:
    return _clean(row.get("branch"), 40).startswith("autopay_")


def _standard_order(row: dict[str, Any]) -> bool:
    return _clean(row.get("tariff"), 40).casefold() == "standard"


async def _baseline_source() -> None:
    fields = _module("getcourse-chat-fields", "service_paid_course_orders")
    data = await fields.service_paid_course_orders(after_source_record_id=0, limit=1)
    if not data.get("ok"):
        raise RuntimeError(data.get("error") or "Источник GetCourse недоступен")
    await _setting_updates(
        {
            "initialized": "1",
            "cursor_id": str(int(data.get("max_updated_id") or data.get("max_source_record_id") or 0)),
            "cursor_updated_at": _clean(data.get("max_updated_at"), 100),
            "last_sync_at": _iso(),
        }
    )


async def _upsert_order(item: dict[str, Any], reminder_hours: int, *, initial_status: str = "") -> None:
    paid = _parse_dt(item.get("paid_at")) or _now_dt()
    flow = item.get("flow") if isinstance(item.get("flow"), dict) else {}
    # Sale source is carried from the same GetCourse rule used by
    # getcourse-amocrm; amoCRM responsibility does not define the sale source.
    branch = ""
    status = initial_status or "classification_needed"
    now = _iso()
    values = (
        int(item.get("source_record_id") or 0), _clean(item.get("order_id"), 100),
        _clean(item.get("deal_number"), 100), _clean(item.get("gc_user_id"), 100),
        _clean(item.get("name"), 300), _clean(item.get("email"), 300), _clean(item.get("phone"), 100),
        _iso(paid), _clean(item.get("course_key"), 50), _clean(item.get("course"), 100),
        _clean(item.get("tariff"), 40), "1" if _truthy(item.get("autopayment")) else "0",
        _clean(item.get("autopayment_source"), 40), branch, _clean(item.get("utm_term"), 1000),
        _clean(flow.get("stream"), 50), _clean(flow.get("vk_link"), 2000), _clean(flow.get("tg_link"), 2000),
        status, _iso(paid), _iso(paid + timedelta(hours=reminder_hours)), now, now,
    )
    db = await _connect()
    try:
        await db.execute(
            """
            INSERT INTO orders(
                source_record_id,order_id,deal_number,gc_user_id,name,email,phone,paid_at,course_key,course,tariff,
                autopayment,autopayment_source,
                branch,utm_term,stream,vk_link,tg_link,status,welcome_due_at,reminder_due_at,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id,course_key) DO UPDATE SET
                source_record_id=excluded.source_record_id,deal_number=excluded.deal_number,gc_user_id=excluded.gc_user_id,
                name=excluded.name,email=excluded.email,phone=excluded.phone,tariff=excluded.tariff,
                autopayment=excluded.autopayment,autopayment_source=excluded.autopayment_source,
                branch=CASE WHEN orders.branch='' THEN excluded.branch ELSE orders.branch END,
                utm_term=excluded.utm_term,stream=excluded.stream,vk_link=excluded.vk_link,tg_link=excluded.tg_link,
                status=CASE WHEN orders.welcome_sent_at='' AND orders.status IN ('pending','waiting_identity','waiting_flow','waiting_config','classification_needed','failed')
                    THEN excluded.status ELSE orders.status END,
                updated_at=excluded.updated_at
            """,
            values,
        )
        await db.commit()
    finally:
        await db.close()


async def _initial_status_for_source_item(item: dict[str, Any]) -> str:
    """Avoid a second welcome if this exact order was partially paid first.

    Normally the onboarding row itself makes the paid transition idempotent.
    The extra ledger check covers a handover/recovery where the partial event
    was observed by GetCourse Orders before this module had a row.
    """
    order_id = _clean(item.get("order_id"), 100)
    course_key = _clean(item.get("course_key"), 50)
    payment_state = _clean(item.get("payment_state"), 40).casefold()
    if not order_id or not course_key or payment_state != "paid":
        return ""
    db = await _connect()
    try:
        existing = await (
            await db.execute(
                "SELECT 1 FROM orders WHERE order_id=? AND course_key=? LIMIT 1",
                (order_id, course_key),
            )
        ).fetchone()
    finally:
        await db.close()
    if existing:
        return ""
    try:
        history = await _module("getcourse-orders", "service_order_payment_history").service_order_payment_history(
            order_id=order_id
        )
    except Exception:
        # The ledger is an additional guard. A temporary sibling-module
        # failure must never stop onboarding for a genuinely new customer.
        return ""
    if bool(history.get("ok")) and bool(history.get("partial_seen")):
        return "backfill_only"
    return ""


async def _sync_source() -> int:
    global _last_flow_result
    settings = await _settings()
    if settings.get("initialized") != "1":
        await _baseline_source()
        return 0
    fields = _module("getcourse-chat-fields", "service_paid_course_orders")
    cursor_id = int(settings.get("cursor_id") or 0)
    cursor_updated_at = settings.get("cursor_updated_at") or ""
    stored = 0
    for _ in range(10):
        data = await fields.service_paid_course_orders(
            after_source_record_id=cursor_id,
            after_updated_at=cursor_updated_at,
            limit=250,
        )
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or "Источник GetCourse недоступен")
        flow_items = int(data.get("flow_items") or 0)
        flow_stale = bool(data.get("flow_stale"))
        flow_errors = [
            _clean(item.get("error") if isinstance(item, dict) else item, 500)
            for item in (data.get("flow_errors") or [])
        ]
        _last_flow_result = {
            "ok": flow_items > 0,
            "status": "stale_fallback" if flow_items > 0 and flow_stale else "ready" if flow_items > 0 else "unavailable",
            "source": _clean(data.get("flow_source"), 100),
            "stale": flow_stale,
            "items": flow_items,
            "errors": [item for item in flow_errors if item][:8],
            "checked_at": _iso(),
        }
        for item in data.get("items") or []:
            initial_status = await _initial_status_for_source_item(item)
            await _upsert_order(
                item,
                int(settings.get("reminder_hours") or 12),
                initial_status=initial_status,
            )
            stored += 1
        cursor_id = int(data.get("cursor") or cursor_id)
        cursor_updated_at = _clean(data.get("cursor_updated_at") or cursor_updated_at, 100)
        await _setting_updates(
            {
                "cursor_id": str(cursor_id),
                "cursor_updated_at": cursor_updated_at,
                "last_sync_at": _iso(),
                "source_max_id": str(int(data.get("max_updated_id") or data.get("max_source_record_id") or cursor_id)),
                "source_max_updated_at": _clean(data.get("max_updated_at"), 100),
            }
        )
        if not data.get("has_more"):
            break
    return stored


def _upgrade_offer_ids(value: Any) -> set[str]:
    return {item for item in re.split(r"[\s,;]+", _clean(value, 2000)) if item.isdigit()}


def _upgrade_target_offer(course_key: str, autopayment: bool, settings: dict[str, str]) -> str:
    suffix = "autopay" if autopayment else "manual"
    return _clean(settings.get(f"upgrade_target_{course_key}_{suffix}"), 30)


def _upgrade_course_label(course_key: str) -> str:
    return {
        "dog": "Послушная собака",
        "puppy": "Первые шаги к воспитанию",
        "combo": "Щенок + Собака",
    }.get(_clean(course_key, 20), "Курс")


def _upgrade_status_kind(value: Any) -> str:
    text = _clean(value, 100).replace("ё", "е").casefold()
    if text in {"in_work", "in work"} or "в работе" in text:
        return "in_work"
    if text in {"payed", "paid", "success"} or "заверш" in text or "оплачен" in text:
        return "completed"
    if any(marker in text for marker in ("отмен", "возврат", "refund", "cancel", "false", "ложн")):
        return "cancelled"
    return text


async def _upgrade_event(
    job_id: int, stage: str, status: str, *, details: dict[str, Any] | None = None, error: str = ""
) -> None:
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO upgrade_events(job_id,stage,status,details_json,error,created_at) VALUES(?,?,?,?,?,?)",
            (
                int(job_id), _clean(stage, 80), _clean(status, 80),
                json.dumps(details or {}, ensure_ascii=False, default=str), _clean(error, 1000), _iso(),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _upsert_upgrade_candidate(candidate: dict[str, Any], settings: dict[str, str]) -> int:
    origins = [dict(item) for item in (candidate.get("origins") or []) if isinstance(item, dict)]
    course_key = _clean(candidate.get("course_key"), 20)
    origin = origins[0] if len(origins) == 1 else {}
    target_offer = _upgrade_target_offer(course_key, bool(origin.get("autopayment")), settings)
    problems: list[str] = []
    if course_key not in {"dog", "puppy", "combo"}:
        problems.append("Не удалось однозначно определить курс доплаты")
    if len(origins) != 1:
        problems.append(f"Подходящих исходных Standard-заказов: {len(origins)}")
    if origin and not _clean(origin.get("offer_id"), 30):
        problems.append("В снимке исходного заказа отсутствует offer_id")
    if not _clean(candidate.get("offer_id"), 30):
        problems.append("В снимке заказа доплаты отсутствует offer_id")
    if not target_offer.isdigit():
        problems.append("Не настроено целевое Premium-предложение")
    operation_id = hashlib.sha256(
        f"upgrade:{candidate.get('order_id')}:{course_key}".encode("utf-8")
    ).hexdigest()[:24]
    source_offer_allowed = _clean(candidate.get("offer_id"), 30) in _upgrade_offer_ids(
        settings.get(f"upgrade_surcharge_offer_ids_{course_key}")
    )
    auto_approved = bool(
        not problems
        and source_offer_allowed
        and settings.get("upgrade_auto_approve") == "1"
    )
    now = _iso()
    snapshot = {
        "upgrade": {key: value for key, value in candidate.items() if key != "origins"},
        "origin": origin,
        "origin_candidates": origins,
        "source_offer_allowed": source_offer_allowed,
    }
    db = await _connect()
    try:
        cursor = await db.execute(
            """
            INSERT INTO upgrade_jobs(
                source_record_id,upgrade_order_id,upgrade_deal_number,origin_order_id,origin_deal_number,
                gc_user_id,name,email,phone,course_key,autopayment,source_offer_id,target_offer_id,
                source_cost,source_payed,upgrade_cost,upgrade_payed,origin_paid_at,upgrade_paid_at,
                status,approved,operation_id,strategy,snapshot_json,error,next_attempt_at,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(upgrade_order_id,course_key) DO UPDATE SET
                source_record_id=excluded.source_record_id,upgrade_deal_number=excluded.upgrade_deal_number,
                origin_order_id=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.origin_order_id ELSE upgrade_jobs.origin_order_id END,
                origin_deal_number=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.origin_deal_number ELSE upgrade_jobs.origin_deal_number END,
                gc_user_id=excluded.gc_user_id,name=excluded.name,email=excluded.email,phone=excluded.phone,
                autopayment=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.autopayment ELSE upgrade_jobs.autopayment END,
                source_offer_id=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.source_offer_id ELSE upgrade_jobs.source_offer_id END,
                target_offer_id=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.target_offer_id ELSE upgrade_jobs.target_offer_id END,
                source_cost=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.source_cost ELSE upgrade_jobs.source_cost END,
                source_payed=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.source_payed ELSE upgrade_jobs.source_payed END,
                upgrade_cost=excluded.upgrade_cost,upgrade_payed=excluded.upgrade_payed,
                origin_paid_at=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.origin_paid_at ELSE upgrade_jobs.origin_paid_at END,
                upgrade_paid_at=excluded.upgrade_paid_at,
                status=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.status ELSE upgrade_jobs.status END,
                approved=CASE WHEN upgrade_jobs.approved=1 THEN 1 ELSE excluded.approved END,
                strategy=CASE WHEN upgrade_jobs.approved=0 THEN excluded.strategy ELSE upgrade_jobs.strategy END,
                snapshot_json=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.snapshot_json ELSE upgrade_jobs.snapshot_json END,
                error=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.error ELSE upgrade_jobs.error END,
                next_attempt_at=CASE WHEN upgrade_jobs.status IN ('preview','manual_review') THEN excluded.next_attempt_at ELSE upgrade_jobs.next_attempt_at END,
                updated_at=excluded.updated_at
            """,
            (
                int(candidate.get("source_record_id") or 0), _clean(candidate.get("order_id"), 100),
                _clean(candidate.get("deal_number"), 100), _clean(origin.get("order_id"), 100),
                _clean(origin.get("deal_number"), 100),
                _clean(candidate.get("gc_user_id") or origin.get("gc_user_id"), 100),
                _clean(candidate.get("name") or origin.get("name"), 300),
                _clean(candidate.get("email") or origin.get("email"), 300),
                _clean(candidate.get("phone") or origin.get("phone"), 100), course_key,
                1 if origin.get("autopayment") else 0, _clean(origin.get("offer_id"), 30), target_offer,
                float(origin.get("cost_money") or 0), float(origin.get("payed_money") or 0),
                float(candidate.get("cost_money") or 0), float(candidate.get("payed_money") or 0),
                _clean(origin.get("paid_at"), 100), _clean(candidate.get("paid_at"), 100),
                "validated" if auto_approved else ("manual_review" if problems else "preview"),
                1 if auto_approved else 0, operation_id,
                "replacement_browser" if settings.get("upgrade_browser_enabled") == "1" else "legacy",
                json.dumps(snapshot, ensure_ascii=False, default=str), "; ".join(problems), now if auto_approved else "",
                now, now,
            ),
        )
        await db.commit()
        row = await (
            await db.execute(
                "SELECT id FROM upgrade_jobs WHERE upgrade_order_id=? AND course_key=?",
                (_clean(candidate.get("order_id"), 100), course_key),
            )
        ).fetchone()
        return int(row[0]) if row else max(0, int(cursor.lastrowid or 0))
    finally:
        await db.close()


async def _sync_upgrades() -> int:
    settings = await _settings()
    service = _module("getcourse-chat-fields", "service_upgrade_candidates")
    cursor = int(settings.get("upgrade_cursor_id") or 0)
    stored = 0
    for _ in range(20):
        result = await service.service_upgrade_candidates(after_source_record_id=cursor, limit=250)
        if not result.get("ok"):
            raise RuntimeError(result.get("error") or "Источник доплат GetCourse недоступен")
        for candidate in result.get("items") or []:
            await _upsert_upgrade_candidate(dict(candidate), settings)
            stored += 1
        cursor = int(result.get("cursor") or cursor)
        await _setting_updates({"upgrade_cursor_id": str(cursor)})
        if not result.get("has_more"):
            break
    return stored


async def _upgrade_job(job_id: int) -> dict[str, Any] | None:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM upgrade_jobs WHERE id=?", (int(job_id),))).fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def _set_upgrade_state(
    job_id: int,
    status: str,
    *,
    error: str = "",
    delay_seconds: int = 0,
    completed: bool = False,
    chat_sent: bool = False,
    reset_attempts: bool = False,
) -> None:
    now = _now_dt()
    db = await _connect()
    try:
        await db.execute(
            """UPDATE upgrade_jobs SET status=?,error=?,next_attempt_at=?,
               attempts=CASE WHEN ? THEN 0 ELSE attempts END,
               chat_sent_at=CASE WHEN ? THEN ? ELSE chat_sent_at END,
               completed_at=CASE WHEN ? THEN ? ELSE completed_at END,updated_at=? WHERE id=?""",
            (
                _clean(status, 80), _clean(error, 1000),
                _iso(now + timedelta(seconds=max(0, delay_seconds))) if delay_seconds else "",
                1 if reset_attempts else 0,
                1 if chat_sent else 0, _iso(now), 1 if completed else 0, _iso(now), _iso(now), int(job_id),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _set_upgrade_browser_result(job_id: int, result: dict[str, Any], replacement_deal_number: str) -> None:
    db = await _connect()
    try:
        await db.execute(
            """UPDATE upgrade_jobs SET replacement_order_id=?,replacement_deal_number=?,payment_id=?,
               browser_journal=?,browser_artifact=?,updated_at=? WHERE id=?""",
            (
                _clean(result.get("target_order_id"), 100), _clean(replacement_deal_number, 100),
                _clean(result.get("payment_id"), 100), _clean(result.get("journal"), 2000),
                _clean(result.get("proof"), 2000), _iso(), int(job_id),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _defer_upgrade(job: dict[str, Any], error: str, *, delay_seconds: int = 120) -> None:
    attempts = int(job.get("attempts") or 0) + 1
    terminal = attempts >= 20
    db = await _connect()
    try:
        await db.execute(
            "UPDATE upgrade_jobs SET status=?,error=?,attempts=?,next_attempt_at=?,updated_at=? WHERE id=?",
            (
                "manual_review" if terminal else _clean(job.get("status"), 80), _clean(error, 1000), attempts,
                "" if terminal else _iso(_now_dt() + timedelta(seconds=delay_seconds)), _iso(), int(job["id"]),
            ),
        )
        await db.commit()
    finally:
        await db.close()
    await _upgrade_event(int(job["id"]), _clean(job.get("status"), 80), "deferred", error=error)


def _completed_monitor_transient(error: Any) -> bool:
    """Recognize GetCourse Export's asynchronous, safe-to-retry responses."""

    text = _clean(error, 1000).casefold().replace("ё", "е")
    return any(marker in text for marker in (
        "файл еще не создан",
        "лимит getcourse export api",
        "live-снимок getcourse пока недоступен",
    ))


def _should_alert_upgrade_exception(job: dict[str, Any], error: Any) -> bool:
    if _clean(job.get("status"), 80) != "completed" or not _completed_monitor_transient(error):
        return True
    # A completed upgrade is only being checked for a later refund.  One or
    # two asynchronous Export responses are normal; alert on the third
    # consecutive failure, while the existing alert key deduplicates repeats.
    return int(job.get("attempts") or 0) + 1 >= 3


async def _upgrade_snapshots(job: dict[str, Any], *, live: bool) -> dict[str, Any]:
    service = _module("getcourse-chat-fields", "service_upgrade_order_snapshots")
    order_ids = [job.get("origin_order_id", ""), job.get("upgrade_order_id", "")]
    if _clean(job.get("replacement_order_id"), 100):
        order_ids.append(job.get("replacement_order_id", ""))
    result = await service.service_upgrade_order_snapshots(
        order_ids=order_ids,
        gc_user_id=_clean(job.get("gc_user_id"), 100), live=live,
    )
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "Не удалось прочитать заказы GetCourse")
    if live and result.get("source") != "live":
        raise RuntimeError(result.get("warning") or "Live-снимок GetCourse пока недоступен")
    snapshots: dict[str, Any] = {
        _clean(item.get("order_id"), 100): dict(item) for item in result.get("items") or []
    }
    snapshots["__related__"] = [
        dict(item) for item in result.get("related_items") or [] if isinstance(item, dict)
    ]
    return snapshots


def _upgrade_refund_conflicts(job: dict[str, Any], related: list[dict[str, Any]]) -> list[dict[str, Any]]:
    excluded = {_clean(job.get("origin_order_id"), 100), _clean(job.get("upgrade_order_id"), 100)}
    conflicts = []
    for item in related:
        if _clean(item.get("order_id"), 100) in excluded or _upgrade_status_kind(item.get("status")) == "cancelled":
            continue
        if _clean(item.get("course_key"), 20) != _clean(job.get("course_key"), 20):
            continue
        if _clean(item.get("tariff"), 40).casefold() in {"premium", "vip"}:
            conflicts.append(item)
    return conflicts


def _upgrade_money_matches(expected: Any, actual: Any) -> bool:
    return abs(float(expected or 0) - float(actual or 0)) <= 0.01


def _upgrade_ledger_error(job: dict[str, Any], origin: dict[str, Any], surcharge: dict[str, Any]) -> str:
    if not origin or not surcharge:
        return "Один из связанных заказов не найден"
    checks = (
        (job.get("source_cost"), origin.get("cost_money"), "стоимость исходного заказа"),
        (job.get("source_payed"), origin.get("payed_money"), "оплата исходного заказа"),
        (job.get("upgrade_cost"), surcharge.get("cost_money"), "стоимость доплаты"),
        (job.get("upgrade_payed"), surcharge.get("payed_money"), "оплата доплаты"),
    )
    changed = [label for expected, actual, label in checks if not _upgrade_money_matches(expected, actual)]
    return "Изменились финансовые данные: " + ", ".join(changed) if changed else ""


def _replacement_deal_number(job: dict[str, Any]) -> str:
    existing = _clean(job.get("replacement_deal_number"), 100)
    if existing.isdigit():
        return existing
    job_id = int(job["id"])
    if job_id <= 0 or job_id >= 1_000_000_000:
        raise RuntimeError("ID задания не помещается в безопасный диапазон номера GetCourse")
    return str(900_000_000 + job_id)


def _replacement_ledger_error(job: dict[str, Any], replacement: dict[str, Any]) -> str:
    if not replacement:
        return "Новый Premium-заказ ещё не появился в live-выгрузке GetCourse"
    checks = (
        (job.get("source_payed"), replacement.get("cost_money"), "стоимость нового Premium-заказа"),
        (job.get("source_payed"), replacement.get("payed_money"), "оплата нового Premium-заказа"),
    )
    changed = [label for expected, actual, label in checks if not _upgrade_money_matches(expected, actual)]
    return "Изменились финансовые данные: " + ", ".join(changed) if changed else ""


def _replacement_from_snapshots(job: dict[str, Any], snapshots: dict[str, Any]) -> dict[str, Any]:
    """Resolve the exact replacement even before its internal ID was journaled.

    A browser action can move the payment and then fail during recalculation.
    In that narrow window the live export already contains the replacement,
    while ``upgrade_jobs.replacement_order_id`` is intentionally still empty.
    The reserved numeric deal number is unique per job and lets us resume
    without guessing among the user's other orders.
    """

    replacement_order_id = _clean(job.get("replacement_order_id"), 100)
    if replacement_order_id and isinstance(snapshots.get(replacement_order_id), dict):
        return dict(snapshots[replacement_order_id])
    replacement_deal_number = _replacement_deal_number(job)
    matches = [
        dict(item)
        for item in snapshots.get("__related__") or []
        if isinstance(item, dict)
        and _clean(item.get("deal_number"), 100) == replacement_deal_number
        and _clean(item.get("gc_user_id"), 100) == _clean(job.get("gc_user_id"), 100)
    ]
    return matches[0] if len(matches) == 1 else {}


def _legacy_repair_offers(job: dict[str, Any]) -> tuple[str, str]:
    """Return the untouched Standard and surcharge offers from the job snapshot."""

    try:
        snapshot = json.loads(_clean(job.get("snapshot_json"), 100_000) or "{}")
    except json.JSONDecodeError:
        snapshot = {}
    origin = snapshot.get("origin") if isinstance(snapshot.get("origin"), dict) else {}
    upgrade = snapshot.get("upgrade") if isinstance(snapshot.get("upgrade"), dict) else {}
    origin_offer = _clean(origin.get("offer_id"), 30)
    upgrade_offer = _clean(upgrade.get("offer_id"), 30)
    if not origin_offer.isdigit() or not upgrade_offer.isdigit():
        return "", ""
    return origin_offer, upgrade_offer


async def _mutate_upgrade_order(job: dict[str, Any], **changes: Any) -> dict[str, Any]:
    service = _module("getcourse-chat-fields", "service_update_upgrade_order")
    result = await service.service_update_upgrade_order(
        gc_user_id=_clean(job.get("gc_user_id"), 100),
        deal_number=_clean(changes.pop("deal_number", job.get("origin_deal_number")), 100),
        email=_clean(job.get("email"), 300), phone=_clean(job.get("phone"), 100), **changes,
    )
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "GetCourse не принял изменение заказа")
    return result


def _upgrade_access_ready(course_key: str, groups: list[dict[str, Any]], *, rollback: bool = False) -> bool:
    names = [
        _clean(item.get("name"), 500).replace("ё", "е").casefold()
        for item in groups if isinstance(item, dict)
    ]
    courses = ("puppy", "dog") if course_key == "combo" else (course_key,)
    for course in courses:
        marker = "щен" if course == "puppy" else "собак"
        premium = any(marker in name and "премиум" in name for name in names)
        standard = any(marker in name and "стандарт" in name for name in names)
        if rollback:
            if not standard or premium:
                return False
        elif not premium or standard:
            return False
    return True


def _upgrade_premium_present(course_key: str, groups: list[dict[str, Any]]) -> bool:
    names = [
        _clean(item.get("name"), 500).replace("ё", "е").casefold()
        for item in groups if isinstance(item, dict)
    ]
    courses = ("puppy", "dog") if course_key == "combo" else (course_key,)
    return all(
        any(("щен" if course == "puppy" else "собак") in name and "премиум" in name for name in names)
        for course in courses
    )


def _upgrade_premium_order_matches(job: dict[str, Any], surcharge: dict[str, Any]) -> bool:
    """Confirm the rebuilt order without trusting one stale export column.

    GetCourse can retain the original surcharge ``offer_id`` in Deals Export
    after the order composition was replaced. The exact order, completed state,
    unchanged ledger, Premium tariff/course classification and the subsequent
    live group check form the reliable invariant in that case.
    """

    actual_offer = _clean(surcharge.get("offer_id"), 30)
    if actual_offer == _clean(job.get("target_offer_id"), 30):
        return True
    actual_tariff = _clean(surcharge.get("tariff"), 40).casefold()
    actual_course = _clean(surcharge.get("course_key"), 20)
    return actual_tariff in {"premium", "vip"} and actual_course == _clean(job.get("course_key"), 20)


async def _upgrade_flows(job: dict[str, Any]) -> dict[str, dict[str, str]]:
    course_keys = ("puppy", "dog") if job.get("course_key") == "combo" else (job.get("course_key"),)
    result: dict[str, dict[str, str]] = {}
    db = await _connect()
    try:
        saved_rows = await (
            await db.execute(
                """SELECT course_key,stream,vk_link,tg_link FROM orders
                   WHERE order_id=? ORDER BY id DESC""",
                (_clean(job.get("origin_order_id"), 100),),
            )
        ).fetchall()
    finally:
        await db.close()
    for saved in saved_rows:
        course_key = _clean(saved["course_key"], 20)
        if course_key in course_keys and all(_clean(saved[key], 2000) for key in ("stream", "vk_link", "tg_link")):
            result[course_key] = {
                "stream": _clean(saved["stream"], 50),
                "vk_link": _clean(saved["vk_link"], 2000),
                "tg_link": _clean(saved["tg_link"], 2000),
            }
    for course_key in course_keys:
        if str(course_key) in result:
            continue
        service = _module("getcourse-chat-fields", "service_resolve_onboarding_flow")
        resolved = await service.service_resolve_onboarding_flow(
            course_key=_clean(course_key, 20), paid_at=_clean(job.get("origin_paid_at"), 100)
        )
        flow = resolved.get("flow") if isinstance(resolved.get("flow"), dict) else {}
        if not resolved.get("ok") or not all(_clean(flow.get(key), 2000) for key in ("stream", "vk_link", "tg_link")):
            raise RuntimeError(f"Для курса {_upgrade_course_label(course_key)} не найдена полная пара ссылок потока")
        result[str(course_key)] = {
            "stream": _clean(flow.get("stream"), 50),
            "vk_link": _clean(flow.get("vk_link"), 2000),
            "tg_link": _clean(flow.get("tg_link"), 2000),
        }
    return result


async def _send_upgrade_links(job: dict[str, Any], settings: dict[str, str]) -> None:
    flows = await _upgrade_flows(job)
    row = {
        **job,
        "id": int(job["id"]), "course": _upgrade_course_label(job.get("course_key", "")),
        "target_platform_id": "", "target_source": "", "utm_term": "",
    }
    if job.get("course_key") == "combo":
        body = await _template("upgrade_premium_combo")
        values = {
            "first_name": _first_name(job.get("name") or ""),
            "puppy_stream": flows["puppy"]["stream"], "puppy_vk_link": flows["puppy"]["vk_link"],
            "puppy_tg_link": flows["puppy"]["tg_link"], "dog_stream": flows["dog"]["stream"],
            "dog_vk_link": flows["dog"]["vk_link"], "dog_tg_link": flows["dog"]["tg_link"],
        }
        for key, value in values.items():
            body = body.replace("{" + key + "}", value)
    else:
        flow = flows[str(job["course_key"])]
        row.update(flow)
        body = _render(await _template("upgrade_premium"), row, settings)
    await _send_stage_with_fallback(row, body, "upgrade_welcome", settings)


async def _process_browser_upgrade_job(job: dict[str, Any], settings: dict[str, str]) -> None:
    """Create a clean Premium purchase and move the original payment through GC UI.

    Every external step is repeatable. The browser executor keeps its own
    operation journal so a timeout after Save cannot move the same payment
    twice or make the coordinator guess what happened.
    """

    job_id = int(job["id"])
    status = _clean(job.get("status"), 80)
    snapshots = await _upgrade_snapshots(job, live=True)
    origin = snapshots.get(_clean(job.get("origin_order_id"), 100), {})
    surcharge = snapshots.get(_clean(job.get("upgrade_order_id"), 100), {})
    replacement = _replacement_from_snapshots(job, snapshots)
    surcharge_cancelled = any(
        _upgrade_status_kind(value) == "cancelled"
        for value in (surcharge.get("status"), surcharge.get("payment_state"))
    )

    if status == "completed":
        if surcharge_cancelled:
            message = (
                "⚠️ Nexus: возвращена доплата Standard → Premium\n"
                f"Пользователь GetCourse: {job.get('gc_user_id')}\n"
                f"Доплата: #{job.get('upgrade_deal_number')}\n"
                "Автоматический финансовый откат остановлен: требуется ручная проверка."
            )
            await _set_upgrade_state(job_id, "manual_review", error="Доплата отменена или возвращена; нужен контролируемый откат")
            await _upgrade_event(job_id, status, "refund_manual_review", error="Доплата отменена или возвращена")
            await _send_upgrade_alert(job, "refund", message)
            return
        await _set_upgrade_state(job_id, "completed", delay_seconds=6 * 60 * 60, reset_attempts=True)
        return

    if surcharge_cancelled:
        message = (
            "❌ Nexus остановил доплату до Premium\n"
            f"Пользователь GetCourse: {job.get('gc_user_id')}\n"
            f"Этап: {status}\nДоплата #{job.get('upgrade_deal_number')} отменена или возвращена."
        )
        await _set_upgrade_state(job_id, "manual_review", error="Доплата отменена или возвращена во время перехода")
        await _upgrade_event(job_id, status, "blocked", error="Доплата отменена или возвращена")
        await _send_upgrade_alert(job, "surcharge_cancelled", message)
        return

    if status in {"validated", "waiting_browser"}:
        ledger_error = _upgrade_ledger_error(job, origin, surcharge)
        if ledger_error:
            await _set_upgrade_state(job_id, "manual_review", error=ledger_error)
            await _upgrade_event(job_id, status, "blocked", error=ledger_error)
            await _send_upgrade_alert(
                job, "ledger", f"❌ Nexus остановил доплату #{job.get('upgrade_deal_number')}\n{ledger_error}",
            )
            return
        if _upgrade_status_kind(origin.get("status")) != "completed" or _upgrade_status_kind(surcharge.get("status")) != "completed":
            await _set_upgrade_state(job_id, "manual_review", error="Оба исходных заказа должны быть завершены")
            return
        if settings.get("upgrade_browser_enabled") != "1":
            await _set_upgrade_state(job_id, "waiting_browser", error="Браузерная автоматизация выключена")
            return
        probe = await _run_browser_action({"action": "probe", "operation_id": f"probe-{job_id}"}, timeout=90)
        if not probe.get("ok"):
            error = _clean(probe.get("error"), 1000) or "Сессия GetCourse не прошла проверку"
            await _set_upgrade_state(job_id, "waiting_browser", error=error, delay_seconds=900)
            await _upgrade_event(job_id, status, "browser_unavailable", details=probe, error=error)
            await _send_upgrade_alert(
                job, "browser_session",
                f"❌ Nexus не может обработать доплату #{job.get('upgrade_deal_number')}\n"
                f"Пользователь GetCourse: {job.get('gc_user_id')}\n{error}\n"
                "Денежные данные не изменялись.",
            )
            return
        replacement_deal_number = _replacement_deal_number(job)
        fields = {
            settings["upgrade_link_field"]: f"source:{job.get('origin_deal_number')};upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        db = await _connect()
        try:
            await db.execute(
                "UPDATE upgrade_jobs SET replacement_deal_number=?,error='',updated_at=? WHERE id=?",
                (replacement_deal_number, _iso(), job_id),
            )
            await db.commit()
        finally:
            await db.close()
        result = await _mutate_upgrade_order(
            {**job, "replacement_deal_number": replacement_deal_number},
            deal_number=replacement_deal_number,
            deal_status="new",
            offer_id=job.get("target_offer_id"),
            deal_cost=job.get("source_payed"),
            addfields=fields,
        )
        await _set_upgrade_state(job_id, "replacement_creating", delay_seconds=60)
        await _upgrade_event(
            job_id, status, "replacement_order_requested",
            details={"deal_number": replacement_deal_number, "result": result},
        )
        return

    if status == "replacement_creating":
        replacement_deal_number = _replacement_deal_number(job)
        result = await _run_browser_action(
            {
                "action": "transfer_payment",
                "operation_id": _clean(job.get("operation_id"), 100),
                "source_order_id": _clean(job.get("origin_order_id"), 100),
                "source_deal_number": _clean(job.get("origin_deal_number"), 100),
                "target_deal_number": replacement_deal_number,
                "expected_amount": float(job.get("source_payed") or 0),
            },
            timeout=180,
        )
        if not result.get("ok"):
            error = _clean(result.get("error"), 1000) or "Браузер не подтвердил перенос платежа"
            db = await _connect()
            try:
                await db.execute(
                    "UPDATE upgrade_jobs SET browser_journal=?,browser_artifact=?,updated_at=? WHERE id=?",
                    (_clean(result.get("journal"), 2000), _clean(result.get("proof"), 2000), _iso(), job_id),
                )
                await db.commit()
            finally:
                await db.close()
            await _defer_upgrade(job, error, delay_seconds=900)
            await _upgrade_event(job_id, status, "browser_failed", details=result, error=error)
            await _send_upgrade_alert(
                job, "browser_action",
                f"❌ Nexus не завершил перенос платежа\nПользователь GetCourse: {job.get('gc_user_id')}\n"
                f"Заказы: #{job.get('origin_deal_number')} → #{replacement_deal_number}\n{error}\n"
                "Повтор будет только после проверки сохранённого состояния; второй платёж не создаётся.",
            )
            return
        await _set_upgrade_browser_result(job_id, result, replacement_deal_number)
        fields = {
            settings["upgrade_command_field"]: "finalize",
            settings["upgrade_link_field"]: f"source:{job.get('origin_deal_number')};upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        await _mutate_upgrade_order(job, deal_number=replacement_deal_number, addfields=fields)
        completed = await _run_browser_action(
            {
                "action": "complete_order",
                "operation_id": f"complete-premium-{_clean(job.get('operation_id'), 100)}",
                "order_id": _clean(result.get("target_order_id"), 100),
                "deal_number": replacement_deal_number,
                "offer_id": _clean(job.get("target_offer_id"), 30),
                "expected_status": "Завершен",
                "expected_cost": float(job.get("source_payed") or 0),
                "expected_received": float(job.get("source_payed") or 0),
            },
            timeout=120,
        )
        if not completed.get("ok"):
            error = _clean(completed.get("error"), 1000) or "Браузер не завершил полностью оплаченный Premium-заказ"
            await _defer_upgrade(job, error, delay_seconds=900)
            await _send_upgrade_alert(job, "replacement_complete_browser", f"❌ Nexus не завершил Premium-заказ #{replacement_deal_number}\n{error}")
            return
        await _set_upgrade_state(job_id, "replacement_finalizing", delay_seconds=90)
        await _upgrade_event(
            job_id, status, "payment_moved_and_recalculated",
            details={key: result.get(key) for key in ("payment_id", "target_order_id", "proof")},
        )
        return

    if status == "replacement_finalizing":
        if not replacement:
            await _defer_upgrade(job, "Новый Premium-заказ ещё не виден в live-выгрузке", delay_seconds=180)
            return
        replacement_error = _replacement_ledger_error(job, replacement)
        if replacement_error:
            await _set_upgrade_state(job_id, "manual_review", error=replacement_error)
            await _send_upgrade_alert(job, "replacement_ledger", f"❌ {replacement_error}")
            return
        if _upgrade_status_kind(replacement.get("status")) != "completed":
            await _defer_upgrade(job, "GetCourse ещё не завершил новый Premium-заказ", delay_seconds=180)
            return
        if not _upgrade_premium_order_matches(job, replacement):
            await _set_upgrade_state(job_id, "manual_review", error="Новый заказ завершён с другим предложением")
            return
        access_service = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
        access = await access_service.service_getcourse_access_snapshot(
            gc_user_id=_clean(job.get("gc_user_id"), 100), email=_clean(job.get("email"), 300),
            live=True, force=True,
        )
        if not access.get("ok") or access.get("stale") or access.get("source") != "live":
            await _defer_upgrade(job, access.get("warning") or access.get("error") or "Live-доступы недоступны", delay_seconds=180)
            return
        if not _upgrade_premium_present(_clean(job.get("course_key"), 20), access.get("groups") or []):
            await _defer_upgrade(job, "Новый Premium-заказ завершён, но Premium-доступ ещё не выдан", delay_seconds=180)
            return
        cancelled = await _run_browser_action(
            {
                "action": "cancel_order",
                "operation_id": f"cancel-origin-{_clean(job.get('operation_id'), 100)}",
                "order_id": _clean(job.get("origin_order_id"), 100),
                "deal_number": _clean(job.get("origin_deal_number"), 100),
                "offer_id": _clean(job.get("source_offer_id"), 30),
                "expected_cost": float(job.get("source_cost") or 0),
                "expected_received": 0,
            },
            timeout=120,
        )
        if not cancelled.get("ok"):
            error = _clean(cancelled.get("error"), 1000) or "Браузер не подтвердил отмену Standard-заказа"
            await _defer_upgrade(job, error, delay_seconds=900)
            await _send_upgrade_alert(
                job, "origin_cancel_browser",
                f"❌ Nexus не отменил Standard-заказ #{job.get('origin_deal_number')}\n{error}",
            )
            return
        await _set_upgrade_state(job_id, "origin_canceling", delay_seconds=90)
        await _upgrade_event(
            job_id, status, "premium_ready_standard_cancelled",
            details={key: cancelled.get(key) for key in ("order_id", "status", "proof", "already_cancelled")},
        )
        return

    if status == "origin_canceling":
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _defer_upgrade(job, "GetCourse ещё не отменил исходный Standard-заказ", delay_seconds=180)
            return
        if _upgrade_status_kind(replacement.get("status")) != "completed" or _replacement_ledger_error(job, replacement):
            await _set_upgrade_state(job_id, "manual_review", error="Premium-заказ изменился во время отмены Standard")
            return
        if not _upgrade_money_matches(job.get("upgrade_payed"), surcharge.get("payed_money")):
            await _set_upgrade_state(job_id, "manual_review", error="Изменилась оплата отдельного заказа доплаты")
            return
        await _set_upgrade_state(job_id, "verifying", delay_seconds=30)
        await _upgrade_event(job_id, status, "standard_cancelled")
        return

    if status == "verifying":
        access_service = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
        access = await access_service.service_getcourse_access_snapshot(
            gc_user_id=_clean(job.get("gc_user_id"), 100), email=_clean(job.get("email"), 300),
            live=True, force=True,
        )
        if not access.get("ok") or access.get("stale") or access.get("source") != "live":
            await _defer_upgrade(job, access.get("warning") or access.get("error") or "Live-доступы ещё не готовы", delay_seconds=180)
            return
        if not _upgrade_access_ready(_clean(job.get("course_key"), 20), access.get("groups") or []):
            await _defer_upgrade(job, "Premium ещё не заменил Standard в группах GetCourse", delay_seconds=180)
            return
        await _set_upgrade_state(job_id, "access_ready")
        await _upgrade_event(job_id, status, "access_ready")
        job = await _upgrade_job(job_id) or job
        status = "access_ready"

    if status == "access_ready":
        await _send_upgrade_links(job, settings)
        await _set_upgrade_state(job_id, "completed", delay_seconds=6 * 60 * 60, completed=True, chat_sent=True)
        await _upgrade_event(job_id, status, "completed")
        await _send_upgrade_alert(
            job, "completed",
            f"✅ Nexus завершил доплату Standard → Premium\n"
            f"Пользователь GetCourse: {job.get('gc_user_id')}\n"
            f"Premium-заказ: #{job.get('replacement_deal_number')} · {float(job.get('source_payed') or 0):.2f} ₽\n"
            f"Доплата: #{job.get('upgrade_deal_number')} · {float(job.get('upgrade_payed') or 0):.2f} ₽\n"
            "Standard закрыт, Premium проверен, ссылки на чаты отправлены.",
            repeat_hours=24 * 365,
        )


async def _process_browser_legacy_repair_job(job: dict[str, Any], settings: dict[str, str]) -> None:
    """Repair an order pair that the retired in-place upgrade already changed.

    Premium access remains in place until a separate, fully paid Premium order
    is live. Only then are the cancelled source order and the surcharge order
    restored to their original offers. The payment move is journaled by the
    browser executor and is therefore safe to resume after a timeout.
    """

    job_id = int(job["id"])
    status = _clean(job.get("status"), 80)
    origin_offer, surcharge_offer = _legacy_repair_offers(job)
    if not origin_offer or not surcharge_offer:
        error = "В исходном снимке не сохранились предложения Standard и доплаты"
        await _set_upgrade_state(job_id, status or "manual_review", error=error, delay_seconds=900)
        await _send_upgrade_alert(job, "repair_snapshot", f"❌ Nexus остановил выравнивание учёта\n{error}")
        return

    if status == "repair_replacement_creating":
        replacement_deal_number = _replacement_deal_number(job)
        await _mutate_upgrade_order(
            job,
            deal_number=replacement_deal_number,
            deal_status="new",
            offer_id=job.get("target_offer_id"),
            deal_cost=job.get("source_payed"),
        )
        await _set_upgrade_state(job_id, "repair_replacement_ready", delay_seconds=60)
        await _upgrade_event(job_id, status, "repair_replacement_set_new")
        return

    # This stage is verified authoritatively in the GetCourse card itself.
    # Avoid consuming an asynchronous Deals Export request before the browser
    # has checked the exact offer, cost, zero payment and cancelled status.
    if status == "repair_restoring_origin":
        cancelled = await _run_browser_action(
            {
                "action": "cancel_order",
                "operation_id": f"repair-cancel-{_clean(job.get('operation_id'), 100)}",
                "order_id": _clean(job.get("origin_order_id"), 100),
                "deal_number": _clean(job.get("origin_deal_number"), 100),
                "offer_id": origin_offer,
                "expected_cost": float(job.get("source_cost") or 0),
                "expected_received": 0,
            },
            timeout=120,
        )
        if not cancelled.get("ok"):
            error = _clean(cancelled.get("error"), 1000) or "Карточка Standard-заказа ещё не восстановлена"
            await _defer_upgrade(job, error, delay_seconds=180)
            return
        await _mutate_upgrade_order(
            job,
            deal_number=_clean(job.get("upgrade_deal_number"), 100),
            offer_id=surcharge_offer,
            deal_cost=job.get("upgrade_cost"),
            addfields={
                settings["upgrade_command_field"]: "finalize",
                settings["upgrade_link_field"]: f"replacement:{job.get('replacement_deal_number')}",
                settings["upgrade_operation_field"]: job.get("operation_id"),
            },
        )
        await _set_upgrade_state(job_id, "repair_restoring_surcharge", delay_seconds=90)
        await _upgrade_event(job_id, status, "repair_surcharge_restore_requested", details={"offer_id": surcharge_offer})
        return

    if status == "repair_restoring_surcharge":
        checks = (
            await _run_browser_action(
                {
                    "action": "complete_order",
                    "operation_id": f"repair-surcharge-{_clean(job.get('operation_id'), 100)}",
                    "order_id": _clean(job.get("upgrade_order_id"), 100),
                    "deal_number": _clean(job.get("upgrade_deal_number"), 100),
                    "offer_id": surcharge_offer,
                    "expected_status": "Завершен",
                    "expected_cost": float(job.get("upgrade_cost") or 0),
                    "expected_received": float(job.get("upgrade_payed") or 0),
                },
                timeout=120,
            ),
            await _run_browser_action(
                {
                    "action": "inspect_order",
                    "operation_id": f"repair-premium-{_clean(job.get('operation_id'), 100)}",
                    "order_id": _clean(job.get("replacement_order_id"), 100),
                    "deal_number": _clean(job.get("replacement_deal_number"), 100),
                    "offer_id": _clean(job.get("target_offer_id"), 30),
                    "expected_status": "Завершен",
                    "expected_cost": float(job.get("source_payed") or 0),
                    "expected_received": float(job.get("source_payed") or 0),
                },
                timeout=120,
            ),
            await _run_browser_action(
                {
                    "action": "inspect_access",
                    "operation_id": f"repair-access-{_clean(job.get('operation_id'), 100)}",
                    "gc_user_id": _clean(job.get("gc_user_id"), 100),
                    "course_key": _clean(job.get("course_key"), 20),
                },
                timeout=120,
            ),
        )
        failed = next((item for item in checks if not item.get("ok")), None)
        if failed:
            await _defer_upgrade(
                job,
                _clean(failed.get("error"), 1000) or "Финальная браузерная проверка ещё не пройдена",
                delay_seconds=180,
            )
            return
        await _set_upgrade_state(job_id, "completed", delay_seconds=6 * 60 * 60, completed=True)
        await _upgrade_event(
            job_id, status, "legacy_repair_completed",
            details={"surcharge_proof": checks[0].get("proof"), "premium_proof": checks[1].get("proof"), "access_proof": checks[2].get("proof")},
        )
        await _send_upgrade_alert(
            job, "repair_completed",
            f"✅ Nexus выровнял учёт старой доплаты\nПользователь GetCourse: {job.get('gc_user_id')}\n"
            f"Premium-заказ: #{job.get('replacement_deal_number')} · {float(job.get('source_payed') or 0):.2f} ₽\n"
            f"Доплата: #{job.get('upgrade_deal_number')} · {float(job.get('upgrade_payed') or 0):.2f} ₽\n"
            "Платёж перенесён вместе с комиссией; Standard и доплата восстановлены в истории.",
            repeat_hours=24 * 365,
        )
        return

    snapshots = await _upgrade_snapshots(job, live=True)
    origin = snapshots.get(_clean(job.get("origin_order_id"), 100), {})
    surcharge = snapshots.get(_clean(job.get("upgrade_order_id"), 100), {})
    replacement = snapshots.get(_clean(job.get("replacement_order_id"), 100), {})

    if status == "completed":
        surcharge_cancelled = any(
            _upgrade_status_kind(value) == "cancelled"
            for value in (surcharge.get("status"), surcharge.get("payment_state"))
        )
        if surcharge_cancelled:
            error = "После выравнивания учёта доплата отменена или возвращена; нужен ручной финансовый разбор"
            await _set_upgrade_state(job_id, "manual_review", error=error)
            await _send_upgrade_alert(job, "repair_refund", f"⚠️ Nexus: {error}")
            return
        await _set_upgrade_state(job_id, "completed", delay_seconds=6 * 60 * 60, reset_attempts=True)
        return

    if status == "repair_validated":
        ledger_error = _upgrade_ledger_error(job, origin, surcharge)
        if ledger_error:
            await _set_upgrade_state(job_id, "manual_review", error=ledger_error)
            await _send_upgrade_alert(job, "repair_ledger", f"❌ Nexus остановил выравнивание учёта\n{ledger_error}")
            return
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _set_upgrade_state(job_id, "manual_review", error="Исходный заказ должен оставаться отменённым")
            return
        if _upgrade_status_kind(surcharge.get("status")) != "completed":
            await _set_upgrade_state(job_id, "manual_review", error="Заказ доплаты должен оставаться завершённым")
            return
        if not _upgrade_premium_order_matches(job, origin) or not _upgrade_premium_order_matches(job, surcharge):
            await _set_upgrade_state(job_id, "manual_review", error="Старый сценарий уже не соответствует ожидаемому Premium-состоянию")
            return
        probe = await _run_browser_action({"action": "probe", "operation_id": f"repair-probe-{job_id}"}, timeout=90)
        if not probe.get("ok"):
            error = _clean(probe.get("error"), 1000) or "Сессия GetCourse не прошла проверку"
            await _set_upgrade_state(job_id, "repair_validated", error=error, delay_seconds=900)
            await _send_upgrade_alert(job, "repair_browser_session", f"❌ Nexus не начал выравнивание учёта\n{error}")
            return
        replacement_deal_number = _replacement_deal_number(job)
        fields = {
            settings["upgrade_link_field"]: f"repair-source:{job.get('origin_deal_number')};upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        db = await _connect()
        try:
            await db.execute(
                "UPDATE upgrade_jobs SET replacement_deal_number=?,error='',updated_at=? WHERE id=?",
                (replacement_deal_number, _iso(), job_id),
            )
            await db.commit()
        finally:
            await db.close()
        result = await _mutate_upgrade_order(
            job,
            deal_number=replacement_deal_number,
            deal_status="in_work",
            offer_id=job.get("target_offer_id"),
            deal_cost=job.get("source_payed"),
            addfields=fields,
        )
        await _set_upgrade_state(job_id, "repair_replacement_creating", delay_seconds=60)
        await _upgrade_event(
            job_id, status, "repair_replacement_requested",
            details={"deal_number": replacement_deal_number, "result": result},
        )
        return

    if status == "repair_replacement_ready":
        ledger_error = _upgrade_ledger_error(job, origin, surcharge)
        if ledger_error:
            await _set_upgrade_state(job_id, status, error=ledger_error, delay_seconds=900)
            await _send_upgrade_alert(job, "repair_ledger_before_open", f"❌ Nexus остановил перенос платежа\n{ledger_error}")
            return
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _set_upgrade_state(job_id, status, error="Перед открытием исходный заказ должен быть отменён", delay_seconds=900)
            return
        await _mutate_upgrade_order(job, deal_status="in_work")
        await _set_upgrade_state(job_id, "repair_origin_opening", delay_seconds=60)
        await _upgrade_event(job_id, status, "repair_origin_set_in_work")
        return

    if status == "repair_origin_opening":
        payment_already_moved = bool(
            origin
            and _upgrade_money_matches(job.get("source_cost"), origin.get("cost_money"))
            and _upgrade_money_matches(0, origin.get("payed_money"))
        )
        if payment_already_moved:
            surcharge_changed = not (
                surcharge
                and _upgrade_money_matches(job.get("upgrade_cost"), surcharge.get("cost_money"))
                and _upgrade_money_matches(job.get("upgrade_payed"), surcharge.get("payed_money"))
            )
            if surcharge_changed:
                error = "После переноса платежа изменились деньги отдельного заказа доплаты"
                await _set_upgrade_state(job_id, status, error=error, delay_seconds=900)
                await _send_upgrade_alert(job, "repair_ledger_after_move", f"❌ Nexus остановил перенос платежа\n{error}")
                return
            if replacement:
                replacement_error = _replacement_ledger_error(job, replacement)
                if replacement_error or not _upgrade_premium_order_matches(job, replacement):
                    error = replacement_error or "После переноса изменился состав нового Premium-заказа"
                    await _set_upgrade_state(job_id, status, error=error, delay_seconds=900)
                    await _send_upgrade_alert(job, "repair_replacement_after_move", f"❌ Nexus остановил перенос платежа\n{error}")
                    return
            elif not _repair_payment_checkpoint(job):
                await _defer_upgrade(
                    job,
                    "Перенесённый платёж не подтверждён ни live-выгрузкой, ни точным браузерным журналом",
                    delay_seconds=180,
                )
                return
        else:
            ledger_error = _upgrade_ledger_error(job, origin, surcharge)
            if ledger_error:
                await _set_upgrade_state(job_id, status, error=ledger_error, delay_seconds=900)
                await _send_upgrade_alert(job, "repair_ledger_after_open", f"❌ Nexus остановил перенос платежа\n{ledger_error}")
                return
        if _upgrade_status_kind(origin.get("status")) != "in_work":
            await _defer_upgrade(job, "GetCourse ещё не перевёл исходный заказ в «В работе»", delay_seconds=180)
            return
        if _upgrade_status_kind(surcharge.get("status")) != "completed":
            await _set_upgrade_state(job_id, status, error="Заказ доплаты изменился перед переносом платежа", delay_seconds=900)
            return
        replacement_deal_number = _replacement_deal_number(job)
        result = await _run_browser_action(
            {
                "action": "transfer_payment",
                "operation_id": f"repair-{_clean(job.get('operation_id'), 100)}",
                "source_order_id": _clean(job.get("origin_order_id"), 100),
                "source_deal_number": _clean(job.get("origin_deal_number"), 100),
                "target_deal_number": replacement_deal_number,
                "expected_amount": float(job.get("source_payed") or 0),
            },
            timeout=180,
        )
        if not result.get("ok"):
            error = _clean(result.get("error"), 1000) or "Браузер не подтвердил перенос платежа"
            db = await _connect()
            try:
                await db.execute(
                    "UPDATE upgrade_jobs SET browser_journal=?,browser_artifact=?,updated_at=? WHERE id=?",
                    (_clean(result.get("journal"), 2000), _clean(result.get("proof"), 2000), _iso(), job_id),
                )
                await db.commit()
            finally:
                await db.close()
            await _defer_upgrade(job, error, delay_seconds=900)
            await _upgrade_event(job_id, status, "repair_browser_failed", details=result, error=error)
            await _send_upgrade_alert(
                job, "repair_browser_action",
                f"❌ Nexus не завершил перенос платежа при выравнивании учёта\n"
                f"Заказы: #{job.get('origin_deal_number')} → #{replacement_deal_number}\n{error}",
            )
            return
        await _set_upgrade_browser_result(job_id, result, replacement_deal_number)
        fields = {
            settings["upgrade_command_field"]: "finalize",
            settings["upgrade_link_field"]: f"repair-source:{job.get('origin_deal_number')};upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        await _mutate_upgrade_order(job, deal_number=replacement_deal_number, addfields=fields)
        completed = await _run_browser_action(
            {
                "action": "complete_order",
                "operation_id": f"repair-complete-{_clean(job.get('operation_id'), 100)}",
                "order_id": _clean(result.get("target_order_id"), 100),
                "deal_number": replacement_deal_number,
                "offer_id": _clean(job.get("target_offer_id"), 30),
                "expected_status": "Завершен",
                "expected_cost": float(job.get("source_payed") or 0),
                "expected_received": float(job.get("source_payed") or 0),
            },
            timeout=120,
        )
        if not completed.get("ok"):
            error = _clean(completed.get("error"), 1000) or "Браузер не завершил Premium-заказ восстановления"
            await _defer_upgrade(job, error, delay_seconds=900)
            await _send_upgrade_alert(job, "repair_complete_browser", f"❌ Nexus не завершил Premium-заказ #{replacement_deal_number}\n{error}")
            return
        await _set_upgrade_state(job_id, "repair_replacement_finalizing", delay_seconds=90)
        await _upgrade_event(
            job_id, status, "repair_payment_moved_and_recalculated",
            details={key: result.get(key) for key in ("payment_id", "target_order_id", "proof")},
        )
        return

    if status == "repair_replacement_finalizing":
        if not replacement:
            await _defer_upgrade(job, "Новый Premium-заказ ещё не виден в live-выгрузке", delay_seconds=180)
            return
        replacement_error = _replacement_ledger_error(job, replacement)
        if replacement_error:
            await _set_upgrade_state(job_id, status, error=replacement_error, delay_seconds=900)
            await _send_upgrade_alert(job, "repair_replacement_ledger", f"❌ {replacement_error}")
            return
        if _upgrade_status_kind(replacement.get("status")) != "completed":
            await _defer_upgrade(job, "GetCourse ещё не завершил новый Premium-заказ", delay_seconds=180)
            return
        if not _upgrade_premium_order_matches(job, replacement):
            await _set_upgrade_state(job_id, status, error="Новый заказ завершён с другим предложением", delay_seconds=900)
            return
        access_service = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
        access = await access_service.service_getcourse_access_snapshot(
            gc_user_id=_clean(job.get("gc_user_id"), 100), email=_clean(job.get("email"), 300),
            live=True, force=True,
        )
        if not access.get("ok") or access.get("stale") or access.get("source") != "live":
            await _defer_upgrade(job, access.get("warning") or access.get("error") or "Live-доступы недоступны", delay_seconds=180)
            return
        if not _upgrade_premium_present(_clean(job.get("course_key"), 20), access.get("groups") or []):
            await _defer_upgrade(job, "Новый Premium-заказ завершён, но Premium-доступ ещё не выдан", delay_seconds=180)
            return
        await _mutate_upgrade_order(
            job,
            offer_id=origin_offer,
            deal_cost=job.get("source_cost"),
            addfields={
                settings["upgrade_command_field"]: "prepare",
                settings["upgrade_link_field"]: f"replacement:{job.get('replacement_deal_number')}",
                settings["upgrade_operation_field"]: job.get("operation_id"),
            },
        )
        await _set_upgrade_state(job_id, "repair_restoring_origin", delay_seconds=90)
        await _upgrade_event(job_id, status, "repair_origin_restore_requested", details={"offer_id": origin_offer})
        return

async def _process_upgrade_job(job: dict[str, Any], settings: dict[str, str]) -> None:
    if _clean(job.get("strategy"), 40) == "replacement_browser_repair":
        await _process_browser_legacy_repair_job(job, settings)
        return
    if _clean(job.get("strategy"), 40) == "replacement_browser":
        await _process_browser_upgrade_job(job, settings)
        return
    job_id = int(job["id"])
    status = _clean(job.get("status"), 80)
    snapshots = await _upgrade_snapshots(job, live=status not in {"validated"})
    origin = snapshots.get(_clean(job.get("origin_order_id"), 100), {})
    surcharge = snapshots.get(_clean(job.get("upgrade_order_id"), 100), {})
    surcharge_cancelled = any(
        _upgrade_status_kind(value) == "cancelled"
        for value in (surcharge.get("status"), surcharge.get("payment_state"))
    )
    if surcharge_cancelled and status not in {
        # During the forward transaction Nexus intentionally cancels the
        # surcharge order before rebuilding that order as Premium.
        "surcharge_preparing", "in_work", "finalizing",
        "rollback_pending", "rollback_in_work", "rollback_finalizing", "rollback_verifying",
    }:
        conflicts = _upgrade_refund_conflicts(job, snapshots.get("__related__") or [])
        if conflicts:
            message = "Найден другой активный Premium/VIP-заказ этого курса; автоматический откат остановлен"
            await _set_upgrade_state(job_id, "manual_review", error=message)
            await _upgrade_event(
                job_id, status, "refund_conflict", details={"order_ids": [item.get("order_id") for item in conflicts]},
                error=message,
            )
            return
        await _set_upgrade_state(job_id, "rollback_pending", error="Доплата отменена или возвращена")
        await _upgrade_event(job_id, status, "refund_detected")
        return
    if status == "completed":
        # Completed upgrades stay under a low-frequency refund watch.  No write
        # is performed while both linked orders remain unchanged.
        await _set_upgrade_state(job_id, "completed", delay_seconds=6 * 60 * 60, reset_attempts=True)
        return
    if status.startswith("rollback_"):
        if not surcharge_cancelled:
            await _set_upgrade_state(job_id, "manual_review", error="Статус доплаты снова изменился во время отката")
            return
        if status == "rollback_pending":
            if _upgrade_status_kind(origin.get("status")) != "cancelled":
                await _set_upgrade_state(
                    job_id, "manual_review",
                    error="Исходный Standard-заказ неожиданно активен; безопасный откат остановлен",
                )
                return
            await _mutate_upgrade_order(job, deal_status="in_work")
            await _set_upgrade_state(job_id, "rollback_in_work", delay_seconds=60)
            await _upgrade_event(job_id, status, "rollback_in_work_requested")
            return
        if status == "rollback_in_work":
            if _upgrade_status_kind(origin.get("status")) != "in_work":
                await _defer_upgrade(job, "GetCourse ещё не перевёл Premium-заказ в «В работе»")
                return
            fields = {
                # The operation id lives in its own field.  Keep the command
                # itself stable so a native GetCourse process can match it
                # exactly and cannot confuse this stage with rollback_pending.
                settings["upgrade_command_field"]: "finalize_rollback",
                settings["upgrade_link_field"]: f"refund:{job.get('upgrade_deal_number')}",
                settings["upgrade_operation_field"]: job.get("operation_id"),
            }
            await _mutate_upgrade_order(
                job, offer_id=job.get("source_offer_id"), deal_cost=job.get("source_cost"), addfields=fields,
            )
            await _set_upgrade_state(job_id, "rollback_finalizing", delay_seconds=90)
            await _upgrade_event(job_id, status, "standard_offer_restored")
            return
        if status == "rollback_finalizing":
            if _upgrade_status_kind(origin.get("status")) != "completed":
                await _defer_upgrade(job, "GetCourse ещё не завершил обратную замену", delay_seconds=180)
                return
            if _clean(origin.get("offer_id"), 30) != _clean(job.get("source_offer_id"), 30):
                await _set_upgrade_state(job_id, "manual_review", error="После отката завершено другое предложение")
                return
            await _set_upgrade_state(job_id, "rollback_verifying", delay_seconds=30)
            return
        if status == "rollback_verifying":
            access_service = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
            access = await access_service.service_getcourse_access_snapshot(
                gc_user_id=_clean(job.get("gc_user_id"), 100), email=_clean(job.get("email"), 300),
                live=True, force=True,
            )
            if not access.get("ok") or access.get("stale") or access.get("source") != "live":
                await _defer_upgrade(job, access.get("warning") or access.get("error") or "Live-доступы ещё не готовы", delay_seconds=180)
                return
            if not _upgrade_access_ready(_clean(job.get("course_key"), 20), access.get("groups") or [], rollback=True):
                await _defer_upgrade(job, "Standard ещё не заменил Premium в группах GetCourse", delay_seconds=180)
                return
            await _set_upgrade_state(job_id, "rolled_back", error="Доплата возвращена; Standard восстановлен")
            await _upgrade_event(job_id, status, "rolled_back")
            return
    ledger_error = _upgrade_ledger_error(job, origin, surcharge)
    if ledger_error:
        await _set_upgrade_state(job_id, "manual_review", error=ledger_error)
        await _upgrade_event(job_id, status, "blocked", error=ledger_error)
        return
    if status == "validated":
        if settings.get("upgrade_process_confirmed") != "1":
            await _set_upgrade_state(job_id, "waiting_config", error="Не подтверждён процесс финализации GetCourse")
            return
        surcharge_fields = {
            settings["upgrade_link_field"]: f"source:{job.get('origin_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        await _mutate_upgrade_order(
            job, deal_number=job.get("upgrade_deal_number"), addfields=surcharge_fields,
        )
        fields = {
            # Moving a completed order to in_work through Import API does not
            # revoke its purchases. A native process first cancels the order;
            # Nexus then safely reuses that same order for Premium.
            settings["upgrade_command_field"]: "prepare",
            settings["upgrade_link_field"]: f"upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        await _mutate_upgrade_order(job, addfields=fields)
        await _set_upgrade_state(job_id, "preparing", delay_seconds=90)
        await _upgrade_event(job_id, "validated", "standard_cancel_requested")
        return
    if status == "waiting_config":
        if settings.get("upgrade_process_confirmed") == "1":
            await _set_upgrade_state(job_id, "validated")
        return
    if status == "preparing":
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _defer_upgrade(job, "GetCourse ещё не отменил Standard-заказ перед заменой", delay_seconds=180)
            return
        fields = {
            # The paid surcharge order becomes the Premium access order. Its
            # own purchase is cancelled first so changing the offer cannot
            # retain a stale surcharge entitlement.
            settings["upgrade_command_field"]: "prepare",
            settings["upgrade_link_field"]: f"source:{job.get('origin_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        await _mutate_upgrade_order(
            job, deal_number=job.get("upgrade_deal_number"), addfields=fields,
        )
        await _set_upgrade_state(job_id, "surcharge_preparing", delay_seconds=90)
        await _upgrade_event(job_id, "preparing", "surcharge_cancel_requested")
        return
    if status == "surcharge_preparing":
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _set_upgrade_state(job_id, "manual_review", error="Standard-заказ снова активен")
            return
        if _upgrade_status_kind(surcharge.get("status")) != "cancelled":
            await _defer_upgrade(job, "GetCourse ещё не отменил заказ доплаты перед заменой", delay_seconds=180)
            return
        result = await _mutate_upgrade_order(
            job, deal_number=job.get("upgrade_deal_number"), deal_status="in_work",
        )
        await _set_upgrade_state(job_id, "in_work", delay_seconds=60)
        await _upgrade_event(job_id, "surcharge_preparing", "in_work_requested", details=result)
        return
    if status == "in_work":
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _set_upgrade_state(job_id, "manual_review", error="Standard-заказ снова активен")
            return
        if _upgrade_status_kind(surcharge.get("status")) != "in_work":
            await _defer_upgrade(job, "GetCourse ещё не перевёл заказ доплаты в «В работе»")
            return
        fields = {
            # GetCourse text-field rules match exact values.  The separate
            # operation field remains the idempotency/audit key.
            settings["upgrade_command_field"]: "finalize",
            settings["upgrade_link_field"]: f"upgrade:{job.get('upgrade_deal_number')}",
            settings["upgrade_operation_field"]: job.get("operation_id"),
        }
        result = await _mutate_upgrade_order(
            job, deal_number=job.get("upgrade_deal_number"), offer_id=job.get("target_offer_id"),
            deal_cost=job.get("upgrade_cost"), addfields=fields,
        )
        await _set_upgrade_state(job_id, "finalizing", delay_seconds=90)
        await _upgrade_event(job_id, "in_work", "offer_changed", details=result)
        return
    if status == "finalizing":
        if _upgrade_status_kind(origin.get("status")) != "cancelled":
            await _set_upgrade_state(job_id, "manual_review", error="Standard-заказ снова активен")
            return
        if _upgrade_status_kind(surcharge.get("status")) != "completed":
            await _defer_upgrade(job, "GetCourse ещё не завершил Premium-заказ", delay_seconds=180)
            return
        actual_offer = _clean(surcharge.get("offer_id"), 30)
        if not _upgrade_premium_order_matches(job, surcharge):
            await _set_upgrade_state(job_id, "manual_review", error="GetCourse завершил заказ с другим предложением")
            await _upgrade_event(
                job_id,
                "finalizing",
                "offer_mismatch",
                details={
                    "offer_id": actual_offer,
                    "target_offer_id": _clean(job.get("target_offer_id"), 30),
                    "tariff": _clean(surcharge.get("tariff"), 40),
                    "course_key": _clean(surcharge.get("course_key"), 20),
                    "title": _clean(surcharge.get("title"), 500),
                },
                error="GetCourse завершил заказ с другим предложением",
            )
            return
        await _set_upgrade_state(job_id, "verifying", delay_seconds=30)
        await _upgrade_event(
            job_id,
            "finalizing",
            "order_verified",
            details={
                "offer_id": actual_offer,
                "target_offer_id": _clean(job.get("target_offer_id"), 30),
                "tariff": _clean(surcharge.get("tariff"), 40),
                "course_key": _clean(surcharge.get("course_key"), 20),
                "used_export_fallback": actual_offer != _clean(job.get("target_offer_id"), 30),
            },
        )
        return
    if status == "verifying":
        access_service = _module("getcourse-chat-fields", "service_getcourse_access_snapshot")
        access = await access_service.service_getcourse_access_snapshot(
            gc_user_id=_clean(job.get("gc_user_id"), 100), email=_clean(job.get("email"), 300),
            live=True, force=True,
        )
        if not access.get("ok") or access.get("stale") or access.get("source") != "live":
            await _defer_upgrade(job, access.get("warning") or access.get("error") or "Live-доступы ещё не готовы", delay_seconds=180)
            return
        if not _upgrade_access_ready(_clean(job.get("course_key"), 20), access.get("groups") or []):
            await _defer_upgrade(job, "Premium ещё не заменил Standard в группах GetCourse", delay_seconds=180)
            return
        await _set_upgrade_state(job_id, "access_ready")
        await _upgrade_event(job_id, "verifying", "access_ready")
        job = await _upgrade_job(job_id) or job
        status = "access_ready"
    if status == "access_ready":
        await _send_upgrade_links(job, settings)
        await _set_upgrade_state(
            job_id, "completed", delay_seconds=6 * 60 * 60, completed=True, chat_sent=True,
        )
        await _upgrade_event(job_id, "access_ready", "completed")


async def _process_due_upgrades() -> int:
    settings = await _settings()
    if settings.get("upgrade_enabled") != "1" or settings.get("upgrade_mode") != "live":
        return 0
    now = _iso()
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """SELECT * FROM upgrade_jobs
                   WHERE approved=1 AND status IN (
                     'validated','waiting_config','waiting_browser','replacement_creating','replacement_finalizing','origin_canceling',
                     'repair_validated','repair_replacement_creating','repair_replacement_finalizing',
                     'repair_replacement_ready','repair_origin_opening','repair_restoring_origin','repair_restoring_surcharge',
                     'preparing','surcharge_preparing','in_work','finalizing','verifying','access_ready','completed',
                     'rollback_pending','rollback_in_work','rollback_finalizing','rollback_verifying'
                   )
                     AND (next_attempt_at='' OR next_attempt_at<=?)
                   ORDER BY id LIMIT 10""",
                (now,),
            )
        ).fetchall()
    finally:
        await db.close()
    processed = 0
    for raw in rows:
        job = dict(raw)
        try:
            await _process_upgrade_job(job, settings)
            processed += 1
        except DeliveryUncertain as exc:
            await _set_upgrade_state(int(job["id"]), "manual_review", error=str(exc))
            await _send_upgrade_alert(
                job, "delivery_uncertain",
                f"❌ Nexus: неопределённый результат отправки ссылок\n"
                f"Пользователь GetCourse: {job.get('gc_user_id')}\n{_clean(exc, 1000)}",
            )
        except Exception as exc:
            await _defer_upgrade(job, _clean(exc, 1000), delay_seconds=180)
            if (
                _clean(job.get("strategy"), 40).startswith("replacement_browser")
                and _should_alert_upgrade_exception(job, exc)
            ):
                completed_monitor = _clean(job.get("status"), 80) == "completed"
                await _send_upgrade_alert(
                    job, "completed_monitor" if completed_monitor else "worker_exception",
                    (
                        "⚠️ Nexus трижды не смог проверить возврат завершённой доплаты\n"
                        f"Пользователь GetCourse: {job.get('gc_user_id')}\n"
                        f"{_clean(exc, 1000)}\n"
                        "Сама завершённая доплата и платежи не изменялись."
                    ) if completed_monitor else (
                        "❌ Ошибка автоматизации доплаты Nexus\n"
                        f"Пользователь GetCourse: {job.get('gc_user_id')}\n"
                        f"Этап: {job.get('status')}\n{_clean(exc, 1000)}"
                    ),
                )
            if _logger:
                _logger.warning("onboarding upgrade job=%s failed: %s", job.get("id"), exc)
    return processed


async def _classify_pending_orders(limit: int = 50) -> int:
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                "SELECT * FROM orders WHERE status='classification_needed' ORDER BY paid_at,id LIMIT ?",
                (max(1, min(250, int(limit or 50))),),
            )
        ).fetchall()
    finally:
        await db.close()
    classified = 0
    for raw in rows:
        row = dict(raw)
        try:
            branch = _scenario_branch(row)
            db = await _connect()
            try:
                cursor = await db.execute(
                    """UPDATE orders SET branch=?,status='pending',error='',updated_at=?
                       WHERE id=? AND status='classification_needed'""",
                    (branch, _iso(), row["id"]),
                )
                await db.commit()
                classified += max(0, cursor.rowcount)
            finally:
                await db.close()
        except Exception as exc:
            db = await _connect()
            try:
                await db.execute(
                    "UPDATE orders SET error=?,updated_at=? WHERE id=? AND status='classification_needed'",
                    (_clean(exc, 1000), _iso(), row["id"]),
                )
                await db.commit()
            finally:
                await db.close()
            if _logger:
                _logger.warning("onboarding scenario classification failed order=%s: %s", row.get("order_id"), exc)
    return classified


def _first_name(name: str) -> str:
    return (_clean(name, 300).split() or ["Здравствуйте"])[0]


def _standard_upgrade_url(row: dict[str, Any], settings: dict[str, str]) -> str:
    course_key = _clean(row.get("course_key"), 50).casefold()
    course = _clean(row.get("course"), 100).casefold()
    if course_key == "puppy" or "щен" in course:
        return (
            settings.get("standard_upgrade_puppy_url")
            or settings.get("standard_upgrade_url")
            or settings.get("upgrade_url", "")
        )
    if course_key == "dog" or "собак" in course:
        return (
            settings.get("standard_upgrade_dog_url")
            or settings.get("standard_upgrade_url")
            or settings.get("upgrade_url", "")
        )
    return settings.get("standard_upgrade_url") or settings.get("upgrade_url", "")


def _render(body: str, row: dict[str, Any], settings: dict[str, str]) -> str:
    tariff = _clean(row.get("tariff"), 40).casefold()
    if tariff == "premium":
        upgrade_url = settings.get("premium_upgrade_url") or settings.get("upgrade_url", "")
    elif tariff == "standard":
        upgrade_url = _standard_upgrade_url(row, settings)
    else:
        upgrade_url = settings.get("upgrade_url", "")
    values = {
        "name": _clean(row.get("name"), 300),
        "first_name": _first_name(row.get("name") or ""),
        "course": _clean(row.get("course"), 100),
        "stream": _clean(row.get("stream"), 50),
        "vk_link": _clean(row.get("vk_link"), 2000),
        "tg_link": _clean(row.get("tg_link"), 2000),
        "video_instruction_url": settings.get("video_instruction_url", ""),
        "text_instruction_url": settings.get("text_instruction_url", ""),
        "upgrade_url": upgrade_url,
    }
    for key, value in values.items():
        body = body.replace("{" + key + "}", value)
    return body.strip()


def _email_package_key(course_key: Any, tariff: Any) -> str:
    course = _clean(course_key, 50).casefold()
    level = _clean(tariff, 40).casefold()
    if course not in {"puppy", "dog", "combo"}:
        return ""
    return f"{course}:{'standard-start' if level == 'standard' else 'premium-entry'}"


def _email_trigger_group(package_key: Any, settings: dict[str, str]) -> str:
    """Return one stable GetCourse trigger group per logical email package.

    GetCourse creates no second task for the same user in one periodic process,
    so Standard and Premium (and each course) must never share that process.
    """
    package = _clean(package_key, 100).casefold()
    safe_package = re.sub(r"[^a-z0-9_-]+", "-", package).strip("-")
    if not safe_package:
        raise RuntimeError("Email-пакет без безопасного ключа триггер-группы")
    template = _clean(
        settings.get("email_trigger_group_template") or "Nexus email {package_key}", 200
    )
    if "{package_key}" not in template:
        raise RuntimeError("Шаблон email-группы не содержит {package_key}")
    group = _clean(template.replace("{package_key}", safe_package), 200)
    if not group.startswith("Nexus email "):
        raise RuntimeError("Email-группа вне разрешённого пространства Nexus")
    return group


def _email_subject(package_key: str, course: str) -> str:
    if package_key.endswith(":standard-start"):
        return f"Поздравляем с началом обучения — курс «{course}»"
    return f"Доступ и чаты курса «{course}»"


async def _email_is_held(gc_user_id: str) -> tuple[bool, str]:
    db = await _connect()
    try:
        row = await (
            await db.execute(
                "SELECT reason FROM email_recipient_holds WHERE gc_user_id=?", (_clean(gc_user_id, 100),)
            )
        ).fetchone()
    finally:
        await db.close()
    return (bool(row), _clean(row["reason"], 500) if row else "")


async def _store_email_package(item: dict[str, Any], settings: dict[str, str], *, force_hold: bool = False) -> int:
    gc_user_id = _clean(item.get("gc_user_id"), 100)
    package_key = _clean(item.get("package_key"), 100)
    if not gc_user_id or not package_key:
        raise RuntimeError("Email-пакет без gc_user_id или package_key")
    held, held_reason = await _email_is_held(gc_user_id)
    live_ready = (
        settings.get("email_enabled") == "1"
        and settings.get("email_mode") == "live"
        and settings.get("email_process_confirmed") == "1"
        and not held
        and not force_hold
    )
    status = "ready" if live_ready else "held"
    hold_reason = "" if live_ready else (
        held_reason or ("Ожидает отдельной команды" if force_hold else "Email-рассылка на паузе")
    )
    operation_id = hashlib.sha256(f"email:{gc_user_id}:{package_key}".encode()).hexdigest()[:32]
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            """
            INSERT INTO email_packages(
                gc_user_id,email,phone,name,package_key,source_kind,source_id,source_order_id,
                course_key,tariff,stream,vk_link,tg_link,template_key,subject,body,operation_id,
                status,hold_reason,next_attempt_at,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(gc_user_id,package_key,channel) DO UPDATE SET
                email=excluded.email,phone=excluded.phone,name=excluded.name,
                stream=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.stream ELSE email_packages.stream END,
                vk_link=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.vk_link ELSE email_packages.vk_link END,
                tg_link=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.tg_link ELSE email_packages.tg_link END,
                subject=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.subject ELSE email_packages.subject END,
                body=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.body ELSE email_packages.body END,
                template_key=CASE WHEN email_packages.status IN ('held','ready','failed') THEN excluded.template_key ELSE email_packages.template_key END,
                updated_at=excluded.updated_at
            """,
            (
                gc_user_id, _clean(item.get("email"), 300), _clean(item.get("phone"), 100),
                _clean(item.get("name"), 300), package_key, _clean(item.get("source_kind"), 40),
                _clean(item.get("source_id"), 100), _clean(item.get("source_order_id"), 100),
                _clean(item.get("course_key"), 50), _clean(item.get("tariff"), 40),
                _clean(item.get("stream"), 50), _clean(item.get("vk_link"), 2000),
                _clean(item.get("tg_link"), 2000), _clean(item.get("template_key"), 80),
                _clean(item.get("subject"), 500), _clean(item.get("body"), 10000), operation_id,
                status, hold_reason, now if status == "ready" else "", now, now,
            ),
        )
        row = await (
            await db.execute(
                "SELECT id FROM email_packages WHERE gc_user_id=? AND package_key=? AND channel='email'",
                (gc_user_id, package_key),
            )
        ).fetchone()
        await db.commit()
        return int(row["id"])
    finally:
        await db.close()


async def _email_from_order(row: dict[str, Any], settings: dict[str, str], *, force_hold: bool = False) -> int:
    row = await _ensure_order_flow(dict(row))
    package_key = _email_package_key(row.get("course_key"), row.get("tariff"))
    if not package_key:
        return 0
    template_key = _template_key_for_order(row)
    if package_key.endswith(":premium-entry") and not (
        _clean(row.get("stream"), 50) and _clean(row.get("vk_link"), 2000) and _clean(row.get("tg_link"), 2000)
    ):
        raise RuntimeError("Для Premium email-пакета не найдена полная пара ссылок потока")
    body = _render(await _template(template_key), row, settings)
    return await _store_email_package(
        {
            **row,
            "package_key": package_key,
            "source_kind": "order",
            "source_id": str(row.get("id") or ""),
            "source_order_id": row.get("order_id"),
            "template_key": template_key,
            "subject": _email_subject(package_key, _clean(row.get("course"), 100)),
            "body": body,
        },
        settings,
        force_hold=force_hold,
    )


async def service_queue_flow_email(
    *, gc_user_id: str, email: str, order_id: str, course_key: str,
    course: str, stream: str, vk_link: str, tg_link: str,
) -> dict[str, Any]:
    """Queue one invitation when an existing student moves to a new course.

    The logical package key makes retries from the transfer workflow
    idempotent.  Chat-bearing transfers are Premium/VIP access, and both map
    to the same ``premium-entry`` package so Premium -> VIP cannot duplicate
    the invitation.
    """
    settings = await _settings()
    package_key = _email_package_key(course_key, "premium")
    if not package_key:
        raise RuntimeError("Email перехода: неизвестный курс")
    row = {
        "gc_user_id": _clean(gc_user_id, 100),
        "email": _clean(email, 300),
        "phone": "",
        "name": "",
        "course_key": _clean(course_key, 50),
        "course": _clean(course, 100) or ("Щенок" if course_key == "puppy" else "Собака"),
        "tariff": "premium",
        "stream": _clean(stream, 50),
        "vk_link": _clean(vk_link, 2000),
        "tg_link": _clean(tg_link, 2000),
    }
    if not row["gc_user_id"] or not row["email"] or not row["stream"] or not row["vk_link"] or not row["tg_link"]:
        raise RuntimeError("Email перехода: не хватает точного пользователя, потока или пары ссылок")
    body = _render(await _template("flow_transition"), row, settings)
    try:
        package_id = await _store_email_package(
            {
                **row,
                "package_key": package_key,
                "source_kind": "flow_transition",
                "source_id": f"{_clean(order_id, 100)}:{row['course_key']}:{row['stream']}",
                "source_order_id": _clean(order_id, 100),
                "template_key": "flow_transition",
                "subject": f"Открыт курс «{row['course']}» — ссылки Вашего потока",
                "body": body,
            },
            settings,
        )
    except Exception as exc:
        try:
            await service_system_alert(
                kind="email_flow_transition",
                message=(
                    "❌ Nexus не поставил приглашение после перехода в новый поток\n"
                    f"GetCourse user: {row['gc_user_id']}\nКурс: {row['course_key']}\n"
                    f"Поток: {row['stream']}\nОшибка: {_clean(exc, 1000)}"
                ),
            )
        except Exception:
            if _logger:
                _logger.exception("flow transition email alert failed")
        raise
    return {"ok": True, "package_id": package_id, "package_key": package_key}


async def _email_from_upgrade(job: dict[str, Any], settings: dict[str, str], *, force_hold: bool = False) -> int:
    course_key = _clean(job.get("course_key"), 20)
    package_key = _email_package_key(course_key, "premium")
    flows = await _upgrade_flows(job)
    row = {
        "id": int(job.get("id") or 0),
        "gc_user_id": _clean(job.get("gc_user_id"), 100),
        "email": _clean(job.get("email"), 300),
        "phone": _clean(job.get("phone"), 100),
        "name": _clean(job.get("name"), 300),
        "course_key": course_key,
        "course": _upgrade_course_label(course_key),
        "tariff": "premium",
    }
    if course_key == "combo":
        template_key = "upgrade_premium_combo"
        body = await _template(template_key)
        values = {
            "first_name": _first_name(row["name"]),
            "puppy_stream": flows["puppy"]["stream"],
            "puppy_vk_link": flows["puppy"]["vk_link"],
            "puppy_tg_link": flows["puppy"]["tg_link"],
            "dog_stream": flows["dog"]["stream"],
            "dog_vk_link": flows["dog"]["vk_link"],
            "dog_tg_link": flows["dog"]["tg_link"],
        }
        for key, value in values.items():
            body = body.replace("{" + key + "}", value)
        stream = f"{flows['puppy']['stream']} / {flows['dog']['stream']}"
        vk_link = f"{flows['puppy']['vk_link']}\n{flows['dog']['vk_link']}"
        tg_link = f"{flows['puppy']['tg_link']}\n{flows['dog']['tg_link']}"
    else:
        template_key = "upgrade_premium"
        flow = flows[course_key]
        row.update(flow)
        body = _render(await _template(template_key), row, settings)
        stream, vk_link, tg_link = flow["stream"], flow["vk_link"], flow["tg_link"]
    return await _store_email_package(
        {
            **row,
            "package_key": package_key,
            "source_kind": "upgrade",
            "source_id": str(job.get("id") or ""),
            "source_order_id": job.get("upgrade_order_id"),
            "stream": stream,
            "vk_link": vk_link,
            "tg_link": tg_link,
            "template_key": template_key,
            "subject": _email_subject(package_key, row["course"]),
            "body": body,
        },
        settings,
        force_hold=force_hold,
    )


async def _discover_email_packages(*, gc_user_id: str = "", force_hold: bool = False) -> int:
    settings = await _settings()
    baseline = _parse_dt(settings.get("email_baseline_at"))
    if not baseline and not gc_user_id:
        await _setting_updates({"email_baseline_at": _iso()})
        return 0
    cutoff = _iso(baseline) if baseline else ""
    wanted = _clean(gc_user_id, 100)
    db = await _connect()
    try:
        if wanted:
            upgrades = await (
                await db.execute(
                    "SELECT * FROM upgrade_jobs WHERE gc_user_id=? AND status='completed' ORDER BY id", (wanted,)
                )
            ).fetchall()
        else:
            upgrades = await (
                await db.execute(
                    "SELECT * FROM upgrade_jobs WHERE status='completed' AND completed_at>=? ORDER BY id", (cutoff,)
                )
            ).fetchall()
        linked_replacements = {
            _clean(row["replacement_order_id"], 100)
            for row in await (await db.execute("SELECT replacement_order_id FROM upgrade_jobs WHERE replacement_order_id<>''")).fetchall()
        }
        if wanted:
            orders = await (
                await db.execute("SELECT * FROM orders WHERE gc_user_id=? ORDER BY paid_at,id", (wanted,))
            ).fetchall()
        else:
            orders = await (
                await db.execute("SELECT * FROM orders WHERE paid_at>=? ORDER BY paid_at,id", (cutoff,))
            ).fetchall()
    finally:
        await db.close()
    stored = 0
    upgraded_courses = {_clean(dict(raw).get("course_key"), 20) for raw in upgrades}
    for raw in upgrades:
        await _email_from_upgrade(dict(raw), settings, force_hold=force_hold)
        stored += 1
    for raw in orders:
        row = dict(raw)
        if _clean(row.get("order_id"), 100) in linked_replacements:
            continue
        if wanted and _clean(row.get("tariff"), 40).casefold() == "standard" and _clean(row.get("course_key"), 20) in upgraded_courses:
            continue
        # A VIP order after an earlier Premium order is not a new package.
        # The unique package key also protects webhook repeats and replacements.
        await _email_from_order(row, settings, force_hold=force_hold)
        stored += 1
    return stored


async def _email_fail(item: dict[str, Any], error: Any, *, uncertain: bool = False) -> None:
    attempts = int(item.get("attempts") or 0) + 1
    status = "manual_review" if uncertain or attempts >= 5 else "failed"
    next_attempt = "" if status == "manual_review" else _iso(_now_dt() + timedelta(minutes=min(60, 2 ** attempts)))
    db = await _connect()
    try:
        await db.execute(
            "UPDATE email_packages SET status=?,attempts=?,next_attempt_at=?,error=?,updated_at=? WHERE id=?",
            (status, attempts, next_attempt, _clean(error, 1000), _iso(), int(item["id"])),
        )
        await db.commit()
    finally:
        await db.close()
    if status == "manual_review":
        await _send_upgrade_alert(
            {"id": -int(item["id"])},
            f"email_{item.get('package_key')}",
            "❌ Nexus остановил email-пакет\n"
            f"GetCourse user: {item.get('gc_user_id')}\n"
            f"Пакет: {item.get('package_key')}\n{_clean(error, 1000)}\n"
            "Автоповтор отключён, чтобы не отправить дубль.",
        )


async def _process_due_email_packages() -> int:
    settings = await _settings()
    if not (
        settings.get("email_enabled") == "1"
        and settings.get("email_mode") == "live"
        and settings.get("email_process_confirmed") == "1"
    ):
        return 0
    now = _iso()
    timeout_minutes = max(5, min(1440, int(settings.get("email_callback_timeout_minutes") or 30)))
    timeout_before = _iso(_now_dt() - timedelta(minutes=timeout_minutes))
    db = await _connect()
    try:
        timed_out = await (
            await db.execute(
                "SELECT * FROM email_packages WHERE status='awaiting_callback' AND triggered_at<? LIMIT 20",
                (timeout_before,),
            )
        ).fetchall()
        row = await (
            await db.execute(
                "SELECT * FROM email_packages WHERE status IN ('ready','failed') "
                "AND (next_attempt_at='' OR next_attempt_at<=?) ORDER BY id LIMIT 1", (now,)
            )
        ).fetchone()
        if row:
            cursor = await db.execute(
                "UPDATE email_packages SET status='triggering',updated_at=? WHERE id=? AND status IN ('ready','failed')",
                (now, int(row["id"])),
            )
            await db.commit()
            row = row if cursor.rowcount == 1 else None
    finally:
        await db.close()
    for raw in timed_out:
        await _email_fail(dict(raw), "GetCourse не подтвердил выполнение процесса в установленный срок", uncertain=True)
    if not row:
        return len(timed_out)
    item = dict(row)
    callback_base = _clean(settings.get("public_base"), 2000).rstrip("/")
    callback_secret = _clean(settings.get("email_callback_secret"), 300)
    callback_url = (
        f"{callback_base}/{MODULE_ID}/api/email/callback/{quote(callback_secret, safe='')}"
        f"?operation_id={quote(item['operation_id'], safe='')}"
    )
    fields = {
        "Nexus email операция": item["operation_id"],
        "Nexus email пакет": item["package_key"],
        "Nexus email тема": item["subject"],
        # GetCourse substitutes additional-field values as text. Preserve the
        # source line breaks; the mailing template renders them with pre-line.
        "Nexus email текст": item["body"],
        "Nexus email callback": callback_url,
    }
    try:
        service = _module("getcourse-chat-fields", "service_trigger_onboarding_email")
        result = await service.service_trigger_onboarding_email(
            gc_user_id=item["gc_user_id"], email=item["email"], phone=item["phone"],
            group_name=_email_trigger_group(item["package_key"], settings),
            fields=fields,
        )
        if not result.get("ok"):
            await _email_fail(item, result.get("error") or "GetCourse отклонил email-триггер")
            return len(timed_out) + 1
    except Exception as exc:
        await _email_fail(item, f"Результат Import API неизвестен: {_clean(exc, 800)}", uncertain=True)
        return len(timed_out) + 1
    db = await _connect()
    try:
        await db.execute(
            "UPDATE email_packages SET status='awaiting_callback',triggered_at=?,error='',updated_at=? "
            "WHERE id=? AND status='triggering'",
            (_iso(), _iso(), int(item["id"])),
        )
        await db.commit()
    finally:
        await db.close()
    return len(timed_out) + 1


def _split_message(text: str) -> list[str]:
    text = _clean(text, 30000)
    if len(text) <= MAX_MESSAGE:
        return [text] if text else []
    chunks: list[str] = []
    current = ""
    for paragraph in re.split(r"\n{2,}", text):
        candidate = f"{current}\n\n{paragraph}".strip() if current else paragraph
        if len(candidate) <= MAX_MESSAGE:
            current = candidate
            continue
        if current:
            chunks.append(current)
        while len(paragraph) > MAX_MESSAGE:
            cut = paragraph.rfind("\n", 0, MAX_MESSAGE)
            if cut < MAX_MESSAGE // 2:
                cut = paragraph.rfind(" ", 0, MAX_MESSAGE)
            cut = cut if cut > 0 else MAX_MESSAGE
            chunks.append(paragraph[:cut].strip())
            paragraph = paragraph[cut:].strip()
        current = paragraph
    if current:
        chunks.append(current)
    return chunks


def _active_test_flow(items: list[dict[str, Any]], course_key: str) -> dict[str, Any]:
    today = datetime.now(MOSCOW).date()
    candidates: list[tuple[datetime, int, dict[str, Any]]] = []
    fallback: list[tuple[int, dict[str, Any]]] = []
    for raw in items:
        if not isinstance(raw, dict) or _clean(raw.get("course_key"), 50) != course_key:
            continue
        if not (_clean(raw.get("vk_link"), 2000) and _clean(raw.get("tg_link"), 2000)):
            continue
        stream_text = _clean(raw.get("stream") or raw.get("stream_number"), 50)
        stream_number = int(stream_text) if stream_text.isdigit() else 0
        fallback.append((stream_number, raw))
        start = _parse_dt(raw.get("date_start"))
        if start and start.date() <= today:
            candidates.append((start, stream_number, raw))
    if candidates:
        return max(candidates, key=lambda value: (value[0], value[1]))[2]
    return max(fallback, key=lambda value: value[0])[1] if fallback else {}


async def _test_preview_messages(settings: dict[str, str]) -> list[tuple[str, str]]:
    fields = _module("getcourse-chat-fields", "service_flow_catalog")
    catalog = await fields.service_flow_catalog()
    if not catalog.get("ok"):
        raise RuntimeError(catalog.get("error") or "Не удалось получить актуальные потоки")
    items = [item for item in catalog.get("items") or [] if isinstance(item, dict)]
    puppy, dog = (_active_test_flow(items, key) for key in ("puppy", "dog"))
    if not puppy or not dog:
        raise RuntimeError("Не найдены актуальные ссылки потоков для обоих курсов")
    safe_settings = dict(settings)
    safe_settings["standard_upgrade_url"] = safe_settings.get("standard_upgrade_url") or "[ссылка не настроена]"
    safe_settings["standard_upgrade_puppy_url"] = safe_settings.get("standard_upgrade_puppy_url") or "[ссылка не настроена]"
    safe_settings["standard_upgrade_dog_url"] = safe_settings.get("standard_upgrade_dog_url") or "[ссылка не настроена]"
    safe_settings["premium_upgrade_url"] = safe_settings.get("premium_upgrade_url") or "[ссылка не настроена]"
    samples = [
        ("manager-premium", "[ТЕСТ · ПРОДАЖА МЕНЕДЖЕРА · PREMIUM/VIP]", "manager", puppy, "Щенок", "Premium"),
        ("manager-standard", "[ТЕСТ · ПРОДАЖА МЕНЕДЖЕРА · СТАНДАРТ]", "manager_standard", dog, "Собака", "Стандарт"),
        ("autopay-vip", "[ТЕСТ · АВТООПЛАТА · VIP]", "autopay_vip", dog, "Собака", "VIP"),
        ("autopay-premium", "[ТЕСТ · АВТООПЛАТА · PREMIUM → VIP]", "premium", dog, "Собака", "Premium"),
        ("autopay-standard", "[ТЕСТ · АВТООПЛАТА · СТАНДАРТ]", "standard", puppy, "Щенок", "Стандарт"),
    ]
    result: list[tuple[str, str]] = []
    for key, heading, template_key, flow, course, tariff in samples:
        row = {
            "name": "Тестовый пользователь", "course": course, "tariff": tariff,
            "stream": _clean(flow.get("stream") or flow.get("stream_number"), 50),
            "vk_link": _clean(flow.get("vk_link"), 2000), "tg_link": _clean(flow.get("tg_link"), 2000),
        }
        result.append((key, f"{heading}\n\n{_render(await _template(template_key), row, safe_settings)}"))
    reminder_row = {"name": "Тестовый пользователь", "course": "Собака"}
    reminder = _render(await _template("reminder"), reminder_row, safe_settings)
    result.extend(
        [
            ("reminder", "[ТЕСТ · НАПОМИНАНИЕ ЧЕРЕЗ 12 ЧАСОВ]\n\n" + reminder + "\n\nКнопки в чат-боте:\n• Да, курс открылся\n• Нет, нужна помощь"),
            ("yes", "[ТЕСТ · ОТВЕТ ПО КНОПКЕ «ДА»]\n\n" + _render(await _template("yes_reply"), reminder_row, safe_settings)),
            ("no", "[ТЕСТ · ОТВЕТ ПО КНОПКЕ «НЕТ» · ЗАДАЧА В AMOCRM НЕ СОЗДАЁТСЯ]\n\n" + _render(await _template("no_reply"), reminder_row, safe_settings)),
        ]
    )
    return result


async def _backfill_day(day_text: str) -> dict[str, Any]:
    try:
        day = datetime.strptime(day_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(400, "Дата должна быть в формате ГГГГ-ММ-ДД") from exc
    start = datetime.combine(day, datetime.min.time(), tzinfo=MOSCOW).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    fields = _module("getcourse-chat-fields", "service_paid_course_orders")
    settings = await _settings()
    cursor, matched, pages = 0, 0, 0
    order_keys: list[str] = []
    while pages < 100:
        data = await fields.service_paid_course_orders(after_source_record_id=cursor, after_updated_at="", limit=1000)
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or "Источник GetCourse недоступен")
        pages += 1
        for item in data.get("items") or []:
            paid = _parse_dt(item.get("paid_at"))
            if paid and start <= paid.astimezone(timezone.utc) < end:
                await _upsert_order(item, int(settings.get("reminder_hours") or 12), initial_status="backfill_only")
                matched += 1
                order_keys.append(f"{_clean(item.get('order_id'), 100)}:{_clean(item.get('course_key'), 50)}")
        next_cursor = int(data.get("cursor") or cursor)
        maximum = int(data.get("max_source_record_id") or next_cursor)
        if next_cursor <= cursor or next_cursor >= maximum:
            break
        cursor = next_cursor
    return {"ok": True, "date": day_text, "matched": matched, "stored": len(set(order_keys)), "status": "backfill_only"}


def _vk_test_payload(request_id: str, answer: str, command: str = "onboarding_test_response") -> dict[str, str]:
    return {"command": command, "request_id": request_id, "answer": answer}


def _vk_test_keyboard(request_id: str, command: str = "onboarding_test_response") -> str:
    return json.dumps(
        {
            "inline": True,
            "buttons": [
                [{"action": {"type": "callback", "label": "Да, курс открылся", "payload": _vk_test_payload(request_id, "yes", command)}, "color": "positive"}],
                [{"action": {"type": "callback", "label": "Нет, нужна помощь", "payload": _vk_test_payload(request_id, "no", command)}, "color": "negative"}],
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _telegram_test_keyboard(public_base: str, request_id: str) -> dict[str, Any]:
    base = _clean(public_base, 2000).rstrip("/")
    if not re.match(r"^https://", base, flags=re.IGNORECASE):
        raise RuntimeError("Публичная база Nexus должна начинаться с https://")
    response_base = f"{base}/{MODULE_ID}/api/test/telegram/respond/{quote(request_id, safe='')}"
    return {
        "inline_keyboard": [
            [{"text": "Да, курс открылся", "url": f"{response_base}?choice=yes"}],
            [{"text": "Нет, нужна помощь", "url": f"{response_base}?choice=no"}],
        ]
    }


async def _register_vk_test_callback(settings: dict[str, str]) -> dict[str, str]:
    messenger = _module("messenger-widget", "_vk_request")
    vk_request = messenger._vk_request
    group_id = _clean(messenger._vk_group_id(), 80)
    if not group_id:
        raise RuntimeError("VK-группа messenger-widget не настроена")
    callback_key = _clean(settings.get("vk_test_callback_key"), 200) or secrets.token_urlsafe(24)
    callback_secret = _clean(settings.get("vk_test_callback_secret"), 50)
    if not re.fullmatch(r"[A-Za-z0-9]{1,50}", callback_secret):
        callback_secret = secrets.token_hex(16)
    confirmation = await vk_request("groups.getCallbackConfirmationCode", {"group_id": group_id})
    confirmation_code = _clean((confirmation or {}).get("code") if isinstance(confirmation, dict) else "", 200)
    public_base = _clean(settings.get("public_base"), 2000).rstrip("/")
    if not re.match(r"^https://", public_base, flags=re.IGNORECASE):
        raise RuntimeError("Публичная база Nexus должна начинаться с https://")
    callback_url = f"{public_base}/{MODULE_ID}/api/vk/callback/{callback_key}"
    # VK verifies a new server immediately, so the public confirmation route
    # must know its key, secret and confirmation code before addCallbackServer.
    await _setting_updates(
        {
            "vk_test_callback_key": callback_key,
            "vk_test_callback_secret": callback_secret,
            "vk_test_confirmation_code": confirmation_code,
        }
    )
    server_id = _clean(settings.get("vk_test_callback_server_id"), 80)
    if not server_id:
        servers = await vk_request("groups.getCallbackServers", {"group_id": group_id})
        items = servers.get("items") if isinstance(servers, dict) and isinstance(servers.get("items"), list) else []
        server_id = next(
            (_clean(item.get("id"), 80) for item in items if isinstance(item, dict) and _clean(item.get("url"), 2000) == callback_url),
            "",
        )
    params = {
        "group_id": group_id, "url": callback_url, "title": "Nexus test",
        "secret_key": callback_secret,
    }
    if server_id:
        await vk_request("groups.editCallbackServer", {**params, "server_id": server_id})
    else:
        created = await vk_request("groups.addCallbackServer", params)
        server_id = _clean((created or {}).get("server_id") if isinstance(created, dict) else created, 80)
    if not server_id:
        raise RuntimeError("VK не вернул ID Callback-сервера")
    await vk_request(
        "groups.setCallbackSettings",
        {"group_id": group_id, "server_id": server_id, "api_version": "5.199", "message_event": 1},
    )
    await _setting_updates({"vk_test_callback_server_id": server_id})
    return {"group_id": group_id, "server_id": server_id}


async def _send_vk_button_test(
    recipient_id: str, request_id: str, settings: dict[str, str], *, live_task: bool = False,
) -> dict[str, Any]:
    callback = await _register_vk_test_callback(settings)
    messenger = _module("messenger-widget", "_vk_request")
    row = {"name": "Тестовый пользователь", "course": "Собака"}
    heading = "[БОЕВОЙ ТЕСТ · ПРОВЕРКА ДОСТУПА И ЗАДАЧИ AMOCRM]" if live_task else "[ТЕСТ · ИНТЕРАКТИВНАЯ ПРОВЕРКА ДОСТУПА]"
    command = "onboarding_live_test_response" if live_task else "onboarding_test_response"
    content = heading + "\n\n" + _render(await _template("reminder"), row, settings)
    message_id = await messenger._vk_request(
        "messages.send",
        {
            "group_id": callback["group_id"], "peer_id": recipient_id,
            "random_id": secrets.randbelow(2_000_000_000) + 1,
            "message": content, "keyboard": _vk_test_keyboard(request_id, command),
        },
    )
    return {"ok": True, "status": "sent", "provider": "vk", "recipient_id": recipient_id, "message_id": message_id}


async def _send_text(chat_id: str, text: str, keyboard: dict[str, Any] | None = None) -> list[str]:
    chunks = _split_message(text)
    message_ids: list[str] = []
    for index, chunk in enumerate(chunks):
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": chunk,
            "link_preview_options": {"is_disabled": True},
        }
        if keyboard and index == len(chunks) - 1:
            payload["reply_markup"] = keyboard
        try:
            result = await _tg_call("sendMessage", payload)
        except TelegramDeliveryError as exc:
            # Once at least one chunk was accepted, even a definitive rejection
            # of a later chunk must be treated as ambiguous for the whole stage.
            # Falling back to VK here would duplicate the already delivered text.
            if message_ids:
                raise TelegramDeliveryError(
                    f"Telegram принял {len(message_ids)} часть(и), затем вернул ошибку: {exc}",
                    definitive=False,
                ) from exc
            raise
        message_ids.append(_clean(result.get("message_id"), 100))
    return message_ids


def _retry_at(attempts: int) -> str:
    delays = (300, 900, 3600, 21600)
    seconds = delays[min(max(0, attempts - 1), len(delays) - 1)]
    return _iso(_now_dt() + timedelta(seconds=seconds))


async def _mark_failure(order_id: int, error: Any) -> None:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT attempts,paid_at FROM orders WHERE id=?", (order_id,))).fetchone()
        current = await (await db.execute("SELECT status FROM orders WHERE id=?", (order_id,))).fetchone()
        if current and current["status"] == "delivery_uncertain":
            return
        attempts = int((row or {"attempts": 0})["attempts"] or 0) + 1
        paid = _parse_dt((row or {"paid_at": ""})["paid_at"]) or _now_dt()
        terminal = _now_dt() >= paid + timedelta(hours=72)
        await db.execute(
            "UPDATE orders SET status=?,error=?,attempts=?,next_attempt_at=?,updated_at=? WHERE id=?",
            (
                "failed" if terminal else "retry",
                _clean(error, 1000),
                attempts,
                "" if terminal else _retry_at(attempts),
                _iso(),
                order_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _delivery_start(order_row_id: int, stage: str) -> tuple[bool, str]:
    operation_id = hashlib.sha256(f"{order_row_id}:{stage}".encode()).hexdigest()[:32]
    db = await _connect()
    try:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute("SELECT status FROM deliveries WHERE order_row_id=? AND stage=?", (order_row_id, stage))
        ).fetchone()
        if row and row["status"] == "sent":
            await db.commit()
            return False, operation_id
        if row and row["status"] in {"sending", "uncertain"}:
            error = "Результат предыдущей отправки не подтверждён; автоматический повтор остановлен"
            await db.execute(
                "UPDATE deliveries SET status='uncertain',error=?,updated_at=? WHERE order_row_id=? AND stage=?",
                (error, _iso(), order_row_id, stage),
            )
            await db.execute(
                "UPDATE orders SET status='delivery_uncertain',error=?,next_attempt_at='',updated_at=? WHERE id=?",
                (error, _iso(), order_row_id),
            )
            await db.commit()
            return False, operation_id
        now = _iso()
        await db.execute(
            """
            INSERT INTO deliveries(order_row_id,stage,operation_id,status,created_at,updated_at)
            VALUES(?,?,?,'sending',?,?)
            ON CONFLICT(order_row_id,stage) DO UPDATE SET status='sending',error='',updated_at=excluded.updated_at
            """,
            (order_row_id, stage, operation_id, now, now),
        )
        await db.commit()
    finally:
        await db.close()
    return True, operation_id


async def _delivery_finish(order_row_id: int, stage: str, message_ids: list[str], error: str = "") -> None:
    db = await _connect()
    try:
        await db.execute(
            "UPDATE deliveries SET status=?,error=?,message_ids_json=?,updated_at=? WHERE order_row_id=? AND stage=?",
            ("failed" if error else "sent", _clean(error, 1000), json.dumps(message_ids), _iso(), order_row_id, stage),
        )
        await db.commit()
    finally:
        await db.close()


async def _delivery_uncertain(order_row_id: int, stage: str, error: Any) -> None:
    detail = "Отправка могла быть принята каналом; автоматический повтор остановлен: " + _clean(error, 700)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE deliveries SET status='uncertain',error=?,updated_at=? WHERE order_row_id=? AND stage=?",
            (_clean(detail, 1000), _iso(), order_row_id, stage),
        )
        await db.execute(
            "UPDATE orders SET status='delivery_uncertain',error=?,next_attempt_at='',updated_at=? WHERE id=?",
            (_clean(detail, 1000), _iso(), order_row_id),
        )
        await db.commit()
    finally:
        await db.close()


async def _raise_if_delivery_uncertain(order_row_id: int) -> None:
    db = await _connect()
    try:
        row = await (await db.execute("SELECT status,error FROM orders WHERE id=?", (order_row_id,))).fetchone()
    finally:
        await db.close()
    if row and row["status"] == "delivery_uncertain":
        raise DeliveryUncertain(_clean(row["error"], 1000) or "Результат отправки не подтверждён")


async def _ensure_order_flow(row: dict[str, Any]) -> dict[str, Any]:
    global _last_flow_result
    if row.get("stream") and (_standard_order(row) or (row.get("vk_link") and row.get("tg_link"))):
        return row
    fields = _module("getcourse-chat-fields", "service_resolve_onboarding_flow")
    result = await fields.service_resolve_onboarding_flow(
        course_key=_clean(row.get("course_key"), 50),
        paid_at=_clean(row.get("paid_at"), 100),
    )
    errors = [
        _clean(item.get("error") if isinstance(item, dict) else item, 500)
        for item in (result.get("errors") or [])
    ]
    _last_flow_result = {
        "ok": bool(result.get("ok")),
        "status": "stale_fallback" if result.get("ok") and result.get("stale") else "ready" if result.get("ok") else result.get("status") or "unavailable",
        "source": _clean(result.get("source"), 100),
        "stale": bool(result.get("stale")),
        "items": 1 if result.get("ok") else 0,
        "errors": [item for item in errors if item][:8],
        "checked_at": _iso(),
    }
    flow = result.get("flow") if isinstance(result.get("flow"), dict) else {}
    if not result.get("ok") or not flow.get("stream"):
        return row
    refreshed = {
        **row,
        "stream": _clean(flow.get("stream"), 50),
        "vk_link": _clean(flow.get("vk_link"), 2000),
        "tg_link": _clean(flow.get("tg_link"), 2000),
    }
    db = await _connect()
    try:
        await db.execute(
            """UPDATE orders SET stream=?,vk_link=?,tg_link=?,status=CASE WHEN status='waiting_flow' THEN 'pending' ELSE status END,
               error=CASE WHEN status='waiting_flow' THEN '' ELSE error END,updated_at=? WHERE id=?""",
            (refreshed["stream"], refreshed["vk_link"], refreshed["tg_link"], _iso(), row["id"]),
        )
        await db.commit()
    finally:
        await db.close()
    return refreshed


async def _resolve_targets(row: dict[str, Any]) -> dict[str, Any]:
    messenger = _module("messenger-widget", "service_resolve_onboarding_targets")
    result = await messenger.service_resolve_onboarding_targets(
        utm_term=_clean(row.get("utm_term"), 1000),
        email=_clean(row.get("email"), 300),
        gc_user_id=_clean(row.get("gc_user_id"), 100),
        phone=_clean(row.get("phone"), 100),
        name=_clean(row.get("name"), 300),
    )
    candidates = [dict(item) for item in (result.get("candidates") or []) if isinstance(item, dict)]
    stored_provider = _clean(row.get("target_source"), 40)
    stored_id = _clean(row.get("target_platform_id"), 200)
    manual_targets = []
    manual_vk_id = _clean(row.get("manual_vk_platform_id"), 200)
    manual_telegram_id = _clean(row.get("manual_telegram_platform_id"), 200)
    if manual_vk_id:
        manual_targets.append({"provider": "vk", "recipient_id": manual_vk_id, "source": "manual_platform_id"})
    if manual_telegram_id:
        manual_targets.append({"provider": "telegram", "recipient_id": manual_telegram_id, "source": "manual_salebot_id"})
    candidates = manual_targets + candidates
    if stored_provider in {"telegram", "vk"} and stored_id:
        stored = {"provider": stored_provider, "recipient_id": stored_id, "source": "stored"}
        candidates = [stored] + [
            item for item in candidates
            if not (item.get("provider") == stored_provider and _clean(item.get("recipient_id"), 200) == stored_id)
        ]
    result["candidates"] = candidates
    result["ok"] = bool(candidates)
    return result


async def _defer_order(row: dict[str, Any], status: str, error: str) -> None:
    attempts = int(row.get("attempts") or 0) + 1
    db = await _connect()
    try:
        await db.execute(
            "UPDATE orders SET status=?,error=?,attempts=?,next_attempt_at=?,updated_at=? WHERE id=?",
            (status, _clean(error, 1000), attempts, _retry_at(attempts), _iso(), row["id"]),
        )
        await db.commit()
    finally:
        await db.close()


async def _send_order_text(
    row: dict[str, Any], text: str, stage: str, keyboard: dict[str, Any] | str | None = None,
) -> list[str]:
    provider = "vk" if _clean(row.get("target_source"), 100) == "vk" else "telegram"
    recipient_id = _clean(row.get("target_platform_id"), 200)
    if provider == "telegram":
        return await _send_text(recipient_id, text, keyboard if isinstance(keyboard, dict) else None)
    sender = _module("messenger-widget", "service_send_transfer_message")
    chunks = _split_message(text)
    message_ids: list[str] = []
    for index, chunk in enumerate(chunks, start=1):
        result = await sender.service_send_transfer_message(
            provider="vk",
            recipient_id=recipient_id,
            content=chunk,
            operation_id=hashlib.sha256(f"{row['id']}:{stage}:{index}".encode()).hexdigest()[:32],
            keyboard=keyboard if index == len(chunks) else None,
        )
        if not result.get("ok"):
            raise RuntimeError(result.get("error") or "VK не принял сообщение")
        message_ids.append(_clean(result.get("message_id"), 100))
    return message_ids


async def _send_stage_with_fallback(
    row: dict[str, Any], text: str, stage: str, settings: dict[str, str], *, reminder: bool = False,
) -> tuple[list[str], dict[str, str]]:
    resolved = await _resolve_targets(row)
    candidates = resolved.get("candidates") or []
    if not candidates:
        raise LookupError(resolved.get("error") or "Получатель не найден ни в Telegram, ни во ВКонтакте")

    started = False
    errors: list[str] = []
    for candidate in candidates:
        provider = _clean(candidate.get("provider"), 40)
        recipient_id = _clean(candidate.get("recipient_id"), 200)
        if provider not in {"telegram", "vk"} or not recipient_id:
            continue
        keyboard: dict[str, Any] | str | None = None
        if provider == "telegram":
            guard = await _webhook_guard()
            if not guard.get("ok"):
                errors.append("Telegram временно недоступен: " + _clean(guard.get("error") or guard.get("status"), 500))
                continue
            try:
                await _tg_call("getChat", {"chat_id": recipient_id})
            except TelegramDeliveryError as exc:
                if exc.definitive:
                    errors.append(str(exc))
                    continue
                errors.append(str(exc))
                continue
            except Exception as exc:
                errors.append("Telegram preflight недоступен: " + _clean(exc, 500))
                continue
            if reminder:
                keyboard = _reminder_keyboard()
        elif reminder:
            try:
                await _register_vk_test_callback(settings)
                keyboard = _vk_test_keyboard(str(row["id"]), "onboarding_order_response")
            except Exception as exc:
                errors.append("VK callback недоступен: " + _clean(exc, 500))
                continue

        if not started:
            started, _operation_id = await _delivery_start(int(row["id"]), stage)
            if not started:
                await _raise_if_delivery_uncertain(int(row["id"]))
                return [], {"provider": provider, "recipient_id": recipient_id}
        delivery_row = {**row, "target_source": provider, "target_platform_id": recipient_id}
        try:
            message_ids = await _send_order_text(delivery_row, text, stage, keyboard)
            await _delivery_finish(int(row["id"]), stage, message_ids)
            return message_ids, {"provider": provider, "recipient_id": recipient_id}
        except TelegramDeliveryError as exc:
            if provider == "telegram" and exc.definitive:
                errors.append(str(exc))
                continue
            await _delivery_uncertain(int(row["id"]), stage, exc)
            raise DeliveryUncertain(str(exc)) from exc
        except Exception as exc:
            if provider == "telegram":
                await _delivery_uncertain(int(row["id"]), stage, exc)
                raise DeliveryUncertain(str(exc)) from exc
            errors.append(_clean(exc, 1000))
            break

    detail = "; ".join(item for item in errors if item) or "Нет доступного канала доставки"
    if started:
        await _delivery_finish(int(row["id"]), stage, [], detail)
    raise RuntimeError(detail)


async def _send_welcome(row: dict[str, Any], settings: dict[str, str]) -> None:
    row = await _ensure_order_flow(row)
    template_key = _template_key_for_order(row)
    needs_upgrade_url = template_key in {"standard", "premium"}
    upgrade_url = _standard_upgrade_url(row, settings) if template_key == "standard" else (
        settings.get("premium_upgrade_url") or settings.get("upgrade_url", "")
    )
    if not settings.get("video_instruction_url") or (needs_upgrade_url and not upgrade_url):
        await _defer_order(row, "waiting_config", "Не заполнены ссылки тарифа")
        return
    if not row.get("stream") or (not _standard_order(row) and not row.get("vk_link")) or (not _standard_order(row) and not row.get("tg_link")):
        await _defer_order(row, "waiting_flow", "Актуальная пара ссылок потока не найдена")
        return
    body = _render(await _template(template_key), row, settings)
    try:
        message_ids, target = await _send_stage_with_fallback(row, body, "welcome", settings)
    except LookupError as exc:
        await _defer_order(row, "waiting_identity", str(exc))
        return
    now = _now_dt()
    paid = _parse_dt(row.get("paid_at")) or now
    reminder_due = (
        max(paid + timedelta(hours=int(settings.get("reminder_hours") or 12)), now + timedelta(minutes=15))
        if _reminder_enabled(row)
        else None
    )
    db = await _connect()
    try:
        await db.execute(
            """UPDATE orders SET target_platform_id=?,target_source=?,status='welcomed',error='',attempts=0,
               next_attempt_at='',welcome_sent_at=?,reminder_due_at=?,updated_at=? WHERE id=?""",
            (
                target["recipient_id"], target["provider"], _iso(now),
                _iso(reminder_due) if reminder_due else "", _iso(now), row["id"],
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _new_response_token(order_row_id: int) -> str:
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    now = _now_dt()
    db = await _connect()
    try:
        await db.execute(
            """INSERT INTO response_tokens(order_row_id,token_hash,expires_at,created_at) VALUES(?,?,?,?)
               ON CONFLICT(order_row_id) DO UPDATE SET token_hash=excluded.token_hash,expires_at=excluded.expires_at,created_at=excluded.created_at""",
            (order_row_id, digest, _iso(now + timedelta(days=RESPONSE_DAYS)), _iso(now)),
        )
        await db.commit()
    finally:
        await db.close()
    return token


def _reminder_keyboard() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "Да, курс открылся", "callback_data": "onboarding_access_yes"}],
            [{"text": "Нет, нужна помощь", "callback_data": "onboarding_access_help"}],
        ]
    }


async def _send_reminder(row: dict[str, Any], settings: dict[str, str]) -> None:
    body = _render(await _template("reminder"), row, settings)
    try:
        _message_ids, target = await _send_stage_with_fallback(
            row, body, "reminder", settings, reminder=True,
        )
    except LookupError as exc:
        await _defer_order(row, "waiting_identity", str(exc))
        return
    db = await _connect()
    try:
        await db.execute(
            """UPDATE orders SET target_platform_id=?,target_source=?,status='awaiting_response',error='',
               attempts=0,next_attempt_at='',reminder_sent_at=?,updated_at=? WHERE id=?""",
            (target["recipient_id"], target["provider"], _iso(), _iso(), row["id"]),
        )
        await db.commit()
    finally:
        await db.close()


async def _process_help(row: dict[str, Any], settings: dict[str, str]) -> None:
    amo = _module("getcourse-amocrm", "service_create_onboarding_support_task")
    task_text = (
        f"Нужна помощь с доступом GetCourse: {row.get('name') or 'клиент'}, "
        f"курс {row.get('course')}, заказ {row.get('order_id')}"
    )
    task = await amo.service_create_onboarding_support_task(
        source_record_id=int(row.get("source_record_id") or 0),
        order_id=_clean(row.get("order_id"), 100),
        text=task_text,
        due_minutes=int(settings.get("support_due_minutes") or 60),
    )
    if not task.get("ok"):
        raise RuntimeError(task.get("error") or "Не удалось создать задачу amoCRM")
    started, _ = await _delivery_start(int(row["id"]), "no_reply")
    if started:
        try:
            body = _render(await _template("no_reply"), row, settings)
            ids = await _send_order_text(row, body, "no_reply")
            await _delivery_finish(int(row["id"]), "no_reply", ids)
        except Exception as exc:
            if _clean(row.get("target_source"), 100) == "telegram":
                await _delivery_uncertain(int(row["id"]), "no_reply", exc)
                raise DeliveryUncertain(str(exc)) from exc
            await _delivery_finish(int(row["id"]), "no_reply", [], _clean(exc, 1000))
            raise
    else:
        await _raise_if_delivery_uncertain(int(row["id"]))
    db = await _connect()
    try:
        await db.execute(
            "UPDATE orders SET status='help_requested',error='',amo_lead_id=?,amo_task_id=?,next_attempt_at='',updated_at=? WHERE id=?",
            (_clean(task.get("lead_id"), 64), _clean(task.get("task_id"), 64), _iso(), row["id"]),
        )
        await db.commit()
    finally:
        await db.close()


async def _process_yes(row: dict[str, Any], settings: dict[str, str]) -> None:
    started, _ = await _delivery_start(int(row["id"]), "yes_reply")
    if started:
        try:
            body = _render(await _template("yes_reply"), row, settings)
            ids = await _send_order_text(row, body, "yes_reply")
            await _delivery_finish(int(row["id"]), "yes_reply", ids)
        except Exception as exc:
            if _clean(row.get("target_source"), 100) == "telegram":
                await _delivery_uncertain(int(row["id"]), "yes_reply", exc)
                raise DeliveryUncertain(str(exc)) from exc
            await _delivery_finish(int(row["id"]), "yes_reply", [], _clean(exc, 1000))
            raise
    else:
        await _raise_if_delivery_uncertain(int(row["id"]))
    db = await _connect()
    try:
        await db.execute("UPDATE orders SET status='confirmed',error='',next_attempt_at='',updated_at=? WHERE id=?", (_iso(), row["id"]))
        await db.commit()
    finally:
        await db.close()


async def _due_rows() -> list[dict[str, Any]]:
    now = _iso()
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                """
                SELECT * FROM orders
                WHERE (next_attempt_at='' OR next_attempt_at<=?)
                  AND (
                    status='response_pending'
                    OR (
                      welcome_sent_at=''
                      AND status IN ('pending','retry','waiting_identity','waiting_flow','waiting_config')
                      AND welcome_due_at<=?
                    )
                    OR (
                      branch LIKE 'autopay_%'
                      AND welcome_sent_at<>'' AND reminder_sent_at=''
                      AND status IN ('welcomed','retry','waiting_identity')
                      AND reminder_due_at<>'' AND reminder_due_at<=?
                    )
                  )
                ORDER BY
                  CASE WHEN status='response_pending' THEN 0 WHEN welcome_sent_at='' THEN 1 ELSE 2 END,
                  COALESCE(NULLIF(next_attempt_at,''),paid_at),id
                LIMIT 200
                """,
                (now, now, now),
            )
        ).fetchall()
    finally:
        await db.close()
    return [dict(row) for row in rows]


async def _process_due() -> int:
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "live":
        return 0
    now = _now_dt()
    semaphore = asyncio.Semaphore(DELIVERY_CONCURRENCY)

    async def process(row: dict[str, Any]) -> int:
        async with semaphore:
            try:
                if row.get("response") == "no" and row.get("status") == "response_pending":
                    await _process_help(row, settings)
                elif row.get("response") == "yes" and row.get("status") == "response_pending":
                    await _process_yes(row, settings)
                elif not row.get("welcome_sent_at") and (_parse_dt(row.get("welcome_due_at")) or now) <= now:
                    await _send_welcome(row, settings)
                elif (
                    _reminder_enabled(row)
                    and row.get("welcome_sent_at")
                    and not row.get("reminder_sent_at")
                    and (_parse_dt(row.get("reminder_due_at")) or now) <= now
                ):
                    await _send_reminder(row, settings)
                else:
                    return 0
                return 1
            except DeliveryUncertain as exc:
                if _logger:
                    _logger.error(
                        "onboarding order=%s delivery result is uncertain: %s", row.get("order_id"), exc
                    )
                return 0
            except Exception as exc:
                if _logger:
                    _logger.warning("onboarding order=%s failed: %s", row.get("order_id"), exc)
                await _mark_failure(int(row["id"]), exc)
                return 0

    return sum(await asyncio.gather(*(process(row) for row in await _due_rows())))


async def _sync_worker_loop() -> None:
    await asyncio.sleep(1)
    while True:
        started = _now_dt()
        _worker_state["sync_started_at"] = _iso(started)
        try:
            settings = await _settings()
            if settings.get("enabled") == "1":
                stored = await _sync_source()
                await _classify_pending_orders()
                _worker_state.update({
                    "sync_success_at": _iso(), "sync_error": "", "source_stored": stored,
                    "sync_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
                })
                await _setting_updates({"last_sync_success_at": _iso(), "last_sync_error": ""})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _worker_state.update({
                "sync_error": _clean(exc, 1000),
                "sync_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
            try:
                await _setting_updates({"last_sync_error": _clean(exc, 1000)})
            except Exception:
                pass
            if _logger:
                _logger.exception("getcourse onboarding sync worker failed: %s", exc)
        await asyncio.sleep(WORKER_SECONDS)


async def _maybe_probe_upgrade_browser(settings: dict[str, str]) -> None:
    if settings.get("upgrade_browser_enabled") != "1":
        return
    last = _parse_dt(_worker_state.get("upgrade_browser_checked_at"))
    hours = max(1, min(24, int(settings.get("upgrade_browser_probe_hours") or 6)))
    if last and _now_dt() - last < timedelta(hours=hours):
        return
    try:
        result = await _run_browser_action({"action": "probe", "operation_id": "scheduled-probe"}, timeout=90)
    except Exception as exc:
        result = {"ok": False, "error": _clean(exc, 1000)}
        _worker_state["upgrade_browser_checked_at"] = _iso()
        _worker_state["upgrade_browser_error"] = _clean(exc, 1000)
    if not result.get("ok"):
        await _send_upgrade_alert(
            None, "browser_health",
            "❌ Nexus потерял доступ к GetCourse\n"
            f"{_clean(result.get('error'), 1000) or 'Браузерная проверка не пройдена'}\n"
            "Новые доплаты остановлены до восстановления сессии; платежи не изменяются.",
            repeat_hours=hours,
        )


async def _upgrade_worker_loop() -> None:
    await asyncio.sleep(4)
    while True:
        started = _now_dt()
        try:
            settings = await _settings()
            await _maybe_probe_upgrade_browser(settings)
            stored = await _sync_upgrades() if settings.get("upgrade_enabled") == "1" else 0
            processed = await _process_due_upgrades()
            _worker_state.update({
                "upgrade_sync_at": _iso(), "upgrade_success_at": _iso(), "upgrade_error": "",
                "upgrade_source_stored": stored, "upgrade_processed": processed,
                "upgrade_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _worker_state.update({
                "upgrade_error": _clean(exc, 1000),
                "upgrade_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
            if _logger:
                _logger.exception("getcourse onboarding upgrade worker failed: %s", exc)
        await asyncio.sleep(UPGRADE_WORKER_SECONDS)


async def _email_worker_loop() -> None:
    await asyncio.sleep(7)
    while True:
        started = _now_dt()
        try:
            discovered = await _discover_email_packages()
            processed = await _process_due_email_packages()
            _worker_state.update({
                "email_success_at": _iso(), "email_error": "",
                "email_discovered": discovered, "email_processed": processed,
                "email_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _worker_state.update({
                "email_error": _clean(exc, 1000),
                "email_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
            await _send_upgrade_alert(
                None, "email_worker",
                "❌ Ошибка email-автоматизации Nexus\n"
                f"{_clean(exc, 1000)}\nНовые письма остановлены до следующей безопасной проверки.",
            )
            if _logger:
                _logger.exception("getcourse onboarding email worker failed: %s", exc)
        await asyncio.sleep(EMAIL_WORKER_SECONDS)


async def _delivery_worker_loop() -> None:
    await asyncio.sleep(2)
    while True:
        started = _now_dt()
        _worker_state["delivery_started_at"] = _iso(started)
        try:
            settings = await _settings()
            processed = await _process_due() if settings.get("enabled") == "1" else 0
            _worker_state.update({
                "delivery_success_at": _iso(), "delivery_error": "", "delivery_processed": processed,
                "delivery_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
            await _setting_updates({"last_delivery_success_at": _iso(), "last_delivery_error": ""})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _worker_state.update({
                "delivery_error": _clean(exc, 1000),
                "delivery_duration_ms": int((_now_dt() - started).total_seconds() * 1000),
            })
            try:
                await _setting_updates({"last_delivery_error": _clean(exc, 1000)})
            except Exception:
                pass
            if _logger:
                _logger.exception("getcourse onboarding delivery worker failed: %s", exc)
        await asyncio.sleep(DELIVERY_WORKER_SECONDS)


async def _cleanup_db() -> None:
    cutoff = _iso(_now_dt() - timedelta(days=EVENT_RETENTION_DAYS))
    stale_claim = _iso(_now_dt() - timedelta(minutes=15))
    now = _iso()
    db = await _connect()
    try:
        # A process may stop after atomically claiming a SaleBot action but
        # before persisting the amoCRM result.  Release only stale claims; both
        # downstream operations are idempotent, so the next click/retry is safe.
        await db.execute(
            """UPDATE orders SET status=CASE WHEN reminder_sent_at<>'' THEN 'awaiting_response' ELSE 'welcomed' END,
               response='',error='Предыдущая обработка ответа прервалась; разрешён безопасный повтор',updated_at=?
               WHERE status IN ('help_processing','confirmed_processing') AND updated_at<?""",
            (now, stale_claim),
        )
        await db.execute("DELETE FROM response_tokens WHERE expires_at<?", (now,))
        await db.execute("DELETE FROM interaction_events WHERE updated_at<?", (cutoff,))
        await db.execute("DELETE FROM test_runs WHERE updated_at<?", (cutoff,))
        await db.execute("DELETE FROM upgrade_alerts WHERE updated_at<?", (cutoff,))
        await db.commit()
        await db.execute("PRAGMA wal_checkpoint(PASSIVE)")
    finally:
        await db.close()
    artifact_cutoff = (_now_dt() - timedelta(days=30)).timestamp()
    for directory in (_browser_artifacts_dir(), _browser_root() / "jobs"):
        if not directory.exists():
            continue
        for item in list(directory.iterdir())[:2000]:
            try:
                if item.is_file() and item.stat().st_mtime < artifact_cutoff:
                    item.unlink()
            except OSError:
                continue


async def _maintenance_loop() -> None:
    await asyncio.sleep(60)
    while True:
        try:
            await _cleanup_db()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.warning("getcourse onboarding maintenance failed: %s", exc)
        await asyncio.sleep(MAINTENANCE_SECONDS)


async def _module_supervisor() -> None:
    while True:
        try:
            await _init_db()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _logger:
                _logger.exception("getcourse onboarding initialization failed; retrying: %s", exc)
            await asyncio.sleep(5)
    await asyncio.gather(
        _sync_worker_loop(), _delivery_worker_loop(), _upgrade_worker_loop(), _email_worker_loop(), _maintenance_loop(),
    )


async def _worker_loop() -> None:
    """Backward-compatible entry point for focused tests and older runtimes."""
    await _module_supervisor()


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    if _init_error:
        return {
            "ok": False, "status": "error", "module": MODULE_ID, "initialized": False,
            "error": _init_error, "worker": dict(_worker_state), "counts": {}, "actionable_due": 0,
        }
    settings = await _settings()
    # Health must reflect the actual Telegram/Senler binding even before the
    # first delivery.  The guard only calls getMe/getWebhookInfo; it never
    # creates, deletes or changes the bot webhook.
    guard = await _webhook_guard()
    now = _now_dt()

    def age(value: Any) -> int | None:
        parsed = _parse_dt(value)
        return max(0, int((now - parsed).total_seconds())) if parsed else None

    db = await _connect()
    try:
        counts = {row["status"]: int(row["total"]) for row in await (await db.execute("SELECT status,COUNT(*) total FROM orders GROUP BY status")).fetchall()}
        upgrade_counts = {
            row["status"]: int(row["total"])
            for row in await (
                await db.execute("SELECT status,COUNT(*) total FROM upgrade_jobs GROUP BY status")
            ).fetchall()
        }
        email_counts = {
            row["status"]: int(row["total"])
            for row in await (
                await db.execute("SELECT status,COUNT(*) total FROM email_packages GROUP BY status")
            ).fetchall()
        }
        template_rows = await (await db.execute("SELECT key,body FROM templates")).fetchall()
        template_errors = {
            row["key"]: error
            for row in template_rows
            if (error := _template_validation_error(row["key"], _template_body(row["key"], row["body"])))
        }
        actionable_due = int((await (await db.execute(
            """SELECT COUNT(*) FROM orders
               WHERE (next_attempt_at='' OR next_attempt_at<=?) AND (
                 status='response_pending'
                 OR (welcome_sent_at='' AND status IN ('pending','retry','waiting_identity','waiting_flow','waiting_config') AND welcome_due_at<=?)
                 OR (branch LIKE 'autopay_%' AND welcome_sent_at<>'' AND reminder_sent_at=''
                     AND status IN ('welcomed','retry','waiting_identity') AND reminder_due_at<>'' AND reminder_due_at<=?)
               )""",
            (_iso(now), _iso(now), _iso(now)),
        )).fetchone())[0])
    finally:
        await db.close()
    enabled = settings.get("enabled") == "1"
    live = settings.get("delivery_mode", "test") == "live"
    initialized = settings.get("initialized") == "1"
    sync_success = _worker_state.get("sync_success_at") or settings.get("last_sync_success_at")
    delivery_success = _worker_state.get("delivery_success_at") or settings.get("last_delivery_success_at")
    sync_error = _worker_state.get("sync_error") or settings.get("last_sync_error", "")
    delivery_error = _worker_state.get("delivery_error") or settings.get("last_delivery_error", "")
    email_live = settings.get("email_enabled") == "1" and settings.get("email_mode") == "live"
    email_error = _clean(_worker_state.get("email_error"), 1000) if email_live else ""
    sync_age = age(sync_success)
    delivery_age = age(delivery_success)
    sync_fresh = not enabled or (sync_age is not None and sync_age <= WORKER_SECONDS * 3)
    delivery_fresh = not (enabled and live) or (
        delivery_age is not None and delivery_age <= max(60, DELIVERY_WORKER_SECONDS * 4)
    )
    flow_ok = not enabled or bool(_last_flow_result.get("ok"))
    healthy = (
        initialized and not sync_error and not delivery_error and not email_error and sync_fresh
        and delivery_fresh and flow_ok and not template_errors
    )
    if not enabled:
        status = "paused"
    elif not live:
        status = "test"
    elif healthy and guard.get("ok"):
        status = "ready"
    elif healthy:
        status = "degraded"
    else:
        status = "error"
    cursor_id = int(settings.get("cursor_id") or 0)
    source_max_id = int(settings.get("source_max_id") or cursor_id)
    return {
        "ok": healthy,
        "status": status,
        "module": MODULE_ID,
        "enabled": enabled,
        "delivery_mode": settings.get("delivery_mode", "test"),
        "initialized": initialized,
        "token_present": bool(_token()),
        "guard": dict(guard),
        "channels": {
            "telegram": "ready" if guard.get("ok") else "unavailable",
            "vk": "available_on_demand",
        },
        "flow_catalog": dict(_last_flow_result),
        "counts": counts,
        "upgrade": {
            "enabled": settings.get("upgrade_enabled") == "1",
            "mode": settings.get("upgrade_mode", "test"),
            "counts": upgrade_counts,
            "sync_at": _worker_state.get("upgrade_sync_at", ""),
            "success_at": _worker_state.get("upgrade_success_at", ""),
            "error": _worker_state.get("upgrade_error", ""),
            "browser": {
                "enabled": settings.get("upgrade_browser_enabled") == "1",
                "runtime_present": _browser_python().is_file(),
                "session_loaded": _browser_state_path().is_file(),
                "checked_at": _worker_state.get("upgrade_browser_checked_at", ""),
                "error": _worker_state.get("upgrade_browser_error", ""),
            },
        },
        "email": {
            "enabled": settings.get("email_enabled") == "1",
            "mode": settings.get("email_mode", "paused"),
            "process_confirmed": settings.get("email_process_confirmed") == "1",
            "counts": email_counts,
            "success_at": _worker_state.get("email_success_at", ""),
            "error": _worker_state.get("email_error", ""),
        },
        "actionable_due": actionable_due,
        "template_errors": template_errors,
        "last_sync_at": settings.get("last_sync_at", ""),
        "cursor": {
            "id": cursor_id,
            "updated_at": settings.get("cursor_updated_at", ""),
            "source_max_id": source_max_id,
            "source_max_updated_at": settings.get("source_max_updated_at", ""),
            "lag_records": max(0, source_max_id - cursor_id),
        },
        "worker": {
            **dict(_worker_state),
            "sync_success_at": sync_success or "",
            "sync_error": sync_error,
            "sync_age_seconds": sync_age,
            "delivery_success_at": delivery_success or "",
            "delivery_error": delivery_error,
            "delivery_age_seconds": delivery_age,
            "sync_fresh": sync_fresh,
            "delivery_fresh": delivery_fresh,
        },
        "error": (
            sync_error or delivery_error or email_error
            or ("Ссылки потоков недоступны" if not flow_ok else "")
            or ("Некорректные шаблоны: " + ", ".join(template_errors) if template_errors else "")
        ),
    }


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    return {
        "settings": {
            key: value for key, value in settings.items()
            if key not in {
                "cursor_id", "cursor_updated_at", "vk_test_callback_key",
                "vk_test_callback_secret", "vk_test_confirmation_code", "salebot_help_secret",
                "email_callback_secret",
            }
        },
        "token_present": bool(_token()),
        "salebot_help_ready": bool(settings.get("salebot_help_secret")),
    }


@router.put("/settings")
async def put_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await request.json()
    settings = await _set_settings(data if isinstance(data, dict) else {})
    if settings.get("enabled") == "1" and settings.get("initialized") != "1":
        await _baseline_source()
        settings = await _settings()
    return {"ok": True, "settings": settings}


@router.post("/backfill")
async def backfill(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "test":
        raise HTTPException(409, "Архивный импорт разрешён только в тестовом режиме")
    data = await request.json()
    day_text = _clean((data if isinstance(data, dict) else {}).get("date"), 20)
    return await _backfill_day(day_text)


@router.post("/test/vk")
async def test_vk(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "test":
        raise HTTPException(409, "Адресная проверка разрешена только в тестовом режиме")
    data = await request.json()
    data = data if isinstance(data, dict) else {}
    reference = _clean(data.get("recipient"), 500)
    request_id = _clean(data.get("request_id"), 100)
    if not reference or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,99}", request_id):
        raise HTTPException(400, "Укажите VK-получателя и уникальный request_id (8–100 символов)")

    db = await _connect()
    try:
        existing = await (await db.execute("SELECT * FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
    finally:
        await db.close()
    if existing:
        saved = dict(existing)
        if saved["status"] == "sent":
            return {"ok": True, "duplicate": True, "request_id": request_id, "recipient_id": saved["recipient_id"], "results": json.loads(saved["results_json"] or "[]")}
        raise HTTPException(409, f"request_id уже использован, статус: {saved['status']}")

    messenger = _module("messenger-widget", "service_resolve_vk_test_target")
    target = await messenger.service_resolve_vk_test_target(reference=reference)
    if not target.get("ok"):
        raise HTTPException(409, target.get("error") or "VK-получатель недоступен для сообщений сообщества")
    recipient_id = _clean(target.get("recipient_id"), 100)
    requested_by = _clean(user.get("username") or user.get("sub") or user.get("id"), 200)
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,requested_by,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, reference, recipient_id, requested_by, "sending", now, now),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise HTTPException(409, "request_id уже используется") from exc
    finally:
        await db.close()

    results: list[dict[str, Any]] = []
    try:
        sender = _module("messenger-widget", "service_send_transfer_message")
        for template_key, content in await _test_preview_messages(settings):
            for chunk_index, chunk in enumerate(_split_message(content), start=1):
                operation_id = f"onboarding-test:{request_id}:{template_key}:{chunk_index}"[:100]
                sent = await sender.service_send_transfer_message(
                    provider="vk", recipient_id=recipient_id, content=chunk, operation_id=operation_id,
                )
                results.append({"template": template_key, "chunk": chunk_index, **sent})
                if not sent.get("ok"):
                    raise RuntimeError(sent.get("error") or f"Не отправлен шаблон {template_key}")
        status, error = "sent", ""
    except Exception as exc:
        status, error = "failed", _clean(exc, 1000)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE test_runs SET status=?,results_json=?,error=?,updated_at=? WHERE request_id=?",
            (status, json.dumps(results, ensure_ascii=False), error, _iso(), request_id),
        )
        await db.commit()
    finally:
        await db.close()
    if status != "sent":
        raise HTTPException(502, error)
    return {"ok": True, "request_id": request_id, "recipient_id": recipient_id, "results": results}


@router.post("/test/vk/buttons")
async def test_vk_buttons(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "test":
        raise HTTPException(409, "Интерактивная проверка разрешена только в тестовом режиме")
    data = await request.json()
    data = data if isinstance(data, dict) else {}
    reference = _clean(data.get("recipient"), 500)
    request_id = _clean(data.get("request_id"), 100)
    if not reference or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,99}", request_id):
        raise HTTPException(400, "Укажите VK-получателя и уникальный request_id (8–100 символов)")
    db = await _connect()
    try:
        existing = await (await db.execute("SELECT status FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
    finally:
        await db.close()
    if existing:
        raise HTTPException(409, f"request_id уже использован, статус: {existing['status']}")
    resolver = _module("messenger-widget", "service_resolve_vk_test_target")
    target = await resolver.service_resolve_vk_test_target(reference=reference)
    if not target.get("ok"):
        raise HTTPException(409, target.get("error") or "VK-получатель недоступен для сообщений сообщества")
    recipient_id = _clean(target.get("recipient_id"), 100)
    requested_by = _clean(user.get("username") or user.get("sub") or user.get("id"), 200)
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,requested_by,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, reference, recipient_id, requested_by, "sending", now, now),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise HTTPException(409, "request_id уже используется") from exc
    finally:
        await db.close()
    try:
        sent = await _send_vk_button_test(recipient_id, request_id, settings)
        status, error = "sent", ""
    except Exception as exc:
        sent, status, error = {"ok": False, "error": _clean(exc, 1000)}, "failed", _clean(exc, 1000)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE test_runs SET status=?,results_json=?,error=?,updated_at=? WHERE request_id=?",
            (status, json.dumps([sent], ensure_ascii=False), error, _iso(), request_id),
        )
        await db.commit()
    finally:
        await db.close()
    if status != "sent":
        raise HTTPException(502, error)
    return {"ok": True, "request_id": request_id, "recipient_id": recipient_id, "result": sent}


@router.post("/test/vk/live-buttons")
async def test_vk_live_buttons(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "test":
        raise HTTPException(409, "Боевой тест разрешён только при остановленной массовой доставке")
    data = await request.json()
    data = data if isinstance(data, dict) else {}
    reference = _clean(data.get("recipient"), 500)
    request_id = _clean(data.get("request_id"), 100)
    lead_id = _clean(data.get("lead_id"), 64)
    if not reference or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,99}", request_id):
        raise HTTPException(400, "Укажите VK-получателя и уникальный request_id (8–100 символов)")
    if not re.fullmatch(r"\d{5,20}", lead_id):
        raise HTTPException(400, "Для боевого теста требуется числовой ID тестовой сделки amoCRM")
    db = await _connect()
    try:
        existing = await (await db.execute("SELECT status FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
    finally:
        await db.close()
    if existing:
        raise HTTPException(409, f"request_id уже использован, статус: {existing['status']}")
    resolver = _module("messenger-widget", "service_resolve_vk_test_target")
    target = await resolver.service_resolve_vk_test_target(reference=reference)
    if not target.get("ok"):
        raise HTTPException(409, target.get("error") or "VK-получатель недоступен для сообщений сообщества")
    recipient_id = _clean(target.get("recipient_id"), 100)
    requested_by = _clean(user.get("username") or user.get("sub") or user.get("id"), 200)
    order_id = f"onboarding-live-test-{request_id}"[:100]
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,requested_by,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, reference, recipient_id, requested_by, "sending", now, now),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise HTTPException(409, "request_id уже используется") from exc
    finally:
        await db.close()
    try:
        sent = await _send_vk_button_test(recipient_id, request_id, settings, live_task=True)
        audit = {**sent, "mode": "live_task", "lead_id": lead_id, "order_id": order_id}
        status, error = "sent", ""
    except Exception as exc:
        audit, status, error = {"ok": False, "mode": "live_task", "lead_id": lead_id, "order_id": order_id, "error": _clean(exc, 1000)}, "failed", _clean(exc, 1000)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE test_runs SET status=?,results_json=?,error=?,updated_at=? WHERE request_id=?",
            (status, json.dumps([audit], ensure_ascii=False), error, _iso(), request_id),
        )
        await db.commit()
    finally:
        await db.close()
    if status != "sent":
        raise HTTPException(502, error)
    return {"ok": True, "request_id": request_id, "recipient_id": recipient_id, "result": audit}


@router.post("/test/telegram/live-buttons")
async def test_telegram_live_buttons(request: Request) -> dict[str, Any]:
    user = await _require_admin(request)
    settings = await _settings()
    if settings.get("delivery_mode", "test") != "test":
        raise HTTPException(409, "Боевой тест разрешён только при остановленной массовой доставке")
    data = await request.json()
    data = data if isinstance(data, dict) else {}
    recipient_id = _clean(data.get("recipient_id"), 100)
    request_id = _clean(data.get("request_id"), 100)
    lead_id = _clean(data.get("lead_id"), 64)
    task_reference = _clean(data.get("task_reference") or request_id, 100)
    if not re.fullmatch(r"\d{5,20}", recipient_id):
        raise HTTPException(400, "Требуется числовой Telegram chat_id")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{31,99}", request_id):
        raise HTTPException(400, "Для кнопок требуется непредсказуемый request_id длиной 32–100 символов")
    if not re.fullmatch(r"\d{5,20}", lead_id):
        raise HTTPException(400, "Для боевого теста требуется числовой ID тестовой сделки amoCRM")
    db = await _connect()
    try:
        existing = await (await db.execute("SELECT status FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
    finally:
        await db.close()
    if existing:
        raise HTTPException(409, f"request_id уже использован, статус: {existing['status']}")

    chat = await _tg_call("getChat", {"chat_id": recipient_id})
    if _clean(chat.get("id"), 100) != recipient_id or _clean(chat.get("type"), 40) != "private":
        raise HTTPException(409, "Telegram-получатель не подтверждён как приватный чат")
    requested_by = _clean(user.get("username") or user.get("sub") or user.get("id"), 200)
    order_id = f"onboarding-live-test-{request_id}"[:100]
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO test_runs(request_id,recipient_ref,recipient_id,requested_by,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, "telegram", recipient_id, requested_by, "sending", now, now),
        )
        await db.commit()
    except aiosqlite.IntegrityError as exc:
        raise HTTPException(409, "request_id уже используется") from exc
    finally:
        await db.close()

    meta = {
        "mode": "telegram_live_task", "lead_id": lead_id, "order_id": order_id,
        "task_reference": task_reference, "chat_username": _clean(chat.get("username"), 200),
    }
    try:
        content = "[БОЕВОЙ ТЕСТ · TELEGRAM · ПРОВЕРКА ДОСТУПА И ЗАДАЧИ AMOCRM]\n\n" + _render(
            await _template("reminder"), {"name": "Тестовый пользователь", "course": "Собака"}, settings
        )
        message_ids = await _send_text(
            recipient_id, content, _telegram_test_keyboard(settings.get("public_base", ""), request_id)
        )
        audit, status, error = {**meta, "ok": True, "provider": "telegram", "message_ids": message_ids}, "sent", ""
    except Exception as exc:
        audit, status, error = {**meta, "ok": False, "error": _clean(exc, 1000)}, "failed", _clean(exc, 1000)
    db = await _connect()
    try:
        await db.execute(
            "UPDATE test_runs SET status=?,results_json=?,error=?,updated_at=? WHERE request_id=?",
            (status, json.dumps([audit], ensure_ascii=False), error, _iso(), request_id),
        )
        await db.commit()
    finally:
        await db.close()
    if status != "sent":
        raise HTTPException(502, error)
    return {"ok": True, "request_id": request_id, "recipient_id": recipient_id, "result": audit}


@router.post("/vk/callback/{key}")
async def vk_test_callback(key: str, request: Request) -> PlainTextResponse:
    settings = await _settings()
    expected_key = _clean(settings.get("vk_test_callback_key"), 200)
    if not expected_key or not secrets.compare_digest(_clean(key, 200), expected_key):
        return PlainTextResponse("not found", status_code=404)
    body = await request.body()
    if len(body) > 64 * 1024:
        return PlainTextResponse("error", status_code=413)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return PlainTextResponse("error", status_code=400)
    messenger = _module("messenger-widget", "_vk_request")
    if _clean(payload.get("group_id"), 80) != _clean(messenger._vk_group_id(), 80):
        return PlainTextResponse("error", status_code=403)
    expected_secret = _clean(settings.get("vk_test_callback_secret"), 100)
    if expected_secret and not secrets.compare_digest(_clean(payload.get("secret"), 100), expected_secret):
        return PlainTextResponse("error", status_code=403)
    if payload.get("type") == "confirmation":
        return PlainTextResponse(_clean(settings.get("vk_test_confirmation_code"), 200))
    if payload.get("type") != "message_event":
        return PlainTextResponse("ok")
    event = payload.get("object") if isinstance(payload.get("object"), dict) else {}
    event_payload = event.get("payload")
    if isinstance(event_payload, str):
        try:
            event_payload = json.loads(event_payload)
        except json.JSONDecodeError:
            event_payload = {}
    event_payload = event_payload if isinstance(event_payload, dict) else {}
    request_id = _clean(event_payload.get("request_id"), 100)
    answer = _clean(event_payload.get("answer"), 20)
    user_id = _clean(event.get("user_id"), 100)
    peer_id = _clean(event.get("peer_id"), 100)
    event_id = _clean(event.get("event_id"), 300)
    command = _clean(event_payload.get("command"), 80)
    if command not in {"onboarding_test_response", "onboarding_live_test_response", "onboarding_order_response"} or answer not in {"yes", "no"}:
        return PlainTextResponse("ok")
    if command == "onboarding_order_response":
        try:
            order_row_id = int(request_id)
        except (TypeError, ValueError):
            return PlainTextResponse("ok")
        db = await _connect()
        claimed = False
        try:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """UPDATE orders SET response=?,responded_at=?,status='response_pending',error='',next_attempt_at='',updated_at=?
                   WHERE id=? AND target_source='vk' AND target_platform_id=? AND response='' AND status='awaiting_response'""",
                (answer, _iso(), _iso(), order_row_id, user_id),
            )
            claimed = cursor.rowcount == 1
            await db.commit()
        finally:
            await db.close()
        snackbar = "Ответ принят" if claimed else "Этот ответ уже обработан"
        if event_id and user_id and peer_id:
            try:
                await messenger._vk_request(
                    "messages.sendMessageEventAnswer",
                    {
                        "event_id": event_id,
                        "user_id": user_id,
                        "peer_id": peer_id,
                        "event_data": json.dumps({"type": "show_snackbar", "text": snackbar}, ensure_ascii=False),
                    },
                )
            except Exception as exc:
                if _logger:
                    _logger.warning("VK onboarding event answer failed order=%s: %s", order_row_id, exc)
        return PlainTextResponse("ok")
    live_task = command == "onboarding_live_test_response"

    db = await _connect()
    claimed = False
    previous_status = ""
    run_results: list[dict[str, Any]] = []
    try:
        await db.execute("BEGIN IMMEDIATE")
        row = await (
            await db.execute("SELECT recipient_id,status,results_json FROM test_runs WHERE request_id=?", (request_id,))
        ).fetchone()
        previous_status = _clean(row["status"], 40) if row else ""
        if row:
            try:
                run_results = [item for item in json.loads(row["results_json"] or "[]") if isinstance(item, dict)]
            except (TypeError, json.JSONDecodeError):
                run_results = []
        recorded_mode = _clean((run_results[0] if run_results else {}).get("mode"), 40)
        mode_matches = (live_task and recorded_mode == "live_task") or (not live_task and recorded_mode != "live_task")
        if row and mode_matches and _clean(row["recipient_id"], 100) == user_id and previous_status == "sent":
            cursor = await db.execute(
                "UPDATE test_runs SET status=?,updated_at=? WHERE request_id=? AND status='sent'",
                (f"responding_{answer}", _iso(), request_id),
            )
            claimed = cursor.rowcount == 1
        await db.commit()
    finally:
        await db.close()

    snackbar = "Ответ принят" if claimed else "Этот тест уже обработан"
    if event_id and user_id and peer_id:
        try:
            await messenger._vk_request(
                "messages.sendMessageEventAnswer",
                {
                    "event_id": event_id, "user_id": user_id, "peer_id": peer_id,
                    "event_data": json.dumps({"type": "show_snackbar", "text": snackbar}, ensure_ascii=False),
                },
            )
        except Exception as exc:
            if _logger:
                _logger.warning("VK test event answer failed request=%s: %s", request_id, exc)
    if not claimed:
        return PlainTextResponse("ok")
    try:
        task: dict[str, Any] = {}
        if live_task and answer == "no":
            meta = run_results[0] if run_results else {}
            amo = _module("getcourse-amocrm", "service_create_onboarding_support_task")
            task = await amo.service_create_onboarding_support_task(
                order_id=_clean(meta.get("order_id"), 100),
                test_lead_id=_clean(meta.get("lead_id"), 64),
                text=f"БОЕВОЙ ТЕСТ onboarding: нужна помощь с доступом GetCourse ({request_id})",
                due_minutes=int(settings.get("support_due_minutes") or 60),
            )
            if not task.get("ok"):
                raise RuntimeError(task.get("error") or "Не удалось создать тестовую задачу amoCRM")
        template_key = "yes_reply" if answer == "yes" else "no_reply"
        if live_task:
            heading = "[БОЕВОЙ ТЕСТ · НАЖАТА КНОПКА «ДА»]" if answer == "yes" else "[БОЕВОЙ ТЕСТ · НАЖАТА КНОПКА «НЕТ»]"
        else:
            heading = "[ТЕСТ · НАЖАТА КНОПКА «ДА»]" if answer == "yes" else "[ТЕСТ · НАЖАТА КНОПКА «НЕТ» · ЗАДАЧА В AMOCRM НЕ СОЗДАЁТСЯ]"
        reply = heading + "\n\n" + _render(await _template(template_key), {"name": "Тестовый пользователь", "course": "Собака"}, settings)
        if task:
            reply += f"\n\nТестовая задача amoCRM: #{_clean(task.get('task_id'), 64) or 'создана'}"
        message_id = await messenger._vk_request(
            "messages.send",
            {
                "group_id": messenger._vk_group_id(), "peer_id": peer_id,
                "random_id": secrets.randbelow(2_000_000_000) + 1, "message": reply,
            },
        )
        db = await _connect()
        try:
            row = await (await db.execute("SELECT results_json FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
            results = json.loads(row["results_json"] or "[]") if row else []
            results.append({
                "stage": f"button_{answer}", "message_id": message_id,
                "amo_lead_id": _clean(task.get("lead_id"), 64), "amo_task_id": _clean(task.get("task_id"), 64),
                "amo_status": _clean(task.get("status"), 40),
            })
            await db.execute(
                "UPDATE test_runs SET status=?,results_json=?,error='',updated_at=? WHERE request_id=?",
                (f"responded_{answer}", json.dumps(results, ensure_ascii=False), _iso(), request_id),
            )
            await db.commit()
        finally:
            await db.close()
    except Exception as exc:
        db = await _connect()
        try:
            await db.execute(
                "UPDATE test_runs SET status='sent',error=?,updated_at=? WHERE request_id=?",
                (_clean(exc, 1000), _iso(), request_id),
            )
            await db.commit()
        finally:
            await db.close()
        if _logger:
            _logger.exception("VK onboarding test callback failed request=%s", request_id)
    return PlainTextResponse("ok")


@router.post("/telegram/check")
async def telegram_check(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return await _telegram_preflight()


@router.post("/telegram/confirm")
async def telegram_confirm(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    current = await _telegram_preflight()
    if not current.get("ok"):
        raise HTTPException(409, "Telegram-бот или webhook не готовы")
    await _setting_updates(
        {
            "webhook_fingerprint": current["webhook"]["fingerprint"],
            "webhook_host": current["webhook"]["host"],
            "bot_id": current["bot_id"],
            "bot_username": current["bot_username"],
        }
    )
    return await _webhook_guard(force=True)


def _explicit_salebot_id(value: Any) -> str:
    text = unquote_plus(_clean(value, 1000))
    if not text:
        return ""
    normalized = text.replace(":", "=", 1) if re.match(r"^salebot_(?:client_)?id:", text, re.I) else text
    for key, item in parse_qsl(normalized.lstrip("?"), keep_blank_values=False):
        if key.strip().casefold() in {"salebot_id", "salebot_client_id"}:
            current = _clean(item, 100)
            return current if re.fullmatch(r"\d{3,20}", current) else ""
    match = re.fullmatch(r"\s*salebot_(?:client_)?id\s*[=:]\s*(\d{3,20})\s*", text, re.I)
    return match.group(1) if match else ""


async def _salebot_help_payload(request: Request) -> dict[str, Any]:
    result = {str(key): value for key, value in request.query_params.items()}
    if request.method == "GET":
        return result
    body = await request.body()
    if len(body) > 64 * 1024:
        raise HTTPException(413, "Слишком большой запрос")
    if "application/json" in request.headers.get("content-type", "").casefold():
        try:
            parsed = json.loads(body or b"{}")
        except json.JSONDecodeError as exc:
            raise HTTPException(400, "Некорректный JSON") from exc
        if isinstance(parsed, dict):
            result.update(parsed)
    else:
        result.update({
            key: values[-1]
            for key, values in parse_qs(
                body.decode("utf-8", errors="replace"), keep_blank_values=True
            ).items()
        })
    return result


def _safe_interaction_payload(payload: dict[str, Any]) -> dict[str, str]:
    allowed = {
        "order_id", "getcourse_order_id", "salebot_id", "client_id",
        "platform_id", "telegram_id", "chat_id", "callback_data", "command",
    }
    return {key: _clean(value, 500) for key, value in payload.items() if key in allowed and _clean(value, 500)}


async def _interaction_start(source: str, event_type: str, payload: dict[str, Any]) -> int:
    now = _iso()
    safe = _safe_interaction_payload(payload)
    recipient_id = _clean(
        safe.get("platform_id") or safe.get("telegram_id") or safe.get("chat_id")
        or safe.get("salebot_id") or safe.get("client_id"),
        100,
    )
    db = await _connect()
    try:
        cursor = await db.execute(
            """INSERT INTO interaction_events(source,event_type,recipient_id,status,payload_json,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?)""",
            (source, event_type, recipient_id, "received", json.dumps(safe, ensure_ascii=False), now, now),
        )
        await db.commit()
        return int(cursor.lastrowid)
    finally:
        await db.close()


async def _interaction_finish(
    event_id: int, *, status: str, order_row_id: int | None = None,
    choice: str = "", result: dict[str, Any] | None = None, error: str = "",
) -> None:
    safe_result = {
        key: _clean(value, 1000)
        for key, value in (result or {}).items()
        if key in {"order_id", "course", "amo_lead_id", "amo_task_id", "amo_note_id", "status"} and _clean(value, 1000)
    }
    db = await _connect()
    try:
        await db.execute(
            """UPDATE interaction_events SET order_row_id=?,choice=?,status=?,result_json=?,error=?,updated_at=?
               WHERE id=?""",
            (
                order_row_id, _clean(choice, 20), _clean(status, 80),
                json.dumps(safe_result, ensure_ascii=False), _clean(error, 1000), _iso(), event_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def _salebot_help_order(payload: dict[str, Any]) -> dict[str, Any] | None:
    order_id = _clean(payload.get("order_id") or payload.get("getcourse_order_id"), 100)
    salebot_id = _clean(payload.get("salebot_id") or payload.get("client_id"), 100)
    platform_id = _clean(payload.get("platform_id") or payload.get("telegram_id") or payload.get("chat_id"), 100)
    if order_id:
        db = await _connect()
        try:
            row = await (
                await db.execute(
                    "SELECT * FROM orders WHERE order_id=? ORDER BY paid_at DESC,id DESC LIMIT 1", (order_id,)
                )
            ).fetchone()
        finally:
            await db.close()
        return dict(row) if row else None
    if salebot_id:
        if not re.fullmatch(r"\d{3,20}", salebot_id):
            return None
        messenger = _module("messenger-widget", "service_resolve_onboarding_telegram_target")
        target = await messenger.service_resolve_onboarding_telegram_target(utm_term=f"salebot_id={salebot_id}")
        if target.get("ok"):
            platform_id = _clean(target.get("platform_id"), 100)
    if platform_id and re.fullmatch(r"\d{3,20}", platform_id):
        db = await _connect()
        try:
            row = await (
                await db.execute(
                    """SELECT * FROM orders WHERE target_platform_id=? AND branch<>''
                       AND status NOT IN ('backfill_only','classification_needed','skipped')
                       ORDER BY paid_at DESC,id DESC LIMIT 1""",
                    (platform_id,),
                )
            ).fetchone()
            if row:
                return dict(row)
        finally:
            await db.close()
    if salebot_id:
        db = await _connect()
        try:
            candidates = await (
                await db.execute(
                    """SELECT * FROM orders WHERE branch<>'' AND utm_term<>''
                       AND status NOT IN ('backfill_only','classification_needed','skipped')
                       ORDER BY paid_at DESC,id DESC LIMIT 2000"""
                )
            ).fetchall()
        finally:
            await db.close()
        for candidate in candidates:
            item = dict(candidate)
            if _explicit_salebot_id(item.get("utm_term")) == salebot_id:
                return item
    return None


def _salebot_help_result(row: dict[str, Any], status: str, reply_text: str) -> dict[str, Any]:
    return {
        "ok": True,
        "status": status,
        "order_id": _clean(row.get("order_id"), 100),
        "course": _clean(row.get("course"), 100),
        "amo_lead_id": _clean(row.get("amo_lead_id"), 64),
        "amo_task_id": _clean(row.get("amo_task_id"), 64),
        "reply_text": reply_text,
    }


@router.get("/salebot/help-config")
async def salebot_help_config(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    base = _clean(settings.get("public_base"), 2000).rstrip("/")
    secret = _clean(settings.get("salebot_help_secret"), 300)
    return {
        "ok": bool(secret),
        "webhook_url": f"{base}/{MODULE_ID}/api/salebot/help?secret={quote(secret, safe='')}",
        "help_webhook_url": f"{base}/{MODULE_ID}/api/salebot/help?secret={quote(secret, safe='')}",
        "confirm_webhook_url": f"{base}/{MODULE_ID}/api/salebot/confirm?secret={quote(secret, safe='')}",
        "callback_data": "onboarding_access_help",
        "confirm_callback_data": "onboarding_access_yes",
        "accepted_identity": ["order_id", "salebot_id", "client_id", "platform_id"],
        "example": {"client_id": "{client_id}"},
    }


@router.api_route("/salebot/help", methods=["GET", "POST"])
async def salebot_help(request: Request) -> JSONResponse:
    settings = await _settings()
    payload = await _salebot_help_payload(request)
    expected = _clean(settings.get("salebot_help_secret"), 300)
    supplied = _clean(payload.get("secret") or request.headers.get("X-Nexus-Secret"), 300)
    if not expected or not supplied or not secrets.compare_digest(expected, supplied):
        return JSONResponse({"ok": False, "status": "unauthorized", "error": "Неверный secret"}, status_code=401)
    interaction_id = await _interaction_start("salebot", "help_request", payload)
    row = await _salebot_help_order(payload)
    if not row:
        await _interaction_finish(
            interaction_id, status="order_not_found", choice="no",
            error="Оплаченный учебный заказ для клиента не найден",
        )
        return JSONResponse(
            {"ok": False, "status": "order_not_found", "error": "Оплаченный учебный заказ для клиента не найден"},
            status_code=404,
        )
    if row.get("status") in {"backfill_only", "classification_needed", "skipped"} or not row.get("branch"):
        await _interaction_finish(
            interaction_id, status="order_not_eligible", order_row_id=int(row["id"]), choice="no",
            result={"order_id": row.get("order_id"), "course": row.get("course")},
            error="Заказ не участвует в onboarding",
        )
        return JSONResponse(
            {"ok": False, "status": "order_not_eligible", "error": "Заказ не участвует в onboarding"},
            status_code=409,
        )
    reply_text = _render(await _template("no_reply"), row, settings)
    if row.get("status") == "help_requested":
        result = _salebot_help_result(row, "already_requested", reply_text)
        await _interaction_finish(
            interaction_id, status="already_requested", order_row_id=int(row["id"]), choice="no", result=result,
        )
        return JSONResponse(result)

    previous_status, previous_response = _clean(row.get("status"), 40), _clean(row.get("response"), 10)
    db = await _connect()
    try:
        cursor = await db.execute(
            """UPDATE orders SET status='help_processing',response='no',responded_at=?,error='',next_attempt_at='',updated_at=?
               WHERE id=? AND status NOT IN ('help_processing','help_requested')""",
            (_iso(), _iso(), row["id"]),
        )
        await db.commit()
        claimed = cursor.rowcount == 1
    finally:
        await db.close()
    if not claimed:
        db = await _connect()
        try:
            current = await (await db.execute("SELECT * FROM orders WHERE id=?", (row["id"],))).fetchone()
        finally:
            await db.close()
        current_row = dict(current) if current else row
        if current_row.get("status") == "help_requested":
            result = _salebot_help_result(current_row, "already_requested", reply_text)
            await _interaction_finish(
                interaction_id, status="already_requested", order_row_id=int(row["id"]), choice="no", result=result,
            )
            return JSONResponse(result)
        await _interaction_finish(
            interaction_id, status="processing", order_row_id=int(row["id"]), choice="no",
            result={"order_id": row.get("order_id")}, error="Запрос уже обрабатывается",
        )
        return JSONResponse({"ok": False, "status": "processing", "error": "Запрос уже обрабатывается"}, status_code=409)

    try:
        amo = _module("getcourse-amocrm", "service_create_onboarding_support_task")
        task = await amo.service_create_onboarding_support_task(
            source_record_id=int(row.get("source_record_id") or 0),
            order_id=_clean(row.get("order_id"), 100),
            phone=_clean(row.get("phone"), 100),
            email=_clean(row.get("email"), 300),
            utm_term=_clean(row.get("utm_term"), 1000),
            text=(
                f"Нужна помощь с доступом GetCourse: {row.get('name') or 'клиент'}, "
                f"курс {row.get('course')}, заказ {row.get('order_id')}"
            ),
            due_minutes=int(settings.get("support_due_minutes") or 60),
        )
        if not task.get("ok"):
            raise RuntimeError(task.get("error") or "Не удалось создать задачу amoCRM")
        db = await _connect()
        try:
            await db.execute(
                """UPDATE orders SET status='help_requested',error='',amo_lead_id=?,amo_task_id=?,next_attempt_at='',updated_at=?
                   WHERE id=?""",
                (_clean(task.get("lead_id"), 64), _clean(task.get("task_id"), 64), _iso(), row["id"]),
            )
            await db.commit()
        finally:
            await db.close()
    except Exception as exc:
        db = await _connect()
        try:
            await db.execute(
                "UPDATE orders SET status=?,response=?,error=?,updated_at=? WHERE id=? AND status='help_processing'",
                (previous_status, previous_response, _clean(exc, 1000), _iso(), row["id"]),
            )
            await db.commit()
        finally:
            await db.close()
        if _logger:
            _logger.exception("SaleBot onboarding help failed order=%s", row.get("order_id"))
        await _interaction_finish(
            interaction_id, status="task_failed", order_row_id=int(row["id"]), choice="no",
            result={"order_id": row.get("order_id"), "course": row.get("course")}, error=_clean(exc, 1000),
        )
        return JSONResponse({"ok": False, "status": "task_failed", "error": _clean(exc, 1000)}, status_code=502)
    result_row = dict(row)
    result_row.update({"amo_lead_id": task.get("lead_id"), "amo_task_id": task.get("task_id")})
    result = _salebot_help_result(result_row, _clean(task.get("status"), 40) or "created", reply_text)
    await _interaction_finish(
        interaction_id, status=_clean(task.get("status"), 40) or "created",
        order_row_id=int(row["id"]), choice="no", result=result,
    )
    return JSONResponse(result)


@router.api_route("/salebot/confirm", methods=["GET", "POST"])
async def salebot_confirm(request: Request) -> JSONResponse:
    settings = await _settings()
    payload = await _salebot_help_payload(request)
    expected = _clean(settings.get("salebot_help_secret"), 300)
    supplied = _clean(payload.get("secret") or request.headers.get("X-Nexus-Secret"), 300)
    if not expected or not supplied or not secrets.compare_digest(expected, supplied):
        return JSONResponse({"ok": False, "status": "unauthorized", "error": "Неверный secret"}, status_code=401)
    interaction_id = await _interaction_start("salebot", "confirm_request", payload)
    row = await _salebot_help_order(payload)
    if not row:
        await _interaction_finish(
            interaction_id, status="order_not_found", choice="yes",
            error="Оплаченный учебный заказ для клиента не найден",
        )
        return JSONResponse(
            {"ok": False, "status": "order_not_found", "error": "Оплаченный учебный заказ для клиента не найден"},
            status_code=404,
        )
    if row.get("status") in {"backfill_only", "classification_needed", "skipped"} or not row.get("branch"):
        await _interaction_finish(
            interaction_id, status="order_not_eligible", order_row_id=int(row["id"]), choice="yes",
            result={"order_id": row.get("order_id"), "course": row.get("course")},
            error="Заказ не участвует в onboarding",
        )
        return JSONResponse(
            {"ok": False, "status": "order_not_eligible", "error": "Заказ не участвует в onboarding"},
            status_code=409,
        )
    reply_text = _render(await _template("yes_reply"), row, settings)
    if row.get("status") == "confirmed":
        result = {
            "ok": True, "status": "already_confirmed", "order_id": _clean(row.get("order_id"), 100),
            "course": _clean(row.get("course"), 100), "amo_lead_id": _clean(row.get("amo_lead_id"), 64),
            "amo_note_id": _clean(row.get("amo_note_id"), 64), "reply_text": reply_text,
        }
        await _interaction_finish(
            interaction_id, status="already_confirmed", order_row_id=int(row["id"]), choice="yes", result=result,
        )
        return JSONResponse(result)

    previous_status, previous_response = _clean(row.get("status"), 40), _clean(row.get("response"), 10)
    db = await _connect()
    try:
        cursor = await db.execute(
            """UPDATE orders SET status='confirmed_processing',response='yes',responded_at=?,error='',updated_at=?
               WHERE id=? AND status NOT IN ('confirmed_processing','confirmed','help_processing','help_requested')""",
            (_iso(), _iso(), row["id"]),
        )
        await db.commit()
        claimed = cursor.rowcount == 1
    finally:
        await db.close()
    if not claimed:
        await _interaction_finish(
            interaction_id, status="processing", order_row_id=int(row["id"]), choice="yes",
            result={"order_id": row.get("order_id")}, error="Ответ уже обрабатывается",
        )
        return JSONResponse({"ok": False, "status": "processing", "error": "Ответ уже обрабатывается"}, status_code=409)

    try:
        amo = _module("getcourse-amocrm", "service_add_onboarding_confirmation_note")
        note = await amo.service_add_onboarding_confirmation_note(
            source_record_id=int(row.get("source_record_id") or 0),
            order_id=_clean(row.get("order_id"), 100),
            phone=_clean(row.get("phone"), 100),
            email=_clean(row.get("email"), 300),
            utm_term=_clean(row.get("utm_term"), 1000),
            text="Пользователь подтвердил вход GetCourse",
        )
        if not note.get("ok"):
            raise RuntimeError(note.get("error") or "Не удалось добавить примечание amoCRM")
        db = await _connect()
        try:
            await db.execute(
                """UPDATE orders SET status='confirmed',error='',amo_lead_id=?,amo_note_id=?,next_attempt_at='',updated_at=?
                   WHERE id=? AND status='confirmed_processing'""",
                (_clean(note.get("lead_id"), 64), _clean(note.get("note_id"), 64), _iso(), row["id"]),
            )
            await db.commit()
        finally:
            await db.close()
    except Exception as exc:
        db = await _connect()
        try:
            await db.execute(
                "UPDATE orders SET status=?,response=?,error=?,updated_at=? WHERE id=? AND status='confirmed_processing'",
                (previous_status, previous_response, _clean(exc, 1000), _iso(), row["id"]),
            )
            await db.commit()
        finally:
            await db.close()
        await _interaction_finish(
            interaction_id, status="note_failed", order_row_id=int(row["id"]), choice="yes",
            result={"order_id": row.get("order_id"), "course": row.get("course")}, error=_clean(exc, 1000),
        )
        if _logger:
            _logger.exception("SaleBot onboarding confirmation failed order=%s", row.get("order_id"))
        return JSONResponse({"ok": False, "status": "note_failed", "error": _clean(exc, 1000)}, status_code=502)

    result = {
        "ok": True, "status": _clean(note.get("status"), 40) or "created",
        "order_id": _clean(row.get("order_id"), 100), "course": _clean(row.get("course"), 100),
        "amo_lead_id": _clean(note.get("lead_id"), 64), "amo_note_id": _clean(note.get("note_id"), 64),
        "reply_text": reply_text,
    }
    await _interaction_finish(
        interaction_id, status="confirmed", order_row_id=int(row["id"]), choice="yes", result=result,
    )
    return JSONResponse(result)


@router.get("/templates")
async def templates(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        rows = await (
            await db.execute(
                "SELECT key,title,body,updated_at FROM templates WHERE key IN ({}) ORDER BY key".format(
                    ",".join("?" for _ in DEFAULT_TEMPLATES)
                ),
                tuple(DEFAULT_TEMPLATES),
            )
        ).fetchall()
    finally:
        await db.close()
    return {
        "items": [
            {
                **dict(row),
                "title": DEFAULT_TEMPLATES.get(row["key"], (row["title"], ""))[0],
                "body": _template_body(row["key"], row["body"]),
            }
            for row in rows
        ]
    }


@router.put("/templates/{key}")
async def save_template(key: str, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    if key not in DEFAULT_TEMPLATES:
        raise HTTPException(404, "Шаблон не найден")
    data = await request.json()
    body = _template_body(key, (data if isinstance(data, dict) else {}).get("body"))
    if not body:
        raise HTTPException(400, "Текст пустой")
    validation_error = _template_validation_error(key, body)
    if validation_error:
        raise HTTPException(400, validation_error)
    db = await _connect()
    try:
        await db.execute("UPDATE templates SET body=?,updated_at=? WHERE key=?", (body, _iso(), key))
        await db.commit()
    finally:
        await db.close()
    return {"ok": True}


async def _email_callback_result(secret: str, request: Request) -> JSONResponse:
    settings = await _settings()
    expected = _clean(settings.get("email_callback_secret"), 300)
    if not expected or not secrets.compare_digest(expected, _clean(secret, 300)):
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    operation_id = _clean(request.query_params.get("operation_id"), 100)
    if not operation_id:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        operation_id = _clean(payload.get("operation_id") if isinstance(payload, dict) else "", 100)
    if not operation_id:
        return JSONResponse({"ok": False, "error": "operation_id required"}, status_code=400)
    db = await _connect()
    try:
        row = await (
            await db.execute("SELECT id,status FROM email_packages WHERE operation_id=?", (operation_id,))
        ).fetchone()
        if not row:
            return JSONResponse({"ok": False, "error": "package not found"}, status_code=404)
        if row["status"] == "sent":
            return JSONResponse({"ok": True, "status": "already_confirmed"})
        if row["status"] not in {"triggering", "awaiting_callback"}:
            return JSONResponse({"ok": False, "error": "package is not awaiting callback"}, status_code=409)
        now = _iso()
        await db.execute(
            "UPDATE email_packages SET status='sent',callback_at=?,sent_at=?,error='',next_attempt_at='',updated_at=? "
            "WHERE id=? AND status IN ('triggering','awaiting_callback')",
            (now, now, now, int(row["id"])),
        )
        await db.commit()
    finally:
        await db.close()
    return JSONResponse({"ok": True, "status": "sent"})


@router.get("/email/callback/{secret}")
async def email_callback_get(secret: str, request: Request) -> JSONResponse:
    return await _email_callback_result(secret, request)


@router.post("/email/callback/{secret}")
async def email_callback_post(secret: str, request: Request) -> JSONResponse:
    return await _email_callback_result(secret, request)


@router.get("/emails")
async def emails(request: Request, status: str = "", search: str = "", limit: int = 300) -> dict[str, Any]:
    await _require_admin(request)
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status=?")
        params.append(_clean(status, 40))
    if search:
        needle = f"%{_clean(search, 200)}%"
        clauses.append("(gc_user_id LIKE ? OR email LIKE ? OR name LIKE ? OR source_order_id LIKE ?)")
        params.extend([needle] * 4)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(500, int(limit))))
    db = await _connect()
    try:
        rows = await (
            await db.execute(f"SELECT * FROM email_packages{where} ORDER BY id DESC LIMIT ?", tuple(params))
        ).fetchall()
    finally:
        await db.close()
    return {"items": [dict(row) for row in rows]}


@router.get("/emails/config")
async def email_config(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    settings = await _settings()
    return {
        "ok": True,
        "mode": settings.get("email_mode", "paused"),
        "enabled": settings.get("email_enabled") == "1",
        "process_confirmed": settings.get("email_process_confirmed") == "1",
        "trigger_group_template": settings.get("email_trigger_group_template", ""),
        "trigger_groups": {
            key: _email_trigger_group(key, settings)
            for key in (
                "puppy:standard-start", "puppy:premium-entry",
                "dog:standard-start", "dog:premium-entry",
                "combo:standard-start", "combo:premium-entry",
            )
        },
        "callback_template": "{object.Nexus email callback}",
    }


@router.post("/emails/preview/{gc_user_id}")
async def preview_email_user(gc_user_id: str, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    wanted = _clean(gc_user_id, 100)
    if not wanted.isdigit():
        raise HTTPException(400, "Нужен числовой GetCourse user ID")
    now = _iso()
    db = await _connect()
    try:
        await db.execute(
            "INSERT INTO email_recipient_holds(gc_user_id,reason,created_at,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(gc_user_id) DO UPDATE SET reason=excluded.reason,updated_at=excluded.updated_at",
            (wanted, "Ожидает отдельной команды владельца", now, now),
        )
        await db.commit()
    finally:
        await db.close()
    stored = await _discover_email_packages(gc_user_id=wanted, force_hold=True)
    db = await _connect()
    try:
        rows = await (
            await db.execute("SELECT * FROM email_packages WHERE gc_user_id=? ORDER BY id", (wanted,))
        ).fetchall()
    finally:
        await db.close()
    return {"ok": True, "stored": stored, "held": True, "items": [dict(row) for row in rows]}


@router.post("/emails/{package_id}/release")
async def release_email_package(package_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await request.json()
    confirm_user = _clean((data if isinstance(data, dict) else {}).get("confirm_gc_user_id"), 100)
    settings = await _settings()
    if settings.get("email_mode") != "live" or settings.get("email_process_confirmed") != "1":
        raise HTTPException(409, "Сначала подтвердите и включите штатный процесс email в GetCourse")
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM email_packages WHERE id=?", (package_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Email-пакет не найден")
        if confirm_user != _clean(row["gc_user_id"], 100):
            raise HTTPException(400, "Подтвердите точный GetCourse user ID")
        if row["status"] == "sent":
            return {"ok": True, "status": "sent"}
        if row["status"] not in {"held", "failed", "manual_review"}:
            raise HTTPException(409, "Пакет уже обрабатывается")
        await db.execute("DELETE FROM email_recipient_holds WHERE gc_user_id=?", (row["gc_user_id"],))
        await db.execute(
            "UPDATE email_packages SET status='ready',hold_reason='',error='',attempts=0,next_attempt_at=?,updated_at=? WHERE id=?",
            (_iso(), _iso(), package_id),
        )
        await db.commit()
    finally:
        await db.close()
    return {"ok": True, "status": "ready"}


def _upgrade_public_row(row: dict[str, Any], events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    item = dict(row)
    try:
        item["snapshot"] = json.loads(item.pop("snapshot_json", "{}") or "{}")
    except (TypeError, json.JSONDecodeError):
        item["snapshot"] = {}
    item["events"] = events or []
    return item


@router.get("/upgrades/browser/status")
async def upgrade_browser_status(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {"ok": True, **await _browser_status()}


@router.post("/upgrades/browser/install")
async def upgrade_browser_install(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    try:
        return await _install_browser_runtime()
    except Exception as exc:
        if _logger:
            _logger.exception("GetCourse browser runtime installation failed")
        raise HTTPException(502, _clean(exc, 1000)) from exc


@router.post("/upgrades/browser/session")
async def upgrade_browser_session(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    body = await request.body()
    if len(body) > 2 * 1024 * 1024:
        raise HTTPException(413, "Файл сессии больше 2 МБ")
    try:
        parsed = json.loads(body or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(400, "Некорректный JSON сессии") from exc
    if isinstance(parsed, dict) and isinstance(parsed.get("storage_state"), dict):
        parsed = parsed["storage_state"]
    state = _valid_browser_storage_state(parsed)
    target = _browser_state_path()
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, target)
    target.chmod(0o600)
    _worker_state["upgrade_browser_checked_at"] = ""
    _worker_state["upgrade_browser_error"] = ""
    return {"ok": True, "session_loaded": True, "session_updated_at": _iso()}


@router.post("/upgrades/browser/probe")
async def upgrade_browser_probe(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    try:
        result = await _run_browser_action({"action": "probe", "operation_id": "manual-probe"}, timeout=90)
    except Exception as exc:
        result = {"ok": False, "error": _clean(exc, 1000)}
    if not result.get("ok"):
        raise HTTPException(409, result.get("error") or "Сессия GetCourse не прошла проверку")
    return result


@router.get("/upgrades")
async def upgrades(request: Request, status: str = "", search: str = "", limit: int = 200) -> dict[str, Any]:
    await _require_admin(request)
    bounded = max(1, min(500, int(limit)))
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status=?")
        params.append(_clean(status, 80))
    needle = _clean(search, 200)
    if needle:
        clauses.append("(gc_user_id LIKE ? OR email LIKE ? OR name LIKE ? OR upgrade_deal_number LIKE ? OR origin_deal_number LIKE ?)")
        params.extend([f"%{needle}%"] * 5)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(bounded)
    db = await _connect()
    try:
        rows = await (
            await db.execute(f"SELECT * FROM upgrade_jobs{where} ORDER BY id DESC LIMIT ?", tuple(params))
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        event_rows = await (
            await db.execute(
                "SELECT * FROM upgrade_events WHERE job_id IN ({}) ORDER BY id DESC".format(
                    ",".join("?" for _ in ids) or "NULL"
                ),
                tuple(ids),
            )
        ).fetchall()
    finally:
        await db.close()
    by_job: dict[int, list[dict[str, Any]]] = {}
    for event in event_rows:
        by_job.setdefault(int(event["job_id"]), [])
        if len(by_job[int(event["job_id"])]) < 12:
            by_job[int(event["job_id"])].append(dict(event))
    return {"items": [_upgrade_public_row(dict(row), by_job.get(int(row["id"]), [])) for row in rows]}


@router.post("/upgrades/sync")
async def sync_upgrades(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    stored = await _sync_upgrades()
    return {"ok": True, "stored": stored}


@router.post("/upgrades/{job_id}/approve")
async def approve_upgrade(job_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    job = await _upgrade_job(job_id)
    if not job:
        raise HTTPException(404, "Доплата не найдена")
    if job.get("status") != "preview" or job.get("error"):
        raise HTTPException(409, "Подтвердить можно только полностью проверенную строку предпросмотра")
    snapshots = await _upgrade_snapshots(job, live=False)
    ledger_error = _upgrade_ledger_error(
        job,
        snapshots.get(_clean(job.get("origin_order_id"), 100), {}),
        snapshots.get(_clean(job.get("upgrade_order_id"), 100), {}),
    )
    if ledger_error:
        raise HTTPException(409, ledger_error)
    settings = await _settings()
    strategy = "replacement_browser" if settings.get("upgrade_browser_enabled") == "1" else "legacy"
    if strategy == "replacement_browser":
        browser = await _browser_status()
        if not browser.get("ready") or not browser.get("session_loaded"):
            raise HTTPException(409, "Сначала установите браузер и загрузите действующую сессию GetCourse")
    db = await _connect()
    try:
        cursor = await db.execute(
            """UPDATE upgrade_jobs SET approved=1,status='validated',strategy=?,error='',attempts=0,
               next_attempt_at=?,updated_at=? WHERE id=? AND status='preview' AND error=''""",
            (strategy, _iso(), _iso(), job_id),
        )
        if cursor.rowcount != 1:
            raise HTTPException(409, "Состояние доплаты уже изменилось; обновите список")
        await db.commit()
    finally:
        await db.close()
    await _upgrade_event(job_id, "preview", "approved")
    return {"ok": True}


@router.post("/upgrades/{job_id}/origin")
async def choose_upgrade_origin(job_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    job = await _upgrade_job(job_id)
    if not job or job.get("status") not in {"preview", "manual_review"} or job.get("approved"):
        raise HTTPException(409 if job else 404, "Исходный заказ уже нельзя изменить")
    data = await request.json()
    wanted = _clean((data if isinstance(data, dict) else {}).get("origin_order_id"), 100)
    try:
        snapshot = json.loads(job.get("snapshot_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        snapshot = {}
    candidates = [dict(item) for item in snapshot.get("origin_candidates") or [] if isinstance(item, dict)]
    origin = next((item for item in candidates if _clean(item.get("order_id"), 100) == wanted), None)
    if not origin:
        raise HTTPException(400, "Выберите исходный заказ из найденных кандидатов")
    settings = await _settings()
    target_offer = _upgrade_target_offer(_clean(job.get("course_key"), 20), bool(origin.get("autopayment")), settings)
    if not _clean(origin.get("offer_id"), 30) or not target_offer.isdigit():
        raise HTTPException(409, "У выбранного заказа нет Standard offer или целевого Premium offer")
    snapshot["origin"] = origin
    db = await _connect()
    try:
        await db.execute(
            """UPDATE upgrade_jobs SET origin_order_id=?,origin_deal_number=?,source_offer_id=?,
               target_offer_id=?,source_cost=?,source_payed=?,origin_paid_at=?,autopayment=?,
               status='preview',error='',snapshot_json=?,updated_at=? WHERE id=? AND approved=0""",
            (
                wanted, _clean(origin.get("deal_number"), 100), _clean(origin.get("offer_id"), 30),
                target_offer, float(origin.get("cost_money") or 0), float(origin.get("payed_money") or 0),
                _clean(origin.get("paid_at"), 100), 1 if origin.get("autopayment") else 0,
                json.dumps(snapshot, ensure_ascii=False, default=str), _iso(), job_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()
    await _upgrade_event(job_id, _clean(job.get("status"), 80), "origin_selected", details={"origin_order_id": wanted})
    return {"ok": True}


@router.post("/upgrades/{job_id}/repair-legacy")
async def repair_legacy_upgrade(job_id: int, request: Request) -> dict[str, Any]:
    """Queue a guarded repair for a job completed by the retired in-place flow."""

    await _require_admin(request)
    job = await _upgrade_job(job_id)
    if not job:
        raise HTTPException(404, "Доплата не найдена")
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not isinstance(data, dict) or _clean(data.get("confirm_gc_user_id"), 100) != _clean(job.get("gc_user_id"), 100):
        raise HTTPException(400, "Подтвердите точный gc_user_id")
    if job.get("status") != "completed" or _clean(job.get("strategy"), 40):
        raise HTTPException(409, "Выравнивание доступно только для завершённого задания старого сценария")
    if _clean(job.get("replacement_order_id"), 100) or _clean(job.get("payment_id"), 100):
        raise HTTPException(409, "У задания уже есть новый заказ или сохранённый платёж")
    origin_offer, surcharge_offer = _legacy_repair_offers(job)
    if not origin_offer or not surcharge_offer:
        raise HTTPException(409, "В исходном снимке не сохранились предложения Standard и доплаты")
    settings = await _settings()
    if settings.get("upgrade_browser_enabled") != "1":
        raise HTTPException(409, "Браузерная автоматизация выключена")
    browser = await _browser_status()
    if not browser.get("ready") or not browser.get("session_loaded"):
        raise HTTPException(409, "Браузер или сессия GetCourse не готовы")
    # The first worker stage performs the live GetCourse export gate before
    # creating anything.  Do not make this acknowledgement endpoint depend on
    # an asynchronous export file being ready at this exact moment.
    now = _iso()
    db = await _connect()
    try:
        cursor = await db.execute(
            """UPDATE upgrade_jobs SET strategy='replacement_browser_repair',status='repair_validated',
               approved=1,error='',attempts=0,next_attempt_at=?,completed_at='',updated_at=?
               WHERE id=? AND status='completed' AND strategy=''""",
            (now, now, job_id),
        )
        if cursor.rowcount != 1:
            raise HTTPException(409, "Состояние задания уже изменилось; обновите список")
        await db.commit()
    finally:
        await db.close()
    await _upgrade_event(
        job_id, "completed", "legacy_repair_approved",
        details={"origin_offer_id": origin_offer, "surcharge_offer_id": surcharge_offer},
    )
    return {"ok": True, "status": "repair_validated", "live_validation": "pending"}


@router.post("/upgrades/{job_id}/retry")
async def retry_upgrade(job_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM upgrade_jobs WHERE id=?", (job_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Доплата не найдена")
        item = dict(row)
        allowed = {
            "waiting_config", "waiting_browser", "replacement_creating", "replacement_finalizing",
            "origin_canceling", "manual_review", "rollback_pending", "rollback_in_work",
            "rollback_finalizing", "rollback_verifying",
            "repair_validated", "repair_replacement_creating", "repair_replacement_finalizing",
            "repair_replacement_ready", "repair_origin_opening", "repair_restoring_origin", "repair_restoring_surcharge",
        }
        if item.get("status") not in allowed:
            raise HTTPException(409, "Для этого состояния ручной повтор не нужен")
        if item.get("status") == "manual_review" and not item.get("approved"):
            try:
                snapshot = json.loads(item.get("snapshot_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                snapshot = {}
            candidate = dict(snapshot.get("upgrade") or {})
            candidate["origins"] = snapshot.get("origin_candidates") or []
            if not candidate.get("order_id"):
                raise HTTPException(409, "Снимок кандидата повреждён; запустите поиск доплат заново")
            await _upsert_upgrade_candidate(candidate, await _settings())
            await _upgrade_event(job_id, "manual_review", "candidate_revalidated")
            return {"ok": True}
        if _clean(item.get("strategy"), 40) == "replacement_browser_repair":
            next_status = item.get("status") if str(item.get("status")).startswith("repair_") else "repair_validated"
        else:
            next_status = "validated" if item.get("approved") and not str(item.get("status")).startswith("rollback_") else item.get("status")
        await db.execute(
            "UPDATE upgrade_jobs SET status=?,attempts=0,error='',next_attempt_at=?,updated_at=? WHERE id=?",
            (next_status, _iso(), _iso(), job_id),
        )
        await db.commit()
    finally:
        await db.close()
    await _upgrade_event(job_id, _clean(item.get("status"), 80), "manual_retry")
    return {"ok": True}


@router.get("/orders")
async def orders(request: Request, status: str = "", limit: int = 200) -> dict[str, Any]:
    await _require_admin(request)
    bounded = max(1, min(500, int(limit)))
    where = " WHERE status=?" if status else ""
    params: tuple[Any, ...] = (status, bounded) if status else (bounded,)
    db = await _connect()
    try:
        rows = await (await db.execute(f"SELECT * FROM orders{where} ORDER BY id DESC LIMIT ?", params)).fetchall()
    finally:
        await db.close()
    return {"items": [dict(row) for row in rows]}


@router.get("/responses")
async def responses(request: Request, limit: int = 200) -> dict[str, Any]:
    await _require_admin(request)
    bounded = max(1, min(500, int(limit)))
    db = await _connect()
    try:
        order_rows = await (
            await db.execute(
                "SELECT * FROM orders WHERE response<>'' ORDER BY responded_at DESC,id DESC LIMIT ?",
                (bounded,),
            )
        ).fetchall()
        test_rows = await (
            await db.execute(
                "SELECT * FROM test_runs ORDER BY updated_at DESC,id DESC LIMIT ?", (bounded,)
            )
        ).fetchall()
        interaction_rows = await (
            await db.execute(
                "SELECT * FROM interaction_events ORDER BY updated_at DESC,id DESC LIMIT ?", (bounded,)
            )
        ).fetchall()
        linked_order_ids = [
            int(row["order_row_id"]) for row in interaction_rows if row["order_row_id"] is not None
        ]
        linked_orders = await (
            await db.execute(
                "SELECT * FROM orders WHERE id IN ({})".format(
                    ",".join("?" for _ in linked_order_ids) or "NULL"
                ),
                tuple(linked_order_ids),
            )
        ).fetchall()
    finally:
        await db.close()
    items: list[dict[str, Any]] = []
    interaction_order_ids = {
        int(row["order_row_id"]) for row in interaction_rows if row["order_row_id"] is not None
    }
    linked_order_map = {int(row["id"]): dict(row) for row in linked_orders}
    for raw in order_rows:
        row = dict(raw)
        if int(row["id"]) in interaction_order_ids:
            continue
        items.append(
            {
                "id": f"order:{row['id']}",
                "kind": "order",
                "event_at": _clean(row.get("responded_at") or row.get("updated_at"), 100),
                "recipient": _clean(row.get("name") or row.get("email") or row.get("target_platform_id"), 300),
                "recipient_id": _clean(row.get("target_platform_id"), 100),
                "channel": _clean(row.get("target_source"), 40),
                "course": _clean(row.get("course"), 100),
                "tariff": _clean(row.get("tariff"), 100),
                "choice": _clean(row.get("response"), 20),
                "status": _clean(row.get("status"), 40),
                "order_id": _clean(row.get("order_id"), 100),
                "request_id": "",
                "requested_by": "ученик",
                "amo_lead_id": _clean(row.get("amo_lead_id"), 64),
                "amo_task_id": _clean(row.get("amo_task_id"), 64),
                "amo_note_id": _clean(row.get("amo_note_id"), 64),
                "error": _clean(row.get("error"), 1000),
                "details": [],
            }
        )
    for raw in test_rows:
        row = dict(raw)
        try:
            results = [item for item in json.loads(row.get("results_json") or "[]") if isinstance(item, dict)]
        except (TypeError, json.JSONDecodeError):
            results = []
        first = results[0] if results else {}
        last = results[-1] if results else {}
        status = _clean(row.get("status"), 40)
        choice = status.removeprefix("responded_") if status.startswith("responded_") else ""
        mode = _clean(first.get("mode"), 40)
        channel = _clean(first.get("provider"), 40)
        if not channel:
            channel = "telegram" if mode == "telegram_live_task" or row.get("recipient_ref") == "telegram" else "vk"
        items.append(
            {
                "id": f"test:{row['id']}",
                "kind": "test_live" if mode in {"live_task", "telegram_live_task"} else "test",
                "event_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
                "created_at": _clean(row.get("created_at"), 100),
                "recipient": _clean(row.get("recipient_ref") or row.get("recipient_id"), 500),
                "recipient_id": _clean(row.get("recipient_id"), 100),
                "channel": channel,
                "course": "Тест",
                "tariff": "",
                "choice": choice,
                "status": status,
                "order_id": _clean(first.get("order_id"), 100),
                "request_id": _clean(row.get("request_id"), 100),
                "requested_by": _clean(row.get("requested_by"), 200),
                "amo_lead_id": _clean(last.get("amo_lead_id") or first.get("lead_id"), 64),
                "amo_task_id": _clean(last.get("amo_task_id"), 64),
                "amo_note_id": _clean(last.get("amo_note_id"), 64),
                "error": _clean(row.get("error"), 1000),
                "details": results,
            }
        )
    for raw in interaction_rows:
        row = dict(raw)
        try:
            payload = json.loads(row.get("payload_json") or "{}")
            result = json.loads(row.get("result_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            payload, result = {}, {}
        order = linked_order_map.get(int(row["order_row_id"]), {}) if row.get("order_row_id") is not None else {}
        items.append(
            {
                "id": f"interaction:{row['id']}", "kind": "request",
                "event_at": _clean(row.get("updated_at") or row.get("created_at"), 100),
                "created_at": _clean(row.get("created_at"), 100),
                "recipient": _clean(order.get("name") or order.get("email") or row.get("recipient_id"), 500),
                "recipient_id": _clean(order.get("target_platform_id") or row.get("recipient_id"), 100),
                "channel": _clean(row.get("source"), 40),
                "course": _clean(order.get("course") or result.get("course"), 100),
                "tariff": _clean(order.get("tariff"), 100),
                "choice": _clean(row.get("choice"), 20), "status": _clean(row.get("status"), 80),
                "order_id": _clean(order.get("order_id") or result.get("order_id"), 100),
                "request_id": "", "requested_by": "SaleBot",
                "amo_lead_id": _clean(result.get("amo_lead_id") or order.get("amo_lead_id"), 64),
                "amo_task_id": _clean(result.get("amo_task_id") or order.get("amo_task_id"), 64),
                "amo_note_id": _clean(result.get("amo_note_id") or order.get("amo_note_id"), 64),
                "error": _clean(row.get("error"), 1000),
                "details": [{"request": payload, "result": result}],
            }
        )
    items.sort(key=lambda item: (item.get("event_at") or "", item.get("id") or ""), reverse=True)
    return {"items": items[:bounded]}


@router.post("/orders/{order_row_id}/retry")
async def retry_order(order_row_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data: dict[str, Any] = {}
    content_type = _clean(getattr(request, "headers", {}).get("content-type"), 100).lower()
    if content_type.startswith("application/json"):
        try:
            raw_data = await request.json()
        except Exception as exc:
            raise HTTPException(400, "Некорректные данные получателя") from exc
        if not isinstance(raw_data, dict):
            raise HTTPException(400, "Некорректные данные получателя")
        data = raw_data
    platform_id = _clean(data.get("platform_id"), 100)
    salebot_id = _clean(data.get("salebot_id"), 100)
    if platform_id and not re.fullmatch(r"\d{3,20}", platform_id):
        raise HTTPException(400, "platform_id должен содержать от 3 до 20 цифр")
    if salebot_id and not re.fullmatch(r"\d{3,20}", salebot_id):
        raise HTTPException(400, "salebot_id должен содержать от 3 до 20 цифр")

    db = await _connect()
    try:
        row = await (await db.execute("SELECT status FROM orders WHERE id=?", (order_row_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Заказ не найден")
        if row["status"] == "backfill_only":
            raise HTTPException(409, "Архивный заказ навсегда исключён из отправки")
        if row["status"] == "delivery_uncertain":
            raise HTTPException(
                409,
                "Автоповтор запрещён: канал мог уже принять сообщение. Сначала проверьте переписку вручную.",
            )
        if (platform_id or salebot_id) and row["status"] != "waiting_identity":
            raise HTTPException(409, "Получателя можно указать вручную только для статуса «Не найден получатель»")

        resolved_platform_id = ""
        resolved_telegram_id = ""
        if salebot_id:
            messenger = _module("messenger-widget", "service_resolve_onboarding_telegram_target")
            resolved = await messenger.service_resolve_onboarding_telegram_target(
                utm_term=f"salebot_id={salebot_id}"
            )
            resolved_telegram_id = _clean(resolved.get("platform_id"), 100)
            if not resolved.get("ok") or not re.fullmatch(r"\d{5,20}", resolved_telegram_id):
                raise HTTPException(
                    409,
                    _clean(resolved.get("error"), 500)
                    or "Для этого salebot_id не найден связанный Telegram platform_id",
                )
        await db.execute(
            """UPDATE orders SET status=CASE WHEN response<>'' THEN 'response_pending' ELSE 'pending' END,
               target_source=CASE WHEN ?<>'' THEN 'vk' WHEN ?<>'' THEN 'telegram' ELSE target_source END,
               target_platform_id=CASE WHEN ?<>'' THEN ? WHEN ?<>'' THEN ? ELSE target_platform_id END,
               manual_vk_platform_id=CASE WHEN ?<>'' THEN ? ELSE manual_vk_platform_id END,
               manual_telegram_platform_id=CASE WHEN ?<>'' THEN ? ELSE manual_telegram_platform_id END,
               error='',attempts=0,next_attempt_at='',updated_at=? WHERE id=?""",
            (platform_id, resolved_telegram_id, platform_id, platform_id, resolved_telegram_id, resolved_telegram_id,
             platform_id, platform_id, resolved_telegram_id, resolved_telegram_id, _iso(), order_row_id),
        )
        await db.execute("DELETE FROM deliveries WHERE order_row_id=? AND status<>'sent'", (order_row_id,))
        await db.commit()
    finally:
        await db.close()
    return {
        "ok": True,
        "target_source": "vk" if platform_id else "telegram" if resolved_telegram_id else "",
        "target_platform_id": platform_id or resolved_telegram_id,
        "identity_source": "platform_id+salebot_id" if platform_id and salebot_id else "salebot_id" if salebot_id else "platform_id" if platform_id else "",
    }


@router.post("/orders/{order_row_id}/classify")
async def classify_order(order_row_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await request.json()
    branch = _clean((data if isinstance(data, dict) else {}).get("branch"), 40)
    if branch not in {"manager_premium", "manager_standard", "autopay_premium", "autopay_standard"}:
        raise HTTPException(400, "Некорректная ветка")
    db = await _connect()
    try:
        row = await (await db.execute("SELECT status,tariff FROM orders WHERE id=?", (order_row_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Заказ не найден")
        if row["status"] == "backfill_only":
            raise HTTPException(409, "Архивный заказ нельзя вернуть в очередь")
        package = "standard" if _clean(row["tariff"], 40).casefold() == "standard" else "premium"
        if not branch.endswith("_" + package):
            raise HTTPException(409, "Сценарий не соответствует тарифу заказа")
        cursor = await db.execute(
            "UPDATE orders SET branch=?,status='pending',error='',updated_at=? WHERE id=? AND status='classification_needed'",
            (branch, _iso(), order_row_id),
        )
        if cursor.rowcount != 1:
            raise HTTPException(409, "Ручной выбор доступен только до первой отправки")
        await db.commit()
    finally:
        await db.close()
    return {"ok": True}


async def _response_order(token: str) -> dict[str, Any] | None:
    digest = hashlib.sha256(_clean(token, 200).encode()).hexdigest()
    db = await _connect()
    try:
        row = await (
            await db.execute(
                """SELECT o.* FROM response_tokens t JOIN orders o ON o.id=t.order_row_id
                   WHERE t.token_hash=? AND t.expires_at>? LIMIT 1""",
                (digest, _iso()),
            )
        ).fetchone()
    finally:
        await db.close()
    return dict(row) if row else None


def _response_page(row: dict[str, Any], choice: str, *, done: bool = False, error: str = "") -> str:
    label = "Да, курс открылся" if choice == "yes" else "Нет, нужна помощь"
    title = "Ответ принят" if done else "Подтвердите ответ"
    body = error or ("Спасибо! Можно вернуться в Telegram." if done else f"Вы выбрали: {label}")
    button = "" if done else (
        f'<form method="post"><input type="hidden" name="choice" value="{choice}">'
        f'<button type="submit">Подтвердить</button></form>'
    )
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title><style>:root{{color-scheme:dark;font-family:system-ui,sans-serif}}body{{margin:0;min-height:100vh;display:grid;place-items:center;background:#060606;color:#f2f2f2}}main{{width:min(420px,calc(100% - 32px));border:1px solid #333;background:#111;padding:24px}}h1{{font-size:20px;margin:0 0 12px}}p{{color:#bbb;line-height:1.5}}button{{width:100%;min-height:44px;border:0;background:#f4f4f4;color:#080808;font-weight:700;cursor:pointer}}</style></head>
<body><main><h1>{html.escape(title)}</h1><p>{html.escape(body)}</p>{button}</main></body></html>"""


async def _telegram_test_run(request_id: str) -> dict[str, Any] | None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{31,99}", _clean(request_id, 100)):
        return None
    db = await _connect()
    try:
        row = await (await db.execute("SELECT * FROM test_runs WHERE request_id=?", (request_id,))).fetchone()
    finally:
        await db.close()
    if not row:
        return None
    result = dict(row)
    try:
        items = [item for item in json.loads(result.get("results_json") or "[]") if isinstance(item, dict)]
    except (TypeError, json.JSONDecodeError):
        return None
    if not items or _clean(items[0].get("mode"), 40) != "telegram_live_task":
        return None
    result["results"] = items
    return result


@router.get("/test/telegram/respond/{request_id}", response_class=HTMLResponse)
async def telegram_test_response_form(request_id: str, request: Request) -> HTMLResponse:
    choice = _clean(request.query_params.get("choice"), 10)
    row = await _telegram_test_run(request_id)
    if not row or choice not in {"yes", "no"}:
        return HTMLResponse(_response_page({}, "yes", done=True, error="Ссылка недействительна или истекла."), status_code=404)
    if _clean(row.get("status"), 40).startswith("responded_"):
        return HTMLResponse(_response_page(row, row["status"].removeprefix("responded_"), done=True))
    if row.get("status") != "sent":
        return HTMLResponse(_response_page(row, choice, done=True, error="Ответ сейчас обрабатывается. Попробуйте позже."), status_code=409)
    return HTMLResponse(_response_page(row, choice))


@router.post("/test/telegram/respond/{request_id}", response_class=HTMLResponse)
async def telegram_test_response_submit(request_id: str, request: Request) -> HTMLResponse:
    body = (await request.body()).decode("utf-8", errors="replace")[:1000]
    choice = _clean((parse_qs(body).get("choice") or [""])[-1], 10)
    row = await _telegram_test_run(request_id)
    if not row or choice not in {"yes", "no"}:
        return HTMLResponse(_response_page({}, "yes", done=True, error="Ссылка недействительна или истекла."), status_code=404)
    db = await _connect()
    try:
        cursor = await db.execute(
            "UPDATE test_runs SET status=?,updated_at=? WHERE request_id=? AND status='sent'",
            (f"responding_{choice}", _iso(), request_id),
        )
        await db.commit()
        claimed = cursor.rowcount == 1
    finally:
        await db.close()
    if not claimed:
        current = await _telegram_test_run(request_id)
        if current and _clean(current.get("status"), 40).startswith("responded_"):
            return HTMLResponse(_response_page(current, current["status"].removeprefix("responded_"), done=True))
        return HTMLResponse(_response_page(row, choice, done=True, error="Ответ сейчас обрабатывается. Попробуйте позже."), status_code=409)

    try:
        settings = await _settings()
        meta = row["results"][0]
        task: dict[str, Any] = {}
        if choice == "no":
            amo = _module("getcourse-amocrm", "service_create_onboarding_support_task")
            task_reference = _clean(meta.get("task_reference") or request_id, 100)
            task = await amo.service_create_onboarding_support_task(
                order_id=_clean(meta.get("order_id"), 100),
                test_lead_id=_clean(meta.get("lead_id"), 64),
                text=f"БОЕВОЙ ТЕСТ onboarding: нужна помощь с доступом GetCourse ({task_reference})",
                due_minutes=int(settings.get("support_due_minutes") or 60),
            )
            if not task.get("ok"):
                raise RuntimeError(task.get("error") or "Не удалось проверить тестовую задачу amoCRM")
        heading = "[БОЕВОЙ ТЕСТ · TELEGRAM · НАЖАТА КНОПКА «ДА»]" if choice == "yes" else "[БОЕВОЙ ТЕСТ · TELEGRAM · НАЖАТА КНОПКА «НЕТ»]"
        reply = heading + "\n\n" + _render(
            await _template("yes_reply" if choice == "yes" else "no_reply"),
            {"name": "Тестовый пользователь", "course": "Собака"}, settings,
        )
        if task:
            lead_id = _clean(task.get("lead_id"), 64)
            reply += f"\n\nТестовая задача amoCRM: #{_clean(task.get('task_id'), 64) or 'найдена'}"
            if lead_id:
                reply += f"\nhttps://sobakovodpro.amocrm.ru/leads/detail/{lead_id}"
        message_ids = await _send_text(_clean(row.get("recipient_id"), 100), reply)
        results = list(row["results"])
        results.append({
            "stage": f"button_{choice}", "message_ids": message_ids,
            "amo_lead_id": _clean(task.get("lead_id"), 64), "amo_task_id": _clean(task.get("task_id"), 64),
            "amo_status": _clean(task.get("status"), 40),
        })
        db = await _connect()
        try:
            await db.execute(
                "UPDATE test_runs SET status=?,results_json=?,error='',updated_at=? WHERE request_id=?",
                (f"responded_{choice}", json.dumps(results, ensure_ascii=False), _iso(), request_id),
            )
            await db.commit()
        finally:
            await db.close()
    except Exception as exc:
        db = await _connect()
        try:
            await db.execute(
                "UPDATE test_runs SET status='sent',error=?,updated_at=? WHERE request_id=?",
                (_clean(exc, 1000), _iso(), request_id),
            )
            await db.commit()
        finally:
            await db.close()
        if _logger:
            _logger.exception("Telegram onboarding test response failed request=%s", request_id)
        return HTMLResponse(_response_page(row, choice, done=True, error="Не удалось обработать ответ. Попробуйте ещё раз."), status_code=502)
    return HTMLResponse(_response_page(row, choice, done=True))


@router.get("/respond/{token}", response_class=HTMLResponse)
async def response_form(token: str, request: Request) -> HTMLResponse:
    choice = _clean(request.query_params.get("choice"), 10)
    row = await _response_order(token)
    if not row or choice not in {"yes", "no"}:
        return HTMLResponse(_response_page({}, "yes", done=True, error="Ссылка недействительна или истекла."), status_code=404)
    if row.get("response"):
        return HTMLResponse(_response_page(row, row["response"], done=True))
    return HTMLResponse(_response_page(row, choice))


@router.post("/respond/{token}", response_class=HTMLResponse)
async def response_submit(token: str, request: Request) -> HTMLResponse:
    body = (await request.body()).decode("utf-8", errors="replace")[:1000]
    choice = _clean((parse_qs(body).get("choice") or [""])[-1], 10)
    row = await _response_order(token)
    if not row or choice not in {"yes", "no"}:
        return HTMLResponse(_response_page({}, "yes", done=True, error="Ссылка недействительна или истекла."), status_code=404)
    if not row.get("response"):
        db = await _connect()
        try:
            await db.execute(
                "UPDATE orders SET response=?,responded_at=?,status='response_pending',error='',next_attempt_at='',updated_at=? WHERE id=? AND response=''",
                (choice, _iso(), _iso(), row["id"]),
            )
            await db.commit()
        finally:
            await db.close()
    return HTMLResponse(_response_page(row, choice, done=True))
