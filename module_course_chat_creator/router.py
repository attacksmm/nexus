from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import mimetypes
import os
import random
import re
import sqlite3
import sys
import time
from urllib.parse import parse_qs, urlparse
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

try:
    from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
    from orchestrator.telegram_proxy import telegram_mtproto_proxy_url, telethon_proxy_config
    from orchestrator.vk_group_poll import VkGroupPollSubscription, shared_vk_group_poll_hub
except Exception:  # pragma: no cover - isolated local tests
    can_access_module = None
    require_admin = None
    verify_token_from_request = None
    telegram_mtproto_proxy_url = None
    telethon_proxy_config = None
    VkGroupPollSubscription = Any
    shared_vk_group_poll_hub = None

router = APIRouter()

VK_API_VERSION = "5.199"
VK_INITIAL_USER_LIMIT = 450
VK_TEST_STAFF_ID = 1105209997
VK_STAFF_REGISTRY_META_KEY = "vk_staff_registry_v1"
VK_STAFF_EXTRA_IDS = {
    765938,  # Анна Тимофеева — timofeevapodbordog
}
VK_STAFF_SOURCE_TITLES = {
    "тех поддержка || собаковод.про",
    "оп || собаковод.про",
}
VK_STAFF_SOURCE_DISCOVERY_LIMIT = 250
SENLER_API_BASE = "https://senler.ru/api"
SENLER_COURSE_CHAT_SUBSCRIPTION_ID = "3801272"
DEFAULT_MODULE_ID = "course-chat-creator"
STAFF_REGISTRY_MODULE_NAME = "_nexus_mod_staff-registry"
STAFF_REGISTRY_PANEL_PATH = "/nexus/staff-registry/panel/"
DEFAULT_CHAT_LINKS_SPREADSHEET_ID = "1zu1__XcKxJH8yC9ForDvibaUnKFCS1pxWHEjLgqlVXA"
CHAT_LINK_SHEETS = {
    "dog": {"telegram": "304757615", "vk": "443062527"},
    "puppy": {"telegram": "1437498106", "vk": "65520414"},
}
TEMPLATE_DEFAULTS_VERSION = "windsurf-2026-06-02-full"
COURSE_CHAT_TITLE_RE = re.compile(
    r"^\s*\d+\.\s*(?:\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})\s*-\s*"
    r"(Курс Щенок\. Современный Собаковод|Современный Собаковод\b)",
    re.IGNORECASE,
)

_ctx = None
_logger = None
_db_initialized = False
_tg_auth_pending: dict[str, dict[str, Any]] = {}
_vk_bootstrap_subscription: VkGroupPollSubscription | None = None
_vk_pin_watchdog_task: asyncio.Task[Any] | None = None
_manual_vk_sync_tasks: set[asyncio.Task[Any]] = set()
_chat_links_sync_tasks: dict[tuple[str, str], asyncio.Task[Any]] = {}
_vk_staff_reconcile_last = 0.0
_vk_staff_registry_refresh_last = 0.0
_vk_staff_registry_discovery_last = 0.0


def _ensure_local_staff_mutation_allowed() -> None:
    if sys.modules.get(STAFF_REGISTRY_MODULE_NAME) is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Сотрудники управляются в едином реестре: {STAFF_REGISTRY_PANEL_PATH}",
        )
_vk_pin_watchdog_state: dict[str, Any] = {
    "interval_seconds": 10,
    "last_check_at": "",
    "checked": 0,
    "restored": 0,
    "suspended": 0,
    "last_error": "",
}


COURSE_DEFAULTS = [
    {
        "key": "puppy",
        "choice": "1",
        "title": "Курс Щенок. Современный Собаковод",
        "vk_title": "Курс Щенок. Современный Собаковод",
        "tg_title": "Курс Щенок. Современный Собаковод",
        "enabled": 1,
    },
    {
        "key": "dog",
        "choice": "2",
        "title": "Современный Собаковод",
        "vk_title": "Современный Собаковод - закрытый чат",
        "tg_title": "Современный Собаковод - закрытый чат.",
        "enabled": 1,
    },
]

PEOPLE_DEFAULTS = [
    {"kind": "author", "name": "Анна", "vk_id": "765938", "vk_mention": "[id765938|@timofeevapodbordog]", "tg_ref": "@Anna_Timofeeva_Podbordog", "enabled": 1},
    {"kind": "admin", "name": "Наталья", "vk_id": "69145639", "vk_mention": "[id69145639|Наталья]", "tg_ref": "", "enabled": 1},
    {"kind": "kurator", "name": "Ирина", "vk_id": "413314992", "vk_mention": "[id413314992|@demidovair]", "tg_ref": "@Irina_zuza", "parity": "any", "enabled": 1, "note": "Telegram ID 1063673416 · телефон 79206159472"},
    {"kind": "kurator", "name": "Настасья", "vk_id": "https://vk.com/nastasyaeggert", "vk_mention": "[id866850402|@nastasyaeggert]", "tg_ref": "@NastasyaEggert", "parity": "any", "enabled": 1},
    {"kind": "admin", "name": "Техническая поддержка", "vk_id": "1105209997", "vk_mention": "[id1105209997|@tehpod_sobakovodpro]", "tg_ref": "@Tech_kurator", "enabled": 1},
    {"kind": "admin", "name": "Никита", "vk_id": "741919467", "vk_mention": "[id741919467|@attackpng]", "tg_ref": "", "enabled": 1},
    {"kind": "admin", "name": "Андрей", "vk_id": "11335495", "vk_mention": "[id11335495|@id11335495]", "tg_ref": "", "enabled": 1},
]

VK_WELCOME_TEMPLATE = "🐾 Добро пожаловать в закрытый чат курса «{course_full_name}»! 🐾\n\nЯ очень рада, что вы здесь. Вы уже сделали важный шаг на пути к осознанному воспитанию вашей собаки.\n\n🗓 Поток №{stream_number}: Обучение стартует {date_start}\nВпереди у нас 11 недель практического обучения, поддержки и маленьких побед! 💪🏼🐶\n\n📍 ПЕРВЫЙ ШАГ — ЗНАКОМСТВО (ВИЗИТКА)\nПожалуйста, расскажите о себе и своем питомце в ОДНОМ сообщении по форме:\n1️⃣ Ваше имя и город\n2️⃣ Кличка собаки, возраст, порода/фенотип/дворняжка\n3️⃣ С какими трудностями пришли и какой результат хотите получить (ваша точка В)?\n\n✅ ОБЯЗАТЕЛЬСТВО НА КУРС:\nВ конце своего сообщения обязательно добавьте фразу:\n«Я обязуюсь внимательно изучать материалы курса, если я что-то не понял(а) — посмотреть урок еще раз. Выполнять практику, задавать вопросы Анне и кураторам. Быть терпеливым(ой) к себе и своей любимой собаке и идти к результату шаг за шагом».\n\n🎓 КАК ПРОХОДИТ ОБУЧЕНИЕ:\n• Модули открываются еженедельно в субботу в 12:00 (МСК) на платформе.\n• Все вопросы по урокам, разборы и обратную связь пишем прямо в этот чат.\n• Обязательно отмечайте нас, чтобы мы не пропустили вопрос!\n\n👩‍🏫 Создатель курса: Анна - [id765938|@timofeevapodbordog]\n🛡 Кураторы-кинологи: {kurators_text}\n❤️ Руководитель отдела заботы: Андрей - [id11335495|@id11335495]\n🛠 Техническая поддержка: https://vk.me/ssobakovod\n📢 Наше сообщество: https://vk.com/ssobakovod?utm_source=vk_edu_chat\n\n⚖ ПРАВИЛА ЧАТА:\n— Общаемся культурно, ненормативная лексика и спам запрещены.\n— Аудиосообщения запрещены (их используют только кураторы).\n— Сообщения, нарушающие правила, удаляются автоматически.\n\nНу что, начинаем наше путешествие в новый мир! ❤️"
VK_TEST_WELCOME_TEMPLATE = "Проверка учебного VK-чата\n\nПроверьте ссылку, закреп, приветствие модератора и права администратора.\n\nСотрудник: [id1105209997|Техническая поддержка]"
TG_WELCOME_TEMPLATE = "<b>Всем привет и добро пожаловать в закрытый чат курса «{course_name}»!🐾</b>\n\n<i>Я очень рада, что вы здесь. Вы уже сделали важный шаг, а именно решили осознанно выстраивать жизнь со своей собакой, а не терпеть, надеяться, что перерастёт или бороться в одиночку.\n\nВпереди у нас <b>11 недель практического обучения</b>, поддержки, вопросов, открытий и маленьких (а иногда и очень больших) побед💪🏼🐶\n\nЗдесь находится ваше новое окружение, которые всегда помогут вам, подскажут и поддержат! Этого же они ждут и с вашей стороны. Поэтому открытость и общительность всегда приветствуется🙏🏼</i>\n\n🗓Обучение стартует: {date_start}\n\n<b>А пока несколько ВАЖНЫХ организационных моментов, чтобы ваше пребывание на курсе стало еще удобнее и продуктивнее⤵️</b>\n\n📌 <u><a href=\"https://t.me/c/{channel_url_id}/{topic_info_id}\">Главный чат (вы сейчас здесь)</a></u>\nЭто наш навигатор. Здесь мы с командой будем писать важные объявления, делиться новостями курса, напоминать про эфиры и обновления.\n\n📌 <u><a href=\"https://t.me/c/{channel_url_id}/{topic_vizitka_id}\">Подчат «🤝 Визитка»</a></u>\nМесто, где мы знакомимся. После прочтения этого сообщения обязательно перейдите в подчат «Визитка» и расскажите о себе по заданной форме. \nТак мы с командой сможем узнать вас и вашего питомца поближе, а соответственно точнее помочь вам с вашей ситуацией. \n\n📌 <u><a href=\"https://t.me/c/{channel_url_id}/{topic_obuchenie_id}\">Подчат «🎓 Обучение»</a></u>\nСвоего рода наш рабочий кабинет. Здесь все, что касается самого обучения: вопросы по урокам, разборы, обратная связь. \nЕсли что-то не получается - это сюда. \n\n📌 <u><a href=\"https://t.me/c/{channel_url_id}/{topic_boltalka_id}\">Подчат «💬 Болталка»</a></u>\nПросто по-человечески поделиться радостью, сомнениями, успехами, поддержать друг друга, выдохнуть, обсудить - в общем, все что угодно (в рамках правил, разумеется😁)\n_________________________________\n\n<b>ПРАВИЛА ЗАКРЫТОГО ЧАТА</b>\n\n1️⃣ Вопросы <u>по рассрочкам и оплатам</u> курса адресуются <u>в службу заботы</u> @andrew_karakchiev\n\n2️⃣ Если вы <u>хотите задать вопрос</u> мне или моим кураторам, то <u>обязательно упоминайте нас в сообщении</u>, чтобы мы точно не пропустили ваш вопрос. \n\nАнна Тимофеева: @Anna_Timofeeva_Podbordog\n\nКураторы-кинологи в чате: {kurators_list}\n\n❗️Только обязательно делайте это в чате, не пишите нам в личные сообщения❗️\n\n3️⃣ По <u>техническим вопросам или проблемам</u> обращайтесь <u>к тех.поддержке</u> школы @tech_sobakovod_pro\n\n\n<b>В ЧАТЕ ЗАПРЕЩЕНО</b> (сообщения нарушающие правила, будут удалены ботом-модератором автоматически)\n\n• Ненормативная лексика\n• Видео, ссылки НЕ относящиеся к теме обучения\n• Аудио сообщения. Их размещаю я и кураторы\n_________________________________\n\nНу что, начинаем путешествие в новый мир!❤️"
TG_VIZITKA_TEMPLATE = "<b>Место, где мы начинаем знакомство 💛</b>\n\nЗдесь вы можете чуть больше рассказать о себе и своей собаке, а мы сможем лучше понять вашу ситуацию и помочь максимально точно.\n\nОчень прошу не пропускать этот шаг!\n\n✍️ <u>Пожалуйста, напишите ОДНО сообщение по следующей форме:</u>\n\n1️⃣ Ваше имя и город\n2️⃣ Кличка собаки, возраст, порода / метис / дворняжка\n3️⃣ С какими трудностями вы пришли на курс? Какой результат вы хотите получить к концу обучения? Что должно измениться в жизни с собакой?\n\n И в конце обязательно добавьте фразу:\n\n<blockquote>«Я обязуюсь внимательно изучать материалы курса, выполнять практику, задавать вопросы Анне и кураторам, быть терпеливым(ой) к себе и своей собаке и идти к результату шаг за шагом».\n</blockquote>\n\nЭто не формальность. Это ваш личный путь из точки А в точку Б и настрой на 100% результат 😉\n\n<u>Пример сообщения, которое у вас должно получится:</u>\n\n<i>Меня зовут Ольга, г. Москва. У меня Лабрадор-ретривер, 3 года.\n\nХочу, чтобы моя собака перестала тянуть поводок и слышала меня на прогулке. Очень нервничаю каждый выход на улицу, потому что первая проезжающая машина сводит ее с ума.\n\nЯ обязуюсь внимательно изучать материалы курса, выполнять практику, задавать вопросы Анне и кураторам, быть терпеливой к себе и своей собаке и идти к результату шаг за шагом!</i>\n\n<b>Ждем ваших визиток🙌🏼</b>"
TG_OBUCHENIE_TEMPLATE = "<b>Наш рабочий кабинет🎓</b>\n\nСамое важное пространство курса. Всё, что касается обучения, живёт здесь.\n\n👩‍🎓 На обучающей платформе уже доступен нулевой модуль в котором есть первые задания.\n\nДоступ должен был прийти вам на почту, если вы не смогли найти письмо с доступом в кабинет, напишите куратору @Tech_kurator\n\n<b>Модули будут открываться еженедельно в субботу в 12:00 по московскому времени</b>. Не забывайте выполнять задания после видеоуроков, я и мои кураторы проверим каждый ответ лично и дадим развернутую обратную связь.\n\nКроме того, за выполнения заданий, вам <b>будут начисляться бонусные баллы</b>. <b>В нулевом модуле об этом рассказано подробнее.</b>\n\n✅ <u>В этом чате вы можете и даже нужно:</u>\n\n• Задавать вопросы по урокам и заданиям\n• Писать, если что-то не получается или вызывает сомнения\n• Делиться наблюдениями и результатами практики\n• Получать обратную связь от меня и кураторов\n• Разбирать конкретные ситуации с вашей собакой\n\n<b>❗️Здесь нет глупых вопросов. </b>\n\nЛучше спросить, чем делать «на авось». Мы рядом, чтобы поддержать вас на каждом этапе🤍\n\n<u>Как задавать вопросы, чтобы помощь была максимально точной</u>👇🏼\n\nПожалуйста, старайтесь сразу прописать:\n- в каком уроке или задании возник вопрос\n- что именно не получается\n- что уже пробовали делать\n- поведение собаки в этот момент (спокойна / возбуждена / отвлекается и т.д.)\n\nИ <b>обязательно отмечайте нас в сообщении</b>, чтобы мы точно не пропустили вопрос🙌🏼\n\nАнна Тимофеева: @Anna_Timofeeva_Podbordog\nКураторы-кинологи: #{kurators_list}\n\nПомните: результат складывается из маленьких шагов!"
TG_BOLTALKA_TEMPLATE = "<b>Чат, где можно просто поболтать 💬</b>\n\nЗдесь можно выдохнуть 💛\n\n✨ Делится радостями и маленькими победами\n✨ Писать о сложностях и получать поддержку\n✨ Обсуждать повседневную жизнь с собакой\n✨ Показывать фото и видео хвостатых учеников\n✨ Общаться, шутить, знакомиться и поддерживать друг друга\n\nИногда именно поддержка других участников помогает не сдаться и продолжить путь 💪🏼"

def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _db_path() -> Path:
    if _ctx is not None:
        return _ctx.db_path
    return Path(__file__).parent / "data" / f"{DEFAULT_MODULE_ID}.db"


def _data_dir() -> Path:
    if _ctx is not None:
        return _ctx.data_dir
    return Path(__file__).parent / "data"


def _asset_path(name: str) -> Path | None:
    candidates = [
        _data_dir() / name,
        Path(__file__).parent / "static" / name,
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _avatar_path() -> Path | None:
    return _asset_path("group_photo.jpg")


def _sibling_module_db(env_key: str, module_id: str, filename: str) -> Path | None:
    configured = _clean(os.environ.get(env_key))
    if configured:
        path = Path(configured)
        return path if path.exists() else None
    candidates: list[Path] = []
    if _ctx is not None:
        candidates.append(Path(_ctx.module_dir).parent / module_id / "data" / filename)
    repo_root = Path(__file__).resolve().parent.parent
    candidates.extend(
        [
            repo_root / "modules" / module_id / "data" / filename,
            repo_root / f"module_{module_id.replace('-', '_')}" / "data" / filename,
        ]
    )
    return next((path for path in candidates if path.exists()), None)


def _vk_student_cohort(course_key: str, stream_number: str) -> dict[str, Any]:
    chat_fields_db = _sibling_module_db(
        "GETCOURSE_CHAT_FIELDS_DB", "getcourse-chat-fields", "getcourse-chat-fields.db"
    )
    customer_db = _sibling_module_db("CUSTOMER_DB_PATH", "customer-db", "customer-db.db")
    empty = {
        "available": False,
        "source": "getcourse-chat-fields.flow_students_cache",
        "total": 0,
        "with_vk": 0,
        "without_vk": 0,
        "vk_ids": [],
    }
    if not chat_fields_db or not customer_db:
        empty["reason"] = "cohort_database_missing"
        return empty
    try:
        with sqlite3.connect(f"file:{chat_fields_db}?mode=ro", uri=True, timeout=5) as db:
            db.row_factory = sqlite3.Row
            cache_row = db.execute(
                "SELECT value_json,updated_at FROM flow_students_cache ORDER BY datetime(updated_at) DESC LIMIT 1"
            ).fetchone()
        if not cache_row:
            return {**empty, "reason": "flow_students_cache_empty"}
        snapshot = _json_dict(cache_row["value_json"])
        flow = next(
            (
                item
                for item in (snapshot.get("items") or [])
                if _course_key(item.get("course_key")) == _course_key(course_key)
                and _clean(item.get("stream")) == _clean(stream_number)
            ),
            None,
        )
        if not isinstance(flow, dict):
            return {
                **empty,
                "reason": "exact_flow_not_in_cache",
                "cache_updated_at": _clean(cache_row["updated_at"]),
            }
        students = [item for item in (flow.get("students") or []) if isinstance(item, dict)]
        customer_by_id: dict[int, dict[str, Any]] = {}
        customer_by_email: dict[str, list[dict[str, Any]]] = {}
        with sqlite3.connect(f"file:{customer_db}?mode=ro", uri=True, timeout=5) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT id,custom_fields,updated_at,created_at FROM cdb_getcourse_orders ORDER BY datetime(COALESCE(updated_at,created_at)) DESC,id DESC"
            ).fetchall()
        for row in rows:
            fields = _json_dict(row["custom_fields"])
            item = {"id": int(row["id"]), "fields": fields}
            customer_by_id[item["id"]] = item
            email = _clean(fields.get("email") or fields.get("user_email")).casefold()
            if email:
                customer_by_email.setdefault(email, []).append(item)
        people: dict[str, dict[str, Any]] = {}
        unmatched = 0
        not_completed_paid = 0
        standard_excluded = 0
        for student in students:
            email = _clean(student.get("email")).casefold()
            source_id = int(student.get("source_record_id") or 0)
            candidates = ([customer_by_id[source_id]] if source_id in customer_by_id else []) + customer_by_email.get(email, [])
            if not candidates:
                unmatched += 1
                continue
            order = next(
                (
                    item
                    for item in candidates
                    if _clean(item["fields"].get("status")).casefold().replace("ё", "е") == "завершен"
                    and _clean(item["fields"].get("payment_state")).casefold() == "paid"
                ),
                None,
            )
            if not order:
                not_completed_paid += 1
                continue
            fields = order["fields"]
            tariff_text = " ".join(
                str(value or "")
                for value in (
                    student.get("tariff"),
                    fields.get("title"),
                    fields.get("positions"),
                    fields.get("offers"),
                )
            ).casefold().replace("ё", "е")
            if "стандарт" in tariff_text:
                standard_excluded += 1
                continue
            phone = re.sub(r"\D+", "", _clean(fields.get("phone")))
            gc_user_id = _clean(fields.get("gc_user_id") or student.get("gc_user_id"))
            raw_vk = _clean(fields.get("vk_id"))
            vk_digits = re.sub(r"\D+", "", raw_vk)
            vk_id = int(vk_digits) if vk_digits and len(vk_digits) <= 19 and int(vk_digits) > 0 else 0
            identity = gc_user_id or email or phone or f"source:{order['id']}"
            current = people.get(identity)
            if current is None or (not int(current.get("vk_id") or 0) and vk_id):
                people[identity] = {"vk_id": vk_id}
        vk_ids = sorted({int(item["vk_id"]) for item in people.values() if int(item.get("vk_id") or 0) > 0})
        total = len(people)
        return {
            "available": True,
            "source": "getcourse-chat-fields.flow_students_cache",
            "total": total,
            "with_vk": len(vk_ids),
            "without_vk": max(0, total - len(vk_ids)),
            "vk_ids": vk_ids,
            "sheet_students": len(students),
            "unmatched_orders": unmatched,
            "not_completed_paid": not_completed_paid,
            "standard_excluded": standard_excluded,
            "sheet_title": _clean(flow.get("sheet_title")),
            "sheet_id": _clean(flow.get("sheet_id")),
            "sheet_url": _clean(flow.get("sheet_url")),
            "cache_updated_at": _clean(cache_row["updated_at"]),
        }
    except Exception as exc:
        _log("error", "VK student cohort lookup failed course=%s stream=%s: %s", course_key, stream_number, exc)
        return {**empty, "reason": _exc_text(exc)}


def _vk_processed_entitlement_cohort(course_key: str, stream_number: str) -> dict[str, Any]:
    """Read only v2 order decisions already assigned to this exact chat flow."""

    chat_fields_db = _sibling_module_db(
        "GETCOURSE_CHAT_FIELDS_DB", "getcourse-chat-fields", "getcourse-chat-fields.db"
    )
    customer_db = _sibling_module_db("CUSTOMER_DB_PATH", "customer-db", "customer-db.db")
    empty = {
        "available": False,
        "source": "getcourse-chat-fields.processed_orders.entitlement_v2",
        "total": 0,
        "with_vk": 0,
        "without_vk": 0,
        "vk_ids": [],
    }
    if not chat_fields_db or not customer_db:
        return {**empty, "reason": "entitlement_database_missing"}
    try:
        with sqlite3.connect(f"file:{chat_fields_db}?mode=ro", uri=True, timeout=5) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                """
                SELECT source_record_id,details_json
                FROM processed_orders
                WHERE course_key=? AND stream=? AND customer_ok=1
                  AND COALESCE(vk_link,'')<>'' AND COALESCE(tg_link,'')<>''
                """,
                (_course_key(course_key), _clean(stream_number)),
            ).fetchall()
        source_ids: set[int] = set()
        for row in rows:
            entitlement = _json_dict(_json_dict(row["details_json"]).get("entitlement"))
            if int(entitlement.get("version") or 0) != 2 or entitlement.get("eligible") is not True:
                continue
            source_id = int(row["source_record_id"] or 0)
            if source_id > 0:
                source_ids.add(source_id)
        if not source_ids:
            return {**empty, "available": True, "reason": "no_entitled_orders_for_flow"}
        placeholders = ",".join("?" for _ in source_ids)
        with sqlite3.connect(f"file:{customer_db}?mode=ro", uri=True, timeout=5) as db:
            db.row_factory = sqlite3.Row
            customer_rows = db.execute(
                f"SELECT id,custom_fields FROM cdb_getcourse_orders WHERE id IN ({placeholders})",
                tuple(sorted(source_ids)),
            ).fetchall()
        people: dict[str, int] = {}
        for row in customer_rows:
            fields = _json_dict(row["custom_fields"])
            raw_vk = re.sub(r"\D+", "", _clean(fields.get("vk_id")))
            vk_id = int(raw_vk) if raw_vk and len(raw_vk) <= 19 and int(raw_vk) > 0 else 0
            identity = (
                _clean(fields.get("gc_user_id"))
                or _clean(fields.get("email") or fields.get("user_email")).casefold()
                or re.sub(r"\D+", "", _clean(fields.get("phone") or fields.get("user_phone")))
                or f"source:{int(row['id'])}"
            )
            if identity not in people or (not people[identity] and vk_id):
                people[identity] = vk_id
        vk_ids = sorted({value for value in people.values() if value > 0})
        return {
            **empty,
            "available": True,
            "total": len(people),
            "with_vk": len(vk_ids),
            "without_vk": max(0, len(people) - len(vk_ids)),
            "vk_ids": vk_ids,
            "source_records": len(source_ids),
        }
    except Exception as exc:
        return {**empty, "reason": "entitlement_read_failed", "error": _exc_text(exc)}


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _db():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def _backup_irina_contact_repair() -> None:
    path = _db_path()
    if not path.exists():
        return
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as source:
        if not source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='people'").fetchone():
            return
        has_meta = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
        if has_meta and source.execute(
            "SELECT 1 FROM meta WHERE key='irina_contacts_version' AND value='20260813-v1'"
        ).fetchone():
            return
        if _ctx is None:
            backup = path.parent / "backups/course-chat-creator-pre-irina-contacts-v1/course-chat-creator.db"
        else:
            module_dir = Path(__file__).resolve().parent
            nexus_root = next((item for item in (module_dir, *module_dir.parents) if (item / "main.py").exists()), module_dir.parent)
            backup = nexus_root / "backups/course-chat-creator-pre-irina-contacts-v1/course-chat-creator.db"
        if backup.exists():
            return
        backup.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(backup) as target:
            source.backup(target)
            if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("course-chat-creator contact repair backup quick_check failed")
        backup.chmod(0o600)


def _init_db() -> None:
    global _db_initialized
    _backup_irina_contact_repair()
    with _db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                name TEXT NOT NULL,
                vk_id TEXT NOT NULL DEFAULT '',
                vk_mention TEXT NOT NULL DEFAULT '',
                tg_ref TEXT NOT NULL DEFAULT '',
                offer_id INTEGER NOT NULL DEFAULT 0,
                parity TEXT NOT NULL DEFAULT 'any',
                enabled INTEGER NOT NULL DEFAULT 1,
                note TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS courses (
                key TEXT PRIMARY KEY,
                choice TEXT NOT NULL,
                title TEXT NOT NULL,
                vk_title TEXT NOT NULL,
                tg_title TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS templates (
                key TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                title TEXT NOT NULL,
                stream_number TEXT NOT NULL,
                date_start TEXT NOT NULL,
                course_key TEXT NOT NULL,
                test_mode INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                link TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                request_json TEXT NOT NULL DEFAULT '{}',
                response_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platforms TEXT NOT NULL DEFAULT '[]',
                message TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'selected',
                selected_json TEXT NOT NULL DEFAULT '[]',
                excluded_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'draft',
                error TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                sent_at INTEGER NOT NULL DEFAULT 0,
                deleted_at INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS broadcast_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                chat_key TEXT NOT NULL,
                chat_title TEXT NOT NULL,
                peer_id TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                message_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                deleted_at INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(broadcast_id) REFERENCES broadcasts(id)
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        people_columns = {row[1] for row in db.execute("PRAGMA table_info(people)").fetchall()}
        if "offer_id" not in people_columns:
            db.execute("ALTER TABLE people ADD COLUMN offer_id INTEGER NOT NULL DEFAULT 0")
        db.execute("DELETE FROM people WHERE name IN ('Екатерина','ТГ куратор 1','ТГ куратор 2')")
        db.execute("UPDATE people SET kind='author',parity='any',updated_at=strftime('%s','now') WHERE name='Анна'")
        db.execute("UPDATE people SET kind='kurator',parity='any',updated_at=strftime('%s','now') WHERE name='Ирина'")
        db.execute(
            "UPDATE people SET kind='admin',parity='any',updated_at=strftime('%s','now') "
            "WHERE name IN ('Наталья','Андрей','Техническая поддержка','Никита')"
        )
        irina_data_version = db.execute("SELECT value FROM meta WHERE key='irina_contacts_version'").fetchone()
        if not irina_data_version or irina_data_version["value"] != "20260813-v1":
            canonical_irina = db.execute(
                "SELECT id FROM people WHERE kind='kurator' AND name='Ирина' ORDER BY enabled DESC,id LIMIT 1"
            ).fetchone()
            if canonical_irina:
                db.execute(
                    """UPDATE people SET vk_id='413314992',vk_mention='[id413314992|@demidovair]',
                       tg_ref='@Irina_zuza',parity='any',enabled=1,
                       note='Telegram ID 1063673416 · телефон 79206159472',updated_at=strftime('%s','now')
                       WHERE id=?""",
                    (int(canonical_irina["id"]),),
                )
                db.execute(
                    "UPDATE people SET enabled=0,updated_at=strftime('%s','now') WHERE kind='kurator' AND name='Ирина' AND id<>?",
                    (int(canonical_irina["id"]),),
                )
            db.execute(
                "INSERT INTO meta(key,value) VALUES('irina_contacts_version','20260813-v1') ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )
        for row in PEOPLE_DEFAULTS:
            row = {
                "kind": row.get("kind", ""),
                "name": row.get("name", ""),
                "vk_id": row.get("vk_id", ""),
                "vk_mention": row.get("vk_mention", ""),
                "tg_ref": row.get("tg_ref", ""),
                "parity": row.get("parity", "any"),
                "enabled": row.get("enabled", 1),
                "note": row.get("note", ""),
            }
            if row.get("kind") == "kurator" and row.get("name") == "Ирина":
                exists = db.execute(
                    "SELECT 1 FROM people WHERE kind='kurator' AND name='Ирина' LIMIT 1"
                ).fetchone()
            else:
                exists = db.execute(
                    "SELECT 1 FROM people WHERE kind=? AND name=? AND COALESCE(vk_id,'')=? AND COALESCE(tg_ref,'')=?",
                    (row.get("kind", ""), row.get("name", ""), row.get("vk_id", ""), row.get("tg_ref", "")),
                ).fetchone()
            if not exists:
                db.execute(
                    """INSERT INTO people(kind,name,vk_id,vk_mention,tg_ref,parity,enabled,note)
                       VALUES(:kind,:name,:vk_id,:vk_mention,:tg_ref,COALESCE(:parity,'any'),:enabled,COALESCE(:note,''))""",
                    row,
                )
        for name, offer_id in (("Ирина", 8593080), ("Слава", 8593081), ("Настасья", 8593084)):
            db.execute(
                "UPDATE people SET offer_id=? WHERE kind='kurator' AND name=? AND COALESCE(offer_id,0)=0",
                (offer_id, name),
            )
        db.execute(
            """UPDATE people SET tg_ref='@NastasyaEggert',
               vk_mention=CASE WHEN COALESCE(vk_mention,'')='' THEN '[id866850402|@nastasyaeggert]' ELSE vk_mention END,
               enabled=1,updated_at=strftime('%s','now')
               WHERE kind='kurator' AND name='Настасья' AND COALESCE(tg_ref,'')=''"""
        )
        for row in COURSE_DEFAULTS:
            db.execute(
                """INSERT INTO courses(key,choice,title,vk_title,tg_title,enabled)
                   VALUES(:key,:choice,:title,:vk_title,:tg_title,:enabled)
                   ON CONFLICT(key) DO NOTHING""",
                row,
            )
        template_defaults = {
            "vk_welcome": VK_WELCOME_TEMPLATE,
            "vk_test_welcome": VK_TEST_WELCOME_TEMPLATE,
            "tg_welcome": TG_WELCOME_TEMPLATE,
            "tg_vizitka": TG_VIZITKA_TEMPLATE,
            "tg_obuchenie": TG_OBUCHENIE_TEMPLATE,
            "tg_boltalka": TG_BOLTALKA_TEMPLATE,
        }
        current_template_version = db.execute("SELECT value FROM meta WHERE key='template_defaults_version'").fetchone()
        should_refresh_templates = not current_template_version or current_template_version["value"] != TEMPLATE_DEFAULTS_VERSION
        for key, body in template_defaults.items():
            if should_refresh_templates:
                db.execute(
                    "INSERT INTO templates(key, body) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET body=excluded.body,updated_at=strftime('%s','now')",
                    (key, body),
                )
            else:
                db.execute(
                    "INSERT INTO templates(key, body) VALUES(?, ?) ON CONFLICT(key) DO NOTHING",
                    (key, body),
                )
        if should_refresh_templates:
            db.execute(
                "INSERT INTO meta(key,value) VALUES('template_defaults_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (TEMPLATE_DEFAULTS_VERSION,),
            )
        for row in db.execute("SELECT id,tg_ref FROM people WHERE COALESCE(tg_ref,'') != ''").fetchall():
            if not _tg_username(row["tg_ref"]):
                db.execute("UPDATE people SET tg_ref='',updated_at=strftime('%s','now') WHERE id=?", (row["id"],))
        db.commit()
    _db_initialized = True


def _ensure_db() -> None:
    if not _db_initialized:
        _init_db()


async def setup(ctx: Any) -> None:
    global _ctx, _logger, _vk_bootstrap_subscription, _vk_pin_watchdog_task
    _ctx = ctx
    _logger = ctx.logger
    _ensure_db()
    _resume_chat_links_syncs()
    token = _clean(os.environ.get("VK_GROUP_TOKEN"))
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    if not token or group_id <= 0:
        return
    if _vk_pin_watchdog_task is None or _vk_pin_watchdog_task.done():
        _vk_pin_watchdog_task = asyncio.create_task(
            _vk_pin_watchdog_loop(), name=f"{DEFAULT_MODULE_ID}-vk-pin-watchdog"
        )
    if shared_vk_group_poll_hub is None:
        return
    try:
        _vk_bootstrap_subscription = await shared_vk_group_poll_hub.subscribe(
            subscriber_id=f"{DEFAULT_MODULE_ID}:bootstrap",
            token=token,
            group_id=group_id,
            on_event=_handle_vk_bootstrap_event,
            on_error=_handle_vk_bootstrap_error,
        )
        _vk_bootstrap_subscription.activate()
        _log("info", "VK community bootstrap listener started for group %s", group_id)
    except Exception as exc:
        _vk_bootstrap_subscription = None
        _log("error", "VK community bootstrap listener failed: %s", exc)


async def shutdown() -> None:
    global _vk_bootstrap_subscription, _vk_pin_watchdog_task
    task, _vk_pin_watchdog_task = _vk_pin_watchdog_task, None
    pending = [
        candidate
        for candidate in (task, *_manual_vk_sync_tasks, *_chat_links_sync_tasks.values())
        if candidate is not None and not candidate.done()
    ]
    for candidate in pending:
        candidate.cancel()
    if pending:
        _, still_pending = await asyncio.wait(pending, timeout=1.0)
        if still_pending:
            _log("warning", "course-chat-creator background tasks exceeded shutdown deadline")
    _manual_vk_sync_tasks.clear()
    _chat_links_sync_tasks.clear()
    subscription, _vk_bootstrap_subscription = _vk_bootstrap_subscription, None
    if subscription is not None:
        try:
            await asyncio.wait_for(subscription.close(), timeout=1.0)
        except TimeoutError:
            _log("warning", "course-chat-creator VK subscription shutdown timed out")


async def _require_panel_access(request: Request) -> dict:
    if verify_token_from_request is None:
        return {"role": "admin", "username": "local"}
    user = await verify_token_from_request(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if require_admin and require_admin(user):
        return user
    if can_access_module and can_access_module(user, DEFAULT_MODULE_ID):
        return user
    raise HTTPException(status_code=403, detail="Forbidden")


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _today_moscow() -> str:
    return datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y")


def _exc_text(exc: Exception) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "y", "да"}


def _password() -> str:
    return _clean(os.environ.get("NEXUS_CHAT_CREATOR_PASSWORD") or os.environ.get("SBKVD_PROCESS_WEBHOOK_PASSWORD"))


def _check_password(data: dict[str, Any], *, trusted: bool = False) -> None:
    if trusted:
        return
    configured = _password()
    if not configured:
        raise HTTPException(status_code=503, detail="Webhook password is not configured")
    if configured and data.get("password") != configured:
        raise HTTPException(status_code=403, detail="Forbidden")


def _course_key(value: Any) -> str:
    raw = _clean(value).lower()
    aliases = {
        "1": "puppy",
        "puppy": "puppy",
        "щенок": "puppy",
        "shchenok": "puppy",
        "2": "dog",
        "dog": "dog",
        "собака": "dog",
        "собаковод": "dog",
    }
    return aliases.get(raw, raw or "puppy")


def _course_by_input(value: Any) -> sqlite3.Row:
    _ensure_db()
    key = _course_key(value)
    with _db() as db:
        row = db.execute("SELECT * FROM courses WHERE key=? AND enabled=1", (key,)).fetchone()
        if row:
            return row
        row = db.execute("SELECT * FROM courses WHERE choice=? AND enabled=1", (_clean(value),)).fetchone()
        if row:
            return row
    raise HTTPException(status_code=400, detail=f"Unknown or disabled course: {value}")


def _format_title(stream_number: str, date_start: str, course: sqlite3.Row, platform: str) -> str:
    course_title = course["vk_title"] if platform == "vk" else course["tg_title"]
    return f"{stream_number}. {date_start} - {course_title}"


def _stream_is_even(stream_number: Any) -> bool:
    try:
        return int(_clean(stream_number)) % 2 == 0
    except Exception:
        return True


def _people(kind: str | None = None, *, enabled: bool = True) -> list[dict[str, Any]]:
    _ensure_db()
    sql = "SELECT * FROM people WHERE 1=1"
    args: list[Any] = []
    if kind:
        sql += " AND kind=?"
        args.append(kind)
    if enabled:
        sql += " AND enabled=1"
    sql += " ORDER BY kind, id"
    with _db() as db:
        return [dict(row) for row in db.execute(sql, args).fetchall()]


def _default_curator_id() -> int | None:
    kurators = _people("kurator", enabled=True)
    for person in kurators:
        if _clean(person.get("name")).lower() == "ирина":
            return int(person["id"])
    return int(kurators[0]["id"]) if kurators else None


def _require_explicit_curator(data: dict[str, Any]) -> int:
    value = data.get("curator_id")
    if value in (None, ""):
        raise HTTPException(status_code=400, detail="Выберите куратора перед созданием чата")
    try:
        curator_id = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Некорректно выбран куратор") from exc
    if curator_id <= 0:
        raise HTTPException(status_code=400, detail="Некорректно выбран куратор")
    data["curator_id"] = curator_id
    return curator_id


def _selected_people(stream_number: str, curator_id: Any | None = None) -> dict[str, list[dict[str, Any]]]:
    is_even = _stream_is_even(stream_number)
    result: dict[str, list[dict[str, Any]]] = {"admins": [], "kurators": [], "authors": [], "techs": []}
    selected_curator_id: int | None = None
    if curator_id not in (None, ""):
        try:
            selected_curator_id = int(curator_id)
        except Exception:
            raise HTTPException(status_code=400, detail="curator_id must be a numeric people id")
    else:
        selected_curator_id = _default_curator_id()
    for person in _people(enabled=True):
        kind = person["kind"]
        if kind == "admin":
            result["admins"].append(person)
        elif kind == "author":
            result["authors"].append(person)
        elif kind == "tech":
            result["techs"].append(person)
        elif kind == "kurator":
            if selected_curator_id is not None:
                if int(person["id"]) == selected_curator_id:
                    result["kurators"].append(person)
            else:
                parity = person.get("parity") or "any"
                if parity == "any" or (parity == "even" and is_even) or (parity == "odd" and not is_even):
                    result["kurators"].append(person)
    if selected_curator_id is not None and not result["kurators"]:
        raise HTTPException(status_code=400, detail="Selected curator is disabled or not found")
    return result


def _selected_curator_id(stream_number: str, curator_id: Any | None = None) -> int | None:
    selected = _selected_people(stream_number, curator_id)
    if selected["kurators"]:
        return int(selected["kurators"][0]["id"])
    return None


def _vk_staff_for_mode(selected: dict[str, list[dict[str, Any]]], *, test_mode: bool) -> list[dict[str, Any]]:
    staff = selected["admins"] + selected["authors"] + selected["kurators"] + selected["techs"]
    if not test_mode:
        return staff
    configured_id = _clean(os.environ.get("VK_TEST_STAFF_ID"))
    test_staff_id = int(configured_id) if configured_id.isdigit() else VK_TEST_STAFF_ID
    return [person for person in staff if test_staff_id in _vk_ids([person])]


def _vk_ids(people: list[dict[str, Any]]) -> list[int]:
    result: list[int] = []
    for person in people:
        value = _clean(person.get("vk_id"))
        if not value:
            continue
        try:
            item = int(value)
        except ValueError:
            continue
        if item not in result:
            result.append(item)
    return result


def _vk_screen_name(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    raw = raw.split("|")[-1].strip("]") if raw.startswith("[") else raw
    raw = raw.replace("https://", "").replace("http://", "")
    for host in ("vk.com/", "vk.ru/", "m.vk.com/", "m.vk.ru/"):
        if raw.lower().startswith(host):
            raw = raw[len(host):]
            break
    raw = raw.split("?")[0].split("/")[0].strip()
    raw = raw[1:] if raw.startswith("@") else raw
    if raw.startswith("id") and raw[2:].isdigit():
        return raw[2:]
    return raw


async def _resolve_vk_people_ids(people: list[dict[str, Any]], token: str) -> list[int]:
    result = _vk_ids(people)
    seen = set(result)
    for person in people:
        candidates = [
            _clean(person.get("vk_id")),
            _clean(person.get("vk_mention")),
        ]
        for candidate in candidates:
            screen_name = _vk_screen_name(candidate)
            if not screen_name or screen_name.isdigit():
                continue
            resolved = await _vk_method("utils.resolveScreenName", {"screen_name": screen_name}, token)
            if isinstance(resolved, dict) and resolved.get("type") == "user" and resolved.get("object_id"):
                user_id = int(resolved["object_id"])
                if user_id not in seen:
                    result.append(user_id)
                    seen.add(user_id)
                break
    return result


def _tg_refs(people: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    for person in people:
        value = _tg_username(person.get("tg_ref"))
        if value and value not in result:
            result.append(value)
    return result


def _tg_username(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    raw = raw.replace("https://", "").replace("http://", "")
    for host in ("t.me/", "telegram.me/"):
        if raw.lower().startswith(host):
            raw = raw[len(host):]
            break
    raw = raw.split("?")[0].split("/")[0].strip()
    if raw.startswith("@"):
        return raw
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", raw):
        return "@" + raw
    return ""


def _mentions(people: list[dict[str, Any]], platform: str) -> str:
    items: list[str] = []
    for person in people:
        if platform == "vk":
            ref = _clean(person.get("vk_mention"))
            if not ref:
                screen_name = _vk_screen_name(person.get("vk_id"))
                ref = f"@{screen_name}" if screen_name and not screen_name.isdigit() else _clean(person.get("name"))
        else:
            ref = _tg_username(person.get("tg_ref")) or _clean(person.get("name"))
        if person.get("kind") == "kurator":
            name = _clean(person.get("name"))
            if ref and name and name.lower() not in ref.lower():
                ref = f"{ref} - {name}"
        if ref:
            items.append(ref)
    return ", ".join(items) if items else "не указаны"


def _staff_source_local_id(employee: dict[str, Any]) -> int:
    links = employee.get("source_links") if isinstance(employee, dict) else {}
    if not isinstance(links, dict):
        return 0
    value = links.get(DEFAULT_MODULE_ID)
    if isinstance(value, dict):
        value = value.get("local_id")
    try:
        local_id = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return local_id if local_id > 0 else 0


def _staff_identity_values(employee: dict[str, Any], providers: set[str]) -> list[str]:
    result: list[str] = []
    identities = employee.get("identities") if isinstance(employee, dict) else []
    if not isinstance(identities, list):
        return result
    for identity in identities:
        if not isinstance(identity, dict):
            continue
        provider = _clean(identity.get("provider")).casefold().replace("_", "-")
        if provider not in providers:
            continue
        for key in ("external_id", "username"):
            value = _clean(identity.get(key))
            if value and value not in result:
                result.append(value)
    return result


def _staff_vk_keys(value: Any) -> set[str]:
    raw = _clean(value)
    if not raw:
        return set()
    result = {match for match in re.findall(r"\bid(\d+)\b", raw, flags=re.IGNORECASE)}
    screen_name = _vk_screen_name(raw).casefold()
    if screen_name:
        result.add(screen_name)
    return result


def _staff_tg_key(value: Any) -> str:
    return _tg_username(value).casefold()


def _staff_person_view(row: dict[str, Any]) -> dict[str, Any]:
    vk_value = _clean(row.get("vk_id"))
    vk_screen = _vk_screen_name(vk_value)
    tg_ref = _tg_username(row.get("tg_ref"))
    identities: list[dict[str, Any]] = []
    if vk_value:
        identity = {"provider": "vk", "external_id": vk_value}
        if vk_screen and not vk_screen.isdigit():
            identity["username"] = vk_screen
        identities.append(identity)
    if tg_ref:
        identities.append({"provider": "telegram", "username": tg_ref})
    config = {
        "kind": _clean(row.get("kind")),
        "vk_id": vk_value,
        "tg_ref": tg_ref,
        "offer_id": int(row.get("offer_id") or 0),
        "parity": _clean(row.get("parity")) or "any",
        "note": _clean(row.get("note")),
        "enabled": bool(row.get("enabled")),
    }
    return {
        "module_id": DEFAULT_MODULE_ID,
        "local_id": str(int(row["id"])),
        "full_name": _clean(row.get("name")),
        "display_name": _clean(row.get("name")),
        "identities": identities,
        "config": config,
        "active": bool(row.get("enabled")),
        "updated_at": row.get("updated_at"),
    }


def _staff_find_person(
    employee: dict[str, Any], rows: list[dict[str, Any]], config: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    local_id = _staff_source_local_id(employee)
    matched_ids: set[int] = set()
    if local_id:
        matched_ids.update(int(row["id"]) for row in rows if int(row["id"]) == local_id)

    vk_values = _staff_identity_values(employee, {"vk", "vkontakte", "vk.com"})
    tg_values = _staff_identity_values(employee, {"telegram", "tg"})
    if isinstance(config, dict):
        if "vk_id" in config and _clean(config.get("vk_id")):
            vk_values.append(_clean(config.get("vk_id")))
        if "tg_ref" in config and _clean(config.get("tg_ref")):
            tg_values.append(_clean(config.get("tg_ref")))
    requested_vk = set().union(*(_staff_vk_keys(value) for value in vk_values)) if vk_values else set()
    requested_tg = {_staff_tg_key(value) for value in tg_values if _staff_tg_key(value)}
    for row in rows:
        row_vk = _staff_vk_keys(row.get("vk_id")) | _staff_vk_keys(row.get("vk_mention"))
        row_tg = _staff_tg_key(row.get("tg_ref"))
        if (requested_vk and requested_vk.intersection(row_vk)) or (requested_tg and row_tg in requested_tg):
            matched_ids.add(int(row["id"]))
    if len(matched_ids) > 1:
        raise ValueError("Точные идентификаторы указывают на разные записи course-chat-creator")
    if not matched_ids:
        return None
    person_id = next(iter(matched_ids))
    return next(row for row in rows if int(row["id"]) == person_id)


def _staff_default_kind(employee: dict[str, Any]) -> str:
    values: list[str] = []
    job_profile = employee.get("job_profile") if isinstance(employee, dict) else ""
    if isinstance(job_profile, dict):
        values.extend(_clean(job_profile.get(key)).casefold() for key in ("key", "id", "name"))
    else:
        values.append(_clean(job_profile).casefold())
    for role in employee.get("roles") or []:
        if isinstance(role, dict):
            values.extend(_clean(role.get(key)).casefold() for key in ("key", "id", "name"))
        else:
            values.append(_clean(role).casefold())
    aliases = {
        "admin": "admin", "administrator": "admin", "администратор": "admin",
        "author": "author", "автор": "author",
        "kurator": "kurator", "curator": "kurator", "куратор": "kurator",
        "tech": "tech", "support": "tech", "техподдержка": "tech",
    }
    return next((aliases[value] for value in values if value in aliases), "")


def service_staff_connector() -> dict[str, Any]:
    return {
        "ok": True,
        "module_id": DEFAULT_MODULE_ID,
        "title": "Учебные чаты",
        "entity": "person",
        "operations": ["upsert", "deactivate"],
        "supports_deactivate": True,
        "deactivate_preserves_history": True,
        "matching": ["source_link", "vk", "telegram"],
        "config_fields": [
            {"key": "kind", "type": "select", "required": True, "options": ["admin", "kurator", "author", "tech"]},
            {"key": "vk_id", "type": "string"},
            {"key": "tg_ref", "type": "string"},
            {"key": "offer_id", "type": "integer", "minimum": 0},
            {"key": "parity", "type": "select", "options": ["any", "even", "odd"]},
            {"key": "note", "type": "string"},
            {"key": "enabled", "type": "boolean"},
        ],
    }


def service_staff_list() -> list[dict[str, Any]]:
    return [_staff_person_view(row) for row in _people(enabled=False)]


def service_staff_snapshot(*, employee: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(employee, dict):
        raise ValueError("employee должен быть объектом")
    items = service_staff_list()
    if not employee:
        return {"ok": True, "module_id": DEFAULT_MODULE_ID, "items": items}
    rows = [{**item["config"], "id": int(item["local_id"]), "name": item["display_name"], "updated_at": item.get("updated_at")} for item in items]
    row = _staff_find_person(employee, rows)
    if not row:
        return {"ok": True, "module_id": DEFAULT_MODULE_ID, "found": False, "local_id": "", "snapshot": None}
    snapshot = _staff_person_view(row)
    return {"ok": True, "module_id": DEFAULT_MODULE_ID, "found": True, "local_id": snapshot["local_id"], "snapshot": snapshot}


def service_staff_apply(
    *, employee: dict[str, Any], config: dict[str, Any], operation: str, idempotency_key: str = "",
) -> dict[str, Any]:
    if not isinstance(employee, dict) or not isinstance(config, dict):
        raise ValueError("employee и config должны быть объектами")
    operation = _clean(operation).casefold()
    if operation not in {"upsert", "deactivate"}:
        raise ValueError("course-chat-creator поддерживает только upsert и deactivate")
    _ensure_db()
    marker = f"staff_apply:{hashlib.sha256(_clean(idempotency_key).encode()).hexdigest()}" if _clean(idempotency_key) else ""
    with _db() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = [dict(row) for row in db.execute("SELECT * FROM people ORDER BY id").fetchall()]
        if marker:
            replay = db.execute("SELECT value FROM meta WHERE key=?", (marker,)).fetchone()
            if replay:
                replay_data = json.loads(replay["value"])
                replay_row = next((row for row in rows if int(row["id"]) == int(replay_data.get("local_id") or 0)), None)
                if replay_row:
                    snapshot = _staff_person_view(replay_row)
                    return {"ok": True, "module_id": DEFAULT_MODULE_ID, "operation": operation, "local_id": snapshot["local_id"], "changed": False, "config": snapshot["config"], "snapshot": snapshot, "idempotent_replay": True}
        row = _staff_find_person(employee, rows, config)
        if operation == "deactivate":
            if not row:
                return {"ok": True, "module_id": DEFAULT_MODULE_ID, "operation": operation, "local_id": "", "changed": False, "config": {}, "snapshot": None, "warnings": ["Локальная запись не найдена; отключать нечего"]}
            changed = bool(row.get("enabled"))
            if changed:
                db.execute("UPDATE people SET enabled=0,updated_at=strftime('%s','now') WHERE id=?", (int(row["id"]),))
            person_id = int(row["id"])
        else:
            name = (_clean(employee.get("display_name")) or _clean(employee.get("full_name")))[:200]
            if not name and row:
                name = _clean(row.get("name"))[:200]
            if not name:
                raise ValueError("Для course-chat-creator требуется имя сотрудника")
            kind = _clean(config.get("kind") if "kind" in config else (row or {}).get("kind")) or _staff_default_kind(employee)
            if kind not in {"admin", "kurator", "author", "tech"}:
                raise ValueError("kind должен быть admin, kurator, author или tech")
            parity = _clean(config.get("parity") if "parity" in config else (row or {}).get("parity")) or "any"
            if parity not in {"any", "even", "odd"}:
                raise ValueError("parity должен быть any, even или odd")
            try:
                offer_id = max(0, int(config.get("offer_id") if "offer_id" in config else (row or {}).get("offer_id") or 0))
            except (TypeError, ValueError) as exc:
                raise ValueError("offer_id должен быть целым неотрицательным числом") from exc
            if kind != "kurator":
                offer_id = 0
            vk_values = _staff_identity_values(employee, {"vk", "vkontakte", "vk.com"})
            tg_values = _staff_identity_values(employee, {"telegram", "tg"})
            vk_id = _clean(config.get("vk_id") if "vk_id" in config else (row or {}).get("vk_id"))
            if not vk_id and not row and vk_values:
                vk_id = vk_values[0]
            tg_ref = _tg_username(config.get("tg_ref") if "tg_ref" in config else (row or {}).get("tg_ref"))
            if not tg_ref and not row:
                tg_ref = next((_tg_username(value) for value in tg_values if _tg_username(value)), "")
            enabled = bool(config.get("enabled")) if "enabled" in config else bool((row or {}).get("enabled", 1))
            note = _clean(config.get("note") if "note" in config else (row or {}).get("note"))[:2000]
            old_vk = _clean((row or {}).get("vk_id"))
            vk_mention = _clean((row or {}).get("vk_mention")) if old_vk == vk_id else ""
            if vk_id and not vk_mention:
                vk_screen = _vk_screen_name(vk_id)
                vk_mention = f"[id{vk_screen}|{name}]" if vk_screen.isdigit() else (f"@{vk_screen}" if vk_screen else "")
            payload = {"kind": kind, "name": name, "vk_id": vk_id, "vk_mention": vk_mention, "tg_ref": tg_ref, "offer_id": offer_id, "parity": parity, "enabled": int(enabled), "note": note}
            if row:
                changed = any(row.get(key) != value for key, value in payload.items())
                person_id = int(row["id"])
                if changed:
                    db.execute(
                        """UPDATE people SET kind=:kind,name=:name,vk_id=:vk_id,vk_mention=:vk_mention,
                           tg_ref=:tg_ref,offer_id=:offer_id,parity=:parity,enabled=:enabled,note=:note,
                           updated_at=strftime('%s','now') WHERE id=:id""",
                        {**payload, "id": person_id},
                    )
            else:
                cur = db.execute(
                    """INSERT INTO people(kind,name,vk_id,vk_mention,tg_ref,offer_id,parity,enabled,note)
                       VALUES(:kind,:name,:vk_id,:vk_mention,:tg_ref,:offer_id,:parity,:enabled,:note)""",
                    payload,
                )
                person_id, changed = int(cur.lastrowid), True
        if marker:
            db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (marker, json.dumps({"local_id": person_id, "operation": operation})))
        db.commit()
        applied = dict(db.execute("SELECT * FROM people WHERE id=?", (person_id,)).fetchone())
    snapshot = _staff_person_view(applied)
    return {"ok": True, "module_id": DEFAULT_MODULE_ID, "operation": operation, "local_id": snapshot["local_id"], "changed": changed, "config": snapshot["config"], "snapshot": snapshot}


def _template(key: str) -> str:
    _ensure_db()
    with _db() as db:
        row = db.execute("SELECT body FROM templates WHERE key=?", (key,)).fetchone()
    return row["body"] if row else ""


def _render_template(key: str, *, course: sqlite3.Row, stream_number: str, date_start: str, selected: dict[str, list[dict[str, Any]]], platform: str, extra: dict[str, Any] | None = None) -> str:
    values = {
        "course_full_name": course["title"],
        "course_name": course["title"],
        "course_key": course["key"],
        "course_choice": course["choice"],
        "stream_number": stream_number,
        "date_start": date_start,
        "authors_text": _mentions(selected["authors"], platform),
        "kurators_text": _mentions(selected["kurators"], platform),
        "kurators_list": _mentions(selected["kurators"], platform),
        "techs_text": _mentions(selected["techs"], platform),
        "admins_text": _mentions(selected["admins"], platform),
        "channel_url_id": "0",
        "topic_info_id": 1,
        "topic_vizitka_id": 1,
        "topic_obuchenie_id": 1,
        "topic_boltalka_id": 1,
    }
    if extra:
        values.update(extra)
    body = _template(key).replace("#{kurators_list}", "{kurators_list}")
    return body.format(**values)


def _record_run(platform: str, title: str, stream_number: str, date_start: str, course_key: str, test_mode: bool, status: str, request_json: dict[str, Any], response_json: dict[str, Any] | None = None, error: str = "", link: str = "", chat_id: str = "") -> int:
    _ensure_db()
    with _db() as db:
        cursor = db.execute(
            """INSERT INTO runs(platform,title,stream_number,date_start,course_key,test_mode,status,link,chat_id,error,request_json,response_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                platform,
                title,
                stream_number,
                date_start,
                course_key,
                int(test_mode),
                status,
                link,
                str(chat_id or ""),
                error,
                json.dumps(request_json, ensure_ascii=False),
                json.dumps(response_json or {}, ensure_ascii=False),
            ),
        )
        db.commit()
        return int(cursor.lastrowid)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _update_run(run_id: int, status: str, response_json: dict[str, Any], *, error: str = "") -> None:
    _ensure_db()
    with _db() as db:
        db.execute(
            "UPDATE runs SET status=?, error=?, response_json=? WHERE id=?",
            (status, error, json.dumps(response_json, ensure_ascii=False), run_id),
        )
        db.commit()


def _chat_links_credentials_path() -> Path | None:
    raw = _clean(
        os.environ.get("COURSE_CHAT_CREATOR_GOOGLE_CREDENTIALS_FILE")
        or os.environ.get("GETCOURSE_CHAT_FIELDS_GOOGLE_CREDENTIALS_FILE")
        or os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    return Path(raw) if raw else None


def _chat_links_sync_status() -> dict[str, Any]:
    credentials_path = _chat_links_credentials_path()
    return {
        "configured": bool(credentials_path and credentials_path.exists()),
        "delivery_source": "google_sheet",
        "spreadsheet_id": _clean(os.environ.get("COURSE_CHAT_CREATOR_CHAT_LINKS_SPREADSHEET_ID"))
        or DEFAULT_CHAT_LINKS_SPREADSHEET_ID,
    }


def _ready_chat_pair(course_key: str, stream_number: str) -> dict[str, dict[str, Any]]:
    pair: dict[str, dict[str, Any]] = {}
    _ensure_db()
    with _db() as db:
        for platform in ("vk", "telegram"):
            row = db.execute(
                """
                SELECT id,platform,title,stream_number,link,status,created_at
                FROM runs
                WHERE course_key=? AND stream_number=? AND platform=? AND test_mode=0
                  AND COALESCE(link,'')<>'' AND status<>'error'
                ORDER BY created_at DESC,id DESC
                LIMIT 1
                """,
                (course_key, stream_number, platform),
            ).fetchone()
            if row:
                pair[platform] = dict(row)
    return pair


def _chat_link_row(rows: list[list[Any]], stream_number: str) -> int | None:
    expected = _clean(stream_number)
    for index, row in enumerate(rows, start=1):
        title = _clean(row[0] if row else "")
        match = re.search(r"(?:^|\D)(\d{1,6})(?:\D|$)", title)
        if match and match.group(1) == expected:
            return index
    return None


def _chat_link_row_title(course_key: str, stream_number: str) -> str:
    return f"{'Щенок' if _course_key(course_key) == 'puppy' else 'Собака'} {_clean(stream_number)}"


def _sheet_link_write_value(platform: str, existing_link: Any, generated_link: Any) -> tuple[str, str]:
    current = _clean(existing_link)
    if current:
        return current, "preserved"
    if platform == "vk":
        return "", "waiting_manual_link"
    return _clean(generated_link), "filled"


def _google_sheet_request(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    response = None
    for attempt in range(3):
        response = getattr(session, method)(url, **kwargs)
        if response.status_code not in {429, 500, 502, 503, 504}:
            return response
        if attempt < 2:
            retry_after = str(response.headers.get("Retry-After") or "").strip()
            try:
                delay = max(1.0, min(15.0, float(retry_after)))
            except ValueError:
                delay = 2.0 * (attempt + 1) + random.random()
            time.sleep(delay)
    return response


def _sync_chat_pair_to_sheet_sync(
    pair: dict[str, dict[str, Any]],
    credentials_path: Path,
) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    course_key = _clean((pair.get("vk") or {}).get("course_key") or (pair.get("telegram") or {}).get("course_key"))
    stream_number = _clean((pair.get("vk") or {}).get("stream_number") or (pair.get("telegram") or {}).get("stream_number"))
    sheet_ids = CHAT_LINK_SHEETS.get(course_key) or {}
    if not sheet_ids:
        raise RuntimeError(f"Неизвестный курс для таблицы ссылок: {course_key}")
    spreadsheet_id = _clean(os.environ.get("COURSE_CHAT_CREATOR_CHAT_LINKS_SPREADSHEET_ID")) or DEFAULT_CHAT_LINKS_SPREADSHEET_ID
    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    session = AuthorizedSession(credentials)
    metadata_response = _google_sheet_request(
        session,
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_response.raise_for_status()
    titles = {
        str((item.get("properties") or {}).get("sheetId")): _clean((item.get("properties") or {}).get("title"))
        for item in (metadata_response.json() or {}).get("sheets") or []
    }
    platforms = [platform for platform in ("telegram", "vk") if platform in pair]
    platform_titles: dict[str, str] = {}
    ranges: list[tuple[str, str]] = []
    for platform in platforms:
        gid = _clean(sheet_ids.get(platform))
        title = titles.get(gid, "")
        if not title:
            raise RuntimeError(f"Лист gid={gid} не найден")
        platform_titles[platform] = title
        escaped = title.replace("'", "''")
        ranges.append((platform, f"'{escaped}'!A:B"))
    values_response = _google_sheet_request(
        session,
        "get",
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params=[("ranges", value_range) for _platform, value_range in ranges] + [("majorDimension", "ROWS")],
        timeout=30,
    )
    values_response.raise_for_status()
    value_ranges = (values_response.json() or {}).get("valueRanges") or []
    data: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    waiting_manual = False
    for index, platform in enumerate(platforms):
        rows = ((value_ranges[index] or {}).get("values") or []) if index < len(value_ranges) else []
        existing_row = _chat_link_row(rows, stream_number)
        row_number = existing_row or (len(rows) + 1)
        title = platform_titles[platform]
        escaped = title.replace("'", "''")
        run = pair[platform]
        existing_link = (
            rows[existing_row - 1][1]
            if existing_row and len(rows[existing_row - 1]) > 1
            else ""
        )
        link_value, action = _sheet_link_write_value(platform, existing_link, run.get("link"))
        waiting_manual = waiting_manual or action == "waiting_manual_link"
        row_title = _chat_link_row_title(course_key, stream_number)
        values = [[row_title, link_value]] if link_value else [[row_title]]
        target_range = (
            f"'{escaped}'!A{row_number}:B{row_number}"
            if link_value
            else f"'{escaped}'!A{row_number}"
        )
        data.append(
            {
                "range": target_range,
                "majorDimension": "ROWS",
                "values": values,
            }
        )
        updated.append(
            {
                "platform": platform,
                "gid": sheet_ids[platform],
                "row": row_number,
                "action": action,
                "link": link_value,
            }
        )
    if data:
        update_response = _google_sheet_request(
            session,
            "post",
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
            json={"valueInputOption": "RAW", "data": data},
            timeout=30,
        )
        update_response.raise_for_status()
    return {
        "ok": True,
        "status": "waiting_manual_vk_link" if waiting_manual else ("synced" if len(platforms) == 2 else "partially_synced"),
        "stream_number": stream_number,
        "spreadsheet_id": spreadsheet_id,
        "manual_vk_link_required": waiting_manual,
        "updated": updated,
    }


def _save_chat_links_sync_result(course_key: str, stream_number: str, result: dict[str, Any]) -> None:
    _ensure_db()
    with _db() as db:
        rows = db.execute(
            "SELECT id,response_json FROM runs WHERE course_key=? AND stream_number=? AND test_mode=0",
            (course_key, stream_number),
        ).fetchall()
        for row in rows:
            response_data = _json_dict(row["response_json"])
            response_data["link_sync"] = result
            db.execute(
                "UPDATE runs SET response_json=? WHERE id=?",
                (json.dumps(response_data, ensure_ascii=False), int(row["id"])),
            )
        db.commit()


async def _retry_chat_links_sync(course_key: str, stream_number: str) -> None:
    result: dict[str, Any] = {"ok": False, "status": "retry_required"}
    attempt = 0
    try:
        while True:
            attempt += 1
            await asyncio.sleep(min(30 * (2 ** (attempt - 1)), 900))
            result = await _sync_chat_pair_to_sheet(
                course_key, stream_number, test_mode=False, schedule_retry=False,
            )
            result = {**result, "background_attempts": attempt}
            _save_chat_links_sync_result(course_key, stream_number, result)
            if result.get("sheet_sync_ok"):
                _log(
                    "info", "Chat links sheet retry completed course=%s stream=%s attempts=%s",
                    course_key, stream_number, attempt,
                )
                return
            if not _chat_links_sync_retryable(result):
                _log(
                    "error", "Chat links sheet retry stopped course=%s stream=%s warning=%s",
                    course_key, stream_number, _clean(result.get("warning")),
                )
                return
    except asyncio.CancelledError:
        raise
    finally:
        _chat_links_sync_tasks.pop((course_key, stream_number), None)


def _chat_links_sync_retryable(result: dict[str, Any]) -> bool:
    if result.get("status") == "waiting_pair":
        return True
    if result.get("status") != "direct_ready_sheet_error":
        return False
    warning = _clean(result.get("warning")).casefold()
    return any(
        marker in warning
        for marker in (
            "429", "too many requests", "resource_exhausted", "500", "502", "503", "504",
            "timeout", "timed out", "connection", "temporarily unavailable",
        )
    )


def _schedule_chat_links_sync_retry(course_key: str, stream_number: str) -> bool:
    key = (_course_key(course_key), _clean(stream_number))
    existing = _chat_links_sync_tasks.get(key)
    if existing is not None and not existing.done():
        return False
    task = asyncio.create_task(
        _retry_chat_links_sync(*key),
        name=f"course-chat-creator-sheet-{key[0]}-{key[1]}",
    )
    _chat_links_sync_tasks[key] = task
    return True


def _resume_chat_links_syncs() -> None:
    latest_by_course: dict[str, tuple[str, str]] = {}
    latest_by_flow: dict[tuple[str, str], dict[str, Any]] = {}
    _ensure_db()
    with _db() as db:
        rows = db.execute(
            """SELECT course_key,stream_number,response_json
               FROM runs WHERE test_mode=0 ORDER BY id DESC LIMIT 400"""
        ).fetchall()
    for row in rows:
        key = (_course_key(row["course_key"]), _clean(row["stream_number"]))
        if not key[1]:
            continue
        latest_by_course.setdefault(key[0], key)
        if key not in latest_by_flow:
            latest_by_flow[key] = _json_dict(row["response_json"]).get("link_sync") or {}
    keys = set(latest_by_course.values())
    keys.update(key for key, result in latest_by_flow.items() if _chat_links_sync_retryable(result))
    for course_key, stream_number in keys:
        _schedule_chat_links_sync_retry(course_key, stream_number)


async def _sync_chat_pair_to_sheet(
    course_key: str, stream_number: str, *, test_mode: bool, schedule_retry: bool = True,
) -> dict[str, Any]:
    if test_mode:
        return {"ok": True, "status": "skipped_test_mode"}
    pair = _ready_chat_pair(course_key, stream_number)
    missing = [platform for platform in ("telegram", "vk") if platform not in pair]
    if not pair:
        return {"ok": True, "status": "waiting_pair", "missing": missing}
    for run in pair.values():
        run["course_key"] = course_key
        run["stream_number"] = stream_number
    credentials_path = _chat_links_credentials_path()
    if not credentials_path or not credentials_path.exists():
        return {
            "ok": True,
            "status": "direct_ready_sheet_not_configured",
            "sheet_sync_ok": False,
            "warning": "Не настроен service account Google Sheets",
        }
    try:
        result = await asyncio.to_thread(_sync_chat_pair_to_sheet_sync, pair, credentials_path)
        result["missing"] = missing
        result["sheet_sync_ok"] = True
        return result
    except Exception as exc:
        _log("error", "Chat links sheet sync failed course=%s stream=%s: %s", course_key, stream_number, exc)
        result = {
            "ok": True,
            "status": "direct_ready_sheet_error",
            "sheet_sync_ok": False,
            "warning": _exc_text(exc),
        }
        if schedule_retry:
            result["retry_scheduled"] = _schedule_chat_links_sync_retry(course_key, stream_number)
        return result


def _vk_admin_run(run_id: int | None = None) -> dict[str, Any] | None:
    _ensure_db()
    with _db() as db:
        if run_id:
            row = db.execute("SELECT * FROM runs WHERE id=? AND platform='vk'", (run_id,)).fetchone()
        else:
            row = db.execute("SELECT * FROM runs WHERE platform='vk' AND status IN ('needs_admins','needs_members','needs_vk_web_admins') ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def _pending_vk_admin_run_ids(limit: int = 25) -> list[int]:
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    _ensure_db()
    with _db() as db:
        rows = db.execute(
            """SELECT id,response_json FROM runs
               WHERE platform='vk' AND test_mode=0
                 AND status IN ('needs_admins','needs_members','needs_vk_web_admins')
               ORDER BY id DESC LIMIT ?""",
            (max(1, min(int(limit), 100)),),
        ).fetchall()
    return [
        int(row["id"])
        for row in rows
        if int(_json_dict(row["response_json"]).get("owner_group_id") or 0) == group_id
    ]


async def _vk_method(method: str, params: dict[str, Any], token: str) -> Any:
    if not token:
        raise HTTPException(status_code=503, detail="VK token is not configured")
    payload = dict(params)
    payload["access_token"] = token
    payload["v"] = VK_API_VERSION
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(f"https://api.vk.com/method/{method}", data=payload)
    data = response.json()
    if "error" in data:
        _log("error", "VK API error in %s: %s", method, data["error"])
        return {"error": data["error"]}
    return data.get("response")


def _vk_staff_source_title(title: Any) -> bool:
    normalized = re.sub(r"\s+", " ", _clean(title).casefold().replace("ё", "е"))
    return normalized in VK_STAFF_SOURCE_TITLES


def _vk_staff_registry() -> dict[str, Any]:
    _ensure_db()
    with _db() as db:
        row = db.execute("SELECT value FROM meta WHERE key=?", (VK_STAFF_REGISTRY_META_KEY,)).fetchone()
    payload = _json_dict(row["value"] if row else "")
    payload["source_peer_ids"] = sorted({
        int(value) for value in (payload.get("source_peer_ids") or [])
        if str(value).isdigit() and int(value) > 2_000_000_000
    })
    payload["user_ids"] = sorted({
        int(value) for value in (payload.get("user_ids") or [])
        if str(value).isdigit() and int(value) > 0
    })
    return payload


def _persist_vk_staff_registry(payload: dict[str, Any]) -> None:
    with _db() as db:
        db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (VK_STAFF_REGISTRY_META_KEY, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
        )
        db.commit()


async def _discover_vk_staff_source_peers(token: str) -> list[int]:
    peers: set[int] = set()
    for start in range(1, VK_STAFF_SOURCE_DISCOVERY_LIMIT + 1, 25):
        local_ids = range(start, min(start + 25, VK_STAFF_SOURCE_DISCOVERY_LIMIT + 1))
        code = "return [" + ",".join(
            f'API.messages.getConversationsById({{"peer_ids":{2_000_000_000 + local_id}}})'
            for local_id in local_ids
        ) + "];"
        responses = await _vk_method("execute", {"code": code}, token)
        if not isinstance(responses, list):
            continue
        for response in responses:
            if not isinstance(response, dict):
                continue
            item = ((response.get("items") or [{}])[0])
            conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
            if not _vk_staff_source_title((conversation.get("chat_settings") or {}).get("title")):
                continue
            peer_id = int((conversation.get("peer") or {}).get("id") or 0)
            if peer_id > 2_000_000_000:
                peers.add(peer_id)
    return sorted(peers)


async def _refresh_vk_staff_registry(
    token: str,
    *,
    discover: bool = False,
    extra_peer_ids: list[int] | None = None,
) -> dict[str, Any]:
    current = _vk_staff_registry()
    source_peer_ids = set(int(value) for value in current.get("source_peer_ids") or [])
    source_peer_ids.update(
        int(value) for value in (extra_peer_ids or []) if int(value) > 2_000_000_000
    )
    if discover or not source_peer_ids:
        source_peer_ids.update(await _discover_vk_staff_source_peers(token))
    if not source_peer_ids:
        return {**current, "ok": False, "reason": "staff_source_chats_not_found"}

    user_ids: set[int] = set(VK_STAFF_EXTRA_IDS)
    source_chats: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for peer_id in sorted(source_peer_ids):
        response = await _vk_method("messages.getConversationMembers", {"peer_id": peer_id, "extended": 1}, token)
        if isinstance(response, dict) and "error" in response:
            errors.append({"peer_id": peer_id, "error": response.get("error")})
            continue
        if not isinstance(response, dict):
            errors.append({"peer_id": peer_id, "error": "empty VK response"})
            continue
        title = ""
        conversation = await _vk_method("messages.getConversationsById", {"peer_ids": peer_id}, token)
        if isinstance(conversation, dict) and not conversation.get("error"):
            item = (conversation.get("items") or [{}])[0]
            item = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
            title = _clean((item.get("chat_settings") or {}).get("title"))
        if title and not _vk_staff_source_title(title):
            errors.append({"peer_id": peer_id, "error": "chat title is no longer a staff source"})
            continue
        members = {
            int(item.get("member_id") or 0)
            for item in (response.get("items") or [])
            if int(item.get("member_id") or 0) > 0
        }
        user_ids.update(members)
        source_chats.append({"peer_id": peer_id, "title": title, "members": len(members)})
    if not source_chats:
        return {**current, "ok": False, "reason": "staff_source_read_failed", "errors": errors}

    payload = {
        "version": 2,
        "source_peer_ids": sorted(int(item["peer_id"]) for item in source_chats),
        "source_chats": source_chats,
        "extra_user_ids": sorted(VK_STAFF_EXTRA_IDS),
        "user_ids": sorted(user_ids),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    _persist_vk_staff_registry(payload)
    return {**payload, "ok": not errors, "errors": errors}


async def _maybe_refresh_vk_staff_source_event(peer_id: int, token: str) -> bool:
    current = _vk_staff_registry()
    if peer_id in set(current.get("source_peer_ids") or []):
        await _refresh_vk_staff_registry(token, extra_peer_ids=[peer_id])
        return True
    conversation = await _vk_method("messages.getConversationsById", {"peer_ids": peer_id}, token)
    if not isinstance(conversation, dict) or conversation.get("error"):
        return False
    item = (conversation.get("items") or [{}])[0]
    item = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
    if not _vk_staff_source_title((item.get("chat_settings") or {}).get("title")):
        return False
    await _refresh_vk_staff_registry(token, extra_peer_ids=[peer_id])
    return True


def _vk_group_token(*, test_mode: bool = False) -> str:
    if test_mode:
        test_token = _clean(os.environ.get("VK_TEST_GROUP_TOKEN"))
        if test_token:
            return test_token
    return _clean(os.environ.get("VK_GROUP_TOKEN"))


def _vk_created_chat_id(response: Any) -> int:
    if isinstance(response, int):
        return int(response)
    if isinstance(response, str) and response.isdigit():
        return int(response)
    if isinstance(response, dict):
        value = response.get("chat_id") or response.get("id")
        if value is not None:
            return int(value)
    raise HTTPException(status_code=502, detail="VK did not return chat_id")


def _vk_message_reference(response: Any) -> tuple[int | None, int | None]:
    if isinstance(response, int):
        return int(response), None
    if not isinstance(response, dict):
        return None, None
    message_id = response.get("message_id") or response.get("id")
    cmid = response.get("conversation_message_id") or response.get("cmid")
    return (int(message_id) if message_id else None, int(cmid) if cmid else None)


def _vk_require_success(method: str, response: Any) -> Any:
    if isinstance(response, dict) and "error" in response:
        error = response.get("error") or {}
        detail = error.get("error_msg") if isinstance(error, dict) else str(error)
        raise RuntimeError(f"{method}: {detail or error}")
    return response


async def _find_vk_community_chat_message(
    peer_id: int,
    token: str,
    *,
    text: str = "",
    attachment_type: str = "",
    max_cmid: int = 64,
) -> tuple[int | None, int | None]:
    """Resolve VK community-chat messages whose global message id is always zero."""

    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    if group_id <= 0:
        return None, None
    expected_text = re.sub(r"\[(?:id|club)-?\d+\|([^\]]+)\]", r"\1", text)
    response = await _vk_method(
        "messages.getByConversationMessageId",
        {
            "peer_id": int(peer_id),
            "conversation_message_ids": ",".join(str(value) for value in range(1, max_cmid + 1)),
        },
        token,
    )
    if not isinstance(response, dict) or "error" in response:
        return None, None
    items = response.get("items") or []
    for item in items:
        if not isinstance(item, dict) or int(item.get("from_id") or 0) != -group_id:
            continue
        if item.get("action"):
            continue
        actual_text = re.sub(
            r"\[(?:id|club)-?\d+\|([^\]]+)\]", r"\1", str(item.get("text") or "")
        )
        if text and actual_text != expected_text:
            continue
        if attachment_type and not any(
            isinstance(attachment, dict) and attachment.get("type") == attachment_type
            for attachment in (item.get("attachments") or [])
        ):
            continue
        message_id, cmid = _vk_message_reference(item)
        if cmid:
            return message_id, cmid
    return None, None


def _event_value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    getter = getattr(source, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            pass
    return getattr(source, key, default)


def _vk_bootstrap_event_message(event: Any) -> dict[str, Any] | None:
    event_type = getattr(getattr(event, "type", ""), "value", getattr(event, "type", ""))
    if str(event_type or "").strip().lower().split(".")[-1] != "message_new":
        return None
    payload = getattr(event, "object", None) or {}
    message = _event_value(payload, "message", {}) or {}
    action = _event_value(message, "action", {}) or {}
    return {
        "peer_id": int(_event_value(message, "peer_id", 0) or 0),
        "from_id": int(_event_value(message, "from_id", 0) or 0),
        "text": _clean(_event_value(message, "text", "")),
        "action_type": _clean(_event_value(action, "type", "")),
        "action_member_id": int(_event_value(action, "member_id", 0) or 0),
        "action_cmid": int(_event_value(action, "conversation_message_id", 0) or 0),
    }


def _vk_owned_run(peer_id: int) -> dict[str, Any] | None:
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    with _db() as db:
        rows = db.execute(
            "SELECT * FROM runs WHERE platform='vk' ORDER BY id DESC LIMIT 200"
        ).fetchall()
    for row in rows:
        item = dict(row)
        response = _json_dict(item.get("response_json"))
        try:
            owner_group_id = int(response.get("owner_group_id") or 0)
            run_peer_id = int(response.get("peer_id") or 0)
        except (TypeError, ValueError):
            continue
        if owner_group_id != group_id or run_peer_id != int(peer_id):
            continue
        item["response"] = response
        item["request"] = _json_dict(item.get("request_json"))
        return item
    return None


def _pending_vk_bootstrap_run(peer_id: int) -> dict[str, Any] | None:
    item = _vk_owned_run(peer_id)
    if item is None or (item.get("response") or {}).get("bootstrap_status") == "ready":
        return None
    return item


def _persist_vk_bootstrap(
    row: dict[str, Any], response: dict[str, Any], status: str, *, error: str = ""
) -> None:
    response["bootstrap_status"] = "ready" if status == "ok" else status
    response["bootstrap_updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    response["needs_attention"] = status not in {"ok", "waiting_for_message"}
    response["followup_status"] = status
    response["detail"] = error
    _update_run(int(row["id"]), status, response, error=error)


async def _initialize_vk_chat_from_run(row: dict[str, Any]) -> dict[str, Any]:
    token = _vk_group_token(test_mode=bool(row.get("test_mode")))
    if not token:
        raise RuntimeError("VK_GROUP_TOKEN is not configured")
    response = dict(row.get("response") or _json_dict(row.get("response_json")))
    request_json = dict(row.get("request") or _json_dict(row.get("request_json")))
    peer_id = int(response.get("peer_id") or 0)
    if peer_id <= 2000000000:
        raise RuntimeError("VK peer_id is missing for chat bootstrap")
    stream_number = _clean(row.get("stream_number") or request_json.get("stream_number"))
    date_start = _clean(row.get("date_start") or request_json.get("date_start") or request_json.get("start_date"))
    course = _course_by_input(row.get("course_key") or request_json.get("course_type") or request_json.get("course_choice"))
    selected = _selected_people(stream_number, request_json.get("curator_id"))
    try:
        welcome_photo = _asset_path("welcome_message_photo.jpg")
        welcome_text = _render_template(
            "vk_test_welcome" if bool(row.get("test_mode")) else "vk_welcome",
            course=course,
            stream_number=stream_number,
            date_start=date_start,
            selected=selected,
            platform="vk",
        )
        if not response.get("welcome_message_id") and not response.get("welcome_cmid"):
            message_id, cmid = await _find_vk_community_chat_message(
                peer_id,
                token,
                text=welcome_text,
                attachment_type="photo" if welcome_photo else "",
            )
            if not message_id and not cmid:
                attachment = ""
                if welcome_photo:
                    attachment = _clean(await _upload_vk_message_photo(peer_id, welcome_photo, token))
                    if not attachment:
                        raise RuntimeError("VK welcome photo upload failed")
                send_params: dict[str, Any] = {
                    "peer_id": peer_id,
                    "message": welcome_text,
                    "random_id": random.randint(1, 2**31 - 1),
                }
                if attachment:
                    send_params["attachment"] = attachment
                welcome_response = await _vk_method(
                    "messages.send",
                    send_params,
                    token,
                )
                _vk_require_success("messages.send welcome", welcome_response)
                message_id, cmid = _vk_message_reference(welcome_response)
                if not message_id and not cmid:
                    for _attempt in range(20):
                        await asyncio.sleep(0.5)
                        message_id, cmid = await _find_vk_community_chat_message(
                            peer_id,
                            token,
                            text=welcome_text,
                            attachment_type="photo" if attachment else "",
                        )
                        if cmid:
                            break
            if not message_id and not cmid:
                raise RuntimeError("VK did not return welcome message ID")
            response["welcome_message_id"] = message_id
            response["welcome_cmid"] = cmid
            response["welcome_photo_sent"] = True
            if welcome_photo:
                response["welcome_photo_cmid"] = cmid
                response["welcome_message_has_photo"] = True
            _persist_vk_bootstrap(row, response, "waiting_for_message")
        if not response.get("welcome_pinned"):
            pin_params: dict[str, Any] = {"peer_id": peer_id}
            if response.get("welcome_cmid"):
                pin_params["cmid"] = int(response["welcome_cmid"])
            else:
                pin_params["message_id"] = int(response["welcome_message_id"])
            pin_response = await _vk_method("messages.pin", pin_params, token)
            _vk_require_success("messages.pin", pin_response)
            response["welcome_pinned"] = True
        response.pop("bootstrap_error", None)
        _persist_vk_bootstrap(row, response, "ok")
        _log("info", "VK community chat bootstrap completed peer_id=%s run_id=%s", peer_id, row["id"])
        return response
    except Exception as exc:
        response["bootstrap_error"] = _exc_text(exc)
        _persist_vk_bootstrap(row, response, "needs_bootstrap", error=_exc_text(exc))
        raise


def _persist_vk_event_result(
    row: dict[str, Any], response: dict[str, Any], *, status: str | None = None, error: str | None = None
) -> None:
    current_status = _clean(row.get("status")) or "ok"
    current_error = _clean(row.get("error"))
    _update_run(
        int(row["id"]),
        status or current_status,
        response,
        error=current_error if error is None else error,
    )


async def _promote_joined_vk_staff(row: dict[str, Any], message: dict[str, Any]) -> None:
    action_type = _clean(message.get("action_type"))
    if action_type and action_type not in {"chat_invite_user", "chat_invite_user_by_link"}:
        return
    member_id = int(message.get("action_member_id") or message.get("from_id") or 0)
    response = dict(row.get("response") or {})
    members_result = response.get("members_result") if isinstance(response.get("members_result"), dict) else {}
    stored_expected_ids = {
        int(value)
        for value in (members_result.get("expected_staff_ids") or [])
        if str(value).lstrip("-").isdigit() and int(value) > 0
    }
    token = _vk_group_token(test_mode=bool(row.get("test_mode")))
    if not token:
        return
    expected_ids = stored_expected_ids
    try:
        request_json = dict(row.get("request") or _json_dict(row.get("request_json")))
        stream_number = _clean(row.get("stream_number") or request_json.get("stream_number"))
        selected = _selected_people(stream_number, request_json.get("curator_id"))
        current_staff = _vk_staff_for_mode(selected, test_mode=bool(row.get("test_mode")))
        expected_ids = set(await _resolve_vk_people_ids(current_staff, token))
    except Exception as exc:
        _log("warning", "VK current staff resolution failed for peer %s: %s", message.get("peer_id"), exc)
    protected_ids = expected_ids | set(_vk_staff_registry().get("user_ids") or [])
    if member_id not in protected_ids:
        return
    staff_roles = response.get("staff_roles") if isinstance(response.get("staff_roles"), dict) else {}
    member_state = staff_roles.get(str(member_id)) if isinstance(staff_roles.get(str(member_id)), dict) else {}
    if member_state.get("status") == "admin":
        return
    peer_id = int(message["peer_id"])
    api_response = await _vk_method(
        "messages.setMemberRole",
        {"peer_id": peer_id, "member_id": member_id, "role": "admin"},
        token,
    )
    state = await _vk_admin_state(peer_id, sorted(protected_ids), token)
    promoted_ids = sorted(set(int(value) for value in (state.get("admins") or [])) & protected_ids)
    pending_ids = sorted(expected_ids - set(promoted_ids))
    failed = isinstance(api_response, dict) and "error" in api_response
    staff_roles[str(member_id)] = {
        "status": "error" if failed or member_id not in promoted_ids else "admin",
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "error": (api_response.get("error") if failed else state.get("error")) or "",
    }
    response["staff_roles"] = staff_roles
    response["admin_result"] = {
        "ok": member_id in promoted_ids,
        "automatic": True,
        "promoted_ids": promoted_ids,
        "pending_join_ids": pending_ids,
        "protected_staff_ids": sorted(protected_ids),
    }
    present_ids = {
        int(item.get("id") or 0)
        for item in (state.get("members") or [])
        if int(item.get("id") or 0) in expected_ids
    }
    members_result = (
        response.get("members_result")
        if isinstance(response.get("members_result"), dict)
        else {}
    )
    members_result["staff_present"] = sorted(present_ids)
    members_result["staff_pending_join"] = sorted(expected_ids - present_ids)
    members_result["ok"] = not members_result["staff_pending_join"]
    response["members_result"] = members_result
    if member_id in promoted_ids and not pending_ids:
        response.update({"needs_attention": False, "followup_status": "ok", "detail": ""})
        _persist_vk_event_result(row, response, status="ok", error="")
        return
    if member_id in promoted_ids:
        status = "needs_members" if members_result["staff_pending_join"] else "needs_admins"
        detail = (
            "Ожидается вход сотрудников: "
            + ", ".join(map(str, members_result["staff_pending_join"]))
            if members_result["staff_pending_join"]
            else "Ожидается выдача администраторских прав"
        )
        response.update({"needs_attention": True, "followup_status": status, "detail": detail})
        _persist_vk_event_result(row, response, status=status, error=detail)
        return
    detail = f"VK не подтвердил роль администратора для {member_id}"
    response.update({"needs_attention": True, "followup_status": "needs_admins", "detail": detail})
    _persist_vk_event_result(row, response, status="needs_admins", error=detail)


async def _restore_vk_course_pin(row: dict[str, Any], message: dict[str, Any]) -> None:
    action_type = _clean(message.get("action_type"))
    if action_type not in {"chat_pin_message", "chat_unpin_message"}:
        return
    response = dict(row.get("response") or {})
    cmid = int(response.get("welcome_cmid") or 0)
    if cmid <= 0:
        return
    action_cmid = int(message.get("action_cmid") or 0)
    watchdog = response.get("pin_watchdog") if isinstance(response.get("pin_watchdog"), dict) else {}
    if action_type == "chat_pin_message" and action_cmid == cmid:
        if watchdog.get("suspended_by_admin"):
            response["pin_watchdog"] = {
                **watchdog,
                "suspended_by_admin": False,
                "suspended_by_admin_id": 0,
                "suspended_at": "",
                "cmid": cmid,
                "trigger": "course_pin_selected",
            }
            response["welcome_pinned"] = True
            _persist_vk_event_result(row, response)
        return
    token = _vk_group_token(test_mode=bool(row.get("test_mode")))
    if not token:
        return
    actor_id = int(message.get("from_id") or 0)
    if actor_id > 0:
        state = await _vk_admin_state(int(message["peer_id"]), [actor_id], token)
        if actor_id in {int(value) for value in (state.get("admins") or [])}:
            response["pin_watchdog"] = {
                **watchdog,
                "suspended_by_admin": True,
                "suspended_by_admin_id": actor_id,
                "suspended_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "admin_cmid": action_cmid,
                "cmid": cmid,
                "trigger": action_type,
            }
            response["welcome_pinned"] = False
            _persist_vk_event_result(row, response)
            _log(
                "info",
                "VK course pin watchdog suspended by admin peer_id=%s actor_id=%s trigger=%s",
                message.get("peer_id"),
                actor_id,
                action_type,
            )
            return
    pin_response = await _vk_method(
        "messages.pin", {"peer_id": int(message["peer_id"]), "cmid": cmid}, token
    )
    if isinstance(pin_response, dict) and "error" in pin_response:
        detail = "VK не восстановил закреплённое сообщение"
        response.update({"needs_attention": True, "followup_status": "needs_pin", "detail": detail})
        _persist_vk_event_result(row, response, status="needs_pin", error=detail)
        return
    response["pin_watchdog"] = {
        **watchdog,
        "restored_count": int(watchdog.get("restored_count") or 0) + 1,
        "last_restored_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "cmid": cmid,
        "trigger": action_type,
        "suspended_by_admin": False,
        "suspended_by_admin_id": 0,
        "suspended_at": "",
    }
    response["welcome_pinned"] = True
    _persist_vk_event_result(row, response, error="")
    _log(
        "info",
        "VK course pin restored peer_id=%s cmid=%s trigger=%s",
        message.get("peer_id"),
        cmid,
        action_type,
    )


def _vk_course_pin_rows() -> list[dict[str, Any]]:
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    if group_id <= 0:
        return []
    with _db() as db:
        rows = db.execute(
            "SELECT * FROM runs WHERE platform='vk' ORDER BY id DESC LIMIT 500"
        ).fetchall()
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for source in rows:
        row = dict(source)
        response = _json_dict(row.get("response_json"))
        try:
            peer_id = int(response.get("peer_id") or 0)
            owner_group_id = int(response.get("owner_group_id") or 0)
            welcome_cmid = int(response.get("welcome_cmid") or 0)
        except (TypeError, ValueError):
            continue
        if (
            peer_id <= 2000000000
            or owner_group_id != group_id
            or welcome_cmid <= 0
            or peer_id in seen
        ):
            continue
        seen.add(peer_id)
        row["response"] = response
        result.append(row)
    return result


def _vk_conversation_pin_map(response: Any) -> dict[int, int]:
    if not isinstance(response, dict) or "error" in response:
        return {}
    result: dict[int, int] = {}
    for item in response.get("items") or []:
        conversation = item.get("conversation") or item
        peer = conversation.get("peer") or {}
        settings = conversation.get("chat_settings") or {}
        pinned = settings.get("pinned_message") or {}
        try:
            peer_id = int(peer.get("id") or 0)
            cmid = int(pinned.get("conversation_message_id") or 0)
        except (TypeError, ValueError):
            continue
        if peer_id > 2000000000:
            result[peer_id] = cmid
    return result


async def _reconcile_vk_course_pins_once() -> dict[str, Any]:
    token = _vk_group_token()
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    rows = _vk_course_pin_rows()
    checked = 0
    restored = 0
    suspended = 0
    failed = 0
    for offset in range(0, len(rows), 100):
        batch = rows[offset : offset + 100]
        peer_ids = [int((row.get("response") or {}).get("peer_id") or 0) for row in batch]
        state = await _vk_method(
            "messages.getConversationsById",
            {"peer_ids": ",".join(map(str, peer_ids)), "group_id": group_id},
            token,
        )
        if isinstance(state, dict) and "error" in state:
            raise RuntimeError(
                _clean((state.get("error") or {}).get("error_msg"))
                or "VK pin state request failed"
            )
        pin_map = _vk_conversation_pin_map(state)
        for row in batch:
            response = dict(row.get("response") or {})
            peer_id = int(response.get("peer_id") or 0)
            cmid = int(response.get("welcome_cmid") or 0)
            if peer_id not in pin_map:
                continue
            checked += 1
            watchdog = (
                response.get("pin_watchdog")
                if isinstance(response.get("pin_watchdog"), dict)
                else {}
            )
            if pin_map[peer_id] == cmid:
                if watchdog.get("suspended_by_admin"):
                    response["pin_watchdog"] = {
                        **watchdog,
                        "suspended_by_admin": False,
                        "suspended_by_admin_id": 0,
                        "suspended_at": "",
                        "cmid": cmid,
                        "trigger": "course_pin_detected",
                    }
                    response["welcome_pinned"] = True
                    _persist_vk_event_result(row, response)
                continue
            if watchdog.get("suspended_by_admin"):
                suspended += 1
                continue
            pin_response = await _vk_method(
                "messages.pin", {"peer_id": peer_id, "cmid": cmid}, token
            )
            if isinstance(pin_response, dict) and "error" in pin_response:
                failed += 1
                continue
            verify = await _vk_method(
                "messages.getConversationsById",
                {"peer_ids": str(peer_id), "group_id": group_id},
                token,
            )
            if _vk_conversation_pin_map(verify).get(peer_id) != cmid:
                failed += 1
                continue
            response["pin_watchdog"] = {
                **watchdog,
                "restored_count": int(watchdog.get("restored_count") or 0) + 1,
                "last_restored_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "cmid": cmid,
                "trigger": "periodic_reconcile",
                "suspended_by_admin": False,
                "suspended_by_admin_id": 0,
                "suspended_at": "",
            }
            response["welcome_pinned"] = True
            _persist_vk_event_result(row, response, error="")
            restored += 1
            _log(
                "info",
                "VK course pin restored peer_id=%s cmid=%s trigger=periodic_reconcile",
                peer_id,
                cmid,
            )
    result = {
        "ok": failed == 0,
        "checked": checked,
        "restored": restored,
        "suspended": suspended,
        "failed": failed,
    }
    _vk_pin_watchdog_state.update(
        {
            **result,
            "last_check_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "last_error": "" if failed == 0 else f"pin restore failed for {failed} chat(s)",
        }
    )
    return result


async def _vk_pin_watchdog_loop() -> None:
    global _vk_staff_reconcile_last, _vk_staff_registry_refresh_last, _vk_staff_registry_discovery_last
    await asyncio.sleep(2)
    while True:
        try:
            await _reconcile_vk_course_pins_once()
            now = time.monotonic()
            if now - _vk_staff_registry_refresh_last >= 60:
                token = _vk_group_token()
                if token:
                    discover = now - _vk_staff_registry_discovery_last >= 6 * 3600
                    registry = await _refresh_vk_staff_registry(token, discover=discover)
                    _vk_staff_registry_refresh_last = now
                    if discover and registry.get("source_peer_ids"):
                        _vk_staff_registry_discovery_last = now
                    if not registry.get("ok", True):
                        _log("warning", "VK staff registry refresh needs attention: %s", registry.get("reason") or registry.get("errors"))
            if time.monotonic() - _vk_staff_reconcile_last >= 30:
                _vk_staff_reconcile_last = time.monotonic()
                for run_id in _pending_vk_admin_run_ids():
                    try:
                        await _retry_vk_admins_from_run(run_id)
                    except Exception as exc:
                        _log("warning", "VK staff reconcile failed run_id=%s: %s", run_id, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            detail = _exc_text(exc)
            if detail != _vk_pin_watchdog_state.get("last_error"):
                _log("warning", "VK course pin reconcile failed: %s", detail)
            _vk_pin_watchdog_state.update(
                {
                    "ok": False,
                    "last_check_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "last_error": detail,
                }
            )
        await asyncio.sleep(int(_vk_pin_watchdog_state["interval_seconds"]))


async def _handle_vk_bootstrap_event(event: Any) -> None:
    message = _vk_bootstrap_event_message(event)
    if (
        not message
        or message["peer_id"] <= 2000000000
        or (message["from_id"] <= 0 and message["action_member_id"] <= 0)
    ):
        return
    owned_row = _vk_owned_run(message["peer_id"])
    token = _vk_group_token()
    if token and owned_row is None:
        try:
            if await _maybe_refresh_vk_staff_source_event(message["peer_id"], token):
                return
        except Exception as exc:
            _log("warning", "VK staff source refresh failed peer_id=%s: %s", message["peer_id"], exc)
    if owned_row is not None:
        try:
            await _promote_joined_vk_staff(owned_row, message)
            if message["action_type"]:
                await _restore_vk_course_pin(owned_row, message)
                if not bool(owned_row.get("test_mode")) and message["action_type"] in {
                    "chat_invite_user",
                    "chat_invite_user_by_link",
                }:
                    member_id = int(message.get("action_member_id") or message["from_id"] or 0)
                    await _sync_vk_users_to_senler([member_id])
        except Exception as exc:
            _log("error", "VK community chat event automation failed peer_id=%s: %s", message["peer_id"], exc)
    row = _pending_vk_bootstrap_run(message["peer_id"])
    if row is None:
        return
    try:
        await _initialize_vk_chat_from_run(row)
    except Exception as exc:
        _log("error", "VK community chat bootstrap failed peer_id=%s: %s", message["peer_id"], exc)


async def _handle_vk_bootstrap_error(error: Exception) -> None:
    _log("warning", "VK community bootstrap listener error: %s", error)


async def _resolve_current_vk_user_id(token: str | None) -> int | None:
    if not token:
        return None
    response = await _vk_method("users.get", {}, token)
    if not isinstance(response, list) or not response:
        return None
    try:
        return int(response[0].get("id"))
    except Exception:
        return None


async def _upload_vk_message_photo(peer_id: int, photo_path: Path, token: str) -> str | None:
    try:
        upload_data = await _vk_method("photos.getMessagesUploadServer", {"peer_id": peer_id}, token)
        if isinstance(upload_data, dict) and "error" in upload_data:
            return None
        upload_url = upload_data.get("upload_url")
        if not upload_url:
            return None
        content_type = mimetypes.guess_type(photo_path.name)[0] or "application/octet-stream"
        async with httpx.AsyncClient(timeout=30.0) as client:
            with open(photo_path, "rb") as f:
                upload_resp = await client.post(upload_url, files={"photo": (photo_path.name, f, content_type)})
        uploaded = upload_resp.json()
        saved = await _vk_method(
            "photos.saveMessagesPhoto",
            {"server": uploaded.get("server"), "photo": uploaded.get("photo"), "hash": uploaded.get("hash")},
            token,
        )
        if not isinstance(saved, list) or not saved:
            return None
        photo = saved[0]
        attachment = f"photo{photo.get('owner_id')}_{photo.get('id')}"
        if photo.get("access_key"):
            attachment += f"_{photo['access_key']}"
        return attachment
    except Exception as exc:
        _log("warning", "VK welcome photo upload failed: %s", exc)
        return None


async def _upload_vk_chat_photo(peer_id: int, photo_path: Path, token: str) -> bool:
    try:
        upload_data = await _vk_method("photos.getChatUploadServer", {"chat_id": peer_id - 2000000000}, token)
        if isinstance(upload_data, dict) and "error" in upload_data:
            return False
        upload_url = upload_data.get("upload_url")
        if not upload_url:
            return False
        content_type = mimetypes.guess_type(photo_path.name)[0] or "image/jpeg"
        async with httpx.AsyncClient(timeout=30.0) as client:
            with open(photo_path, "rb") as f:
                upload_resp = await client.post(upload_url, files={"file": (photo_path.name, f, content_type)})
        data = upload_resp.json()
        response_file = data.get("response")
        if not response_file:
            return False
        set_result = await _vk_method("messages.setChatPhoto", {"file": response_file}, token)
        return not (isinstance(set_result, dict) and "error" in set_result)
    except Exception as exc:
        _log("warning", "VK chat avatar upload failed: %s", exc)
        return False


def _vk_member_role(member: dict[str, Any]) -> str:
    return _clean(member.get("role") or member.get("member_role") or member.get("is_admin") and "admin")


async def _vk_admin_state(peer_id: int, target_ids: list[int], token: str) -> dict[str, Any]:
    members_resp = await _vk_method("messages.getConversationMembers", {"peer_id": peer_id}, token)
    if isinstance(members_resp, dict) and "error" in members_resp:
        return {"ok": False, "error": members_resp["error"], "admins": [], "members": [], "missing_admins": target_ids}
    raw_items = members_resp.get("items", []) if isinstance(members_resp, dict) else []
    profiles = {int(p.get("id")): p for p in (members_resp.get("profiles", []) if isinstance(members_resp, dict) else []) if p.get("id") is not None}
    rows: list[dict[str, Any]] = []
    admins: list[int] = []
    for item in raw_items:
        member_id = int(item.get("member_id", 0) or 0)
        profile = profiles.get(member_id, {})
        role = _vk_member_role(item)
        is_admin = role in {"admin", "creator", "administrator"} or bool(item.get("is_admin"))
        if is_admin:
            admins.append(member_id)
        rows.append({
            "id": member_id,
            "role": role,
            "is_admin": is_admin,
            "screen_name": profile.get("screen_name", ""),
            "name": " ".join(filter(None, [_clean(profile.get("first_name")), _clean(profile.get("last_name"))])),
        })
    missing = [user_id for user_id in target_ids if user_id not in admins]
    return {"ok": True, "admins": admins, "members": rows, "missing_admins": missing}


async def _vk_wait_for_chat_members(chat_id: int, peer_id: int, target_ids: list[int], token: str, *, timeout_seconds: int = 75) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    attempts: list[dict[str, Any]] = []
    missing = list(target_ids)
    while time.time() < deadline:
        state = await _vk_admin_state(peer_id, target_ids, token)
        present = [int(item.get("id")) for item in state.get("members", []) if item.get("id") in target_ids]
        missing = [user_id for user_id in target_ids if user_id not in present]
        attempts.append({"present": present, "missing": missing, "error": state.get("error")})
        if not missing:
            return {"ok": True, "present": present, "missing": [], "attempts": attempts[-5:]}
        await asyncio.sleep(2.5)
    return {"ok": False, "present": [user_id for user_id in target_ids if user_id not in missing], "missing": missing, "attempts": attempts[-5:]}


async def _vk_try_api_admins(peer_id: int, target_ids: list[int], token: str) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for admin_id in target_ids:
        await asyncio.sleep(0.5)
        resp = await _vk_method("messages.setMemberRole", {"peer_id": peer_id, "member_id": admin_id, "role": "admin"}, token)
        ok = not (isinstance(resp, dict) and "error" in resp)
        results.append({"member_id": admin_id, "ok": ok, "response": resp})
    state = await _vk_admin_state(peer_id, target_ids, token)
    return {"ok": not state.get("missing_admins"), "results": results, "state": state}


async def _send_vk_invite_fallbacks(
    user_ids: list[int],
    *,
    invite_link: str,
    group_id: int,
    token: str,
    course: sqlite3.Row,
    stream_number: str,
    date_start: str,
    selected: dict[str, list[dict[str, Any]]],
    dedupe_namespace: int = 0,
) -> dict[str, Any]:
    # Customer chat links are delivered by getcourse-onboarding. Keep this
    # legacy entry point fail-closed so an old task or direct internal call
    # cannot send a duplicate personal message from the community.
    return {
        "candidates": len(user_ids),
        "sent": 0,
        "not_allowed": 0,
        "failed": 0,
        "errors": [],
        "sent_user_ids": [],
        "not_allowed_user_ids": [],
        "failed_user_ids": [],
        "disabled": True,
    }

    result: dict[str, Any] = {
        "candidates": len(user_ids),
        "sent": 0,
        "not_allowed": 0,
        "failed": 0,
        "errors": [],
        "sent_user_ids": [],
        "not_allowed_user_ids": [],
        "failed_user_ids": [],
    }
    if not user_ids or not invite_link:
        return result
    message = _render_template(
        "vk_invite_fallback",
        course=course,
        stream_number=stream_number,
        date_start=date_start,
        selected=selected,
        platform="vk",
        extra={"invite_link": invite_link},
    )
    for user_id in user_ids:
        try:
            allowed_response = await _vk_method(
                "messages.isMessagesFromGroupAllowed",
                {"group_id": int(group_id), "user_id": int(user_id)},
                token,
            )
            allowed = bool(
                allowed_response.get("is_allowed")
                if isinstance(allowed_response, dict)
                else allowed_response
            )
            if not allowed:
                result["not_allowed"] += 1
                result["not_allowed_user_ids"].append(int(user_id))
                continue
            send_response = await _vk_method(
                "messages.send",
                {
                    "peer_id": int(user_id),
                    "message": message,
                    "random_id": (
                        ((int(dedupe_namespace) * 1_000_003 + int(user_id)) % (2**31 - 1)) or 1
                        if dedupe_namespace
                        else random.randint(1, 2**31 - 1)
                    ),
                },
                token,
            )
            if isinstance(send_response, dict) and "error" in send_response:
                raise RuntimeError(_clean((send_response.get("error") or {}).get("error_msg")) or "VK send failed")
            result["sent"] += 1
            result["sent_user_ids"].append(int(user_id))
        except Exception as exc:
            result["failed"] += 1
            result["failed_user_ids"].append(int(user_id))
            if len(result["errors"]) < 20:
                result["errors"].append({"user_id": int(user_id), "error": _exc_text(exc)})
        await asyncio.sleep(0.1)
    return result


def _manual_vk_link_from_rows(
    rows: list[list[Any]], stream_number: str, generated_link: str = ""
) -> str:
    expected = _clean(stream_number)
    generated = _clean(generated_link)
    for row in rows:
        if len(row) < 2:
            continue
        title = _clean(row[0])
        link = _clean(row[1])
        match = re.search(r"(?:^|\D)(\d{1,6})(?:\D|$)", title)
        if match and match.group(1) == expected and link.startswith("http") and link != generated:
            return link
    return ""


async def _manual_vk_invite_link(course_key: str, stream_number: str, generated_link: str) -> str:
    gid = _clean((CHAT_LINK_SHEETS.get(course_key) or {}).get("vk"))
    if not gid:
        return ""
    spreadsheet_id = (
        _clean(os.environ.get("COURSE_CHAT_CREATOR_CHAT_LINKS_SPREADSHEET_ID"))
        or DEFAULT_CHAT_LINKS_SPREADSHEET_ID
    )
    async with httpx.AsyncClient(timeout=25.0) as client:
        response = await client.get(
            f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq",
            params={"tqx": "out:csv", "gid": gid},
            headers={"User-Agent": "Nexus Course Chat Creator"},
        )
        response.raise_for_status()
    rows = [list(row) for row in csv.reader(io.StringIO(response.text.lstrip("\ufeff")))]
    return _manual_vk_link_from_rows(rows, stream_number, generated_link)


def _canonical_vk_invite_runs(rows: list[Any]) -> list[dict[str, Any]]:
    """Keep one run per flow and carry durable recipient outcomes across old runs."""

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = (_course_key(row.get("course_key")), _clean(row.get("stream_number")))
        if not key[0] or not key[1]:
            continue
        group = grouped.setdefault(key, {"run": row, "completed_vk_ids": set()})
        if int(row.get("id") or 0) > int(group["run"].get("id") or 0):
            group["run"] = row
        response = _json_dict(row.get("response_json"))
        invites = response.get("student_invites") if isinstance(response.get("student_invites"), dict) else {}
        for field in ("sent_vk_ids", "not_allowed_vk_ids", "joined_vk_ids"):
            group["completed_vk_ids"].update(
                int(value)
                for value in (invites.get(field) or [])
                if str(value).lstrip("-").isdigit() and int(value) > 0
            )
    result: list[dict[str, Any]] = []
    for group in grouped.values():
        row = dict(group["run"])
        row["historical_completed_vk_ids"] = sorted(group["completed_vk_ids"])
        result.append(row)
    return sorted(result, key=lambda item: int(item.get("id") or 0))


def _community_owned_vk_invite_runs(rows: list[Any], group_id: int) -> list[dict[str, Any]]:
    owned: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        response = _json_dict(row.get("response_json"))
        try:
            owner_group_id = int(response.get("owner_group_id") or 0)
        except (TypeError, ValueError):
            owner_group_id = 0
        if owner_group_id == int(group_id):
            owned.append(row)
    return _canonical_vk_invite_runs(owned)


async def _dispatch_pending_vk_student_invites_once() -> dict[str, int]:
    # Disabled permanently: onboarding owns customer-facing link delivery.
    return {
        "checked": 0,
        "sent": 0,
        "waiting_link": 0,
        "not_allowed": 0,
        "failed": 0,
        "disabled": 1,
    }

    token = _vk_group_token(test_mode=False)
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    result = {"checked": 0, "sent": 0, "waiting_link": 0, "not_allowed": 0, "failed": 0}
    if not token or group_id <= 0:
        return result
    _ensure_db()
    with _db() as db:
        stored_rows = db.execute(
            """
            SELECT id,status,error,course_key,stream_number,date_start,link,request_json,response_json
            FROM runs
            WHERE platform='vk' AND test_mode=0 AND COALESCE(response_json,'')<>''
            ORDER BY id
            """
        ).fetchall()
    for stored in _community_owned_vk_invite_runs(stored_rows, group_id):
        response_json = _json_dict(stored["response_json"])
        invites = response_json.get("student_invites")
        entitlement_cohort = _vk_processed_entitlement_cohort(
            _clean(stored["course_key"]), _clean(stored["stream_number"])
        )
        entitlement_ids = {
            int(value)
            for value in (entitlement_cohort.get("vk_ids") or [])
            if str(value).lstrip("-").isdigit() and int(value) > 0
        }
        if not isinstance(invites, dict):
            invites = {
                "initial_added": 0,
                "delivery": "community_message_and_client_chat_links_page",
                "community_messages": "waiting_manual_link" if entitlement_ids else "not_needed",
                "pending_vk_ids": sorted(entitlement_ids),
                "sent_vk_ids": [],
                "not_allowed_vk_ids": [],
                "joined_vk_ids": [],
            }
        else:
            existing_pending = {
                int(value)
                for value in (invites.get("pending_vk_ids") or [])
                if str(value).lstrip("-").isdigit() and int(value) > 0
            }
            invites["pending_vk_ids"] = sorted(existing_pending | entitlement_ids)
        invites["entitlement_cohort"] = {
            key: value for key, value in entitlement_cohort.items() if key != "vk_ids"
        }
        response_json["student_invites"] = invites
        if entitlement_ids:
            _update_run(
                int(stored["id"]),
                _clean(stored["status"]),
                response_json,
                error=_clean(stored["error"]),
            )
        pending_ids = {
            int(value)
            for value in (invites.get("pending_vk_ids") or [])
            if str(value).lstrip("-").isdigit() and int(value) > 0
        }
        completed_ids = {
            int(value)
            for key in ("sent_vk_ids", "not_allowed_vk_ids", "joined_vk_ids")
            for value in (invites.get(key) or [])
            if str(value).lstrip("-").isdigit() and int(value) > 0
        }
        completed_ids.update(int(value) for value in stored.get("historical_completed_vk_ids") or [])
        target_ids = sorted(pending_ids - completed_ids)
        if not target_ids:
            continue
        peer_id = int(response_json.get("peer_id") or 0)
        if peer_id > 2000000000:
            member_state = await _vk_admin_state(peer_id, target_ids, token)
            joined_ids = {
                int(item.get("id") or 0)
                for item in (member_state.get("members") or [])
                if int(item.get("id") or 0) in target_ids
            }
            if joined_ids:
                invites["joined_vk_ids"] = sorted(
                    set(invites.get("joined_vk_ids") or []) | joined_ids
                )
                target_ids = sorted(set(target_ids) - joined_ids)
                response_json["student_invites"] = invites
                _update_run(
                    int(stored["id"]),
                    _clean(stored["status"]),
                    response_json,
                    error=_clean(stored["error"]),
                )
        if not target_ids:
            continue
        result["checked"] += len(target_ids)
        try:
            manual_link = await _manual_vk_invite_link(
                _clean(stored["course_key"]), _clean(stored["stream_number"]), _clean(stored["link"])
            )
        except Exception as exc:
            invites["community_messages"] = "waiting_manual_link"
            invites["last_error"] = _exc_text(exc)
            response_json["student_invites"] = invites
            _update_run(int(stored["id"]), _clean(stored["status"]), response_json, error=_clean(stored["error"]))
            result["waiting_link"] += len(target_ids)
            continue
        if not manual_link:
            invites["community_messages"] = "waiting_manual_link"
            invites["last_error"] = ""
            response_json["student_invites"] = invites
            _update_run(int(stored["id"]), _clean(stored["status"]), response_json, error=_clean(stored["error"]))
            result["waiting_link"] += len(target_ids)
            continue
        request_json = _json_dict(stored["request_json"])
        course = _course_by_input(_clean(stored["course_key"]))
        selected = _selected_people(_clean(stored["stream_number"]), request_json.get("curator_id"))
        delivery = await _send_vk_invite_fallbacks(
            target_ids,
            invite_link=manual_link,
            group_id=group_id,
            token=token,
            course=course,
            stream_number=_clean(stored["stream_number"]),
            date_start=_clean(stored["date_start"]),
            selected=selected,
            dedupe_namespace=int(stored["id"]),
        )
        sent_ids = sorted(set(invites.get("sent_vk_ids") or []) | set(delivery["sent_user_ids"]))
        not_allowed_ids = sorted(
            set(invites.get("not_allowed_vk_ids") or []) | set(delivery["not_allowed_user_ids"])
        )
        invites.update(
            {
                "community_messages": "sent" if not delivery["failed"] else "partial",
                "manual_vk_link": manual_link,
                "sent_vk_ids": sent_ids,
                "not_allowed_vk_ids": not_allowed_ids,
                "last_error": "; ".join(item["error"] for item in delivery["errors"][:3]),
            }
        )
        response_json["student_invites"] = invites
        _update_run(int(stored["id"]), _clean(stored["status"]), response_json, error=_clean(stored["error"]))
        result["sent"] += delivery["sent"]
        result["not_allowed"] += delivery["not_allowed"]
        result["failed"] += delivery["failed"]
    return result


async def _send_vk_staff_invites(
    user_ids: list[int],
    *,
    invite_link: str,
    group_id: int,
    token: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "candidates": len(user_ids),
        "sent": 0,
        "not_allowed": 0,
        "failed": 0,
        "errors": [],
    }
    if not user_ids or not invite_link:
        return result
    message = (
        "Вступите в учебный чат по ссылке:\n"
        f"{invite_link}\n\n"
        "После входа права администратора будут выданы автоматически."
    )
    for user_id in user_ids:
        try:
            allowed_response = await _vk_method(
                "messages.isMessagesFromGroupAllowed",
                {"group_id": int(group_id), "user_id": int(user_id)},
                token,
            )
            allowed = bool(
                allowed_response.get("is_allowed")
                if isinstance(allowed_response, dict)
                else allowed_response
            )
            if not allowed:
                result["not_allowed"] += 1
                continue
            send_response = await _vk_method(
                "messages.send",
                {
                    "peer_id": int(user_id),
                    "message": message,
                    "random_id": random.randint(1, 2**31 - 1),
                },
                token,
            )
            if isinstance(send_response, dict) and "error" in send_response:
                raise RuntimeError(
                    _clean((send_response.get("error") or {}).get("error_msg"))
                    or "VK send failed"
                )
            result["sent"] += 1
        except Exception as exc:
            result["failed"] += 1
            if len(result["errors"]) < 20:
                result["errors"].append({"user_id": int(user_id), "error": _exc_text(exc)})
        await asyncio.sleep(0.4)
    return result


async def _create_vk_chat(data: dict[str, Any], *, trusted: bool = False) -> dict[str, Any]:
    _check_password(data, trusted=trusted)
    test_mode = _bool(data.get("test_mode"))
    token = _vk_group_token(test_mode=test_mode)
    group_id = _clean(os.environ.get("VK_GROUP_ID"))
    if not token:
        raise HTTPException(status_code=503, detail="VK_GROUP_TOKEN is not configured")
    if not group_id.isdigit() or int(group_id) <= 0:
        raise HTTPException(status_code=503, detail="VK_GROUP_ID is not configured")
    stream_number = _clean(data.get("stream_number") or "15")
    date_start = _clean(data.get("date_start") or data.get("start_date")) or _today_moscow()
    course = _course_by_input(data.get("course_type") or data.get("course_choice") or "puppy")
    title = _format_title(stream_number, date_start, course, "vk")
    selected = _selected_people(stream_number, data.get("curator_id"))
    staff_people = _vk_staff_for_mode(selected, test_mode=test_mode)
    if test_mode and not staff_people:
        raise HTTPException(
            status_code=409,
            detail=f"Тестовый сотрудник VK {VK_TEST_STAFF_ID} не найден среди активных сотрудников",
        )
    expected_staff_ids = await _resolve_vk_people_ids(staff_people, token)
    if test_mode:
        cohort = {
            "available": True,
            "source": "test_mode",
            "total": 0,
            "with_vk": 0,
            "without_vk": 0,
            "vk_ids": [],
            "reason": "test_mode",
        }
    else:
        cohort = _vk_processed_entitlement_cohort(course["key"], stream_number)
    student_vk_ids = [int(value) for value in cohort.get("vk_ids", []) if int(value) > 0]
    requested_ids = list(dict.fromkeys(expected_staff_ids + student_vk_ids))
    initial_ids = requested_ids[:VK_INITIAL_USER_LIMIT]
    initial_params: dict[str, Any] = {"title": title, "group_id": int(group_id)}
    if initial_ids:
        initial_params["user_ids"] = ",".join(map(str, initial_ids))
    create_resp = await _vk_method("messages.createChat", initial_params, token)
    bulk_create_error = ""
    if isinstance(create_resp, dict) and "error" in create_resp and initial_ids:
        bulk_create_error = _clean((create_resp.get("error") or {}).get("error_msg")) or "VK rejected initial members"
        create_resp = await _vk_method(
            "messages.createChat", {"title": title, "group_id": int(group_id)}, token
        )
    if isinstance(create_resp, dict) and "error" in create_resp:
        raise HTTPException(status_code=500, detail=create_resp["error"].get("error_msg") or create_resp["error"])
    chat_id = _vk_created_chat_id(create_resp)
    peer_id = 2000000000 + chat_id
    response_peer_ids = {
        int(value)
        for value in (create_resp.get("peer_ids", []) if isinstance(create_resp, dict) else [])
        if str(value).lstrip("-").isdigit() and int(value) > 0
    }
    member_state = await _vk_admin_state(peer_id, expected_staff_ids, token)
    actual_ids = {
        int(item.get("id") or 0)
        for item in member_state.get("members", [])
        if int(item.get("id") or 0) > 0
    } or response_peer_ids
    added_student_ids = sorted(actual_ids & set(student_vk_ids))
    failed_student_ids = sorted(set(student_vk_ids) - set(added_student_ids))
    present_staff_ids = sorted(actual_ids & set(expected_staff_ids))
    missing_staff_ids = sorted(set(expected_staff_ids) - set(present_staff_ids))
    members_result: dict[str, Any] = {
        "ok": not bulk_create_error and not failed_student_ids and not missing_staff_ids,
        "join_mode": "initial_members_and_invite_link" if initial_ids else "invite_link",
        "expected_staff_ids": expected_staff_ids,
        "staff_present": present_staff_ids,
        "staff_pending_join": missing_staff_ids,
        "student_source": cohort.get("source"),
        "student_cohort_available": bool(cohort.get("available")),
        "student_total": int(cohort.get("total") or 0),
        "student_with_vk": int(cohort.get("with_vk") or 0),
        "student_without_vk": int(cohort.get("without_vk") or 0),
        "student_requested": len(student_vk_ids),
        "student_added": len(added_student_ids),
        "student_not_added": len(failed_student_ids),
        "initial_request_limit": VK_INITIAL_USER_LIMIT,
        "bulk_create_error": bulk_create_error,
        "reason": "test_mode" if test_mode else _clean(cohort.get("reason")),
    }
    avatar_ready = False
    photo = _avatar_path()
    if photo:
        avatar_ready = await _upload_vk_chat_photo(peer_id, photo, token)
    invite_data = await _vk_method("messages.getInviteLink", {"peer_id": peer_id}, token)
    invite_link = invite_data.get("link", "") if isinstance(invite_data, dict) else ""
    run_status = "waiting_for_message" if invite_link else "needs_invite_link"
    run_error = "" if invite_link else "VK did not return an invite link"
    members_result["invite_link_ready"] = bool(invite_link)
    if present_staff_ids:
        admin_result = await _vk_try_api_admins(peer_id, present_staff_ids, token)
        admin_result["pending_join_ids"] = missing_staff_ids
    else:
        admin_result = {
            "ok": not expected_staff_ids,
            "skipped": True,
            "reason": "no_staff_present" if expected_staff_ids else "no_staff_members",
            "expected_staff_ids": expected_staff_ids,
            "pending_join_ids": missing_staff_ids,
        }
    response = {
        "message": "VK community chat created. Waiting for the first member." if invite_link else "VK community chat created without an invite link.",
        "group_link": invite_link,
        "chat_id": chat_id,
        "peer_id": peer_id,
        "owner_group_id": int(group_id),
        "join_mode": members_result["join_mode"],
        "avatar_ready": avatar_ready,
        "bootstrap_status": run_status,
        "test_mode": test_mode,
        "title": title,
        "curator_id": _selected_curator_id(stream_number, data.get("curator_id")),
        "members_result": members_result,
        "admin_result": admin_result,
        "needs_attention": run_status == "needs_invite_link",
        "followup_status": run_status,
        "detail": run_error,
    }
    run_id = _record_run(
        "vk",
        title,
        stream_number,
        date_start,
        course["key"],
        test_mode,
        run_status,
        data,
        response,
        error=run_error,
        link=invite_link,
        chat_id=str(chat_id),
    )
    if not invite_link:
        return response
    row = {
        "id": run_id,
        "stream_number": stream_number,
        "date_start": date_start,
        "course_key": course["key"],
        "test_mode": int(test_mode),
        "request": dict(data),
        "response": response,
    }
    try:
        prepared = await _initialize_vk_chat_from_run(row)
        if not test_mode:
            prepared["senler_chat_members"] = await _sync_vk_course_chat_members_to_senler([peer_id])
        link_sync = await _sync_chat_pair_to_sheet(course["key"], stream_number, test_mode=test_mode)
        prepared["student_invites"] = {
            "initial_added": len(added_student_ids),
            "delivery": "onboarding_message",
            "community_messages": "disabled",
            "pending_vk_ids": failed_student_ids,
            "sent_vk_ids": [],
            "not_allowed_vk_ids": [],
            "client_link_required": int(cohort.get("without_vk") or 0) + len(failed_student_ids),
            "getcourse_link_required": int(cohort.get("without_vk") or 0) + len(failed_student_ids),
        }
        prepared["staff_invites"] = {
            "delivery": "history_link",
            "community_messages": "disabled",
            "pending": missing_staff_ids,
        }
        prepared["link_sync"] = link_sync
        prepared["message"] = "VK community chat created and prepared."
        if missing_staff_ids:
            detail = "Ожидается вход сотрудников: " + ", ".join(map(str, missing_staff_ids))
            prepared.update(
                {
                    "needs_attention": True,
                    "followup_status": "needs_members",
                    "detail": detail,
                }
            )
            _update_run(run_id, "needs_members", prepared, error=detail)
        elif not bool((prepared.get("admin_result") or {}).get("ok", True)):
            detail = "Ожидается выдача администраторских прав"
            prepared.update(
                {
                    "needs_attention": True,
                    "followup_status": "needs_admins",
                    "detail": detail,
                }
            )
            _update_run(run_id, "needs_admins", prepared, error=detail)
        elif not bool(link_sync.get("ok", True)):
            detail = _clean(link_sync.get("error")) or "Не удалось обновить таблицу ссылок"
            prepared.update(
                {
                    "needs_attention": True,
                    "followup_status": "needs_link_sync",
                    "detail": detail,
                }
            )
            _update_run(run_id, "needs_link_sync", prepared, error=detail)
        else:
            prepared.update({"needs_attention": False, "followup_status": "ok", "detail": ""})
            _update_run(run_id, "ok", prepared)
        return prepared
    except Exception:
        with _db() as db:
            stored = db.execute("SELECT response_json FROM runs WHERE id=?", (run_id,)).fetchone()
        failed = _json_dict(stored["response_json"] if stored else response)
        failed["message"] = "VK community chat created; preparation needs attention."
        _update_run(run_id, "needs_bootstrap", failed, error=_clean(failed.get("bootstrap_error")))
        return failed


async def _retry_vk_admins_from_run(run_id: int | None = None) -> dict[str, Any]:
    row = _vk_admin_run(run_id)
    if not row:
        return {"ok": True, "skipped": True, "reason": "no_pending_vk_admin_runs"}
    request_json = _json_dict(row.get("request_json"))
    response_json = _json_dict(row.get("response_json"))
    peer_id = int(response_json.get("peer_id") or 0)
    if not peer_id and row.get("chat_id"):
        peer_id = 2000000000 + int(row["chat_id"])
    if not peer_id:
        raise HTTPException(status_code=400, detail="В запуске не сохранён peer_id VK-чата")
    test_mode = bool(row.get("test_mode"))
    token = _vk_group_token(test_mode=test_mode)
    if not token:
        raise HTTPException(status_code=503, detail="VK_GROUP_TOKEN is not configured")
    stream_number = _clean(row.get("stream_number") or request_json.get("stream_number"))
    selected = _selected_people(stream_number, request_json.get("curator_id"))
    staff_people = _vk_staff_for_mode(selected, test_mode=test_mode)
    target_ids = await _resolve_vk_people_ids(staff_people, token)
    protected_ids = sorted(set(target_ids) | set(_vk_staff_registry().get("user_ids") or []))
    if not protected_ids:
        result = {"ok": True, "skipped": True, "reason": "no_staff_members", "run_id": row["id"], "peer_id": peer_id}
        response_json.update({"admin_result": result, "needs_attention": False, "followup_status": "ok", "detail": ""})
        _update_run(int(row["id"]), "ok", response_json)
        return result
    state = await _vk_admin_state(peer_id, protected_ids, token)
    present_ids = {
        int(item.get("id") or 0)
        for item in state.get("members", [])
        if int(item.get("id") or 0) > 0
    }
    missing_members = [user_id for user_id in target_ids if user_id not in present_ids]
    members_result = {
        "ok": not missing_members,
        "present": [user_id for user_id in target_ids if user_id in present_ids],
        "missing": missing_members,
        "staff_present": [user_id for user_id in target_ids if user_id in present_ids],
        "staff_pending_join": missing_members,
        "join_mode": "invite_link",
        "error": state.get("error"),
    }
    missing_admins = [user_id for user_id in (state.get("missing_admins") or []) if user_id in present_ids]
    api_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "already_admin", "state": state}
    if missing_admins:
        api_result = await _vk_try_api_admins(peer_id, missing_admins, token)
    final_state = await _vk_admin_state(peer_id, protected_ids, token)
    final_missing = [user_id for user_id in (final_state.get("missing_admins") or []) if user_id in present_ids]
    result = {
        "ok": not missing_members and not final_missing,
        "run_id": row["id"],
        "peer_id": peer_id,
        "members_result": members_result,
        "api": api_result,
        "state": final_state,
        "missing_admins": final_missing,
        "protected_staff_ids": protected_ids,
    }
    if final_missing:
        error = f"VK API не подтвердил роли администраторов: missing_admins={', '.join(map(str, final_missing))}"
        response_json.update({"admin_result": result, "needs_attention": True, "followup_status": "needs_admins", "detail": error})
        _update_run(int(row["id"]), "needs_admins", response_json, error=error)
    elif missing_members:
        error = f"Ожидается вход сотрудников: {', '.join(map(str, missing_members))}"
        response_json.update({"members_result": members_result, "admin_result": result, "needs_attention": True, "followup_status": "needs_members", "detail": error})
        _update_run(int(row["id"]), "needs_members", response_json, error=error)
    else:
        response_json.update({"admin_result": result, "needs_attention": False, "followup_status": "ok", "detail": ""})
        _update_run(int(row["id"]), "ok", response_json)
    return result


def _telegram_credentials() -> tuple[int, str, str]:
    api_id_raw = _clean(os.environ.get("TELEGRAM_API_ID"))
    api_hash = _clean(os.environ.get("TELEGRAM_API_HASH"))
    session_file = _telegram_session_file()
    if not api_id_raw or not api_hash:
        raise HTTPException(status_code=503, detail="Telegram credentials are not configured")
    return int(api_id_raw), api_hash, session_file


def _telegram_session_file() -> str:
    return _clean(os.environ.get("TELEGRAM_SESSION_FILE")) or str(_data_dir() / "telegram.session")


def _telegram_proxy_url() -> str:
    if telegram_mtproto_proxy_url is not None:
        return telegram_mtproto_proxy_url(os.environ.get("TELEGRAM_PROXY_URL"))
    return _clean(os.environ.get("TELEGRAM_MTPROTO_PROXY_URL") or os.environ.get("TELEGRAM_MTPROTO_PROXY") or os.environ.get("TELEGRAM_PROXY_URL"))


def _telegram_proxy_config() -> tuple[Any | None, tuple[str, int, str] | None]:
    raw = _telegram_proxy_url()
    if not raw:
        return None, None
    if telethon_proxy_config is not None:
        try:
            return telethon_proxy_config(raw)
        except ValueError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"} and parsed.netloc == "t.me" and parsed.path == "/proxy":
        query = parse_qs(parsed.query)
        server = _clean((query.get("server") or [""])[0])
        port_raw = _clean((query.get("port") or [""])[0])
        secret = _clean((query.get("secret") or [""])[0])
    else:
        server = _clean(parsed.hostname or "")
        port_raw = str(parsed.port or "")
        secret = _clean((parse_qs(parsed.query).get("secret") or [""])[0] or parsed.password or "")
    if not server or not port_raw or not secret:
        raise HTTPException(status_code=503, detail="Telegram MTProto proxy URL is invalid")
    try:
        port = int(port_raw)
    except ValueError:
        raise HTTPException(status_code=503, detail="Telegram MTProto proxy port is invalid")
    from telethon import connection
    return connection.ConnectionTcpMTProxyRandomizedIntermediate, (server, port, secret)


def _telegram_client(api_id: int, api_hash: str, session_file: str):
    from telethon import TelegramClient
    conn, proxy = _telegram_proxy_config()
    kwargs: dict[str, Any] = {"connection_retries": 1, "request_retries": 1, "timeout": 8}
    if conn and proxy:
        kwargs["connection"] = conn
        kwargs["proxy"] = proxy
    return TelegramClient(session_file, api_id, api_hash, **kwargs)


async def _telegram_connect(client: Any) -> None:
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            await asyncio.wait_for(client.connect(), timeout=40)
            return
        except Exception as exc:
            last_exc = exc
            if attempt == 0:
                await asyncio.sleep(3)
    if last_exc:
        raise last_exc


async def _telegram_auth_state(*, include_user: bool = False) -> dict[str, Any]:
    try:
        from telethon import TelegramClient
    except Exception as exc:
        return {"api": False, "authorized": False, "session_file": _telegram_session_file(), "error": f"Telethon is not installed: {exc}"}
    try:
        api_id, api_hash, session_file = _telegram_credentials()
    except HTTPException as exc:
        return {"api": False, "authorized": False, "session_file": _telegram_session_file(), "error": exc.detail}
    client = _telegram_client(api_id, api_hash, session_file)
    try:
        await _telegram_connect(client)
    except Exception as exc:
        return {"api": True, "authorized": False, "session_file": session_file, "proxy": bool(_telegram_proxy_url()), "error": f"Telegram connection failed: {_exc_text(exc)}"}
    try:
        authorized = await client.is_user_authorized()
        me = await client.get_me() if authorized else None
        state = {
            "api": True,
            "authorized": authorized,
            "session_file": session_file,
            "proxy": bool(_telegram_proxy_url()),
        }
        if include_user:
            state["user"] = {
                "id": getattr(me, "id", None),
                "username": getattr(me, "username", None),
                "phone": getattr(me, "phone", None),
            } if me else None
        return state
    except Exception as exc:
        return {"api": True, "authorized": False, "session_file": session_file, "proxy": bool(_telegram_proxy_url()), "error": f"Telegram status failed: {_exc_text(exc)}"}
    finally:
        await client.disconnect()


def _format_date_russian(date_str: str) -> str:
    months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return f"{dt.day} {months[dt.month - 1]}"
    except Exception:
        return date_str


async def _resolve_vk_target_id(target: str, token: str) -> int:
    raw = _clean(target)
    screen_name = _vk_screen_name(raw)
    if screen_name.isdigit():
        return int(screen_name)
    resolved = await _vk_method("utils.resolveScreenName", {"screen_name": screen_name}, token)
    if isinstance(resolved, dict) and resolved.get("type") == "user" and resolved.get("object_id"):
        return int(resolved["object_id"])
    users = await _vk_method("users.get", {"user_ids": screen_name}, token)
    if isinstance(users, list) and users:
        return int(users[0]["id"])
    raise HTTPException(status_code=400, detail="VK user cannot be resolved")


def _is_course_chat_title(title: Any) -> bool:
    return bool(COURSE_CHAT_TITLE_RE.search(_clean(title)))


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except Exception:
        return []
    return loaded if isinstance(loaded, list) else []


def _chat_title_meta(title: str) -> dict[str, str]:
    match = re.match(r"^\s*(\d+)\.\s*(\d{2}\.\d{2}\.\d{4})", title or "")
    if not match:
        return {"stream_number": "", "date_start": ""}
    return {"stream_number": match.group(1), "date_start": match.group(2)}


def _broadcast_empty_status(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "items": []}


def _broadcast_normalize_platform(value: Any) -> str:
    platform = _clean(value).lower()
    if platform in {"tg", "telegram"}:
        return "telegram"
    if platform == "vk":
        return "vk"
    return ""


def _broadcast_chat_key(platform: str, value: Any, title: str) -> str:
    marker = _clean(value) or _clean(title).lower()
    return f"{platform}:{marker}"


def _merge_broadcast_chat(candidates: dict[str, dict[str, Any]], item: dict[str, Any]) -> None:
    key = _clean(item.get("chat_key"))
    if not key:
        return
    existing = candidates.get(key)
    if not existing:
        candidates[key] = item
        return
    sources = set(_json_array(existing.get("sources")))
    sources.update(_json_array(item.get("sources")))
    existing["sources"] = sorted(sources)
    for field in ("peer_id", "chat_id", "title", "stream_number", "date_start", "link"):
        if not existing.get(field) and item.get(field):
            existing[field] = item[field]
    if item.get("can_send"):
        existing["can_send"] = True
        existing["status"] = "ready"
        existing["error"] = ""
    elif not existing.get("can_send") and item.get("error"):
        existing["error"] = item["error"]


def _runs_broadcast_chats(platforms: set[str]) -> list[dict[str, Any]]:
    _ensure_db()
    items: list[dict[str, Any]] = []
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM runs WHERE status='ok' ORDER BY id DESC").fetchall()]
    for row in rows:
        platform = _broadcast_normalize_platform(row.get("platform"))
        title = _clean(row.get("title"))
        if platform not in platforms or not _is_course_chat_title(title):
            continue
        response = _json_object(row.get("response_json"))
        meta = _chat_title_meta(title)
        if platform == "vk":
            chat_id = _clean(row.get("chat_id") or response.get("chat_id"))
            peer_id = _clean(response.get("peer_id"))
            if not peer_id and chat_id.isdigit():
                peer_id = str(2000000000 + int(chat_id))
            key_value = peer_id or chat_id or title
            items.append({
                "platform": "vk",
                "chat_key": _broadcast_chat_key("vk", key_value, title),
                "title": title,
                "stream_number": meta["stream_number"],
                "date_start": meta["date_start"],
                "peer_id": peer_id,
                "chat_id": chat_id,
                "link": _clean(row.get("link")),
                "sources": ["runs"],
                "can_send": bool(peer_id),
                "status": "ready" if peer_id else "needs_live_scan",
                "error": "" if peer_id else "peer_id not stored in runs",
            })
        elif platform == "telegram":
            chat_id = _clean(row.get("chat_id") or response.get("chat_id") or response.get("channel_id"))
            items.append({
                "platform": "telegram",
                "chat_key": _broadcast_chat_key("telegram", chat_id or title, title),
                "title": title,
                "stream_number": meta["stream_number"],
                "date_start": meta["date_start"],
                "peer_id": "",
                "chat_id": chat_id,
                "link": _clean(row.get("link")),
                "sources": ["runs"],
                "can_send": False,
                "status": "needs_live_scan",
                "error": "Telegram entity is resolved by live scan",
            })
    return items


async def _scan_vk_broadcast_chats(limit: int = 500) -> dict[str, Any]:
    token = _vk_group_token()
    if not token:
        return _broadcast_empty_status("VK_GROUP_TOKEN is not configured")
    items: list[dict[str, Any]] = []
    offset = 0
    while offset < limit:
        data = await _vk_method("messages.getConversations", {"count": min(200, limit - offset), "offset": offset}, token)
        if isinstance(data, dict) and "error" in data:
            return {"ok": False, "reason": data["error"], "items": items}
        conversations = data.get("items", []) if isinstance(data, dict) else []
        if not conversations:
            break
        for item in conversations:
            conv = item.get("conversation", {}) or {}
            peer = conv.get("peer", {}) or {}
            peer_id = int(peer.get("id", 0) or 0)
            title = _clean((conv.get("chat_settings") or {}).get("title"))
            if peer_id <= 2000000000 or not _is_course_chat_title(title):
                continue
            meta = _chat_title_meta(title)
            chat_id = str(peer_id - 2000000000)
            items.append({
                "platform": "vk",
                "chat_key": _broadcast_chat_key("vk", peer_id, title),
                "title": title,
                "stream_number": meta["stream_number"],
                "date_start": meta["date_start"],
                "peer_id": str(peer_id),
                "chat_id": chat_id,
                "link": "",
                "sources": ["live"],
                "can_send": True,
                "status": "ready",
                "error": "",
            })
        if len(conversations) < 200:
            break
        offset += len(conversations)
    return {"ok": True, "items": items}


async def _scan_tg_broadcast_chats(limit: int = 500) -> dict[str, Any]:
    try:
        from telethon import TelegramClient
    except Exception as exc:
        return _broadcast_empty_status(f"Telethon is not installed: {exc}")
    try:
        api_id, api_hash, session_file = _telegram_credentials()
    except HTTPException as exc:
        return _broadcast_empty_status(str(exc.detail))
    client = _telegram_client(api_id, api_hash, session_file)
    try:
        await _telegram_connect(client)
    except Exception as exc:
        return _broadcast_empty_status(f"Telegram connection failed: {_exc_text(exc)}")
    try:
        if not await client.is_user_authorized():
            return _broadcast_empty_status("Telegram session is not authorized")
        items: list[dict[str, Any]] = []
        async for dialog in client.iter_dialogs(limit=limit):
            title = _clean(getattr(dialog, "name", ""))
            if not _is_course_chat_title(title):
                continue
            entity = dialog.entity
            chat_id = str(getattr(entity, "id", "") or "")
            meta = _chat_title_meta(title)
            items.append({
                "platform": "telegram",
                "chat_key": _broadcast_chat_key("telegram", chat_id or title, title),
                "title": title,
                "stream_number": meta["stream_number"],
                "date_start": meta["date_start"],
                "peer_id": "",
                "chat_id": chat_id,
                "link": "",
                "sources": ["live"],
                "can_send": True,
                "status": "ready",
                "error": "",
            })
        return {"ok": True, "items": items}
    finally:
        await client.disconnect()


async def _broadcast_chat_candidates(platforms: set[str], *, limit: int = 500) -> dict[str, Any]:
    candidates: dict[str, dict[str, Any]] = {}
    scan_status: dict[str, Any] = {}
    for item in _runs_broadcast_chats(platforms):
        _merge_broadcast_chat(candidates, item)
    if "vk" in platforms:
        vk = await _scan_vk_broadcast_chats(limit=limit)
        scan_status["vk"] = {k: v for k, v in vk.items() if k != "items"}
        for item in vk.get("items", []):
            _merge_broadcast_chat(candidates, item)
    if "telegram" in platforms:
        tg = await _scan_tg_broadcast_chats(limit=limit)
        scan_status["telegram"] = {k: v for k, v in tg.items() if k != "items"}
        # Merge Telegram runs by title because runs often do not store entity id.
        title_index = {
            (item.get("platform"), _clean(item.get("title")).lower()): key
            for key, item in candidates.items()
            if item.get("platform") == "telegram"
        }
        for item in tg.get("items", []):
            title_key = ("telegram", _clean(item.get("title")).lower())
            old_key = title_index.get(title_key)
            if old_key and old_key != item["chat_key"]:
                existing = candidates.pop(old_key)
                item["sources"] = sorted(set(_json_array(existing.get("sources"))) | set(_json_array(item.get("sources"))))
            _merge_broadcast_chat(candidates, item)
    items = sorted(candidates.values(), key=lambda x: (x.get("platform", ""), x.get("date_start", ""), x.get("stream_number", ""), x.get("title", "")))
    return {"ok": True, "items": items, "status": scan_status}


def _broadcast_filter_selection(items: list[dict[str, Any]], mode: str, selected: set[str], excluded: set[str]) -> list[dict[str, Any]]:
    if mode == "all_except":
        return [item for item in items if item.get("chat_key") not in excluded]
    return [item for item in items if item.get("chat_key") in selected]


def _broadcast_message_counts(broadcast_id: int) -> dict[str, int]:
    with _db() as db:
        rows = db.execute("SELECT status, COUNT(*) count FROM broadcast_messages WHERE broadcast_id=? GROUP BY status", (broadcast_id,)).fetchall()
    return {row["status"]: int(row["count"]) for row in rows}


def _broadcast_delay_bounds(data: dict[str, Any]) -> tuple[int, int, str]:
    speed = _clean(data.get("speed") or "balanced").lower()
    profiles = {
        "fast": (1200, 2500),
        "balanced": (2500, 5000),
        "safe": (5000, 9000),
    }
    if speed not in profiles:
        speed = "balanced"
    min_ms, max_ms = profiles[speed]
    try:
        custom_min = int(data.get("delay_min_ms") or 0)
        custom_max = int(data.get("delay_max_ms") or 0)
        if custom_min >= 500 and custom_max >= custom_min:
            min_ms, max_ms = min(custom_min, 60000), min(custom_max, 120000)
            speed = "custom"
    except Exception:
        pass
    return min_ms, max_ms, speed


async def _broadcast_sleep(delay_bounds: tuple[int, int], *, index: int, total: int) -> None:
    if index >= total - 1:
        return
    min_ms, max_ms = delay_bounds
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)


async def _send_vk_broadcast_message(chat: dict[str, Any], message: str) -> tuple[bool, str, str]:
    token = _vk_group_token()
    peer_id = _clean(chat.get("peer_id"))
    if not token:
        return False, "", "VK_GROUP_TOKEN is not configured"
    if not peer_id:
        return False, "", "peer_id is empty"
    last_error = ""
    for attempt in range(3):
        response = await _vk_method("messages.send", {"peer_id": peer_id, "message": message, "random_id": random.randint(1, 2**31 - 1)}, token)
        if isinstance(response, dict) and "error" in response:
            error = response["error"]
            code = int(error.get("error_code", 0) or 0) if isinstance(error, dict) else 0
            last_error = str(error)
            if code in {6, 9, 10} and attempt < 2:
                await asyncio.sleep(3 + attempt * 5)
                continue
            return False, "", last_error
        message_id, cmid = _vk_message_reference(response)
        if message_id or cmid:
            return True, str(message_id or cmid), ""
        last_error = "VK did not return message id"
    return False, "", last_error


async def _delete_vk_broadcast_message(peer_id: str, message_id: str) -> tuple[bool, str]:
    token = _vk_group_token()
    if not token:
        return False, "VK_GROUP_TOKEN is not configured"
    if not message_id:
        return False, "message_id is empty"
    params = {"message_ids": message_id, "delete_for_all": 1}
    if peer_id:
        params["peer_id"] = peer_id
    response = await _vk_method("messages.delete", params, token)
    if isinstance(response, dict) and "error" in response:
        return False, str(response["error"])
    return True, ""


async def _send_tg_broadcast_message(chat: dict[str, Any], message: str) -> tuple[bool, str, str]:
    try:
        from telethon import TelegramClient
    except Exception as exc:
        return False, "", f"Telethon is not installed: {exc}"
    try:
        api_id, api_hash, session_file = _telegram_credentials()
    except HTTPException as exc:
        return False, "", str(exc.detail)
    title = _clean(chat.get("title"))
    chat_id = _clean(chat.get("chat_id"))
    client = _telegram_client(api_id, api_hash, session_file)
    try:
        await _telegram_connect(client)
        if not await client.is_user_authorized():
            return False, "", "Telegram session is not authorized"
        entity = None
        async for dialog in client.iter_dialogs(limit=500):
            entity_id = str(getattr(dialog.entity, "id", "") or "")
            dialog_title = _clean(getattr(dialog, "name", ""))
            if (chat_id and entity_id == chat_id) or (title and dialog_title == title):
                entity = dialog.entity
                break
        if entity is None:
            return False, "", "Telegram chat was not found by live scan"
        sent = await client.send_message(entity, message)
        return True, str(getattr(sent, "id", "") or ""), ""
    except Exception as exc:
        return False, "", str(exc)
    finally:
        await client.disconnect()


async def _delete_tg_broadcast_message(chat_title: str, chat_id: str, message_id: str) -> tuple[bool, str]:
    try:
        from telethon import TelegramClient
    except Exception as exc:
        return False, f"Telethon is not installed: {exc}"
    if not message_id:
        return False, "message_id is empty"
    try:
        api_id, api_hash, session_file = _telegram_credentials()
    except HTTPException as exc:
        return False, str(exc.detail)
    client = _telegram_client(api_id, api_hash, session_file)
    try:
        await _telegram_connect(client)
        if not await client.is_user_authorized():
            return False, "Telegram session is not authorized"
        entity = None
        async for dialog in client.iter_dialogs(limit=500):
            entity_id = str(getattr(dialog.entity, "id", "") or "")
            dialog_title = _clean(getattr(dialog, "name", ""))
            if (chat_id and entity_id == chat_id) or (chat_title and dialog_title == chat_title):
                entity = dialog.entity
                break
        if entity is None:
            return False, "Telegram chat was not found by live scan"
        await client.delete_messages(entity, [int(message_id)])
        return True, ""
    except Exception as exc:
        return False, str(exc)
    finally:
        await client.disconnect()


async def _telegram_broadcast_entity_map(client: Any, chats: list[dict[str, Any]]) -> dict[str, Any]:
    ids = {_clean(chat.get("chat_id")) for chat in chats if _clean(chat.get("chat_id"))}
    titles = {_clean(chat.get("title")) for chat in chats if _clean(chat.get("title"))}
    found: dict[str, Any] = {}
    async for dialog in client.iter_dialogs(limit=1000):
        entity_id = str(getattr(dialog.entity, "id", "") or "")
        dialog_title = _clean(getattr(dialog, "name", ""))
        if entity_id in ids:
            found[f"id:{entity_id}"] = dialog.entity
        if dialog_title in titles:
            found[f"title:{dialog_title}"] = dialog.entity
    return found


async def _send_tg_with_entity(client: Any, entity: Any, message: str) -> tuple[bool, str, str]:
    for attempt in range(2):
        try:
            sent = await client.send_message(entity, message)
            return True, str(getattr(sent, "id", "") or ""), ""
        except Exception as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0)
            if exc.__class__.__name__ == "FloodWaitError" and wait_seconds and wait_seconds <= 180 and attempt == 0:
                await asyncio.sleep(wait_seconds + 2)
                continue
            return False, "", _exc_text(exc)
    return False, "", "Telegram send failed"


def _recorded_vk_course_chats() -> list[dict[str, Any]]:
    _ensure_db()
    with _db() as db:
        rows = [
            dict(row)
            for row in db.execute(
                "SELECT id,title,chat_id,link,response_json,test_mode,status,course_key,stream_number FROM runs WHERE platform=? ORDER BY id DESC",
                ("vk",),
            ).fetchall()
        ]
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    for row in rows:
        title = _clean(row.get("title"))
        if not _is_course_chat_title(title):
            continue
        response = _json_object(row.get("response_json"))
        peer_id = int(response.get("peer_id") or 0)
        chat_id = int(row.get("chat_id") or response.get("chat_id") or 0)
        if peer_id <= 2000000000 and chat_id > 0:
            peer_id = 2000000000 + chat_id
        if peer_id <= 2000000000 or peer_id in seen:
            continue
        seen.add(peer_id)
        result.append(
            {
                "run_id": int(row["id"]),
                "peer_id": peer_id,
                "chat_id": peer_id - 2000000000,
                "title": title,
                "test_mode": bool(row.get("test_mode")),
                "run_status": _clean(row.get("status")),
                "course_key": _clean(row.get("course_key")),
                "stream_number": _clean(row.get("stream_number")),
                "link": _clean(row.get("link"))[:2000],
                "community_owned": int(response.get("owner_group_id") or 0) == group_id,
                "welcome_cmid": int(response.get("welcome_cmid") or 0),
                "pin_watchdog_suspended": bool(
                    isinstance(response.get("pin_watchdog"), dict)
                    and response["pin_watchdog"].get("suspended_by_admin")
                ),
                "url": f"https://vk.com/gim{group_id}?sel=c{peer_id - 2000000000}" if group_id else "",
            }
        )
    return result


def _senler_course_chat_config() -> dict[str, str] | None:
    token = _clean(os.environ.get("SENLER_ACCESS_TOKEN"))
    group_id = _clean(os.environ.get("SENLER_GROUP_ID"))
    subscription_id = _clean(
        os.environ.get("SENLER_COURSE_CHAT_SUBSCRIPTION_ID") or SENLER_COURSE_CHAT_SUBSCRIPTION_ID
    )
    if not token or not group_id or not subscription_id.isdigit():
        return None
    return {"token": token, "group_id": group_id, "subscription_id": subscription_id}


async def _senler_course_chat_request(payload: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(f"{SENLER_API_BASE}/subscribers/add", json=payload)
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not response.is_success or not isinstance(body, dict) or not body.get("success"):
        raise RuntimeError(_clean(body.get("error") if isinstance(body, dict) else "") or "Senler subscribers/add failed")
    return body


async def _senler_course_chat_members(payload: dict[str, Any]) -> set[int]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(f"{SENLER_API_BASE}/subscribers/get", json=payload)
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not response.is_success or not isinstance(body, dict) or not body.get("success"):
        raise RuntimeError(_clean(body.get("error") if isinstance(body, dict) else "") or "Senler subscribers/get failed")
    return {int(item.get("vk_user_id") or 0) for item in body.get("items", []) if int(item.get("vk_user_id") or 0) > 0}


async def _sync_vk_users_to_senler(vk_user_ids: list[int] | set[int]) -> dict[str, Any]:
    ids = sorted({int(value) for value in vk_user_ids if int(value) > 0})
    config = _senler_course_chat_config()
    if not ids:
        return {"ok": True, "status": "empty", "users": 0}
    if config is None:
        return {"ok": False, "status": "not_configured", "users": len(ids)}
    accepted = 0
    errors: list[str] = []
    for index in range(0, len(ids), 100):
        batch = ids[index : index + 100]
        try:
            await _senler_course_chat_request(
                {
                    "access_token": config["token"],
                    "group_id": config["group_id"],
                    "subscription_id": config["subscription_id"],
                    "vk_user_id": batch,
                    "v": "2",
                }
            )
            accepted += len(batch)
        except Exception as exc:
            errors.append(_exc_text(exc))
    verified: set[int] = set()
    if not errors:
        try:
            verified = await _senler_course_chat_members(
                {
                    "access_token": config["token"],
                    "group_id": config["group_id"],
                    "subscription_id": [int(config["subscription_id"])],
                    "vk_user_id": ids,
                    "v": "2",
                    "count": 100,
                }
            )
        except Exception as exc:
            errors.append(_exc_text(exc))
    missing = sorted(set(ids) - verified)
    result = {
        "ok": not errors and not missing,
        "status": "synced" if not errors and not missing else "partial_error",
        "users": len(ids),
        "accepted": accepted,
        "verified": len(verified),
        "unresolved_vk_ids": missing,
        "subscription_id": config["subscription_id"],
        "errors": errors,
    }
    if errors or missing:
        _log("warning", "Senler course-chat sync incomplete users=%s verified=%s missing=%s errors=%s", len(ids), len(verified), missing, errors)
    return result


async def _vk_course_chat_member_ids(peer_ids: list[int] | None = None) -> dict[str, Any]:
    token = _vk_group_token()
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    if not token or group_id <= 0:
        return {"ok": False, "status": "not_configured", "users": []}
    wanted = {int(value) for value in (peer_ids or []) if int(value) > 2000000000}
    users: set[int] = set()
    chats = 0
    errors: list[dict[str, Any]] = []
    for chat in _recorded_vk_course_chats():
        if not chat.get("community_owned") or chat.get("test_mode"):
            continue
        peer_id = int(chat["peer_id"])
        if wanted and peer_id not in wanted:
            continue
        response = await _vk_method(
            "messages.getConversationMembers",
            {"peer_id": peer_id, "count": 1000, "group_id": group_id},
            token,
        )
        error = _vk_error_summary(response)
        if error:
            errors.append({"peer_id": peer_id, "error": error})
            continue
        chats += 1
        users.update(
            int(item.get("member_id") or 0)
            for item in response.get("items", [])
            if int(item.get("member_id") or 0) > 0
        )
    return {"ok": not errors, "status": "ready" if not errors else "partial_error", "chats": chats, "users": sorted(users), "errors": errors}


async def _sync_vk_course_chat_members_to_senler(peer_ids: list[int] | None = None) -> dict[str, Any]:
    members = await _vk_course_chat_member_ids(peer_ids)
    if not members.get("ok") and not members.get("users"):
        return members
    return {**members, "senler": await _sync_vk_users_to_senler(members.get("users") or [])}


def _vk_error_summary(response: Any) -> dict[str, Any] | None:
    if not isinstance(response, dict) or "error" not in response:
        return None
    error = response.get("error") or {}
    if isinstance(error, dict):
        return {"code": error.get("error_code"), "message": _clean(error.get("error_msg"))}
    return {"code": None, "message": _clean(error)}


async def _vk_course_chat_inventory(target_user_id: int | None = None) -> dict[str, Any]:
    token = _vk_group_token()
    if not token:
        raise HTTPException(status_code=503, detail="VK_GROUP_TOKEN is not configured")
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    items: list[dict[str, Any]] = []
    for chat in _recorded_vk_course_chats():
        if not chat.get("community_owned"):
            item = dict(chat)
            item.update(
                {
                    "accessible": False,
                    "status": "legacy_inaccessible",
                    "error": {
                        "code": 927,
                        "message": "Чат создан старым пользовательским контуром и недоступен сообществу",
                    },
                }
            )
            items.append(item)
            continue
        members = await _vk_method(
            "messages.getConversationMembers",
            {"peer_id": chat["peer_id"], "count": 1000, "group_id": group_id},
            token,
        )
        error = _vk_error_summary(members)
        item = dict(chat)
        if error:
            item.update({"accessible": False, "status": "inaccessible", "error": error})
            items.append(item)
            continue
        member_rows = members.get("items", []) if isinstance(members, dict) else []
        item["accessible"] = True
        item["members_count"] = len(member_rows)
        if target_user_id:
            target = next(
                (row for row in member_rows if int(row.get("member_id") or 0) == target_user_id),
                None,
            )
            item["target_present"] = bool(target)
            item["target_role"] = _vk_member_role(target or {}) or ("member" if target else "")
        item["status"] = "ready"
        items.append(item)
    return {
        "ok": True,
        "source": "recorded_course_chats",
        "target": target_user_id,
        "accessible": sum(1 for item in items if item.get("accessible")),
        "inaccessible": sum(1 for item in items if not item.get("accessible")),
        "items": items,
    }


async def _restore_vk_course_pin_manual(peer_id: int, *, dry_run: bool) -> dict[str, Any]:
    row = _vk_owned_run(peer_id)
    if row is None:
        raise HTTPException(status_code=404, detail="VK chat is not recorded or is not community-owned")
    response = dict(row.get("response") or {})
    cmid = int(response.get("welcome_cmid") or 0)
    if cmid <= 0:
        raise HTTPException(status_code=409, detail="В запуске не сохранён закреп курса")
    token = _vk_group_token(test_mode=bool(row.get("test_mode")))
    if not token:
        raise HTTPException(status_code=503, detail="VK_GROUP_TOKEN is not configured")
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    current = await _vk_method(
        "messages.getConversationsById",
        {"peer_ids": str(peer_id), "group_id": group_id},
        token,
    )
    current_cmid = int(_vk_conversation_pin_map(current).get(peer_id) or 0)
    legacy_photo_cmid = int(response.get("welcome_photo_cmid") or 0)
    needs_photo_upgrade = legacy_photo_cmid > 0 and legacy_photo_cmid != cmid
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "peer_id": peer_id,
            "current_cmid": current_cmid,
            "target_cmid": cmid,
            "status": (
                "would_add_photo"
                if needs_photo_upgrade
                else ("already_pinned" if current_cmid == cmid else "would_restore")
            ),
        }
    photo_added = False
    if needs_photo_upgrade:
        request_json = dict(row.get("request") or _json_dict(row.get("request_json")))
        stream_number = _clean(row.get("stream_number") or request_json.get("stream_number"))
        date_start = _clean(
            row.get("date_start") or request_json.get("date_start") or request_json.get("start_date")
        )
        course = _course_by_input(
            row.get("course_key") or request_json.get("course_type") or request_json.get("course_choice")
        )
        selected = _selected_people(stream_number, request_json.get("curator_id"))
        welcome_text = _render_template(
            "vk_test_welcome" if bool(row.get("test_mode")) else "vk_welcome",
            course=course,
            stream_number=stream_number,
            date_start=date_start,
            selected=selected,
            platform="vk",
        )
        welcome_photo = _asset_path("welcome_message_photo.jpg")
        if not welcome_photo:
            raise HTTPException(status_code=500, detail="Изображение закрепа не найдено")
        attachment = _clean(await _upload_vk_message_photo(peer_id, welcome_photo, token))
        if not attachment:
            raise HTTPException(status_code=502, detail="VK не загрузил изображение закрепа")
        welcome_response = await _vk_method(
            "messages.send",
            {
                "peer_id": peer_id,
                "message": welcome_text,
                "attachment": attachment,
                "random_id": random.randint(1, 2**31 - 1),
            },
            token,
        )
        _vk_require_success("messages.send welcome with photo", welcome_response)
        message_id, upgraded_cmid = _vk_message_reference(welcome_response)
        if not message_id and not upgraded_cmid:
            for _attempt in range(3):
                await asyncio.sleep(0.3)
                message_id, upgraded_cmid = await _find_vk_community_chat_message(
                    peer_id, token, text=welcome_text, attachment_type="photo"
                )
                if upgraded_cmid:
                    break
        if not message_id and not upgraded_cmid:
            raise HTTPException(status_code=502, detail="VK не вернул идентификатор закрепа")
        cmid = int(upgraded_cmid or 0)
        if cmid <= 0:
            raise HTTPException(status_code=502, detail="VK не вернул локальный идентификатор закрепа")
        response["welcome_message_id"] = message_id
        response["welcome_cmid"] = cmid
        response["welcome_photo_sent"] = True
        response["welcome_photo_cmid"] = cmid
        response["welcome_message_has_photo"] = True
        photo_added = True
    if current_cmid != cmid:
        pin_response = await _vk_method(
            "messages.pin", {"peer_id": peer_id, "cmid": cmid}, token
        )
        if isinstance(pin_response, dict) and "error" in pin_response:
            raise HTTPException(status_code=502, detail="VK не восстановил закреп курса")
        verify = await _vk_method(
            "messages.getConversationsById",
            {"peer_ids": str(peer_id), "group_id": group_id},
            token,
        )
        if _vk_conversation_pin_map(verify).get(peer_id) != cmid:
            raise HTTPException(status_code=502, detail="VK не подтвердил закреп курса")
    watchdog = response.get("pin_watchdog") if isinstance(response.get("pin_watchdog"), dict) else {}
    response["pin_watchdog"] = {
        **watchdog,
        "restored_count": int(watchdog.get("restored_count") or 0)
        + (1 if current_cmid != cmid else 0),
        "last_restored_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "cmid": cmid,
        "trigger": "manual",
        "suspended_by_admin": False,
        "suspended_by_admin_id": 0,
        "suspended_at": "",
    }
    response["welcome_pinned"] = True
    _persist_vk_event_result(row, response)
    return {
        "ok": True,
        "dry_run": False,
        "peer_id": peer_id,
        "current_cmid": cmid,
        "target_cmid": cmid,
        "status": "photo_added" if photo_added else ("already_pinned" if current_cmid == cmid else "restored"),
    }


async def _manage_vk_course_chats(target: str, *, action: str, dry_run: bool = True) -> dict[str, Any]:
    token = _vk_group_token()
    if not token:
        raise HTTPException(status_code=503, detail="VK_GROUP_TOKEN is not configured")
    if action not in {"grant_admin", "revoke_admin", "remove"}:
        raise HTTPException(status_code=400, detail="action must be grant_admin, revoke_admin or remove")
    user_id = await _resolve_vk_target_id(target, token)
    inventory = await _vk_course_chat_inventory(user_id)
    result_items: list[dict[str, Any]] = []
    admin_roles = {"admin", "administrator", "creator"}
    for source in inventory["items"]:
        item = dict(source)
        if not item.get("accessible"):
            result_items.append(item)
            continue
        present = bool(item.get("target_present"))
        is_admin = _clean(item.get("target_role")).lower() in admin_roles
        if not present:
            item["status"] = "not_member"
            result_items.append(item)
            continue
        if action == "grant_admin" and is_admin:
            item["status"] = "already_admin"
            result_items.append(item)
            continue
        if action == "revoke_admin" and not is_admin:
            item["status"] = "already_member"
            result_items.append(item)
            continue
        planned = {
            "grant_admin": "would_grant_admin",
            "revoke_admin": "would_revoke_admin",
            "remove": "would_remove",
        }[action]
        if dry_run:
            item["status"] = planned
            result_items.append(item)
            continue
        if action == "remove":
            response = await _vk_method(
                "messages.removeChatUser",
                {"chat_id": item["chat_id"], "member_id": user_id},
                token,
            )
        else:
            response = await _vk_method(
                "messages.setMemberRole",
                {
                    "peer_id": item["peer_id"],
                    "member_id": user_id,
                    "role": "admin" if action == "grant_admin" else "member",
                },
                token,
            )
        error = _vk_error_summary(response)
        if error:
            item.update({"status": "error", "error": error})
            result_items.append(item)
            await asyncio.sleep(0.4)
            continue
        await asyncio.sleep(0.4)
        verify = await _vk_method(
            "messages.getConversationMembers",
            {"peer_id": item["peer_id"], "count": 1000},
            token,
        )
        verify_rows = verify.get("items", []) if isinstance(verify, dict) and "error" not in verify else []
        member = next((row for row in verify_rows if int(row.get("member_id") or 0) == user_id), None)
        verified_role = _vk_member_role(member or {}) or ("member" if member else "")
        verified_admin = verified_role.lower() in admin_roles
        verified = (
            (action == "remove" and member is None)
            or (action == "grant_admin" and verified_admin)
            or (action == "revoke_admin" and member is not None and not verified_admin)
        )
        item.update(
            {
                "status": {"grant_admin": "admin_granted", "revoke_admin": "admin_revoked", "remove": "removed"}[action]
                if verified
                else "verify_failed",
                "target_present": member is not None,
                "target_role": verified_role,
            }
        )
        result_items.append(item)
    return {
        "ok": not any(item.get("status") in {"error", "verify_failed"} for item in result_items),
        "platform": "vk",
        "target": user_id,
        "action": action,
        "dry_run": dry_run,
        "accessible": inventory["accessible"],
        "inaccessible": inventory["inaccessible"],
        "items": result_items,
    }


async def _remove_vk_from_course_chats(target: str, *, dry_run: bool = True, limit: int = 200) -> dict[str, Any]:
    del limit
    return await _manage_vk_course_chats(target, action="remove", dry_run=dry_run)


async def _remove_tg_from_course_chats(target: str, *, dry_run: bool = True, limit: int = 200) -> dict[str, Any]:
    try:
        from telethon import TelegramClient
        from telethon.tl import functions, types
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Telethon is not installed: {exc}")
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    try:
        if not await client.is_user_authorized():
            raise HTTPException(status_code=401, detail="Telegram session is not authorized")
        entity = await client.get_entity(target)
        touched: list[dict[str, Any]] = []
        async for dialog in client.iter_dialogs(limit=limit):
            title = getattr(dialog, "name", "") or ""
            if not _is_course_chat_title(title):
                continue
            try:
                await client.get_permissions(dialog.entity, entity)
                present = True
            except Exception:
                present = False
            if not present:
                touched.append({"platform": "telegram", "title": title, "chat_id": getattr(dialog.entity, "id", None), "status": "not_found", "present": False})
                continue
            if not dry_run:
                banned = types.ChatBannedRights(until_date=None, view_messages=True)
                await client(functions.channels.EditBannedRequest(channel=dialog.entity, participant=entity, banned_rights=banned))
            touched.append({"platform": "telegram", "title": title, "chat_id": getattr(dialog.entity, "id", None), "status": "would_remove" if dry_run else "removed", "present": True})
        return {"ok": True, "platform": "telegram", "target": target, "dry_run": dry_run, "items": touched}
    finally:
        await client.disconnect()


def service_flow_setup() -> dict[str, Any]:
    _ensure_db()
    with _db() as db:
        courses = [dict(row) for row in db.execute("SELECT * FROM courses WHERE enabled=1 ORDER BY choice,key").fetchall()]
    return {
        "courses": courses,
        "teachers": [
            {
                "id": int(person["id"]),
                "name": _clean(person.get("name")),
                "offer_id": int(person.get("offer_id") or 0),
                "vk": _clean(person.get("vk_id")),
                "telegram": _clean(person.get("tg_ref")),
            }
            for person in _people("kurator", enabled=True)
        ],
    }


def service_flow_catalog() -> dict[str, Any]:
    _ensure_db()
    with _db() as db:
        db.row_factory = sqlite3.Row
        rows = [
            dict(row)
            for row in db.execute(
                """
                SELECT * FROM runs
                WHERE test_mode=0 AND platform IN ('vk','telegram')
                ORDER BY created_at,id
                """
            ).fetchall()
        ]
    people = {int(item["id"]): item for item in _people(enabled=False)}
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        course_key = _course_key(row.get("course_key"))
        stream = _clean(row.get("stream_number"))
        if not course_key or not stream:
            continue
        request_data = _json_dict(row.get("request_json"))
        response_data = _json_dict(row.get("response_json"))
        curator_id = int(request_data.get("curator_id") or response_data.get("curator_id") or 0)
        teacher = people.get(curator_id) or {}
        item = grouped.setdefault(
            (course_key, stream),
            {
                "course_key": course_key,
                "course": "Щенок" if course_key == "puppy" else "Собака",
                "stream": stream,
                "date_start": _clean(row.get("date_start")),
                "teacher_id": curator_id,
                "teacher": _clean(teacher.get("name")),
                "offer_id": int(teacher.get("offer_id") or 0),
                "vk": None,
                "telegram": None,
            },
        )
        if curator_id:
            item.update(
                {
                    "teacher_id": curator_id,
                    "teacher": _clean(teacher.get("name")),
                    "offer_id": int(teacher.get("offer_id") or 0),
                }
            )
        item["date_start"] = _clean(row.get("date_start") or item.get("date_start"))
        item[row["platform"]] = {
            "run_id": int(row.get("id") or 0),
            "status": _clean(row.get("status")),
            "title": _clean(row.get("title")),
            "link": _clean(row.get("link")),
            "chat_id": _clean(row.get("chat_id")),
            "error": _clean(row.get("error")),
        }
        if row["platform"] == "vk":
            peer_id = int(response_data.get("peer_id") or 0)
            owner_group_id = int(response_data.get("owner_group_id") or 0)
            if peer_id > 2000000000 and owner_group_id:
                item["vk_admin_url"] = f"https://vk.ru/gim{owner_group_id}?sel=c{peer_id - 2000000000}"
    items = list(grouped.values())
    items.sort(key=lambda item: (0 if item["course_key"] == "puppy" else 1, -int(item["stream"]) if item["stream"].isdigit() else 0))
    return {"ok": True, "items": items, **service_flow_setup()}


def service_notification_chat_context(
    *, platform: str, chat_id: str = "", title: str = "",
) -> dict[str, Any]:
    """Resolve one real course chat and its curator for Nexus notifications.

    VK callbacks identify a conversation by peer id while Telegram events are
    most reliably matched by the title saved when the chat was created.  Only
    successful, non-test runs are eligible; an arbitrary group conversation
    must never become a sales notification by accident.
    """
    platform = _clean(platform).lower()
    chat_id = _clean(chat_id)
    title = _clean(title).rstrip(".").casefold()
    if platform not in {"vk", "telegram"} or (not chat_id and not title):
        return {"ok": True, "found": False}
    _ensure_db()
    with _db() as db:
        db.row_factory = sqlite3.Row
        people = {int(row["id"]): dict(row) for row in db.execute("SELECT * FROM people")}
        rows = db.execute(
            """SELECT * FROM runs
               WHERE platform=? AND test_mode=0
               ORDER BY created_at DESC,id DESC""",
            (platform,),
        ).fetchall()
    for raw in rows:
        row = dict(raw)
        if _clean(row.get("status")) not in {"ok", "needs_members", "needs_vk_web_admins", "attention"}:
            continue
        request_data = _json_dict(row.get("request_json"))
        response_data = _json_dict(row.get("response_json"))
        stored_title = _clean(row.get("title")).rstrip(".")
        stored_id = _clean(row.get("chat_id") or response_data.get("chat_id") or response_data.get("channel_id"))
        ids = {stored_id} if stored_id else set()
        if platform == "vk":
            peer_id = int(response_data.get("peer_id") or 0)
            if peer_id:
                ids.add(str(peer_id))
            if stored_id.isdigit():
                ids.add(str(2_000_000_000 + int(stored_id)))
        matched = bool(chat_id and chat_id in ids) or bool(title and stored_title.casefold() == title)
        if not matched:
            continue
        curator_id = int(request_data.get("curator_id") or response_data.get("curator_id") or 0)
        curator = people.get(curator_id) or {}
        owner_group_id = int(response_data.get("owner_group_id") or _clean(os.environ.get("VK_GROUP_ID")) or 0)
        chat_url = _clean(row.get("link") or response_data.get("group_link"))
        if platform == "vk" and stored_id.isdigit() and owner_group_id:
            chat_url = f"https://vk.com/gim{owner_group_id}?sel=c{stored_id}"
        return {
            "ok": True,
            "found": True,
            "platform": platform,
            "chat_id": chat_id or stored_id,
            "title": stored_title,
            "chat_url": chat_url,
            "course_key": _course_key(row.get("course_key")),
            "stream_number": _clean(row.get("stream_number")),
            "curator_id": curator_id,
            "curator_name": _clean(curator.get("name")),
            "curator_vk_id": re.sub(r"\D+", "", _clean(curator.get("vk_id"))),
            "curator_vk_ref": _vk_screen_name(curator.get("vk_id") or curator.get("vk_mention")),
            "curator_telegram": _clean(curator.get("tg_ref")).lstrip("@").casefold(),
        }
    return {"ok": True, "found": False}


def service_set_flow_curator(*, course_key: str, stream_number: str, teacher_id: int) -> dict[str, Any]:
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    teacher = next(
        (item for item in service_flow_setup()["teachers"] if int(item["id"]) == int(teacher_id)),
        None,
    )
    if not teacher or not int(teacher.get("offer_id") or 0):
        raise ValueError("Куратор не поддерживается")
    _ensure_db()
    changed = 0
    with _db() as db:
        rows = db.execute(
            "SELECT id,request_json,response_json FROM runs WHERE course_key=? AND stream_number=? AND test_mode=0",
            (course_key, stream_number),
        ).fetchall()
        for row in rows:
            request_data = _json_dict(row["request_json"])
            response_data = _json_dict(row["response_json"])
            request_data["curator_id"] = int(teacher_id)
            response_data["curator_id"] = int(teacher_id)
            db.execute(
                "UPDATE runs SET request_json=?,response_json=? WHERE id=?",
                (json.dumps(request_data, ensure_ascii=False), json.dumps(response_data, ensure_ascii=False), int(row["id"])),
            )
            changed += 1
        db.commit()
    return {"ok": True, "found": bool(rows), "changed": changed, "teacher": teacher}


def _set_manual_vk_link_sync(course_key: str, stream_number: str, link: str, credentials_path: Path) -> dict[str, Any]:
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    spreadsheet_id = _clean(os.environ.get("COURSE_CHAT_CREATOR_CHAT_LINKS_SPREADSHEET_ID")) or DEFAULT_CHAT_LINKS_SPREADSHEET_ID
    gid = _clean((CHAT_LINK_SHEETS.get(course_key) or {}).get("vk"))
    session = AuthorizedSession(
        Credentials.from_service_account_file(str(credentials_path), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    )
    metadata_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "sheets.properties(sheetId,title)"}, timeout=30,
    )
    metadata_response.raise_for_status()
    title = next(
        (
            _clean((sheet.get("properties") or {}).get("title"))
            for sheet in (metadata_response.json() or {}).get("sheets") or []
            if str((sheet.get("properties") or {}).get("sheetId")) == gid
        ),
        "",
    )
    if not title:
        raise RuntimeError("VK links worksheet not found")
    escaped = title.replace("'", "''")
    values_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/'{escaped}'!A:B",
        params={"majorDimension": "ROWS"}, timeout=30,
    )
    values_response.raise_for_status()
    rows = (values_response.json() or {}).get("values") or []
    row_number = _chat_link_row(rows, stream_number) or len(rows) + 1
    response = session.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
        json={
            "valueInputOption": "RAW",
            "data": [{"range": f"'{escaped}'!A{row_number}:B{row_number}", "values": [[_chat_link_row_title(course_key, stream_number), link]]}],
        },
        timeout=30,
    )
    response.raise_for_status()
    return {"ok": True, "spreadsheet_id": spreadsheet_id, "gid": gid, "row": row_number, "link": link}


async def _sync_manual_vk_link_in_background(
    *, run_id: int, course_key: str, stream_number: str, link: str, credentials_path: Path,
) -> None:
    sync_data: dict[str, Any] = {"ok": False, "status": "retry_required"}
    for attempt in range(1, 6):
        try:
            result = await asyncio.to_thread(
                _set_manual_vk_link_sync, course_key, stream_number, link, credentials_path,
            )
            sync_data = {"ok": True, "status": "completed", "attempts": attempt, **result}
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error = _exc_text(exc)
            retryable = "429" in error or "Too Many Requests" in error or "RESOURCE_EXHAUSTED" in error
            if retryable and attempt < 5:
                delay = min(10 * (2 ** (attempt - 1)), 80)
                _log(
                    "warning", "Manual VK link sheet sync rate-limited course=%s stream=%s attempt=%s retry_in=%ss",
                    course_key, stream_number, attempt, delay,
                )
                await asyncio.sleep(delay)
                continue
            _log("error", "Manual VK link sheet sync failed course=%s stream=%s: %s", course_key, stream_number, exc)
            sync_data = {"ok": False, "status": "retry_required", "attempts": attempt, "error": error}
            break
    _ensure_db()
    with _db() as db:
        row = db.execute("SELECT response_json FROM runs WHERE id=?", (run_id,)).fetchone()
        if row:
            response_data = _json_dict(row["response_json"])
            response_data["manual_link_sync"] = sync_data
            db.execute(
                "UPDATE runs SET response_json=? WHERE id=?",
                (json.dumps(response_data, ensure_ascii=False), run_id),
            )
            db.commit()


async def service_set_manual_vk_link(*, course_key: str, stream_number: str, link: str) -> dict[str, Any]:
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    link = _clean(link)
    if not re.match(r"^https://vk\.me/join/", link, flags=re.IGNORECASE):
        raise ValueError("VK invite link must start with https://vk.me/join/")
    _ensure_db()
    run_id = 0
    with _db() as db:
        row = db.execute(
            "SELECT id,response_json FROM runs WHERE platform='vk' AND course_key=? AND stream_number=? AND test_mode=0 ORDER BY id DESC LIMIT 1",
            (course_key, stream_number),
        ).fetchone()
        if row:
            run_id = int(row["id"])
            response_data = _json_dict(row["response_json"])
            response_data["manual_invite_link"] = link
            response_data["manual_link_sync"] = {"ok": True, "status": "scheduled"}
            db.execute(
                "UPDATE runs SET link=?,response_json=? WHERE id=?",
                (link, json.dumps(response_data, ensure_ascii=False), run_id),
            )
            db.commit()
    if not run_id:
        raise ValueError("Созданный VK-чат этого потока не найден")
    credentials_path = _chat_links_credentials_path()
    if not credentials_path or not credentials_path.exists():
        return {
            "ok": True, "status": "saved_locally", "link": link,
            "sheet_sync_ok": False, "warning": "Google Sheets не настроен",
        }
    task = asyncio.create_task(
        _sync_manual_vk_link_in_background(
            run_id=run_id, course_key=course_key, stream_number=stream_number,
            link=link, credentials_path=credentials_path,
        ),
        name=f"course-chat-creator-manual-vk-{course_key}-{stream_number}",
    )
    _manual_vk_sync_tasks.add(task)
    task.add_done_callback(_manual_vk_sync_tasks.discard)
    return {"ok": True, "status": "saved_locally", "link": link, "sheet_sync": "scheduled"}


async def service_create_flow_pair(
    *, course_key: str, stream_number: str, date_start: str, teacher_id: int
) -> dict[str, Any]:
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    if not stream_number or not stream_number.isdigit():
        raise ValueError("stream_number must be numeric")
    setup = service_flow_setup()
    teacher = next((item for item in setup["teachers"] if int(item["id"]) == int(teacher_id)), None)
    if not teacher:
        raise ValueError("teacher is disabled or not found")
    if not int(teacher.get("offer_id") or 0):
        raise ValueError("teacher offer_id is not configured")
    try:
        datetime.strptime(_clean(date_start), "%Y-%m-%d")
    except ValueError:
        raise ValueError("date_start must be YYYY-MM-DD")
    payload = {
        "course_type": course_key,
        "course_choice": course_key,
        "stream_number": stream_number,
        "date_start": _clean(date_start) or _today_moscow(),
        "curator_id": int(teacher_id),
        "test_mode": False,
    }
    existing = {
        item["platform"]: item
        for item in _ready_chat_pair(course_key, stream_number).values()
        if _clean(item.get("link"))
    }
    result: dict[str, Any] = {"ok": True, "course_key": course_key, "stream": stream_number}
    result["telegram"] = existing.get("telegram") or await _create_tg_chat(payload, trusted=True)
    result["vk"] = existing.get("vk") or await _create_vk_chat(payload, trusted=True)
    result["catalog"] = service_flow_catalog()
    return result


async def service_reapply_flow_vk_avatar(*, course_key: str, stream_number: str) -> dict[str, Any]:
    """Apply the configured avatar to an existing production VK flow chat.

    Creation can succeed while VK rejects the photo upload transiently.  Reuse
    the stored run instead of creating a second chat when an operator retries
    just this recoverable step.
    """
    _ensure_db()
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    if not stream_number.isdigit():
        raise ValueError("stream_number must be numeric")
    photo = _avatar_path()
    if not photo:
        raise RuntimeError("Файл логотипа беседы не найден")
    with _db() as db:
        row = db.execute(
            """SELECT * FROM runs WHERE platform='vk' AND course_key=? AND stream_number=?
               AND test_mode=0 ORDER BY id DESC LIMIT 1""",
            (course_key, stream_number),
        ).fetchone()
    if not row:
        raise ValueError("VK-беседа этого потока не найдена")
    run = dict(row)
    response = _json_dict(run.get("response_json"))
    peer_id = int(response.get("peer_id") or 0)
    if not peer_id:
        chat_id = _clean(run.get("chat_id"))
        peer_id = 2000000000 + int(chat_id) if chat_id.isdigit() else 0
    if not peer_id:
        raise RuntimeError("У созданной VK-беседы не найден идентификатор")
    token = _vk_group_token(test_mode=False)
    if not token:
        raise RuntimeError("VK_GROUP_TOKEN is not configured")
    applied = await _upload_vk_chat_photo(peer_id, photo, token)
    if not applied:
        raise RuntimeError("VK не подтвердил установку логотипа; повторите попытку позже")
    response["avatar_ready"] = True
    response["avatar_reapplied_at"] = datetime.now(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _update_run(int(run["id"]), _clean(run.get("status")) or "ok", response, error=_clean(run.get("error")))
    return {"ok": True, "platform": "vk", "stream": stream_number, "avatar_ready": True}


def service_transfer_chat_readiness(course_key: str, stream_number: str) -> dict[str, Any]:
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    vk = next(
        (
            item
            for item in _recorded_vk_course_chats()
            if item.get("course_key") == course_key and item.get("stream_number") == stream_number
        ),
        None,
    )
    _ensure_db()
    with _db() as db:
        tg_row = db.execute(
            """
            SELECT id,title,chat_id,status
            FROM runs
            WHERE platform='telegram' AND course_key=? AND stream_number=? AND test_mode=0
            ORDER BY created_at DESC,id DESC LIMIT 1
            """,
            (course_key, stream_number),
        ).fetchone()
    return {
        "vk": {
            "recorded": bool(vk),
            "manageable": bool(vk and vk.get("community_owned")),
            "status": "ready" if vk and vk.get("community_owned") else ("legacy_inaccessible" if vk else "not_recorded"),
            "title": _clean((vk or {}).get("title")),
        },
        "telegram": {
            "recorded": bool(tg_row),
            "manageable": bool(tg_row),
            "status": "ready" if tg_row else "not_recorded",
            "title": _clean(tg_row["title"] if tg_row else ""),
        },
    }


async def _transfer_vk_member_state(*, target: str, course_key: str, stream_number: str) -> dict[str, Any]:
    token = _vk_group_token()
    if not token:
        return {"ok": False, "status": "not_configured", "error": "VK_GROUP_TOKEN is not configured"}
    user_id = await _resolve_vk_target_id(_clean(target), token)
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    chat = next(
        (
            item for item in _recorded_vk_course_chats()
            if item.get("course_key") == course_key
            and item.get("stream_number") == stream_number
            and not item.get("test_mode")
        ),
        None,
    )
    if not chat:
        return {"ok": False, "status": "not_recorded", "platform": "vk"}
    if not chat.get("community_owned"):
        return {"ok": False, "status": "legacy_inaccessible", "platform": "vk"}
    response = await _vk_method(
        "messages.getConversationMembers",
        {
            "peer_id": int(chat["peer_id"]),
            "count": 1000,
            "group_id": int(_clean(os.environ.get("VK_GROUP_ID")) or 0),
        },
        token,
    )
    error = _vk_error_summary(response)
    if error:
        return {"ok": False, "status": "inaccessible", "platform": "vk", "error": error}
    members = response.get("items", []) if isinstance(response, dict) else []
    present = any(int(item.get("member_id") or 0) == user_id for item in members)
    return {
        "ok": True,
        "status": "joined" if present else "not_member",
        "platform": "vk",
        "present": present,
        "user_id": user_id,
        "chat": chat,
    }


async def service_transfer_target_membership(
    *, platform: str, target: str, course_key: str, stream_number: str
) -> dict[str, Any]:
    """Check the exact destination chat before the source member is removed."""

    if _clean(platform).lower() != "vk":
        return {"ok": False, "status": "unsupported", "platform": _clean(platform).lower()}
    state = await _transfer_vk_member_state(
        target=target, course_key=course_key, stream_number=stream_number,
    )
    state.pop("user_id", None)
    state.pop("chat", None)
    return state


async def service_prepare_transfer_vk_member(
    *, target: str, student_name: str, source_stream: str, target_course_key: str,
    target_stream: str, idempotency_key: str, vk_link: str = "", tg_link: str = "",
) -> dict[str, Any]:
    """Try direct VK admission, then deliver a corrective community DM.

    This function never removes the member from the source chat.  The caller
    must wait for ``service_transfer_target_membership`` to confirm the join.
    """

    token = _vk_group_token()
    if not token:
        return {"ok": False, "status": "not_configured", "error": "VK_GROUP_TOKEN is not configured"}
    state = await _transfer_vk_member_state(
        target=target, course_key=target_course_key, stream_number=target_stream,
    )
    if not state.get("ok"):
        state.pop("user_id", None)
        state.pop("chat", None)
        return state
    if state.get("present"):
        state.pop("user_id", None)
        state.pop("chat", None)
        return {**state, "status": "joined", "direct_add": "already_member"}
    user_id = int(state["user_id"])
    chat = dict(state["chat"])
    add_response = await _vk_method(
        "messages.addChatUser",
        {"chat_id": int(chat["chat_id"]), "user_id": user_id},
        token,
    )
    add_error = _vk_error_summary(add_response)
    await asyncio.sleep(0.4)
    verified = await _transfer_vk_member_state(
        target=str(user_id), course_key=target_course_key, stream_number=target_stream,
    )
    if verified.get("present"):
        return {
            "ok": True, "status": "joined", "platform": "vk",
            "direct_add": "added", "notification": "not_needed",
        }
    invite_link = _clean(vk_link or chat.get("link"))[:2000]
    if not invite_link.startswith("https://vk.me/join/"):
        return {
            "ok": False, "status": "missing_invite_link", "platform": "vk",
            "direct_add": "failed", "add_error": add_error,
        }
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    allowed_response = await _vk_method(
        "messages.isMessagesFromGroupAllowed",
        {"group_id": group_id, "user_id": user_id},
        token,
    )
    allowed = bool(
        allowed_response.get("is_allowed") if isinstance(allowed_response, dict) else allowed_response
    )
    if not allowed:
        return {
            "ok": False, "status": "messages_not_allowed", "platform": "vk",
            "direct_add": "failed", "add_error": add_error,
        }
    first_name = _clean(student_name)[:200].split(" ", 1)[0]
    greeting = f"{first_name}, здравствуйте!" if first_name else "Здравствуйте!"
    links = []
    telegram_link = _clean(tg_link)[:2000]
    if telegram_link.startswith(("https://t.me/", "http://t.me/")):
        links.append(f"Telegram: {telegram_link}")
    links.append(f"ВКонтакте: {invite_link}")
    links_text = "\n".join(links)
    message = (
        f"{greeting}\n\n"
        f"Мы перенесли вас из потока {source_stream} в поток {target_stream}.\n\n"
        f"Актуальные ссылки:\n{links_text}\n\n"
        f"После вступления в новый чат мы автоматически удалим вас из чата потока {source_stream}."
    )
    seed = _clean(idempotency_key)[:200] or f"{user_id}:{target_course_key}:{target_stream}"
    random_id = int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16) % (2**31 - 1) or 1
    send_response = await _vk_method(
        "messages.send",
        {"peer_id": user_id, "message": message, "random_id": random_id},
        token,
    )
    send_error = _vk_error_summary(send_response)
    if send_error:
        return {
            "ok": False, "status": "send_failed", "platform": "vk",
            "direct_add": "failed", "add_error": add_error, "error": send_error,
        }
    return {
        "ok": True, "status": "invite_sent", "platform": "vk",
        "direct_add": "failed", "add_error": add_error,
        "notification": "community_dm", "message_id": send_response,
    }


async def service_remove_transfer_member(
    *, platform: str, target: str, course_key: str, stream_number: str, dry_run: bool = False
) -> dict[str, Any]:
    platform = _clean(platform).lower()
    course_key = _course_key(course_key)
    stream_number = _clean(stream_number)
    target = _clean(target)
    if not target or not course_key or not stream_number:
        raise ValueError("target, course_key and stream_number are required")
    if platform == "vk":
        token = _vk_group_token()
        if not token:
            return {"ok": False, "status": "not_configured", "error": "VK_GROUP_TOKEN is not configured"}
        user_id = await _resolve_vk_target_id(target, token)
        inventory = await _vk_course_chat_inventory(user_id)
        item = next(
            (
                row
                for row in inventory.get("items") or []
                if row.get("course_key") == course_key and row.get("stream_number") == stream_number
            ),
            None,
        )
        if not item:
            return {"ok": False, "status": "not_recorded", "platform": "vk"}
        if not item.get("accessible"):
            return {"ok": False, "status": item.get("status") or "inaccessible", "platform": "vk", "item": item}
        if not item.get("target_present"):
            return {"ok": True, "status": "not_member", "platform": "vk", "item": item}
        if dry_run:
            return {"ok": True, "status": "would_remove", "platform": "vk", "item": item}
        response = await _vk_method(
            "messages.removeChatUser",
            {"chat_id": item["chat_id"], "member_id": user_id},
            token,
        )
        error = _vk_error_summary(response)
        if error:
            return {"ok": False, "status": "error", "platform": "vk", "error": error, "item": item}
        await asyncio.sleep(0.4)
        verify = await _vk_method(
            "messages.getConversationMembers",
            {"peer_id": item["peer_id"], "count": 1000},
            token,
        )
        rows = verify.get("items", []) if isinstance(verify, dict) and "error" not in verify else []
        present = any(int(row.get("member_id") or 0) == user_id for row in rows)
        return {"ok": not present, "status": "removed" if not present else "verify_failed", "platform": "vk", "item": item}
    if platform not in {"tg", "telegram"}:
        raise ValueError("platform must be vk or telegram")
    try:
        from telethon.tl import functions, types
    except Exception as exc:
        return {"ok": False, "status": "not_configured", "error": f"Telethon is not installed: {exc}"}
    _ensure_db()
    with _db() as db:
        row = db.execute(
            """
            SELECT id,title,chat_id FROM runs
            WHERE platform='telegram' AND course_key=? AND stream_number=? AND test_mode=0
            ORDER BY created_at DESC,id DESC LIMIT 1
            """,
            (course_key, stream_number),
        ).fetchone()
    if not row:
        return {"ok": False, "status": "not_recorded", "platform": "telegram"}
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    try:
        if not await client.is_user_authorized():
            return {"ok": False, "status": "not_authorized", "platform": "telegram"}
        member = await client.get_entity(target)
        wanted_id = _clean(row["chat_id"])
        wanted_entity_id = wanted_id[4:] if wanted_id.startswith("-100") else wanted_id.lstrip("-")
        wanted_title = _clean(row["title"])
        dialog = None
        async for candidate in client.iter_dialogs(limit=1000):
            entity_id = _clean(getattr(candidate.entity, "id", ""))
            if (wanted_entity_id and entity_id == wanted_entity_id) or _clean(candidate.name) == wanted_title:
                dialog = candidate
                break
        if dialog is None:
            return {"ok": False, "status": "chat_not_found", "platform": "telegram", "title": wanted_title}
        try:
            await client.get_permissions(dialog.entity, member)
            present = True
        except Exception:
            present = False
        if not present:
            return {"ok": True, "status": "not_member", "platform": "telegram", "title": wanted_title}
        if dry_run:
            return {"ok": True, "status": "would_remove", "platform": "telegram", "title": wanted_title}
        if isinstance(dialog.entity, types.Channel):
            await client(
                functions.channels.EditBannedRequest(
                    channel=dialog.entity,
                    participant=member,
                    banned_rights=types.ChatBannedRights(until_date=None, view_messages=True),
                )
            )
            await client(
                functions.channels.EditBannedRequest(
                    channel=dialog.entity,
                    participant=member,
                    banned_rights=types.ChatBannedRights(until_date=None),
                )
            )
        else:
            await client(functions.messages.DeleteChatUserRequest(chat_id=dialog.entity, user_id=member))
        try:
            await client.get_permissions(dialog.entity, member)
            still_present = True
        except Exception:
            still_present = False
        return {
            "ok": not still_present,
            "status": "removed" if not still_present else "verify_failed",
            "platform": "telegram",
            "title": wanted_title,
        }
    finally:
        await client.disconnect()


def _telegram_moderator_rights(types: Any) -> Any:
    return types.ChatAdminRights(
        change_info=False,
        post_messages=True,
        edit_messages=True,
        delete_messages=True,
        ban_users=True,
        invite_users=False,
        pin_messages=False,
        add_admins=False,
        anonymous=False,
        manage_call=False,
    )


def _telegram_not_modified(exc: Exception) -> bool:
    return (
        exc.__class__.__name__ in {"ChatNotModifiedError", "MessageNotModifiedError"}
        or "NOT_MODIFIED" in _exc_text(exc).upper()
        or "WASN'T MODIFIED" in _exc_text(exc).upper()
    )


async def _telegram_prepare_flow_chat(client: Any, channel: Any, functions: Any, types: Any) -> dict[str, Any]:
    """Apply safe member permissions and promote the school moderation bot."""
    try:
        await client(functions.messages.EditChatDefaultBannedRightsRequest(
            peer=channel,
            banned_rights=types.ChatBannedRights(
                until_date=None,
                change_info=True,
                invite_users=True,
                pin_messages=True,
            ),
        ))
    except Exception as exc:
        if not _telegram_not_modified(exc):
            raise
    moderator_ref = "@bullterrier_sobakovod_bot"
    moderator = await client.get_entity(moderator_ref)
    try:
        await client(functions.channels.InviteToChannelRequest(channel=channel, users=[moderator]))
    except Exception as exc:
        message = _exc_text(exc).lower()
        if "already" not in message and "participant" not in message:
            raise
    try:
        await client(functions.channels.EditAdminRequest(
            channel=channel,
            user_id=moderator,
            admin_rights=_telegram_moderator_rights(types),
            rank="Модератор",
        ))
    except Exception as exc:
        if not _telegram_not_modified(exc):
            raise
    return {"moderator_ref": moderator_ref, "moderator_user_id": int(getattr(moderator, "id", 0) or 0)}


async def _telegram_cleanup_service_messages(client: Any, channel: Any) -> int:
    """Remove technical action notices without touching forum topic roots or user content."""
    message_ids: list[int] = []
    async for message in client.iter_messages(channel, limit=250):
        action = getattr(message, "action", None)
        if action is None or action.__class__.__name__ == "MessageActionTopicCreate":
            continue
        message_id = int(getattr(message, "id", 0) or 0)
        if message_id:
            message_ids.append(message_id)
    if message_ids:
        await client.delete_messages(channel, message_ids)
    return len(message_ids)


async def service_repair_telegram_flow_curator(
    *, course_key: str, stream_number: str, correct_ref: str, old_ref: str = "",
) -> dict[str, Any]:
    """Repair one existing flow chat after an incorrect curator contact was configured."""
    from telethon import functions, types

    _ensure_db()
    course_key = _clean(course_key)
    stream_number = _clean(stream_number)
    correct_ref = _tg_username(correct_ref)
    old_ref = _tg_username(old_ref)
    if course_key not in {"puppy", "dog"} or not stream_number or not correct_ref:
        raise ValueError("Course, stream and correct Telegram username are required")
    with _db() as db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            """SELECT * FROM runs WHERE platform='telegram' AND course_key=? AND stream_number=? AND test_mode=0
               ORDER BY created_at DESC,id DESC LIMIT 1""",
            (course_key, stream_number),
        ).fetchone()
    if not row:
        return {"ok": False, "status": "not_recorded"}
    stored = dict(row)
    response = _json_dict(stored.get("response_json"))
    request_data = _json_dict(stored.get("request_json"))
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    try:
        if not await client.is_user_authorized():
            return {"ok": False, "status": "not_authorized"}
        wanted_title = _clean(stored.get("title"))
        dialog = None
        async for candidate in client.iter_dialogs(limit=1000):
            if _clean(candidate.name) == wanted_title:
                dialog = candidate
                break
        if dialog is None:
            return {"ok": False, "status": "chat_not_found", "title": wanted_title}

        correct_entity = await client.get_entity(correct_ref)
        try:
            await client(functions.channels.InviteToChannelRequest(channel=dialog.entity, users=[correct_entity]))
        except Exception as exc:
            if "already" not in _exc_text(exc).lower() and "participant" not in _exc_text(exc).lower():
                raise
        await client(functions.channels.EditAdminRequest(
            channel=dialog.entity,
            user_id=correct_entity,
            admin_rights=types.ChatAdminRights(
                change_info=True, post_messages=True, edit_messages=True, delete_messages=True,
                ban_users=True, invite_users=True, pin_messages=True, add_admins=True,
                anonymous=False, manage_call=True,
            ),
            rank="Куратор школы",
        ))
        moderator = await _telegram_prepare_flow_chat(client, dialog.entity, functions, types)

        removed_old = False
        if old_ref and old_ref.casefold() != correct_ref.casefold():
            try:
                old_entity = await client.get_entity(old_ref)
                if int(getattr(old_entity, "id", 0) or 0) != int(getattr(correct_entity, "id", 0) or 0):
                    try:
                        await client.get_permissions(dialog.entity, old_entity)
                        await client(functions.channels.EditBannedRequest(
                            channel=dialog.entity,
                            participant=old_entity,
                            banned_rights=types.ChatBannedRights(until_date=None, view_messages=True),
                        ))
                        await client(functions.channels.EditBannedRequest(
                            channel=dialog.entity,
                            participant=old_entity,
                            banned_rights=types.ChatBannedRights(until_date=None),
                        ))
                        removed_old = True
                    except Exception:
                        pass
            except Exception:
                pass

        topic_ids = response.get("topic_ids") if isinstance(response.get("topic_ids"), dict) else {}
        course = _course_by_input(course_key)
        selected = _selected_people(stream_number, request_data.get("curator_id"))
        channel_url_id = str(abs(int(getattr(dialog.entity, "id", 0) or 0)))
        extras = {
            "date_start": _format_date_russian(stored.get("date_start")),
            "channel_url_id": channel_url_id,
            **{f"topic_{key}_id": value or 1 for key, value in topic_ids.items()},
        }
        welcome = _render_template(
            "tg_welcome", course=course, stream_number=stream_number,
            date_start=stored.get("date_start"), selected=selected, platform="tg", extra=extras,
        )
        welcome_updated = False
        welcome_pinned = False
        async for message in client.iter_messages(dialog.entity, limit=120):
            raw = _clean(getattr(message, "raw_text", ""))
            if "Всем привет и добро пожаловать" in raw or (old_ref and old_ref.casefold() in raw.casefold()):
                try:
                    await client.edit_message(dialog.entity, message.id, welcome, parse_mode="html")
                except Exception as exc:
                    if not _telegram_not_modified(exc):
                        raise
                welcome_updated = True
                try:
                    await client(functions.messages.UpdatePinnedMessageRequest(
                        peer=dialog.entity, id=message.id, silent=True,
                    ))
                except Exception as exc:
                    if not _telegram_not_modified(exc):
                        raise
                welcome_pinned = True
                break
        info_topic_id = int(topic_ids.get("info") or 1)
        try:
            await client(functions.messages.EditForumTopicRequest(
                peer=dialog.entity, topic_id=info_topic_id, closed=True,
            ))
        except Exception as exc:
            if not _telegram_not_modified(exc):
                raise
        service_messages_removed = await _telegram_cleanup_service_messages(client, dialog.entity)
        repair = {
            "status": "completed", "correct_ref": correct_ref, "old_ref": old_ref,
            "correct_user_id": int(getattr(correct_entity, "id", 0) or 0),
            "removed_old": removed_old, "welcome_updated": welcome_updated,
            "welcome_pinned": welcome_pinned, "info_topic_closed": True,
            "service_messages_removed": service_messages_removed, **moderator,
            "updated_at": int(time.time()),
        }
        response["curator_contact_repair"] = repair
        _update_run(int(stored["id"]), _clean(stored.get("status")) or "ok", response, error=_clean(stored.get("error")))
        return {"ok": True, "title": wanted_title, **repair}
    finally:
        await client.disconnect()


async def _create_tg_chat(data: dict[str, Any], *, trusted: bool = False) -> dict[str, Any]:
    _check_password(data, trusted=trusted)
    try:
        from telethon import TelegramClient, functions, types
        from telethon.tl.functions.channels import EditPhotoRequest
        from telethon.tl.types import InputChatUploadedPhoto
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Telethon is not installed: {exc}")
    stream_number = _clean(data.get("stream_number"))
    date_start = _clean(data.get("start_date") or data.get("date_start")) or _today_moscow()
    course = _course_by_input(data.get("course_choice") or data.get("course_type") or "puppy")
    test_mode = _bool(data.get("test_mode"))
    if not stream_number:
        raise HTTPException(status_code=400, detail="Missing required parameter: stream_number")
    title = _format_title(stream_number, date_start, course, "tg")
    selected = _selected_people(stream_number, data.get("curator_id"))
    admins = _tg_refs(selected["admins"])
    kurators = _tg_refs(selected["kurators"])
    authors = _tg_refs(selected["authors"])
    techs = _tg_refs(selected["techs"])
    all_users = [] if test_mode else list(dict.fromkeys(admins + kurators + authors + techs))
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    if not await client.is_user_authorized():
        raise HTTPException(status_code=401, detail="Telegram session is not authorized. Configure TELEGRAM_SESSION_FILE with an authorized Telethon session.")
    async with client:
        valid_users = []
        if not test_mode:
            for user in all_users:
                try:
                    await client.get_entity(user)
                    valid_users.append(user)
                except Exception:
                    _log("warning", "Telegram user cannot be resolved: %s", user)
        result = await client(functions.channels.CreateChannelRequest(title=title, about="", megagroup=True, forum=True))
        channel = result.chats[0]
        telegram_chat_setup: dict[str, Any] = {}
        topic_ids = {"info": 1, "vizitka": None, "obuchenie": None, "boltalka": None}

        def get_topic_id(updates_obj: Any) -> int | None:
            for update in getattr(updates_obj, "updates", []):
                message = getattr(update, "message", None)
                action = getattr(message, "action", None)
                if action and action.__class__.__name__ == "MessageActionTopicCreate":
                    return getattr(message, "id", None)
                if hasattr(update, "id"):
                    return update.id
            return None

        async def create_topic(title: str, icon_emoji_id: int) -> int | None:
            last_exc: Exception | None = None
            for attempt in range(3):
                try:
                    updates = await client(functions.messages.CreateForumTopicRequest(
                        peer=channel,
                        title=title,
                        icon_emoji_id=icon_emoji_id,
                        random_id=random.randint(1, 2**31 - 1),
                    ))
                    return get_topic_id(updates)
                except Exception as exc:
                    last_exc = exc
                    _log("warning", "Telegram topic create retry %s for %s failed: %s", attempt + 1, title, exc)
                    await asyncio.sleep(2 + attempt * 3)
            if last_exc:
                raise last_exc
            return None

        async def fetch_topic_ids() -> dict[str, int]:
            topics = await client(functions.messages.GetForumTopicsRequest(
                peer=channel,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=20,
                q="",
            ))
            return {
                str(getattr(topic, "title", "")): int(getattr(topic, "id"))
                for topic in getattr(topics, "topics", []) or []
                if getattr(topic, "id", None) is not None
            }

        try:
            await client(functions.messages.EditForumTopicRequest(peer=channel, topic_id=1, title="Инфо"))
            await asyncio.sleep(1)
            topic_ids["vizitka"] = await create_topic("Визитка", 5237999392438371490)
            topic_ids["obuchenie"] = await create_topic("Обучение", 5357419403325481346)
            topic_ids["boltalka"] = await create_topic("Болталка", 5417915203100613993)
            topic_map = await fetch_topic_ids()
            topic_ids["info"] = topic_map.get("Инфо", 1)
            topic_ids["vizitka"] = topic_ids["vizitka"] or topic_map.get("Визитка")
            topic_ids["obuchenie"] = topic_ids["obuchenie"] or topic_map.get("Обучение")
            topic_ids["boltalka"] = topic_ids["boltalka"] or topic_map.get("Болталка")
            missing_topics = [name for key, name in (("info", "Инфо"), ("vizitka", "Визитка"), ("obuchenie", "Обучение"), ("boltalka", "Болталка")) if not topic_ids.get(key)]
            if missing_topics:
                raise RuntimeError("missing topics: " + ", ".join(missing_topics))
            await client(functions.messages.UpdatePinnedForumTopicRequest(peer=channel, topic_id=topic_ids["info"], pinned=True))
        except Exception as exc:
            _log("warning", "Telegram topic setup failed: %s", exc)
            raise HTTPException(status_code=500, detail=f"Telegram topic setup failed: {exc}")
        photo = _avatar_path()
        if photo:
            try:
                uploaded = await client.upload_file(str(photo))
                await client(EditPhotoRequest(channel=channel, photo=InputChatUploadedPhoto(uploaded)))
            except Exception as exc:
                _log("warning", "Telegram avatar setup failed: %s", exc)
        if not test_mode:
            try:
                telegram_chat_setup.update(await _telegram_prepare_flow_chat(client, channel, functions, types))
                telegram_chat_setup["member_permissions"] = "admins_only_invite_info_and_pin"
            except Exception as exc:
                _log("error", "Telegram moderator/permissions setup failed: %s", exc)
                raise HTTPException(status_code=500, detail=f"Telegram moderator setup failed: {exc}")

        async def invite_and_admin(user_refs: list[str], rank: str) -> None:
            for user in user_refs:
                try:
                    entity = await client.get_entity(user)
                    await client(functions.channels.InviteToChannelRequest(channel=channel, users=[entity]))
                    await asyncio.sleep(random.uniform(3, 6))
                    await client(functions.channels.EditAdminRequest(
                        channel=channel,
                        user_id=entity,
                        admin_rights=types.ChatAdminRights(
                            change_info=True,
                            post_messages=True,
                            edit_messages=True,
                            delete_messages=True,
                            ban_users=True,
                            invite_users=True,
                            pin_messages=True,
                            add_admins=True,
                            anonymous=False,
                            manage_call=True,
                        ),
                        rank=rank,
                    ))
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as exc:
                    _log("warning", "Telegram invite/admin failed for %s: %s", user, exc)
                    await asyncio.sleep(random.uniform(5, 10))

        if not test_mode:
            await invite_and_admin(admins, "")
            await invite_and_admin(kurators, "Куратор школы")
            await invite_and_admin(authors, "Автор курса")
            await invite_and_admin(techs, "")
            await invite_and_admin([u for u in valid_users if u not in admins + kurators + authors + techs], "")
        channel_url_id = str(abs(int(getattr(channel, "id", 0))))
        extras = {"date_start": _format_date_russian(date_start), "channel_url_id": channel_url_id, **{f"topic_{k}_id": v or 1 for k, v in topic_ids.items()}}
        bot_channel = await client.get_entity(channel)
        sent: list[tuple[Any, int | None, str]] = []
        for key, topic_key, label in [
            ("tg_welcome", "info", "Info"),
            ("tg_vizitka", "vizitka", "Vizitka"),
            ("tg_obuchenie", "obuchenie", "Obuchenie"),
            ("tg_boltalka", "boltalka", "Boltalka"),
        ]:
            topic_id = topic_ids.get(topic_key)
            try:
                text = _render_template(key, course=course, stream_number=stream_number, date_start=date_start, selected=selected, platform="tg", extra=extras)
                if key == "tg_welcome":
                    welcome_photo = _asset_path("welcome_message_photo.jpg")
                    if welcome_photo and topic_id:
                        try:
                            await client.send_file(bot_channel, str(welcome_photo), reply_to=topic_id)
                        except Exception as exc:
                            _log("warning", "Telegram welcome image send failed: %s", exc)
                msg = await client.send_message(bot_channel, text, parse_mode="html", reply_to=topic_id)
                sent.append((msg, topic_id, label))
                if key == "tg_welcome" and topic_id:
                    try:
                        await client(functions.messages.EditForumTopicRequest(peer=channel, topic_id=topic_id, closed=True))
                    except Exception as exc:
                        _log("warning", "Telegram info topic close failed: %s", exc)
            except Exception as exc:
                _log("warning", "Telegram message failed %s: %s", label, exc)
        await asyncio.sleep(2 if test_mode else 8)
        for msg, topic_id, label in sent:
            for attempt in range(3):
                try:
                    await client(functions.messages.UpdatePinnedMessageRequest(peer=bot_channel, id=msg.id, silent=True))
                    await asyncio.sleep(1)
                    break
                except Exception as exc:
                    if attempt == 2:
                        _log("warning", "Telegram pin failed %s: %s", label, exc)
                    else:
                        await asyncio.sleep(2 * (attempt + 1))
        try:
            telegram_chat_setup["service_messages_removed"] = await _telegram_cleanup_service_messages(client, channel)
        except Exception as exc:
            _log("warning", "Telegram service message cleanup failed: %s", exc)
            telegram_chat_setup["service_messages_cleanup_error"] = _exc_text(exc)
        try:
            invite = await client(functions.messages.ExportChatInviteRequest(peer=channel))
            invite_link = invite.link
        except Exception as exc:
            _log("warning", "Telegram invite export failed: %s", exc)
            invite_link = ""
    response = {"message": "Group created successfully", "group_title": title, "group_link": invite_link, "course_choice": course["choice"], "test_mode": test_mode, "topic_ids": topic_ids, "curator_id": _selected_curator_id(stream_number, data.get("curator_id")), "telegram_chat_setup": telegram_chat_setup}
    run_id = _record_run("telegram", title, stream_number, date_start, course["key"], test_mode, "ok", data, response, link=invite_link, chat_id="")
    link_sync = await _sync_chat_pair_to_sheet(course["key"], stream_number, test_mode=test_mode)
    response["link_sync"] = link_sync
    if not bool(link_sync.get("ok", True)):
        detail = _clean(link_sync.get("error")) or "Не удалось обновить таблицу ссылок"
        response.update({"needs_attention": True, "followup_status": "needs_link_sync", "detail": detail})
        _update_run(run_id, "needs_link_sync", response, error=detail)
    else:
        _update_run(run_id, "ok", response)
    return response


@router.post("/vk/create")
@router.post("/process_vk")
async def process_vk(request: Request):
    data = await request.json()
    _require_explicit_curator(data)
    try:
        return JSONResponse(await _create_vk_chat(data))
    except Exception as exc:
        stream_number = _clean(data.get("stream_number"))
        date_start = _clean(data.get("date_start") or data.get("start_date")) or _today_moscow()
        course_key = _course_key(data.get("course_type") or data.get("course_choice"))
        title = f"{stream_number}. {date_start}"
        _record_run("vk", title, stream_number, date_start, course_key, _bool(data.get("test_mode")), "error", data, error=str(exc))
        raise


@router.post("/telegram/create")
@router.post("/process6")
async def process6(request: Request):
    data = await request.json()
    _require_explicit_curator(data)
    try:
        return JSONResponse(await _create_tg_chat(data))
    except Exception as exc:
        stream_number = _clean(data.get("stream_number"))
        date_start = _clean(data.get("date_start") or data.get("start_date")) or _today_moscow()
        course_key = _course_key(data.get("course_type") or data.get("course_choice"))
        title = f"{stream_number}. {date_start}"
        _record_run("telegram", title, stream_number, date_start, course_key, _bool(data.get("test_mode")), "error", data, error=str(exc))
        raise


@router.post("/chats/create")
@router.post("/create")
async def create_from_panel(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    _require_explicit_curator(data)
    platform = _clean(data.get("platform")).lower()
    try:
        if platform == "vk":
            return JSONResponse(await _create_vk_chat(data, trusted=True))
        if platform in {"tg", "telegram"}:
            return JSONResponse(await _create_tg_chat(data, trusted=True))
        raise HTTPException(status_code=400, detail="platform must be vk or telegram")
    except Exception as exc:
        if platform in {"vk", "tg", "telegram"}:
            stream_number = _clean(data.get("stream_number"))
            date_start = _clean(data.get("date_start") or data.get("start_date")) or _today_moscow()
            course_key = _course_key(data.get("course_type") or data.get("course_choice"))
            title = f"{stream_number}. {date_start}"
            _record_run("telegram" if platform in {"tg", "telegram"} else "vk", title, stream_number, date_start, course_key, _bool(data.get("test_mode")), "error", data, error=str(exc))
        raise


@router.get("/telegram/auth/status")
async def telegram_auth_status(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "telegram": await _telegram_auth_state(include_user=True)}


@router.post("/telegram/auth/send-code")
async def telegram_auth_send_code(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    phone = _clean(data.get("phone"))
    if not phone:
        raise HTTPException(status_code=400, detail="phone is required")
    try:
        from telethon import TelegramClient
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Telethon is not installed: {exc}")
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    try:
        sent = await client.send_code_request(phone)
        _tg_auth_pending[phone] = {
            "phone_code_hash": sent.phone_code_hash,
            "created_at": time.time(),
        }
        return {"ok": True, "phone": phone, "session_file": session_file}
    finally:
        await client.disconnect()


@router.post("/telegram/auth/confirm")
async def telegram_auth_confirm(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    phone = _clean(data.get("phone"))
    code = _clean(data.get("code"))
    password = _clean(data.get("password"))
    pending = _tg_auth_pending.get(phone)
    if not phone or not code:
        raise HTTPException(status_code=400, detail="phone and code are required")
    if not pending or time.time() - float(pending.get("created_at", 0)) > 600:
        raise HTTPException(status_code=400, detail="Telegram code request expired. Send code again.")
    try:
        from telethon import TelegramClient
        from telethon.errors import SessionPasswordNeededError
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Telethon is not installed: {exc}")
    api_id, api_hash, session_file = _telegram_credentials()
    client = _telegram_client(api_id, api_hash, session_file)
    await _telegram_connect(client)
    try:
        try:
            await client.sign_in(phone=phone, code=code, phone_code_hash=pending["phone_code_hash"])
        except SessionPasswordNeededError:
            if not password:
                return JSONResponse({"ok": False, "password_required": True}, status_code=401)
            await client.sign_in(password=password)
        authorized = await client.is_user_authorized()
        if authorized:
            _tg_auth_pending.pop(phone, None)
        return {"ok": True, "authorized": authorized, "session_file": session_file}
    finally:
        await client.disconnect()


@router.post("/vk/admins/retry")
@router.post("/vk-web/admins/retry", include_in_schema=False)
async def vk_admins_retry(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    run_id = data.get("run_id")
    if run_id in (None, ""):
        raise HTTPException(status_code=400, detail="run_id is required")
    return {"ok": True, "result": await _retry_vk_admins_from_run(int(run_id))}


@router.post("/vk/admins/retry-pending")
@router.post("/vk-web/admins/retry-pending", include_in_schema=False)
async def vk_admins_retry_pending(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "result": await _retry_vk_admins_from_run(None)}


@router.get("/status")
async def status():
    _ensure_db()
    telegram = await _telegram_auth_state()
    required_env = {
        "vk_group_token": bool(os.environ.get("VK_GROUP_TOKEN")),
        "vk_group_id": bool(os.environ.get("VK_GROUP_ID")),
        "telegram_api": bool(os.environ.get("TELEGRAM_API_ID") and os.environ.get("TELEGRAM_API_HASH")),
        "telegram_session": bool(telegram.get("authorized")),
    }
    optional_env = {
        "webhook_password": bool(_password()),
        "sbkvd_legacy_password": bool(os.environ.get("SBKVD_PROCESS_WEBHOOK_PASSWORD")),
        "vk_test_group_token": bool(os.environ.get("VK_TEST_GROUP_TOKEN")),
    }
    return {
        "ok": True,
        "env": required_env,
        "required_env": required_env,
        "optional_env": optional_env,
        "telegram": telegram,
        "vk_pin_watchdog": dict(_vk_pin_watchdog_state),
        "chat_links_sync": _chat_links_sync_status(),
        "asset_group_photo": bool(_avatar_path()),
        "asset_welcome_photo": bool(_asset_path("welcome_message_photo.jpg")),
    }


@router.post("/chat-links/sync")
async def sync_chat_links(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    stream_number = _clean(data.get("stream_number"))
    if not stream_number:
        raise HTTPException(status_code=400, detail="stream_number is required")
    course = _course_by_input(data.get("course_key") or data.get("course") or data.get("course_type"))
    result = await _sync_chat_pair_to_sheet(course["key"], stream_number, test_mode=False)
    return {"ok": bool(result.get("ok")), "course_key": course["key"], **result}


@router.get("/people")
async def list_people(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "items": _people(enabled=False)}


@router.post("/people")
async def upsert_person(request: Request):
    await _require_panel_access(request)
    _ensure_local_staff_mutation_allowed()
    _ensure_db()
    data = await request.json()
    kind = _clean(data.get("kind"))
    name = _clean(data.get("name"))
    if kind not in {"admin", "kurator", "author", "tech"}:
        raise HTTPException(status_code=400, detail="Invalid kind")
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    vk_value = _clean(data.get("vk_id"))
    vk_screen = _vk_screen_name(vk_value)
    vk_mention = _clean(data.get("vk_mention"))
    if not vk_mention and vk_screen:
        vk_mention = f"[id{vk_screen}|{name}]" if vk_screen.isdigit() else f"@{vk_screen}"
    tg_ref = _tg_username(data.get("tg_ref"))
    payload = {
        "kind": kind,
        "name": name,
        "vk_id": vk_value,
        "vk_mention": vk_mention,
        "tg_ref": tg_ref,
        "offer_id": max(0, int(data.get("offer_id") or 0)) if kind == "kurator" else 0,
        "parity": _clean(data.get("parity")) or "any",
        "enabled": 1 if data.get("enabled", True) else 0,
        "note": _clean(data.get("note")),
    }
    with _db() as db:
        if data.get("id"):
            payload["id"] = int(data["id"])
            db.execute(
                """UPDATE people SET kind=:kind,name=:name,vk_id=:vk_id,vk_mention=:vk_mention,tg_ref=:tg_ref,offer_id=:offer_id,parity=:parity,enabled=:enabled,note=:note,updated_at=strftime('%s','now') WHERE id=:id""",
                payload,
            )
            person_id = payload["id"]
        else:
            cur = db.execute(
                """INSERT INTO people(kind,name,vk_id,vk_mention,tg_ref,offer_id,parity,enabled,note) VALUES(:kind,:name,:vk_id,:vk_mention,:tg_ref,:offer_id,:parity,:enabled,:note)""",
                payload,
            )
            person_id = cur.lastrowid
        db.commit()
    return {"ok": True, "id": person_id}


@router.delete("/people/{person_id}")
async def delete_person(person_id: int, request: Request):
    await _require_panel_access(request)
    _ensure_local_staff_mutation_allowed()
    _ensure_db()
    with _db() as db:
        db.execute("DELETE FROM people WHERE id=?", (person_id,))
        db.commit()
    return {"ok": True}


@router.get("/courses")
async def list_courses(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM courses ORDER BY choice, key").fetchall()]
    return {"ok": True, "items": rows}


@router.post("/courses")
async def upsert_course(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    data = await request.json()
    key = _course_key(data.get("key"))
    payload = {
        "key": key,
        "choice": _clean(data.get("choice")) or key,
        "title": _clean(data.get("title")),
        "vk_title": _clean(data.get("vk_title")),
        "tg_title": _clean(data.get("tg_title")),
        "enabled": 1 if data.get("enabled", True) else 0,
    }
    if not payload["title"] or not payload["vk_title"] or not payload["tg_title"]:
        raise HTTPException(status_code=400, detail="title, vk_title and tg_title are required")
    with _db() as db:
        db.execute(
            """INSERT INTO courses(key,choice,title,vk_title,tg_title,enabled) VALUES(:key,:choice,:title,:vk_title,:tg_title,:enabled)
               ON CONFLICT(key) DO UPDATE SET choice=excluded.choice,title=excluded.title,vk_title=excluded.vk_title,tg_title=excluded.tg_title,enabled=excluded.enabled,updated_at=strftime('%s','now')""",
            payload,
        )
        db.commit()
    return {"ok": True, "key": key}


@router.get("/templates")
async def list_templates(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    with _db() as db:
        rows = [
            dict(row)
            for row in db.execute(
                "SELECT * FROM templates WHERE key <> 'vk_invite_fallback' ORDER BY key"
            ).fetchall()
        ]
    return {"ok": True, "items": rows}


@router.post("/templates")
async def update_template(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    data = await request.json()
    key = _clean(data.get("key"))
    body = str(data.get("body") or "")
    if not key:
        raise HTTPException(status_code=400, detail="key is required")
    if key == "vk_invite_fallback":
        raise HTTPException(status_code=409, detail="VK personal invite messages are disabled")
    with _db() as db:
        db.execute(
            "INSERT INTO templates(key,body) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET body=excluded.body,updated_at=strftime('%s','now')",
            (key, body),
        )
        db.commit()
    return {"ok": True}


@router.get("/preview")
async def preview(stream_number: str = "51", start_date: str = "", course: str = "puppy", curator_id: str = "", test_mode: str = "false"):
    curator_id = str(_require_explicit_curator({"curator_id": curator_id}))
    start_date = _clean(start_date) or _today_moscow()
    course_row = _course_by_input(course)
    selected = _selected_people(stream_number, curator_id)
    is_test = _bool(test_mode)
    vk_staff = _vk_staff_for_mode(selected, test_mode=is_test)
    cohort = (
        {"available": True, "source": "test_mode", "total": 0, "with_vk": 0, "without_vk": 0, "reason": "test_mode"}
        if is_test
        else _vk_student_cohort(course_row["key"], stream_number)
    )
    return {
        "ok": True,
        "test_mode": is_test,
        "vk_title": _format_title(stream_number, start_date, course_row, "vk"),
        "tg_title": _format_title(stream_number, start_date, course_row, "tg"),
        "selected": selected,
        "vk_staff": [{"id": person.get("id"), "name": person.get("name"), "vk_id": person.get("vk_id")} for person in vk_staff],
        "curator_id": _selected_curator_id(stream_number, curator_id),
        "vk_students": {key: value for key, value in cohort.items() if key != "vk_ids"},
        "vk_welcome": _render_template("vk_test_welcome" if is_test else "vk_welcome", course=course_row, stream_number=stream_number, date_start=start_date, selected=selected, platform="vk"),
        "tg_welcome": _render_template("tg_welcome", course=course_row, stream_number=stream_number, date_start=start_date, selected=selected, platform="tg"),
    }


@router.get("/runs")
async def list_runs(request: Request, limit: int = 50):
    await _require_panel_access(request)
    _ensure_db()
    limit = max(1, min(200, int(limit)))
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
    return {"ok": True, "items": rows}


@router.post("/runs/clear")
async def clear_runs(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    with _db() as db:
        db.execute("DELETE FROM runs")
        db.commit()
    return {"ok": True}


@router.get("/broadcast/status")
async def broadcast_status(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    vk_token = _vk_group_token()
    group_id = _clean(os.environ.get("VK_GROUP_ID"))
    vk_community: dict[str, Any] | None = None
    vk_error = ""
    if vk_token and group_id:
        try:
            groups = await _vk_method("groups.getById", {"group_id": group_id, "fields": "screen_name"}, vk_token)
            group_items = groups.get("groups", []) if isinstance(groups, dict) and "groups" in groups else groups
            if isinstance(group_items, list) and group_items:
                group = group_items[0]
                vk_community = {
                    "id": group.get("id"),
                    "screen_name": group.get("screen_name"),
                    "name": _clean(group.get("name")),
                }
            elif isinstance(groups, dict) and "error" in groups:
                vk_error = str(groups["error"])
        except Exception as exc:
            vk_error = str(exc)
    telegram = await _telegram_auth_state(include_user=True)
    return {
        "ok": True,
        "vk": {"configured": bool(vk_token and group_id), "community": vk_community, "error": vk_error},
        "telegram": telegram,
        "course_title_rule": r"^\d+\. DD.MM.YYYY",
    }


@router.get("/broadcast/chats")
async def broadcast_chats(request: Request, platform: str = "all", limit: int = 500):
    await _require_panel_access(request)
    normalized = _broadcast_normalize_platform(platform)
    platforms = {"vk", "telegram"} if platform == "all" or not normalized else {normalized}
    limit = max(1, min(1000, int(limit)))
    return await _broadcast_chat_candidates(platforms, limit=limit)


@router.get("/broadcasts")
async def list_broadcasts(request: Request, limit: int = 30):
    await _require_panel_access(request)
    _ensure_db()
    limit = max(1, min(100, int(limit)))
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM broadcasts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
        messages = [dict(row) for row in db.execute(
            """SELECT * FROM broadcast_messages
               WHERE broadcast_id IN (SELECT id FROM broadcasts ORDER BY id DESC LIMIT ?)
               ORDER BY id DESC""",
            (limit,),
        ).fetchall()]
    by_broadcast: dict[int, list[dict[str, Any]]] = {}
    for row in messages:
        by_broadcast.setdefault(int(row["broadcast_id"]), []).append(row)
    for row in rows:
        row["platforms"] = _json_array(row.get("platforms"))
        row["selected"] = _json_array(row.get("selected_json"))
        row["excluded"] = _json_array(row.get("excluded_json"))
        row["result"] = _json_object(row.get("result_json"))
        row["messages"] = by_broadcast.get(int(row["id"]), [])
        counts: dict[str, int] = {}
        for msg in row["messages"]:
            counts[msg["status"]] = counts.get(msg["status"], 0) + 1
        row["counts"] = counts
    return {"ok": True, "items": rows}


@router.post("/broadcast/send")
async def send_broadcast(request: Request):
    await _require_panel_access(request)
    _ensure_db()
    data = await request.json()
    message = str(data.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    requested_platforms = data.get("platforms") or ["vk", "telegram"]
    if isinstance(requested_platforms, str):
        requested_platforms = [requested_platforms]
    platforms = {_broadcast_normalize_platform(item) for item in requested_platforms}
    platforms = {item for item in platforms if item}
    if not platforms:
        raise HTTPException(status_code=400, detail="platforms must include vk or telegram")
    mode = _clean(data.get("mode") or "selected")
    if mode not in {"selected", "all_except"}:
        raise HTTPException(status_code=400, detail="mode must be selected or all_except")
    selected = {_clean(item) for item in (data.get("selected") or []) if _clean(item)}
    excluded = {_clean(item) for item in (data.get("excluded") or []) if _clean(item)}
    candidates = await _broadcast_chat_candidates(platforms)
    targets = _broadcast_filter_selection(candidates["items"], mode, selected, excluded)
    if not targets:
        raise HTTPException(status_code=400, detail="No chats selected for broadcast")
    delay_min_ms, delay_max_ms, speed = _broadcast_delay_bounds(data)
    created_at = int(time.time())
    with _db() as db:
        cur = db.execute(
            """INSERT INTO broadcasts(platforms,message,mode,selected_json,excluded_json,status,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (
                json.dumps(sorted(platforms), ensure_ascii=False),
                message,
                mode,
                json.dumps(sorted(selected), ensure_ascii=False),
                json.dumps(sorted(excluded), ensure_ascii=False),
                "running",
                created_at,
            ),
        )
        broadcast_id = int(cur.lastrowid)
        db.commit()
    tg_client = None
    tg_entities: dict[str, Any] = {}
    tg_error = ""
    tg_targets = [chat for chat in targets if chat.get("platform") == "telegram" and chat.get("can_send")]
    if tg_targets:
        try:
            api_id, api_hash, session_file = _telegram_credentials()
            tg_client = _telegram_client(api_id, api_hash, session_file)
            await _telegram_connect(tg_client)
            if not await tg_client.is_user_authorized():
                tg_error = "Telegram session is not authorized"
            else:
                tg_entities = await _telegram_broadcast_entity_map(tg_client, tg_targets)
        except Exception as exc:
            tg_error = f"Telegram connection failed: {_exc_text(exc)}"
    sent = 0
    errors = 0
    skipped = 0
    try:
        for index, chat in enumerate(targets):
            status_value = "sent"
            message_id = ""
            error = ""
            if not chat.get("can_send"):
                status_value = "skipped"
                error = _clean(chat.get("error")) or "chat is not sendable"
                skipped += 1
            elif chat.get("platform") == "vk":
                ok, message_id, error = await _send_vk_broadcast_message(chat, message)
                status_value = "sent" if ok else "error"
            elif chat.get("platform") == "telegram":
                if tg_error:
                    ok, message_id, error = False, "", tg_error
                elif tg_client is None:
                    ok, message_id, error = False, "", "Telegram client is not initialized"
                else:
                    chat_id = _clean(chat.get("chat_id"))
                    title = _clean(chat.get("title"))
                    entity = tg_entities.get(f"id:{chat_id}") or tg_entities.get(f"title:{title}")
                    if entity is None:
                        ok, message_id, error = False, "", "Telegram chat was not found by live scan"
                    else:
                        ok, message_id, error = await _send_tg_with_entity(tg_client, entity, message)
                status_value = "sent" if ok else "error"
            else:
                status_value = "error"
                error = "unknown platform"
            if status_value == "sent":
                sent += 1
            elif status_value == "error":
                errors += 1
            with _db() as db:
                db.execute(
                    """INSERT INTO broadcast_messages(broadcast_id,platform,chat_key,chat_title,peer_id,chat_id,message_id,status,error)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        broadcast_id,
                        _clean(chat.get("platform")),
                        _clean(chat.get("chat_key")),
                        _clean(chat.get("title")),
                        _clean(chat.get("peer_id")),
                        _clean(chat.get("chat_id")),
                        message_id,
                        status_value,
                        error,
                    ),
                )
                db.commit()
            await _broadcast_sleep((delay_min_ms, delay_max_ms), index=index, total=len(targets))
    finally:
        if tg_client is not None:
            await tg_client.disconnect()
    final_status = "done"
    if errors and sent:
        final_status = "partial"
    elif errors and not sent:
        final_status = "error"
    elif skipped and not sent:
        final_status = "skipped"
    result = {"target_count": len(targets), "sent": sent, "errors": errors, "skipped": skipped, "speed": speed, "delay_min_ms": delay_min_ms, "delay_max_ms": delay_max_ms}
    with _db() as db:
        db.execute(
            "UPDATE broadcasts SET status=?, sent_at=?, result_json=? WHERE id=?",
            (final_status, int(time.time()), json.dumps(result, ensure_ascii=False), broadcast_id),
        )
        db.commit()
    return {"ok": True, "id": broadcast_id, "status": final_status, "result": result, "counts": _broadcast_message_counts(broadcast_id)}


@router.post("/broadcasts/{broadcast_id}/delete")
async def delete_broadcast_messages(broadcast_id: int, request: Request):
    await _require_panel_access(request)
    _ensure_db()
    with _db() as db:
        broadcast = db.execute("SELECT * FROM broadcasts WHERE id=?", (broadcast_id,)).fetchone()
        if not broadcast:
            raise HTTPException(status_code=404, detail="broadcast not found")
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM broadcast_messages WHERE broadcast_id=? AND status IN ('sent','delete_error') ORDER BY id",
            (broadcast_id,),
        ).fetchall()]
    deleted = 0
    errors = 0
    for row in rows:
        if row["platform"] == "vk":
            ok, error = await _delete_vk_broadcast_message(_clean(row.get("peer_id")), _clean(row.get("message_id")))
        elif row["platform"] == "telegram":
            ok, error = await _delete_tg_broadcast_message(_clean(row.get("chat_title")), _clean(row.get("chat_id")), _clean(row.get("message_id")))
        else:
            ok, error = False, "unknown platform"
        new_status = "deleted" if ok else "delete_error"
        if ok:
            deleted += 1
        else:
            errors += 1
        with _db() as db:
            db.execute(
                "UPDATE broadcast_messages SET status=?, error=?, deleted_at=? WHERE id=?",
                (new_status, error, int(time.time()) if ok else 0, row["id"]),
            )
            db.commit()
        await asyncio.sleep(0.3)
    status_value = "deleted" if rows and errors == 0 else ("delete_error" if errors else _clean(broadcast["status"]))
    with _db() as db:
        db.execute("UPDATE broadcasts SET status=?, deleted_at=? WHERE id=?", (status_value, int(time.time()) if deleted else 0, broadcast_id))
        db.commit()
    return {"ok": True, "id": broadcast_id, "deleted": deleted, "errors": errors, "counts": _broadcast_message_counts(broadcast_id)}


@router.post("/members/remove")
async def remove_member_from_course_chats(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    target = _clean(data.get("target"))
    platform = _clean(data.get("platform")).lower()
    dry_run = _bool(data.get("dry_run", True))
    if not target:
        raise HTTPException(status_code=400, detail="target is required")
    if platform == "vk":
        return await _remove_vk_from_course_chats(target, dry_run=dry_run)
    if platform in {"tg", "telegram"}:
        return await _remove_tg_from_course_chats(target, dry_run=dry_run)
    if platform == "both":
        return {
            "ok": True,
            "dry_run": dry_run,
            "vk": await _remove_vk_from_course_chats(target, dry_run=dry_run),
            "telegram": await _remove_tg_from_course_chats(target, dry_run=dry_run),
        }
    raise HTTPException(status_code=400, detail="platform must be vk, telegram or both")


@router.post("/telegram/flow-curator/repair")
async def repair_telegram_flow_curator(request: Request):
    user = await _require_panel_access(request)
    if require_admin and not require_admin(user):
        raise HTTPException(status_code=403, detail="admin required")
    data = await request.json()
    try:
        return await service_repair_telegram_flow_curator(
            course_key=_clean(data.get("course_key")),
            stream_number=_clean(data.get("stream")),
            correct_ref=_clean(data.get("correct_ref")),
            old_ref=_clean(data.get("old_ref")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/vk-course-chats")
async def list_vk_course_chats(request: Request):
    await _require_panel_access(request)
    return await _vk_course_chat_inventory()


@router.post("/vk-course-chats/senler/sync")
async def sync_vk_course_chat_members_to_senler(request: Request):
    await _require_panel_access(request)
    return await _sync_vk_course_chat_members_to_senler()


@router.post("/vk-course-chats/pin/restore")
async def restore_vk_course_chat_pin(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    try:
        peer_id = int(data.get("peer_id") or 0)
    except (TypeError, ValueError):
        peer_id = 0
    if peer_id <= 2000000000:
        raise HTTPException(status_code=400, detail="peer_id is required")
    return await _restore_vk_course_pin_manual(peer_id, dry_run=_bool(data.get("dry_run", False)))


@router.post("/vk-members/manage")
async def manage_vk_course_chat_members(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    target = _clean(data.get("target"))
    if not target:
        raise HTTPException(status_code=400, detail="target is required")
    return await _manage_vk_course_chats(
        target,
        action=_clean(data.get("action")).lower(),
        dry_run=_bool(data.get("dry_run", True)),
    )
