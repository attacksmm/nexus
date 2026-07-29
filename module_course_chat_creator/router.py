from __future__ import annotations

import asyncio
import csv
import io
import json
import mimetypes
import os
import random
import re
import sqlite3
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
DEFAULT_MODULE_ID = "course-chat-creator"
DEFAULT_CHAT_LINKS_SPREADSHEET_ID = "1zu1__XcKxJH8yC9ForDvibaUnKFCS1pxWHEjLgqlVXA"
CHAT_LINK_SHEETS = {
    "dog": {"telegram": "304757615", "vk": "443062527"},
    "puppy": {"telegram": "1437498106", "vk": "65520414"},
}
TEMPLATE_DEFAULTS_VERSION = "windsurf-2026-06-02-full"
COURSE_CHAT_TITLE_RE = re.compile(r"^\s*\d+\.\s*\d{2}\.\d{2}\.\d{4}\s*-\s*(Курс Щенок\. Современный Собаковод|Современный Собаковод\b)", re.IGNORECASE)

_ctx = None
_logger = None
_db_initialized = False
_tg_auth_pending: dict[str, dict[str, Any]] = {}
_vk_bootstrap_subscription: VkGroupPollSubscription | None = None
_vk_pin_watchdog_task: asyncio.Task[Any] | None = None
_vk_invite_dispatch_task: asyncio.Task[Any] | None = None
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
    {"kind": "kurator", "name": "Ирина", "vk_id": "413314992", "vk_mention": "[id413314992|@demidovair]", "tg_ref": "", "parity": "any", "enabled": 1},
    {"kind": "admin", "name": "Техническая поддержка", "vk_id": "1105209997", "vk_mention": "[id1105209997|@tehpod_sobakovodpro]", "tg_ref": "@Tech_kurator", "enabled": 1},
    {"kind": "admin", "name": "Никита", "vk_id": "741919467", "vk_mention": "[id741919467|@attackpng]", "tg_ref": "", "enabled": 1},
    {"kind": "admin", "name": "Андрей", "vk_id": "11335495", "vk_mention": "[id11335495|@id11335495]", "tg_ref": "", "enabled": 1},
]

VK_WELCOME_TEMPLATE = "🐾 Добро пожаловать в закрытый чат курса «{course_full_name}»! 🐾\n\nЯ очень рада, что вы здесь. Вы уже сделали важный шаг на пути к осознанному воспитанию вашей собаки.\n\n🗓 Поток №{stream_number}: Обучение стартует {date_start}\nВпереди у нас 11 недель практического обучения, поддержки и маленьких побед! 💪🏼🐶\n\n📍 ПЕРВЫЙ ШАГ — ЗНАКОМСТВО (ВИЗИТКА)\nПожалуйста, расскажите о себе и своем питомце в ОДНОМ сообщении по форме:\n1️⃣ Ваше имя и город\n2️⃣ Кличка собаки, возраст, порода/фенотип/дворняжка\n3️⃣ С какими трудностями пришли и какой результат хотите получить (ваша точка В)?\n\n✅ ОБЯЗАТЕЛЬСТВО НА КУРС:\nВ конце своего сообщения обязательно добавьте фразу:\n«Я обязуюсь внимательно изучать материалы курса, если я что-то не понял(а) — посмотреть урок еще раз. Выполнять практику, задавать вопросы Анне и кураторам. Быть терпеливым(ой) к себе и своей любимой собаке и идти к результату шаг за шагом».\n\n🎓 КАК ПРОХОДИТ ОБУЧЕНИЕ:\n• Модули открываются еженедельно в субботу в 12:00 (МСК) на платформе.\n• Все вопросы по урокам, разборы и обратную связь пишем прямо в этот чат.\n• Обязательно отмечайте нас, чтобы мы не пропустили вопрос!\n\n👩‍🏫 Создатель курса: Анна - [id765938|@timofeevapodbordog]\n🛡 Кураторы-кинологи: {kurators_text}\n❤️ Руководитель отдела заботы: Андрей - [id11335495|@id11335495]\n🛠 Технические специалисты: Техническая поддержка - [id1105209997|@tehpod_sobakovodpro], Никита - [id741919467|@attackpng]\n📢 Наше сообщество: https://vk.com/ssobakovod?utm_source=vk_edu_chat\n\n⚖ ПРАВИЛА ЧАТА:\n— Общаемся культурно, ненормативная лексика и спам запрещены.\n— Аудиосообщения запрещены (их используют только кураторы).\n— Сообщения, нарушающие правила, удаляются автоматически.\n\nНу что, начинаем наше путешествие в новый мир! ❤️"
VK_TEST_WELCOME_TEMPLATE = "Проверка учебного VK-чата\n\nПроверьте ссылку, закреп, приветствие модератора и права администратора.\n\nСотрудник: [id1105209997|Техническая поддержка]"
VK_INVITE_FALLBACK_TEMPLATE = "Здравствуйте! Вас не удалось добавить в учебный чат автоматически.\n\nВступите по ссылке: {invite_link}\n\nЕсли войти не получилось, напишите в техническую поддержку: https://vk.me/tehpod_sobakovodpro"
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


def _init_db() -> None:
    global _db_initialized
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
        db.execute("DELETE FROM people WHERE name IN ('Екатерина','ТГ куратор 1','ТГ куратор 2')")
        db.execute("UPDATE people SET kind='author',parity='any',updated_at=strftime('%s','now') WHERE name='Анна'")
        db.execute("UPDATE people SET kind='kurator',parity='any',updated_at=strftime('%s','now') WHERE name='Ирина'")
        db.execute(
            "UPDATE people SET kind='admin',parity='any',updated_at=strftime('%s','now') "
            "WHERE name IN ('Наталья','Андрей','Техническая поддержка','Никита')"
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
            "vk_invite_fallback": VK_INVITE_FALLBACK_TEMPLATE,
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
    global _ctx, _logger, _vk_bootstrap_subscription, _vk_pin_watchdog_task, _vk_invite_dispatch_task
    _ctx = ctx
    _logger = ctx.logger
    _ensure_db()
    token = _clean(os.environ.get("VK_GROUP_TOKEN"))
    group_id = int(_clean(os.environ.get("VK_GROUP_ID")) or 0)
    if not token or group_id <= 0:
        return
    if _vk_pin_watchdog_task is None or _vk_pin_watchdog_task.done():
        _vk_pin_watchdog_task = asyncio.create_task(
            _vk_pin_watchdog_loop(), name=f"{DEFAULT_MODULE_ID}-vk-pin-watchdog"
        )
    if _vk_invite_dispatch_task is None or _vk_invite_dispatch_task.done():
        _vk_invite_dispatch_task = asyncio.create_task(
            _vk_invite_dispatch_loop(), name=f"{DEFAULT_MODULE_ID}-vk-invite-dispatch"
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
    global _vk_bootstrap_subscription, _vk_pin_watchdog_task, _vk_invite_dispatch_task
    invite_task, _vk_invite_dispatch_task = _vk_invite_dispatch_task, None
    if invite_task is not None and not invite_task.done():
        invite_task.cancel()
    if invite_task is not None:
        try:
            await invite_task
        except asyncio.CancelledError:
            pass
    task, _vk_pin_watchdog_task = _vk_pin_watchdog_task, None
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        try:
            await task
        except asyncio.CancelledError:
            pass
    subscription, _vk_bootstrap_subscription = _vk_bootstrap_subscription, None
    if subscription is not None:
        await subscription.close()


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


def _sheet_link_write_value(platform: str, existing_link: Any, generated_link: Any) -> tuple[str, str]:
    current = _clean(existing_link)
    if current:
        return current, "preserved"
    if platform == "vk":
        return "", "waiting_manual_link"
    return _clean(generated_link), "filled"


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
    metadata_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
        params={"fields": "sheets.properties(sheetId,title)"},
        timeout=30,
    )
    metadata_response.raise_for_status()
    titles = {
        str((item.get("properties") or {}).get("sheetId")): _clean((item.get("properties") or {}).get("title"))
        for item in (metadata_response.json() or {}).get("sheets") or []
    }
    platform_titles: dict[str, str] = {}
    ranges: list[tuple[str, str]] = []
    for platform in ("telegram", "vk"):
        gid = _clean(sheet_ids.get(platform))
        title = titles.get(gid, "")
        if not title:
            raise RuntimeError(f"Лист gid={gid} не найден")
        platform_titles[platform] = title
        escaped = title.replace("'", "''")
        ranges.append((platform, f"'{escaped}'!A:B"))
    values_response = session.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet",
        params=[("ranges", value_range) for _platform, value_range in ranges] + [("majorDimension", "ROWS")],
        timeout=30,
    )
    values_response.raise_for_status()
    value_ranges = (values_response.json() or {}).get("valueRanges") or []
    data: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    waiting_manual = False
    for index, platform in enumerate(("telegram", "vk")):
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
        if action != "preserved":
            values = [[_clean(run.get("title")), link_value]] if platform != "vk" else [[_clean(run.get("title"))]]
            target_range = (
                f"'{escaped}'!A{row_number}:B{row_number}"
                if platform != "vk"
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
        update_response = session.post(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate",
            json={"valueInputOption": "RAW", "data": data},
            timeout=30,
        )
        update_response.raise_for_status()
    return {
        "ok": True,
        "status": "waiting_manual_vk_link" if waiting_manual else "synced",
        "stream_number": stream_number,
        "spreadsheet_id": spreadsheet_id,
        "manual_vk_link_required": waiting_manual,
        "updated": updated,
    }


async def _sync_chat_pair_to_sheet(course_key: str, stream_number: str, *, test_mode: bool) -> dict[str, Any]:
    if test_mode:
        return {"ok": True, "status": "skipped_test_mode"}
    pair = _ready_chat_pair(course_key, stream_number)
    missing = [platform for platform in ("telegram", "vk") if platform not in pair]
    if missing:
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
        result["sheet_sync_ok"] = True
        return result
    except Exception as exc:
        _log("error", "Chat links sheet sync failed course=%s stream=%s: %s", course_key, stream_number, exc)
        return {
            "ok": True,
            "status": "direct_ready_sheet_error",
            "sheet_sync_ok": False,
            "warning": _exc_text(exc),
        }


def _vk_admin_run(run_id: int | None = None) -> dict[str, Any] | None:
    _ensure_db()
    with _db() as db:
        if run_id:
            row = db.execute("SELECT * FROM runs WHERE id=? AND platform='vk'", (run_id,)).fetchone()
        else:
            row = db.execute("SELECT * FROM runs WHERE platform='vk' AND status IN ('needs_admins','needs_members','needs_vk_web_admins') ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


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
    for item in reversed(items):
        if not isinstance(item, dict) or int(item.get("from_id") or 0) != -group_id:
            continue
        if item.get("action"):
            continue
        if text and str(item.get("text") or "") != text:
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
                    for _attempt in range(3):
                        await asyncio.sleep(0.3)
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
    if member_id not in expected_ids:
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
    state = await _vk_admin_state(peer_id, sorted(expected_ids), token)
    promoted_ids = sorted(set(int(value) for value in (state.get("admins") or [])) & expected_ids)
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
    await asyncio.sleep(2)
    while True:
        try:
            await _reconcile_vk_course_pins_once()
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
    if not message or message["peer_id"] <= 2000000000 or message["from_id"] <= 0:
        return
    owned_row = _vk_owned_run(message["peer_id"])
    if owned_row is not None:
        try:
            await _promote_joined_vk_staff(owned_row, message)
            if message["action_type"]:
                await _restore_vk_course_pin(owned_row, message)
        except Exception as exc:
            _log("error", "VK community chat event automation failed peer_id=%s: %s", message["peer_id"], exc)
    row = _pending_vk_bootstrap_run(message["peer_id"])
    if row is None:
        return
    # A join service event arrives before VK grants the community message-history
    # access. Wait for the participant's first ordinary message instead of
    # recording a false send success or duplicating the welcome on retry.
    if message["action_type"]:
        response = dict(row.get("response") or {})
        response.pop("bootstrap_error", None)
        _persist_vk_bootstrap(row, response, "waiting_for_message")
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


async def _vk_invite_dispatch_loop() -> None:
    await asyncio.sleep(5)
    while True:
        try:
            await _dispatch_pending_vk_student_invites_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "VK student invite dispatch failed: %s", _exc_text(exc))
        await asyncio.sleep(60)


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
        link_sync = await _sync_chat_pair_to_sheet(course["key"], stream_number, test_mode=test_mode)
        prepared["student_invites"] = {
            "initial_added": len(added_student_ids),
            "delivery": "community_message_and_client_chat_links_page",
            "community_messages": "waiting_manual_link" if failed_student_ids else "not_needed",
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
    if not target_ids:
        result = {"ok": True, "skipped": True, "reason": "no_staff_members", "run_id": row["id"], "peer_id": peer_id}
        response_json.update({"admin_result": result, "needs_attention": False, "followup_status": "ok", "detail": ""})
        _update_run(int(row["id"]), "ok", response_json)
        return result
    state = await _vk_admin_state(peer_id, target_ids, token)
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
        "join_mode": "invite_link",
        "error": state.get("error"),
    }
    if missing_members:
        error = f"VK не подтвердил всех участников перед выдачей админок: missing_members={', '.join(map(str, missing_members))}"
        result = {"ok": False, "run_id": row["id"], "peer_id": peer_id, "members_result": members_result, "error": error}
        response_json.update({"members_result": members_result, "admin_result": result, "needs_attention": True, "followup_status": "needs_members", "detail": error})
        _update_run(int(row["id"]), "needs_members", response_json, error=error)
        return result
    missing_admins = list(state.get("missing_admins") or [])
    api_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "already_admin", "state": state}
    if missing_admins:
        api_result = await _vk_try_api_admins(peer_id, missing_admins, token)
        missing_admins = list(((api_result.get("state") or {}).get("missing_admins") or []))
    final_state = api_result.get("state") or state
    final_missing = list(final_state.get("missing_admins") or [])
    result = {
        "ok": not final_missing,
        "run_id": row["id"],
        "peer_id": peer_id,
        "members_result": members_result,
        "api": api_result,
        "state": final_state,
        "missing_admins": final_missing,
    }
    if final_missing:
        error = f"VK API не подтвердил роли администраторов: missing_admins={', '.join(map(str, final_missing))}"
        response_json.update({"admin_result": result, "needs_attention": True, "followup_status": "needs_admins", "detail": error})
        _update_run(int(row["id"]), "needs_admins", response_json, error=error)
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
                "SELECT id,title,chat_id,response_json,test_mode,status FROM runs WHERE platform=? ORDER BY id DESC",
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
            for bot_username in ["bullterrier_sobakovod_bot"]:
                try:
                    bot_entity = await client.get_entity(bot_username)
                    await client(functions.channels.InviteToChannelRequest(channel=channel, users=[bot_entity]))
                except Exception:
                    pass

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
        await asyncio.sleep(10 if test_mode else 180)
        for msg, topic_id, label in sent:
            try:
                await client(functions.messages.UpdatePinnedMessageRequest(peer=bot_channel, id=msg.id, silent=True))
                await asyncio.sleep(1)
            except Exception as exc:
                _log("warning", "Telegram pin failed %s: %s", label, exc)
        try:
            invite = await client(functions.messages.ExportChatInviteRequest(peer=channel))
            invite_link = invite.link
        except Exception as exc:
            _log("warning", "Telegram invite export failed: %s", exc)
            invite_link = ""
    response = {"message": "Group created successfully", "group_title": title, "group_link": invite_link, "course_choice": course["choice"], "test_mode": test_mode, "topic_ids": topic_ids, "curator_id": _selected_curator_id(stream_number, data.get("curator_id"))}
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
        "parity": _clean(data.get("parity")) or "any",
        "enabled": 1 if data.get("enabled", True) else 0,
        "note": _clean(data.get("note")),
    }
    with _db() as db:
        if data.get("id"):
            payload["id"] = int(data["id"])
            db.execute(
                """UPDATE people SET kind=:kind,name=:name,vk_id=:vk_id,vk_mention=:vk_mention,tg_ref=:tg_ref,parity=:parity,enabled=:enabled,note=:note,updated_at=strftime('%s','now') WHERE id=:id""",
                payload,
            )
            person_id = payload["id"]
        else:
            cur = db.execute(
                """INSERT INTO people(kind,name,vk_id,vk_mention,tg_ref,parity,enabled,note) VALUES(:kind,:name,:vk_id,:vk_mention,:tg_ref,:parity,:enabled,:note)""",
                payload,
            )
            person_id = cur.lastrowid
        db.commit()
    return {"ok": True, "id": person_id}


@router.delete("/people/{person_id}")
async def delete_person(person_id: int, request: Request):
    await _require_panel_access(request)
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
        rows = [dict(row) for row in db.execute("SELECT * FROM templates ORDER BY key").fetchall()]
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
    with _db() as db:
        db.execute(
            "INSERT INTO templates(key,body) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET body=excluded.body,updated_at=strftime('%s','now')",
            (key, body),
        )
        db.commit()
    return {"ok": True}


@router.get("/preview")
async def preview(stream_number: str = "51", start_date: str = "", course: str = "puppy", curator_id: str = "", test_mode: str = "false"):
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


@router.get("/vk-course-chats")
async def list_vk_course_chats(request: Request):
    await _require_panel_access(request)
    return await _vk_course_chat_inventory()


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
