from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import random
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

try:
    from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
except Exception:  # pragma: no cover - isolated local tests
    can_access_module = None
    require_admin = None
    verify_token_from_request = None

router = APIRouter()

VK_API_VERSION = "5.131"
SALEBOT_CHAT_LINK_CALLBACK = "get_potok_link"
STANDARD_SALEBOT_CLIENT_ID = "771116046"
DEFAULT_MODULE_ID = "course-chat-creator"
TEMPLATE_DEFAULTS_VERSION = "windsurf-2026-06-02"

_ctx = None
_logger = None


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
    {"kind": "kurator", "name": "Екатерина", "vk_id": "1025748213", "vk_mention": "[id1025748213|@psypuppy]", "tg_ref": "", "parity": "odd", "enabled": 1},
    {"kind": "kurator", "name": "Ирина", "vk_id": "413314992", "vk_mention": "[id413314992|@demidovair]", "tg_ref": "", "parity": "even", "enabled": 1},
    {"kind": "kurator", "name": "ТГ куратор 1", "vk_id": "", "vk_mention": "", "tg_ref": "+79206159472", "parity": "any", "enabled": 1},
    {"kind": "kurator", "name": "ТГ куратор 2", "vk_id": "", "vk_mention": "", "tg_ref": "+79818123970", "parity": "any", "enabled": 1},
    {"kind": "tech", "name": "Техническая поддержка", "vk_id": "1105209997", "vk_mention": "[id1105209997|@tehpod_sobakovodpro]", "tg_ref": "@Tech_kurator", "enabled": 1},
    {"kind": "tech", "name": "Никита", "vk_id": "741919467", "vk_mention": "[id741919467|@attackpng]", "tg_ref": "", "enabled": 1},
    {"kind": "admin", "name": "Андрей", "vk_id": "11335495", "vk_mention": "[id11335495|@id11335495]", "tg_ref": "", "enabled": 1},
]

VK_WELCOME_TEMPLATE = "🐾 Добро пожаловать в закрытый чат курса «{course_full_name}»! 🐾\n\nЯ очень рада, что вы здесь. Вы уже сделали важный шаг на пути к осознанному воспитанию вашей собаки.\n\n🗓 Поток №{stream_number}: Обучение стартует {date_start}\nВпереди у нас 11 недель практического обучения, поддержки и маленьких побед! 💪🏼🐶\n\n📍 ПЕРВЫЙ ШАГ — ЗНАКОМСТВО (ВИЗИТКА)\nПожалуйста, расскажите о себе и своем питомце в ОДНОМ сообщении по форме:\n1️⃣ Ваше имя и город\n2️⃣ Кличка собаки, возраст, порода/фенотип/дворняжка\n3️⃣ С какими трудностями пришли и какой результат хотите получить (ваша точка В)?\n\n✅ ОБЯЗАТЕЛЬСТВО НА КУРС:\nВ конце своего сообщения обязательно добавьте фразу:\n«Я обязуюсь внимательно изучать материалы курса, если я что-то не понял(а) — посмотреть урок еще раз. Выполнять практику, задавать вопросы Анне и кураторам. Быть терпеливым(ой) к себе и своей любимой собаке и идти к результату шаг за шагом».\n\n🎓 КАК ПРОХОДИТ ОБУЧЕНИЕ:\n• Модули открываются еженедельно в субботу в 12:00 (МСК) на платформе.\n• Все вопросы по урокам, разборы и обратную связь пишем прямо в этот чат.\n• Обязательно отмечайте нас, чтобы мы не пропустили вопрос!\n\n👩‍🏫 Создатель курса: Анна - [id765938|@timofeevapodbordog]\n🛡 Кураторы-кинологи: {kurators_text}\n❤️ Руководитель отдела заботы: Андрей - [id11335495|@id11335495]\n🛠 Технические специалисты: Техническая поддержка - [id1105209997|@tehpod_sobakovodpro], Никита - [id741919467|@attackpng]\n📢 Наше сообщество: https://vk.com/ssobakovod?utm_source=vk_edu_chat\n\n⚖ ПРАВИЛА ЧАТА:\n— Общаемся культурно, ненормативная лексика и спам запрещены.\n— Аудиосообщения запрещены (их используют только кураторы).\n— Сообщения, нарушающие правила, удаляются автоматически.\n\nНу что, начинаем наше путешествие в новый мир! ❤️"
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
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
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
        db.commit()


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


def _bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "y", "да"}


def _password() -> str:
    return _clean(os.environ.get("NEXUS_CHAT_CREATOR_PASSWORD") or os.environ.get("SBKVD_PROCESS_WEBHOOK_PASSWORD"))


def _check_password(data: dict[str, Any]) -> None:
    configured = _password()
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


def _selected_people(stream_number: str) -> dict[str, list[dict[str, Any]]]:
    is_even = _stream_is_even(stream_number)
    result: dict[str, list[dict[str, Any]]] = {"admins": [], "kurators": [], "authors": [], "techs": []}
    for person in _people(enabled=True):
        kind = person["kind"]
        if kind == "admin":
            result["admins"].append(person)
        elif kind == "author":
            result["authors"].append(person)
        elif kind == "tech":
            result["techs"].append(person)
        elif kind == "kurator":
            parity = person.get("parity") or "any"
            if parity == "any" or (parity == "even" and is_even) or (parity == "odd" and not is_even):
                result["kurators"].append(person)
    return result


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


def _tg_refs(people: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    for person in people:
        value = _clean(person.get("tg_ref"))
        if value and value not in result:
            result.append(value)
    return result


def _mentions(people: list[dict[str, Any]], platform: str) -> str:
    items: list[str] = []
    for person in people:
        if platform == "vk":
            ref = _clean(person.get("vk_mention")) or (_clean(person.get("vk_id")) and f"[id{person['vk_id']}|{person['name']}]")
        else:
            ref = _clean(person.get("tg_ref")) or _clean(person.get("name"))
        if ref:
            items.append(f"{person['name']} - {ref}" if platform == "vk" and " - " not in ref else ref)
    return ", ".join(items) if items else "не указаны"


def _template(key: str) -> str:
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


def _record_run(platform: str, title: str, stream_number: str, date_start: str, course_key: str, test_mode: bool, status: str, request_json: dict[str, Any], response_json: dict[str, Any] | None = None, error: str = "", link: str = "", chat_id: str = "") -> None:
    with _db() as db:
        db.execute(
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


async def _send_salebot(*, invite_link: str, stream_number: str, course_value: str, date_start: str, salebot_id: Any, vk: bool, test_mode: bool) -> None:
    api_key = _clean(os.environ.get("SALEBOT_API_KEY_3"))
    if test_mode or not api_key:
        return
    salebot_ids = [STANDARD_SALEBOT_CLIENT_ID]
    if salebot_id:
        salebot_ids.append(str(salebot_id))
    variables = {
        "link_potok": invite_link,
        "number_potok": stream_number,
        "course_potok": course_value,
        "date_potok": date_start,
    }
    if vk:
        variables["link_potok_vk"] = invite_link
    async with httpx.AsyncClient(timeout=30.0) as client:
        for client_id in dict.fromkeys(salebot_ids):
            await client.post(f"https://chatter.salebot.pro/api/{api_key}/save_variables", json={"client_id": client_id, "variables": variables})
            await asyncio.sleep(3)
            await client.get(f"https://chatter.salebot.pro/api/{api_key}/callback", params={"client_id": client_id, "message": SALEBOT_CHAT_LINK_CALLBACK})


async def _vk_method(method: str, params: dict[str, Any], token: str) -> Any:
    if not token:
        raise HTTPException(status_code=500, detail="VK token is not configured")
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


async def _create_vk_chat(data: dict[str, Any]) -> dict[str, Any]:
    _check_password(data)
    test_mode = _bool(data.get("test_mode"))
    token = _clean(os.environ.get("VK_TEST_USER_TOKEN") if test_mode and os.environ.get("VK_TEST_USER_TOKEN") else os.environ.get("VK_USER_TOKEN"))
    stream_number = _clean(data.get("stream_number") or "15")
    date_start = _clean(data.get("date_start") or data.get("start_date") or "17 марта")
    course = _course_by_input(data.get("course_type") or data.get("course_choice") or "puppy")
    title = _format_title(stream_number, date_start, course, "vk")
    selected = _selected_people(stream_number)
    chat_member_ids = _vk_ids(selected["admins"] + selected["authors"] + selected["kurators"] + selected["techs"])
    if test_mode:
        chat_member_ids = []
    create_params: dict[str, Any] = {"title": title}
    if chat_member_ids:
        create_params["user_ids"] = ",".join(map(str, chat_member_ids))
    create_resp = await _vk_method("messages.createChat", create_params, token)
    if isinstance(create_resp, dict) and "error" in create_resp and chat_member_ids:
        create_resp = await _vk_method("messages.createChat", {"title": title}, token)
    if isinstance(create_resp, dict) and "error" in create_resp:
        raise HTTPException(status_code=500, detail=create_resp["error"].get("error_msg") or create_resp["error"])
    chat_id = int(create_resp)
    peer_id = 2000000000 + chat_id
    await asyncio.sleep(0.5)
    await _vk_method("messages.editChat", {"chat_id": chat_id, "show_history": 1}, token)
    group_id = _clean(os.environ.get("VK_GROUP_ID"))
    if not test_mode and group_id:
        try:
            await asyncio.sleep(1)
            await _vk_method("messages.addChatUser", {"chat_id": chat_id, "user_id": -int(group_id)}, token)
        except Exception as exc:
            _log("warning", "VK group add failed: %s", exc)
    for user_id in chat_member_ids:
        try:
            await asyncio.sleep(0.2)
            await _vk_method("messages.addChatUser", {"chat_id": chat_id, "user_id": user_id}, token)
        except Exception:
            pass
    if not test_mode:
        for admin_id in chat_member_ids:
            await asyncio.sleep(0.5)
            await _vk_method("messages.setMemberRole", {"peer_id": peer_id, "member_id": admin_id, "role": "admin"}, token)
    photo = _asset_path("welcome_message_photo.jpg")
    if photo:
        attachment = await _upload_vk_message_photo(peer_id, photo, token)
        if attachment:
            await _vk_method("messages.send", {"peer_id": peer_id, "attachment": attachment, "random_id": 0}, token)
    welcome_text = _render_template("vk_welcome", course=course, stream_number=stream_number, date_start=date_start, selected=selected, platform="vk")
    welcome_resp = await _vk_method("messages.send", {"peer_id": peer_id, "message": welcome_text, "random_id": 0}, token)
    if isinstance(welcome_resp, int):
        await asyncio.sleep(2)
        await _vk_method("messages.pin", {"peer_id": peer_id, "message_id": welcome_resp}, token)
    invite_data = await _vk_method("messages.getInviteLink", {"peer_id": peer_id}, token)
    invite_link = invite_data.get("link", "") if isinstance(invite_data, dict) else ""
    log_chat_id = _clean(os.environ.get("VK_LOG_CHAT_ID"))
    if not test_mode and log_chat_id:
        try:
            await _vk_method("messages.send", {"peer_id": log_chat_id, "message": f"Новый VK чат создан\n{title}\n{invite_link}", "random_id": 0}, token)
        except Exception:
            pass
    await _send_salebot(invite_link=invite_link, stream_number=stream_number, course_value=course["key"], date_start=date_start, salebot_id=data.get("salebot_id"), vk=True, test_mode=test_mode)
    response = {"message": "Success! VK chat created.", "group_link": invite_link, "chat_id": chat_id, "peer_id": peer_id, "test_mode": test_mode, "title": title}
    _record_run("vk", title, stream_number, date_start, course["key"], test_mode, "ok", data, response, link=invite_link, chat_id=str(chat_id))
    return response


def _telegram_credentials() -> tuple[int, str, str]:
    api_id_raw = _clean(os.environ.get("TELEGRAM_API_ID"))
    api_hash = _clean(os.environ.get("TELEGRAM_API_HASH"))
    session_file = _clean(os.environ.get("TELEGRAM_SESSION_FILE")) or str(_data_dir() / "telegram.session")
    if not api_id_raw or not api_hash:
        raise HTTPException(status_code=500, detail="Telegram credentials are not configured")
    return int(api_id_raw), api_hash, session_file


def _format_date_russian(date_str: str) -> str:
    months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return f"{dt.day} {months[dt.month - 1]}"
    except Exception:
        return date_str


async def _create_tg_chat(data: dict[str, Any]) -> dict[str, Any]:
    _check_password(data)
    try:
        from telethon import TelegramClient, functions, types
        from telethon.tl.functions.channels import EditPhotoRequest
        from telethon.tl.functions.messages import CreateForumTopicRequest, EditForumTopicRequest, UpdatePinnedForumTopicRequest, UpdatePinnedMessageRequest
        from telethon.tl.types import InputChatUploadedPhoto
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Telethon is not installed: {exc}")
    stream_number = _clean(data.get("stream_number"))
    date_start = _clean(data.get("start_date") or data.get("date_start"))
    course = _course_by_input(data.get("course_choice") or data.get("course_type") or "puppy")
    test_mode = _bool(data.get("test_mode"))
    if not stream_number or not date_start:
        raise HTTPException(status_code=400, detail="Missing required parameters: stream_number, start_date")
    title = _format_title(stream_number, date_start, course, "tg")
    selected = _selected_people(stream_number)
    admins = _tg_refs(selected["admins"])
    kurators = _tg_refs(selected["kurators"])
    authors = _tg_refs(selected["authors"])
    techs = _tg_refs(selected["techs"])
    all_users = [] if test_mode else list(dict.fromkeys(admins + kurators + authors + techs))
    api_id, api_hash, session_file = _telegram_credentials()
    client = TelegramClient(session_file, api_id, api_hash)
    await client.connect()
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

        try:
            await client(EditForumTopicRequest(peer=channel, topic_id=1, title="Инфо"))
            await asyncio.sleep(1)
            topic_vizitka = await client(CreateForumTopicRequest(peer=channel, title="Визитка", icon_emoji_id=5237999392438371490, random_id=random.randint(1, 2**31 - 1)))
            topic_ids["vizitka"] = get_topic_id(topic_vizitka)
            topic_obuchenie = await client(CreateForumTopicRequest(peer=channel, title="Обучение", icon_emoji_id=5357419403325481346, random_id=random.randint(1, 2**31 - 1)))
            topic_ids["obuchenie"] = get_topic_id(topic_obuchenie)
            topic_boltalka = await client(CreateForumTopicRequest(peer=channel, title="Болталка", icon_emoji_id=5417915203100613993, random_id=random.randint(1, 2**31 - 1)))
            topic_ids["boltalka"] = get_topic_id(topic_boltalka)
            await client(UpdatePinnedForumTopicRequest(peer=channel, topic_id=1, pinned=True))
        except Exception as exc:
            _log("warning", "Telegram topic setup failed: %s", exc)
        photo = _asset_path("welcome_message_photo.jpg")
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
                        admin_rights=types.ChatAdminRights(change_info=True, post_messages=True, edit_messages=True, delete_messages=True, ban_users=True, invite_users=True, pin_messages=True, add_admins=True, manage_call=True),
                        rank=rank,
                    ))
                    await asyncio.sleep(random.uniform(1, 3))
                except Exception as exc:
                    _log("warning", "Telegram invite/admin failed for %s: %s", user, exc)
                    await asyncio.sleep(random.uniform(5, 10))

        if not test_mode:
            await invite_and_admin(admins, "admin")
            await invite_and_admin(kurators, "Куратор школы")
            await invite_and_admin(authors, "Автор курса")
            await invite_and_admin(techs, "Тех. отдел")
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
                        await client(EditForumTopicRequest(peer=channel, topic_id=topic_id, closed=True))
                    except Exception as exc:
                        _log("warning", "Telegram info topic close failed: %s", exc)
            except Exception as exc:
                _log("warning", "Telegram message failed %s: %s", label, exc)
        await asyncio.sleep(10 if test_mode else 180)
        for msg, topic_id, label in sent:
            try:
                await client(UpdatePinnedMessageRequest(peer=bot_channel, id=msg.id, silent=True))
                await asyncio.sleep(1)
            except Exception as exc:
                _log("warning", "Telegram pin failed %s: %s", label, exc)
        if not test_mode:
            start = time.monotonic()
            added = 0
            for user in valid_users:
                if time.monotonic() - start > 5 * 60:
                    break
                try:
                    entity = await client.get_entity(user)
                    await client(functions.channels.InviteToChannelRequest(channel=channel, users=[entity]))
                    added += 1
                    await asyncio.sleep(random.uniform(10, 20))
                except Exception as exc:
                    _log("warning", "Telegram user add failed %s: %s", user, exc)
                    await asyncio.sleep(random.uniform(20, 40))
        try:
            invite = await client(functions.messages.ExportChatInviteRequest(peer=channel))
            invite_link = invite.link
        except Exception as exc:
            _log("warning", "Telegram invite export failed: %s", exc)
            invite_link = ""
    await _send_salebot(invite_link=invite_link, stream_number=stream_number, course_value=course["choice"], date_start=date_start, salebot_id=data.get("salebot_id"), vk=False, test_mode=test_mode)
    response = {"message": "Group created successfully", "group_title": title, "group_link": invite_link, "course_choice": course["choice"], "test_mode": test_mode}
    _record_run("telegram", title, stream_number, date_start, course["key"], test_mode, "ok", data, response, link=invite_link, chat_id="")
    return response


@router.post("/vk/create")
@router.post("/process_vk")
async def process_vk(request: Request):
    data = await request.json()
    try:
        return JSONResponse(await _create_vk_chat(data))
    except Exception as exc:
        stream_number = _clean(data.get("stream_number"))
        date_start = _clean(data.get("date_start") or data.get("start_date"))
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
        date_start = _clean(data.get("date_start") or data.get("start_date"))
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
    if platform == "vk":
        return await process_vk(request)
    if platform in {"tg", "telegram"}:
        return await process6(request)
    raise HTTPException(status_code=400, detail="platform must be vk or telegram")


@router.get("/status")
async def status():
    return {
        "ok": True,
        "env": {
            "password": bool(_password()),
            "vk_user_token": bool(os.environ.get("VK_USER_TOKEN")),
            "vk_test_user_token": bool(os.environ.get("VK_TEST_USER_TOKEN")),
            "vk_group_token": bool(os.environ.get("VK_GROUP_TOKEN")),
            "telegram_api": bool(os.environ.get("TELEGRAM_API_ID") and os.environ.get("TELEGRAM_API_HASH")),
            "telegram_session_file": _clean(os.environ.get("TELEGRAM_SESSION_FILE")),
            "salebot": bool(os.environ.get("SALEBOT_API_KEY_3")),
        },
        "asset_welcome_photo": bool(_asset_path("welcome_message_photo.jpg")),
    }


@router.get("/people")
async def list_people(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "items": _people(enabled=False)}


@router.post("/people")
async def upsert_person(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    kind = _clean(data.get("kind"))
    name = _clean(data.get("name"))
    if kind not in {"admin", "kurator", "author", "tech"}:
        raise HTTPException(status_code=400, detail="Invalid kind")
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    payload = {
        "kind": kind,
        "name": name,
        "vk_id": _clean(data.get("vk_id")),
        "vk_mention": _clean(data.get("vk_mention")),
        "tg_ref": _clean(data.get("tg_ref")),
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
    with _db() as db:
        db.execute("DELETE FROM people WHERE id=?", (person_id,))
        db.commit()
    return {"ok": True}


@router.get("/courses")
async def list_courses(request: Request):
    await _require_panel_access(request)
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM courses ORDER BY choice, key").fetchall()]
    return {"ok": True, "items": rows}


@router.post("/courses")
async def upsert_course(request: Request):
    await _require_panel_access(request)
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
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM templates ORDER BY key").fetchall()]
    return {"ok": True, "items": rows}


@router.post("/templates")
async def update_template(request: Request):
    await _require_panel_access(request)
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
async def preview(stream_number: str = "51", start_date: str = "01.06.2026", course: str = "puppy"):
    course_row = _course_by_input(course)
    selected = _selected_people(stream_number)
    return {
        "ok": True,
        "vk_title": _format_title(stream_number, start_date, course_row, "vk"),
        "tg_title": _format_title(stream_number, start_date, course_row, "tg"),
        "selected": selected,
        "vk_welcome": _render_template("vk_welcome", course=course_row, stream_number=stream_number, date_start=start_date, selected=selected, platform="vk"),
        "tg_welcome": _render_template("tg_welcome", course=course_row, stream_number=stream_number, date_start=start_date, selected=selected, platform="tg"),
    }


@router.get("/runs")
async def list_runs(request: Request, limit: int = 50):
    await _require_panel_access(request)
    limit = max(1, min(200, int(limit)))
    with _db() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
    return {"ok": True, "items": rows}
