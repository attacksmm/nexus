from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import random
import re
import shutil
import sqlite3
import time
from urllib.parse import parse_qs, urlparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

try:
    from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
except Exception:  # pragma: no cover - isolated local tests
    can_access_module = None
    require_admin = None
    verify_token_from_request = None

router = APIRouter()

VK_API_VERSION = "5.131"
DEFAULT_MODULE_ID = "course-chat-creator"
TEMPLATE_DEFAULTS_VERSION = "windsurf-2026-06-02-full"
COURSE_CHAT_TITLE_RE = re.compile(r"^\s*\d+\.\s*\d{2}\.\d{2}\.\d{4}\s*-\s*(Курс Щенок\. Современный Собаковод|Современный Собаковод\b)", re.IGNORECASE)

_ctx = None
_logger = None
_db_initialized = False
_tg_auth_pending: dict[str, dict[str, Any]] = {}
_vk_web_lock = asyncio.Lock()
_vk_web_playwright: Any | None = None
_vk_web_context: Any | None = None
_vk_web_page: Any | None = None


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
        db.execute("UPDATE people SET kind='author',parity='any',enabled=1,updated_at=strftime('%s','now') WHERE name='Анна'")
        db.execute("UPDATE people SET kind='kurator',parity='any',enabled=1,updated_at=strftime('%s','now') WHERE name='Ирина'")
        db.execute(
            "UPDATE people SET kind='admin',parity='any',enabled=1,updated_at=strftime('%s','now') "
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


def _record_run(platform: str, title: str, stream_number: str, date_start: str, course_key: str, test_mode: bool, status: str, request_json: dict[str, Any], response_json: dict[str, Any] | None = None, error: str = "", link: str = "", chat_id: str = "") -> None:
    _ensure_db()
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


def _vk_admin_run(run_id: int | None = None) -> dict[str, Any] | None:
    _ensure_db()
    with _db() as db:
        if run_id:
            row = db.execute("SELECT * FROM runs WHERE id=? AND platform='vk'", (run_id,)).fetchone()
        else:
            row = db.execute("SELECT * FROM runs WHERE platform='vk' AND status='needs_vk_web_admins' ORDER BY id DESC LIMIT 1").fetchone()
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


def _vk_web_profile_dir() -> Path:
    return Path(_clean(os.environ.get("VK_WEB_PROFILE_DIR")) or (_data_dir() / "vk-web-profile"))


def _vk_web_screenshot_path() -> Path:
    return _data_dir() / "vk-web-last.png"


async def _vk_web_start() -> tuple[Any, Any]:
    global _vk_web_playwright, _vk_web_context, _vk_web_page
    async with _vk_web_lock:
        if _vk_web_context is not None:
            pages = list(getattr(_vk_web_context, "pages", []) or [])
            if _vk_web_page is None or getattr(_vk_web_page, "is_closed", lambda: True)():
                _vk_web_page = pages[0] if pages else await _vk_web_context.new_page()
            return _vk_web_context, _vk_web_page
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Playwright is not installed: {exc}")
        profile_dir = _vk_web_profile_dir()
        profile_dir.mkdir(parents=True, exist_ok=True)
        _vk_web_playwright = await async_playwright().start()
        _vk_web_context = await _vk_web_playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=_clean(os.environ.get("VK_WEB_HEADLESS") or "1").lower() not in {"0", "false", "no"},
            viewport={"width": 1366, "height": 900},
            locale="ru-RU",
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        pages = list(getattr(_vk_web_context, "pages", []) or [])
        _vk_web_page = pages[0] if pages else await _vk_web_context.new_page()
        _vk_web_page.set_default_timeout(10000)
        return _vk_web_context, _vk_web_page


async def _vk_web_stop() -> None:
    global _vk_web_playwright, _vk_web_context, _vk_web_page
    async with _vk_web_lock:
        context, playwright = _vk_web_context, _vk_web_playwright
        _vk_web_context = None
        _vk_web_playwright = None
        _vk_web_page = None
    if context is not None:
        try:
            await context.close()
        except Exception:
            pass
    if playwright is not None:
        try:
            await playwright.stop()
        except Exception:
            pass


async def _vk_web_save_screenshot(page: Any | None = None) -> str:
    if page is None:
        _, page = await _vk_web_start()
    path = _vk_web_screenshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    await page.screenshot(path=str(path), full_page=False)
    return str(path)


async def _vk_web_is_authorized(page: Any, *, navigate: bool = True) -> tuple[bool, str]:
    if navigate:
        await page.goto("https://vk.com/im", wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(2500)
    url = _clean(getattr(page, "url", ""))
    cookies = await page.context.cookies("https://vk.com")
    cookie_names = {cookie.get("name") for cookie in cookies}
    body = ""
    try:
        body = (await page.locator("body").inner_text(timeout=3000)).lower()
    except Exception:
        body = ""
    login_words = ("войти", "телефон или почта", "qr", "код", "пароль")
    app_words = ("мессенджер", "сообщения", "новости", "моя страница", "друзья")
    if "not_robot_captcha" in body or "captcha-widget" in body or await page.locator("iframe[src*='not_robot_captcha'], [data-test-id='captcha-widget']").count():
        return False, "captcha_required"
    has_session_cookie = bool(cookie_names.intersection({"remixsid", "remixusid", "remixua"}))
    looks_like_login = any(word in body for word in login_words) and not any(word in body for word in app_words)
    if any(part in url.lower() for part in ("/login", "act=login", "login.vk.", "connect.vk.")):
        return False, "waiting_auth"
    if has_session_cookie and not looks_like_login:
        return True, "authorized"
    if any(word in body for word in app_words) and not looks_like_login:
        return True, "authorized"
    return False, "waiting_auth"


async def _vk_web_auth_state(*, open_browser: bool = False, screenshot: bool = False) -> dict[str, Any]:
    profile_dir = _vk_web_profile_dir()
    state = {
        "available": True,
        "browser_open": _vk_web_context is not None,
        "authorized": False,
        "status": "not_opened",
        "profile_dir": str(profile_dir),
        "screenshot": False,
        "screenshot_url": "../api/vk-web/auth/screenshot",
    }
    if not open_browser and _vk_web_context is None:
        state["profile_exists"] = profile_dir.exists()
        return state
    try:
        _, page = await _vk_web_start()
        authorized, status_value = await _vk_web_is_authorized(page, navigate=open_browser)
        state.update({"browser_open": True, "authorized": authorized, "status": status_value, "url": getattr(page, "url", "")})
        if screenshot:
            await _vk_web_save_screenshot(page)
            state["screenshot"] = True
    except HTTPException:
        raise
    except Exception as exc:
        state.update({"status": "error", "error": _exc_text(exc)})
        try:
            await _vk_web_save_screenshot()
            state["screenshot"] = True
        except Exception:
            pass
    return state


async def _vk_web_interaction_state(page: Any) -> dict[str, Any]:
    authorized, status_value = await _vk_web_is_authorized(page, navigate=False)
    await _vk_web_save_screenshot(page)
    return {
        "available": True,
        "browser_open": True,
        "authorized": authorized,
        "status": status_value,
        "url": getattr(page, "url", ""),
        "profile_dir": str(_vk_web_profile_dir()),
        "screenshot": True,
        "screenshot_url": "../api/vk-web/auth/screenshot",
    }


async def _vk_web_require_authorized() -> Any:
    _, page = await _vk_web_start()
    authorized, status_value = await _vk_web_is_authorized(page, navigate=True)
    if not authorized:
        await _vk_web_save_screenshot(page)
        if status_value == "captcha_required":
            raise HTTPException(status_code=409, detail="VK требует проверку «не робот». Откройте вкладку «VK Авторизация», пройдите проверку и повторите создание/выдачу админок.")
        raise HTTPException(status_code=409, detail="Нужно авторизовать ВКонтакте во вкладке «Авторизация ВКонтакте».")
    return page


async def _vk_web_raise_if_captcha(page: Any) -> None:
    if await page.locator("iframe[src*='not_robot_captcha'], [data-test-id='captcha-widget']").count():
        await _vk_web_save_screenshot(page)
        raise RuntimeError("VK требует проверку «не робот». Откройте вкладку «VK Авторизация», пройдите проверку и повторите выдачу админок.")


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
        for user_id in missing:
            try:
                await _vk_method("messages.addChatUser", {"chat_id": chat_id, "user_id": user_id}, token)
            except Exception:
                pass
            await asyncio.sleep(0.4)
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


async def _vk_web_click_first(page: Any, selectors: list[str], *, timeout: int = 2500) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first()
            await locator.wait_for(state="visible", timeout=timeout)
            await locator.click(timeout=timeout)
            await page.wait_for_timeout(700)
            return True
        except Exception:
            continue
    return False


async def _vk_web_open_members(page: Any, peer_id: int) -> None:
    chat_id = peer_id - 2000000000
    await page.goto(f"https://vk.com/im?sel=c{chat_id}", wait_until="domcontentloaded", timeout=45000)
    await page.wait_for_timeout(3000)
    await _vk_web_raise_if_captcha(page)
    try:
        await page.get_by_text(re.compile(r"\d+\s+участник", re.IGNORECASE)).click(timeout=5000)
        await page.wait_for_timeout(2500)
        await _vk_web_raise_if_captcha(page)
    except Exception:
        await _vk_web_raise_if_captcha(page)
        pass
    if await _vk_web_click_first(page, [
        "[aria-label*='Информация']",
        "[aria-label*='информация']",
        "[data-testid*='conversation_header']",
        ".ConvoHeader",
        ".im-page--title-main",
        ".im-page--chat-header",
    ]):
        await _vk_web_raise_if_captcha(page)
        await _vk_web_click_first(page, [
            "text=/Участники/i",
            "text=/участник/i",
            "[href*='members']",
            "[data-testid*='members']",
        ], timeout=3500)
    await page.wait_for_timeout(1500)
    await _vk_web_raise_if_captcha(page)


async def _vk_web_promote_one(page: Any, peer_id: int, user_id: int, person: dict[str, Any] | None = None) -> None:
    await _vk_web_open_members(page, peer_id)
    screen = _vk_screen_name((person or {}).get("vk_id")) or _vk_screen_name((person or {}).get("vk_mention"))
    found = await page.evaluate(
        """({userId, screen}) => {
            const needles = [`/id${userId}`, `sel=${userId}`, `/${screen}`].filter(Boolean).map(String);
            const links = Array.from(document.querySelectorAll('a[href]'));
            const link = links.find((a) => needles.some((n) => a.href.includes(n)));
            if (!link) return false;
            let row = link;
            let node = link;
            for (let i = 0; i < 8 && node; i += 1) {
                const box = node.getBoundingClientRect();
                const hasAction = !!node.querySelector('button, [role="button"], [aria-label]');
                if (box.width > 420 || hasAction || node.matches('[role="listitem"], .vkuiSimpleCell, .ListItem, .im-member, .nim-dialog, li')) {
                    row = node;
                }
                node = node.parentElement;
            }
            row.scrollIntoView({block: 'center'});
            row.setAttribute('data-nexus-target-member', String(userId));
            return true;
        }""",
        {"userId": user_id, "screen": screen},
    )
    if not found:
        raise RuntimeError(f"VK member {user_id} was not found in conversation members UI")
    row = page.locator(f"[data-nexus-target-member='{user_id}']").first()
    await row.hover(timeout=5000)
    clicked_menu = await _vk_web_click_first(page, [
        f"[data-nexus-target-member='{user_id}'] [aria-label*='…']",
        f"[data-nexus-target-member='{user_id}'] [aria-label*='Ещё']",
        f"[data-nexus-target-member='{user_id}'] [aria-label*='Еще']",
        f"[data-nexus-target-member='{user_id}'] [aria-label*='Действ']",
        f"[data-nexus-target-member='{user_id}'] .vkuiIconButton",
        f"[data-nexus-target-member='{user_id}'] .vkuiTappable",
        f"[data-nexus-target-member='{user_id}'] button",
        f"[data-nexus-target-member='{user_id}'] [role='button']",
    ], timeout=2500)
    if not clicked_menu:
        await row.click(button="right", timeout=5000)
        await page.wait_for_timeout(700)
    if not await _vk_web_click_first(page, [
        "text=/Назначить администратором/i",
        "text=/Сделать администратором/i",
        "text=/Назначить админ/i",
    ], timeout=4000):
        raise RuntimeError(f"VK admin action was not found for member {user_id}")
    await page.wait_for_timeout(1500)
    await _vk_web_click_first(page, ["text=/Подтвердить/i", "text=/Назначить/i", "text=/Да/i"], timeout=1500)


async def _vk_web_promote_admins(peer_id: int, target_people: list[dict[str, Any]], target_ids: list[int], token: str) -> dict[str, Any]:
    page = await _vk_web_require_authorized()
    by_id: dict[int, dict[str, Any]] = {}
    for person in target_people:
        for user_id in await _resolve_vk_people_ids([person], token):
            by_id[user_id] = person
    results: list[dict[str, Any]] = []
    for user_id in target_ids:
        try:
            await _vk_web_promote_one(page, peer_id, user_id, by_id.get(user_id))
            state = await _vk_admin_state(peer_id, [user_id], token)
            ok = not state.get("missing_admins")
            results.append({"member_id": user_id, "ok": ok, "state": state})
        except Exception as exc:
            await _vk_web_save_screenshot(page)
            results.append({"member_id": user_id, "ok": False, "error": _exc_text(exc), "screenshot_url": "../api/vk-web/auth/screenshot"})
    final_state = await _vk_admin_state(peer_id, target_ids, token)
    return {"ok": not final_state.get("missing_admins"), "results": results, "state": final_state, "screenshot_url": "../api/vk-web/auth/screenshot"}


async def _create_vk_chat(data: dict[str, Any], *, trusted: bool = False) -> dict[str, Any]:
    _check_password(data, trusted=trusted)
    test_mode = _bool(data.get("test_mode"))
    token = _clean(os.environ.get("VK_TEST_USER_TOKEN") if test_mode and os.environ.get("VK_TEST_USER_TOKEN") else os.environ.get("VK_USER_TOKEN"))
    stream_number = _clean(data.get("stream_number") or "15")
    date_start = _clean(data.get("date_start") or data.get("start_date") or "17 марта")
    course = _course_by_input(data.get("course_type") or data.get("course_choice") or "puppy")
    title = _format_title(stream_number, date_start, course, "vk")
    selected = _selected_people(stream_number, data.get("curator_id"))
    staff_people = selected["admins"] + selected["authors"] + selected["kurators"] + selected["techs"]
    chat_member_ids = await _resolve_vk_people_ids(staff_people, token)
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
    members_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "test_mode" if test_mode else "no_staff_members"}
    if not test_mode and chat_member_ids:
        members_result = await _vk_wait_for_chat_members(chat_id, peer_id, chat_member_ids, token)
    photo = _avatar_path()
    if photo:
        await _upload_vk_chat_photo(peer_id, photo, token)
    welcome_photo = _asset_path("welcome_message_photo.jpg")
    if welcome_photo:
        attachment = await _upload_vk_message_photo(peer_id, welcome_photo, token)
        if attachment:
            await _vk_method("messages.send", {"peer_id": peer_id, "attachment": attachment, "random_id": random.randint(1, 2**31 - 1)}, token)
            await asyncio.sleep(1)
    welcome_text = _render_template("vk_welcome", course=course, stream_number=stream_number, date_start=date_start, selected=selected, platform="vk")
    welcome_resp = await _vk_method("messages.send", {"peer_id": peer_id, "message": welcome_text, "random_id": random.randint(1, 2**31 - 1)}, token)
    if isinstance(welcome_resp, int):
        await asyncio.sleep(2)
        await _vk_method("messages.pin", {"peer_id": peer_id, "message_id": welcome_resp}, token)
    admin_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "test_mode" if test_mode else "no_staff_members"}
    run_status = "ok"
    run_error = ""
    if not test_mode and chat_member_ids:
        if members_result.get("missing"):
            missing_members = ", ".join(map(str, members_result.get("missing") or []))
            admin_result = {"ok": False, "skipped": True, "reason": "members_missing", "missing_members": members_result.get("missing") or []}
            run_status = "needs_members"
            run_error = f"VK не подтвердил всех участников после наполнения беседы: missing_members={missing_members}"
        else:
            await asyncio.sleep(3)
            api_result = await _vk_try_api_admins(peer_id, chat_member_ids, token)
            admin_result = api_result
            missing_admins = list(((api_result.get("state") or {}).get("missing_admins") or []))
            if missing_admins:
                try:
                    web_result = await _vk_web_promote_admins(peer_id, staff_people, missing_admins, token)
                except Exception as exc:
                    final_state = await _vk_admin_state(peer_id, chat_member_ids, token)
                    web_result = {
                        "ok": False,
                        "error": _exc_text(exc),
                        "state": final_state,
                        "screenshot_url": "../api/vk-web/auth/screenshot",
                    }
                admin_result = {"ok": web_result.get("ok"), "api": api_result, "web": web_result}
                missing = list((web_result.get("state") or {}).get("missing_admins") or [])
                if missing:
                    run_status = "needs_vk_web_admins"
                    run_error = f"VK Web не смог выдать админки после наполнения беседы: missing_admins={', '.join(map(str, missing))}"
    invite_data = await _vk_method("messages.getInviteLink", {"peer_id": peer_id}, token)
    invite_link = invite_data.get("link", "") if isinstance(invite_data, dict) else ""
    response = {
        "message": "Success! VK chat created." if run_status == "ok" else "VK chat created, but follow-up action is required.",
        "group_link": invite_link,
        "chat_id": chat_id,
        "peer_id": peer_id,
        "test_mode": test_mode,
        "title": title,
        "curator_id": _selected_curator_id(stream_number, data.get("curator_id")),
        "members_result": members_result,
        "admin_result": admin_result,
        "needs_attention": run_status != "ok",
        "followup_status": run_status,
        "detail": run_error,
    }
    _record_run("vk", title, stream_number, date_start, course["key"], test_mode, run_status, data, response, error=run_error, link=invite_link, chat_id=str(chat_id))
    return response


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
    chat_id = peer_id - 2000000000
    test_mode = bool(row.get("test_mode"))
    token = _clean(os.environ.get("VK_TEST_USER_TOKEN") if test_mode and os.environ.get("VK_TEST_USER_TOKEN") else os.environ.get("VK_USER_TOKEN"))
    stream_number = _clean(row.get("stream_number") or request_json.get("stream_number"))
    selected = _selected_people(stream_number, request_json.get("curator_id"))
    staff_people = selected["admins"] + selected["authors"] + selected["kurators"] + selected["techs"]
    target_ids = await _resolve_vk_people_ids(staff_people, token)
    if not target_ids:
        result = {"ok": True, "skipped": True, "reason": "no_staff_members", "run_id": row["id"], "peer_id": peer_id}
        response_json.update({"admin_result": result, "needs_attention": False, "followup_status": "ok", "detail": ""})
        _update_run(int(row["id"]), "ok", response_json)
        return result
    members_result = await _vk_wait_for_chat_members(chat_id, peer_id, target_ids, token, timeout_seconds=20)
    if members_result.get("missing"):
        missing_members = list(members_result.get("missing") or [])
        error = f"VK не подтвердил всех участников перед выдачей админок: missing_members={', '.join(map(str, missing_members))}"
        result = {"ok": False, "run_id": row["id"], "peer_id": peer_id, "members_result": members_result, "error": error}
        response_json.update({"members_result": members_result, "admin_result": result, "needs_attention": True, "followup_status": "needs_members", "detail": error})
        _update_run(int(row["id"]), "needs_members", response_json, error=error)
        return result
    state = await _vk_admin_state(peer_id, target_ids, token)
    missing_admins = list(state.get("missing_admins") or [])
    api_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "already_admin", "state": state}
    if missing_admins:
        api_result = await _vk_try_api_admins(peer_id, missing_admins, token)
        missing_admins = list(((api_result.get("state") or {}).get("missing_admins") or []))
    web_result: dict[str, Any] = {"ok": True, "skipped": True, "reason": "api_completed"}
    if missing_admins:
        try:
            web_result = await _vk_web_promote_admins(peer_id, staff_people, missing_admins, token)
        except Exception as exc:
            final_state = await _vk_admin_state(peer_id, target_ids, token)
            web_result = {
                "ok": False,
                "error": _exc_text(exc),
                "state": final_state,
                "screenshot_url": "../api/vk-web/auth/screenshot",
            }
    final_state = (web_result.get("state") or api_result.get("state") or state)
    final_missing = list(final_state.get("missing_admins") or [])
    result = {
        "ok": not final_missing,
        "run_id": row["id"],
        "peer_id": peer_id,
        "members_result": members_result,
        "api": api_result,
        "web": web_result,
        "state": final_state,
        "missing_admins": final_missing,
    }
    if final_missing:
        error = f"VK Web не смог выдать админки: missing_admins={', '.join(map(str, final_missing))}"
        response_json.update({"admin_result": result, "needs_attention": True, "followup_status": "needs_vk_web_admins", "detail": error})
        _update_run(int(row["id"]), "needs_vk_web_admins", response_json, error=error)
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
    return _clean(os.environ.get("TELEGRAM_MTPROTO_PROXY_URL") or os.environ.get("TELEGRAM_PROXY_URL"))


def _telegram_proxy_config() -> tuple[Any | None, tuple[str, int, str] | None]:
    raw = _telegram_proxy_url()
    if not raw:
        return None, None
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
    token = _clean(os.environ.get("VK_USER_TOKEN"))
    if not token:
        return _broadcast_empty_status("VK_USER_TOKEN is not configured")
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
    token = _clean(os.environ.get("VK_USER_TOKEN"))
    peer_id = _clean(chat.get("peer_id"))
    if not token:
        return False, "", "VK_USER_TOKEN is not configured"
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
        if response:
            return True, str(response), ""
        last_error = "VK did not return message id"
    return False, "", last_error


async def _delete_vk_broadcast_message(peer_id: str, message_id: str) -> tuple[bool, str]:
    token = _clean(os.environ.get("VK_USER_TOKEN"))
    if not token:
        return False, "VK_USER_TOKEN is not configured"
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


async def _remove_vk_from_course_chats(target: str, *, dry_run: bool = True, limit: int = 200) -> dict[str, Any]:
    token = _clean(os.environ.get("VK_USER_TOKEN"))
    user_id = await _resolve_vk_target_id(target, token)
    touched: list[dict[str, Any]] = []
    offset = 0
    while offset < limit:
        data = await _vk_method("messages.getConversations", {"count": min(200, limit - offset), "offset": offset}, token)
        items = data.get("items", []) if isinstance(data, dict) else []
        if not items:
            break
        for item in items:
            conv = item.get("conversation", {})
            peer = conv.get("peer", {})
            peer_id = int(peer.get("id", 0) or 0)
            chat_settings = conv.get("chat_settings", {}) or {}
            title = chat_settings.get("title", "")
            if peer_id <= 2000000000 or not _is_course_chat_title(title):
                continue
            chat_id = peer_id - 2000000000
            present = False
            try:
                members = await _vk_method("messages.getConversationMembers", {"peer_id": peer_id}, token)
                profiles = members.get("profiles", []) if isinstance(members, dict) else []
                present = any(int(p.get("id", 0)) == user_id for p in profiles)
            except Exception:
                present = True
            if not present:
                touched.append({"platform": "vk", "title": title, "peer_id": peer_id, "status": "not_found", "present": False})
                continue
            if not dry_run:
                result = await _vk_method("messages.removeChatUser", {"chat_id": chat_id, "member_id": user_id}, token)
                if isinstance(result, dict) and "error" in result:
                    touched.append({"platform": "vk", "title": title, "peer_id": peer_id, "status": "error", "error": result["error"]})
                    continue
            touched.append({"platform": "vk", "title": title, "peer_id": peer_id, "status": "would_remove" if dry_run else "removed", "present": present})
        if len(items) < 200:
            break
        offset += len(items)
    return {"ok": True, "platform": "vk", "target": user_id, "dry_run": dry_run, "items": touched}


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
    date_start = _clean(data.get("start_date") or data.get("date_start"))
    course = _course_by_input(data.get("course_choice") or data.get("course_type") or "puppy")
    test_mode = _bool(data.get("test_mode"))
    if not stream_number or not date_start:
        raise HTTPException(status_code=400, detail="Missing required parameters: stream_number, start_date")
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
    try:
        if platform == "vk":
            return JSONResponse(await _create_vk_chat(data, trusted=True))
        if platform in {"tg", "telegram"}:
            return JSONResponse(await _create_tg_chat(data, trusted=True))
        raise HTTPException(status_code=400, detail="platform must be vk or telegram")
    except Exception as exc:
        if platform in {"vk", "tg", "telegram"}:
            stream_number = _clean(data.get("stream_number"))
            date_start = _clean(data.get("date_start") or data.get("start_date"))
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


@router.get("/vk-web/auth/status")
async def vk_web_auth_status(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "vk_web": await _vk_web_auth_state(open_browser=True, screenshot=True)}


@router.post("/vk-web/auth/open")
async def vk_web_auth_open(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "vk_web": await _vk_web_auth_state(open_browser=True, screenshot=True)}


@router.get("/vk-web/auth/screenshot")
async def vk_web_auth_screenshot(request: Request):
    await _require_panel_access(request)
    path = _vk_web_screenshot_path()
    if _vk_web_context is not None:
        try:
            await _vk_web_save_screenshot()
        except Exception:
            pass
    if not path.exists():
        return Response(status_code=404)
    return Response(path.read_bytes(), media_type="image/png", headers={"Cache-Control": "no-store"})


@router.post("/vk-web/auth/click")
async def vk_web_auth_click(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    _, page = await _vk_web_start()
    viewport = getattr(page, "viewport_size", None) or {"width": 1366, "height": 900}
    image_width = float(data.get("image_width") or viewport["width"])
    image_height = float(data.get("image_height") or viewport["height"])
    x = float(data.get("x") or 0)
    y = float(data.get("y") or 0)
    click_x = max(0, min(float(viewport["width"]), x * float(viewport["width"]) / max(1, image_width)))
    click_y = max(0, min(float(viewport["height"]), y * float(viewport["height"]) / max(1, image_height)))
    await page.mouse.click(click_x, click_y)
    await page.wait_for_timeout(1200)
    return {"ok": True, "vk_web": await _vk_web_interaction_state(page)}


@router.post("/vk-web/auth/type")
async def vk_web_auth_type(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    text = str(data.get("text") or "")
    _, page = await _vk_web_start()
    if text:
        await page.keyboard.type(text, delay=25)
        await page.wait_for_timeout(700)
    return {"ok": True, "vk_web": await _vk_web_interaction_state(page)}


@router.post("/vk-web/auth/key")
async def vk_web_auth_key(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    key = _clean(data.get("key")) or "Enter"
    allowed = {"Enter", "Tab", "Escape", "Backspace", "Delete", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight"}
    if key not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported key")
    _, page = await _vk_web_start()
    await page.keyboard.press(key)
    await page.wait_for_timeout(900)
    return {"ok": True, "vk_web": await _vk_web_interaction_state(page)}


@router.post("/vk-web/admins/retry")
async def vk_web_admins_retry(request: Request):
    await _require_panel_access(request)
    data = await request.json()
    run_id = data.get("run_id")
    if run_id in (None, ""):
        raise HTTPException(status_code=400, detail="run_id is required")
    return {"ok": True, "result": await _retry_vk_admins_from_run(int(run_id))}


@router.post("/vk-web/admins/retry-pending")
async def vk_web_admins_retry_pending(request: Request):
    await _require_panel_access(request)
    return {"ok": True, "result": await _retry_vk_admins_from_run(None)}


@router.post("/vk-web/auth/close")
async def vk_web_auth_close(request: Request):
    await _require_panel_access(request)
    await _vk_web_stop()
    return {"ok": True, "vk_web": await _vk_web_auth_state(open_browser=False)}


@router.post("/vk-web/auth/reset")
async def vk_web_auth_reset(request: Request):
    await _require_panel_access(request)
    state = await _vk_web_auth_state(open_browser=True, screenshot=True)
    if state.get("authorized"):
        raise HTTPException(status_code=409, detail="Профиль авторизован. Сброс отменён, чтобы не потерять рабочую VK-сессию.")
    await _vk_web_stop()
    profile_dir = _vk_web_profile_dir()
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    shot = _vk_web_screenshot_path()
    if shot.exists():
        shot.unlink()
    return {"ok": True, "reset": True, "profile_dir": str(profile_dir)}


@router.get("/status")
async def status():
    _ensure_db()
    telegram = await _telegram_auth_state()
    vk_web = await _vk_web_auth_state(open_browser=False)
    required_env = {
        "vk_user_token": bool(os.environ.get("VK_USER_TOKEN")),
        "telegram_api": bool(os.environ.get("TELEGRAM_API_ID") and os.environ.get("TELEGRAM_API_HASH")),
        "telegram_session": bool(telegram.get("authorized")),
    }
    optional_env = {
        "webhook_password": bool(_password()),
        "sbkvd_legacy_password": bool(os.environ.get("SBKVD_PROCESS_WEBHOOK_PASSWORD")),
        "vk_test_user_token": bool(os.environ.get("VK_TEST_USER_TOKEN")),
        "vk_group_token": bool(os.environ.get("VK_GROUP_TOKEN")),
        "vk_group_id": bool(os.environ.get("VK_GROUP_ID")),
        "vk_web_profile": bool(vk_web.get("profile_exists") or vk_web.get("browser_open")),
    }
    return {
        "ok": True,
        "env": required_env,
        "required_env": required_env,
        "optional_env": optional_env,
        "telegram": telegram,
        "vk_web": vk_web,
        "asset_group_photo": bool(_avatar_path()),
        "asset_welcome_photo": bool(_asset_path("welcome_message_photo.jpg")),
    }


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
async def preview(stream_number: str = "51", start_date: str = "01.06.2026", course: str = "puppy", curator_id: str = ""):
    course_row = _course_by_input(course)
    selected = _selected_people(stream_number, curator_id)
    return {
        "ok": True,
        "vk_title": _format_title(stream_number, start_date, course_row, "vk"),
        "tg_title": _format_title(stream_number, start_date, course_row, "tg"),
        "selected": selected,
        "curator_id": _selected_curator_id(stream_number, curator_id),
        "vk_welcome": _render_template("vk_welcome", course=course_row, stream_number=stream_number, date_start=start_date, selected=selected, platform="vk"),
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
    vk_token = _clean(os.environ.get("VK_USER_TOKEN"))
    vk_user: dict[str, Any] | None = None
    vk_error = ""
    if vk_token:
        try:
            users = await _vk_method("users.get", {"fields": "screen_name"}, vk_token)
            if isinstance(users, list) and users:
                user = users[0]
                vk_user = {
                    "id": user.get("id"),
                    "screen_name": user.get("screen_name"),
                    "name": " ".join(filter(None, [_clean(user.get("first_name")), _clean(user.get("last_name"))])),
                }
            elif isinstance(users, dict) and "error" in users:
                vk_error = str(users["error"])
        except Exception as exc:
            vk_error = str(exc)
    telegram = await _telegram_auth_state(include_user=True)
    return {
        "ok": True,
        "vk": {"configured": bool(vk_token), "user": vk_user, "error": vk_error},
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
