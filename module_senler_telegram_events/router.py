from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, require_admin, verify_token_from_request


router = APIRouter()

MODULE_ID = "senler-telegram-events"
TELEGRAM_BOT_API_VERSION = "10.2"
WEBHOOK_HEADER = "X-Nexus-Senler-Secret"
MAX_BODY_BYTES = 5 * 1024 * 1024
DEFAULT_RETENTION_DAYS = 30
MAX_RETENTION_DAYS = 365
MAX_PAGE_SIZE = 200

_db_path: Path | None = None
_logger: logging.Logger | None = None
_cleanup_task: asyncio.Task | None = None
_stop_event = asyncio.Event()


UPDATE_TYPES = (
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "business_connection",
    "business_message",
    "edited_business_message",
    "deleted_business_messages",
    "guest_message",
    "message_reaction",
    "message_reaction_count",
    "inline_query",
    "chosen_inline_result",
    "callback_query",
    "shipping_query",
    "pre_checkout_query",
    "purchased_paid_media",
    "poll",
    "poll_answer",
    "my_chat_member",
    "chat_member",
    "chat_join_request",
    "chat_boost",
    "removed_chat_boost",
    "managed_bot",
    "subscription",
)

MESSAGE_TYPES = {
    "message": "Сообщение",
    "edited_message": "Сообщение изменено",
    "channel_post": "Публикация в канале",
    "edited_channel_post": "Публикация в канале изменена",
    "business_message": "Бизнес-сообщение",
    "edited_business_message": "Бизнес-сообщение изменено",
    "guest_message": "Гостевое сообщение",
}

CONTENT_TYPES = (
    ("checklist", "Чек-лист"),
    ("photo", "Фото"),
    ("animation", "Анимация"),
    ("audio", "Аудио"),
    ("document", "Документ"),
    ("video", "Видео"),
    ("video_note", "Видеосообщение"),
    ("voice", "Голосовое сообщение"),
    ("sticker", "Стикер"),
    ("live_photo", "Живое фото"),
    ("paid_media", "Платный медиаматериал"),
    ("story", "История"),
    ("contact", "Контакт"),
    ("location", "Геопозиция"),
    ("venue", "Место"),
    ("dice", "Бросок кубика"),
    ("game", "Игра"),
    ("poll", "Опрос"),
    ("invoice", "Счёт на оплату"),
    ("successful_payment", "Успешная оплата"),
    ("refunded_payment", "Возврат платежа"),
    ("web_app_data", "Данные Web App"),
    ("users_shared", "Переданы пользователи"),
    ("chat_shared", "Передан чат"),
    ("passport_data", "Данные Telegram Passport"),
    ("gift", "Подарок"),
    ("unique_gift", "Уникальный подарок"),
    ("giveaway", "Розыгрыш"),
    ("giveaway_winners", "Победители розыгрыша"),
    ("giveaway_completed", "Розыгрыш завершён"),
    ("rich_message", "Расширенное сообщение"),
)

SERVICE_TYPES = (
    ("new_chat_members", "В чат добавлены участники"),
    ("left_chat_member", "Участник покинул чат"),
    ("new_chat_title", "Название чата изменено"),
    ("new_chat_photo", "Фотография чата изменена"),
    ("delete_chat_photo", "Фотография чата удалена"),
    ("group_chat_created", "Создан групповой чат"),
    ("supergroup_chat_created", "Создан супергрупповой чат"),
    ("channel_chat_created", "Создан канал"),
    ("message_auto_delete_timer_changed", "Изменён таймер удаления сообщений"),
    ("pinned_message", "Сообщение закреплено"),
    ("migrate_to_chat_id", "Группа преобразована в супергруппу"),
    ("migrate_from_chat_id", "Супергруппа создана из группы"),
    ("video_chat_scheduled", "Видеочат запланирован"),
    ("video_chat_started", "Видеочат начался"),
    ("video_chat_ended", "Видеочат завершён"),
    ("video_chat_participants_invited", "В видеочат приглашены участники"),
    ("forum_topic_created", "Тема форума создана"),
    ("forum_topic_edited", "Тема форума изменена"),
    ("forum_topic_closed", "Тема форума закрыта"),
    ("forum_topic_reopened", "Тема форума открыта повторно"),
    ("general_forum_topic_hidden", "Основная тема форума скрыта"),
    ("general_forum_topic_unhidden", "Основная тема форума показана"),
    ("write_access_allowed", "Разрешена отправка сообщений"),
    ("proximity_alert_triggered", "Сработало оповещение о близости"),
    ("boost_added", "Чат получил буст"),
    ("connected_website", "Подключён сайт"),
    ("giveaway_created", "Создан розыгрыш"),
    ("chat_owner_left", "Владелец чата покинул чат"),
    ("chat_owner_changed", "Владелец чата изменён"),
    ("gift_upgrade_sent", "Улучшение подарка оплачено"),
    ("chat_background_set", "Фон чата изменён"),
    ("checklist_tasks_done", "Изменён статус задач чек-листа"),
    ("checklist_tasks_added", "В чек-лист добавлены задачи"),
    ("community_chat_added", "Чат добавлен в сообщество"),
    ("community_chat_removed", "Чат удалён из сообщества"),
    ("direct_message_price_changed", "Изменилась цена личных сообщений канала"),
    ("managed_bot_created", "Создан управляемый бот"),
    ("paid_message_price_changed", "Изменилась цена платных сообщений"),
    ("poll_option_added", "В опрос добавлен вариант ответа"),
    ("poll_option_deleted", "Из опроса удалён вариант ответа"),
    ("suggested_post_approved", "Предложенная публикация одобрена"),
    ("suggested_post_approval_failed", "Не удалось одобрить предложенную публикацию"),
    ("suggested_post_declined", "Предложенная публикация отклонена"),
    ("suggested_post_paid", "Предложенная публикация оплачена"),
    ("suggested_post_refunded", "Оплата предложенной публикации возвращена"),
)


async def setup(ctx) -> None:
    global _db_path, _logger, _cleanup_task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.senler-telegram-events"))
    _stop_event.clear()
    await _init_db()
    await _cleanup_events()
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(_cleanup_loop(), name="senler-telegram-events-cleanup")


async def shutdown() -> None:
    global _cleanup_task
    _stop_event.set()
    if _cleanup_task and not _cleanup_task.done():
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
    _cleanup_task = None


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("senler-telegram-events module is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 1000) -> str:
    if value is None:
        return ""
    return str(value).strip()[:limit]


def _preview(value: Any, limit: int = 360) -> str:
    text = " ".join(_clean(value, limit * 3).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


@asynccontextmanager
async def _connect():
    db = await aiosqlite.connect(_must_db(), timeout=30)
    try:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA foreign_keys=ON")
        yield db
    finally:
        await db.close()


async def _require_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user) or not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "Доступ разрешён только администратору")
    return user


async def _init_db() -> None:
    async with _connect() as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                secret_hash TEXT NOT NULL,
                secret_hint TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                received_count INTEGER NOT NULL DEFAULT 0,
                unique_count INTEGER NOT NULL DEFAULT 0,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                last_received_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                update_id TEXT NOT NULL DEFAULT '',
                body_hash TEXT NOT NULL,
                event_type TEXT NOT NULL DEFAULT 'unknown',
                summary TEXT NOT NULL DEFAULT '',
                actor_id TEXT NOT NULL DEFAULT '',
                actor_username TEXT NOT NULL DEFAULT '',
                actor_name TEXT NOT NULL DEFAULT '',
                chat_id TEXT NOT NULL DEFAULT '',
                chat_type TEXT NOT NULL DEFAULT '',
                chat_title TEXT NOT NULL DEFAULT '',
                message_id TEXT NOT NULL DEFAULT '',
                parse_status TEXT NOT NULL DEFAULT 'ok',
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                body_size INTEGER NOT NULL DEFAULT 0,
                raw_payload TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                content_type TEXT NOT NULL DEFAULT '',
                received_at TEXT NOT NULL,
                last_received_at TEXT NOT NULL,
                FOREIGN KEY(source_id) REFERENCES sources(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_ste_events_update
                ON events(source_id,update_id) WHERE update_id <> '';
            CREATE UNIQUE INDEX IF NOT EXISTS ux_ste_events_body
                ON events(source_id,body_hash) WHERE update_id = '';
            CREATE INDEX IF NOT EXISTS ix_ste_events_received ON events(received_at DESC,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ste_events_source ON events(source_id,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ste_events_type ON events(event_type,id DESC);
            CREATE INDEX IF NOT EXISTS ix_ste_events_actor ON events(actor_username,actor_id,id DESC);
            """
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES('retention_days',?,?)",
            (str(DEFAULT_RETENTION_DAYS), _now()),
        )
        await db.commit()
    _log("info", "senler-telegram-events initialized")


def _secret_hash(source_uuid: str, secret: str) -> str:
    return hashlib.sha256(f"{source_uuid}:{secret}".encode("utf-8")).hexdigest()


def _public_source(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key != "secret_hash"
    } | {
        "enabled": bool(row.get("enabled")),
        "webhook_path": f"/{MODULE_ID}/api/webhook/{row['uuid']}",
        "header_name": WEBHOOK_HEADER,
    }


def _event_type(payload: dict[str, Any]) -> str:
    for key in UPDATE_TYPES:
        if key in payload and payload.get(key) is not None:
            return key
    return next((str(key) for key in payload if key != "update_id"), "unknown")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _actor_and_chat(event_type: str, node: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    message = node if event_type in MESSAGE_TYPES else {}
    if event_type == "callback_query":
        message = _dict(node.get("message"))
    chat = _dict(message.get("chat")) or _dict(node.get("chat"))
    actor = _dict(node.get("from")) or _dict(node.get("user"))
    if not actor and event_type == "poll_answer":
        actor = _dict(node.get("voter_chat"))
    if not actor and event_type in {"message_reaction", "message_reaction_count"}:
        actor = _dict(node.get("actor_chat"))
    if not actor and event_type in {"chat_boost", "removed_chat_boost"}:
        boost = _dict(node.get("boost")) or node
        source = _dict(boost.get("source"))
        actor = _dict(source.get("user"))
    return actor, chat, message


def _display_name(entity: dict[str, Any]) -> str:
    username = _clean(entity.get("username"), 80).lstrip("@")
    if username:
        return f"@{username}"
    name = " ".join(
        part for part in (_clean(entity.get("first_name"), 100), _clean(entity.get("last_name"), 100)) if part
    )
    return name or _clean(entity.get("title"), 200) or (f"ID {_clean(entity.get('id'), 80)}" if entity.get("id") is not None else "неизвестного пользователя")


def _duration(node: dict[str, Any]) -> str:
    seconds = int(node.get("duration") or 0)
    if seconds <= 0:
        return ""
    return f" · {seconds // 60:02d}:{seconds % 60:02d}"


def _message_content(message: dict[str, Any]) -> tuple[str, str]:
    text = _preview(message.get("text"), 500)
    if text:
        return "text", text
    caption = _preview(message.get("caption"), 500)
    for key, label in CONTENT_TYPES:
        if key not in message:
            continue
        value = message.get(key)
        extra = ""
        if key in {"voice", "video", "video_note", "audio", "animation"}:
            extra = _duration(_dict(value))
        elif key == "document":
            extra = f" · {_preview(_dict(value).get('file_name'), 160)}" if _dict(value).get("file_name") else ""
        elif key == "sticker":
            sticker = _dict(value)
            extra = f" · {sticker.get('emoji')}" if sticker.get("emoji") else ""
        elif key == "contact":
            contact = _dict(value)
            contact_name = " ".join(filter(None, (_clean(contact.get("first_name"), 80), _clean(contact.get("last_name"), 80))))
            extra = f" · {contact_name}" if contact_name else ""
        elif key == "location":
            location = _dict(value)
            if location.get("latitude") is not None and location.get("longitude") is not None:
                extra = f" · {location['latitude']}, {location['longitude']}"
        elif key == "venue":
            extra = f" · {_preview(_dict(value).get('title'), 160)}" if _dict(value).get("title") else ""
        elif key == "poll":
            extra = f" · {_preview(_dict(value).get('question'), 200)}" if _dict(value).get("question") else ""
        elif key == "checklist":
            extra = f" · {_preview(_dict(value).get('title'), 200)}" if _dict(value).get("title") else ""
        elif key == "dice":
            dice = _dict(value)
            extra = f" · {dice.get('emoji', '')} {dice.get('value', '')}".rstrip()
        return key, f"{label}{extra}" + (f": {caption}" if caption else "")
    for key, label in SERVICE_TYPES:
        if key in message:
            value = message.get(key)
            if key == "new_chat_title":
                return key, f"{label}: {_preview(value, 200)}"
            if key == "new_chat_members" and isinstance(value, list):
                names = ", ".join(_display_name(_dict(item)) for item in value[:6])
                return key, f"{label}: {names}"
            if key == "left_chat_member":
                return key, f"{label}: {_display_name(_dict(value))}"
            return key, label
    keys = [str(key) for key in message if key not in {"message_id", "date", "chat", "from", "sender_chat", "reply_to_message", "entities", "caption_entities"}]
    return "service", "Событие сообщения" + (f" · поля: {', '.join(keys[:10])}" if keys else "")


def _reaction_text(values: Any) -> str:
    if not isinstance(values, list):
        return "—"
    rendered: list[str] = []
    for value in values[:12]:
        item = _dict(value)
        rendered.append(_clean(item.get("emoji") or item.get("custom_emoji_id") or item.get("type"), 80) or "реакция")
    return ", ".join(rendered) or "нет"


def describe_update(payload: dict[str, Any]) -> dict[str, str]:
    event_type = _event_type(payload)
    node = _dict(payload.get(event_type))
    actor, chat, message = _actor_and_chat(event_type, node)
    actor_label = _display_name(actor)
    chat_label = _display_name(chat) if chat else ""
    message_id = _clean(message.get("message_id") or node.get("message_id"), 80)

    if event_type in MESSAGE_TYPES:
        content_type, content = _message_content(message)
        prefix = MESSAGE_TYPES[event_type]
        target = actor_label if actor else chat_label or "неизвестного отправителя"
        summary = f"{prefix} от {target}: {content}" if content_type == "text" else f"{content} от {target}"
    elif event_type == "callback_query":
        value = _preview(node.get("data") or node.get("game_short_name"), 500) or "без данных"
        summary = f"Нажатие кнопки от {actor_label}: {value}"
    elif event_type == "inline_query":
        summary = f"Inline-запрос от {actor_label}: {_preview(node.get('query'), 500) or 'пустой запрос'}"
    elif event_type == "chosen_inline_result":
        summary = f"Выбран inline-результат пользователем {actor_label}: {_preview(node.get('result_id'), 200)}"
    elif event_type == "shipping_query":
        summary = f"Запрос доставки от {actor_label}: {_preview(node.get('invoice_payload'), 240)}"
    elif event_type == "pre_checkout_query":
        summary = f"Предоплатный запрос от {actor_label}: {_clean(node.get('total_amount'), 60)} {_clean(node.get('currency'), 20)}"
    elif event_type == "purchased_paid_media":
        summary = f"Покупка платного медиаматериала пользователем {actor_label}: {_preview(node.get('paid_media_payload'), 240)}"
    elif event_type == "poll":
        summary = f"Опрос «{_preview(node.get('question'), 300)}» · {'закрыт' if node.get('is_closed') else 'обновлён'}"
    elif event_type == "poll_answer":
        summary = f"Ответ в опросе от {actor_label}: варианты {', '.join(map(str, node.get('option_ids') or [])) or 'отозван'}"
    elif event_type in {"my_chat_member", "chat_member"}:
        old_status = _clean(_dict(node.get("old_chat_member")).get("status"), 80) or "—"
        new_status = _clean(_dict(node.get("new_chat_member")).get("status"), 80) or "—"
        subject = _display_name(_dict(_dict(node.get("new_chat_member")).get("user")))
        prefix = "Статус бота" if event_type == "my_chat_member" else f"Статус участника {subject}"
        summary = f"{prefix} в {chat_label}: {old_status} → {new_status}"
    elif event_type == "chat_join_request":
        summary = f"Запрос на вступление от {actor_label} в {chat_label}"
    elif event_type == "message_reaction":
        summary = f"Реакция от {actor_label} в {chat_label}: {_reaction_text(node.get('old_reaction'))} → {_reaction_text(node.get('new_reaction'))}"
    elif event_type == "message_reaction_count":
        summary = f"Изменилось число реакций в {chat_label} · сообщение {message_id or '—'}"
    elif event_type == "business_connection":
        state = "подключён" if node.get("is_enabled") else "отключён"
        summary = f"Telegram Business {state} для {actor_label}"
    elif event_type == "deleted_business_messages":
        ids = node.get("message_ids") if isinstance(node.get("message_ids"), list) else []
        summary = f"Удалены бизнес-сообщения в {chat_label}: {len(ids)} шт."
    elif event_type == "chat_boost":
        summary = f"Буст чата {chat_label} добавлен или изменён"
    elif event_type == "removed_chat_boost":
        summary = f"Буст чата {chat_label} удалён"
    elif event_type == "managed_bot":
        summary = f"Изменение управляемого бота · поля: {', '.join(node.keys()) or 'нет данных'}"
    elif event_type == "subscription":
        summary = f"Изменение платной подписки · поля: {', '.join(node.keys()) or 'нет данных'}"
    else:
        summary = f"Неизвестное событие Telegram «{event_type}» — полный JSON сохранён"

    username = _clean(actor.get("username"), 80).lstrip("@")
    actor_name = " ".join(filter(None, (_clean(actor.get("first_name"), 100), _clean(actor.get("last_name"), 100))))
    return {
        "event_type": event_type,
        "summary": _preview(summary, 900),
        "actor_id": _clean(actor.get("id"), 80),
        "actor_username": username,
        "actor_name": actor_name,
        "chat_id": _clean(chat.get("id"), 80),
        "chat_type": _clean(chat.get("type"), 80),
        "chat_title": _clean(chat.get("title") or chat.get("username"), 240),
        "message_id": message_id,
    }


async def _settings() -> dict[str, int]:
    async with _connect() as db:
        cur = await db.execute("SELECT key,value FROM settings")
        values = {str(key): str(value) for key, value in await cur.fetchall()}
    try:
        days = int(values.get("retention_days") or DEFAULT_RETENTION_DAYS)
    except ValueError:
        days = DEFAULT_RETENTION_DAYS
    return {"retention_days": max(1, min(MAX_RETENTION_DAYS, days))}


async def _cleanup_events() -> int:
    days = (await _settings())["retention_days"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    async with _connect() as db:
        cur = await db.execute("DELETE FROM events WHERE received_at < ?", (cutoff,))
        deleted = max(0, int(cur.rowcount or 0))
        await db.commit()
    if deleted:
        _log("info", "deleted %s expired Telegram events", deleted)
    return deleted


async def _cleanup_loop() -> None:
    while not _stop_event.is_set():
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=6 * 60 * 60)
        except asyncio.TimeoutError:
            try:
                await _cleanup_events()
            except Exception:
                _log("exception", "Telegram event cleanup failed")


async def _source_by_uuid(source_uuid: str) -> dict[str, Any] | None:
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM sources WHERE uuid=?", (_clean(source_uuid, 100),))
        row = await cur.fetchone()
    return dict(row) if row else None


async def _store_event(
    source: dict[str, Any],
    *,
    update_id: str,
    body_hash: str,
    body_size: int,
    raw_payload: str,
    content_type: str,
    parse_status: str,
    error: str,
    metadata: dict[str, str],
) -> tuple[int, bool]:
    now = _now()
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        if update_id:
            cur = await db.execute("SELECT id FROM events WHERE source_id=? AND update_id=?", (source["id"], update_id))
        else:
            cur = await db.execute("SELECT id FROM events WHERE source_id=? AND update_id='' AND body_hash=?", (source["id"], body_hash))
        existing = await cur.fetchone()
        if existing:
            event_id = int(existing["id"])
            await db.execute(
                "UPDATE events SET duplicate_count=duplicate_count+1,last_received_at=? WHERE id=?",
                (now, event_id),
            )
            await db.execute(
                "UPDATE sources SET received_count=received_count+1,duplicate_count=duplicate_count+1,last_received_at=?,updated_at=? WHERE id=?",
                (now, now, source["id"]),
            )
            await db.commit()
            return event_id, True

        keys = (
            "event_type", "summary", "actor_id", "actor_username", "actor_name",
            "chat_id", "chat_type", "chat_title", "message_id",
        )
        values = [metadata.get(key, "") for key in keys]
        cur = await db.execute(
            f"""
            INSERT INTO events(
                source_id,update_id,body_hash,{','.join(keys)},parse_status,body_size,
                raw_payload,error,content_type,received_at,last_received_at
            ) VALUES({','.join(['?'] * 19)})
            """,
            (
                source["id"], update_id, body_hash, *values, parse_status, body_size,
                raw_payload, error, content_type, now, now,
            ),
        )
        event_id = int(cur.lastrowid)
        rejected = 0 if parse_status == "ok" else 1
        await db.execute(
            """
            UPDATE sources SET received_count=received_count+1,unique_count=unique_count+1,
                rejected_count=rejected_count+?,last_received_at=?,updated_at=? WHERE id=?
            """,
            (rejected, now, now, source["id"]),
        )
        await db.commit()
    return event_id, False


async def _read_limited_body(request: Request) -> tuple[bytes, bool]:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        available = MAX_BODY_BYTES - total
        if len(chunk) > available:
            if available > 0:
                chunks.append(chunk[:available])
            return b"".join(chunks), True
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks), False


@router.get("/health")
async def health() -> dict[str, Any]:
    settings = await _settings()
    async with _connect() as db:
        source_count = int((await (await db.execute("SELECT COUNT(*) FROM sources WHERE enabled=1")).fetchone())[0])
    return {
        "ok": True,
        "module": MODULE_ID,
        "enabled_sources": source_count,
        "telegram_bot_api_version": TELEGRAM_BOT_API_VERSION,
        "supported_update_types": len(UPDATE_TYPES),
        **settings,
    }


@router.get("/sources")
async def list_sources(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = [dict(row) for row in await (await db.execute("SELECT * FROM sources ORDER BY id")).fetchall()]
    return {"ok": True, "sources": [_public_source(row) for row in rows], "header_name": WEBHOOK_HEADER}


@router.post("/sources")
async def create_source(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = await request.json()
    name = _clean(_dict(data).get("name"), 160)
    if not name:
        raise HTTPException(400, "Укажите название источника")
    source_uuid = secrets.token_urlsafe(12)
    secret = secrets.token_urlsafe(32)
    now = _now()
    async with _connect() as db:
        cur = await db.execute(
            "INSERT INTO sources(uuid,name,secret_hash,secret_hint,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            (source_uuid, name, _secret_hash(source_uuid, secret), f"••••{secret[-4:]}", now, now),
        )
        source_id = int(cur.lastrowid)
        await db.commit()
        db.row_factory = aiosqlite.Row
        row = dict(await (await db.execute("SELECT * FROM sources WHERE id=?", (source_id,))).fetchone())
    _log("info", "Senler Telegram source created: id=%s name=%s", source_id, name)
    return {"ok": True, "source": _public_source(row), "secret": secret, "secret_once": True}


@router.patch("/sources/{source_id}")
async def update_source(source_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = _dict(await request.json())
    updates: list[str] = []
    values: list[Any] = []
    if "name" in data:
        name = _clean(data.get("name"), 160)
        if not name:
            raise HTTPException(400, "Название не может быть пустым")
        updates.append("name=?")
        values.append(name)
    if "enabled" in data:
        updates.append("enabled=?")
        values.append(1 if bool(data.get("enabled")) else 0)
    if not updates:
        raise HTTPException(400, "Нет изменений")
    updates.append("updated_at=?")
    values.append(_now())
    values.append(source_id)
    async with _connect() as db:
        cur = await db.execute(f"UPDATE sources SET {','.join(updates)} WHERE id=?", tuple(values))
        if not cur.rowcount:
            raise HTTPException(404, "Источник не найден")
        await db.commit()
        db.row_factory = aiosqlite.Row
        row = dict(await (await db.execute("SELECT * FROM sources WHERE id=?", (source_id,))).fetchone())
    return {"ok": True, "source": _public_source(row)}


@router.delete("/sources/{source_id}")
async def delete_source(source_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    async with _connect() as db:
        cur = await db.execute("DELETE FROM sources WHERE id=?", (source_id,))
        if not cur.rowcount:
            raise HTTPException(404, "Источник не найден")
        await db.commit()
    _log("info", "Senler Telegram source deleted: id=%s", source_id)
    return {"ok": True}


@router.post("/sources/{source_id}/rotate-secret")
async def rotate_source_secret(source_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    secret = secrets.token_urlsafe(32)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM sources WHERE id=?", (source_id,))).fetchone()
        if not row:
            raise HTTPException(404, "Источник не найден")
        source = dict(row)
        await db.execute(
            "UPDATE sources SET secret_hash=?,secret_hint=?,updated_at=? WHERE id=?",
            (_secret_hash(source["uuid"], secret), f"••••{secret[-4:]}", _now(), source_id),
        )
        await db.commit()
        source["secret_hint"] = f"••••{secret[-4:]}"
    _log("info", "Senler Telegram source secret rotated: id=%s", source_id)
    return {"ok": True, "source": _public_source(source), "secret": secret, "secret_once": True}


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    return {
        "ok": True,
        **await _settings(),
        "max_body_bytes": MAX_BODY_BYTES,
        "header_name": WEBHOOK_HEADER,
        "telegram_bot_api_version": TELEGRAM_BOT_API_VERSION,
        "supported_update_types": len(UPDATE_TYPES),
        "update_types": list(UPDATE_TYPES),
    }


@router.patch("/settings")
async def update_settings(request: Request) -> dict[str, Any]:
    await _require_admin(request)
    data = _dict(await request.json())
    try:
        days = int(data.get("retention_days"))
    except (TypeError, ValueError):
        raise HTTPException(400, "Срок хранения должен быть числом")
    if not 1 <= days <= MAX_RETENTION_DAYS:
        raise HTTPException(400, f"Срок хранения: от 1 до {MAX_RETENTION_DAYS} дней")
    async with _connect() as db:
        await db.execute(
            "INSERT INTO settings(key,value,updated_at) VALUES('retention_days',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            (str(days), _now()),
        )
        await db.commit()
    deleted = await _cleanup_events()
    return {"ok": True, "retention_days": days, "deleted": deleted}


@router.get("/stats")
async def stats(request: Request, source_id: int = 0) -> dict[str, Any]:
    await _require_admin(request)
    where = "WHERE source_id=?" if source_id else ""
    params: tuple[Any, ...] = (source_id,) if source_id else ()
    today = datetime.now(timezone.utc).date().isoformat()
    async with _connect() as db:
        row = await (
            await db.execute(
                f"""
                SELECT COUNT(*),COALESCE(SUM(duplicate_count),0),
                    COALESCE(SUM(CASE WHEN parse_status<>'ok' THEN 1 ELSE 0 END),0),
                    COALESCE(SUM(CASE WHEN substr(received_at,1,10)=? THEN 1 ELSE 0 END),0),
                    MAX(received_at)
                FROM events {where}
                """,
                (today, *params),
            )
        ).fetchone()
    return {
        "ok": True,
        "stored": int(row[0] or 0),
        "duplicates": int(row[1] or 0),
        "rejected": int(row[2] or 0),
        "today": int(row[3] or 0),
        "last_received_at": str(row[4] or ""),
    }


@router.get("/events")
async def list_events(
    request: Request,
    source_id: int = 0,
    event_type: str = "",
    parse_status: str = "",
    q: str = "",
    date_from: str = "",
    date_to: str = "",
    cursor: int = 0,
    limit: int = Query(60, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    await _require_admin(request)
    clauses: list[str] = []
    params: list[Any] = []
    if source_id:
        clauses.append("e.source_id=?")
        params.append(source_id)
    if event_type:
        clauses.append("e.event_type=?")
        params.append(_clean(event_type, 100))
    if parse_status:
        clauses.append("e.parse_status=?")
        params.append(_clean(parse_status, 30))
    if cursor:
        clauses.append("e.id<?")
        params.append(cursor)
    if date_from:
        clauses.append("e.received_at>=?")
        params.append(_clean(date_from, 40))
    if date_to:
        clauses.append("e.received_at<=?")
        params.append(_clean(date_to, 40))
    query = _clean(q, 200)
    if query:
        clauses.append("(e.summary LIKE ? OR e.actor_username LIKE ? OR e.actor_id LIKE ? OR e.chat_id LIKE ? OR e.update_id LIKE ?)")
        like = f"%{query}%"
        params.extend([like] * 5)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit + 1)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        rows = [
            dict(row)
            for row in await (
                await db.execute(
                    f"""
                    SELECT e.id,e.source_id,s.name source_name,e.update_id,e.event_type,e.summary,
                        e.actor_id,e.actor_username,e.actor_name,e.chat_id,e.chat_type,e.chat_title,
                        e.message_id,e.parse_status,e.duplicate_count,e.body_size,e.error,
                        e.received_at,e.last_received_at
                    FROM events e JOIN sources s ON s.id=e.source_id
                    {where} ORDER BY e.id DESC LIMIT ?
                    """,
                    tuple(params),
                )
            ).fetchall()
        ]
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {"ok": True, "events": rows, "next_cursor": rows[-1]["id"] if has_more and rows else None}


@router.get("/events/{event_id}")
async def event_detail(event_id: int, request: Request) -> dict[str, Any]:
    await _require_admin(request)
    async with _connect() as db:
        db.row_factory = aiosqlite.Row
        row = await (
            await db.execute(
                "SELECT e.*,s.name source_name,s.uuid source_uuid FROM events e JOIN sources s ON s.id=e.source_id WHERE e.id=?",
                (event_id,),
            )
        ).fetchone()
    if not row:
        raise HTTPException(404, "Событие не найдено")
    data = dict(row)
    try:
        data["payload"] = json.loads(data.pop("raw_payload")) if data.get("raw_payload") else None
    except json.JSONDecodeError:
        data["payload"] = data.pop("raw_payload")
    return {"ok": True, "event": data}


@router.post("/webhook/{source_uuid}")
async def webhook(source_uuid: str, request: Request) -> JSONResponse:
    source = await _source_by_uuid(source_uuid)
    if not source:
        return JSONResponse({"ok": False, "error": "unknown source"}, status_code=404)
    supplied = request.headers.get(WEBHOOK_HEADER, "")
    expected = _secret_hash(source["uuid"], supplied)
    if not supplied or not hmac.compare_digest(expected, str(source["secret_hash"])):
        _log("warning", "Senler Telegram webhook rejected: invalid secret source=%s", source["id"])
        return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=401)
    if not source.get("enabled"):
        return JSONResponse({"ok": False, "error": "source disabled"}, status_code=200)

    body, oversized = await _read_limited_body(request)
    content_type = _clean(request.headers.get("content-type"), 160)
    body_hash = hashlib.sha256(body).hexdigest()
    parse_status = "ok"
    error = ""
    update_id = ""
    raw_payload = ""
    metadata = {
        "event_type": "invalid",
        "summary": "Некорректный webhook от Senler",
        "actor_id": "", "actor_username": "", "actor_name": "",
        "chat_id": "", "chat_type": "", "chat_title": "", "message_id": "",
    }

    if oversized:
        parse_status = "rejected"
        error = f"request body exceeds {MAX_BODY_BYTES} bytes"
        metadata["event_type"] = "oversized"
        metadata["summary"] = "Webhook отклонён: размер превышает 5 МБ"
        raw_payload = ""
        body_size = MAX_BODY_BYTES + 1
    else:
        body_size = len(body)
        raw_payload = body.decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("JSON root must be an object")
            update_id = _clean(payload.get("update_id"), 80)
            metadata = describe_update(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            parse_status = "invalid_json"
            error = _clean(exc, 500)
            metadata["event_type"] = "invalid_json"
            metadata["summary"] = "Некорректный JSON от Senler — исходное тело сохранено"

    event_id, duplicate = await _store_event(
        source,
        update_id=update_id,
        body_hash=body_hash,
        body_size=body_size,
        raw_payload=raw_payload,
        content_type=content_type,
        parse_status=parse_status,
        error=error,
        metadata=metadata,
    )
    _log(
        "info" if parse_status == "ok" else "warning",
        "Senler Telegram event source=%s event=%s type=%s duplicate=%s status=%s",
        source["id"], event_id, metadata["event_type"], duplicate, parse_status,
    )
    return JSONResponse({"ok": True, "event_id": event_id, "duplicate": duplicate, "parse_status": parse_status}, status_code=200)
