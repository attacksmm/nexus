from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "scanner"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.199"
DEFAULT_POLL_SECONDS = 120
DEFAULT_SLA_SECONDS = 600
DEFAULT_HUMAN_REPLY_SLA_SECONDS = 7200
DEFAULT_MAX_MESSAGE_AGE_SECONDS = 172800
DEFAULT_WORK_ITEM_MAX_AGE_SECONDS = 14 * 86400
DEFAULT_RETENTION_DAYS = 14
DEFAULT_GROUP_ID = "225075265"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: Any = None
_scan_task: asyncio.Task | None = None
_write_lock = asyncio.Lock()


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", None)
    return _setup_async()


async def _setup_async() -> None:
    await _init_db()
    _start_scanner()


async def shutdown() -> None:
    global _scan_task
    if _scan_task and not _scan_task.done():
        _scan_task.cancel()
        try:
            await _scan_task
        except asyncio.CancelledError:
            pass
    _scan_task = None


def _log(level: str, message: str, *args: Any, **kwargs: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("scanner module is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _utc_from_epoch(value: Any) -> str:
    try:
        ts = int(value)
    except Exception:
        ts = int(time.time())
    return datetime.fromtimestamp(max(0, ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch_from_utc(value: str) -> float:
    parsed = _parse_utc(value)
    return parsed.timestamp() if parsed else 0.0


def _same_moscow_day(a_epoch: int, b_epoch: int) -> bool:
    if not a_epoch or not b_epoch:
        return False
    a = datetime.fromtimestamp(a_epoch, timezone.utc).astimezone(MOSCOW_TZ)
    b = datetime.fromtimestamp(b_epoch, timezone.utc).astimezone(MOSCOW_TZ)
    return a.date() == b.date()


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _truncate_preview(value: Any, limit: int = 500) -> str:
    text = re.sub(r"\s+", " ", _clean(value, limit * 2)).strip()
    return text[:limit]


def _normalize_person_name(value: str) -> str:
    words = [part.strip(" -") for part in re.split(r"\s+", _clean(value, 160)) if part.strip(" -")]
    words = [word for word in words if len(word) > 1]
    return " ".join(words[:2])


def _outgoing_human_author_id(item: dict[str, Any]) -> str:
    admin_author_id = _clean(item.get("admin_author_id"), 80)
    if admin_author_id and admin_author_id.isdigit():
        return admin_author_id
    from_id = _clean(item.get("from_id"), 80)
    if from_id and from_id.isdigit():
        return from_id
    return ""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "да"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.environ.get(name, "") or default).strip())
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


def _env() -> dict[str, str]:
    return {
        "vk_group_token": os.environ.get("VK_GROUP_TOKEN", "").strip(),
        "vk_user_token": os.environ.get("VK_USER_TOKEN", "").strip(),
        "vk_group_id": os.environ.get("VK_GROUP_ID", "").strip(),
        "openrouter_db_path": os.environ.get("SCANNER_OPENROUTER_DB_PATH", "").strip(),
        "customer_db_path": (
            os.environ.get("SCANNER_CUSTOMER_DB_PATH", "")
            or os.environ.get("OPENROUTER_CUSTOMER_DB_PATH", "")
        ).strip(),
    }


def _vk_read_token() -> str:
    env = _env()
    return env["vk_group_token"] or env["vk_user_token"]


def _scanner_enabled() -> bool:
    return _env_bool("SCANNER_ENABLED", True) and bool(_vk_read_token())


def _poll_seconds() -> int:
    return _env_int("SCANNER_POLL_SECONDS", DEFAULT_POLL_SECONDS, 30, 3600)


def _sla_seconds() -> int:
    return _env_int("SCANNER_SLA_SECONDS", DEFAULT_SLA_SECONDS, 30, 86400)


def _human_reply_sla_seconds() -> int:
    return _env_int("SCANNER_HUMAN_REPLY_SLA_SECONDS", DEFAULT_HUMAN_REPLY_SLA_SECONDS, 300, 86400)


def _max_message_age_seconds() -> int:
    return _env_int("SCANNER_MAX_MESSAGE_AGE_SECONDS", DEFAULT_MAX_MESSAGE_AGE_SECONDS, 300, 604800)


def _work_item_max_age_seconds() -> int:
    return _env_int("SCANNER_WORK_ITEM_MAX_AGE_SECONDS", DEFAULT_WORK_ITEM_MAX_AGE_SECONDS, 3600, 90 * 86400)


def _modules_dir() -> Path:
    if _module_dir is not None:
        return _module_dir.parent
    return Path(__file__).resolve().parents[1] / "modules"


def _openrouter_db_path() -> Path:
    env_path = _env()["openrouter_db_path"]
    if env_path:
        return Path(env_path)
    return _modules_dir() / "openrouter" / "data" / "openrouter.db"


def _customer_db_path() -> Path:
    env_path = _env()["customer_db_path"]
    if env_path:
        return Path(env_path)
    return _modules_dir() / "customer-db" / "data" / "customer-db.db"


def _vk_conversation_url(peer_id: str) -> str:
    group_id = _clean(_env().get("vk_group_id"), 40) or DEFAULT_GROUP_ID
    clean_peer = _clean(peer_id, 80)
    return f"https://vk.com/gim{group_id}/convo/{clean_peer}?entrypoint=list_unread&tab=unread"


@asynccontextmanager
async def _connect(path: Path | None = None):
    db_path = path or _must_db()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(db_path, timeout=60)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA busy_timeout=60000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        yield db
    finally:
        await db.close()


async def _init_db() -> None:
    async with _connect() as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS scan_events (
                id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                source                     TEXT NOT NULL DEFAULT 'vk',
                vk_message_id              TEXT NOT NULL DEFAULT '',
                conversation_message_id    TEXT NOT NULL DEFAULT '',
                peer_id                    TEXT NOT NULL DEFAULT '',
                vk_user_id                 TEXT NOT NULL DEFAULT '',
                profile_name               TEXT NOT NULL DEFAULT '',
                message_at                 TEXT NOT NULL DEFAULT '',
                message_text_preview       TEXT NOT NULL DEFAULT '',
                first_seen_at              TEXT NOT NULL,
                updated_at                 TEXT NOT NULL,
                status                     TEXT NOT NULL DEFAULT 'pending_sla',
                reason                     TEXT NOT NULL DEFAULT '',
                openrouter_seen_at         TEXT NOT NULL DEFAULT '',
                openrouter_conversation_id TEXT NOT NULL DEFAULT '',
                work_item_at               TEXT NOT NULL DEFAULT '',
                admin_before_at            TEXT NOT NULL DEFAULT '',
                admin_before_author_id     TEXT NOT NULL DEFAULT '',
                admin_after_at             TEXT NOT NULL DEFAULT '',
                admin_after_author_id      TEXT NOT NULL DEFAULT '',
                read_at                    TEXT NOT NULL DEFAULT '',
                opened_at                  TEXT NOT NULL DEFAULT '',
                raw_json                   TEXT NOT NULL DEFAULT '{}'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_scan_events_vk_message ON scan_events(source, vk_message_id);
            CREATE INDEX IF NOT EXISTS idx_scan_events_status_updated ON scan_events(status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_scan_events_vk_user ON scan_events(vk_user_id, message_at);
            CREATE INDEX IF NOT EXISTS idx_scan_events_read ON scan_events(read_at, updated_at);
            """
        )
        await db.commit()
    _log("info", "scanner DB initialized")


async def _require_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user:
        raise HTTPException(401, "unauthorized")
    if not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "Недостаточно прав")
    return user


async def _vk_api_call(method: str, params: dict[str, Any], *, timeout: float = 15.0) -> dict[str, Any]:
    token = _vk_read_token()
    if not token:
        raise RuntimeError("VK token is not configured")
    clean_params = dict(params)
    clean_params["access_token"] = token
    clean_params.setdefault("v", VK_API_VERSION)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{VK_API_BASE}/{method}", data=clean_params)
        data = resp.json()
    except Exception as exc:
        raise RuntimeError(f"VK API {method} transport error: {type(exc).__name__}: {exc}") from exc
    if isinstance(data, dict) and data.get("error"):
        err = data.get("error") or {}
        raise RuntimeError(f"VK API {method} error {err.get('error_code')}: {err.get('error_msg') or err}")
    if not isinstance(data, dict):
        raise RuntimeError(f"VK API {method} returned non-object response")
    return data.get("response", data)


async def _history_context(peer_id: str, message_at: str, conversation_message_id: str = "") -> dict[str, str]:
    if not peer_id:
        return {}
    message_ts = int(_epoch_from_utc(message_at) or 0)
    try:
        current_cmid = int(conversation_message_id or 0)
    except Exception:
        current_cmid = 0
    try:
        response = await _vk_api_call("messages.getHistory", {"peer_id": peer_id, "count": 50}, timeout=15.0)
    except Exception as exc:
        _log("warning", "scanner VK history check failed peer_id=%s error=%s", peer_id, exc)
        return {}

    after: list[tuple[int, int, str]] = []
    before_same_day: list[tuple[int, int, str]] = []
    for item in response.get("items") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("out") or "0") not in {"1", "True", "true"}:
            continue
        author_id = _outgoing_human_author_id(item)
        if not author_id:
            continue
        try:
            item_ts = int(item.get("date") or 0)
        except Exception:
            item_ts = 0
        try:
            item_cmid = int(item.get("conversation_message_id") or 0)
        except Exception:
            item_cmid = 0
        if item_ts >= message_ts and (not current_cmid or not item_cmid or item_cmid > current_cmid):
            after.append((item_ts, item_cmid, author_id))
        elif item_ts < message_ts and _same_moscow_day(item_ts, message_ts):
            before_same_day.append((item_ts, item_cmid, author_id))

    result: dict[str, str] = {}
    if after:
        item_ts, _item_cmid, author_id = sorted(after, key=lambda item: (item[0], item[1]))[0]
        result["admin_after_at"] = _utc_from_epoch(item_ts)
        result["admin_after_author_id"] = author_id
    if before_same_day:
        item_ts, _item_cmid, author_id = sorted(before_same_day, key=lambda item: (item[0], item[1]), reverse=True)[0]
        result["admin_before_at"] = _utc_from_epoch(item_ts)
        result["admin_before_author_id"] = author_id
    return result


async def _fetch_unanswered_vk() -> list[dict[str, Any]]:
    if not _vk_read_token():
        return []
    count = _env_int("SCANNER_VK_COUNT", 100, 1, 200)
    response = await _vk_api_call(
        "messages.getConversations",
        {"count": count, "filter": "unanswered", "extended": 1},
    )
    profiles: dict[int, str] = {}
    for profile in response.get("profiles") or []:
        try:
            pid = int(profile.get("id"))
        except Exception:
            continue
        name = _normalize_person_name(f"{profile.get('first_name') or ''} {profile.get('last_name') or ''}")
        if name:
            profiles[pid] = name

    now_ts = time.time()
    max_age = _max_message_age_seconds()
    items: list[dict[str, Any]] = []
    for item in response.get("items") or []:
        if not isinstance(item, dict):
            continue
        conversation = item.get("conversation") or {}
        message = item.get("last_message") or {}
        peer = conversation.get("peer") or {}
        peer_id = _clean(peer.get("id"), 80)
        from_id = _clean(message.get("from_id"), 80)
        vk_user_id = from_id if from_id and not from_id.startswith("-") else peer_id
        try:
            msg_ts = int(message.get("date") or 0)
        except Exception:
            msg_ts = 0
        if not peer_id or not vk_user_id or not vk_user_id.isdigit():
            continue
        if str(message.get("out") or "0") not in {"0", "False", "false"}:
            continue
        if msg_ts and now_ts - msg_ts > max_age:
            continue
        raw_message_id = _clean(message.get("id") or message.get("conversation_message_id"), 80)
        if not raw_message_id:
            continue
        conversation_message_id = _clean(message.get("conversation_message_id"), 80)
        text_preview = _truncate_preview(message.get("text") or "")
        attachments = message.get("attachments") if isinstance(message.get("attachments"), list) else []
        attachment_types = [
            _clean(attachment.get("type"), 60)
            for attachment in attachments
            if isinstance(attachment, dict) and _clean(attachment.get("type"), 60)
        ]
        context = await _history_context(peer_id, _utc_from_epoch(msg_ts), conversation_message_id)
        event_key = f"{peer_id}:{raw_message_id}"
        items.append(
            {
                "vk_message_id": event_key,
                "conversation_message_id": conversation_message_id,
                "peer_id": peer_id,
                "vk_user_id": vk_user_id,
                "profile_name": profiles.get(int(vk_user_id), ""),
                "message_at": _utc_from_epoch(msg_ts),
                "message_text_preview": text_preview,
                "ignored": not bool(text_preview),
                "admin_before_at": context.get("admin_before_at", ""),
                "admin_before_author_id": context.get("admin_before_author_id", ""),
                "admin_after_at": context.get("admin_after_at", ""),
                "admin_after_author_id": context.get("admin_after_author_id", ""),
                "raw_json": _json_dumps(
                    {
                        "peer_id": peer_id,
                        "from_id": from_id,
                        "date": msg_ts,
                        "id": raw_message_id,
                        "conversation_message_id": conversation_message_id,
                        "out": message.get("out"),
                        "text_present": bool(text_preview),
                        "attachment_types": attachment_types,
                        **context,
                    }
                ),
            }
        )
    return items


async def _openrouter_answer_after(vk_user_id: str, message_at: str) -> tuple[str, str]:
    db_path = _openrouter_db_path()
    if not db_path.exists():
        return "", ""
    since_epoch = max(0.0, _epoch_from_utc(message_at) - 30.0)
    since = datetime.fromtimestamp(since_epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        async with _connect(db_path) as db:
            cur = await db.execute(
                """
                SELECT created_at, conversation_id
                FROM messages
                WHERE platform_id=? AND source IN ('senler','senler_retry_delivered') AND role='assistant' AND created_at>=?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (vk_user_id, since),
            )
            row = await cur.fetchone()
    except Exception as exc:
        _log("warning", "scanner openrouter lookup failed vk_user_id=%s error=%s", vk_user_id, exc)
        return "", ""
    if not row:
        return "", ""
    return str(row["created_at"] or ""), str(row["conversation_id"] or "")


async def _customer_work_item(vk_user_id: str, message_at: str) -> tuple[str, str]:
    vk_id = _clean(vk_user_id, 80)
    db_path = _customer_db_path()
    if not vk_id or not db_path.exists():
        return "", ""

    message_ts = _epoch_from_utc(message_at) or time.time()
    cutoff_ts = message_ts - _work_item_max_age_seconds()
    latest: tuple[float, str, str] | None = None

    async def consider(ts_text: str, reason: str) -> None:
        nonlocal latest
        ts = _epoch_from_utc(ts_text) or 0.0
        if not ts or ts < cutoff_ts or ts > message_ts + 300:
            return
        item = (ts, _utc_from_epoch(ts), reason)
        if latest is None or item[0] > latest[0]:
            latest = item

    try:
        async with _connect(db_path) as db:
            for table in ("cdb_getcourse_orders", "cdb_amo_deals"):
                try:
                    cur = await db.execute(
                        f"""
                        SELECT custom_fields, created_at, updated_at
                        FROM {table}
                        WHERE custom_fields LIKE ? OR custom_fields LIKE ?
                        ORDER BY updated_at DESC, id DESC
                        LIMIT 20
                        """,
                        (f'%"{vk_id}"%', f"%{vk_id}%"),
                    )
                    rows = await cur.fetchall()
                except Exception:
                    continue
                for row in rows:
                    fields = _safe_json_dict(row["custom_fields"])
                    field_vk = _clean(fields.get("vk_id") or fields.get("utm_term") or fields.get("user_term"), 80)
                    if field_vk != vk_id:
                        continue
                    ts_text = _clean(fields.get("received_at") or row["updated_at"] or row["created_at"], 80)
                    order_id = _clean(fields.get("order_id") or fields.get("deal_id") or fields.get("number"), 80)
                    status = _clean(fields.get("status"), 120)
                    label = "GetCourse order" if table == "cdb_getcourse_orders" else "amoCRM deal"
                    reason = f"Customer already has {label}"
                    if order_id:
                        reason += f" {order_id}"
                    if status:
                        reason += f" ({status})"
                    await consider(ts_text, reason)

            try:
                cur = await db.execute(
                    """
                    SELECT custom_fields, created_at, updated_at
                    FROM cdb_visitor_profiles
                    WHERE custom_fields LIKE ? OR custom_fields LIKE ?
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 20
                    """,
                    (f'%"{vk_id}"%', f"%{vk_id}%"),
                )
                visitor_rows = await cur.fetchall()
            except Exception:
                visitor_rows = []
            for row in visitor_rows:
                fields = _safe_json_dict(row["custom_fields"])
                field_vk = _clean(fields.get("utm_term") or fields.get("vk_id") or fields.get("user_term"), 80)
                if field_vk != vk_id:
                    continue
                try:
                    confirmed_forms = int(fields.get("confirmed_form_count") or 0)
                except Exception:
                    confirmed_forms = 0
                if confirmed_forms <= 0:
                    continue
                ts_text = _clean(fields.get("last_seen_at") or row["updated_at"] or row["created_at"], 80)
                await consider(ts_text, "Customer already submitted a tracked form")
    except Exception as exc:
        _log("warning", "scanner customer-db lookup failed vk_user_id=%s error=%s", vk_id, exc)
        return "", ""

    if latest is None:
        return "", ""
    return latest[1], latest[2]


def _work_item_status(*, now_epoch: float, message_at: str, work_item_at: str, work_item_reason: str) -> tuple[str, str]:
    message_ts = _epoch_from_utc(message_at) or now_epoch
    work_ts = _epoch_from_utc(work_item_at) or message_ts
    age = now_epoch - max(message_ts, work_ts)
    if age >= _human_reply_sla_seconds():
        return "missing_human_reply", f"{work_item_reason or 'Customer already has a tracked work item'}; no outgoing VK admin reply after human SLA"
    return "pending_human_response", f"{work_item_reason or 'Customer already has a tracked work item'}; waiting for human reply SLA"


async def _upsert_event(db: aiosqlite.Connection, event: dict[str, Any], now: str) -> None:
    cur = await db.execute(
        "SELECT first_seen_at, read_at, opened_at FROM scan_events WHERE source='vk' AND vk_message_id=?",
        (event["vk_message_id"],),
    )
    existing = await cur.fetchone()
    first_seen_at = str(existing["first_seen_at"] or now) if existing else now
    read_at = str(existing["read_at"] or "") if existing else ""
    opened_at = str(existing["opened_at"] or "") if existing else ""
    openrouter_seen_at, openrouter_conversation_id = await _openrouter_answer_after(
        event["vk_user_id"],
        event["message_at"],
    )
    work_item_at, work_item_reason = await _customer_work_item(event["vk_user_id"], event["message_at"])
    now_epoch = _epoch_from_utc(now) or time.time()
    message_age = now_epoch - (_epoch_from_utc(event["message_at"]) or now_epoch)
    first_seen_age = now_epoch - (_epoch_from_utc(first_seen_at) or now_epoch)
    sla = _sla_seconds()

    if event.get("ignored"):
        status = "ignored_non_text"
        reason = "VK unanswered message has no text; stickers/attachments are ignored"
        openrouter_seen_at = ""
        openrouter_conversation_id = ""
    elif event.get("admin_after_at"):
        status = "resolved_not_unanswered"
        reason = "VK has outgoing admin reply after this message"
        openrouter_seen_at = str(event.get("admin_after_at") or "")
        openrouter_conversation_id = ""
    elif event.get("admin_before_at"):
        status = "admin_conversation"
        reason = "VK dialog already had an outgoing admin message before this inbound message today"
        openrouter_seen_at = ""
        openrouter_conversation_id = ""
    elif work_item_at:
        status, reason = _work_item_status(
            now_epoch=now_epoch,
            message_at=event["message_at"],
            work_item_at=work_item_at,
            work_item_reason=work_item_reason,
        )
        openrouter_seen_at = work_item_at
        openrouter_conversation_id = ""
    elif openrouter_seen_at:
        status = "answered_by_openrouter"
        reason = "OpenRouter generated a Senler answer after this VK message"
    elif message_age >= sla and first_seen_age >= sla:
        status = "missing_webhook"
        reason = "VK has an unanswered inbound message, but Scanner found no OpenRouter/Senler answer after it"
    else:
        status = "pending_sla"
        reason = "Waiting for scanner SLA before listing as a problem"

    await db.execute(
        """
        INSERT INTO scan_events(
            source,vk_message_id,conversation_message_id,peer_id,vk_user_id,profile_name,message_at,
            message_text_preview,first_seen_at,updated_at,status,reason,openrouter_seen_at,
            openrouter_conversation_id,work_item_at,admin_before_at,admin_before_author_id,
            admin_after_at,admin_after_author_id,read_at,opened_at,raw_json
        )
        VALUES('vk',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source,vk_message_id) DO UPDATE SET
            conversation_message_id=excluded.conversation_message_id,
            peer_id=excluded.peer_id,
            vk_user_id=excluded.vk_user_id,
            profile_name=excluded.profile_name,
            message_at=excluded.message_at,
            message_text_preview=excluded.message_text_preview,
            updated_at=excluded.updated_at,
            status=excluded.status,
            reason=excluded.reason,
            openrouter_seen_at=excluded.openrouter_seen_at,
            openrouter_conversation_id=excluded.openrouter_conversation_id,
            work_item_at=excluded.work_item_at,
            admin_before_at=excluded.admin_before_at,
            admin_before_author_id=excluded.admin_before_author_id,
            admin_after_at=excluded.admin_after_at,
            admin_after_author_id=excluded.admin_after_author_id,
            raw_json=excluded.raw_json
        """,
        (
            event["vk_message_id"],
            event["conversation_message_id"],
            event["peer_id"],
            event["vk_user_id"],
            event["profile_name"],
            event["message_at"],
            event["message_text_preview"],
            first_seen_at,
            now,
            status,
            reason,
            openrouter_seen_at,
            openrouter_conversation_id,
            work_item_at,
            event.get("admin_before_at") or "",
            event.get("admin_before_author_id") or "",
            event.get("admin_after_at") or "",
            event.get("admin_after_author_id") or "",
            read_at,
            opened_at,
            event["raw_json"],
        ),
    )


async def _mark_absent_events(db: aiosqlite.Connection, active_vk_message_ids: set[str], now: str) -> None:
    statuses = (
        "pending_sla",
        "missing_webhook",
        "pending_human_response",
        "missing_human_reply",
        "admin_conversation",
        "answered_by_openrouter",
        "ignored_non_text",
    )
    status_placeholders = ",".join("?" for _ in statuses)
    if active_vk_message_ids:
        placeholders = ",".join("?" for _ in active_vk_message_ids)
        await db.execute(
            f"""
            UPDATE scan_events
            SET status='resolved_not_unanswered',
                reason='VK no longer returns this dialog in unanswered list',
                updated_at=?
            WHERE source='vk'
              AND status IN ({status_placeholders})
              AND vk_message_id NOT IN ({placeholders})
            """,
            (now, *statuses, *sorted(active_vk_message_ids)),
        )
    else:
        await db.execute(
            f"""
            UPDATE scan_events
            SET status='resolved_not_unanswered',
                reason='VK returned no unanswered dialogs in current check',
                updated_at=?
            WHERE source='vk'
              AND status IN ({status_placeholders})
            """,
            (now, *statuses),
        )


async def _cleanup(db: aiosqlite.Connection, now: str) -> None:
    retention_days = _env_int("SCANNER_RETENTION_DAYS", DEFAULT_RETENTION_DAYS, 1, 90)
    cutoff = datetime.fromtimestamp(time.time() - retention_days * 86400, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    await db.execute("DELETE FROM scan_events WHERE updated_at<?", (cutoff,))


async def _scan_once() -> dict[str, Any]:
    if not _scanner_enabled():
        return {"ok": False, "enabled": False, "reason": "disabled_or_vk_token_missing"}
    now = _now()
    events = await _fetch_unanswered_vk()
    async with _write_lock:
        async with _connect() as db:
            active: set[str] = set()
            for event in events:
                active.add(str(event.get("vk_message_id") or ""))
                await _upsert_event(db, event, now)
            await _mark_absent_events(db, {item for item in active if item}, now)
            await _cleanup(db, now)
            await db.commit()
    _log("info", "scanner poll ok seen=%s", len(events))
    return {"ok": True, "enabled": True, "vk_unanswered_seen": len(events)}


def _start_scanner() -> None:
    global _scan_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if not _scanner_enabled():
        _log("info", "scanner disabled or VK token missing")
        return
    if _scan_task is None or _scan_task.done():
        _scan_task = loop.create_task(_scanner_loop())


async def _scanner_loop() -> None:
    await asyncio.sleep(10)
    while True:
        try:
            await _scan_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "scanner poll failed: %s", exc)
        await asyncio.sleep(_poll_seconds())


def _tab_statuses(tab: str) -> tuple[str, ...]:
    if tab == "admin":
        return ("admin_conversation",)
    if tab == "all":
        return ("missing_webhook", "missing_human_reply", "admin_conversation")
    return ("missing_webhook", "missing_human_reply")


async def _events_payload(tab: str = "problems", unread_only: bool = True, limit: int = 20) -> dict[str, Any]:
    statuses = _tab_statuses(tab)
    limit = max(1, min(20, int(limit or 20)))
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT status,
                   COUNT(*) AS total,
                   SUM(CASE WHEN read_at='' THEN 1 ELSE 0 END) AS unread
            FROM scan_events
            GROUP BY status
            """
        )
        counts = {
            str(row["status"]): {"total": int(row["total"] or 0), "unread": int(row["unread"] or 0)}
            for row in await cur.fetchall()
        }
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = [*statuses]
        unread_sql = ""
        if unread_only:
            unread_sql = "AND read_at=''"
        cur = await db.execute(
            f"""
            SELECT id,vk_message_id,conversation_message_id,peer_id,vk_user_id,profile_name,message_at,
                   message_text_preview,first_seen_at,updated_at,status,reason,openrouter_seen_at,
                   openrouter_conversation_id,work_item_at,admin_before_at,admin_before_author_id,
                   admin_after_at,admin_after_author_id,read_at,opened_at
            FROM scan_events
            WHERE status IN ({placeholders}) {unread_sql}
            ORDER BY message_at DESC, id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["vk_url"] = _vk_conversation_url(str(row.get("peer_id") or row.get("vk_user_id") or ""))
    return {
        "ok": True,
        "enabled": _scanner_enabled(),
        "poll_seconds": _poll_seconds(),
        "sla_seconds": _sla_seconds(),
        "unread_only": unread_only,
        "limit": limit,
        "tab": tab,
        "counts": counts,
        "items": rows,
    }


@router.get("/events")
async def events(request: Request, tab: str = "problems", unread: int = 1, limit: int = 20):
    await _require_user(request)
    return await _events_payload(tab=tab, unread_only=bool(unread), limit=limit)


@router.post("/events/{event_id}/open")
async def open_event(event_id: int, request: Request):
    await _require_user(request)
    now = _now()
    async with _write_lock:
        async with _connect() as db:
            cur = await db.execute("SELECT vk_user_id,peer_id FROM scan_events WHERE id=?", (event_id,))
            row = await cur.fetchone()
            if not row:
                raise HTTPException(404, "event not found")
            vk_user_id = str(row["vk_user_id"] or "")
            peer_id = str(row["peer_id"] or vk_user_id)
            await db.execute(
                """
                UPDATE scan_events
                SET read_at=CASE WHEN read_at='' THEN ? ELSE read_at END,
                    opened_at=?,
                    updated_at=?
                WHERE vk_user_id=? AND status IN ('missing_webhook','missing_human_reply','admin_conversation')
                """,
                (now, now, now, vk_user_id),
            )
            await db.commit()
    return {"ok": True, "url": _vk_conversation_url(peer_id)}


@router.post("/events/{event_id}/unread")
async def unread_event(event_id: int, request: Request):
    await _require_user(request)
    async with _write_lock:
        async with _connect() as db:
            await db.execute("UPDATE scan_events SET read_at='', updated_at=? WHERE id=?", (_now(), event_id))
            await db.commit()
    return {"ok": True}


@router.post("/scan")
async def scan(request: Request):
    await _require_user(request)
    result = await _scan_once()
    payload = await _events_payload(tab="problems", unread_only=True, limit=20)
    return {**result, "events": payload}


@router.get("/env-status")
async def env_status(request: Request):
    await _require_user(request)
    return {
        "ok": True,
        "enabled": _scanner_enabled(),
        "VK_GROUP_TOKEN": bool(os.environ.get("VK_GROUP_TOKEN")),
        "VK_USER_TOKEN": bool(os.environ.get("VK_USER_TOKEN")),
        "VK_GROUP_ID": bool(os.environ.get("VK_GROUP_ID")),
        "openrouter_db": str(_openrouter_db_path()),
        "openrouter_db_ready": _openrouter_db_path().exists(),
        "customer_db": str(_customer_db_path()),
        "customer_db_ready": _customer_db_path().exists(),
        "poll_seconds": _poll_seconds(),
        "sla_seconds": _sla_seconds(),
        "human_reply_sla_seconds": _human_reply_sla_seconds(),
    }
