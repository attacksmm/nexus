from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

_db_path: str | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_startup_task: asyncio.Task | None = None
_queue_task: asyncio.Task | None = None
_reconcile_task: asyncio.Task | None = None
_manager_index_task: asyncio.Task | None = None
_customer_write_lock = asyncio.Lock()
_amo_rate_lock = asyncio.Lock()
_amo_next_request_at = 0.0
_amo_state: dict[str, Any] = {
    "requests": 0,
    "rate_limit_responses": 0,
    "retries": 0,
    "last_429_at": "",
    "last_error": "",
}
_reconcile_state: dict[str, Any] = {
    "running": False,
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
    "last_missing": 0,
    "last_synced": 0,
    "last_failed": 0,
    "last_days": [],
}
_amo_users_cache: dict[str, Any] = {"expires_at": 0.0, "items": {}}

MODULE_ID = "amocrm-db"
TABLE_NAME = "amo_deals"
TABLE_DISPLAY_NAME = "Сделки amoCRM"
LEGACY_REPLAY_SINCE = "2026-07-15T21:00:00Z"

DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "request_timeout": "12",
    "debounce_seconds": "3",
    "reconcile_enabled": "1",
    "reconcile_interval_minutes": "360",
    "reconcile_lookback_days": "14",
    "reconcile_delay_seconds": "0.35",
    "amo_min_interval_seconds": "0.5",
    "amo_retry_attempts": "5",
}

LEAD_KEY_RE = re.compile(r"^leads\[(?P<action>[^\]]+)\]\[(?P<idx>\d+)\]\[(?P<field>[^\]]+)\]$")


@asynccontextmanager
async def _module_db():
    if not _db_path:
        raise RuntimeError("amocrm-db is not initialized")
    db = await aiosqlite.connect(_db_path, timeout=60)
    try:
        await db.execute("PRAGMA busy_timeout=60000")
        yield db
    finally:
        await db.close()


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def setup(ctx):
    global _db_path, _module_dir, _logger, _startup_task
    _db_path = ctx.db_path
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.amocrm-db"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        if _startup_task is None or _startup_task.done():
            _startup_task = loop.create_task(_start_background_tasks())
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _startup_task, _queue_task, _reconcile_task, _manager_index_task
    tasks = [_startup_task, _queue_task, _reconcile_task, _manager_index_task]
    for task in tasks:
        if task and not task.done():
            task.cancel()
    for task in tasks:
        if not task:
            continue
        try:
            await task
        except asyncio.CancelledError:
            pass
    _startup_task = None
    _queue_task = None
    _reconcile_task = None
    _manager_index_task = None


async def _start_background_tasks() -> None:
    global _queue_task, _reconcile_task, _manager_index_task
    await _init_db()
    if _manager_index_task is None or _manager_index_task.done():
        _manager_index_task = asyncio.create_task(_ensure_successful_manager_index())
    if _queue_task is None or _queue_task.done():
        _queue_task = asyncio.create_task(_queue_worker_loop())
    if _reconcile_task is None or _reconcile_task.done():
        _reconcile_task = asyncio.create_task(_reconcile_loop())


async def _init_db() -> None:
    if not _db_path:
        return
    await asyncio.to_thread(_backup_manager_index_migration)
    async with _module_db() as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                action TEXT NOT NULL DEFAULT '',
                deal_id TEXT NOT NULL DEFAULT '',
                pipeline_id TEXT NOT NULL DEFAULT '',
                status_id TEXT NOT NULL DEFAULT '',
                old_status_id TEXT NOT NULL DEFAULT '',
                responsible_user_id TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                ignored INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                raw_payload TEXT NOT NULL DEFAULT '',
                queue_status TEXT NOT NULL DEFAULT 'completed',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_events_deal ON events(deal_id);
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE TABLE IF NOT EXISTS pipeline_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS successful_manager_identities (
                identity TEXT NOT NULL,
                deal_id TEXT NOT NULL,
                manager_id TEXT NOT NULL DEFAULT '',
                manager_name TEXT NOT NULL DEFAULT '',
                deal_url TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(identity,deal_id)
            );
            CREATE INDEX IF NOT EXISTS idx_successful_manager_identity
                ON successful_manager_identities(identity,updated_at DESC);
            """
        )
        columns = {str(row[1]) for row in await (await db.execute("PRAGMA table_info(events)")).fetchall()}
        migrations = {
            "queue_status": "ALTER TABLE events ADD COLUMN queue_status TEXT NOT NULL DEFAULT 'completed'",
            "attempts": "ALTER TABLE events ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            "next_attempt_at": "ALTER TABLE events ADD COLUMN next_attempt_at TEXT NOT NULL DEFAULT ''",
            "updated_at": "ALTER TABLE events ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        }
        for column, sql in migrations.items():
            if column not in columns:
                await db.execute(sql)
        manager_columns = {
            str(row[1]) for row in await (await db.execute("PRAGMA table_info(successful_manager_identities)")).fetchall()
        }
        if "manager_name" not in manager_columns:
            await db.execute("ALTER TABLE successful_manager_identities ADD COLUMN manager_name TEXT NOT NULL DEFAULT ''")
            await db.execute("DELETE FROM successful_manager_identities")
        await db.execute(
            "UPDATE events SET queue_status='retry',next_attempt_at=?,updated_at=? WHERE queue_status='processing'",
            (_now(), _now()),
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_queue ON events(queue_status,next_attempt_at,id)")
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        if not _env()["webhook_secret"]:
            cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
            row = await cur.fetchone()
            if not _clean(row[0] if row else "", 300):
                await db.execute(
                    "INSERT INTO settings(key,value) VALUES('webhook_secret',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (secrets.token_urlsafe(24),),
                )
        requeued = await _requeue_unrecovered_rate_limits(db)
        await db.commit()
    _log("info", "amocrm-db initialized legacy_429_requeued=%s", requeued)


def _backup_manager_index_migration() -> None:
    if not _db_path or _module_dir is None:
        return
    db_path = Path(_db_path)
    if not db_path.exists():
        return
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60) as source:
        table = source.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='successful_manager_identities'"
        ).fetchone()
        columns = {
            str(row[1]) for row in source.execute("PRAGMA table_info(successful_manager_identities)").fetchall()
        } if table else set()
        if "manager_name" in columns:
            return
        backup = _module_dir.parents[1] / "backups" / "amocrm-db-pre-manager-name-20260808T2245MSK" / "amocrm-db.db"
        if backup.exists():
            return
        backup.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(backup) as target:
            source.backup(target)
            if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("amocrm-db backup quick_check failed")
        backup.chmod(0o600)
        _log("info", "amocrm-db migration backup created: %s", backup)


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _after(seconds: float) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=max(0.0, seconds))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _safe_int(value: Any) -> int | None:
    text = _clean(value, 64)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _json_loads(value: Any, fallback: Any) -> Any:
    if not value:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _env() -> dict[str, str]:
    return {
        "amo_base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "amo_token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "webhook_secret": os.environ.get("AMO_DB_WEBHOOK_SECRET", "").strip(),
        "customer_db_path": os.environ.get("AMO_DB_CUSTOMER_DB_PATH", "").strip(),
    }


def _timeout(settings: dict[str, str]) -> float:
    try:
        return max(2.0, min(60.0, float(settings.get("request_timeout") or 12)))
    except Exception:
        return 12.0


def _debounce_seconds(settings: dict[str, str]) -> float:
    try:
        return max(0.0, min(30.0, float(settings.get("debounce_seconds") or 3)))
    except Exception:
        return 3.0


def _setting_bool(settings: dict[str, str], key: str, default: bool = False) -> bool:
    value = _clean(settings.get(key), 20).lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "да"}


def _setting_int(settings: dict[str, str], key: str, default: int, *, min_value: int, max_value: int) -> int:
    try:
        return max(min_value, min(max_value, int(float(settings.get(key) or default))))
    except Exception:
        return default


def _setting_float(settings: dict[str, str], key: str, default: float, *, min_value: float, max_value: float) -> float:
    try:
        return max(min_value, min(max_value, float(settings.get(key) or default)))
    except Exception:
        return default


async def _settings_map() -> dict[str, str]:
    data = dict(DEFAULT_SETTINGS)
    if not _db_path:
        return data
    async with _module_db() as db:
        async with db.execute("SELECT key,value FROM settings") as cur:
            async for key, value in cur:
                data[str(key)] = str(value or "")
    env = _env()
    if env["webhook_secret"]:
        data["webhook_secret"] = env["webhook_secret"]
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "webhook_secret",
        "request_timeout",
        "debounce_seconds",
        "reconcile_enabled",
        "reconcile_interval_minutes",
        "reconcile_lookback_days",
        "reconcile_delay_seconds",
        "amo_min_interval_seconds",
        "amo_retry_attempts",
    }
    async with _module_db() as db:
        for key in allowed:
            if key in data:
                await db.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, _clean(data.get(key), 300)),
                )
        await db.commit()
    return await _settings_payload()


async def _settings_payload() -> dict[str, Any]:
    settings = await _settings_map()
    return {
        **settings,
        "webhook_secret_source": "env" if _env()["webhook_secret"] else "db",
        "customer_db_path": str(_customer_db_path()),
    }


def _secret_ok(request: Request, settings: dict[str, str]) -> bool:
    secret = _clean(settings.get("webhook_secret"), 200)
    if not secret:
        return True
    supplied = (
        request.query_params.get("secret")
        or request.headers.get("X-Nexus-Secret")
        or request.headers.get("X-Webhook-Secret")
        or ""
    )
    return _clean(supplied, 200) == secret


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    content_type = request.headers.get("content-type", "")
    raw_body = await request.body()
    if "application/json" in content_type:
        try:
            data = json.loads(raw_body.decode("utf-8") or "{}")
            payload = data if isinstance(data, dict) else {"raw": data}
            return payload, json.dumps(payload, ensure_ascii=False)
        except Exception:
            text = raw_body.decode("utf-8", "replace")
            return {"raw_data": text}, json.dumps({"raw_data": text}, ensure_ascii=False)

    form = {}
    try:
        form_data = await request.form()
        form = {str(k): str(v) for k, v in form_data.items()}
    except Exception:
        form = {}
    if form:
        return form, json.dumps(form, ensure_ascii=False)
    if raw_body:
        text = raw_body.decode("utf-8", "replace")
        try:
            data = json.loads(text)
            payload = data if isinstance(data, dict) else {"raw": data}
            return payload, json.dumps(payload, ensure_ascii=False)
        except Exception:
            return {"raw_data": text}, json.dumps({"raw_data": text}, ensure_ascii=False)
    return {}, "{}"


def _iter_lead_events(payload: dict[str, Any]) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    leads = payload.get("leads")
    if isinstance(leads, dict):
        for action in ("add", "update", "status", "responsible", "delete"):
            raw_items = leads.get(action)
            if isinstance(raw_items, dict):
                raw_items = list(raw_items.values())
            if not isinstance(raw_items, list):
                continue
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                events.append({
                    "action": f"lead.{action}",
                    "deal_id": _clean(item.get("id"), 64),
                    "pipeline_id": _clean(item.get("pipeline_id"), 64),
                    "status_id": _clean(item.get("status_id"), 64),
                    "old_status_id": _clean(item.get("old_status_id"), 64),
                    "responsible_user_id": _clean(item.get("responsible_user_id"), 64),
                })

    flat: dict[tuple[str, str], dict[str, str]] = {}
    for key, value in payload.items():
        match = LEAD_KEY_RE.match(str(key))
        if not match:
            continue
        action = match.group("action")
        idx = match.group("idx")
        field = match.group("field")
        item = flat.setdefault((action, idx), {"action": f"lead.{action}"})
        item[field] = _clean(value, 500)
    for item in flat.values():
        events.append({
            "action": _clean(item.get("action"), 64),
            "deal_id": _clean(item.get("id"), 64),
            "pipeline_id": _clean(item.get("pipeline_id"), 64),
            "status_id": _clean(item.get("status_id"), 64),
            "old_status_id": _clean(item.get("old_status_id"), 64),
            "responsible_user_id": _clean(item.get("responsible_user_id"), 64),
        })

    if not events:
        deal_id = _clean(payload.get("deal_id") or payload.get("id"), 64)
        if deal_id:
            events.append({"action": "manual", "deal_id": deal_id, "pipeline_id": "", "status_id": "", "old_status_id": "", "responsible_user_id": ""})

    result = []
    seen = set()
    for event in events:
        key = (event.get("action"), event.get("deal_id"), event.get("pipeline_id"), event.get("status_id"), event.get("old_status_id"), event.get("responsible_user_id"))
        if key in seen:
            continue
        seen.add(key)
        result.append(event)
    return result


async def _store_event(data: dict[str, Any]) -> int:
    queue_status = _clean(data.get("queue_status"), 32)
    if not queue_status:
        if data.get("ignored"):
            queue_status = "ignored"
        elif data.get("success"):
            queue_status = "completed"
        elif data.get("error"):
            queue_status = "failed"
        else:
            queue_status = "pending"
    async with _module_db() as db:
        cur = await db.execute(
            """
            INSERT INTO events(
                action,deal_id,pipeline_id,status_id,old_status_id,responsible_user_id,
                success,ignored,error,details,raw_payload,
                queue_status,attempts,next_attempt_at,updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                _clean(data.get("action"), 64),
                _clean(data.get("deal_id"), 64),
                _clean(data.get("pipeline_id"), 64),
                _clean(data.get("status_id"), 64),
                _clean(data.get("old_status_id"), 64),
                _clean(data.get("responsible_user_id"), 64),
                1 if data.get("success") else 0,
                1 if data.get("ignored") else 0,
                _clean(data.get("error"), 2000),
                data.get("details") if isinstance(data.get("details"), str) else json.dumps(data.get("details") or {}, ensure_ascii=False),
                data.get("raw_payload") if isinstance(data.get("raw_payload"), str) else json.dumps(data.get("raw_payload") or {}, ensure_ascii=False),
                queue_status,
                int(data.get("attempts") or 0),
                _clean(data.get("next_attempt_at"), 64),
                _now(),
            ),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _update_event(event_id: int, **data: Any) -> None:
    assignments = []
    values: list[Any] = []
    for key in ("success", "ignored", "error", "details", "queue_status", "attempts", "next_attempt_at"):
        if key not in data:
            continue
        assignments.append(f"{key}=?")
        value = data[key]
        if key in {"success", "ignored"}:
            values.append(1 if value else 0)
        elif key == "attempts":
            values.append(int(value or 0))
        elif key == "details" and not isinstance(value, str):
            values.append(json.dumps(value or {}, ensure_ascii=False))
        else:
            values.append(_clean(value, 2000) if key == "error" else _clean(value, 64) if key in {"queue_status", "next_attempt_at"} else value)
    if not assignments:
        return
    assignments.append("updated_at=?")
    values.append(_now())
    values.append(int(event_id))
    async with _module_db() as db:
        await db.execute(f"UPDATE events SET {', '.join(assignments)} WHERE id=?", values)
        await db.commit()


async def _requeue_unrecovered_rate_limits(db: aiosqlite.Connection) -> int:
    await db.execute(
        """
        UPDATE events
        SET queue_status='retry',success=0,ignored=0,attempts=0,
            next_attempt_at=?,updated_at=?
        WHERE id IN (
            SELECT failed.id
            FROM events AS failed
            WHERE failed.deal_id<>''
              AND failed.received_at>=?
              AND failed.error LIKE 'amoCRM HTTP 429:%'
              AND NOT EXISTS (
                  SELECT 1 FROM events AS ok
                  WHERE ok.deal_id=failed.deal_id
                    AND ok.success=1
                    AND ok.received_at>failed.received_at
              )
              AND failed.id=(
                  SELECT MAX(newer.id)
                  FROM events AS newer
                  WHERE newer.deal_id=failed.deal_id
                    AND newer.error LIKE 'amoCRM HTTP 429:%'
                    AND NOT EXISTS (
                        SELECT 1 FROM events AS later_ok
                        WHERE later_ok.deal_id=newer.deal_id
                          AND later_ok.success=1
                          AND later_ok.received_at>newer.received_at
                    )
              )
        )
        """,
        (_now(), _now(), LEGACY_REPLAY_SINCE),
    )
    row = await (await db.execute("SELECT changes()" )).fetchone()
    return int(row[0] if row else 0)


async def _enqueue_event(event_id: int, deal_id: str, settings: dict[str, str]) -> None:
    next_attempt_at = _after(_debounce_seconds(settings))
    async with _module_db() as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            """
            UPDATE events
            SET queue_status='ignored',ignored=1,error='superseded by newer webhook',
                next_attempt_at='',updated_at=?
            WHERE deal_id=? AND id<>? AND queue_status IN ('pending','retry')
            """,
            (_now(), deal_id, event_id),
        )
        await db.execute(
            """
            UPDATE events
            SET queue_status='pending',success=0,ignored=0,error='',attempts=0,
                next_attempt_at=?,updated_at=?
            WHERE id=?
            """,
            (next_attempt_at, _now(), event_id),
        )
        await db.commit()


async def _claim_event() -> dict[str, Any] | None:
    async with _module_db() as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute(
            """
            SELECT * FROM events
            WHERE queue_status IN ('pending','retry')
              AND (next_attempt_at='' OR next_attempt_at<=?)
            ORDER BY next_attempt_at ASC,id ASC
            LIMIT 1
            """,
            (_now(),),
        )
        row = await cur.fetchone()
        if not row:
            await db.commit()
            return None
        attempts = int(row["attempts"] or 0) + 1
        await db.execute(
            "UPDATE events SET queue_status='processing',attempts=?,updated_at=? WHERE id=?",
            (attempts, _now(), int(row["id"])),
        )
        await db.commit()
        item = dict(row)
        item["attempts"] = attempts
        item["queue_status"] = "processing"
        return item


def _is_transient_amo_error(error: str) -> bool:
    text = _clean(error, 2000).lower()
    if re.search(r"amocrm http (408|409|425|429|5\d\d)\b", text):
        return True
    return any(marker in text for marker in ("timeout", "timed out", "transport", "connection", "network", "temporar"))


def _queue_retry_delay(attempts: int) -> int:
    return min(3600, 30 * (2 ** min(max(0, attempts - 1), 7)))


async def _process_queue_event(event: dict[str, Any]) -> None:
    event_id = int(event["id"])
    deal_id = _clean(event.get("deal_id"), 64)
    source_event = {
        "action": _clean(event.get("action"), 64),
        "deal_id": deal_id,
        "pipeline_id": _clean(event.get("pipeline_id"), 64),
        "status_id": _clean(event.get("status_id"), 64),
        "old_status_id": _clean(event.get("old_status_id"), 64),
        "responsible_user_id": _clean(event.get("responsible_user_id"), 64),
    }
    result = await _sync_deal(deal_id, source_event=source_event)
    if result.get("ok"):
        await _update_event(
            event_id,
            success=True,
            ignored=False,
            error="",
            details={"storage": result.get("storage") or {}, "status": (result.get("record") or {}).get("status_name")},
            queue_status="completed",
            next_attempt_at="",
        )
        return
    error = _clean(result.get("error") or "amoCRM sync failed", 2000)
    attempts = int(event.get("attempts") or 1)
    if _is_transient_amo_error(error) or attempts < 5:
        delay = _queue_retry_delay(attempts)
        await _update_event(
            event_id,
            success=False,
            ignored=False,
            error=error,
            details={"deal_id": deal_id, "retry_in_seconds": delay},
            queue_status="retry",
            next_attempt_at=_after(delay),
        )
        _log("warning", "amocrm-db queued retry deal_id=%s attempts=%s delay=%s error=%s", deal_id, attempts, delay, error[:300])
        return
    await _update_event(
        event_id,
        success=False,
        ignored=False,
        error=error,
        details={"deal_id": deal_id},
        queue_status="failed",
        next_attempt_at="",
    )


async def _queue_worker_loop() -> None:
    while True:
        try:
            event = await _claim_event()
            if event:
                await _process_queue_event(event)
                continue
            await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "amocrm-db queue worker error: %s", exc)
            await asyncio.sleep(1)


def _retry_after_seconds(value: str, fallback: float) -> float:
    text = _clean(value, 200)
    if text:
        try:
            return max(0.0, min(300.0, float(text)))
        except Exception:
            try:
                moment = parsedate_to_datetime(text)
                if moment.tzinfo is None:
                    moment = moment.replace(tzinfo=timezone.utc)
                return max(0.0, min(300.0, (moment - datetime.now(timezone.utc)).total_seconds()))
            except Exception:
                pass
    return max(0.5, min(300.0, fallback))


async def _amo_get(
    path: str,
    settings: dict[str, str],
    *,
    params: dict[str, str] | None = None,
) -> tuple[dict[str, Any] | None, str]:
    global _amo_next_request_at
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы"
    min_interval = _setting_float(settings, "amo_min_interval_seconds", 0.5, min_value=0.1, max_value=10.0)
    max_attempts = _setting_int(settings, "amo_retry_attempts", 5, min_value=1, max_value=10)
    last_error = ""
    for attempt in range(1, max_attempts + 1):
        try:
            async with _amo_rate_lock:
                loop = asyncio.get_running_loop()
                wait_for = max(0.0, _amo_next_request_at - loop.time())
                if wait_for:
                    await asyncio.sleep(wait_for)
                async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
                    resp = await client.get(
                        env["amo_base_url"] + path,
                        params=params,
                        headers={"Authorization": f"Bearer {env['amo_token']}"},
                    )
                _amo_state["requests"] = int(_amo_state.get("requests") or 0) + 1
                if resp.status_code == 429:
                    fallback = min(30.0, float(2 ** min(attempt - 1, 5)))
                    cooldown = _retry_after_seconds(resp.headers.get("Retry-After", ""), fallback)
                    _amo_next_request_at = loop.time() + cooldown
                    _amo_state["rate_limit_responses"] = int(_amo_state.get("rate_limit_responses") or 0) + 1
                    _amo_state["last_429_at"] = _now()
                elif resp.status_code >= 500:
                    _amo_next_request_at = loop.time() + min(30.0, float(2 ** min(attempt - 1, 5)))
                else:
                    _amo_next_request_at = loop.time() + min_interval
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"amoCRM HTTP {resp.status_code}: {resp.text[:500]}"
                if attempt < max_attempts:
                    _amo_state["retries"] = int(_amo_state.get("retries") or 0) + 1
                    continue
                _amo_state["last_error"] = last_error
                return None, last_error
            if resp.status_code >= 400:
                last_error = f"amoCRM HTTP {resp.status_code}: {resp.text[:500]}"
                _amo_state["last_error"] = last_error
                return None, last_error
            if not resp.text.strip():
                _amo_state["last_error"] = ""
                return {}, ""
            _amo_state["last_error"] = ""
            return resp.json(), ""
        except Exception as exc:
            last_error = f"amoCRM transport {type(exc).__name__}: {exc}"
            _amo_state["last_error"] = last_error
            if attempt >= max_attempts:
                return None, last_error
            _amo_state["retries"] = int(_amo_state.get("retries") or 0) + 1
            async with _amo_rate_lock:
                loop = asyncio.get_running_loop()
                _amo_next_request_at = max(_amo_next_request_at, loop.time() + min(30.0, float(2 ** min(attempt - 1, 5))))
    return None, last_error or "amoCRM request failed"


async def _load_pipelines(settings: dict[str, str], *, allow_cache: bool = True) -> tuple[list[dict[str, Any]], str]:
    body, error = await _amo_get("/api/v4/leads/pipelines", settings)
    if error:
        if allow_cache:
            cached = await _read_pipeline_cache()
            if cached:
                return cached, error
        return [], error
    pipelines = _normalize_pipelines(body or {})
    await _write_pipeline_cache(pipelines)
    return pipelines, ""


def _normalize_pipelines(body: dict[str, Any]) -> list[dict[str, Any]]:
    raw_pipelines = ((body or {}).get("_embedded") or {}).get("pipelines") or []
    pipelines = []
    for pipeline in raw_pipelines:
        if not isinstance(pipeline, dict):
            continue
        statuses = []
        for status in ((pipeline.get("_embedded") or {}).get("statuses") or []):
            if not isinstance(status, dict):
                continue
            status_id = _clean(status.get("id"), 64)
            if not status_id:
                continue
            statuses.append({
                "id": status_id,
                "name": _clean(status.get("name"), 300) or status_id,
                "sort": status.get("sort") or 0,
                "type": _clean(status.get("type"), 64),
                "color": _clean(status.get("color"), 64),
            })
        pipelines.append({
            "id": _clean(pipeline.get("id"), 64),
            "name": _clean(pipeline.get("name"), 300) or _clean(pipeline.get("id"), 64),
            "sort": pipeline.get("sort") or 0,
            "statuses": statuses,
        })
    return sorted(pipelines, key=lambda item: (item.get("sort") or 0, item.get("name") or ""))


async def _read_pipeline_cache() -> list[dict[str, Any]]:
    if not _db_path:
        return []
    async with _module_db() as db:
        cur = await db.execute("SELECT payload FROM pipeline_cache WHERE id=1")
        row = await cur.fetchone()
    payload = _json_loads(row[0] if row else "", {})
    pipelines = payload.get("pipelines") if isinstance(payload, dict) else []
    return pipelines if isinstance(pipelines, list) else []


async def _write_pipeline_cache(pipelines: list[dict[str, Any]]) -> None:
    if not _db_path:
        return
    payload = json.dumps({"pipelines": pipelines}, ensure_ascii=False)
    async with _module_db() as db:
        await db.execute(
            """
            INSERT INTO pipeline_cache(id,payload,updated_at) VALUES(1,?,strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at
            """,
            (payload,),
        )
        await db.commit()


def _status_lookup(pipelines: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, str]]:
    lookup = {}
    for pipeline in pipelines:
        pipeline_id = _clean(pipeline.get("id"), 64)
        for status in pipeline.get("statuses") or []:
            status_id = _clean(status.get("id"), 64)
            lookup[(pipeline_id, status_id)] = {
                "pipeline_id": pipeline_id,
                "pipeline_name": _clean(pipeline.get("name"), 300),
                "status_id": status_id,
                "status_name": _clean(status.get("name"), 300),
                "status_color": _clean(status.get("color"), 64),
            }
    return lookup


def _field_values(entity: dict[str, Any]) -> list[dict[str, Any]]:
    fields = entity.get("custom_fields_values") or entity.get("custom_fields") or []
    return fields if isinstance(fields, list) else []


def _first_field_value(field: dict[str, Any]) -> str:
    values = field.get("values") or []
    if not isinstance(values, list) or not values:
        return ""
    value = values[0].get("value") if isinstance(values[0], dict) else values[0]
    return _clean(value, 2000)


def _extract_tracking(deal: dict[str, Any]) -> dict[str, str]:
    keys = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "yclid", "ym_uid", "_ym_uid")
    result = {}
    if isinstance(deal.get("utm"), dict):
        for key in keys:
            value = _clean(deal["utm"].get(key), 1000)
            if value:
                result["ym_uid" if key == "_ym_uid" else key] = value
    for key in keys:
        out_key = "ym_uid" if key == "_ym_uid" else key
        if result.get(out_key):
            continue
        value = _clean(deal.get(key), 1000)
        if value:
            result[out_key] = value
    for field in _field_values(deal):
        field_name = _clean(field.get("field_name"), 300).lower()
        field_code = _clean(field.get("field_code"), 300).lower()
        for key in keys:
            out_key = "ym_uid" if key == "_ym_uid" else key
            if result.get(out_key):
                continue
            if key in field_name or key in field_code:
                value = _first_field_value(field)
                if value:
                    result[out_key] = value
    return result


def _normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        return "7" + digits[1:]
    if len(digits) == 10:
        return "7" + digits
    return digits


def _extract_contact_channels(contact: dict[str, Any]) -> tuple[list[str], list[str]]:
    phones: list[str] = []
    emails: list[str] = []
    for field in _field_values(contact):
        field_code = _clean(field.get("field_code"), 100).lower()
        field_name = _clean(field.get("field_name"), 300).lower()
        for item in field.get("values") or []:
            value = item.get("value") if isinstance(item, dict) else item
            text = _clean(value, 500)
            if not text:
                continue
            if field_code == "phone" or "телефон" in field_name or "phone" in field_name:
                phone = _normalize_phone(text)
                if phone:
                    phones.append(phone)
            if field_code == "email" or "email" in field_name or "почта" in field_name:
                emails.append(text.lower())
    return sorted(dict.fromkeys(phones)), sorted(dict.fromkeys(emails))


async def _load_contact_details(deal: dict[str, Any], settings: dict[str, str]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    embedded = ((deal or {}).get("_embedded") or {}).get("contacts") or []
    contacts = []
    phones: list[str] = []
    emails: list[str] = []
    for item in embedded if isinstance(embedded, list) else []:
        if not isinstance(item, dict):
            continue
        contact_id = _clean(item.get("id"), 64)
        detailed = item
        if contact_id:
            body, error = await _amo_get(f"/api/v4/contacts/{contact_id}", settings)
            if body and not error:
                detailed = body
        item_phones, item_emails = _extract_contact_channels(detailed)
        phones.extend(item_phones)
        emails.extend(item_emails)
        contacts.append({
            "id": _clean(detailed.get("id") or contact_id, 64),
            "name": _clean(detailed.get("name"), 500),
            "is_main": bool(item.get("is_main")),
            "phones": item_phones,
            "emails": item_emails,
            "custom_fields_values": detailed.get("custom_fields_values") or [],
        })
    return contacts, sorted(dict.fromkeys(phones)), sorted(dict.fromkeys(emails))


def _extract_tags(deal: dict[str, Any]) -> list[dict[str, str]]:
    tags = ((deal or {}).get("_embedded") or {}).get("tags") or []
    result = []
    for tag in tags if isinstance(tags, list) else []:
        if isinstance(tag, dict):
            result.append({"id": _clean(tag.get("id"), 64), "name": _clean(tag.get("name"), 300)})
    return result


def _customer_db_path() -> Path:
    env_path = _env()["customer_db_path"]
    if env_path:
        return Path(env_path)
    if _module_dir is None:
        raise RuntimeError("module is not initialized")
    candidates = [
        _module_dir.parent / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent / "module_customer_db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


async def _ensure_customer_table(db: aiosqlite.Connection) -> None:
    await db.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS _cdb_tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            description TEXT DEFAULT '',
            schema_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE TABLE IF NOT EXISTS cdb_{TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform_id TEXT NOT NULL DEFAULT '',
            custom_fields TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_cdb_{TABLE_NAME}_platform_id ON cdb_{TABLE_NAME}(platform_id);
        """
    )
    schema = [
        {"name": "deal_name", "label": "Сделка", "type": "text"},
        {"name": "price", "label": "Сумма", "type": "number"},
        {"name": "pipeline_name", "label": "Воронка", "type": "text"},
        {"name": "status_name", "label": "Статус", "type": "text"},
        {"name": "contact_name", "label": "Контакт", "type": "text"},
        {"name": "updated_at_ts", "label": "Обновлена", "type": "number"},
    ]
    await db.execute(
        """
        INSERT INTO _cdb_tables(name,display_name,description,schema_json)
        VALUES(?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET display_name=excluded.display_name, description=excluded.description, schema_json=excluded.schema_json
        """,
        (TABLE_NAME, TABLE_DISPLAY_NAME, "Snapshot сделок amoCRM из webhook", json.dumps(schema, ensure_ascii=False)),
    )


def _deep_merge(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    result = dict(existing or {})
    for key, value in (incoming or {}).items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _deal_url(deal_id: str) -> str:
    base = _env()["amo_base_url"]
    return f"{base}/leads/detail/{deal_id}" if base and deal_id else ""


async def _upsert_customer_deal(deal_id: str, custom_fields: dict[str, Any]) -> dict[str, Any]:
    db_path = _customer_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with _customer_write_lock:
        async with aiosqlite.connect(db_path, timeout=60) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA busy_timeout=60000")
            await _ensure_customer_table(db)
            cur = await db.execute(f"SELECT id, custom_fields FROM cdb_{TABLE_NAME} WHERE platform_id=? ORDER BY id ASC", (deal_id,))
            rows = await cur.fetchall()
            duplicate_ids = [int(row["id"]) for row in rows[1:]]
            if rows:
                record_id = int(rows[0]["id"])
                merged = _json_loads(rows[0]["custom_fields"], {})
                for row in rows[1:]:
                    merged = _deep_merge(merged, _json_loads(row["custom_fields"], {}))
                merged = _deep_merge(merged, custom_fields)
                await db.execute(
                    f"UPDATE cdb_{TABLE_NAME} SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
                    (json.dumps(merged, ensure_ascii=False), record_id),
                )
                for duplicate_id in duplicate_ids:
                    await db.execute(f"DELETE FROM cdb_{TABLE_NAME} WHERE id=?", (duplicate_id,))
                action = "updated"
            else:
                cur = await db.execute(
                    f"INSERT INTO cdb_{TABLE_NAME}(platform_id,custom_fields) VALUES(?,?)",
                    (deal_id, json.dumps(custom_fields, ensure_ascii=False)),
                )
                record_id = int(cur.lastrowid)
                action = "created"
            await db.commit()
    return {"action": action, "record_id": record_id, "deduped": len(duplicate_ids), "db_path": str(db_path)}


async def _build_deal_record(deal: dict[str, Any], settings: dict[str, str], *, source_event: dict[str, Any] | None = None) -> dict[str, Any]:
    pipelines = await _read_pipeline_cache()
    if not pipelines:
        pipelines, _ = await _load_pipelines(settings, allow_cache=True)
    lookup = _status_lookup(pipelines)
    pipeline_id = _clean(deal.get("pipeline_id"), 64)
    status_id = _clean(deal.get("status_id"), 64)
    status_meta = lookup.get((pipeline_id, status_id), {})
    contacts, phones, emails = await _load_contact_details(deal, settings)
    tags = _extract_tags(deal)
    utms = _extract_tracking(deal)
    deal_id = _clean(deal.get("id"), 64)
    contact_name = next((_clean(item.get("name"), 500) for item in contacts if _clean(item.get("name"), 500)), "")
    tag_names = [tag["name"] for tag in tags if tag.get("name")]
    return {
        "platform": "amocrm",
        "deal_id": deal_id,
        "deal_name": _clean(deal.get("name"), 500),
        "price": deal.get("price") or 0,
        "pipeline_id": pipeline_id,
        "pipeline_name": status_meta.get("pipeline_name") or pipeline_id,
        "status_id": status_id,
        "status_name": status_meta.get("status_name") or status_id,
        "status_color": status_meta.get("status_color") or "",
        "responsible_user_id": _clean(deal.get("responsible_user_id"), 64),
        "account_id": _clean(deal.get("account_id"), 64),
        "created_at_ts": deal.get("created_at"),
        "updated_at_ts": deal.get("updated_at"),
        "closed_at_ts": deal.get("closed_at"),
        "loss_reason_id": deal.get("loss_reason_id"),
        "deal_url": _deal_url(deal_id),
        "contact_fields": {
            "name": contact_name,
            "phone": phones[0] if phones else "",
            "email": emails[0] if emails else "",
        },
        "contacts": contacts,
        "phones": phones,
        "emails": emails,
        "tags": tags,
        "tag_names": tag_names,
        "utms": utms,
        "possible_accounts": {
            "amo_deal_id": deal_id,
            "phone": phones[0] if phones else "",
            "email": emails[0] if emails else "",
            "salebot_id": utms.get("utm_term", ""),
            "utm_term": utms.get("utm_term", ""),
        },
        "amo": {
            "deal_id": deal_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": status_meta.get("pipeline_name") or pipeline_id,
            "status_id": status_id,
            "status_name": status_meta.get("status_name") or status_id,
            "responsible_user_id": _clean(deal.get("responsible_user_id"), 64),
        },
        "custom_fields_values": deal.get("custom_fields_values") or [],
        "raw_deal": deal,
        "last_webhook_event": source_event or {},
        "synced_at": _now(),
    }


async def _sync_deal(deal_id: str, *, source_event: dict[str, Any] | None = None, event_id: int | None = None) -> dict[str, Any]:
    settings = await _settings_map()
    body, error = await _amo_get(f"/api/v4/leads/{deal_id}?with=contacts,tags", settings)
    if error or not body:
        if event_id:
            await _update_event(event_id, success=False, ignored=False, error=error or "deal not found", details={"deal_id": deal_id})
        return {"ok": False, "deal_id": deal_id, "error": error or "deal not found"}
    record = await _build_deal_record(body, settings, source_event=source_event)
    storage = await _upsert_customer_deal(deal_id, record)
    await _refresh_successful_manager_deal(deal_id, record)
    if event_id:
        await _update_event(event_id, success=True, ignored=False, error="", details={"storage": storage, "status": record.get("status_name")})
    _log("info", "amocrm-db synced deal_id=%s action=%s", deal_id, storage.get("action"))
    return {"ok": True, "deal_id": deal_id, "storage": storage, "record": record}


def _deal_from_row(row: aiosqlite.Row) -> dict[str, Any]:
    fields = _json_loads(row["custom_fields"], {})
    return {
        "id": int(row["id"]),
        "platform_id": row["platform_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "custom_fields": fields,
    }


async def _amo_user_names() -> dict[str, str]:
    now = time.monotonic()
    if now < float(_amo_users_cache.get("expires_at") or 0):
        return dict(_amo_users_cache.get("items") or {})
    body, error = await _amo_get("/api/v4/users", await _settings_map(), params={"limit": "250"})
    if error:
        return dict(_amo_users_cache.get("items") or {})
    names = {
        _clean(user.get("id"), 64): _manager_name(user.get("name"))
        for user in (((body or {}).get("_embedded") or {}).get("users") or [])
        if isinstance(user, dict) and _clean(user.get("id"), 64)
    }
    _amo_users_cache.update(expires_at=now + 1800, items=names)
    return names


def _manager_name(value: Any) -> str:
    return re.sub(r"\s*\((?:auto|авто)\)\s*$", "", _clean(value, 300), flags=re.IGNORECASE).strip()


def _deal_identity_keys(fields: dict[str, Any]) -> set[str]:
    emails = list(fields.get("emails") or [])
    phones = list(fields.get("phones") or [])
    contact = fields.get("contact_fields") if isinstance(fields.get("contact_fields"), dict) else {}
    emails.append(contact.get("email"))
    phones.append(contact.get("phone"))
    return {
        *(f"email:{_clean(value, 320).casefold()}" for value in emails if _clean(value, 320)),
        *(f"phone:{_normalize_phone(value)}" for value in phones if _normalize_phone(value)),
    }


def _is_successful_deal(fields: dict[str, Any]) -> bool:
    amo = fields.get("amo") if isinstance(fields.get("amo"), dict) else {}
    status_id = _clean(fields.get("status_id") or amo.get("status_id"), 64)
    status_name = " ".join(
        _clean(fields.get("status_name") or amo.get("status_name"), 300).casefold().replace("ё", "е").split()
    )
    return status_id == "142" or status_name == "успешно реализовано"


def _rebuild_successful_manager_index_sync(module_db_path: str, customer_db_path: Path) -> int:
    if not customer_db_path.exists():
        return 0
    with sqlite3.connect(f"file:{customer_db_path}?mode=ro", uri=True, timeout=60) as source:
        source.row_factory = sqlite3.Row
        rows = source.execute(
            f"""
            SELECT platform_id,custom_fields,updated_at FROM cdb_{TABLE_NAME}
            WHERE json_valid(custom_fields)
            """
        ).fetchall()
    entries: list[tuple[str, str, str, str, str, str]] = []
    for row in rows:
        fields = _json_loads(row["custom_fields"], {})
        if not _is_successful_deal(fields):
            continue
        amo = fields.get("amo") if isinstance(fields.get("amo"), dict) else {}
        deal_id = _clean(row["platform_id"], 64)
        deal = (
            deal_id,
            _clean(fields.get("responsible_user_id") or amo.get("responsible_user_id"), 64),
            _manager_name(fields.get("responsible_user_name") or amo.get("responsible_user_name")),
            _clean(fields.get("deal_url"), 1000),
            _clean(row["updated_at"], 100),
        )
        entries.extend((identity, *deal) for identity in _deal_identity_keys(fields))
    with sqlite3.connect(module_db_path, timeout=60) as target:
        target.execute("PRAGMA busy_timeout=60000")
        target.execute("DELETE FROM successful_manager_identities")
        target.executemany(
            """
            INSERT OR REPLACE INTO successful_manager_identities(
                identity,deal_id,manager_id,manager_name,deal_url,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            entries,
        )
        target.commit()
    return len(entries)


async def _ensure_successful_manager_index() -> None:
    try:
        async with _module_db() as db:
            count = int((await (await db.execute("SELECT COUNT(*) FROM successful_manager_identities")).fetchone())[0])
        if count:
            return
        inserted = await asyncio.to_thread(
            _rebuild_successful_manager_index_sync, str(_db_path), _customer_db_path()
        )
        _log("info", "amocrm-db manager identity index initialized rows=%s", inserted)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log("warning", "amocrm-db manager identity index failed: %s", exc)


async def _refresh_successful_manager_deal(deal_id: str, fields: dict[str, Any]) -> None:
    async with _module_db() as db:
        await db.execute("DELETE FROM successful_manager_identities WHERE deal_id=?", (deal_id,))
        if _is_successful_deal(fields):
            amo = fields.get("amo") if isinstance(fields.get("amo"), dict) else {}
            await db.executemany(
                """
                INSERT OR REPLACE INTO successful_manager_identities(
                    identity,deal_id,manager_id,manager_name,deal_url,updated_at
                ) VALUES(?,?,?,?,?,?)
                """,
                [
                    (
                        identity,
                        deal_id,
                        _clean(fields.get("responsible_user_id") or amo.get("responsible_user_id"), 64),
                        _manager_name(fields.get("responsible_user_name") or amo.get("responsible_user_name")),
                        _clean(fields.get("deal_url"), 1000),
                        _clean(fields.get("updated_at") or fields.get("synced_at"), 100),
                    )
                    for identity in _deal_identity_keys(fields)
                ],
            )
        await db.commit()


async def service_successful_managers(*, identities: list[dict[str, Any]]) -> dict[str, Any]:
    """Resolve responsible users from the newest successful amoCRM deal without mutating amoCRM."""

    requested: dict[str, set[str]] = {}
    for item in identities[:250]:
        key = _clean(item.get("key"), 100)
        email = _clean(item.get("email"), 320).casefold()
        phone = _normalize_phone(item.get("phone"))
        if key:
            requested[key] = {
                *( [f"email:{email}"] if email else [] ),
                *( [f"phone:{phone}"] if phone else [] ),
            }
    needed = set().union(*requested.values()) if requested else set()
    if not needed:
        return {"ok": True, "items": []}
    async with _module_db() as db:
        db.row_factory = aiosqlite.Row
        rows = await (
            await db.execute(
                f"""
                SELECT identity,deal_id,manager_id,manager_name,deal_url,updated_at
                FROM successful_manager_identities
                WHERE identity IN ({','.join('?' for _ in needed)})
                ORDER BY updated_at DESC,deal_id DESC
                """,
                sorted(needed),
            )
        ).fetchall()
    matches: dict[str, dict[str, str]] = {}
    for row in rows:
        matches.setdefault(str(row["identity"]), {
            "deal_id": _clean(row["deal_id"], 64),
            "deal_url": _clean(row["deal_url"], 1000),
            "manager_id": _clean(row["manager_id"], 64),
            "manager_name": _clean(row["manager_name"], 300),
            "updated_at": _clean(row["updated_at"], 100),
        })
    names = await _amo_user_names() if matches else {}
    result = []
    for key, identity_keys in requested.items():
        candidates = [matches[value] for value in identity_keys if value in matches]
        deal = max(candidates, key=lambda item: item.get("updated_at", ""), default=None)
        if not deal:
            continue
        result.append({
            "key": key,
            **{name: value for name, value in deal.items() if name != "updated_at"},
            "manager_name": deal.get("manager_name") or names.get(deal["manager_id"], ""),
        })
    return {"ok": True, "items": result}


async def _amo_lead_ids_created_between(start_ts: int, end_ts: int, settings: dict[str, str]) -> tuple[list[str], str]:
    ids: list[str] = []
    for page in range(1, 51):
        body, error = await _amo_get(
            "/api/v4/leads",
            settings,
            params={
                "limit": "250",
                "page": str(page),
                "filter[created_at][from]": str(start_ts),
                "filter[created_at][to]": str(end_ts),
                "order[created_at]": "asc",
            },
        )
        if error:
            return ids, error
        batch = (((body or {}).get("_embedded") or {}).get("leads") or [])
        ids.extend(_clean(item.get("id"), 64) for item in batch if isinstance(item, dict) and _clean(item.get("id"), 64))
        next_href = ((((body or {}).get("_links") or {}).get("next") or {}).get("href") or "")
        if len(batch) < 250 or not next_href:
            break
    return ids, ""


async def _indexed_deal_ids(deal_ids: list[str]) -> set[str]:
    clean_ids = [deal_id for deal_id in dict.fromkeys(_clean(item, 64) for item in deal_ids) if deal_id]
    if not clean_ids:
        return set()
    db_path = _customer_db_path()
    if not db_path.exists():
        return set()
    result: set[str] = set()
    async with aiosqlite.connect(db_path, timeout=60) as db:
        await db.execute("PRAGMA busy_timeout=60000")
        await _ensure_customer_table(db)
        for idx in range(0, len(clean_ids), 500):
            chunk = clean_ids[idx:idx + 500]
            placeholders = ",".join("?" for _ in chunk)
            cur = await db.execute(f"SELECT platform_id FROM cdb_{TABLE_NAME} WHERE platform_id IN ({placeholders})", chunk)
            result.update(str(row[0]) for row in await cur.fetchall())
    return result


def _utc_day_bounds(day: datetime) -> tuple[int, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    start_ts = int(start.timestamp())
    return start_ts, start_ts + 86400 - 1


async def _reconcile_once(*, lookback_days: int | None = None, max_sync: int | None = None) -> dict[str, Any]:
    settings = await _settings_map()
    days = lookback_days if lookback_days is not None else _setting_int(settings, "reconcile_lookback_days", 14, min_value=1, max_value=90)
    days = max(1, min(90, int(days)))
    delay = _setting_float(settings, "reconcile_delay_seconds", 0.35, min_value=0.0, max_value=5.0)
    limit = max_sync if max_sync is not None else 500
    limit = max(1, min(2000, int(limit)))
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    started_at = _now()
    _reconcile_state.update({
        "running": True,
        "last_started_at": started_at,
        "last_finished_at": "",
        "last_error": "",
        "last_missing": 0,
        "last_synced": 0,
        "last_failed": 0,
        "last_days": [],
    })
    day_results: list[dict[str, Any]] = []
    all_missing: list[str] = []
    try:
        for offset in range(days - 1, -1, -1):
            day = today - timedelta(days=offset)
            start_ts, end_ts = _utc_day_bounds(day)
            ids, error = await _amo_lead_ids_created_between(start_ts, end_ts, settings)
            if error:
                raise RuntimeError(f"{day.date()}: {error}")
            indexed = await _indexed_deal_ids(ids)
            missing = [deal_id for deal_id in ids if deal_id not in indexed]
            all_missing.extend(missing)
            day_results.append({
                "day": day.date().isoformat(),
                "amo_count": len(ids),
                "indexed_count": len(indexed),
                "missing_count": len(missing),
            })
            await asyncio.sleep(0.2)
        synced = 0
        failed: list[dict[str, str]] = []
        for deal_id in all_missing[:limit]:
            result = await _sync_deal(deal_id, source_event={"action": "scheduled-reconcile", "started_at": started_at})
            if result.get("ok"):
                synced += 1
            else:
                failed.append({"deal_id": deal_id, "error": _clean(result.get("error"), 300)})
            if delay:
                await asyncio.sleep(delay)
        remaining = max(0, len(all_missing) - limit)
        finished_at = _now()
        payload = {
            "ok": not failed and remaining == 0,
            "started_at": started_at,
            "finished_at": finished_at,
            "lookback_days": days,
            "missing": len(all_missing),
            "synced": synced,
            "failed": failed,
            "remaining": remaining,
            "days": day_results,
        }
        _reconcile_state.update({
            "running": False,
            "last_finished_at": finished_at,
            "last_missing": len(all_missing),
            "last_synced": synced,
            "last_failed": len(failed),
            "last_days": day_results,
            "last_error": "" if payload["ok"] else f"failed={len(failed)} remaining={remaining}",
        })
        _log("info", "amocrm-db reconcile finished missing=%s synced=%s failed=%s remaining=%s", len(all_missing), synced, len(failed), remaining)
        return payload
    except Exception as exc:
        finished_at = _now()
        _reconcile_state.update({
            "running": False,
            "last_finished_at": finished_at,
            "last_error": str(exc),
            "last_missing": len(all_missing),
            "last_days": day_results,
        })
        _log("error", "amocrm-db reconcile failed: %s", exc)
        return {
            "ok": False,
            "started_at": started_at,
            "finished_at": finished_at,
            "lookback_days": days,
            "missing": len(all_missing),
            "synced": 0,
            "failed": [],
            "remaining": len(all_missing),
            "days": day_results,
            "error": str(exc),
        }


async def _reconcile_loop() -> None:
    await asyncio.sleep(120)
    while True:
        try:
            settings = await _settings_map()
            interval = _setting_int(settings, "reconcile_interval_minutes", 360, min_value=15, max_value=10080)
            if _setting_bool(settings, "reconcile_enabled", True) and not _reconcile_state.get("running"):
                await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "amocrm-db reconcile loop error: %s", exc)
        await asyncio.sleep(max(900, interval * 60))


async def _queue_counts() -> dict[str, int]:
    if not _db_path:
        return {}
    async with _module_db() as db:
        rows = await (await db.execute("SELECT queue_status,COUNT(*) FROM events GROUP BY queue_status")).fetchall()
    return {str(status): int(count) for status, count in rows}


@router.get("/health")
async def health():
    try:
        queue = await _queue_counts()
    except Exception as exc:
        return JSONResponse({"ok": False, "module": MODULE_ID, "error": str(exc)}, status_code=503)
    return {
        "ok": True,
        "module": MODULE_ID,
        "worker": bool(_queue_task and not _queue_task.done()),
        "queue": queue,
        "amo_requests": dict(_amo_state),
    }


@router.get("/env-status")
async def env_status(request: Request):
    await _require_panel_user(request)
    env = _env()
    settings = await _settings_map()
    db_path = _customer_db_path()
    return {
        "AMO_BASE_URL": bool(env["amo_base_url"]),
        "AMO_ACCESS_TOKEN": bool(env["amo_token"]),
        "webhook_secret": bool(settings.get("webhook_secret")),
        "webhook_secret_source": "env" if env["webhook_secret"] else "db",
        "customer_db_path": str(db_path),
        "customer_db_ready": db_path.exists() or db_path.parent.exists(),
        "ready": bool(env["amo_base_url"] and env["amo_token"]),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    return await _settings_payload()


@router.post("/settings")
async def post_settings(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.post("/webhook")
async def webhook(request: Request):
    settings = await _settings_map()
    raw_payload = "{}"
    try:
        if not _secret_ok(request, settings):
            await _store_event({
                "action": "webhook",
                "ignored": True,
                "error": "invalid secret",
                "details": {"reason": "invalid secret"},
                "raw_payload": "{}",
            })
            return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=200)
        payload, raw_payload = await _read_payload(request)
        events = _iter_lead_events(payload)
        if not events:
            await _store_event({
                "action": "webhook",
                "ignored": True,
                "error": "lead events not found",
                "details": {"payload_keys": list(payload.keys())[:50]},
                "raw_payload": raw_payload,
            })
            return {"ok": True, "processed": 0, "ignored": True, "error": "lead events not found"}
        results = []
        for event in events:
            deal_id = _clean(event.get("deal_id"), 64)
            event_id = await _store_event({**event, "ignored": not bool(deal_id), "raw_payload": raw_payload})
            if not deal_id:
                await _update_event(event_id, ignored=True, error="deal_id not found", details=event)
                results.append({"ok": True, "ignored": True, "event_id": event_id})
                continue
            await _enqueue_event(event_id, deal_id, settings)
            results.append({"ok": True, "queued": True, "deal_id": deal_id, "event_id": event_id})
        return {"ok": True, "processed": len(results), "results": results}
    except Exception as exc:
        _log("error", "webhook error: %s", exc)
        await _store_event({
            "action": "webhook",
            "success": False,
            "ignored": False,
            "error": str(exc),
            "details": {},
            "raw_payload": raw_payload,
        })
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=200)


@router.get("/webhook")
async def webhook_get():
    return {"ok": True, "hint": "Use POST from amoCRM"}


@router.get("/amo/pipelines")
async def amo_pipelines(request: Request, refresh: int = 0):
    await _require_panel_user(request)
    settings = await _settings_map()
    if not refresh:
        cached = await _read_pipeline_cache()
        if cached:
            return {"pipelines": cached, "cached": True}
    pipelines, error = await _load_pipelines(settings, allow_cache=True)
    status = 502 if error and not pipelines else 200
    return JSONResponse({"pipelines": pipelines, "error": error, "cached": bool(error and pipelines)}, status_code=status)


@router.post("/deals/{deal_id}/sync")
async def sync_deal_endpoint(deal_id: str, request: Request):
    await _require_panel_user(request)
    result = await _sync_deal(_clean(deal_id, 64), source_event={"action": "manual"})
    return JSONResponse(result, status_code=200 if result.get("ok") else 502)


@router.get("/deals")
async def list_deals(
    request: Request,
    pipeline_id: str = "",
    status_id: str = "",
    q: str = "",
    limit: int = 200,
    offset: int = 0,
):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit)))
    offset = max(0, int(offset))
    db_path = _customer_db_path()
    if not db_path.exists():
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    items = []
    total = 0
    query = q.strip().lower()
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_customer_table(db)
        cur = await db.execute(f"SELECT * FROM cdb_{TABLE_NAME} ORDER BY updated_at DESC")
        rows = await cur.fetchall()
    for row in rows:
        item = _deal_from_row(row)
        fields = item["custom_fields"]
        if pipeline_id and _clean(fields.get("pipeline_id"), 64) != pipeline_id:
            continue
        if status_id and _clean(fields.get("status_id"), 64) != status_id:
            continue
        haystack = " ".join([
            _clean(item.get("platform_id"), 64),
            _clean(fields.get("deal_name"), 500),
            _clean((fields.get("contact_fields") or {}).get("name"), 500),
            " ".join(fields.get("phones") or []),
            " ".join(fields.get("emails") or []),
            _clean((fields.get("utms") or {}).get("utm_term"), 1000),
        ]).lower()
        if query and query not in haystack:
            continue
        total += 1
        if len(items) < limit and total > offset:
            items.append(item)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/deals/{deal_id}")
async def get_deal(deal_id: str, request: Request):
    await _require_panel_user(request)
    db_path = _customer_db_path()
    if not db_path.exists():
        return JSONResponse({"error": "not found"}, status_code=404)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_customer_table(db)
        cur = await db.execute(f"SELECT * FROM cdb_{TABLE_NAME} WHERE platform_id=? ORDER BY id ASC LIMIT 1", (_clean(deal_id, 64),))
        row = await cur.fetchone()
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    return _deal_from_row(row)


@router.get("/events")
async def list_events(request: Request, limit: int = 200, result: str = "all"):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit)))
    where = ""
    if result == "ok":
        where = "WHERE success=1"
    elif result == "error":
        where = "WHERE success=0 AND ignored=0 AND error != '' AND queue_status NOT IN ('pending','processing','retry')"
    elif result == "ignored":
        where = "WHERE ignored=1"
    async with _module_db() as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", (limit,))
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["details"] = _json_loads(row.get("details"), {})
        row["raw_payload"] = _json_loads(row.get("raw_payload"), {})
    return rows


@router.get("/reconcile/status")
async def reconcile_status(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    return {
        **_reconcile_state,
        "enabled": _setting_bool(settings, "reconcile_enabled", True),
        "interval_minutes": _setting_int(settings, "reconcile_interval_minutes", 360, min_value=15, max_value=10080),
        "lookback_days": _setting_int(settings, "reconcile_lookback_days", 14, min_value=1, max_value=90),
    }


@router.get("/queue/status")
async def queue_status(request: Request):
    await _require_panel_user(request)
    return {
        "worker": bool(_queue_task and not _queue_task.done()),
        "queue": await _queue_counts(),
        "amo_requests": dict(_amo_state),
    }


@router.post("/reconcile/run")
async def reconcile_run(request: Request):
    await _require_panel_user(request)
    if _reconcile_state.get("running"):
        return JSONResponse({"ok": False, "error": "reconcile already running", **_reconcile_state}, status_code=409)
    data = {}
    try:
        data = await request.json()
    except Exception:
        data = {}
    lookback_days = _safe_int(data.get("lookback_days")) if isinstance(data, dict) else None
    max_sync = _safe_int(data.get("max_sync")) if isinstance(data, dict) else None
    return await _reconcile_once(lookback_days=lookback_days, max_sync=max_sync)


@router.get("/stats")
async def stats(request: Request):
    await _require_panel_user(request)
    db_path = _customer_db_path()
    deals = 0
    if db_path.exists():
        async with aiosqlite.connect(db_path) as db:
            await _ensure_customer_table(db)
            deals = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{TABLE_NAME}")).fetchone())[0]
    async with _module_db() as db:
        events = (await (await db.execute("SELECT COUNT(*) FROM events")).fetchone())[0]
        errors = (await (await db.execute("SELECT COUNT(*) FROM events WHERE success=0 AND ignored=0 AND error != '' AND queue_status NOT IN ('pending','processing','retry')")).fetchone())[0]
        ignored = (await (await db.execute("SELECT COUNT(*) FROM events WHERE ignored=1")).fetchone())[0]
    return {"deals": deals, "events": events, "errors": errors, "ignored": ignored, "queue": await _queue_counts()}
