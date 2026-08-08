from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "bizon-google-sheets"
DEFAULT_SPREADSHEET_ID = "1fh3oACPi--EpR0ciIzn8LQQFIuoFGE-ejy4yNnD-lvQ"
DEFAULT_WORKSHEET = "Bizon365 Nexus"
DEFAULT_VAKAS_WORKSHEET = "Vakas"
HEADERS = [
    "attendance_key", "person_key", "name", "email", "tel", "city",
    "click_button", "click_banner", "full_web", "watched_from", "watched_to",
    "date_web", "room_id", "webinar_id", "webinar_type", "web_min", "watch_seconds",
    "watch_valid", "watch_error", "comment", "utm_source", "utm_medium", "utm_campaign",
    "utm_content", "utm_term", "webinar_at", "cu1", "param1", "param2", "imported_at",
]
VAKAS_HEADERS = [
    "name", "email", "tel", "sity", "click_button", "click_banner", "full_web",
    "Watched from", "watched to", "date_web", "room_id", "web_min", "comment",
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "webinar_at", "cu1", "web_min", "param1", "param2",
]
DEFAULT_SETTINGS = {
    "feed_url": "http://127.0.0.1:8080/bizon-reports/api/attendance-feed",
    "feed_token": "",
    "spreadsheet_id": DEFAULT_SPREADSHEET_ID,
    "worksheet_title": DEFAULT_WORKSHEET,
    "vakas_mirror_enabled": "1",
    "vakas_worksheet_title": DEFAULT_VAKAS_WORKSHEET,
    "dry_run": "1",
    "poll_enabled": "1",
    "poll_seconds": "30",
    "request_timeout": "30",
    "feed_cursor": "0",
}

_db_path: Path | None = None
_logger: logging.Logger | None = None
_task: asyncio.Task | None = None
_sync_lock = asyncio.Lock()
_sheet_check_lock = asyncio.Lock()
_sheet_check_key: tuple[str, str, str, bool] | None = None
_sheet_check_until = 0.0
WRITE_THROTTLE_SECONDS = 2.1
RETRY_STATUSES = ("failed", "received")
DEFAULT_RETRY_LIMIT = 10


class SettingsIn(BaseModel):
    feed_url: str | None = None
    feed_token: str | None = None
    spreadsheet_id: str | None = None
    worksheet_title: str | None = None
    vakas_mirror_enabled: bool | None = None
    vakas_worksheet_title: str | None = None
    dry_run: bool | None = None
    poll_enabled: bool | None = None
    poll_seconds: int | None = None
    request_timeout: int | None = None


def setup(ctx):
    global _db_path, _logger, _task
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.bizon-google-sheets"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        if _task is None or _task.done():
            _task = loop.create_task(_poll_loop())
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _task
    task, _task = _task, None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("bizon-google-sheets is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "да"}


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '');
            CREATE TABLE IF NOT EXISTS sheet_rows(
                attendance_key TEXT PRIMARY KEY,
                row_number INTEGER NOT NULL,
                source_hash TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS vakas_sheet_rows(
                attendance_key TEXT PRIMARY KEY,
                row_number INTEGER NOT NULL,
                source_hash TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_id INTEGER UNIQUE NOT NULL,
                attendance_key TEXT NOT NULL DEFAULT '',
                source_hash TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '',
                row_number INTEGER,
                vakas_action TEXT NOT NULL DEFAULT '',
                vakas_row_number INTEGER,
                error TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_sheet_events_status ON events(status,id);
            """
        )
        cur = await db.execute("PRAGMA table_info(events)")
        event_columns = {str(row[1]) for row in await cur.fetchall()}
        if "vakas_action" not in event_columns:
            await db.execute("ALTER TABLE events ADD COLUMN vakas_action TEXT NOT NULL DEFAULT ''")
        if "vakas_row_number" not in event_columns:
            await db.execute("ALTER TABLE events ADD COLUMN vakas_row_number INTEGER")
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
        values = {str(row[0]): str(row[1]) for row in await cur.fetchall()}
    return {**DEFAULT_SETTINGS, **values}


async def _set_settings(values: dict[str, Any]) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        for key, value in values.items():
            await db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
        await db.commit()


def _credentials_path() -> Path | None:
    raw = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    return Path(raw) if raw else None


def _authorized_session():
    try:
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account
    except Exception as exc:
        raise RuntimeError(f"google-auth недоступен: {exc}") from exc
    path = _credentials_path()
    if not path or not path.is_file():
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS не указывает на существующий файл")
    credentials = service_account.Credentials.from_service_account_file(
        str(path), scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return AuthorizedSession(credentials)


async def _google_request(method: str, url: str, *, json_body: Any = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = await _settings()
    timeout = max(5, min(120, int(settings.get("request_timeout") or 30)))

    def run() -> dict[str, Any]:
        session = _authorized_session()
        response = session.request(method, url, json=json_body, params=params, timeout=timeout)
        if response.status_code >= 400:
            raise RuntimeError(f"Google Sheets HTTP {response.status_code}: {response.text[:1000]}")
        return response.json() if response.text else {}

    return await asyncio.to_thread(run)


def _sheet_base(spreadsheet_id: str) -> str:
    return f"https://sheets.googleapis.com/v4/spreadsheets/{quote(spreadsheet_id, safe='')}"


def _range(title: str, cells: str) -> str:
    return quote(f"'{title.replace(chr(39), chr(39)*2)}'!{cells}", safe="")


async def _ensure_sheet() -> None:
    global _sheet_check_key, _sheet_check_until
    settings = await _settings()
    spreadsheet_id, title = settings["spreadsheet_id"], settings["worksheet_title"]
    vakas_enabled = _bool(settings.get("vakas_mirror_enabled"))
    vakas_title = settings.get("vakas_worksheet_title") or DEFAULT_VAKAS_WORKSHEET
    key = (spreadsheet_id, title, vakas_title, vakas_enabled)
    now = time.monotonic()
    if key == _sheet_check_key and now < _sheet_check_until:
        return
    async with _sheet_check_lock:
        now = time.monotonic()
        if key == _sheet_check_key and now < _sheet_check_until:
            return
        base = _sheet_base(spreadsheet_id)
        metadata = await _google_request("GET", base, params={"fields": "sheets.properties"})
        titles = [str((item.get("properties") or {}).get("title") or "") for item in metadata.get("sheets") or []]
        if title not in titles:
            await _google_request(
                "POST",
                base + ":batchUpdate",
                json_body={"requests": [{"addSheet": {"properties": {"title": title, "gridProperties": {"frozenRowCount": 1}}}}]},
            )
        values_url = base + "/values/" + _range(title, "A1:AD1")
        current = await _google_request("GET", values_url)
        rows = current.get("values") or []
        if not rows:
            await _google_request("PUT", values_url, params={"valueInputOption": "RAW"}, json_body={"values": [HEADERS]})
        elif rows[0] != HEADERS:
            raise RuntimeError("Заголовок целевой вкладки не совпадает с ожидаемой схемой")
        if vakas_enabled:
            if vakas_title not in titles:
                raise RuntimeError(f"Вкладка {vakas_title} для зеркала Vakas не найдена")
            vakas_url = base + "/values/" + _range(vakas_title, "A1:W1")
            vakas_rows = (await _google_request("GET", vakas_url)).get("values") or []
            if not vakas_rows or vakas_rows[0] != VAKAS_HEADERS:
                raise RuntimeError("Заголовок вкладки Vakas не совпадает с ожидаемой схемой A:W; таблица не изменена")
        _sheet_check_key = key
        _sheet_check_until = time.monotonic() + 300


def _first_click(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(_clean((item or {}).get("id") if isinstance(item, dict) else item, 300) for item in value)
    return _clean(value, 1000)


def attendance_row(attendance: dict[str, Any], imported_at: str = "") -> list[Any]:
    profile = (attendance.get("profiles") or [{}])[0] if isinstance(attendance.get("profiles"), list) else {}
    watched_from = attendance.get("view") or profile.get("view") or ""
    watched_to = attendance.get("viewTill") or profile.get("viewTill") or ""
    return [
        attendance.get("attendance_key", ""), attendance.get("person_key", ""),
        attendance.get("username", ""), attendance.get("email", ""), attendance.get("phone", ""), attendance.get("city", ""),
        _first_click(attendance.get("buttons")), _first_click(attendance.get("banners")), 1 if attendance.get("finished") else 0,
        watched_from, watched_to, attendance.get("date_web") or "", attendance.get("roomid") or attendance.get("room_id") or "",
        attendance.get("webinarId", ""), attendance.get("type", ""), attendance.get("watch_minutes", ""), attendance.get("watch_seconds", ""),
        1 if attendance.get("watch_valid") else 0, attendance.get("watch_error", ""), attendance.get("comment", ""),
        attendance.get("utm_source", ""), attendance.get("utm_medium", ""), attendance.get("utm_campaign", ""), attendance.get("utm_content", ""), attendance.get("utm_term", ""),
        attendance.get("created") or attendance.get("webinar_at") or "", attendance.get("cu1", ""), attendance.get("p1") or attendance.get("param1") or "", attendance.get("p2") or attendance.get("param2") or "", imported_at or _now(),
    ]


def vakas_attendance_row(attendance: dict[str, Any]) -> list[Any]:
    profile = (attendance.get("profiles") or [{}])[0] if isinstance(attendance.get("profiles"), list) else {}
    watched_from = attendance.get("view") or profile.get("view") or ""
    watched_to = attendance.get("viewTill") or profile.get("viewTill") or ""
    web_min = attendance.get("watch_minutes", "")
    return [
        attendance.get("username", ""), attendance.get("email", ""), attendance.get("phone", ""), attendance.get("city", ""),
        _first_click(attendance.get("buttons")), _first_click(attendance.get("banners")), 1 if attendance.get("finished") else 0,
        watched_from, watched_to, attendance.get("date_web") or "", attendance.get("roomid") or attendance.get("room_id") or "",
        web_min, attendance.get("comment", ""), attendance.get("utm_source", ""), attendance.get("utm_medium", ""),
        attendance.get("utm_campaign", ""), attendance.get("utm_content", ""), attendance.get("utm_term", ""),
        attendance.get("created") or attendance.get("webinar_at") or "", attendance.get("cu1", ""), web_min,
        attendance.get("p1") or attendance.get("param1") or "", attendance.get("p2") or attendance.get("param2") or "",
    ]


async def _known_row(attendance_key: str) -> tuple[int, str] | None:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT row_number,source_hash FROM sheet_rows WHERE attendance_key=?", (attendance_key,))
        row = await cur.fetchone()
    return (int(row[0]), str(row[1])) if row else None


async def _remember_row(attendance_key: str, row_number: int, source_hash: str) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """INSERT INTO sheet_rows(attendance_key,row_number,source_hash,updated_at) VALUES(?,?,?,?) ON CONFLICT(attendance_key) DO UPDATE SET row_number=excluded.row_number,source_hash=excluded.source_hash,updated_at=excluded.updated_at""",
            (attendance_key, row_number, source_hash, _now()),
        )
        await db.commit()


async def _known_vakas_row(attendance_key: str) -> tuple[int, str] | None:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT row_number,source_hash FROM vakas_sheet_rows WHERE attendance_key=?", (attendance_key,))
        row = await cur.fetchone()
    return (int(row[0]), str(row[1])) if row else None


async def _remember_vakas_row(attendance_key: str, row_number: int, source_hash: str) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """INSERT INTO vakas_sheet_rows(attendance_key,row_number,source_hash,updated_at) VALUES(?,?,?,?) ON CONFLICT(attendance_key) DO UPDATE SET row_number=excluded.row_number,source_hash=excluded.source_hash,updated_at=excluded.updated_at""",
            (attendance_key, row_number, source_hash, _now()),
        )
        await db.commit()


def _row_number(updated_range: str) -> int:
    match = re.search(r"![A-Z]+(\d+):", updated_range or "")
    return int(match.group(1)) if match else 0


async def _write_row(attendance: dict[str, Any], source_hash: str, imported_at: str) -> tuple[str, int]:
    settings = await _settings()
    base = _sheet_base(settings["spreadsheet_id"])
    title = settings["worksheet_title"]
    key = _clean(attendance.get("attendance_key"), 300)
    known = await _known_row(key)
    row = attendance_row(attendance, imported_at)
    if known and known[1] == source_hash:
        return "unchanged", known[0]
    if known:
        row_number = known[0]
        url = base + "/values/" + _range(title, f"A{row_number}:AD{row_number}")
        await _google_request("PUT", url, params={"valueInputOption": "RAW"}, json_body={"values": [row]})
        action = "updated"
    else:
        url = base + "/values/" + _range(title, "A:AD") + ":append"
        response = await _google_request("POST", url, params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"}, json_body={"values": [row]})
        row_number = _row_number(((response.get("updates") or {}).get("updatedRange") or ""))
        if not row_number:
            raise RuntimeError("Google Sheets не вернул номер добавленной строки")
        action = "created"
    await _remember_row(key, row_number, source_hash)
    return action, row_number


async def _write_vakas_row(attendance: dict[str, Any], source_hash: str) -> tuple[str, int]:
    settings = await _settings()
    if not _bool(settings.get("vakas_mirror_enabled")):
        return "disabled", 0
    base = _sheet_base(settings["spreadsheet_id"])
    title = settings.get("vakas_worksheet_title") or DEFAULT_VAKAS_WORKSHEET
    key = _clean(attendance.get("attendance_key"), 300)
    known = await _known_vakas_row(key)
    row = vakas_attendance_row(attendance)
    if known and known[1] == source_hash:
        return "unchanged", known[0]
    if known:
        row_number = known[0]
        url = base + "/values/" + _range(title, f"A{row_number}:W{row_number}")
        await _google_request("PUT", url, params={"valueInputOption": "RAW"}, json_body={"values": [row]})
        action = "updated"
    else:
        url = base + "/values/" + _range(title, "A:W") + ":append"
        response = await _google_request("POST", url, params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"}, json_body={"values": [row]})
        row_number = _row_number(((response.get("updates") or {}).get("updatedRange") or ""))
        if not row_number:
            raise RuntimeError("Google Sheets не вернул номер строки Vakas")
        action = "created"
    await _remember_vakas_row(key, row_number, source_hash)
    return action, row_number


async def _insert_event(change: dict[str, Any]) -> int:
    attendance = change.get("attendance") if isinstance(change.get("attendance"), dict) else {}
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            """INSERT OR IGNORE INTO events(change_id,attendance_key,source_hash,status,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)""",
            (int(change.get("id") or 0), _clean(change.get("attendance_key"), 300), _clean(change.get("source_hash"), 100), "received", json.dumps(attendance, ensure_ascii=False, default=str), _now(), _now()),
        )
        await db.commit()
        if cur.lastrowid:
            return int(cur.lastrowid)
        return 0


async def _event_update(
    event_id: int,
    *,
    status: str,
    action: str = "",
    row_number: int | None = None,
    vakas_action: str = "",
    vakas_row_number: int | None = None,
    error: str = "",
    attempts: int = 1,
) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE events SET status=?,action=?,row_number=?,vakas_action=?,vakas_row_number=?,error=?,attempts=?,updated_at=? WHERE id=?",
            (status, action, row_number, vakas_action, vakas_row_number, error[:2000], attempts, _now(), event_id),
        )
        await db.commit()


async def _process_event(event_id: int) -> dict[str, Any]:
    async with _sync_lock:
        async with aiosqlite.connect(_must_db()) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
            row = await cur.fetchone()
        if not row:
            return {"ok": False, "error": "event not found"}
        event = dict(row)
        attendance = json.loads(event.get("payload_json") or "{}")
        attempts = int(event.get("attempts") or 0) + 1
        if _bool((await _settings()).get("dry_run")):
            await _event_update(event_id, status="shadow", action="upsert", attempts=attempts)
            return {"ok": True, "status": "shadow"}
        try:
            await _ensure_sheet()
            action, row_number = await _write_row(attendance, event["source_hash"], event.get("created_at") or _now())
            vakas_action, vakas_row_number = await _write_vakas_row(attendance, event["source_hash"])
            await _event_update(
                event_id,
                status="success",
                action=action,
                row_number=row_number,
                vakas_action=vakas_action,
                vakas_row_number=vakas_row_number or None,
                attempts=attempts,
            )
            return {
                "ok": True,
                "status": "success",
                "action": action,
                "row_number": row_number,
                "vakas_action": vakas_action,
                "vakas_row_number": vakas_row_number,
            }
        except Exception as exc:
            await _event_update(event_id, status="failed", action="upsert", error=str(exc), attempts=attempts)
            return {"ok": False, "error": str(exc)}


async def _poll_once(limit: int = 200) -> dict[str, Any]:
    settings = await _settings()
    token = os.environ.get("NEXUS_BIZON_FEED_TOKEN", "").strip() or settings.get("feed_token", "")
    if not token:
        return {"ok": False, "processed": 0, "error": "feed token не настроен"}
    cursor = int(settings.get("feed_cursor") or 0)
    timeout = max(5, min(120, int(settings.get("request_timeout") or 30)))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(settings["feed_url"], params={"after": cursor, "limit": min(500, limit)}, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    data = response.json()
    processed = 0
    items = data.get("items") or []
    for index, change in enumerate(items):
        event_id = await _insert_event(change)
        if event_id:
            await _process_event(event_id)
            processed += 1
        cursor = max(cursor, int(change.get("id") or 0))
        await _set_settings({"feed_cursor": cursor})
        if event_id and index < len(items) - 1:
            # A new attendance can write to both Nexus and Vakas tabs. Keep
            # manual and background batches below the Sheets per-minute quota.
            await asyncio.sleep(WRITE_THROTTLE_SECONDS)
    return {"ok": True, "processed": processed, "cursor": cursor, "has_more": bool(data.get("has_more"))}


async def _poll_loop() -> None:
    await asyncio.sleep(7)
    while True:
        wait = 30
        try:
            settings = await _settings()
            wait = max(5, min(300, int(settings.get("poll_seconds") or 30)))
            if _bool(settings.get("poll_enabled")):
                for _ in range(10):
                    result = await _poll_once()
                    if not result.get("has_more"):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "bizon-google-sheets poll failed: %s", exc)
        await asyncio.sleep(wait)


@router.get("/health")
async def health():
    settings = await _settings()
    path = _credentials_path()
    return {"ok": True, "module": MODULE_ID, "dry_run": _bool(settings.get("dry_run")), "cursor": int(settings.get("feed_cursor") or 0), "credentials_configured": bool(path and path.is_file())}


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    settings = await _settings()
    path = _credentials_path()
    env_token = os.environ.get("NEXUS_BIZON_FEED_TOKEN", "").strip()
    feed_token = env_token or settings.get("feed_token", "")
    return {
        **settings,
        "feed_token": "",
        "feed_token_configured": bool(feed_token),
        "feed_token_source": "env" if env_token else "module_db",
        "dry_run": _bool(settings.get("dry_run")),
        "poll_enabled": _bool(settings.get("poll_enabled")),
        "vakas_mirror_enabled": _bool(settings.get("vakas_mirror_enabled")),
        "credentials_configured": bool(path and path.is_file()),
        "headers": HEADERS,
        "vakas_headers": VAKAS_HEADERS,
    }


@router.put("/settings")
async def put_settings(data: SettingsIn, request: Request):
    global _sheet_check_key, _sheet_check_until
    await _require_user(request)
    values: dict[str, Any] = {}
    for key in ("feed_url", "feed_token", "spreadsheet_id", "worksheet_title", "vakas_worksheet_title"):
        value = getattr(data, key)
        if value is not None:
            values[key] = _clean(value, 2000)
    if data.dry_run is not None:
        values["dry_run"] = "1" if data.dry_run else "0"
    if data.poll_enabled is not None:
        values["poll_enabled"] = "1" if data.poll_enabled else "0"
    if data.vakas_mirror_enabled is not None:
        values["vakas_mirror_enabled"] = "1" if data.vakas_mirror_enabled else "0"
    if data.poll_seconds is not None:
        values["poll_seconds"] = max(5, min(300, data.poll_seconds))
    if data.request_timeout is not None:
        values["request_timeout"] = max(5, min(120, data.request_timeout))
    await _set_settings(values)
    if set(values) & {"spreadsheet_id", "worksheet_title", "vakas_worksheet_title", "vakas_mirror_enabled"}:
        _sheet_check_key, _sheet_check_until = None, 0.0
    return {"ok": True, "changed": sorted(values)}


@router.get("/events")
async def events(request: Request, limit: int = Query(100, ge=1, le=500), status: str = ""):
    await _require_user(request)
    where, params = ("WHERE status=?", [status]) if status else ("", [])
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", params + [limit])
        items = [dict(row) for row in await cur.fetchall()]
        counts_cur = await db.execute("SELECT status,COUNT(*) FROM events GROUP BY status")
        counts = {str(row[0]): int(row[1]) for row in await counts_cur.fetchall()}
        return {"items": items, "counts": counts}


@router.post("/sync/run")
async def sync_run(request: Request):
    await _require_user(request)
    return await _poll_once()


@router.post("/events/retry")
async def retry_events(request: Request, include_shadow: int = 0, limit: int = Query(DEFAULT_RETRY_LIMIT, ge=1, le=500)):
    await _require_user(request)
    statuses = ["shadow"] if include_shadow else list(RETRY_STATUSES)
    placeholders = ",".join("?" for _ in statuses)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(f"SELECT id FROM events WHERE status IN ({placeholders}) ORDER BY id ASC LIMIT ?", statuses + [limit])
        ids = [int(row[0]) for row in await cur.fetchall()]
    results = []
    for index, event_id in enumerate(ids):
        results.append(await _process_event(event_id))
        if index < len(ids) - 1:
            await asyncio.sleep(WRITE_THROTTLE_SECONDS)
    return {"ok": True, "processed": len(results), "failed": sum(1 for item in results if not item.get("ok"))}


@router.post("/sheet/check")
async def sheet_check(request: Request):
    await _require_user(request)
    await _ensure_sheet()
    return {"ok": True}
