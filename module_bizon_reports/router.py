from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import secrets
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from orchestrator.auth import ENV_PATH, _read_env_values, _write_env_values, can_access_module, verify_token_from_request

_logic_path = Path(__file__).with_name("logic.py")
_logic_spec = importlib.util.spec_from_file_location("_nexus_bizon_reports_logic", _logic_path)
logic = importlib.util.module_from_spec(_logic_spec)
assert _logic_spec and _logic_spec.loader
_logic_spec.loader.exec_module(logic)

router = APIRouter()

MODULE_ID = "bizon-reports"
DEFAULT_TIMEOUT = 20
BIZON_API_ROOT = "https://online.bizon365.ru/api/v2"

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None


class SettingsIn(BaseModel):
    webhook_secret: str | None = None
    request_timeout: int | None = None
    bizon_api_token: str | None = None
    project_id: str | None = None


class ReportImportIn(BaseModel):
    webinar_id: str


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.bizon-reports"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("bizon-reports module is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at         TEXT NOT NULL DEFAULT '',
                forward_url         TEXT NOT NULL DEFAULT '',
                webinar_id          TEXT NOT NULL DEFAULT '',
                raw_payload         TEXT NOT NULL DEFAULT '{}',
                viewers_source      TEXT NOT NULL DEFAULT '',
                viewers_count       INTEGER NOT NULL DEFAULT 0,
                records_count       INTEGER NOT NULL DEFAULT 0,
                db_ok               INTEGER NOT NULL DEFAULT 0,
                db_status           TEXT NOT NULL DEFAULT '',
                forward_ok          INTEGER NOT NULL DEFAULT 0,
                forward_status_code INTEGER,
                forward_error       TEXT NOT NULL DEFAULT '',
                completed_at        TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_events_webinar ON events(webinar_id);
            CREATE TABLE IF NOT EXISTS attendance_changes (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                attendance_key TEXT NOT NULL,
                source_hash    TEXT NOT NULL,
                event_id       INTEGER NOT NULL DEFAULT 0,
                payload_json   TEXT NOT NULL DEFAULT '{}',
                created_at     TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_attendance_change_version
                ON attendance_changes(attendance_key, source_hash);
            CREATE INDEX IF NOT EXISTS idx_attendance_changes_id ON attendance_changes(id);
            """
        )
        for key, value in {
            "webhook_secret": "",
            "request_timeout": str(DEFAULT_TIMEOUT),
            "customer_table_ready": "0",
            "project_id": "97242",
            "feed_token": "",
        }.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (key, value, _now()),
            )
        if not _env_secret():
            cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
            row = await cur.fetchone()
            if not row or not _clean(row[0]):
                await db.execute(
                    """
                    INSERT INTO settings(key,value,updated_at) VALUES('webhook_secret',?,?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                    """,
                    (secrets.token_urlsafe(24), _now()),
                )
        cur = await db.execute("SELECT value FROM settings WHERE key='feed_token'")
        row = await cur.fetchone()
        if not row or not _clean(row[0]):
            await db.execute(
                "UPDATE settings SET value=?, updated_at=? WHERE key='feed_token'",
                (secrets.token_urlsafe(32), _now()),
            )
        await db.commit()
    try:
        await _ensure_customer_table()
    except Exception as exc:
        _log("warning", "bizon-reports customer table ensure failed: %s", exc)
    _log("info", "bizon-reports DB initialized")


def _env_secret() -> str:
    return os.environ.get("NEXUS_BIZON_REPORTS_WEBHOOK_SECRET", "").strip()


def _bizon_api_token() -> str:
    token = os.environ.get("BIZON365_API_TOKEN", "").strip()
    if token:
        return token
    try:
        values = _read_env_values()
    except Exception:
        values = {}
    token = str(values.get("BIZON365_API_TOKEN") or "").strip()
    if token:
        os.environ["BIZON365_API_TOKEN"] = token
    return token


async def _setting(key: str, fallback: str = "") -> str:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
    return _clean(row[0] if row else fallback, 5000)


async def _webhook_secret() -> str:
    return _env_secret() or await _setting("webhook_secret")


async def _timeout() -> int:
    try:
        return max(5, min(60, int(await _setting("request_timeout", str(DEFAULT_TIMEOUT)))))
    except Exception:
        return DEFAULT_TIMEOUT


async def _settings_public() -> dict[str, Any]:
    webhook_secret = await _webhook_secret()
    timeout = await _timeout()
    return {
        "webhook_secret": webhook_secret,
        "webhook_secret_source": "env" if _env_secret() else "module_db",
        "request_timeout": timeout,
        "bizon_api_token_configured": bool(_bizon_api_token()),
        "customer_db_token_configured": bool(_customer_db_token()),
        "customer_table": logic.TABLE_NAME,
        "customer_table_display": logic.TABLE_DISPLAY_NAME,
        "attendance_table": "bizon365_attendance",
        "project_id": await _setting("project_id", "97242"),
        "feed_token": await _setting("feed_token"),
        "allowed_forward_prefix": logic.VAKAS_ALLOWED_PREFIX,
        "env_path": str(ENV_PATH),
    }


async def _save_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, _now()),
        )
        await db.commit()


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _extract_token(request: Request, payload: dict[str, Any] | None = None) -> str:
    for key in ("secret", "token", "api_token", "webhook_secret"):
        value = _clean(request.query_params.get(key), 500)
        if value:
            return value
    if isinstance(payload, dict):
        for key in ("secret", "token", "api_token", "webhook_secret"):
            value = _clean(payload.get(key), 500)
            if value:
                return value
    return ""


async def _require_webhook_secret(request: Request, payload: dict[str, Any] | None = None) -> None:
    expected = await _webhook_secret()
    if not expected:
        raise HTTPException(503, "webhook secret is not configured")
    candidate = _extract_token(request, payload)
    if not candidate or not secrets.compare_digest(candidate, expected):
        raise HTTPException(401, "unauthorized")


def _query_payload_for_get(request: Request) -> dict[str, Any]:
    excluded = {"url", "secret", "token", "api_token", "webhook_secret"}
    data: dict[str, Any] = {}
    for key, value in request.query_params.multi_items():
        key = str(key)
        if key in excluded:
            continue
        if key in data:
            current = data[key]
            if isinstance(current, list):
                current.append(value)
            else:
                data[key] = [current, value]
        else:
            data[key] = value
    return data


async def _request_payload_and_forward_body(request: Request) -> tuple[dict[str, Any], bytes, str, dict[str, Any] | None]:
    if request.method.upper() == "GET":
        payload = _query_payload_for_get(request)
        return payload, b"", "application/x-www-form-urlencoded", logic.sanitize_payload(payload)

    content_type = request.headers.get("content-type", "application/json")
    raw = await request.body()
    payload: dict[str, Any] = {}
    if "application/json" in content_type.lower():
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
        except Exception as exc:
            raise HTTPException(400, f"invalid json: {exc}") from exc
        if not isinstance(data, dict):
            raise HTTPException(400, "JSON body must be an object")
        payload = data
    else:
        form = await request.form()
        payload = {str(key): value for key, value in form.multi_items()}

    sanitized = logic.sanitize_payload(payload)
    if sanitized != payload:
        if "application/json" in content_type.lower():
            return payload, json.dumps(sanitized, ensure_ascii=False).encode("utf-8"), "application/json", None
        return payload, urlencode(sanitized, doseq=True).encode("utf-8"), "application/x-www-form-urlencoded", None
    return payload, raw, content_type, None


def _customer_db_url(path: str) -> str:
    base = os.environ.get("NEXUS_INTERNAL_BASE", "http://127.0.0.1:8080").rstrip("/")
    return f"{base}/customer-db/api{path}"


def _customer_db_token() -> str:
    token = os.environ.get("NEXUS_CUSTOMER_DB_API_TOKEN", "").strip()
    if token:
        return token
    try:
        values = _read_env_values()
    except Exception:
        values = {}
    token = str(values.get("NEXUS_CUSTOMER_DB_API_TOKEN") or "").strip()
    if token:
        os.environ["NEXUS_CUSTOMER_DB_API_TOKEN"] = token
    return token


def _customer_db_path() -> Path:
    env_path = os.environ.get("BIZON_REPORTS_CUSTOMER_DB_PATH", "").strip()
    if env_path:
        return Path(env_path)
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    module_dir = Path(_module_dir)
    candidates = [
        module_dir.parent / "customer-db" / "data" / "customer-db.db",
        module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
        module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


async def _ensure_customer_table() -> None:
    db_path = _customer_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = [
        {"name": "username", "label": "Имя", "type": "text"},
        {"name": "email", "label": "Email", "type": "text"},
        {"name": "phone", "label": "Телефон", "type": "text"},
        {"name": "webinarId", "label": "Webinar ID", "type": "text"},
        {"name": "roomid", "label": "Комната", "type": "text"},
        {"name": "utm_source", "label": "UTM source", "type": "text"},
        {"name": "utm_campaign", "label": "UTM campaign", "type": "text"},
    ]
    attendance_schema = schema + [
        {"name": "attendance_key", "label": "Attendance key", "type": "text"},
        {"name": "person_key", "label": "Person key", "type": "text"},
        {"name": "watch_minutes", "label": "Минут на вебинаре", "type": "number"},
        {"name": "watch_valid", "label": "Время валидно", "type": "boolean"},
        {"name": "watch_error", "label": "Ошибка времени", "type": "text"},
    ]
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS _cdb_tables (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description  TEXT DEFAULT '',
                schema_json  TEXT NOT NULL DEFAULT '[]',
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS cdb_{logic.TABLE_NAME} (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id   TEXT NOT NULL DEFAULT '',
                custom_fields TEXT NOT NULL DEFAULT '{{}}',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_cdb_{logic.TABLE_NAME}_platform_id ON cdb_{logic.TABLE_NAME}(platform_id);
            CREATE TABLE IF NOT EXISTS cdb_bizon365_attendance (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id   TEXT NOT NULL DEFAULT '',
                custom_fields TEXT NOT NULL DEFAULT '{{}}',
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_cdb_bizon365_attendance_platform_id
                ON cdb_bizon365_attendance(platform_id);
            """
        )
        await db.execute(
            """
            INSERT INTO _cdb_tables(name,display_name,description,schema_json)
            VALUES(?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                display_name=excluded.display_name,
                description=excluded.description,
                schema_json=excluded.schema_json
            """,
            (
                logic.TABLE_NAME,
                logic.TABLE_DISPLAY_NAME,
                "Клиенты из отчетов Bizon365, принятые временным перехватчиком",
                json.dumps(schema, ensure_ascii=False),
            ),
        )
        await db.execute(
            """
            INSERT INTO _cdb_tables(name,display_name,description,schema_json)
            VALUES(?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                display_name=excluded.display_name,
                description=excluded.description,
                schema_json=excluded.schema_json
            """,
            (
                "bizon365_attendance",
                "Посещения Bizon365",
                "Отдельный идемпотентный факт посещения: человек + вебинар",
                json.dumps(attendance_schema, ensure_ascii=False),
            ),
        )
        await db.commit()


async def _record_event_start(payload: dict[str, Any], forward_url: str, webinar_id: str) -> int:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            """
            INSERT INTO events(received_at, forward_url, webinar_id, raw_payload)
            VALUES(?,?,?,?)
            """,
            (
                _now(),
                forward_url,
                webinar_id,
                json.dumps(logic.sanitize_payload(payload), ensure_ascii=False, default=str),
            ),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _record_event_finish(event_id: int, **updates: Any) -> None:
    allowed = {
        "viewers_source",
        "viewers_count",
        "records_count",
        "db_ok",
        "db_status",
        "forward_ok",
        "forward_status_code",
        "forward_error",
    }
    pairs = [(key, value) for key, value in updates.items() if key in allowed]
    pairs.append(("completed_at", _now()))
    assignments = ", ".join(f"{key}=?" for key, _ in pairs)
    values = [value for _, value in pairs]
    values.append(event_id)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(f"UPDATE events SET {assignments} WHERE id=?", values)
        await db.commit()


async def _fetch_bizon_viewers(webinar_id: str, timeout: int) -> list[dict[str, Any]]:
    token = _bizon_api_token()
    if not webinar_id or not token:
        return []
    viewers: list[dict[str, Any]] = []
    skip = 0
    limit = 1000
    headers = {"X-Token": token}
    project_id = await _setting("project_id", "97242")
    url = f"{BIZON_API_ROOT}/{project_id}/reports/getviewers"
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            response = await client.get(
                url,
                params={"webinarId": webinar_id, "skip": skip, "limit": limit},
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
            batch = data.get("viewers") if isinstance(data, dict) else None
            if not isinstance(batch, list) or not batch:
                break
            viewers.extend(item for item in batch if isinstance(item, dict))
            loaded = int(data.get("loaded") or len(batch))
            total = int(data.get("total") or len(viewers))
            skip += loaded
            if loaded <= 0 or skip >= total or len(batch) < limit:
                break
    return viewers


async def _fetch_bizon_report(webinar_id: str, timeout: int) -> dict[str, Any]:
    token = _bizon_api_token()
    if not webinar_id or not token:
        return {}
    project_id = await _setting("project_id", "97242")
    url = f"{BIZON_API_ROOT}/{project_id}/reports/get"
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params={"webinarId": webinar_id}, headers={"X-Token": token})
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, dict) else {}


async def _fetch_enriched_viewers(webinar_id: str, timeout: int) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    try:
        report_payload = await _fetch_bizon_report(webinar_id, timeout)
        viewers = logic.extract_viewers(report_payload)
        if viewers:
            return viewers, logic.report_meta_from_payload(report_payload), "bizon_report"
    except Exception as exc:
        _log("warning", "bizon-reports full report unavailable webinar_id=%s error=%s", webinar_id, exc)
    viewers = await _fetch_bizon_viewers(webinar_id, timeout)
    return viewers, {}, "bizon_api" if viewers else "none"


async def _fetch_bizon_reports(
    *,
    skip: int,
    limit: int,
    report_type: str,
    min_date: str,
    max_date: str,
    timeout: int,
) -> dict[str, Any]:
    token = _bizon_api_token()
    if not token:
        raise HTTPException(400, "BIZON365_API_TOKEN не задан")
    params: dict[str, Any] = {"skip": skip, "limit": limit}
    if report_type in {"LiveWebinars", "AutoWebinars"}:
        params["type"] = report_type
    if min_date:
        params["minDate"] = min_date
    if max_date:
        params["maxDate"] = max_date
    project_id = await _setting("project_id", "97242")
    url = f"{BIZON_API_ROOT}/{project_id}/reports/getlist"
    params["limit"] = min(100, int(params["limit"]))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params=params, headers={"X-Token": token})
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, dict):
        raise HTTPException(502, "Bizon reports getlist returned unexpected response")
    return data


async def _imported_reports_map() -> dict[str, dict[str, Any]]:
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT webinar_id,
                   MAX(received_at) AS imported_at,
                   MAX(records_count) AS records_count,
                   MAX(viewers_count) AS viewers_count
            FROM events
            WHERE db_ok=1 AND webinar_id <> '' AND records_count > 0
            GROUP BY webinar_id
            """
        )
        rows = await cur.fetchall()
    return {str(row["webinar_id"]): dict(row) for row in rows}


def _payload_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _record_attendance_changes(records: list[dict[str, Any]], event_id: int) -> int:
    created = 0
    async with aiosqlite.connect(_must_db()) as db:
        for record in records:
            attendance_key = _clean(record.get("platform_id"), 300)
            fields = record.get("custom_fields") if isinstance(record.get("custom_fields"), dict) else {}
            if not attendance_key:
                continue
            source_hash = _payload_hash(fields)
            cur = await db.execute(
                """
                INSERT OR IGNORE INTO attendance_changes(
                    attendance_key,source_hash,event_id,payload_json,created_at
                ) VALUES(?,?,?,?,?)
                """,
                (attendance_key, source_hash, event_id, json.dumps(fields, ensure_ascii=False, default=str), _now()),
            )
            created += int(cur.rowcount or 0)
        await db.commit()
    return created


async def _require_feed_token(request: Request) -> None:
    expected = await _setting("feed_token")
    authorization = request.headers.get("authorization", "")
    candidate = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    if not expected or not candidate or not secrets.compare_digest(candidate, expected):
        raise HTTPException(401, "unauthorized")


async def _upsert_customer_records(
    records: list[dict[str, Any]],
    timeout: int,
    table: str = logic.TABLE_NAME,
) -> dict[str, Any]:
    if not records:
        return {"ok": True, "stored": 0, "status": "no_records"}
    token = _customer_db_token()
    if not token:
        raise RuntimeError("NEXUS_CUSTOMER_DB_API_TOKEN is not configured")
    result = {"ok": True, "stored": 0, "chunks": []}
    async with httpx.AsyncClient(timeout=timeout) as client:
        for start in range(0, len(records), 1000):
            chunk = records[start : start + 1000]
            response = await client.post(
                _customer_db_url(f"/tables/{table}/records/batch-upsert"),
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"records": chunk},
            )
            if response.status_code == 404:
                await _ensure_customer_table()
                response = await client.post(
                    _customer_db_url(f"/tables/{table}/records/batch-upsert"),
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"records": chunk},
                )
            response.raise_for_status()
            data = response.json()
            result["chunks"].append(data)
            result["stored"] += int(data.get("created") or 0) + int(data.get("updated") or 0)
    return result


async def _forward_report(
    url: str,
    body: bytes,
    content_type: str,
    timeout: int,
    *,
    method: str = "POST",
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"Content-Type": content_type or "application/json"}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        if method.upper() == "GET":
            response = await client.get(url, params=params or {}, headers=headers)
        else:
            response = await client.post(url, content=body, headers=headers)
    return {
        "ok": 200 <= response.status_code < 300,
        "status_code": response.status_code,
        "text": response.text[:1000],
    }


@router.get("/health")
async def health():
    return {
        "ok": True,
        "module": MODULE_ID,
        "table": logic.TABLE_NAME,
        "bizon_api_token": bool(_bizon_api_token()),
        "customer_db_token": bool(_customer_db_token()),
        "env_path": str(ENV_PATH),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    return await _settings_public()


@router.put("/settings")
async def put_settings(data: SettingsIn, request: Request):
    await _require_panel_user(request)

    if data.webhook_secret is not None:
        value = _clean(data.webhook_secret, 500)
        if len(value) < 12:
            raise HTTPException(400, "webhook_secret должен быть не короче 12 символов")
        await _save_setting("webhook_secret", value)

    if data.request_timeout is not None:
        value = str(max(5, min(60, int(data.request_timeout))))
        await _save_setting("request_timeout", value)

    if data.bizon_api_token:
        token = _clean(data.bizon_api_token, 2000)
        values = _read_env_values()
        values["BIZON365_API_TOKEN"] = token
        _write_env_values(values)
        os.environ["BIZON365_API_TOKEN"] = token

    if data.project_id is not None:
        project_id = _clean(data.project_id, 100)
        if not project_id or not project_id.replace("-", "").replace("_", "").isalnum():
            raise HTTPException(400, "project_id содержит недопустимые символы")
        await _save_setting("project_id", project_id)

    return await _settings_public()


@router.get("/settings/token")
async def settings_token(request: Request):
    await _require_panel_user(request)
    secret = await _webhook_secret()
    return {
        "secret": secret,
        "source": "NEXUS_BIZON_REPORTS_WEBHOOK_SECRET" if _env_secret() else "module DB",
        "sample_url": (
            "/nexus/bizon-reports/api/webhook"
            f"?secret={secret}"
            "&url="
        ),
        "allowed_forward_prefix": logic.VAKAS_ALLOWED_PREFIX,
    }


@router.get("/reports")
async def reports(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    report_type: str = Query("", alias="type"),
    min_date: str = Query("", alias="minDate"),
    max_date: str = Query("", alias="maxDate"),
):
    await _require_panel_user(request)
    timeout = await _timeout()
    data = await _fetch_bizon_reports(
        skip=skip,
        limit=limit,
        report_type=_clean(report_type, 50),
        min_date=_clean(min_date, 80),
        max_date=_clean(max_date, 80),
        timeout=timeout,
    )
    raw_items = data.get("reports") or data.get("items") or data.get("list") or []
    if not isinstance(raw_items, list):
        raw_items = []
    imported = await _imported_reports_map()
    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        webinar_id = _clean(
            raw.get("webinarId")
            or raw.get("webinar_id")
            or raw.get("id")
            or raw.get("reportId")
            or raw.get("report_id"),
            1000,
        )
        imported_meta = imported.get(webinar_id) if webinar_id else None
        items.append({
            "webinar_id": webinar_id,
            "name": _clean(raw.get("name") or raw.get("title") or raw.get("room") or "", 1000),
            "created": _clean(raw.get("created") or raw.get("createdAt") or raw.get("date") or "", 200),
            "type": _clean(raw.get("type") or "", 100),
            "in_base": bool(imported_meta),
            "imported_at": imported_meta.get("imported_at") if imported_meta else "",
            "records_count": int((imported_meta or {}).get("records_count") or 0),
            "viewers_count": int((imported_meta or {}).get("viewers_count") or 0),
            "raw": raw,
        })
    return {
        "items": items,
        "total": data.get("total", data.get("count", len(items))),
        "loaded": data.get("loaded", len(items)),
        "skip": skip,
        "limit": limit,
    }


@router.post("/reports/import")
async def import_report(data: ReportImportIn, request: Request):
    await _require_panel_user(request)
    webinar_id = _clean(data.webinar_id, 1000)
    if not webinar_id:
        raise HTTPException(400, "webinar_id обязателен")

    timeout = await _timeout()
    event_id = await _record_event_start(
        {"source": "manual_report_import", "webinarId": webinar_id},
        "",
        webinar_id,
    )
    viewers: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    viewers_source = "none"
    db_result: dict[str, Any] = {"ok": False}
    db_ok = 0
    db_error = ""
    try:
        viewers, api_meta, viewers_source = await _fetch_enriched_viewers(webinar_id, timeout)
        meta = {**api_meta, "webinarId": webinar_id}
        client_records = logic.normalize_viewers(viewers, meta)
        records = logic.normalize_attendances(viewers, meta)
        await _ensure_customer_table()
        clients_result = await _upsert_customer_records(client_records, timeout)
        attendance_result = await _upsert_customer_records(records, timeout, "bizon365_attendance")
        change_count = await _record_attendance_changes(records, event_id)
        db_result = {"clients": clients_result, "attendance": attendance_result, "changes": change_count}
        db_ok = 1
    except Exception as exc:
        db_error = str(exc)[:1000]
        _log("error", "bizon-reports manual import failed event_id=%s error=%s", event_id, exc, exc_info=True)

    await _record_event_finish(
        event_id,
        viewers_source=viewers_source,
        viewers_count=len(viewers),
        records_count=len(records),
        db_ok=db_ok,
        db_status=json.dumps(db_result if db_ok else {"ok": False, "error": db_error}, ensure_ascii=False),
        forward_ok=0,
        forward_status_code=None,
        forward_error="manual_import_no_forward",
    )
    if not db_ok:
        raise HTTPException(502, db_error or "import failed")
    return {
        "ok": True,
        "event_id": event_id,
        "webinar_id": webinar_id,
        "viewers_count": len(viewers),
        "records_count": len(records),
        "customer_db": db_result,
    }


@router.get("/events")
async def events(request: Request, limit: int = Query(50, ge=1, le=200)):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return {"items": [dict(row) for row in await cur.fetchall()]}


@router.get("/attendance-feed")
async def attendance_feed(
    request: Request,
    after: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=500),
):
    await _require_feed_token(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id,attendance_key,source_hash,event_id,payload_json,created_at
            FROM attendance_changes
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (after, limit),
        )
        rows = await cur.fetchall()
    items = []
    for row in rows:
        item = dict(row)
        try:
            item["attendance"] = json.loads(item.pop("payload_json") or "{}")
        except Exception:
            item["attendance"] = {}
            item.pop("payload_json", None)
        items.append(item)
    next_cursor = int(items[-1]["id"]) if items else after
    return {"items": items, "after": after, "next_cursor": next_cursor, "has_more": len(items) == limit}


@router.get("/webhook")
@router.post("/webhook")
async def webhook(request: Request, url: str = Query("")):
    payload, forward_body, forward_content_type, forward_params = await _request_payload_and_forward_body(request)
    await _require_webhook_secret(request, payload)
    forward_url = _clean(url, 3000)
    if forward_url and not logic.is_allowed_forward_url(forward_url):
        raise HTTPException(400, "url must start with https://vakas-tools.ru/base/report/")

    timeout = await _timeout()
    webinar_id = logic.extract_webinar_id(payload)
    event_id = await _record_event_start(payload, forward_url, webinar_id)

    viewers = logic.extract_viewers(payload)
    viewers_source = "webhook" if viewers else ""
    db_result: dict[str, Any] = {"ok": False, "stored": 0, "status": "not_run"}
    db_error = ""
    try:
        api_meta: dict[str, Any] = {}
        if webinar_id:
            api_viewers, api_meta, api_source = await _fetch_enriched_viewers(webinar_id, timeout)
            if api_viewers:
                viewers, viewers_source = api_viewers, api_source
        meta = {**api_meta, **logic.report_meta_from_payload(payload)}
        if webinar_id:
            meta.setdefault("webinarId", webinar_id)
        client_records = logic.normalize_viewers(viewers, meta)
        records = logic.normalize_attendances(viewers, meta)
        await _ensure_customer_table()
        clients_result = await _upsert_customer_records(client_records, timeout)
        attendance_result = await _upsert_customer_records(records, timeout, "bizon365_attendance")
        change_count = await _record_attendance_changes(records, event_id)
        db_result = {"clients": clients_result, "attendance": attendance_result, "changes": change_count}
        db_ok = 1
    except Exception as exc:
        db_error = str(exc)[:1000]
        db_ok = 0
        records = []
        _log("error", "bizon-reports customer-db failed event_id=%s error=%s", event_id, exc, exc_info=True)

    forward_result: dict[str, Any]
    if forward_url:
        try:
            forward_result = await _forward_report(
                forward_url,
                forward_body,
                forward_content_type,
                timeout,
                method=request.method,
                params=forward_params,
            )
        except Exception as exc:
            forward_result = {"ok": False, "status_code": None, "text": str(exc)[:1000]}
            _log("error", "bizon-reports forward failed event_id=%s error=%s", event_id, exc, exc_info=True)
    else:
        forward_result = {"ok": True, "status_code": None, "text": "no_forward_url"}

    await _record_event_finish(
        event_id,
        viewers_source=viewers_source or "none",
        viewers_count=len(viewers),
        records_count=len(records),
        db_ok=db_ok,
        db_status=json.dumps(db_result if db_ok else {"ok": False, "error": db_error}, ensure_ascii=False),
        forward_ok=1 if forward_url and forward_result.get("ok") else 0,
        forward_status_code=forward_result.get("status_code"),
        forward_error="" if forward_url and forward_result.get("ok") else _clean(forward_result.get("text"), 1000),
    )
    _log(
        "info",
        "bizon_report event_id=%s webinar_id=%s viewers=%s records=%s db_ok=%s forward_status=%s",
        event_id,
        webinar_id or "-",
        len(viewers),
        len(records),
        db_ok,
        forward_result.get("status_code"),
    )
    response_ok = bool(forward_result.get("ok")) if forward_url else bool(db_ok)
    return JSONResponse(
        {
            "ok": response_ok,
            "event_id": event_id,
            "webinar_id": webinar_id,
            "viewers_source": viewers_source or "none",
            "viewers_count": len(viewers),
            "records_count": len(records),
            "customer_db": db_result if db_ok else {"ok": False, "error": db_error},
            "forward": {
                "ok": bool(forward_result.get("ok")) if forward_url else None,
                "status_code": forward_result.get("status_code"),
                "skipped": not bool(forward_url),
            },
        },
        status_code=200 if response_ok else 502,
    )
