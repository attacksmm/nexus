from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote_plus

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "amocrm-salebot"
SALEBOT_API_BASE = "https://chatter.salebot.pro/api"
AMO_WEBHOOK_SETTINGS = ["add_lead", "update_lead", "status_lead", "responsible_lead"]
MAX_ATTEMPTS = 5
RETRY_DELAYS = (30, 120, 600, 1800, 3600)

DEFAULT_SETTINGS = {
    "enabled": "0",
    "webhook_secret": "",
    "utm_field": "utm_term",
    "callback_message": "callback_amoCRM",
    "identity_mode": "strict",
    "request_timeout": "20",
    "debounce_seconds": "3",
    "send_human_readable": "1",
    "send_custom_fields": "1",
}

_db_path: str | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_worker_task: asyncio.Task | None = None
_worker_wakeup = asyncio.Event()


def setup(ctx) -> None:
    global _db_path, _module_dir, _logger, _worker_task
    _db_path = ctx.db_path
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.amocrm-salebot"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        if _worker_task is None or _worker_task.done():
            _worker_task = loop.create_task(_worker_loop())
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    _worker_task = None


def _must_db() -> str:
    if not _db_path:
        raise RuntimeError("amocrm-salebot module is not initialized")
    return _db_path


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _after(seconds: int | float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(0, float(seconds)))).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _json(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return fallback


def _truthy(value: Any) -> bool:
    return _clean(value, 20).lower() in {"1", "true", "yes", "on", "да"}


def _number(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        return max(minimum, min(maximum, float(value)))
    except Exception:
        return default


def _env() -> dict[str, str]:
    return {
        "amo_base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "amo_token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "salebot_key": (os.environ.get("SALEBOT_API_KEY", "") or os.environ.get("SALEBOT_API_KEY_3", "")).strip(),
        "webhook_secret": os.environ.get("AMO_SALEBOT_WEBHOOK_SECRET", "").strip(),
        "public_base": os.environ.get("AMO_SALEBOT_PUBLIC_BASE", "https://junior.sobakovod.pro/nexus").strip().rstrip("/"),
        "customer_db_path": os.environ.get("AMO_SALEBOT_CUSTOMER_DB_PATH", "").strip(),
        "openrouter_db_path": os.environ.get("AMO_SALEBOT_OPENROUTER_DB_PATH", "").strip(),
    }


def _modules_dir() -> Path:
    if _module_dir is not None:
        return _module_dir.parent
    return Path(__file__).resolve().parents[1] / "modules"


def _customer_db_path() -> Path:
    configured = _env()["customer_db_path"]
    if configured:
        return Path(configured)
    return _modules_dir() / "customer-db" / "data" / "customer-db.db"


def _openrouter_db_path() -> Path:
    configured = _env()["openrouter_db_path"]
    if configured:
        return Path(configured)
    return _modules_dir() / "openrouter" / "data" / "openrouter.db"


async def _require_panel_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                action TEXT NOT NULL DEFAULT '',
                deal_id TEXT NOT NULL DEFAULT '',
                salebot_id TEXT NOT NULL DEFAULT '',
                identity_source TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL DEFAULT '',
                fingerprint TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '{}',
                raw_payload TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_events_queue ON events(status,next_attempt_at,id);
            CREATE INDEX IF NOT EXISTS idx_events_deal ON events(deal_id,id);
            CREATE TABLE IF NOT EXISTS last_deliveries (
                deal_id TEXT PRIMARY KEY,
                salebot_id TEXT NOT NULL DEFAULT '',
                fingerprint TEXT NOT NULL DEFAULT '',
                event_id INTEGER,
                delivered_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS identity_cache (
                identifier TEXT PRIMARY KEY,
                verified INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
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
        await db.execute(
            "UPDATE events SET status='retry',next_attempt_at=? WHERE status='processing'",
            (_now(),),
        )
        await db.commit()
    _log("info", "amocrm-salebot initialized")
    _worker_wakeup.set()


async def _settings_map() -> dict[str, str]:
    data = dict(DEFAULT_SETTINGS)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        for key, value in await cur.fetchall():
            data[str(key)] = str(value or "")
    if _env()["webhook_secret"]:
        data["webhook_secret"] = _env()["webhook_secret"]
    return data


async def _save_settings(payload: dict[str, Any]) -> dict[str, str]:
    allowed = set(DEFAULT_SETTINGS)
    normalized: dict[str, str] = {}
    for key in allowed:
        if key not in payload or (key == "webhook_secret" and _env()["webhook_secret"]):
            continue
        value = _clean(payload.get(key), 500)
        if key in {"enabled", "send_human_readable", "send_custom_fields"}:
            value = "1" if _truthy(value) else "0"
        elif key == "identity_mode":
            value = value if value in {"strict", "verify_api", "trust_utm_term"} else "strict"
        elif key == "request_timeout":
            value = str(int(_number(value, 20, 5, 60)))
        elif key == "debounce_seconds":
            value = str(_number(value, 3, 0, 30)).rstrip("0").rstrip(".") or "0"
        elif key == "utm_field":
            value = value or "utm_term"
        elif key == "callback_message":
            value = value or "callback_amoCRM"
        normalized[key] = value
    async with aiosqlite.connect(_must_db()) as db:
        for key, value in normalized.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    if normalized.get("enabled") == "1":
        _worker_wakeup.set()
    return await _settings_map()


def _timeout(settings: dict[str, str]) -> float:
    return _number(settings.get("request_timeout"), 20, 5, 60)


def _webhook_url(settings: dict[str, str]) -> str:
    url = f"{_env()['public_base']}/{MODULE_ID}/api/webhook"
    secret = _clean(settings.get("webhook_secret"), 300)
    return f"{url}?secret={secret}" if secret else url


def _secret_ok(request: Request, settings: dict[str, str]) -> bool:
    expected = _clean(settings.get("webhook_secret"), 300)
    if not expected:
        return True
    supplied = request.query_params.get("secret") or request.headers.get("X-Nexus-Secret") or ""
    return secrets.compare_digest(expected, _clean(supplied, 300))


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    ctype = request.headers.get("content-type", "").lower()
    if "application/json" in ctype:
        raw = await request.json()
        payload = raw if isinstance(raw, dict) else {}
        return payload, json.dumps(raw, ensure_ascii=False)[:30000]
    form = await request.form()
    flat = {str(key): value for key, value in form.items()}
    if not flat:
        body = await request.body()
        try:
            raw = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            raw = {"raw": body.decode("utf-8", errors="replace")}
        payload = raw if isinstance(raw, dict) else {}
        return payload, json.dumps(raw, ensure_ascii=False)[:30000]
    return flat, json.dumps(flat, ensure_ascii=False)[:30000]


LEAD_KEY_RE = re.compile(r"^leads\[(?P<action>[^\]]+)\]\[(?P<idx>\d+)\]\[(?P<field>[^\]]+)\]$")


def _lead_events(payload: dict[str, Any]) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    grouped: dict[tuple[str, str], dict[str, str]] = {}
    for key, value in payload.items():
        match = LEAD_KEY_RE.match(str(key))
        if not match:
            continue
        action = match.group("action")
        if action not in {"add", "update", "status", "responsible"}:
            continue
        grouped.setdefault((action, match.group("idx")), {})[match.group("field")] = _clean(value, 1000)
    for (action, _), item in grouped.items():
        deal_id = _clean(item.get("id"), 64)
        if deal_id:
            events.append({"action": action, "deal_id": deal_id})
    leads = payload.get("leads")
    if isinstance(leads, dict):
        for action in ("add", "update", "status", "responsible"):
            bucket = leads.get(action)
            if isinstance(bucket, dict):
                items = list(bucket.values())
            elif isinstance(bucket, list):
                items = bucket
            else:
                items = []
            for item in items:
                if isinstance(item, dict) and _clean(item.get("id"), 64):
                    events.append({"action": action, "deal_id": _clean(item.get("id"), 64)})
    unique: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in events:
        key = (item["action"], item["deal_id"])
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


async def _enqueue(action: str, deal_id: str, raw_payload: str, *, force: bool = False) -> int:
    settings = await _settings_map()
    due = _after(_number(settings.get("debounce_seconds"), 3, 0, 30))
    payload = _json(raw_payload, {})
    if not isinstance(payload, dict):
        payload = {"raw": raw_payload}
    if force:
        payload["_manual_force"] = True
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            """
            INSERT INTO events(action,deal_id,status,next_attempt_at,raw_payload)
            VALUES(?,?,'pending',?,?)
            """,
            (action, deal_id, due, json.dumps(payload, ensure_ascii=False)[:30000]),
        )
        await db.commit()
        event_id = int(cur.lastrowid)
    _worker_wakeup.set()
    return event_id


async def _claim_event() -> dict[str, Any] | None:
    async with aiosqlite.connect(_must_db(), timeout=30) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute(
            """
            SELECT * FROM events
            WHERE status IN ('pending','retry') AND (next_attempt_at='' OR next_attempt_at<=?)
            ORDER BY id LIMIT 1
            """,
            (_now(),),
        )
        row = await cur.fetchone()
        if not row:
            await db.commit()
            return None
        await db.execute(
            "UPDATE events SET status='processing',attempts=attempts+1,updated_at=? WHERE id=?",
            (_now(), int(row["id"])),
        )
        await db.commit()
        result = dict(row)
        result["attempts"] = int(result.get("attempts") or 0) + 1
        return result


async def _finish_event(event_id: int, *, status: str, error: str = "", details: Any = None, salebot_id: str = "", identity_source: str = "", fingerprint: str = "", next_attempt_at: str = "") -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            UPDATE events
            SET status=?,error=?,details=?,salebot_id=?,identity_source=?,fingerprint=?,next_attempt_at=?,updated_at=?
            WHERE id=?
            """,
            (
                status,
                _clean(error, 2000),
                json.dumps(details or {}, ensure_ascii=False)[:30000],
                _clean(salebot_id, 80),
                _clean(identity_source, 120),
                _clean(fingerprint, 128),
                next_attempt_at,
                _now(),
                event_id,
            ),
        )
        await db.commit()


async def _retry_or_fail(event: dict[str, Any], error: str, details: Any = None, *, salebot_id: str = "", identity_source: str = "") -> None:
    attempts = int(event.get("attempts") or 1)
    if attempts >= MAX_ATTEMPTS:
        await _finish_event(int(event["id"]), status="failed", error=error, details=details, salebot_id=salebot_id, identity_source=identity_source)
        return
    delay = RETRY_DELAYS[min(attempts - 1, len(RETRY_DELAYS) - 1)]
    await _finish_event(
        int(event["id"]),
        status="retry",
        error=error,
        details=details,
        salebot_id=salebot_id,
        identity_source=identity_source,
        next_attempt_at=_after(delay),
    )


async def _worker_loop() -> None:
    await asyncio.sleep(1)
    while True:
        try:
            event = await _claim_event()
            if event:
                await _deliver_event(event)
                continue
            _worker_wakeup.clear()
            try:
                await asyncio.wait_for(_worker_wakeup.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "amocrm-salebot worker error: %s", exc)
            await asyncio.sleep(3)


async def _amo_request(method: str, path: str, settings: dict[str, str], *, payload: Any = None) -> tuple[Any, str]:
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            response = await client.request(
                method,
                env["amo_base_url"] + path,
                headers={"Authorization": f"Bearer {env['amo_token']}", "Content-Type": "application/json"},
                json=payload,
            )
        if response.status_code >= 400:
            return None, f"amoCRM HTTP {response.status_code}: {response.text[:500]}"
        return response.json() if response.text else {}, ""
    except Exception as exc:
        return None, f"amoCRM transport: {type(exc).__name__}: {exc}"


async def _load_deal(deal_id: str, settings: dict[str, str]) -> tuple[dict[str, Any], str]:
    body, error = await _amo_request("GET", f"/api/v4/leads/{deal_id}?with=contacts", settings)
    return body if isinstance(body, dict) else {}, error


def _query_value(text: str, target: str) -> str:
    current = html.unescape(str(text or ""))
    for _ in range(3):
        match = re.search(rf"(?:^|[?&#;\s]){re.escape(target)}=([^&#;\s]+)", current, re.IGNORECASE)
        if match:
            return _clean(unquote_plus(match.group(1)), 200)
        decoded = unquote_plus(current)
        if decoded == current:
            break
        current = decoded
    return ""


def _field_value(deal: dict[str, Any], field_name: str) -> str:
    target = _clean(field_name, 200).casefold()
    for key, value in deal.items():
        if str(key).casefold() == target and value not in (None, ""):
            return _clean(value, 300)
    fields = deal.get("custom_fields_values") or []
    for field in fields if isinstance(fields, list) else []:
        if not isinstance(field, dict):
            continue
        candidates = [field.get("field_code"), field.get("field_name"), field.get("name")]
        if not any(_clean(item, 200).casefold() == target for item in candidates):
            continue
        values = field.get("values") or []
        if isinstance(values, list) and values:
            first = values[0]
            return _clean(first.get("value") if isinstance(first, dict) else first, 300)
    for field in fields if isinstance(fields, list) else []:
        if isinstance(field, dict):
            for value in field.get("values") or []:
                raw = value.get("value") if isinstance(value, dict) else value
                found = _query_value(str(raw or ""), target)
                if found:
                    return found
    return ""


def _numeric_client_id(value: Any) -> str:
    text = _clean(value, 80)
    return text if re.fullmatch(r"\d{4,20}", text) else ""


def _contains_key_value(value: Any, target: str, keys: set[str]) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).casefold() in keys and _clean(item, 80) == target:
                return True
            if _contains_key_value(item, target, keys):
                return True
    elif isinstance(value, list):
        return any(_contains_key_value(item, target, keys) for item in value)
    return False


async def _identity_from_openrouter(identifier: str) -> tuple[bool, str, dict[str, Any]]:
    path = _openrouter_db_path()
    if not path.exists():
        return False, "", {"db": str(path), "reason": "missing"}
    try:
        async with aiosqlite.connect(path, timeout=10) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT source,conversation_id,created_at FROM messages WHERE platform_id=? AND source='salebot' ORDER BY id DESC LIMIT 1",
                (identifier,),
            )
            row = await cur.fetchone()
            if row:
                return True, "openrouter_messages", {"conversation_id": row["conversation_id"], "created_at": row["created_at"]}
            cur = await db.execute(
                "SELECT id,payload_json,created_at FROM outbound_jobs WHERE source='salebot' AND payload_json LIKE ? ORDER BY id DESC LIMIT 100",
                (f"%{identifier}%",),
            )
            for row in await cur.fetchall():
                payload = _json(row["payload_json"], {})
                if _contains_key_value(payload, identifier, {"salebot_id", "client_id"}):
                    return True, "openrouter_jobs", {"job_id": row["id"], "created_at": row["created_at"]}
    except Exception as exc:
        return False, "", {"db": str(path), "error": f"{type(exc).__name__}: {exc}"}
    return False, "", {"db": str(path), "reason": "not_found"}


async def _identity_from_customer_db(identifier: str) -> tuple[bool, str, dict[str, Any]]:
    path = _customer_db_path()
    if not path.exists():
        return False, "", {"db": str(path), "reason": "missing"}
    try:
        async with aiosqlite.connect(path, timeout=10) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'cdb_%'")
            tables = [str(row[0]) for row in await cur.fetchall()]
            for table in tables:
                if table == "cdb_amo_deals" or not re.fullmatch(r"cdb_[A-Za-z0-9_]+", table):
                    continue
                cur = await db.execute(
                    f"SELECT id,platform_id,custom_fields FROM {table} WHERE custom_fields LIKE ? LIMIT 100",
                    (f"%{identifier}%",),
                )
                for row in await cur.fetchall():
                    fields = _json(row["custom_fields"], {})
                    if _contains_key_value(fields, identifier, {"salebot_id", "client.salebot_id"}):
                        return True, "customer_db", {"table": table, "record_id": row["id"], "platform_id": row["platform_id"]}
                if "salebot" in table:
                    cur = await db.execute(f"SELECT id,platform_id FROM {table} WHERE platform_id=? LIMIT 1", (identifier,))
                    row = await cur.fetchone()
                    if row:
                        return True, "customer_db", {"table": table, "record_id": row["id"], "platform_id": row["platform_id"]}
    except Exception as exc:
        return False, "", {"db": str(path), "error": f"{type(exc).__name__}: {exc}"}
    return False, "", {"db": str(path), "reason": "not_found"}


async def _identity_from_salebot_api(identifier: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    api_key = _env()["salebot_key"]
    if not api_key:
        return False, "", {"reason": "SALEBOT_API_KEY_missing"}
    url = f"{SALEBOT_API_BASE}/{api_key}/get_variables"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            response = await client.get(url, params={"client_id": identifier})
        raw = response.text[:2000]
        try:
            body = response.json()
        except Exception:
            body = raw
        details = {"http_status": response.status_code, "response_type": type(body).__name__}
        if response.status_code >= 400:
            return False, "", details
        if isinstance(body, dict) and body.get("status") != "error" and not body.get("error"):
            return True, "salebot_api", details
        return False, "", details
    except Exception as exc:
        return False, "", {"error": f"{type(exc).__name__}: {exc}"}


async def _resolve_identity(identifier: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    clean_id = _numeric_client_id(identifier)
    if not clean_id:
        return False, "", {"reason": "utm_term_is_not_numeric_salebot_id"}
    if settings.get("identity_mode") == "trust_utm_term":
        return True, "trusted_utm_term", {"mode": "trust_utm_term"}
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT verified,source,details,updated_at FROM identity_cache WHERE identifier=? AND verified=1 AND updated_at>=datetime('now','-30 days')",
            (clean_id,),
        )
        cached = await cur.fetchone()
    if cached:
        return True, str(cached["source"]), {"cache": True, "verified_at": cached["updated_at"], "details": _json(cached["details"], {})}
    checks = []
    for resolver in (_identity_from_openrouter, _identity_from_customer_db):
        ok, source, details = await resolver(clean_id)
        checks.append({"resolver": resolver.__name__, "ok": ok, "source": source, "details": details})
        if ok:
            await _cache_identity(clean_id, source, details)
            return True, source, {"checks": checks}
    if settings.get("identity_mode") == "verify_api":
        ok, source, details = await _identity_from_salebot_api(clean_id, settings)
        checks.append({"resolver": "salebot_api", "ok": ok, "source": source, "details": details})
        if ok:
            await _cache_identity(clean_id, source, details)
            return True, source, {"checks": checks}
    return False, "", {"checks": checks, "reason": "salebot_identity_not_confirmed"}


async def _cache_identity(identifier: str, source: str, details: Any) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO identity_cache(identifier,verified,source,details,updated_at) VALUES(?,1,?,?,?)
            ON CONFLICT(identifier) DO UPDATE SET verified=1,source=excluded.source,details=excluded.details,updated_at=excluded.updated_at
            """,
            (identifier, source, json.dumps(details or {}, ensure_ascii=False)[:10000], _now()),
        )
        await db.commit()


def _value_text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("value", "enum", "name"):
            if key in value:
                return _value_text(value[key])
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, list):
        return ", ".join(filter(None, (_value_text(item) for item in value)))
    if value is None:
        return ""
    return str(value)


def _variable_key(field: dict[str, Any]) -> str:
    source = _clean(field.get("field_code") or field.get("field_name") or field.get("name"), 200).lower()
    source = re.sub(r"[^0-9a-zа-яё_]+", "_", source, flags=re.IGNORECASE).strip("_")
    return f"amo_{source}" if source else ""


def _build_variables(deal: dict[str, Any], settings: dict[str, str]) -> dict[str, str]:
    variables: dict[str, str] = {}
    direct = {
        "amo_deal_id": deal.get("id"),
        "amo_deal_name": deal.get("name"),
        "amo_price": deal.get("price"),
        "amo_responsible": deal.get("responsible_user_id"),
        "amo_pipeline": deal.get("pipeline_id"),
        "amo_status": deal.get("status_id"),
        "amo_created_at": deal.get("created_at"),
        "amo_updated_at": deal.get("updated_at"),
        "amo_closed_at": deal.get("closed_at"),
    }
    company = ((deal.get("_embedded") or {}).get("companies") or []) if isinstance(deal.get("_embedded"), dict) else []
    if company and isinstance(company[0], dict):
        direct["amo_company_id"] = company[0].get("id")
    for key, value in direct.items():
        if value not in (None, ""):
            variables[key] = _clean(_value_text(value), 50000)
    contacts = ((deal.get("_embedded") or {}).get("contacts") or []) if isinstance(deal.get("_embedded"), dict) else []
    contact_ids = [_clean(item.get("id"), 64) for item in contacts if isinstance(item, dict) and item.get("id")]
    main_contact = next((_clean(item.get("id"), 64) for item in contacts if isinstance(item, dict) and item.get("is_main") and item.get("id")), "")
    if contact_ids:
        variables["amo_contacts"] = ",".join(contact_ids)
    if main_contact:
        variables["amo_main_contact"] = main_contact
    fields = deal.get("custom_fields_values") or []
    if _truthy(settings.get("send_custom_fields")):
        for field in fields if isinstance(fields, list) else []:
            if not isinstance(field, dict):
                continue
            key = _variable_key(field)
            value = _value_text(field.get("values") or [])
            if key and value:
                variables[key] = _clean(value, 50000)
    if _truthy(settings.get("send_human_readable")):
        lines = [
            f"СДЕЛКА #{_clean(deal.get('id'), 64)}",
            f"Название: {_clean(deal.get('name'), 1000)}",
            f"Сумма: {_clean(deal.get('price'), 100)}",
            f"Воронка ID: {_clean(deal.get('pipeline_id'), 64)}",
            f"Статус ID: {_clean(deal.get('status_id'), 64)}",
            f"Ответственный ID: {_clean(deal.get('responsible_user_id'), 64)}",
            f"Создана: {_clean(deal.get('created_at'), 64)}",
            f"Обновлена: {_clean(deal.get('updated_at'), 64)}",
        ]
        for field in fields if isinstance(fields, list) else []:
            if isinstance(field, dict):
                name = _clean(field.get("field_name") or field.get("field_code"), 500)
                value = _value_text(field.get("values") or [])
                if name and value:
                    lines.append(f"{name}: {value}")
        variables["amo_deal_info"] = _clean("\n".join(lines), 100000)
    return variables


def _fingerprint(salebot_id: str, variables: dict[str, str], callback_message: str) -> str:
    raw = json.dumps({"salebot_id": salebot_id, "variables": variables, "callback": callback_message}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _last_fingerprint(deal_id: str) -> str:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT fingerprint FROM last_deliveries WHERE deal_id=?", (deal_id,))
        row = await cur.fetchone()
    return str(row[0] or "") if row else ""


async def _record_delivery(deal_id: str, salebot_id: str, fingerprint: str, event_id: int) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO last_deliveries(deal_id,salebot_id,fingerprint,event_id,delivered_at) VALUES(?,?,?,?,?)
            ON CONFLICT(deal_id) DO UPDATE SET salebot_id=excluded.salebot_id,fingerprint=excluded.fingerprint,event_id=excluded.event_id,delivered_at=excluded.delivered_at
            """,
            (deal_id, salebot_id, fingerprint, event_id, _now()),
        )
        await db.commit()


async def _salebot_post(action: str, payload: dict[str, Any], settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    key = _env()["salebot_key"]
    if not key:
        return False, "SALEBOT_API_KEY или SALEBOT_API_KEY_3 не задан", {}
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            response = await client.post(f"{SALEBOT_API_BASE}/{key}/{action}", json=payload)
        raw = response.text[:3000]
        try:
            body: Any = response.json()
        except Exception:
            body = raw
        details = {"http_status": response.status_code, "response": body}
        if response.status_code >= 400:
            return False, f"SaleBot HTTP {response.status_code}", details
        if isinstance(body, dict) and (body.get("success") is False or body.get("status") == "error" or body.get("error")):
            return False, _clean(body.get("error") or body.get("message") or body, 1000), details
        if action == "save_variables" and isinstance(body, str) and body.strip().lower() not in {"ok", "true", "success"}:
            return False, f"Неожиданный ответ save_variables: {body[:500]}", details
        return True, "", details
    except Exception as exc:
        return False, f"SaleBot transport: {type(exc).__name__}: {exc}", {"exception": str(exc)}


async def _deliver_event(event: dict[str, Any]) -> None:
    event_id = int(event["id"])
    deal_id = _clean(event.get("deal_id"), 64)
    settings = await _settings_map()
    if not _truthy(settings.get("enabled")):
        await _finish_event(event_id, status="ignored", error="Модуль выключен")
        return
    deal, error = await _load_deal(deal_id, settings)
    if error or not deal:
        await _retry_or_fail(event, error or "Сделка amoCRM не найдена", {"deal_id": deal_id})
        return
    identifier = _field_value(deal, settings.get("utm_field") or "utm_term")
    salebot_id = _numeric_client_id(identifier)
    if not salebot_id:
        await _finish_event(event_id, status="ignored", error=f"В поле {settings.get('utm_field')} нет числового SaleBot ID", details={"value": identifier})
        return
    verified, identity_source, identity_details = await _resolve_identity(salebot_id, settings)
    if not verified:
        await _finish_event(
            event_id,
            status="ignored",
            error="utm_term не подтверждён как salebot_id",
            details={"identity": identity_details},
            salebot_id=salebot_id,
        )
        return
    variables = _build_variables(deal, settings)
    callback_message = _clean(settings.get("callback_message"), 300) or "callback_amoCRM"
    fingerprint = _fingerprint(salebot_id, variables, callback_message)
    raw_payload = _json(event.get("raw_payload"), {})
    force = bool(isinstance(raw_payload, dict) and raw_payload.get("_manual_force"))
    if not force and fingerprint == await _last_fingerprint(deal_id):
        await _finish_event(
            event_id,
            status="deduped",
            error="Состояние сделки уже отправлено",
            details={"variable_count": len(variables)},
            salebot_id=salebot_id,
            identity_source=identity_source,
            fingerprint=fingerprint,
        )
        return
    save_ok, save_error, save_details = await _salebot_post(
        "save_variables",
        {"client_id": salebot_id, "variables": variables},
        settings,
    )
    if not save_ok:
        await _retry_or_fail(
            event,
            f"save_variables: {save_error}",
            {"identity": identity_details, "save_variables": save_details, "variable_count": len(variables)},
            salebot_id=salebot_id,
            identity_source=identity_source,
        )
        return
    callback_ok, callback_error, callback_details = await _salebot_post(
        "callback",
        {"client_id": salebot_id, "message": callback_message},
        settings,
    )
    if not callback_ok:
        await _retry_or_fail(
            event,
            f"callback: {callback_error}",
            {"identity": identity_details, "save_variables": save_details, "callback": callback_details},
            salebot_id=salebot_id,
            identity_source=identity_source,
        )
        return
    await _record_delivery(deal_id, salebot_id, fingerprint, event_id)
    await _finish_event(
        event_id,
        status="success",
        details={
            "identity": identity_details,
            "variable_count": len(variables),
            "variables": sorted(variables),
            "callback_message": callback_message,
            "save_variables": save_details,
            "callback": callback_details,
        },
        salebot_id=salebot_id,
        identity_source=identity_source,
        fingerprint=fingerprint,
    )
    _log("info", "amoCRM -> SaleBot delivered deal_id=%s salebot_id=%s event_id=%s variables=%s", deal_id, salebot_id, event_id, len(variables))


@router.get("/health")
async def health() -> dict[str, Any]:
    settings = await _settings_map()
    env = _env()
    return {
        "ok": True,
        "module": MODULE_ID,
        "enabled": _truthy(settings.get("enabled")),
        "ready": bool(env["amo_base_url"] and env["amo_token"] and env["salebot_key"]),
    }


@router.get("/env-status")
async def env_status(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    env = _env()
    return {
        "ok": True,
        "ready": bool(env["amo_base_url"] and env["amo_token"] and env["salebot_key"]),
        "AMO_BASE_URL": bool(env["amo_base_url"]),
        "AMO_ACCESS_TOKEN": bool(env["amo_token"]),
        "SALEBOT_API_KEY": bool(env["salebot_key"]),
        "customer_db": str(_customer_db_path()),
        "customer_db_ready": _customer_db_path().exists(),
        "openrouter_db": str(_openrouter_db_path()),
        "openrouter_db_ready": _openrouter_db_path().exists(),
    }


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    settings = await _settings_map()
    return {"ok": True, "settings": settings, "webhook_url": _webhook_url(settings)}


@router.post("/settings")
async def save_settings(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    payload = await request.json()
    settings = await _save_settings(payload if isinstance(payload, dict) else {})
    return {"ok": True, "settings": settings, "webhook_url": _webhook_url(settings)}


@router.post("/webhook/register")
async def register_webhook(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    settings = await _settings_map()
    destination = _webhook_url(settings)
    current, error = await _amo_request("GET", "/api/v4/webhooks", settings)
    if error:
        raise HTTPException(502, error)
    hooks = ((current or {}).get("_embedded") or {}).get("webhooks") or []
    for hook in hooks if isinstance(hooks, list) else []:
        if isinstance(hook, dict) and _clean(hook.get("destination"), 2000) == destination and not hook.get("disabled"):
            return {"ok": True, "already_registered": True, "destination": destination, "webhook": hook}
    body, error = await _amo_request(
        "POST",
        "/api/v4/webhooks",
        settings,
        payload={"destination": destination, "settings": AMO_WEBHOOK_SETTINGS, "sort": 5},
    )
    if error:
        raise HTTPException(502, error)
    return {"ok": True, "registered": True, "destination": destination, "response": body}


@router.get("/events")
async def events(request: Request, limit: int = 100) -> dict[str, Any]:
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit)))
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
        rows = [dict(row) for row in await cur.fetchall()]
        cur = await db.execute("SELECT status,COUNT(*) count FROM events GROUP BY status")
        counts = {str(row[0]): int(row[1]) for row in await cur.fetchall()}
    for row in rows:
        row["details"] = _json(row.get("details"), {})
        row.pop("raw_payload", None)
    return {"ok": True, "items": rows, "counts": counts}


@router.post("/events/{event_id}/retry")
async def retry_event(event_id: int, request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("UPDATE events SET status='retry',attempts=0,error='',next_attempt_at=?,updated_at=? WHERE id=?", (_now(), _now(), event_id))
        await db.commit()
        if cur.rowcount < 1:
            raise HTTPException(404, "event not found")
    _worker_wakeup.set()
    return {"ok": True, "event_id": event_id}


@router.get("/preview/{deal_id}")
async def preview(deal_id: str, request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    settings = await _settings_map()
    deal, error = await _load_deal(_clean(deal_id, 64), settings)
    if error or not deal:
        raise HTTPException(502, error or "deal not found")
    identifier = _field_value(deal, settings.get("utm_field") or "utm_term")
    verified, source, identity = await _resolve_identity(identifier, settings)
    variables = _build_variables(deal, settings)
    return {
        "ok": True,
        "deal_id": _clean(deal.get("id"), 64),
        "utm_value": identifier,
        "identity_verified": verified,
        "identity_source": source,
        "identity": identity,
        "callback_message": settings.get("callback_message"),
        "variables": variables,
    }


@router.post("/send/{deal_id}")
async def send_deal(deal_id: str, request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    event_id = await _enqueue("manual", _clean(deal_id, 64), json.dumps({"manual": True}), force=True)
    return {"ok": True, "queued": True, "event_id": event_id}


@router.post("/webhook")
async def webhook(request: Request) -> JSONResponse:
    try:
        settings = await _settings_map()
        if not _secret_ok(request, settings):
            _log("warning", "amoCRM SaleBot webhook rejected: invalid secret")
            return JSONResponse(status_code=200, content={"ok": False, "error": "invalid secret"})
        payload, raw_payload = await _read_payload(request)
        lead_events = _lead_events(payload)
        event_ids = []
        for item in lead_events:
            event_ids.append(await _enqueue(item["action"], item["deal_id"], raw_payload))
        if not lead_events:
            _log("warning", "amoCRM SaleBot webhook ignored: no lead events")
        return JSONResponse(status_code=200, content={"ok": True, "accepted": len(event_ids), "event_ids": event_ids})
    except Exception as exc:
        _log("error", "amoCRM SaleBot webhook error: %s", exc)
        return JSONResponse(status_code=200, content={"ok": False, "error": "internal error"})
