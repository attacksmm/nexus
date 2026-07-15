from __future__ import annotations

import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "admin-handoff"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.199"
SENLER_API_BASE = "https://senler.ru/api"
SENLER_API_VERSION = "2"
SALEBOT_API_BASE = "https://chatter.salebot.pro/api"
DEFAULT_SENLER_SUBSCRIPTION_ID = "3748755"
DEFAULT_SALEBOT_LIST_ID = "2427562"
DEFAULT_VK_GROUP_ID = "225075265"
DEFAULT_POLL_SECONDS = 120
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_RETENTION_DAYS = 7
DEFAULT_SOURCE_GRACE_SECONDS = 600
DEFAULT_SALEBOT_LOOKUP_FALLBACK = True
MAX_PROCESS_LIMIT = 50
ADMIN_NAME_CACHE_SECONDS = 3600

_db_path: Path | None = None
_module_dir: Path | None = None
_logger: Any = None
_worker_task: asyncio.Task | None = None
_write_lock = asyncio.Lock()
_admin_name_lock = asyncio.Lock()
_admin_name_cache: dict[str, tuple[str, float]] = {}


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = Path(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", None)
    return _setup_async()


async def _setup_async() -> None:
    await _init_db()
    _start_worker()


async def shutdown() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    _worker_task = None


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("admin-handoff module is not initialized")
    return _db_path


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
            CREATE TABLE IF NOT EXISTS actions (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                scanner_event_id   INTEGER NOT NULL UNIQUE,
                scanner_vk_message TEXT NOT NULL DEFAULT '',
                vk_user_id         TEXT NOT NULL DEFAULT '',
                source             TEXT NOT NULL DEFAULT '',
                target_type        TEXT NOT NULL DEFAULT '',
                target_id          TEXT NOT NULL DEFAULT '',
                external_client_id TEXT NOT NULL DEFAULT '',
                status             TEXT NOT NULL DEFAULT 'pending',
                error              TEXT NOT NULL DEFAULT '',
                details_json       TEXT NOT NULL DEFAULT '{}',
                attempts           INTEGER NOT NULL DEFAULT 0,
                admin_message_at   TEXT NOT NULL DEFAULT '',
                created_at         TEXT NOT NULL,
                updated_at         TEXT NOT NULL,
                processed_at       TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS memberships (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                member_key            TEXT NOT NULL UNIQUE,
                vk_user_id            TEXT NOT NULL DEFAULT '',
                source                TEXT NOT NULL DEFAULT '',
                target_type           TEXT NOT NULL DEFAULT '',
                target_id             TEXT NOT NULL DEFAULT '',
                external_client_id    TEXT NOT NULL DEFAULT '',
                last_scanner_event_id INTEGER NOT NULL DEFAULT 0,
                last_admin_message_at TEXT NOT NULL DEFAULT '',
                expires_at            TEXT NOT NULL DEFAULT '',
                status                TEXT NOT NULL DEFAULT 'active',
                remove_error          TEXT NOT NULL DEFAULT '',
                remove_details_json   TEXT NOT NULL DEFAULT '{}',
                remove_attempts       INTEGER NOT NULL DEFAULT 0,
                created_at            TEXT NOT NULL,
                updated_at            TEXT NOT NULL,
                removed_at            TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_actions_status_updated ON actions(status, updated_at);
            CREATE INDEX IF NOT EXISTS idx_actions_vk_user ON actions(vk_user_id, updated_at);
            CREATE INDEX IF NOT EXISTS idx_memberships_status_expiry ON memberships(status, expires_at);
            """
        )
        await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('started_at',?)", (_now(),))
        await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('retention_days',?)", (str(_retention_days()),))
        cur = await db.execute("SELECT value FROM settings WHERE key='retention_days'")
        row = await cur.fetchone()
        if row:
            os.environ["ADMIN_HANDOFF_RETENTION_DAYS"] = str(row["value"] or DEFAULT_RETENTION_DAYS)
        await _seed_memberships_from_actions(db)
        await db.commit()
    _log("info", "admin-handoff DB initialized")


async def _require_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user:
        raise HTTPException(401, "unauthorized")
    if not can_access_module(user, MODULE_ID):
        raise HTTPException(403, "Недостаточно прав")
    return user


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: Any) -> datetime | None:
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


def _epoch(value: Any) -> float:
    parsed = _parse_utc(value)
    return parsed.timestamp() if parsed else 0.0


def _utc_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(max(0, value), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _numeric(value: Any, limit: int = 32) -> str:
    text = _clean(value, limit)
    return text if re.fullmatch(r"\d{1,%d}" % limit, text) else ""


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


def _modules_dir() -> Path:
    if _module_dir is not None:
        return _module_dir.parent
    return Path(__file__).resolve().parents[1] / "modules"


def _scanner_db_path() -> Path:
    configured = _clean(os.environ.get("ADMIN_HANDOFF_SCANNER_DB_PATH"), 2000)
    return Path(configured) if configured else _modules_dir() / "scanner" / "data" / "scanner.db"


def _openrouter_db_path() -> Path:
    configured = _clean(os.environ.get("ADMIN_HANDOFF_OPENROUTER_DB_PATH"), 2000)
    return Path(configured) if configured else _modules_dir() / "openrouter" / "data" / "openrouter.db"


def _poll_seconds() -> int:
    return _env_int("ADMIN_HANDOFF_POLL_SECONDS", DEFAULT_POLL_SECONDS, 30, 3600)


def _lookback_days() -> int:
    return _env_int("ADMIN_HANDOFF_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS, 1, 90)


def _source_grace_seconds() -> int:
    return _env_int("ADMIN_HANDOFF_SOURCE_GRACE_SECONDS", DEFAULT_SOURCE_GRACE_SECONDS, 0, 3600)


def _retention_days() -> int:
    return _env_int("ADMIN_HANDOFF_RETENTION_DAYS", DEFAULT_RETENTION_DAYS, 1, 90)


def _coerce_retention_days(value: Any) -> int:
    try:
        days = int(str(value or DEFAULT_RETENTION_DAYS).strip())
    except Exception:
        days = DEFAULT_RETENTION_DAYS
    return max(1, min(90, days))


def _enabled() -> bool:
    return _env_bool("ADMIN_HANDOFF_ENABLED", True)


def _salebot_lookup_fallback_enabled() -> bool:
    return _env_bool("ADMIN_HANDOFF_SALEBOT_LOOKUP_FALLBACK", DEFAULT_SALEBOT_LOOKUP_FALLBACK)


def _env() -> dict[str, str]:
    return {
        "vk_token": _clean(os.environ.get("VK_GROUP_TOKEN") or os.environ.get("VK_USER_TOKEN"), 1000),
        "senler_token": _clean(os.environ.get("SENLER_ACCESS_TOKEN"), 1000),
        "senler_group_id": _clean(os.environ.get("SENLER_GROUP_ID"), 80),
        "senler_subscription_id": _numeric(os.environ.get("ADMIN_HANDOFF_SENLER_SUBSCRIPTION_ID") or DEFAULT_SENLER_SUBSCRIPTION_ID),
        "salebot_key": _clean(os.environ.get("SALEBOT_API_KEY") or os.environ.get("SALEBOT_API_KEY_3"), 1000),
        "salebot_list_id": _numeric(os.environ.get("ADMIN_HANDOFF_SALEBOT_LIST_ID") or DEFAULT_SALEBOT_LIST_ID),
        "salebot_group_id": _numeric(os.environ.get("SALEBOT_GROUP_ID"), 32),
    }


def _vk_group_id() -> str:
    return _numeric(os.environ.get("VK_GROUP_ID") or os.environ.get("SENLER_GROUP_ID") or DEFAULT_VK_GROUP_ID, 32)


def _vk_dialog_url(vk_user_id: Any) -> str:
    vk_id = _numeric(vk_user_id, 32)
    group_id = _vk_group_id()
    if not vk_id or not group_id:
        return ""
    return f"https://vk.ru/gim{group_id}?sel={vk_id}"


async def _setting(key: str) -> str:
    async with _connect() as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
    return str(row["value"] or "") if row else ""


async def _settings_payload() -> dict[str, Any]:
    admin_filter_enabled = (await _setting("admin_filter_enabled")).lower() in {"1", "true", "yes", "on", "да"}
    return {
        "retention_days": _retention_days(),
        "retention_min": 1,
        "retention_max": 90,
        "poll_seconds": _poll_seconds(),
        "lookback_days": _lookback_days(),
        "source_grace_seconds": _source_grace_seconds(),
        "process_existing": _env_bool("ADMIN_HANDOFF_PROCESS_EXISTING", False),
        "salebot_lookup_fallback": _salebot_lookup_fallback_enabled(),
        "vk_group_id": _vk_group_id(),
        "admin_filter_enabled": admin_filter_enabled,
        "allowed_admin_ids": await _allowed_admin_ids(),
    }


async def _save_settings(data: dict[str, Any]) -> dict[str, Any]:
    days = _coerce_retention_days(data.get("retention_days"))
    admin_filter_enabled = bool(data.get("admin_filter_enabled"))
    allowed_admin_ids = [
        admin_id
        for admin_id in (_numeric(item, 32) for item in (data.get("allowed_admin_ids") or []))
        if admin_id
    ]
    clean_days = _coerce_retention_days(days)
    os.environ["ADMIN_HANDOFF_RETENTION_DAYS"] = str(clean_days)
    now = _now()
    recomputed = 0
    async with _write_lock:
        async with _connect() as db:
            await db.execute(
                """
                INSERT INTO settings(key,value)
                VALUES('retention_days',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (str(clean_days),),
            )
            await db.execute(
                """
                INSERT INTO settings(key,value)
                VALUES('admin_filter_enabled',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                ("1" if admin_filter_enabled else "0",),
            )
            await db.execute(
                """
                INSERT INTO settings(key,value)
                VALUES('allowed_admin_ids',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (_json_dumps(sorted(set(allowed_admin_ids), key=int)),),
            )
            cur = await db.execute(
                """
                SELECT id,last_admin_message_at
                FROM memberships
                WHERE status='active' AND last_admin_message_at!=''
                """
            )
            rows = await cur.fetchall()
            for row in rows:
                await db.execute(
                    "UPDATE memberships SET expires_at=?,updated_at=? WHERE id=?",
                    (_expiry_from_admin_at(str(row["last_admin_message_at"] or "")), now, int(row["id"])),
                )
                recomputed += 1
            await db.commit()
        expiry = await _expire_memberships(MAX_PROCESS_LIMIT)
    return {
        "ok": True,
        "settings": await _settings_payload(),
        "recomputed": recomputed,
        "expired": expiry["expired"],
        "expire_failed": expiry["failed"],
    }


async def _allowed_admin_ids() -> list[str]:
    raw = await _setting("allowed_admin_ids")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    result = sorted({admin_id for admin_id in (_numeric(item, 32) for item in parsed) if admin_id}, key=int)
    return result


def _admin_author_id(event: dict[str, Any]) -> str:
    after = _clean(event.get("admin_after_author_id"), 80)
    before = _clean(event.get("admin_before_author_id"), 80)
    if after and after.isdigit():
        return after
    if before and before.isdigit():
        return before
    return ""


async def _admin_allowed(event: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    admin_id = _admin_author_id(event)
    enabled = (await _setting("admin_filter_enabled")).lower() in {"1", "true", "yes", "on", "да"}
    allowed_ids = await _allowed_admin_ids()
    names = await _admin_names([admin_id]) if enabled and admin_id else {}
    payload = {
        "admin_author_id": admin_id,
        "admin_name": names.get(admin_id) or admin_id,
        "admin_filter_enabled": enabled,
        "allowed_admin_ids": allowed_ids,
    }
    if not enabled:
        return True, payload
    return bool(admin_id and admin_id in set(allowed_ids)), payload


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_loads_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def _vk_api_call(method: str, params: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
    token = _env()["vk_token"]
    if not token:
        raise RuntimeError("VK_GROUP_TOKEN or VK_USER_TOKEN is not configured")
    payload = {key: value for key, value in params.items() if value is not None and value != ""}
    payload["access_token"] = token
    payload.setdefault("v", VK_API_VERSION)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{VK_API_BASE}/{method}", data=payload)
        body = resp.json()
    except Exception as exc:
        raise RuntimeError(f"VK API {method} transport error: {type(exc).__name__}: {exc}") from exc
    if isinstance(body, dict) and body.get("error"):
        err = body["error"]
        raise RuntimeError(f"VK API {method} error {err.get('error_code')}: {err.get('error_msg') or err}")
    response = body.get("response") if isinstance(body, dict) else None
    if not isinstance(response, (dict, list)):
        raise RuntimeError(f"VK API {method} returned unexpected response")
    return {"response": response}


async def _admin_names(admin_ids: list[str]) -> dict[str, str]:
    ids = sorted({admin_id for admin_id in (_numeric(item, 32) for item in admin_ids) if admin_id}, key=int)
    if not ids:
        return {}
    now = time.monotonic()
    result = {
        admin_id: cached[0]
        for admin_id in ids
        if (cached := _admin_name_cache.get(admin_id)) and now - cached[1] < ADMIN_NAME_CACHE_SECONDS
    }
    missing = [admin_id for admin_id in ids if admin_id not in result]
    if missing:
        async with _admin_name_lock:
            now = time.monotonic()
            for admin_id in list(missing):
                cached = _admin_name_cache.get(admin_id)
                if cached and now - cached[1] < ADMIN_NAME_CACHE_SECONDS:
                    result[admin_id] = cached[0]
                    missing.remove(admin_id)
            if missing:
                fetched = {admin_id: admin_id for admin_id in missing}
                try:
                    data = await _vk_api_call("users.get", {"user_ids": ",".join(missing)})
                    response = data.get("response")
                    users = response if isinstance(response, list) else []
                    for item in users:
                        if not isinstance(item, dict):
                            continue
                        admin_id = _numeric(item.get("id"), 32)
                        name = " ".join(
                            part
                            for part in [_clean(item.get("first_name"), 80), _clean(item.get("last_name"), 80)]
                            if part
                        ).strip()
                        if admin_id in fetched and name:
                            fetched[admin_id] = name
                except Exception as exc:
                    _log("warning", "admin-handoff VK users.get failed: %s", exc)
                cached_at = time.monotonic()
                for admin_id, name in fetched.items():
                    _admin_name_cache[admin_id] = (name, cached_at)
                    result[admin_id] = name
    return {admin_id: result.get(admin_id, admin_id) for admin_id in ids}


async def _admins_payload(days: int = 90) -> dict[str, Any]:
    scanner_db = _scanner_db_path()
    clean_days = max(1, min(365, int(days or 90)))
    cutoff = _utc_from_epoch(time.time() - clean_days * 86400)
    if not scanner_db.exists():
        return {"items": [], "allowed_admin_ids": await _allowed_admin_ids(), "admin_filter_enabled": False, "scanner_db_ready": False}
    seen: dict[str, dict[str, Any]] = {}
    async with _connect(scanner_db) as db:
        for column, date_column in (("admin_before_author_id", "admin_before_at"), ("admin_after_author_id", "admin_after_at")):
            cur = await db.execute(
                f"""
                SELECT {column} AS admin_id, MAX({date_column}) AS last_seen, COUNT(*) AS total
                FROM scan_events
                WHERE {column}!='' AND {column} NOT LIKE '-%' AND {date_column}>=?
                GROUP BY {column}
                """,
                (cutoff,),
            )
            for row in await cur.fetchall():
                admin_id = _numeric(row["admin_id"], 32)
                if not admin_id:
                    continue
                item = seen.setdefault(admin_id, {"id": admin_id, "count": 0, "last_seen": ""})
                item["count"] += int(row["total"] or 0)
                last_seen = str(row["last_seen"] or "")
                if last_seen > item["last_seen"]:
                    item["last_seen"] = last_seen
    names = await _admin_names(list(seen))
    allowed_ids = await _allowed_admin_ids()
    filter_enabled = (await _setting("admin_filter_enabled")).lower() in {"1", "true", "yes", "on", "да"}
    items = []
    for admin_id, item in seen.items():
        items.append(
            {
                **item,
                "name": names.get(admin_id) or admin_id,
                "enabled": (not filter_enabled) or admin_id in set(allowed_ids),
            }
        )
    items.sort(key=lambda item: (item.get("name") or item["id"]).lower())
    return {
        "items": items,
        "allowed_admin_ids": allowed_ids,
        "admin_filter_enabled": filter_enabled,
        "scanner_db_ready": True,
        "days": clean_days,
    }


def _looks_like_salebot_client_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{5,20}", str(value or "").strip()))


def _expiry_from_admin_at(admin_at: str) -> str:
    parsed = _parse_utc(admin_at) or datetime.now(timezone.utc)
    return (parsed + timedelta(days=_retention_days())).strftime("%Y-%m-%dT%H:%M:%SZ")


def _membership_key(source: str, target_type: str, target_id: str, external_client_id: str, vk_user_id: str) -> str:
    client = _clean(external_client_id, 120) or _clean(vk_user_id, 120)
    return ":".join([_clean(source, 40), _clean(target_type, 80), _clean(target_id, 80), client])


def _has_human_admin_author(event: dict[str, Any]) -> bool:
    before = _clean(event.get("admin_before_author_id"), 80)
    after = _clean(event.get("admin_after_author_id"), 80)
    return bool((before and before.isdigit()) or (after and after.isdigit()))


async def _seed_memberships_from_actions(db: aiosqlite.Connection) -> None:
    cur = await db.execute("SELECT value FROM settings WHERE key='memberships_seed_version'")
    seed_version = await cur.fetchone()
    if seed_version and str(seed_version["value"] or "") == "2":
        return

    now = _now()
    cur = await db.execute(
        """
        SELECT scanner_event_id,vk_user_id,source,target_type,target_id,external_client_id,admin_message_at
        FROM actions
        WHERE status='success'
          AND target_type IN ('senler_subscription','salebot_list')
        ORDER BY admin_message_at ASC,id ASC
        """
    )
    rows = await cur.fetchall()
    cur = await db.execute("SELECT member_key FROM memberships")
    existing_keys = {str(row["member_key"] or "") for row in await cur.fetchall()}

    # Keep only the latest successful action for a membership. Backfill is a
    # one-time compatibility migration: existing rows are authoritative and
    # must never be reactivated after expiry or an explicit removal.
    latest_by_key: dict[str, aiosqlite.Row] = {}
    for row in rows:
        key = _membership_key(
            str(row["source"] or ""),
            str(row["target_type"] or ""),
            str(row["target_id"] or ""),
            str(row["external_client_id"] or ""),
            str(row["vk_user_id"] or ""),
        )
        if key.strip(":"):
            latest_by_key[key] = row

    seeded = 0
    for key, row in latest_by_key.items():
        if key in existing_keys:
            continue
        await _upsert_membership(
            db,
            scanner_event_id=int(row["scanner_event_id"] or 0),
            vk_user_id=str(row["vk_user_id"] or ""),
            source=str(row["source"] or ""),
            target_type=str(row["target_type"] or ""),
            target_id=str(row["target_id"] or ""),
            external_client_id=str(row["external_client_id"] or ""),
            admin_message_at=str(row["admin_message_at"] or now),
            now=now,
        )
        existing_keys.add(key)
        seeded += 1

    await db.execute(
        """
        INSERT INTO settings(key,value) VALUES('memberships_seed_version','2')
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """
    )
    _log("info", "admin-handoff membership backfill v2 completed seeded=%s", seeded)


async def _upsert_membership(
    db: aiosqlite.Connection,
    *,
    scanner_event_id: int,
    vk_user_id: str,
    source: str,
    target_type: str,
    target_id: str,
    external_client_id: str,
    admin_message_at: str,
    now: str,
) -> None:
    if target_type not in {"senler_subscription", "salebot_list"}:
        return
    key = _membership_key(source, target_type, target_id, external_client_id, vk_user_id)
    if not key.strip(":"):
        return
    expires_at = _expiry_from_admin_at(admin_message_at)
    await db.execute(
        """
        INSERT INTO memberships(
            member_key,vk_user_id,source,target_type,target_id,external_client_id,
            last_scanner_event_id,last_admin_message_at,expires_at,status,remove_error,
            remove_details_json,remove_attempts,created_at,updated_at,removed_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,'active','','{}',0,?,?,'')
        ON CONFLICT(member_key) DO UPDATE SET
            vk_user_id=excluded.vk_user_id,
            source=excluded.source,
            target_type=excluded.target_type,
            target_id=excluded.target_id,
            external_client_id=excluded.external_client_id,
            last_scanner_event_id=CASE
                WHEN memberships.expires_at <= excluded.expires_at THEN excluded.last_scanner_event_id
                ELSE memberships.last_scanner_event_id
            END,
            last_admin_message_at=CASE
                WHEN memberships.expires_at <= excluded.expires_at THEN excluded.last_admin_message_at
                ELSE memberships.last_admin_message_at
            END,
            expires_at=CASE
                WHEN memberships.expires_at <= excluded.expires_at THEN excluded.expires_at
                ELSE memberships.expires_at
            END,
            status=CASE
                WHEN memberships.status!='active' OR memberships.expires_at <= excluded.expires_at THEN 'active'
                ELSE memberships.status
            END,
            remove_error=CASE
                WHEN memberships.status!='active' OR memberships.expires_at <= excluded.expires_at THEN ''
                ELSE memberships.remove_error
            END,
            remove_details_json=CASE
                WHEN memberships.status!='active' OR memberships.expires_at <= excluded.expires_at THEN '{}'
                ELSE memberships.remove_details_json
            END,
            updated_at=excluded.updated_at,
            removed_at=CASE
                WHEN memberships.status!='active' OR memberships.expires_at <= excluded.expires_at THEN ''
                ELSE memberships.removed_at
            END
        """,
        (
            key,
            _clean(vk_user_id, 80),
            _clean(source, 40),
            _clean(target_type, 80),
            _clean(target_id, 80),
            _clean(external_client_id, 120),
            scanner_event_id,
            _clean(admin_message_at, 80),
            expires_at,
            now,
            now,
        ),
    )


async def _scanner_candidates(limit: int) -> list[dict[str, Any]]:
    scanner_db = _scanner_db_path()
    if not scanner_db.exists():
        return []
    cutoff = _utc_from_epoch(time.time() - _lookback_days() * 86400)
    if not _env_bool("ADMIN_HANDOFF_PROCESS_EXISTING", False):
        started_at = await _setting("started_at")
        if _epoch(started_at) > _epoch(cutoff):
            cutoff = started_at
    limit = max(1, min(MAX_PROCESS_LIMIT, int(limit or MAX_PROCESS_LIMIT)))
    async with _connect(scanner_db) as db:
        cur = await db.execute(
            """
            SELECT id,vk_message_id,peer_id,vk_user_id,profile_name,message_at,status,reason,
                   message_text_preview,admin_before_at,admin_before_author_id,
                   admin_after_at,admin_after_author_id,updated_at
            FROM scan_events
            WHERE source='vk'
              AND updated_at>=?
              AND vk_user_id!=''
              AND (admin_before_at!='' OR admin_after_at!='')
              AND (
                    (admin_before_author_id!='' AND admin_before_author_id NOT LIKE '-%')
                 OR (admin_after_author_id!='' AND admin_after_author_id NOT LIKE '-%')
              )
              AND status IN ('admin_conversation','resolved_not_unanswered','missing_human_reply','pending_human_response')
            ORDER BY COALESCE(NULLIF(admin_after_at,''), NULLIF(admin_before_at,''), message_at) ASC, id ASC
            LIMIT ?
            """,
            (cutoff, limit * 4),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    if not rows:
        return []
    ids = [int(row["id"]) for row in rows]
    async with _connect() as db:
        placeholders = ",".join("?" for _ in ids)
        cur = await db.execute(f"SELECT scanner_event_id,status FROM actions WHERE scanner_event_id IN ({placeholders})", ids)
        existing = {int(row["scanner_event_id"]): str(row["status"] or "") for row in await cur.fetchall()}
    result: list[dict[str, Any]] = []
    for row in rows:
        status = existing.get(int(row["id"]))
        if status == "success" or status == "skipped":
            continue
        result.append(row)
        if len(result) >= limit:
            break
    return result


async def _latest_openrouter_source(vk_user_id: str, event_at: str) -> dict[str, Any]:
    db_path = _openrouter_db_path()
    if not db_path.exists():
        return {"source": "", "reason": "openrouter_db_missing"}
    vk_id = _clean(vk_user_id, 80)
    event_ts = _epoch(event_at) or time.time()
    since = _utc_from_epoch(event_ts - _lookback_days() * 86400)
    until = _utc_from_epoch(event_ts + 86400)
    try:
        async with _connect(db_path) as db:
            cur = await db.execute(
                """
                SELECT source,conversation_id,platform_id,created_at
                FROM messages
                WHERE platform_id=? AND source IN ('senler','salebot') AND created_at BETWEEN ? AND ?
                ORDER BY created_at DESC,id DESC
                LIMIT 1
                """,
                (vk_id, since, until),
            )
            row = await cur.fetchone()
            if row:
                payload = {
                    "source": str(row["source"] or ""),
                    "conversation_id": str(row["conversation_id"] or ""),
                    "platform_id": str(row["platform_id"] or ""),
                    "created_at": str(row["created_at"] or ""),
                }
                if payload["source"] == "salebot":
                    payload["salebot_id"] = await _salebot_id_from_jobs(db, vk_id, payload["conversation_id"], event_ts)
                return payload
            salebot_id = await _salebot_id_from_jobs(db, vk_id, "", event_ts)
            if salebot_id:
                return {"source": "salebot", "salebot_id": salebot_id, "reason": "matched_openrouter_salebot_job"}
    except Exception as exc:
        _log("warning", "admin-handoff source lookup failed vk_user_id=%s error=%s", vk_id, exc)
        return {"source": "", "reason": f"openrouter_lookup_failed: {type(exc).__name__}: {exc}"}
    grace_seconds = _source_grace_seconds()
    age_seconds = max(0, int(time.time() - event_ts))
    if grace_seconds and age_seconds < grace_seconds:
        return {
            "source": "",
            "reason": "waiting_for_openrouter_source",
            "retryable": True,
            "age_seconds": age_seconds,
            "source_grace_seconds": grace_seconds,
        }
    if _salebot_lookup_fallback_enabled():
        salebot_id, lookup_details = await _salebot_find_client_id(vk_id)
        if salebot_id:
            return {
                "source": "salebot",
                "salebot_id": salebot_id,
                "platform_id": vk_id,
                "reason": "matched_salebot_platform_lookup",
                "lookup": lookup_details,
            }
    return {"source": "", "reason": "no_recent_senler_or_salebot_history"}


async def _salebot_id_from_jobs(db: aiosqlite.Connection, platform_id: str, conversation_id: str, event_ts: float) -> str:
    since = _utc_from_epoch(event_ts - _lookback_days() * 86400)
    patterns = [f'%"{platform_id}"%']
    if conversation_id:
        patterns.append(f'%"{conversation_id}"%')
    rows: list[dict[str, Any]] = []
    for pattern in patterns:
        cur = await db.execute(
            """
            SELECT payload_json,created_at
            FROM outbound_jobs
            WHERE source='salebot' AND created_at>=? AND payload_json LIKE ?
            ORDER BY created_at DESC,id DESC
            LIMIT 20
            """,
            (since, pattern),
        )
        rows.extend(dict(row) for row in await cur.fetchall())
    for row in sorted(rows, key=lambda item: str(item.get("created_at") or ""), reverse=True):
        payload = _json_loads_dict(row.get("payload_json"))
        salebot_id = _numeric(payload.get("salebot_id") or payload.get("client_id"), 32)
        payload_platform_id = _clean(payload.get("platform_id") or payload.get("user_id"), 300)
        payload_conversation_id = _clean(payload.get("conversation_id"), 120)
        if not salebot_id:
            continue
        if payload_platform_id == platform_id or (conversation_id and payload_conversation_id == conversation_id):
            return salebot_id
    return ""


async def _senler_post(endpoint: str, data: dict[str, Any], timeout: float = 15.0) -> tuple[bool, str, dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(f"{SENLER_API_BASE}/{endpoint}", data=data)
        raw = resp.text[:3000]
        try:
            body = resp.json()
        except Exception:
            return False, f"Senler returned non-JSON HTTP {resp.status_code}", {"http_status": resp.status_code, "raw": raw}
    except Exception as exc:
        return False, f"Senler transport error: {type(exc).__name__}: {exc}", {"exception": str(exc)}
    details = {"http_status": resp.status_code, "response": body}
    if resp.status_code >= 400:
        return False, f"Senler HTTP {resp.status_code}", details
    if isinstance(body, dict) and body.get("success"):
        return True, "", details
    error = body.get("error") if isinstance(body, dict) else body
    if isinstance(error, dict):
        msg = _clean(error.get("error_msg") or error.get("message") or error, 500)
    else:
        msg = _clean(body.get("error_message") if isinstance(body, dict) else error, 500)
    return False, msg or "Senler API returned success=false", details


async def _senler_add(vk_user_id: str) -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    vk_id = _numeric(vk_user_id, 32)
    subscription_id = env["senler_subscription_id"]
    if not vk_id:
        return False, "vk_user_id is not numeric", {"vk_user_id": vk_user_id}
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN or SENLER_GROUP_ID is not configured", {}
    if not subscription_id:
        return False, "ADMIN_HANDOFF_SENLER_SUBSCRIPTION_ID is invalid", {}
    base = {
        "access_token": env["senler_token"],
        "group_id": env["senler_group_id"],
        "subscription_id": subscription_id,
        "vk_user_id": vk_id,
        "v": SENLER_API_VERSION,
    }
    already, check_error, check_details = await _senler_post("subscribers/get", base)
    if already:
        items = (check_details.get("response") or {}).get("items") if isinstance(check_details.get("response"), dict) else []
        if isinstance(items, list) and items:
            return True, "", {"skipped": True, "reason": "already_subscribed", "check": check_details}
    ok, error, add_details = await _senler_post("subscribers/add", base)
    details = {"check": check_details, "add": add_details, "target_subscription_id": subscription_id}
    if ok:
        return True, "", details
    return False, error or check_error, details


async def _senler_find(vk_user_id: str) -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    vk_id = _numeric(vk_user_id, 32)
    if not vk_id:
        return False, "vk_user_id is not numeric", {"vk_user_id": vk_user_id}
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN or SENLER_GROUP_ID is not configured", {}
    ok, error, details = await _senler_post(
        "subscribers/get",
        {
            "access_token": env["senler_token"],
            "group_id": env["senler_group_id"],
            "vk_user_id": vk_id,
            "v": SENLER_API_VERSION,
        },
    )
    if not ok:
        return False, error, details
    body = details.get("response")
    items = body.get("items") if isinstance(body, dict) else []
    return bool(isinstance(items, list) and items), "", details


async def _senler_remove(vk_user_id: str, subscription_id: str = "") -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    vk_id = _numeric(vk_user_id, 32)
    clean_subscription_id = _numeric(subscription_id or env["senler_subscription_id"], 32)
    if not vk_id:
        return False, "vk_user_id is not numeric", {"vk_user_id": vk_user_id}
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN or SENLER_GROUP_ID is not configured", {}
    if not clean_subscription_id:
        return False, "Senler subscription_id is invalid", {}
    ok, error, details = await _senler_post(
        "subscribers/del",
        {
            "access_token": env["senler_token"],
            "group_id": env["senler_group_id"],
            "subscription_id": clean_subscription_id,
            "vk_user_id": vk_id,
            "v": SENLER_API_VERSION,
        },
    )
    return ok, error, {"delete": details, "target_subscription_id": clean_subscription_id}


async def _salebot_post(action: str, payload: dict[str, Any], timeout: float = 20.0) -> tuple[bool, str, dict[str, Any]]:
    api_key = _env()["salebot_key"]
    if not api_key:
        return False, "SALEBOT_API_KEY or SALEBOT_API_KEY_3 is not configured", {}
    url = f"{SALEBOT_API_BASE}/{api_key}/{action}"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=payload)
        raw = resp.text[:3000]
        try:
            body: Any = resp.json()
        except Exception:
            body = raw
    except Exception as exc:
        return False, f"Salebot transport error: {type(exc).__name__}: {exc}", {"exception": str(exc)}
    details = {"http_status": resp.status_code, "response": body}
    if resp.status_code >= 400:
        return False, f"Salebot HTTP {resp.status_code}", details
    if isinstance(body, dict):
        if body.get("success") is False or body.get("status") == "error" or body.get("error"):
            return False, _clean(body.get("error") or body.get("message") or body, 500), details
    return True, "", details


async def _salebot_find_client_id(platform_id: str) -> tuple[str, dict[str, Any]]:
    env = _env()
    clean_platform_id = _numeric(platform_id, 80)
    if not clean_platform_id:
        return "", {"skipped": True, "reason": "platform_id_is_not_numeric"}
    payload: dict[str, Any] = {"platform_ids": [clean_platform_id]}
    if env["salebot_group_id"]:
        payload["group_id"] = env["salebot_group_id"]
    ok, error, details = await _salebot_post("find_client_id_by_platform_id", payload)
    if not ok:
        return "", {"ok": False, "error": error, "details": details}
    body = details.get("response")
    candidates: list[Any] = []
    if isinstance(body, dict):
        for key in ("client_id", "clients", "client_ids", "result", "items", "data"):
            value = body.get(key)
            if isinstance(value, list):
                candidates.extend(value)
            elif isinstance(value, dict):
                candidates.extend(value.values())
            elif value is not None:
                candidates.append(value)
    elif isinstance(body, list):
        candidates.extend(body)
    for item in candidates:
        if isinstance(item, dict):
            item = item.get("client_id") or item.get("id")
        client_id = _numeric(item, 32)
        if client_id:
            return client_id, {"ok": True, "lookup": details}
    return "", {"ok": False, "error": "client_id not found in Salebot response", "lookup": details}


async def _salebot_add(client_id: str, platform_id: str) -> tuple[bool, str, dict[str, Any], str]:
    env = _env()
    clean_client_id = _numeric(client_id, 32)
    lookup_details: dict[str, Any] = {}
    if not clean_client_id:
        if _looks_like_salebot_client_id(platform_id):
            clean_client_id = _numeric(platform_id, 32)
        else:
            clean_client_id, lookup_details = await _salebot_find_client_id(platform_id)
    if not clean_client_id:
        return False, "Salebot client_id is not known", {"lookup": lookup_details}, ""
    list_id = env["salebot_list_id"]
    if not list_id:
        return False, "ADMIN_HANDOFF_SALEBOT_LIST_ID is invalid", {}, clean_client_id
    ok, error, details = await _salebot_post("add_to_list", {"list_id": int(list_id), "clients": [int(clean_client_id)]})
    return ok, error, {"lookup": lookup_details, "add": details, "target_list_id": list_id}, clean_client_id


async def _salebot_remove(client_id: str, list_id: str = "") -> tuple[bool, str, dict[str, Any]]:
    clean_client_id = _numeric(client_id, 32)
    clean_list_id = _numeric(list_id or _env()["salebot_list_id"], 32)
    if not clean_client_id:
        return False, "Salebot client_id is not numeric", {"client_id": client_id}
    if not clean_list_id:
        return False, "Salebot list_id is invalid", {"list_id": list_id}
    ok, error, details = await _salebot_post(
        "remove_from_list",
        {"list_id": int(clean_list_id), "clients": [int(clean_client_id)]},
    )
    return ok, error, {"remove": details, "target_list_id": clean_list_id}


async def _record_action(
    event: dict[str, Any],
    *,
    source: str,
    target_type: str,
    target_id: str,
    external_client_id: str,
    status: str,
    error: str,
    details: dict[str, Any],
    memberships: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    now = _now()
    admin_at = _clean(event.get("admin_after_at") or event.get("admin_before_at") or event.get("message_at"), 80)
    async with _connect() as db:
        await db.execute(
            """
            INSERT INTO actions(
                scanner_event_id,scanner_vk_message,vk_user_id,source,target_type,target_id,external_client_id,
                status,error,details_json,attempts,admin_message_at,created_at,updated_at,processed_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)
            ON CONFLICT(scanner_event_id) DO UPDATE SET
                source=excluded.source,
                target_type=excluded.target_type,
                target_id=excluded.target_id,
                external_client_id=excluded.external_client_id,
                status=excluded.status,
                error=excluded.error,
                details_json=excluded.details_json,
                attempts=actions.attempts+1,
                admin_message_at=excluded.admin_message_at,
                updated_at=excluded.updated_at,
                processed_at=excluded.processed_at
            """,
            (
                int(event["id"]),
                _clean(event.get("vk_message_id"), 160),
                _clean(event.get("vk_user_id"), 80),
                source,
                target_type,
                target_id,
                external_client_id,
                status,
                error[:1000],
                _json_dumps(details),
                admin_at,
                now,
                now,
                now if status in {"success", "skipped"} else "",
            ),
        )
        success_memberships = memberships
        if success_memberships is None and status == "success":
            success_memberships = [
                {
                    "source": source,
                    "target_type": target_type,
                    "target_id": target_id,
                    "external_client_id": external_client_id,
                }
            ]
        for item in success_memberships or []:
            await _upsert_membership(
                db,
                scanner_event_id=int(event["id"]),
                vk_user_id=_clean(event.get("vk_user_id"), 80),
                source=_clean(item.get("source"), 40),
                target_type=_clean(item.get("target_type"), 80),
                target_id=_clean(item.get("target_id"), 80),
                external_client_id=_clean(item.get("external_client_id"), 120),
                admin_message_at=admin_at,
                now=now,
            )
        await db.commit()
    return {
        "scanner_event_id": int(event["id"]),
        "vk_user_id": _clean(event.get("vk_user_id"), 80),
        "source": source,
        "target_type": target_type,
        "target_id": target_id,
        "external_client_id": external_client_id,
        "status": status,
        "error": error,
    }


async def _process_event(event: dict[str, Any]) -> dict[str, Any]:
    vk_user_id = _clean(event.get("vk_user_id"), 80)
    if not _has_human_admin_author(event):
        return await _record_action(
            event,
            source="group_message",
            target_type="",
            target_id="",
            external_client_id="",
            status="skipped",
            error="outgoing message was from group, not a named admin",
            details={"admin_author_id": event.get("admin_before_author_id") or event.get("admin_after_author_id") or ""},
        )
    admin_allowed, admin_payload = await _admin_allowed(event)
    if not admin_allowed:
        return await _record_action(
            event,
            source="admin_filtered",
            target_type="",
            target_id="",
            external_client_id="",
            status="skipped",
            error=f"admin is not enabled for handoff: {admin_payload.get('admin_name') or admin_payload.get('admin_author_id')}",
            details={"admin": admin_payload},
        )
    env = _env()
    target_results: list[dict[str, Any]] = []
    success_memberships: list[dict[str, str]] = []

    senler_found, senler_find_error, senler_find_details = await _senler_find(vk_user_id)
    senler_details: dict[str, Any] = {"lookup": senler_find_details, "found": senler_found}
    if senler_found:
        ok, error, add_details = await _senler_add(vk_user_id)
        senler_details["add"] = add_details
        target_results.append({"source": "senler", "ok": ok, "error": error, "target_type": "senler_subscription", "target_id": env["senler_subscription_id"], "external_client_id": vk_user_id})
        if ok:
            success_memberships.append({"source": "senler", "target_type": "senler_subscription", "target_id": env["senler_subscription_id"], "external_client_id": vk_user_id})
    elif senler_find_error:
        target_results.append({"source": "senler", "ok": False, "error": senler_find_error, "target_type": "senler_subscription", "target_id": env["senler_subscription_id"], "external_client_id": vk_user_id})

    salebot_id, salebot_lookup_details = await _salebot_find_client_id(vk_user_id)
    salebot_details: dict[str, Any] = {"lookup": salebot_lookup_details, "found": bool(salebot_id), "client_id": salebot_id}
    if salebot_id:
        ok, error, add_details, client_id = await _salebot_add(salebot_id, vk_user_id)
        salebot_details["add"] = add_details
        clean_client_id = client_id or salebot_id
        target_results.append({"source": "salebot", "ok": ok, "error": error, "target_type": "salebot_list", "target_id": env["salebot_list_id"], "external_client_id": clean_client_id})
        if ok:
            success_memberships.append({"source": "salebot", "target_type": "salebot_list", "target_id": env["salebot_list_id"], "external_client_id": clean_client_id})
    elif salebot_lookup_details.get("error") and not salebot_lookup_details.get("skipped"):
        target_results.append({"source": "salebot", "ok": False, "error": _clean(salebot_lookup_details.get("error"), 1000), "target_type": "salebot_list", "target_id": env["salebot_list_id"], "external_client_id": ""})

    found_sources = [item["source"] for item in target_results]
    failed_targets = [item for item in target_results if not item.get("ok")]
    success_targets = [item for item in target_results if item.get("ok")]
    if success_targets and failed_targets:
        status = "partial"
    elif success_targets:
        status = "success"
    elif failed_targets:
        status = "failed"
    else:
        status = "skipped"
    source = "+".join(sorted(set(found_sources))) if found_sources else "unknown"
    target_type = "+".join(item["target_type"] for item in success_targets) if success_targets else ""
    target_id = "+".join(item["target_id"] for item in success_targets) if success_targets else ""
    external_client_id = "+".join(item["external_client_id"] for item in success_targets) if success_targets else ""
    error = "; ".join(f"{item['source']}: {item.get('error')}" for item in failed_targets if item.get("error"))
    if status == "skipped":
        error = "not found in Senler or Salebot"
    result = await _record_action(
        event,
        source=source,
        target_type=target_type,
        target_id=target_id,
        external_client_id=external_client_id,
        status=status,
        error=error,
        details={"admin": admin_payload, "senler": senler_details, "salebot": salebot_details, "targets": target_results, "source_strategy": "primary_api_lookup"},
        memberships=success_memberships,
    )
    if result["status"] == "success":
        _log(
            "info",
            "admin-handoff processed scanner_event=%s vk_user_id=%s source=%s target=%s:%s",
            result["scanner_event_id"],
            result["vk_user_id"],
            result["source"],
            result["target_type"],
            result["target_id"],
        )
    elif result["status"] == "failed":
        _log(
            "warning",
            "admin-handoff failed scanner_event=%s vk_user_id=%s source=%s error=%s",
            result["scanner_event_id"],
            result["vk_user_id"],
            result["source"],
            result["error"],
        )
    return result


async def _process_once(limit: int = MAX_PROCESS_LIMIT) -> dict[str, Any]:
    if not _enabled():
        return {"ok": False, "enabled": False, "reason": "disabled"}
    async with _write_lock:
        events = await _scanner_candidates(limit)
        results = []
        for event in events:
            try:
                results.append(await _process_event(event))
            except Exception as exc:
                _log("warning", "admin-handoff event crashed scanner_event=%s error=%s", event.get("id"), exc)
                results.append(
                    await _record_action(
                        event,
                        source="error",
                        target_type="",
                        target_id="",
                        external_client_id="",
                        status="failed",
                        error=f"{type(exc).__name__}: {exc}",
                        details={"exception": str(exc)},
                    )
                )
        expiry = await _expire_memberships(MAX_PROCESS_LIMIT)
        return {
            "ok": True,
            "enabled": True,
            "seen": len(events),
            "processed": len([item for item in results if item.get("status") == "success"]),
            "skipped": len([item for item in results if item.get("status") == "skipped"]),
            "failed": len([item for item in results if item.get("status") == "failed"]),
            "expired": expiry["expired"],
            "expire_failed": expiry["failed"],
            "items": results,
        }


async def _expire_memberships(limit: int = MAX_PROCESS_LIMIT) -> dict[str, Any]:
    now = _now()
    limit = max(1, min(MAX_PROCESS_LIMIT, int(limit or MAX_PROCESS_LIMIT)))
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT *
            FROM memberships
            WHERE status='active' AND expires_at!='' AND expires_at<=?
            ORDER BY expires_at ASC,id ASC
            LIMIT ?
            """,
            (now, limit),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    expired = 0
    failed = 0
    for row in rows:
        membership_id = int(row["id"])
        source = _clean(row.get("source"), 40)
        target_type = _clean(row.get("target_type"), 80)
        target_id = _clean(row.get("target_id"), 80)
        external_client_id = _clean(row.get("external_client_id"), 120)
        vk_user_id = _clean(row.get("vk_user_id"), 80)
        if target_type == "senler_subscription":
            ok, error, details = await _senler_remove(vk_user_id, target_id)
        elif target_type == "salebot_list":
            ok, error, details = await _salebot_remove(external_client_id, target_id)
        else:
            ok, error, details = False, f"unsupported target_type: {target_type}", {}
        async with _connect() as db:
            if ok:
                await db.execute(
                    """
                    UPDATE memberships
                    SET status='expired',
                        remove_error='',
                        remove_details_json=?,
                        remove_attempts=remove_attempts+1,
                        updated_at=?,
                        removed_at=?
                    WHERE id=?
                    """,
                    (_json_dumps(details), now, now, membership_id),
                )
                expired += 1
                _log(
                    "info",
                    "admin-handoff expired membership id=%s source=%s target=%s:%s client=%s",
                    membership_id,
                    source,
                    target_type,
                    target_id,
                    external_client_id or vk_user_id,
                )
            else:
                await db.execute(
                    """
                    UPDATE memberships
                    SET remove_error=?,
                        remove_details_json=?,
                        remove_attempts=remove_attempts+1,
                        updated_at=?
                    WHERE id=?
                    """,
                    (_clean(error, 1000), _json_dumps(details), now, membership_id),
                )
                failed += 1
                _log(
                    "warning",
                    "admin-handoff membership expire failed id=%s source=%s target=%s:%s error=%s",
                    membership_id,
                    source,
                    target_type,
                    target_id,
                    error,
                )
            await db.commit()
    return {"expired": expired, "failed": failed}


async def _cleanup_excluded_admin_memberships(limit: int = 500, dry_run: bool = False) -> dict[str, Any]:
    scanner_db = _scanner_db_path()
    if not scanner_db.exists():
        return {"ok": False, "reason": "scanner DB is not ready", "items": [], "removed": 0, "failed": 0, "dry_run": dry_run}
    filter_enabled = (await _setting("admin_filter_enabled")).lower() in {"1", "true", "yes", "on", "да"}
    allowed_ids = set(await _allowed_admin_ids())
    if not filter_enabled:
        return {"ok": True, "reason": "admin filter is disabled", "items": [], "removed": 0, "failed": 0, "dry_run": dry_run}
    limit = max(1, min(1000, int(limit or 500)))
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT *
            FROM memberships
            WHERE status='active'
            ORDER BY updated_at DESC,id DESC
            LIMIT ?
            """,
            (limit,),
        )
        memberships = [dict(row) for row in await cur.fetchall()]
    scanner_ids = sorted({int(row["last_scanner_event_id"] or 0) for row in memberships if int(row["last_scanner_event_id"] or 0)})
    authors: dict[int, str] = {}
    if scanner_ids:
        async with _connect(scanner_db) as db:
            for chunk_start in range(0, len(scanner_ids), 500):
                chunk = scanner_ids[chunk_start : chunk_start + 500]
                placeholders = ",".join("?" for _ in chunk)
                cur = await db.execute(
                    f"SELECT id,admin_before_author_id,admin_after_author_id FROM scan_events WHERE id IN ({placeholders})",
                    chunk,
                )
                for row in await cur.fetchall():
                    event = dict(row)
                    authors[int(event["id"])] = _admin_author_id(event)
    candidates: list[dict[str, Any]] = []
    for row in memberships:
        scanner_id = int(row["last_scanner_event_id"] or 0)
        admin_id = authors.get(scanner_id, "")
        if not admin_id or admin_id in allowed_ids:
            continue
        item = {**row, "admin_author_id": admin_id}
        candidates.append(item)
    admin_names = await _admin_names([item["admin_author_id"] for item in candidates])
    removed = 0
    failed = 0
    items: list[dict[str, Any]] = []
    now = _now()
    for row in candidates:
        membership_id = int(row["id"])
        source = _clean(row.get("source"), 40)
        target_type = _clean(row.get("target_type"), 80)
        target_id = _clean(row.get("target_id"), 80)
        external_client_id = _clean(row.get("external_client_id"), 120)
        vk_user_id = _clean(row.get("vk_user_id"), 80)
        admin_id = _clean(row.get("admin_author_id"), 80)
        admin_name = admin_names.get(admin_id) or admin_id
        if dry_run:
            ok, error, details = True, "", {"dry_run": True}
        elif target_type == "senler_subscription":
            ok, error, details = await _senler_remove(vk_user_id, target_id)
        elif target_type == "salebot_list":
            ok, error, details = await _salebot_remove(external_client_id, target_id)
        else:
            ok, error, details = False, f"unsupported target_type: {target_type}", {}
        if not dry_run:
            async with _connect() as db:
                if ok:
                    await db.execute(
                        """
                        UPDATE memberships
                        SET status='expired',
                            remove_error='',
                            remove_details_json=?,
                            remove_attempts=remove_attempts+1,
                            updated_at=?,
                            removed_at=?
                        WHERE id=?
                        """,
                        (_json_dumps({"cleanup_excluded_admin": True, "admin_id": admin_id, "admin_name": admin_name, "remove": details}), now, now, membership_id),
                    )
                    removed += 1
                else:
                    await db.execute(
                        """
                        UPDATE memberships
                        SET remove_error=?,
                            remove_details_json=?,
                            remove_attempts=remove_attempts+1,
                            updated_at=?
                        WHERE id=?
                        """,
                        (_clean(error, 1000), _json_dumps({"cleanup_excluded_admin": True, "admin_id": admin_id, "admin_name": admin_name, "remove": details}), now, membership_id),
                    )
                    failed += 1
                await db.commit()
        items.append(
            {
                "id": membership_id,
                "vk_user_id": vk_user_id,
                "source": source,
                "target_type": target_type,
                "target_id": target_id,
                "external_client_id": external_client_id,
                "admin_author_id": admin_id,
                "admin_name": admin_name,
                "ok": ok,
                "error": error,
            }
        )
        if dry_run and ok:
            removed += 1
    return {"ok": failed == 0, "items": items, "removed": removed, "failed": failed, "dry_run": dry_run}


def _start_worker() -> None:
    global _worker_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if not _enabled():
        _log("info", "admin-handoff disabled")
        return
    if _worker_task is None or _worker_task.done():
        _worker_task = loop.create_task(_worker_loop())


async def _worker_loop() -> None:
    await asyncio.sleep(15)
    while True:
        try:
            await _process_once(MAX_PROCESS_LIMIT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "admin-handoff poll failed: %s", exc)
        await asyncio.sleep(_poll_seconds())


async def _actions_payload(limit: int = 100, status: str = "", source: str = "") -> dict[str, Any]:
    limit = max(1, min(500, int(limit or 100)))
    where_parts: list[str] = []
    params: list[Any] = []
    if status:
        where_parts.append("status=?")
        params.append(status)
    if source:
        where_parts.append("source=?")
        params.append(source)
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT status,COUNT(*) AS total
            FROM actions
            GROUP BY status
            """
        )
        counts = {str(row["status"]): int(row["total"] or 0) for row in await cur.fetchall()}
        cur = await db.execute(
            f"""
            SELECT *
            FROM actions
            {where}
            ORDER BY updated_at DESC,id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["details"] = _json_loads_dict(row.pop("details_json", "{}"))
        row["vk_url"] = _vk_dialog_url(row.get("vk_user_id"))
    return {"items": rows, "counts": counts, "limit": limit}


async def _memberships_payload(limit: int = 100, status: str = "", source: str = "") -> dict[str, Any]:
    limit = max(1, min(500, int(limit or 100)))
    where_parts: list[str] = []
    params: list[Any] = []
    if status:
        where_parts.append("status=?")
        params.append(status)
    if source:
        where_parts.append("source=?")
        params.append(source)
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT status,COUNT(*) AS total
            FROM memberships
            GROUP BY status
            """
        )
        counts = {str(row["status"]): int(row["total"] or 0) for row in await cur.fetchall()}
        cur = await db.execute(
            f"""
            SELECT *
            FROM memberships
            {where}
            ORDER BY expires_at ASC,id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    scanner_ids = sorted({int(row["last_scanner_event_id"] or 0) for row in rows if int(row["last_scanner_event_id"] or 0)})
    admin_by_scanner_id: dict[int, str] = {}
    if scanner_ids and _scanner_db_path().exists():
        async with _connect(_scanner_db_path()) as db:
            for chunk_start in range(0, len(scanner_ids), 500):
                chunk = scanner_ids[chunk_start : chunk_start + 500]
                placeholders = ",".join("?" for _ in chunk)
                cur = await db.execute(
                    f"SELECT id,admin_before_author_id,admin_after_author_id FROM scan_events WHERE id IN ({placeholders})",
                    chunk,
                )
                for event in await cur.fetchall():
                    event_dict = dict(event)
                    admin_by_scanner_id[int(event_dict["id"])] = _admin_author_id(event_dict)
    admin_names = await _admin_names(list(admin_by_scanner_id.values()))
    for row in rows:
        row["remove_details"] = _json_loads_dict(row.pop("remove_details_json", "{}"))
        row["vk_url"] = _vk_dialog_url(row.get("vk_user_id"))
        admin_id = admin_by_scanner_id.get(int(row.get("last_scanner_event_id") or 0), "")
        row["admin_author_id"] = admin_id
        row["admin_name"] = admin_names.get(admin_id) or admin_id
    return {"items": rows, "counts": counts, "limit": limit}


async def _workspace_payload(limit: int = 500) -> dict[str, Any]:
    """Return person-centric protection state plus current Scanner counters."""
    limit = max(1, min(500, int(limit or 500)))
    async with _connect() as db:
        cur = await db.execute(
            """
            SELECT *
            FROM memberships
            WHERE status='active'
            ORDER BY expires_at ASC,id DESC
            """
        )
        active_rows = [dict(row) for row in await cur.fetchall()]
        cur = await db.execute("SELECT status,COUNT(*) AS total FROM actions GROUP BY status")
        action_counts = {str(row["status"]): int(row["total"] or 0) for row in await cur.fetchall()}

    now_utc = datetime.now(timezone.utc)
    moscow = timezone(timedelta(hours=3))
    now_moscow = now_utc.astimezone(moscow)
    today_start = now_moscow.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    today_end = today_start + timedelta(days=1)

    people_by_id: dict[str, dict[str, Any]] = {}
    expiring_today_people: set[str] = set()
    expiring_today_stops = 0
    service_counts = {"senler": 0, "salebot": 0}
    for row in active_rows:
        vk_user_id = _clean(row.get("vk_user_id"), 80)
        if not vk_user_id:
            continue
        target_type = _clean(row.get("target_type"), 80)
        service = "senler" if target_type == "senler_subscription" else "salebot" if target_type == "salebot_list" else _clean(row.get("source"), 40)
        if service in service_counts:
            service_counts[service] += 1
        expires = _parse_utc(row.get("expires_at"))
        if expires and today_start <= expires < today_end:
            expiring_today_stops += 1
            expiring_today_people.add(vk_user_id)

        person = people_by_id.setdefault(
            vk_user_id,
            {
                "vk_user_id": vk_user_id,
                "vk_url": _vk_dialog_url(vk_user_id),
                "profile_name": "",
                "services": [],
                "memberships": [],
                "active_stops": 0,
                "expires_at": "",
                "last_admin_message_at": "",
                "admin_author_id": "",
                "admin_name": "",
                "scanner_event": None,
            },
        )
        membership = {
            "id": int(row.get("id") or 0),
            "service": service,
            "source": _clean(row.get("source"), 40),
            "target_type": target_type,
            "target_id": _clean(row.get("target_id"), 80),
            "external_client_id": _clean(row.get("external_client_id"), 120),
            "expires_at": _clean(row.get("expires_at"), 80),
            "last_admin_message_at": _clean(row.get("last_admin_message_at"), 80),
            "last_scanner_event_id": int(row.get("last_scanner_event_id") or 0),
        }
        person["memberships"].append(membership)
        person["active_stops"] += 1
        if service and service not in person["services"]:
            person["services"].append(service)
        expiry_text = membership["expires_at"]
        if expiry_text and (not person["expires_at"] or _epoch(expiry_text) < _epoch(person["expires_at"])):
            person["expires_at"] = expiry_text
        admin_at = membership["last_admin_message_at"]
        if admin_at and _epoch(admin_at) >= _epoch(person["last_admin_message_at"]):
            person["last_admin_message_at"] = admin_at

    scanner_counts: dict[str, dict[str, int]] = {}
    latest_by_user: dict[str, dict[str, Any]] = {}
    scanner_db = _scanner_db_path()
    if scanner_db.exists():
        async with _connect(scanner_db) as db:
            cur = await db.execute(
                """
                SELECT status,
                       COUNT(DISTINCT NULLIF(vk_user_id,'')) AS total,
                       COUNT(DISTINCT CASE WHEN read_at='' THEN NULLIF(vk_user_id,'') END) AS unread
                FROM scan_events
                WHERE status IN ('missing_webhook','missing_human_reply','admin_conversation')
                GROUP BY status
                """
            )
            scanner_counts = {
                str(row["status"]): {"total": int(row["total"] or 0), "unread": int(row["unread"] or 0)}
                for row in await cur.fetchall()
            }
            vk_ids = sorted(people_by_id)
            for chunk_start in range(0, len(vk_ids), 400):
                chunk = vk_ids[chunk_start : chunk_start + 400]
                placeholders = ",".join("?" for _ in chunk)
                cur = await db.execute(
                    f"""
                    SELECT id,vk_user_id,peer_id,profile_name,message_at,message_text_preview,status,reason,
                           admin_before_at,admin_before_author_id,admin_after_at,admin_after_author_id,
                           read_at,opened_at,updated_at
                    FROM scan_events
                    WHERE vk_user_id IN ({placeholders})
                    ORDER BY message_at DESC,id DESC
                    """,
                    chunk,
                )
                for event in await cur.fetchall():
                    item = dict(event)
                    vk_user_id = _clean(item.get("vk_user_id"), 80)
                    if vk_user_id and vk_user_id not in latest_by_user:
                        latest_by_user[vk_user_id] = item

    event_admin_ids: dict[int, str] = {}
    for vk_user_id, person in people_by_id.items():
        event = latest_by_user.get(vk_user_id)
        if not event:
            continue
        event["vk_url"] = _vk_dialog_url(event.get("peer_id") or vk_user_id)
        person["scanner_event"] = event
        person["profile_name"] = _clean(event.get("profile_name"), 300)
        admin_id = _admin_author_id(event)
        if admin_id:
            event_admin_ids[int(event.get("id") or 0)] = admin_id
            person["admin_author_id"] = admin_id

    admin_names = await _admin_names(list(event_admin_ids.values()))
    for person in people_by_id.values():
        admin_id = str(person.get("admin_author_id") or "")
        person["admin_name"] = admin_names.get(admin_id) or admin_id
        person["services"].sort()

    people = sorted(
        people_by_id.values(),
        key=lambda item: (_epoch(item.get("expires_at")) or float("inf"), str(item.get("vk_user_id") or "")),
    )
    admin_count = scanner_counts.get("admin_conversation", {})
    missing_webhook = scanner_counts.get("missing_webhook", {})
    missing_reply = scanner_counts.get("missing_human_reply", {})
    return {
        "ok": True,
        "scanner_ready": scanner_db.exists(),
        "generated_at": _now(),
        "counts": {
            "waiting_admin_people": int(admin_count.get("total", 0)),
            "waiting_admin_unread": int(admin_count.get("unread", 0)),
            "problem_people": int(missing_webhook.get("total", 0)) + int(missing_reply.get("total", 0)),
            "problem_unread": int(missing_webhook.get("unread", 0)) + int(missing_reply.get("unread", 0)),
            "protected_people": len(people_by_id),
            "active_stops": len(active_rows),
            "expiring_today_people": len(expiring_today_people),
            "expiring_today_stops": expiring_today_stops,
            "senler_stops": service_counts["senler"],
            "salebot_stops": service_counts["salebot"],
        },
        "action_counts": action_counts,
        "scanner_counts": scanner_counts,
        "people": people[:limit],
        "total_people": len(people_by_id),
        "limit": limit,
    }


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID}


@router.get("/env-status")
async def env_status(request: Request):
    await _require_user(request)
    env = _env()
    scanner_db = _scanner_db_path()
    openrouter_db = _openrouter_db_path()
    return {
        "ok": True,
        "enabled": _enabled(),
        "poll_seconds": _poll_seconds(),
        "lookback_days": _lookback_days(),
        "retention_days": _retention_days(),
        "salebot_lookup_fallback": _salebot_lookup_fallback_enabled(),
        "started_at": await _setting("started_at"),
        "process_existing": _env_bool("ADMIN_HANDOFF_PROCESS_EXISTING", False),
        "scanner_db": str(scanner_db),
        "scanner_db_ready": scanner_db.exists(),
        "openrouter_db": str(openrouter_db),
        "openrouter_db_ready": openrouter_db.exists(),
        "SENLER_ACCESS_TOKEN": bool(env["senler_token"]),
        "SENLER_GROUP_ID": bool(env["senler_group_id"]),
        "SALEBOT_API_KEY": bool(env["salebot_key"]),
        "SALEBOT_GROUP_ID": bool(env["salebot_group_id"]),
        "VK_TOKEN": bool(env["vk_token"]),
        "senler_subscription_id": env["senler_subscription_id"],
        "salebot_list_id": env["salebot_list_id"],
        "ready": bool(scanner_db.exists() and openrouter_db.exists() and env["senler_token"] and env["senler_group_id"] and env["salebot_key"]),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    return await _settings_payload()


@router.get("/admins")
async def admins(request: Request, days: int = 90):
    await _require_user(request)
    return await _admins_payload(days=days)


@router.post("/settings")
async def save_settings(request: Request):
    await _require_user(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(400, "invalid settings payload")
    return await _save_settings(body)


@router.post("/admins/cleanup-excluded")
async def cleanup_excluded_admins(request: Request, limit: int = 500, dry_run: bool = False):
    await _require_user(request)
    return await _cleanup_excluded_admin_memberships(limit=limit, dry_run=dry_run)


@router.get("/actions")
async def actions(request: Request, limit: int = 100, status: str = "", source: str = ""):
    await _require_user(request)
    return await _actions_payload(limit=limit, status=_clean(status, 40), source=_clean(source, 40))


@router.get("/memberships")
async def memberships(request: Request, limit: int = 100, status: str = "", source: str = ""):
    await _require_user(request)
    return await _memberships_payload(limit=limit, status=_clean(status, 40), source=_clean(source, 40))


@router.get("/workspace")
async def workspace(request: Request, limit: int = 500):
    await _require_user(request)
    return await _workspace_payload(limit=limit)


@router.post("/process")
async def process(request: Request, limit: int = MAX_PROCESS_LIMIT):
    await _require_user(request)
    result = await _process_once(limit=limit)
    payload = await _actions_payload(limit=100)
    active = await _memberships_payload(limit=100, status="active")
    return {**result, "actions": payload, "memberships": active}


@router.post("/actions/{action_id}/retry")
async def retry_action(action_id: int, request: Request):
    await _require_user(request)
    async with _connect() as db:
        cur = await db.execute("SELECT scanner_event_id FROM actions WHERE id=?", (action_id,))
        action = await cur.fetchone()
    if not action:
        raise HTTPException(404, "action not found")
    scanner_db = _scanner_db_path()
    if not scanner_db.exists():
        raise HTTPException(503, "scanner DB is not ready")
    async with _connect(scanner_db) as db:
        cur = await db.execute("SELECT * FROM scan_events WHERE id=?", (int(action["scanner_event_id"]),))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "scanner event not found")
    result = await _process_event(dict(row))
    return {"ok": True, "item": result}
