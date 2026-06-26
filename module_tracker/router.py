from __future__ import annotations

import gzip
import ipaddress
import json
import logging
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "tracker"
DEFAULT_RETENTION_MONTHS = 6
MAX_RAW_EVENTS = 500
MAX_PROFILE_LIMIT = 200
MAX_PAYLOAD_BYTES = 220_000
UTM_FIELDS = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")
CLICK_ID_KEYS = ("yclid", "gclid", "fbclid", "ttclid", "msclkid", "roistat", "_openstat", "_ym_uid")
SENSITIVE_FIELD_RE = re.compile(
    r"(password|passwd|pwd|token|secret|captcha|otp|sms[_-]?code|verification|confirm[_-]?code|csrf|card|cvv|cvc)",
    re.IGNORECASE,
)

PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00"
    b"\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00"
    b"\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
)

_db_path: Path | None = None
_data_dir: Path | None = None
_logger: logging.Logger | None = None
_geo_reader: Any = None
_geo_reader_loaded = False


def setup(ctx):
    global _db_path, _data_dir, _logger
    _db_path = Path(ctx.db_path)
    _data_dir = Path(ctx.data_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.tracker"))
    return _init_db()


def _must_db_path() -> Path:
    if _db_path is None:
        raise HTTPException(500, "tracker is not initialized")
    return _db_path


def _must_data_dir() -> Path:
    if _data_dir is None:
        raise HTTPException(500, "tracker data dir is not initialized")
    return _data_dir


@asynccontextmanager
async def _connect():
    path = _must_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(path, timeout=30)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        yield db
    finally:
        await db.close()


async def _init_db():
    async with _connect() as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS profiles (
                visit_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                first_seen_ts REAL NOT NULL,
                last_seen_ts REAL NOT NULL,
                first_visitor_id TEXT NOT NULL DEFAULT '',
                last_visitor_id TEXT NOT NULL DEFAULT '',
                first_site_host TEXT NOT NULL DEFAULT '',
                last_site_host TEXT NOT NULL DEFAULT '',
                first_page_url TEXT NOT NULL DEFAULT '',
                last_page_url TEXT NOT NULL DEFAULT '',
                first_referrer TEXT NOT NULL DEFAULT '',
                last_referrer TEXT NOT NULL DEFAULT '',
                first_name TEXT NOT NULL DEFAULT '',
                last_name TEXT NOT NULL DEFAULT '',
                first_phone TEXT NOT NULL DEFAULT '',
                last_phone TEXT NOT NULL DEFAULT '',
                first_email TEXT NOT NULL DEFAULT '',
                last_email TEXT NOT NULL DEFAULT '',
                first_fingerprint TEXT NOT NULL DEFAULT '',
                last_fingerprint TEXT NOT NULL DEFAULT '',
                first_ip TEXT NOT NULL DEFAULT '',
                last_ip TEXT NOT NULL DEFAULT '',
                first_country TEXT NOT NULL DEFAULT '',
                last_country TEXT NOT NULL DEFAULT '',
                first_city TEXT NOT NULL DEFAULT '',
                last_city TEXT NOT NULL DEFAULT '',
                first_browser TEXT NOT NULL DEFAULT '',
                last_browser TEXT NOT NULL DEFAULT '',
                first_device TEXT NOT NULL DEFAULT '',
                last_device TEXT NOT NULL DEFAULT '',
                first_utm_source TEXT NOT NULL DEFAULT '',
                first_utm_medium TEXT NOT NULL DEFAULT '',
                first_utm_campaign TEXT NOT NULL DEFAULT '',
                first_utm_content TEXT NOT NULL DEFAULT '',
                first_utm_term TEXT NOT NULL DEFAULT '',
                last_utm_source TEXT NOT NULL DEFAULT '',
                last_utm_medium TEXT NOT NULL DEFAULT '',
                last_utm_campaign TEXT NOT NULL DEFAULT '',
                last_utm_content TEXT NOT NULL DEFAULT '',
                last_utm_term TEXT NOT NULL DEFAULT '',
                event_count INTEGER NOT NULL DEFAULT 0,
                pageview_count INTEGER NOT NULL DEFAULT 0,
                form_count INTEGER NOT NULL DEFAULT 0,
                confirmed_form_count INTEGER NOT NULL DEFAULT 0,
                attributes_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS profile_keys (
                key_type TEXT NOT NULL,
                key_value TEXT NOT NULL,
                visit_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (key_type, key_value)
            );
            CREATE INDEX IF NOT EXISTS idx_profile_keys_visit ON profile_keys(visit_id);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                created_ts REAL NOT NULL,
                visit_id TEXT NOT NULL,
                visitor_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                confirmed INTEGER NOT NULL DEFAULT 0,
                confirmation_reason TEXT NOT NULL DEFAULT '',
                site_key TEXT NOT NULL DEFAULT '',
                site_host TEXT NOT NULL DEFAULT '',
                page_url TEXT NOT NULL DEFAULT '',
                referrer TEXT NOT NULL DEFAULT '',
                page_title TEXT NOT NULL DEFAULT '',
                ip TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                user_agent TEXT NOT NULL DEFAULT '',
                browser TEXT NOT NULL DEFAULT '',
                device TEXT NOT NULL DEFAULT '',
                fingerprint TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                utm_source TEXT NOT NULL DEFAULT '',
                utm_medium TEXT NOT NULL DEFAULT '',
                utm_campaign TEXT NOT NULL DEFAULT '',
                utm_content TEXT NOT NULL DEFAULT '',
                utm_term TEXT NOT NULL DEFAULT '',
                external_ids_json TEXT NOT NULL DEFAULT '{}',
                form_fields_json TEXT NOT NULL DEFAULT '{}',
                form_meta_json TEXT NOT NULL DEFAULT '{}',
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_ts);
            CREATE INDEX IF NOT EXISTS idx_events_visit ON events(visit_id, created_ts);
            CREATE INDEX IF NOT EXISTS idx_events_host ON events(site_host, created_ts);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, created_ts);
            CREATE TABLE IF NOT EXISTS sites (
                site_key TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                allowed_hosts TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cleanup_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                retention_months INTEGER NOT NULL,
                deleted_events INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        await _set_default(db, "retention_months", str(DEFAULT_RETENTION_MONTHS))
        await _set_default(db, "customer_db_sync", "1")
        await db.commit()
    _log("info", "tracker DB initialized")


async def _set_default(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))


def _log(level: str, message: str, *args, **kwargs):
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400",
        "Cache-Control": "no-store",
    }


def _json_public(payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code, headers=_cors_headers())


async def _require_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _now() -> tuple[float, str]:
    ts = time.time()
    return ts, datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _json_loads(raw: Any, fallback: Any = None) -> Any:
    if fallback is None:
        fallback = {}
    if not raw:
        return fallback
    try:
        loaded = json.loads(str(raw))
    except Exception:
        return fallback
    return loaded


def _clean_text(value: Any, max_len: int = 1000) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text[:max_len]


def _normalize_email(value: Any) -> str:
    text = _clean_text(value, 320).lower()
    if not text or "@" not in text or "." not in text.rsplit("@", 1)[-1]:
        return ""
    return text


def _normalize_phone(value: Any) -> str:
    digits = "".join(ch for ch in _clean_text(value, 120) if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        return "7" + digits[1:]
    if len(digits) == 10:
        return "7" + digits
    return digits[:32]


def _normalize_name(value: Any) -> str:
    text = _clean_text(value, 255)
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"[<>{}\[\]`|\\/]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .,:;\"'")
    return text[:255]


def _event_type(value: Any) -> str:
    text = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", _clean_text(value, 80).lower()).strip("_")
    return text or "pageview"


def _site_key(value: Any, host: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", _clean_text(value, 80)).strip("_")
    return text or host[:80] or "default"


def _extract_ip(request: Request) -> str:
    xff = _clean_text(request.headers.get("x-forwarded-for"), 500)
    if xff:
        return xff.split(",", 1)[0].strip()
    real_ip = _clean_text(request.headers.get("x-real-ip"), 120)
    if real_ip:
        return real_ip
    return _clean_text(request.client.host if request.client else "", 120)


def _public_ip(ip: str) -> str:
    raw = _clean_text(ip, 120)
    if raw.startswith("[") and "]" in raw:
        raw = raw[1:].split("]", 1)[0]
    elif raw.count(":") == 1:
        host, port = raw.rsplit(":", 1)
        if port.isdigit():
            raw = host
    try:
        parsed = ipaddress.ip_address(raw)
    except ValueError:
        return ""
    if parsed.is_private or parsed.is_loopback or parsed.is_reserved or parsed.is_multicast:
        return ""
    return raw


def _geo_db_path() -> Path | None:
    candidates = []
    env = os.getenv("TRACKER_GEOIP_CITY_DB", "").strip()
    if env:
        candidates.append(Path(env))
    try:
        candidates.append(_must_data_dir() / "geo" / "GeoLite2-City.mmdb")
    except Exception:
        pass
    candidates.append(Path("/home/attack/develop/sobakovod/sbkvd_server/databases/GeoLite2-City.mmdb"))
    for path in candidates:
        if path.exists():
            return path
    return None


def _geo_lookup(ip: str) -> dict[str, str]:
    global _geo_reader, _geo_reader_loaded
    clean_ip = _public_ip(ip)
    if not clean_ip:
        return {"status": "private_or_invalid", "country": "", "city": ""}
    path = _geo_db_path()
    if path is None:
        return {"status": "not_configured", "country": "", "city": ""}
    if not _geo_reader_loaded:
        _geo_reader_loaded = True
        try:
            import geoip2.database

            _geo_reader = geoip2.database.Reader(str(path))
        except Exception as exc:
            _log("warning", "geoip init failed: %s", exc)
            _geo_reader = None
    if _geo_reader is None:
        return {"status": "unavailable", "country": "", "city": ""}
    try:
        resp = _geo_reader.city(clean_ip)
        return {
            "status": "ok",
            "country": resp.country.names.get("ru") or resp.country.name or "",
            "city": resp.city.names.get("ru") or resp.city.name or "",
        }
    except Exception:
        return {"status": "not_found", "country": "", "city": ""}


def _parse_browser(ua: str) -> str:
    low = ua.lower()
    if "yabrowser" in low or "ybrowser" in low:
        return "Yandex"
    if "edg/" in low or "edge/" in low:
        return "Edge"
    if "opr/" in low or "opera" in low:
        return "Opera"
    if "samsungbrowser" in low:
        return "Samsung Internet"
    if "firefox" in low or "fxios" in low:
        return "Firefox"
    if "chrome" in low or "crios" in low:
        return "Chrome"
    if "safari" in low:
        return "Safari"
    return "Other"


def _parse_device(ua: str) -> str:
    low = ua.lower()
    if "tablet" in low or "ipad" in low:
        return "tablet"
    if "mobile" in low or "android" in low or "iphone" in low or "ipod" in low:
        return "mobile"
    return "desktop"


def _safe_url(value: Any, fallback: str = "") -> str:
    text = _clean_text(value, 4000)
    if not text:
        return fallback
    return text


def _host_from_url(page_url: str, fallback: str = "") -> str:
    try:
        host = urlsplit(page_url).netloc
    except Exception:
        host = ""
    return _clean_text(host or fallback, 255)


def _sanitize_mapping(value: Any, max_items: int = 120, max_value_len: int = 4000) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for key, item in list(value.items())[:max_items]:
        clean_key = _clean_text(key, 120)
        if not clean_key or SENSITIVE_FIELD_RE.search(clean_key):
            continue
        if item is None:
            continue
        if isinstance(item, (str, int, float, bool)):
            clean_item = _clean_text(item, max_value_len) if isinstance(item, str) else item
        else:
            clean_item = _clean_text(item, max_value_len)
        if clean_item == "":
            continue
        result[clean_key] = clean_item
    return result


def _extract_utm(payload: dict[str, Any]) -> dict[str, str]:
    return {field: _clean_text(payload.get(field), 500) for field in UTM_FIELDS if _clean_text(payload.get(field), 500)}


def _extract_external_ids(payload: dict[str, Any], url_params: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    explicit = payload.get("external_ids") if isinstance(payload.get("external_ids"), dict) else {}
    for source in (url_params, explicit, payload):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            k = _clean_text(key, 80)
            if not k:
                continue
            if k in CLICK_ID_KEYS or k.lower().endswith("clid"):
                v = _clean_text(value, 500)
                if v:
                    result[k] = v
    return result


def _extract_identity(payload: dict[str, Any], form_fields: dict[str, Any]) -> dict[str, str]:
    explicit = payload.get("identity") if isinstance(payload.get("identity"), dict) else {}

    def pick(*keys: str) -> str:
        for source in (explicit, payload, form_fields):
            for key in keys:
                value = _clean_text(source.get(key) if isinstance(source, dict) else "", 500)
                if value:
                    return value
        return ""

    identity = {
        "name": _normalize_name(pick("name", "full_name", "first_name", "fio", "username")),
        "phone": _normalize_phone(pick("phone", "phone_number", "tel", "mobile")),
        "email": _normalize_email(pick("email", "mail")),
        "fingerprint": _clean_text(pick("fingerprint", "fp"), 600),
        "visit_id": _clean_text(pick("visit_id", "sbkvd_visit", "nexus_visit"), 120),
    }
    return {k: v for k, v in identity.items() if v}


def _visitor_id(payload: dict[str, Any], request: Request) -> str:
    value = _clean_text(
        payload.get("visitor_id")
        or payload.get("vid")
        or payload.get("nexus_vid")
        or request.cookies.get("nexus_vid")
        or request.cookies.get("sbkvd_vid"),
        120,
    )
    if re.fullmatch(r"[A-Za-z0-9_.:-]{8,120}", value or ""):
        return value
    return "vid_" + uuid.uuid4().hex


def _identity_candidates(identity: dict[str, str], utm: dict[str, str], external_ids: dict[str, str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen = set()

    def add(kind: str, value: str):
        clean = _clean_text(value, 600)
        if not clean:
            return
        pair = (kind, clean)
        if pair in seen:
            return
        seen.add(pair)
        pairs.append(pair)

    add("email", identity.get("email", ""))
    add("phone", identity.get("phone", ""))
    add("utm_term", utm.get("utm_term", ""))
    add("fingerprint", identity.get("fingerprint", ""))
    for key in CLICK_ID_KEYS:
        add(f"external:{key}", external_ids.get(key, ""))
    for key in sorted(external_ids):
        add(f"external:{key}", external_ids[key])
    return pairs


async def _resolve_visit_id(db: aiosqlite.Connection, visitor_id: str, explicit_visit_id: str, candidates: list[tuple[str, str]]) -> str:
    if explicit_visit_id:
        return explicit_visit_id
    cur = await db.execute("SELECT visit_id FROM events WHERE visitor_id=? ORDER BY id DESC LIMIT 1", (visitor_id,))
    row = await cur.fetchone()
    if row and row["visit_id"]:
        return str(row["visit_id"])
    for kind, value in candidates:
        cur = await db.execute("SELECT visit_id FROM profile_keys WHERE key_type=? AND key_value=? LIMIT 1", (kind, value))
        row = await cur.fetchone()
        if row and row["visit_id"]:
            return str(row["visit_id"])
    return "visit_" + uuid.uuid4().hex


async def _upsert_profile(
    db: aiosqlite.Connection,
    *,
    visit_id: str,
    visitor_id: str,
    event_type: str,
    confirmed: bool,
    site_host: str,
    page_url: str,
    referrer: str,
    ip: str,
    geo: dict[str, str],
    ua: str,
    browser: str,
    device: str,
    identity: dict[str, str],
    utm: dict[str, str],
    external_ids: dict[str, str],
    ts: float,
    now_iso: str,
) -> None:
    cur = await db.execute("SELECT visit_id, attributes_json FROM profiles WHERE visit_id=?", (visit_id,))
    existing = await cur.fetchone()
    attrs = _json_loads(existing["attributes_json"], {}) if existing else {}
    attrs["external_ids"] = {**(attrs.get("external_ids") if isinstance(attrs.get("external_ids"), dict) else {}), **external_ids}
    attrs["geo_status"] = geo.get("status", "")
    attrs["latest_user_agent"] = ua

    if existing is None:
        await db.execute(
            """
            INSERT INTO profiles(
                visit_id,created_at,updated_at,first_seen_ts,last_seen_ts,
                first_visitor_id,last_visitor_id,first_site_host,last_site_host,
                first_page_url,last_page_url,first_referrer,last_referrer,
                first_name,last_name,first_phone,last_phone,first_email,last_email,
                first_fingerprint,last_fingerprint,first_ip,last_ip,first_country,last_country,
                first_city,last_city,first_browser,last_browser,first_device,last_device,
                first_utm_source,first_utm_medium,first_utm_campaign,first_utm_content,first_utm_term,
                last_utm_source,last_utm_medium,last_utm_campaign,last_utm_content,last_utm_term,
                event_count,pageview_count,form_count,confirmed_form_count,attributes_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                visit_id,
                now_iso,
                now_iso,
                ts,
                ts,
                visitor_id,
                visitor_id,
                site_host,
                site_host,
                page_url,
                page_url,
                referrer,
                referrer,
                identity.get("name", ""),
                identity.get("name", ""),
                identity.get("phone", ""),
                identity.get("phone", ""),
                identity.get("email", ""),
                identity.get("email", ""),
                identity.get("fingerprint", ""),
                identity.get("fingerprint", ""),
                ip,
                ip,
                geo.get("country", ""),
                geo.get("country", ""),
                geo.get("city", ""),
                geo.get("city", ""),
                browser,
                browser,
                device,
                device,
                utm.get("utm_source", ""),
                utm.get("utm_medium", ""),
                utm.get("utm_campaign", ""),
                utm.get("utm_content", ""),
                utm.get("utm_term", ""),
                utm.get("utm_source", ""),
                utm.get("utm_medium", ""),
                utm.get("utm_campaign", ""),
                utm.get("utm_content", ""),
                utm.get("utm_term", ""),
                1,
                1 if event_type == "pageview" else 0,
                1 if event_type.startswith("form") else 0,
                1 if confirmed else 0,
                _json_dumps(attrs),
            ),
        )
        return

    await db.execute(
        """
        UPDATE profiles
        SET updated_at=?, last_seen_ts=?, last_visitor_id=?, last_site_host=?, last_page_url=?,
            last_referrer=?, first_name=COALESCE(NULLIF(first_name,''),?), last_name=?,
            first_phone=COALESCE(NULLIF(first_phone,''),?), last_phone=?,
            first_email=COALESCE(NULLIF(first_email,''),?), last_email=?,
            first_fingerprint=COALESCE(NULLIF(first_fingerprint,''),?), last_fingerprint=?,
            first_ip=COALESCE(NULLIF(first_ip,''),?), last_ip=?,
            first_country=COALESCE(NULLIF(first_country,''),?), last_country=?,
            first_city=COALESCE(NULLIF(first_city,''),?), last_city=?,
            first_browser=COALESCE(NULLIF(first_browser,''),?), last_browser=?,
            first_device=COALESCE(NULLIF(first_device,''),?), last_device=?,
            first_utm_source=COALESCE(NULLIF(first_utm_source,''),?), first_utm_medium=COALESCE(NULLIF(first_utm_medium,''),?),
            first_utm_campaign=COALESCE(NULLIF(first_utm_campaign,''),?), first_utm_content=COALESCE(NULLIF(first_utm_content,''),?),
            first_utm_term=COALESCE(NULLIF(first_utm_term,''),?),
            last_utm_source=?, last_utm_medium=?, last_utm_campaign=?, last_utm_content=?, last_utm_term=?,
            event_count=event_count+1,
            pageview_count=pageview_count+?,
            form_count=form_count+?,
            confirmed_form_count=confirmed_form_count+?,
            attributes_json=?
        WHERE visit_id=?
        """,
        (
            now_iso,
            ts,
            visitor_id,
            site_host,
            page_url,
            referrer,
            identity.get("name", ""),
            identity.get("name", ""),
            identity.get("phone", ""),
            identity.get("phone", ""),
            identity.get("email", ""),
            identity.get("email", ""),
            identity.get("fingerprint", ""),
            identity.get("fingerprint", ""),
            ip,
            ip,
            geo.get("country", ""),
            geo.get("country", ""),
            geo.get("city", ""),
            geo.get("city", ""),
            browser,
            browser,
            device,
            device,
            utm.get("utm_source", ""),
            utm.get("utm_medium", ""),
            utm.get("utm_campaign", ""),
            utm.get("utm_content", ""),
            utm.get("utm_term", ""),
            utm.get("utm_source", ""),
            utm.get("utm_medium", ""),
            utm.get("utm_campaign", ""),
            utm.get("utm_content", ""),
            utm.get("utm_term", ""),
            1 if event_type == "pageview" else 0,
            1 if event_type.startswith("form") else 0,
            1 if confirmed else 0,
            _json_dumps(attrs),
            visit_id,
        ),
    )


async def _upsert_profile_keys(db: aiosqlite.Connection, visit_id: str, candidates: list[tuple[str, str]], now_iso: str) -> None:
    for kind, value in candidates:
        await db.execute(
            """
            INSERT INTO profile_keys(key_type,key_value,visit_id,created_at,updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(key_type,key_value) DO UPDATE SET visit_id=excluded.visit_id, updated_at=excluded.updated_at
            """,
            (kind, value, visit_id, now_iso, now_iso),
        )


async def _read_settings(db: aiosqlite.Connection) -> dict[str, str]:
    rows = await (await db.execute("SELECT key,value FROM settings")).fetchall()
    return {str(row["key"]): str(row["value"]) for row in rows}


def _customer_db_path() -> Path | None:
    configured = os.getenv("TRACKER_CUSTOMER_DB_PATH", "").strip()
    if configured:
        return Path(configured)
    try:
        base = _must_db_path().parents[1].parent
        candidate = base / "customer-db" / "data" / "customer-db.db"
        if candidate.exists():
            return candidate
    except Exception:
        pass
    candidate = Path("/home/attack/nexus/modules/customer-db/data/customer-db.db")
    return candidate if candidate.exists() else None


async def _sync_customer_db(profile: dict[str, Any]) -> dict[str, Any]:
    path = _customer_db_path()
    if path is None:
        return {"ok": False, "reason": "customer_db_not_found"}
    fields = {
        "source": "tracker",
        "visit_id": profile.get("visit_id", ""),
        "visitor_id": profile.get("last_visitor_id", ""),
        "name": profile.get("last_name") or profile.get("first_name") or "",
        "phone": profile.get("last_phone") or profile.get("first_phone") or "",
        "email": profile.get("last_email") or profile.get("first_email") or "",
        "utm_source": profile.get("last_utm_source") or profile.get("first_utm_source") or "",
        "utm_medium": profile.get("last_utm_medium") or profile.get("first_utm_medium") or "",
        "utm_campaign": profile.get("last_utm_campaign") or profile.get("first_utm_campaign") or "",
        "utm_content": profile.get("last_utm_content") or profile.get("first_utm_content") or "",
        "utm_term": profile.get("last_utm_term") or profile.get("first_utm_term") or "",
        "geo": {
            "country": profile.get("last_country") or profile.get("first_country") or "",
            "city": profile.get("last_city") or profile.get("first_city") or "",
        },
        "device": profile.get("last_device") or profile.get("first_device") or "",
        "browser": profile.get("last_browser") or profile.get("first_browser") or "",
        "latest_page_url": profile.get("last_page_url") or "",
        "first_page_url": profile.get("first_page_url") or "",
        "first_seen_at": profile.get("created_at") or "",
        "last_seen_at": profile.get("updated_at") or "",
        "event_count": profile.get("event_count") or 0,
        "pageview_count": profile.get("pageview_count") or 0,
        "form_count": profile.get("form_count") or 0,
        "confirmed_form_count": profile.get("confirmed_form_count") or 0,
        "attributes": _json_loads(profile.get("attributes_json"), {}),
    }
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiosqlite.connect(path, timeout=30) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS _cdb_tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT DEFAULT '',
                schema_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            )
            """
        )
        await db.execute(
            """
            INSERT OR IGNORE INTO _cdb_tables(name,display_name,description,schema_json)
            VALUES('visitor_profiles','Профили трекера','Склеенные профили людей из Nexus Tracker','[]')
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS cdb_visitor_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id TEXT NOT NULL DEFAULT '',
                custom_fields TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_cdb_visitor_profiles_platform_id ON cdb_visitor_profiles(platform_id)")
        row = await (await db.execute("SELECT id FROM cdb_visitor_profiles WHERE platform_id=? ORDER BY id ASC LIMIT 1", (profile.get("visit_id", ""),))).fetchone()
        if row:
            await db.execute(
                "UPDATE cdb_visitor_profiles SET custom_fields=?, updated_at=? WHERE id=?",
                (_json_dumps(fields), now, row["id"]),
            )
            action = "updated"
        else:
            await db.execute(
                "INSERT INTO cdb_visitor_profiles(platform_id,custom_fields,created_at,updated_at) VALUES(?,?,?,?)",
                (profile.get("visit_id", ""), _json_dumps(fields), now, now),
            )
            action = "created"
        await db.commit()
    return {"ok": True, "action": action}


async def _profile_by_visit(db: aiosqlite.Connection, visit_id: str) -> dict[str, Any] | None:
    row = await (await db.execute("SELECT * FROM profiles WHERE visit_id=?", (visit_id,))).fetchone()
    return dict(row) if row else None


async def _record_event(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    if len(json.dumps(payload, ensure_ascii=False, default=str)) > MAX_PAYLOAD_BYTES:
        payload = {"event_type": "payload_too_large", "truncated": True}

    ts, now_iso = _now()
    page_url = _safe_url(payload.get("page_url") or payload.get("url") or str(request.url), str(request.url))
    site_host = _host_from_url(page_url, _clean_text(payload.get("site_host") or request.headers.get("host"), 255))
    site_key = _site_key(payload.get("site") or payload.get("site_key"), site_host)
    event_type = _event_type(payload.get("event_type") or payload.get("type"))
    confirmed = bool(payload.get("confirmed")) or event_type in {"form_confirmed", "form_submit_success", "bizon_auth_success"}
    confirmation_reason = _clean_text(payload.get("confirmation_reason") or payload.get("auth_reason"), 120)
    visitor_id = _visitor_id(payload, request)
    url_params = _sanitize_mapping(payload.get("url_params") if isinstance(payload.get("url_params"), dict) else {}, 200, 2000)
    form_fields = _sanitize_mapping(payload.get("form_fields") if isinstance(payload.get("form_fields"), dict) else {}, 160, 4000)
    form_meta = _sanitize_mapping(payload.get("form_meta") if isinstance(payload.get("form_meta"), dict) else {}, 60, 1000)
    utm = _extract_utm({**url_params, **payload})
    external_ids = _extract_external_ids(payload, url_params)
    identity = _extract_identity(payload, form_fields)
    candidates = _identity_candidates(identity, utm, external_ids)
    ip = _extract_ip(request)
    geo = _geo_lookup(ip)
    ua = _clean_text(request.headers.get("user-agent") or payload.get("user_agent"), 4000)
    browser = _clean_text(payload.get("browser"), 80) or _parse_browser(ua)
    device = _clean_text(payload.get("device"), 80) or _parse_device(ua)
    referrer = _clean_text(payload.get("referrer"), 4000)
    page_title = _clean_text(payload.get("page_title"), 500)
    safe_payload = _sanitize_payload(payload)

    async with _connect() as db:
        visit_id = await _resolve_visit_id(db, visitor_id, identity.get("visit_id", ""), candidates)
        await _upsert_profile(
            db,
            visit_id=visit_id,
            visitor_id=visitor_id,
            event_type=event_type,
            confirmed=confirmed,
            site_host=site_host,
            page_url=page_url,
            referrer=referrer,
            ip=ip,
            geo=geo,
            ua=ua,
            browser=browser,
            device=device,
            identity=identity,
            utm=utm,
            external_ids=external_ids,
            ts=ts,
            now_iso=now_iso,
        )
        await _upsert_profile_keys(db, visit_id, candidates, now_iso)
        await db.execute(
            """
            INSERT INTO events(
                created_at,created_ts,visit_id,visitor_id,event_type,confirmed,confirmation_reason,
                site_key,site_host,page_url,referrer,page_title,ip,country,city,user_agent,browser,device,
                fingerprint,name,phone,email,utm_source,utm_medium,utm_campaign,utm_content,utm_term,
                external_ids_json,form_fields_json,form_meta_json,payload_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                now_iso,
                ts,
                visit_id,
                visitor_id,
                event_type,
                1 if confirmed else 0,
                confirmation_reason,
                site_key,
                site_host,
                page_url,
                referrer,
                page_title,
                ip,
                geo.get("country", ""),
                geo.get("city", ""),
                ua,
                browser,
                device,
                identity.get("fingerprint", ""),
                identity.get("name", ""),
                identity.get("phone", ""),
                identity.get("email", ""),
                utm.get("utm_source", ""),
                utm.get("utm_medium", ""),
                utm.get("utm_campaign", ""),
                utm.get("utm_content", ""),
                utm.get("utm_term", ""),
                _json_dumps(external_ids),
                _json_dumps(form_fields),
                _json_dumps(form_meta),
                _json_dumps(safe_payload),
            ),
        )
        await db.execute(
            """
            INSERT INTO sites(site_key,title,allowed_hosts,enabled,created_at,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(site_key) DO UPDATE SET updated_at=excluded.updated_at
            """,
            (site_key, site_key, site_host, 1, now_iso, now_iso),
        )
        settings = await _read_settings(db)
        profile = await _profile_by_visit(db, visit_id)
        await db.commit()

    sync_result = {"ok": False, "reason": "disabled"}
    if settings.get("customer_db_sync", "1") == "1" and profile:
        try:
            sync_result = await _sync_customer_db(profile)
        except Exception as exc:
            sync_result = {"ok": False, "reason": str(exc)}
            _log("warning", "customer-db sync failed: %s", exc)

    return {
        "ok": True,
        "visitor_id": visitor_id,
        "visit_id": visit_id,
        "sbkvd_visit": visit_id,
        "geo": geo,
        "customer_db_sync": sync_result,
    }


def _sanitize_payload(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return _clean_text(value, 1000)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _clean_text(value, 4000)
    if isinstance(value, dict):
        result = {}
        for key, item in list(value.items())[:180]:
            clean_key = _clean_text(key, 120)
            if not clean_key or SENSITIVE_FIELD_RE.search(clean_key):
                continue
            clean_item = _sanitize_payload(item, depth + 1)
            if clean_item in ("", None):
                continue
            result[clean_key] = clean_item
        return result
    if isinstance(value, (list, tuple)):
        return [_sanitize_payload(item, depth + 1) for item in list(value)[:80]]
    return _clean_text(value, 1000)


async def _request_payload(request: Request) -> dict[str, Any]:
    payload = dict(request.query_params)
    if request.method.upper() != "POST":
        return payload
    try:
        body = await request.body()
    except Exception:
        return payload
    if not body:
        return payload
    content_type = (request.headers.get("content-type") or "").lower()
    parsed = None
    try:
        if "application/json" in content_type or "text/plain" in content_type:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
        elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            form = await request.form()
            parsed = dict(form)
        else:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        payload.update(parsed)
    return payload


@router.options("/collect")
async def collect_options():
    return JSONResponse({}, headers=_cors_headers())


@router.post("/collect")
@router.get("/collect")
async def collect(request: Request):
    try:
        payload = await _request_payload(request)
        result = await _record_event(request, payload)
        response = _json_public(result)
        response.set_cookie("nexus_vid", result["visitor_id"], max_age=60 * 60 * 24 * 730, samesite="lax", httponly=False)
        response.set_cookie("nexus_visit", result["visit_id"], max_age=60 * 60 * 24 * 730, samesite="lax", httponly=False)
        return response
    except Exception as exc:
        _log("error", "collect failed: %s", exc, exc_info=True)
        return _json_public({"ok": False, "error": "collect_failed"})


@router.get("/pixel.gif")
async def pixel(request: Request):
    try:
        payload = dict(request.query_params)
        payload.setdefault("event_type", "pageview")
        await _record_event(request, payload)
    except Exception as exc:
        _log("warning", "pixel failed: %s", exc)
    return Response(PIXEL_GIF, media_type="image/gif", headers=_cors_headers())


@router.get("/script.js")
async def script_js():
    return PlainTextResponse(TRACKER_SCRIPT, media_type="application/javascript; charset=utf-8", headers={**_cors_headers(), "Cache-Control": "public, max-age=300"})


@router.get("/snippet")
async def snippet(
    request: Request,
    site: str = "sobakovod",
    consent: str = "off",
    banner: str = "0",
    auto_consent: str = "",
    policy_url: str = "",
):
    try:
        url = str(request.url_for("script_js"))
    except Exception:
        base = str(request.base_url).rstrip("/")
        root_path = _clean_text(request.scope.get("root_path", ""), 80).rstrip("/")
        prefix = "" if root_path and base.endswith(root_path) else root_path
        url = f"{base}{prefix}/tracker/api/script.js"
    attrs = [
        f'src="{url}"',
        "async",
        f'data-site="{_clean_text(site, 80)}"',
    ]
    consent_clean = _clean_text(consent, 32).lower()
    if consent_clean and consent_clean != "off":
        attrs.append(f'data-consent="{consent_clean}"')
    if str(banner).lower() in {"1", "true", "yes", "on"}:
        attrs.append('data-banner="1"')
    auto_clean = _clean_text(auto_consent, 32).lower()
    if auto_clean:
        attrs.append(f'data-auto-consent="{auto_clean}"')
    policy_clean = _clean_text(policy_url, 500)
    if policy_clean:
        attrs.append(f'data-policy-url="{policy_clean}"')
    html = f'<script {" ".join(attrs)}></script>'
    return PlainTextResponse(html, media_type="text/plain; charset=utf-8")


@router.get("/settings")
async def get_settings(request: Request):
    await _require_user(request)
    async with _connect() as db:
        settings = await _read_settings(db)
    geo_path = _geo_db_path()
    return {
        "retention_months": int(settings.get("retention_months") or DEFAULT_RETENTION_MONTHS),
        "customer_db_sync": settings.get("customer_db_sync", "1") == "1",
        "geo": {
            "configured": bool(geo_path),
            "path": str(geo_path or ""),
            "dependency_loaded": _geo_reader_loaded,
        },
        "customer_db_path": str(_customer_db_path() or ""),
    }


@router.post("/settings")
async def post_settings(request: Request):
    await _require_user(request)
    data = await request.json()
    retention = max(1, min(int(data.get("retention_months") or DEFAULT_RETENTION_MONTHS), 60))
    customer_db_sync = "1" if data.get("customer_db_sync", True) else "0"
    async with _connect() as db:
        await db.execute("INSERT INTO settings(key,value) VALUES('retention_months',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(retention),))
        await db.execute("INSERT INTO settings(key,value) VALUES('customer_db_sync',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (customer_db_sync,))
        await db.commit()
    return {"ok": True, "retention_months": retention, "customer_db_sync": customer_db_sync == "1"}


@router.get("/stats")
async def stats(request: Request):
    await _require_user(request)
    async with _connect() as db:
        row = await (await db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM profiles) AS profiles,
                (SELECT COUNT(*) FROM events) AS events,
                (SELECT COUNT(*) FROM events WHERE event_type='pageview') AS pageviews,
                (SELECT COUNT(*) FROM events WHERE confirmed=1) AS confirmed,
                (SELECT COUNT(DISTINCT site_host) FROM events WHERE site_host!='') AS sites
            """
        )).fetchone()
        recent = await (await db.execute(
            """
            SELECT date(created_at) AS day, COUNT(*) AS count
            FROM events
            WHERE created_ts >= ?
            GROUP BY day
            ORDER BY day
            """,
            (time.time() - 14 * 86400,),
        )).fetchall()
        top_sources = await (await db.execute(
            "SELECT COALESCE(NULLIF(utm_source,''),'(none)') AS source, COUNT(*) AS count FROM events GROUP BY source ORDER BY count DESC LIMIT 10"
        )).fetchall()
    return {
        "profiles": int(row["profiles"] or 0),
        "events": int(row["events"] or 0),
        "pageviews": int(row["pageviews"] or 0),
        "confirmed": int(row["confirmed"] or 0),
        "sites": int(row["sites"] or 0),
        "daily": [dict(item) for item in recent],
        "top_sources": [dict(item) for item in top_sources],
    }


@router.get("/profiles")
async def profiles(request: Request, q: str = "", limit: int = 80, offset: int = 0):
    await _require_user(request)
    limit = max(1, min(int(limit), MAX_PROFILE_LIMIT))
    offset = max(0, int(offset))
    query = _clean_text(q, 500)
    where = ""
    params: list[Any] = []
    if query:
        like = f"%{query}%"
        where = """
        WHERE visit_id=? OR last_visitor_id=? OR last_email=? OR first_email=? OR last_phone=? OR first_phone=?
           OR last_name LIKE ? OR first_name LIKE ? OR last_utm_term=? OR first_utm_term=?
           OR last_page_url LIKE ? OR first_page_url LIKE ?
        """
        params = [query, query, query.lower(), query.lower(), _normalize_phone(query), _normalize_phone(query), like, like, query, query, like, like]
    async with _connect() as db:
        total_row = await (await db.execute(f"SELECT COUNT(*) AS c FROM profiles {where}", params)).fetchone()
        rows = await (await db.execute(
            f"""
            SELECT *
            FROM profiles
            {where}
            ORDER BY last_seen_ts DESC
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        )).fetchall()
    return {"total": int(total_row["c"] or 0), "items": [_profile_payload(dict(row)) for row in rows]}


@router.get("/profiles/{visit_id}")
async def profile_detail(visit_id: str, request: Request):
    await _require_user(request)
    clean = _clean_text(visit_id, 120)
    async with _connect() as db:
        profile = await _profile_by_visit(db, clean)
        if not profile:
            raise HTTPException(404, "profile not found")
        events = await (await db.execute(
            "SELECT * FROM events WHERE visit_id=? ORDER BY created_ts DESC LIMIT ?",
            (clean, MAX_RAW_EVENTS),
        )).fetchall()
        keys = await (await db.execute(
            "SELECT key_type,key_value,updated_at FROM profile_keys WHERE visit_id=? ORDER BY updated_at DESC",
            (clean,),
        )).fetchall()
    return {"profile": _profile_payload(profile), "events": [_event_payload(dict(row)) for row in events], "keys": [dict(row) for row in keys]}


@router.get("/events")
async def events(
    request: Request,
    visit_id: str = "",
    site_host: str = "",
    event_type: str = "",
    limit: int = 100,
    offset: int = 0,
):
    await _require_user(request)
    limit = max(1, min(int(limit), MAX_RAW_EVENTS))
    offset = max(0, int(offset))
    clauses = []
    params: list[Any] = []
    if visit_id:
        clauses.append("visit_id=?")
        params.append(_clean_text(visit_id, 120))
    if site_host:
        clauses.append("site_host=?")
        params.append(_clean_text(site_host, 255))
    if event_type:
        clauses.append("event_type=?")
        params.append(_event_type(event_type))
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with _connect() as db:
        total = await (await db.execute(f"SELECT COUNT(*) AS c FROM events {where}", params)).fetchone()
        rows = await (await db.execute(
            f"SELECT * FROM events {where} ORDER BY created_ts DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )).fetchall()
    return {"total": int(total["c"] or 0), "items": [_event_payload(dict(row)) for row in rows]}


@router.get("/forms")
async def forms(request: Request, limit: int = 100, offset: int = 0):
    await _require_user(request)
    limit = max(1, min(int(limit), MAX_RAW_EVENTS))
    offset = max(0, int(offset))
    async with _connect() as db:
        total = await (await db.execute("SELECT COUNT(*) AS c FROM events WHERE form_fields_json!='{}' OR confirmed=1")).fetchone()
        rows = await (await db.execute(
            """
            SELECT * FROM events
            WHERE form_fields_json!='{}' OR confirmed=1
            ORDER BY created_ts DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )).fetchall()
    return {"total": int(total["c"] or 0), "items": [_event_payload(dict(row)) for row in rows]}


@router.get("/sites")
async def sites(request: Request):
    await _require_user(request)
    async with _connect() as db:
        rows = await (await db.execute(
            """
            SELECT site_host, COUNT(*) AS events, COUNT(DISTINCT visit_id) AS people,
                   MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
            FROM events
            WHERE site_host!=''
            GROUP BY site_host
            ORDER BY last_seen DESC
            """
        )).fetchall()
    return {"items": [dict(row) for row in rows]}


@router.post("/cleanup")
async def cleanup(request: Request):
    await _require_user(request)
    async with _connect() as db:
        settings = await _read_settings(db)
        retention = max(1, min(int(settings.get("retention_months") or DEFAULT_RETENTION_MONTHS), 60))
        cutoff = time.time() - retention * 31 * 86400
        cur = await db.execute("DELETE FROM events WHERE created_ts < ?", (cutoff,))
        deleted = int(cur.rowcount or 0)
        _, now_iso = _now()
        await db.execute(
            "INSERT INTO cleanup_runs(created_at,retention_months,deleted_events) VALUES(?,?,?)",
            (now_iso, retention, deleted),
        )
        await db.commit()
    return {"ok": True, "deleted_events": deleted, "retention_months": retention}


def _profile_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["attributes"] = _json_loads(payload.pop("attributes_json", "{}"), {})
    return payload


def _event_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in ("external_ids_json", "form_fields_json", "form_meta_json", "payload_json"):
        payload[key.replace("_json", "")] = _json_loads(payload.pop(key, "{}"), {})
    return payload


TRACKER_SCRIPT = r"""
(function () {
  "use strict";
  if (window.__NexusTrackerInstalled) return;
  window.__NexusTrackerInstalled = true;

  var COOKIE_VISITOR = "nexus_vid";
  var COOKIE_VISIT = "nexus_visit";
  var STORAGE_KEY = "nexus_tracker_state_v1";
  var PENDING_KEY = "nexus_tracker_pending_form_v1";
  var SENT_KEY = "nexus_tracker_sent_v1";
  var UTM_FIELDS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"];
  var CLICK_KEYS = ["yclid", "gclid", "fbclid", "ttclid", "msclkid", "roistat", "_openstat", "_ym_uid"];
  var SUCCESS_SELECTORS = [
    ".js-successbox", ".t-form__successbox", ".t-form-success-popup", ".alert-success",
    ".success-message", ".lt-form-success", ".gc-form-success", ".builder-success",
    ".payform-success", ".thank-you", ".thankyou"
  ];
  var currentScript = document.currentScript || (function () {
    var scripts = document.getElementsByTagName("script");
    return scripts[scripts.length - 1] || null;
  })();

  function clean(value) { return String(value == null ? "" : value).trim(); }
  function safeJson(raw) { try { return JSON.parse(raw || "{}") || {}; } catch (_) { return {}; } }
  function readStorage(key) { try { return safeJson(sessionStorage.getItem(key)); } catch (_) { return {}; } }
  function writeStorage(key, value) { try { sessionStorage.setItem(key, JSON.stringify(value || {})); } catch (_) {} }
  function readLocal() { try { return safeJson(localStorage.getItem(STORAGE_KEY)); } catch (_) { return {}; } }
  function writeLocal(value) { try { localStorage.setItem(STORAGE_KEY, JSON.stringify(value || {})); } catch (_) {} }
  function cookie(name) {
    var match = document.cookie.match(new RegExp("(?:^|; )" + name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "=([^;]*)"));
    return match ? decodeURIComponent(match[1]) : "";
  }
  function setCookie(name, value) {
    try {
      var expires = new Date(Date.now() + 730 * 86400000).toUTCString();
      document.cookie = name + "=" + encodeURIComponent(value) + "; expires=" + expires + "; path=/; SameSite=Lax";
    } catch (_) {}
  }
  function randomId(prefix) {
    if (window.crypto && crypto.randomUUID) return prefix + crypto.randomUUID().replace(/-/g, "");
    return prefix + Math.random().toString(16).slice(2) + Date.now().toString(16);
  }
  function getUrl(raw) { try { return new URL(raw || location.href, location.href); } catch (_) { return null; } }
  function params(url) {
    var result = {};
    if (!url) return result;
    url.searchParams.forEach(function (value, key) { if (!(key in result)) result[key] = value; });
    return result;
  }
  function externalIds(urlParams) {
    var result = {};
    Object.keys(urlParams || {}).forEach(function (key) {
      if (CLICK_KEYS.indexOf(key) >= 0 || /clid$/i.test(key)) result[key] = urlParams[key];
    });
    return result;
  }
  function hash32(input) {
    var h = 5381, s = String(input || "");
    for (var i = 0; i < s.length; i += 1) h = ((h << 5) + h) ^ s.charCodeAt(i);
    return (h >>> 0).toString(16);
  }
  function fingerprint() {
    var tz = "";
    try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch (_) {}
    var screenInfo = window.screen ? [screen.width, screen.height, screen.colorDepth || ""].join("x") : "";
    return "fp_" + hash32([
      navigator.userAgent || "", navigator.language || "", navigator.platform || "",
      tz, screenInfo, navigator.hardwareConcurrency || "", navigator.deviceMemory || "",
      navigator.maxTouchPoints || "", navigator.cookieEnabled ? "1" : "0", location.hostname || ""
    ].join("|"));
  }
  function context() {
    var tz = "";
    try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch (_) {}
    return {
      language: navigator.language || "",
      timezone: tz,
      screen_width: window.screen && screen.width || 0,
      screen_height: window.screen && screen.height || 0,
      viewport_width: window.innerWidth || 0,
      viewport_height: window.innerHeight || 0,
      hardware_concurrency: navigator.hardwareConcurrency || "",
      device_memory: navigator.deviceMemory || "",
      touch_points: navigator.maxTouchPoints || 0,
      cookie_enabled: !!navigator.cookieEnabled
    };
  }
  function config() {
    var src = currentScript && currentScript.src ? getUrl(currentScript.src) : null;
    var endpoint = src ? new URL("collect", src).toString() : (location.origin + "/nexus/tracker/api/collect");
    var ds = currentScript && currentScript.dataset || {};
    var consent = clean(ds.consent || ds.trackingConsent || "off").toLowerCase();
    if (consent === "1" || consent === "true" || consent === "yes") consent = "required";
    if (consent !== "required") consent = "off";
    var autoConsent = clean(ds.autoConsent || ds.consentAuto || "").toLowerCase();
    var defaultConsentText = autoConsent === "continue" || autoConsent === "time"
      ? "На сайте используются файлы cookie и сервисы аналитики. Продолжая пользоваться сайтом, вы соглашаетесь с"
      : "На сайте используются файлы cookie и сервисы аналитики. Нажимая кнопку, вы соглашаетесь с";
    var defaultConsentShortText = autoConsent === "continue" || autoConsent === "time"
      ? "Используем cookie и аналитику. Продолжая пользоваться сайтом, вы соглашаетесь с"
      : "Используем cookie и аналитику. Нажимая кнопку, вы соглашаетесь с";
    return {
      endpoint: ds.endpoint || endpoint,
      site: ds.site || location.hostname || "default",
      confirmSuccessUrl: ds.confirmSuccessUrl !== "0",
      consent: consent,
      banner: consent === "required" && ds.banner !== "0" && ds.consentBanner !== "0",
      policyUrl: ds.policyUrl || ds.privacyUrl || "https://sobakovod.pro/popd",
      policyText: ds.policyText || "Политикой обработки персональных данных",
      policyShortText: ds.policyShortText || "Политикой обработки персональных данных",
      consentText: ds.consentText || defaultConsentText,
      consentShortText: ds.consentShortText || defaultConsentShortText,
      acceptText: ds.acceptText || "Принять",
      autoConsent: autoConsent,
      autoDelay: Math.max(0, Number(ds.autoConsentDelay || ds.consentAutoDelay || 3000) || 3000),
      autoScroll: Math.max(40, Number(ds.autoConsentScroll || 120) || 120),
      formConsent: ds.formConsent !== "0"
    };
  }
  function removeLocal(key) { try { localStorage.removeItem(key); } catch (_) {} }
  function removeSession(key) { try { sessionStorage.removeItem(key); } catch (_) {} }
  function clearCookie(name) {
    try { document.cookie = name + "=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/; SameSite=Lax"; } catch (_) {}
  }
  var CONSENT_COOKIE = "nexus_tracking_consent";
  var CONSENT_KEY = "nexus_tracker_consent_v1";
  function readConsent() {
    var raw = cookie(CONSENT_COOKIE);
    if (raw === "1" || raw === "yes" || raw === "true") return true;
    try {
      var stored = localStorage.getItem(CONSENT_KEY);
      if (stored === "1") return true;
      var data = safeJson(stored);
      return data && data.granted === true;
    } catch (_) {
      return false;
    }
  }
  function writeConsent(reason) {
    var data = { granted: true, reason: reason || "manual", at: new Date().toISOString() };
    try {
      var expires = new Date(Date.now() + 730 * 86400000).toUTCString();
      document.cookie = CONSENT_COOKIE + "=1; expires=" + expires + "; path=/; SameSite=Lax";
    } catch (_) {}
    try { localStorage.setItem(CONSENT_KEY, JSON.stringify(data)); } catch (_) {}
  }
  function needsConsent() { return cfg.consent === "required" && !readConsent(); }
  function state() {
    var s = readLocal();
    s.visitorId = s.visitorId || cookie(COOKIE_VISITOR) || randomId("vid_");
    s.visitId = s.visitId || cookie(COOKIE_VISIT) || "";
    setCookie(COOKIE_VISITOR, s.visitorId);
    if (s.visitId) setCookie(COOKIE_VISIT, s.visitId);
    writeLocal(s);
    return s;
  }
  function syncState(s, response) {
    if (!s) return;
    if (!response || typeof response !== "object") return;
    if (response.visitor_id) s.visitorId = String(response.visitor_id);
    if (response.visit_id || response.sbkvd_visit) s.visitId = String(response.visit_id || response.sbkvd_visit);
    setCookie(COOKIE_VISITOR, s.visitorId);
    if (s.visitId) setCookie(COOKIE_VISIT, s.visitId);
    writeLocal(s);
  }
  function post(url, payload, done) {
    var body = JSON.stringify(payload || {});
    function finish(data) { if (typeof done === "function") done(data || null); }
    try {
      fetch(url, {
        method: "POST",
        mode: "cors",
        credentials: "omit",
        keepalive: true,
        headers: { "Content-Type": "application/json" },
        body: body
      }).then(function (r) { return r && r.ok ? r.json().catch(function () { return null; }) : null; })
        .then(finish)
        .catch(function () {
          try {
            if (navigator.sendBeacon) navigator.sendBeacon(url, new Blob([body], { type: "text/plain;charset=UTF-8" }));
          } catch (_) {}
          finish(null);
        });
    } catch (_) {
      finish(null);
    }
  }
  function basePayload(eventType, extra) {
    var url = getUrl(location.href);
    var ps = params(url);
    var currentState = appState || {};
    var payload = {
      event_type: eventType || "pageview",
      visitor_id: currentState.visitorId || "",
      visit_id: currentState.visitId || "",
      site: cfg.site,
      page_url: location.href,
      page_title: document.title || "",
      site_host: location.host || "",
      referrer: document.referrer || "",
      url_params: ps,
      external_ids: externalIds(ps),
      fingerprint: fingerprint(),
      client_context: context()
    };
    UTM_FIELDS.forEach(function (field) { if (ps[field]) payload[field] = ps[field]; });
    if (extra && typeof extra === "object") {
      Object.keys(extra).forEach(function (key) { payload[key] = extra[key]; });
    }
    return payload;
  }
  function sensitive(input) {
    var meta = [input.type || "", input.name || "", input.id || "", input.autocomplete || ""].join(" ").toLowerCase();
    return /password|passwd|pwd|token|secret|captcha|otp|sms_code|smscode|verification|confirm_code|csrf|card|cvv|cvc/.test(meta);
  }
  function fieldKey(input, index) {
    var raw = clean(input.name || input.id || input.placeholder || "");
    var key = raw.toLowerCase().replace(/[^a-z0-9а-яё_]+/gi, "_").replace(/^_+|_+$/g, "");
    if (!key) key = "field_" + index;
    if (/^\d/.test(key)) key = "field_" + key;
    return key.slice(0, 80);
  }
  function normalizePhone(value) {
    var d = clean(value).replace(/\D+/g, "");
    if (d.length === 11 && d.charAt(0) === "8") return "7" + d.slice(1);
    if (d.length === 10) return "7" + d;
    return d;
  }
  function normalizeEmail(value) { return clean(value).toLowerCase(); }
  function normalizeName(value) {
    var text = clean(value);
    if (text.normalize) text = text.normalize("NFKC");
    return text.replace(/[\u0000-\u001f\u007f]+/g, " ").replace(/[<>{}\[\]`|\\/]+/g, " ").replace(/\s+/g, " ").replace(/^[\s.,:;"']+|[\s.,:;"']+$/g, "").slice(0, 255);
  }
  function captureFields(root) {
    var node = root && root.querySelectorAll ? root : document;
    var fields = node.querySelectorAll("input, textarea, select");
    var result = {};
    for (var i = 0; i < fields.length; i += 1) {
      var input = fields[i], type = String(input.type || "").toLowerCase();
      if (sensitive(input) || type === "password" || type === "hidden" || type === "submit" || type === "button" || type === "file") continue;
      if ((type === "checkbox" || type === "radio") && !input.checked) continue;
      if (input.tagName === "SELECT" && input.multiple) continue;
      var value = type === "checkbox" ? (input.value || "true") : clean(input.value);
      if (!value) continue;
      var key = fieldKey(input, i);
      if (/email|mail/i.test(key)) value = normalizeEmail(value);
      else if (/phone|tel|mobile|тел/i.test(key)) value = normalizePhone(value);
      else if (/name|full_name|fio|имя|фио/i.test(key)) value = normalizeName(value);
      if (value) result[key] = value;
    }
    return result;
  }
  function identity(fields) {
    var out = {};
    Object.keys(fields || {}).some(function (key) {
      if (/email|mail/i.test(key)) { out.email = normalizeEmail(fields[key]); return true; }
      return false;
    });
    Object.keys(fields || {}).some(function (key) {
      if (/phone|tel|mobile|тел/i.test(key)) { out.phone = normalizePhone(fields[key]); return true; }
      return false;
    });
    Object.keys(fields || {}).some(function (key) {
      if (/name|full_name|fio|имя|фио/i.test(key)) { out.name = normalizeName(fields[key]); return true; }
      return false;
    });
    return out;
  }
  function formMeta(form) {
    if (!form || !form.getAttribute) return {};
    return {
      form_id: clean(form.id),
      form_name: clean(form.getAttribute("name")),
      form_action: clean(form.getAttribute("action")),
      form_method: clean(form.getAttribute("method") || "get").toLowerCase(),
      form_class: clean(form.className),
      form_data_order: clean(form.getAttribute("data-formorder") || form.getAttribute("data-order"))
    };
  }
  function labelText(input) {
    var parts = [input.name || "", input.id || "", input.value || ""];
    try {
      if (input.id) {
        var label = document.querySelector("label[for='" + String(input.id).replace(/'/g, "\\'") + "']");
        if (label) parts.push(label.textContent || "");
      }
      var parentLabel = input.closest && input.closest("label");
      if (parentLabel) parts.push(parentLabel.textContent || "");
    } catch (_) {}
    return parts.join(" ").toLowerCase();
  }
  function hasFormConsent(form) {
    if (!form || !form.querySelectorAll) return false;
    var boxes = form.querySelectorAll("input[type='checkbox']");
    for (var i = 0; i < boxes.length; i += 1) {
      var box = boxes[i];
      if (!box.checked) continue;
      if (/персон|пдн|personal|privacy|policy|политик|соглас|consent|обработк/.test(labelText(box))) return true;
    }
    return false;
  }
  function formPayload(form, type, reason) {
    var fields = captureFields(form || document);
    return basePayload(type || "form_submit", {
      form_fields: fields,
      form_meta: formMeta(form),
      identity: identity(fields),
      confirmation_reason: reason || ""
    });
  }
  function signature(payload) {
    var meta = payload.form_meta || {};
    return [payload.event_type, payload.page_url, meta.form_id, meta.form_name, meta.form_action, payload.identity && payload.identity.email, payload.identity && payload.identity.phone].join("|");
  }
  function sent(sig) {
    var map = readStorage(SENT_KEY), ts = Number(map[sig] || 0);
    return !!(ts && Date.now() - ts < 120000);
  }
  function mark(sig) {
    var map = readStorage(SENT_KEY);
    map[sig] = Date.now();
    writeStorage(SENT_KEY, map);
  }
  function remember(form, reason) {
    if (!ensureConsent("form_" + (reason || "pending"), form)) return;
    var payload = formPayload(form, "form_submit", reason || "pending");
    payload.pending_at = Date.now();
    payload.pending_signature = signature(payload);
    writeStorage(PENDING_KEY, payload);
  }
  function visible(node) {
    if (!node) return false;
    if (node.offsetParent !== null) return true;
    var style = window.getComputedStyle ? getComputedStyle(node) : null;
    return !!(style && style.display !== "none" && style.visibility !== "hidden" && style.opacity !== "0");
  }
  function hasSuccess() {
    for (var i = 0; i < SUCCESS_SELECTORS.length; i += 1) {
      var node = document.querySelector(SUCCESS_SELECTORS[i]);
      if (visible(node)) return true;
    }
    return false;
  }
  function successUrl() { return /thanks|thankyou|thank-you|success|oplata|payment|pay|spasibo/i.test(location.href); }
  function sendPending(reason) {
    if (needsConsent()) return false;
    var pending = readStorage(PENDING_KEY);
    if (!pending || !pending.pending_at) return false;
    if (Date.now() - Number(pending.pending_at) > 30 * 60 * 1000) { writeStorage(PENDING_KEY, {}); return false; }
    var sig = String(pending.pending_signature || signature(pending)) + "|confirmed";
    if (sent(sig)) return true;
    pending.event_type = "form_submit_success";
    pending.confirmed = true;
    pending.confirmation_reason = reason || "success";
    post(cfg.endpoint, pending, function (data) { syncState(appState, data); });
    mark(sig);
    writeStorage(PENDING_KEY, {});
    return true;
  }
  function checkSuccess() {
    if ((cfg.confirmSuccessUrl && successUrl()) || hasSuccess()) sendPending(successUrl() ? "success_url" : "success_node");
  }
  function styleBanner() {
    if (document.getElementById("nexus-tracker-consent-style")) return;
    var style = document.createElement("style");
    style.id = "nexus-tracker-consent-style";
    style.textContent = [
      "#nexus-tracker-consent{box-sizing:border-box;position:fixed;left:50%;bottom:max(10px,env(safe-area-inset-bottom));transform:translateX(-50%);z-index:2147483000;width:min(860px,calc(100vw - 22px));min-height:58px;display:flex;align-items:center;gap:14px;padding:12px 14px;background:rgba(255,255,255,.98);border:1px solid rgba(90,172,236,.38);box-shadow:0 14px 38px rgba(0,0,0,.22);border-radius:10px;color:#3f3f3f;font:12px/1.35 Circe,Arial,sans-serif;overflow:hidden;letter-spacing:0;animation:nexusTrackerConsentIn .16s ease-out both}",
      "#nexus-tracker-consent *{box-sizing:border-box;letter-spacing:0}",
      "#nexus-tracker-consent .nxtc-text{min-width:0;flex:1;display:block;color:#3f3f3f}",
      "#nexus-tracker-consent .nxtc-message{display:inline}",
      "#nexus-tracker-consent .nxtc-message-short{display:none}",
      "#nexus-tracker-consent a{display:inline;color:#348fd6;text-decoration:none;border-bottom:1px solid rgba(52,143,214,.45);font-weight:700}",
      "#nexus-tracker-consent button{flex:0 0 auto;min-width:92px;height:38px;border:0;border-radius:8px;background:#5aacec;color:#fff;padding:0 16px;font:700 13px/1 Circe,Arial,sans-serif;cursor:pointer;white-space:nowrap;box-shadow:0 5px 14px rgba(90,172,236,.28)}",
      "#nexus-tracker-consent button:focus{outline:2px solid rgba(52,143,214,.4);outline-offset:2px}",
      "@keyframes nexusTrackerConsentIn{from{opacity:0}to{opacity:1}}",
      "@media(max-width:560px){#nexus-tracker-consent{width:calc(100vw - 16px);bottom:max(8px,env(safe-area-inset-bottom));min-height:72px;gap:10px;padding:11px 10px 11px 12px;border-radius:10px;font-size:11px;line-height:1.28}#nexus-tracker-consent .nxtc-message-full{display:none}#nexus-tracker-consent .nxtc-message-short{display:inline}#nexus-tracker-consent button{min-width:78px;height:38px;border-radius:8px;padding:0 12px;font-size:12px}}",
      "@media(max-width:340px){#nexus-tracker-consent{width:calc(100vw - 12px);gap:8px;padding-left:10px;font-size:10.5px}#nexus-tracker-consent button{min-width:70px;padding:0 10px}}"
    ].join("");
    (document.head || document.documentElement).appendChild(style);
  }
  function removeBanner() {
    var banner = document.getElementById("nexus-tracker-consent");
    if (banner && banner.parentNode) banner.parentNode.removeChild(banner);
  }
  function showBanner() {
    if (!cfg.banner || !needsConsent()) return;
    if (!document.body) { setTimeout(showBanner, 50); return; }
    if (document.getElementById("nexus-tracker-consent")) return;
    styleBanner();
    var banner = document.createElement("div");
    banner.id = "nexus-tracker-consent";
    banner.setAttribute("role", "region");
    banner.setAttribute("aria-label", "Согласие на аналитику");
    var textNode = document.createElement("div");
    textNode.className = "nxtc-text";
    var message = document.createElement("span");
    message.className = "nxtc-message nxtc-message-full";
    message.textContent = cfg.consentText + " ";
    var shortMessage = document.createElement("span");
    shortMessage.className = "nxtc-message nxtc-message-short";
    shortMessage.textContent = cfg.consentShortText + " ";
    var link = document.createElement("a");
    link.href = cfg.policyUrl || "/privacy";
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = cfg.policyText;
    function syncPolicyText() {
      var compact = false;
      try { compact = window.matchMedia && window.matchMedia("(max-width: 560px)").matches; } catch (_) {}
      link.textContent = compact && cfg.policyShortText ? cfg.policyShortText : cfg.policyText;
    }
    syncPolicyText();
    try { window.addEventListener("resize", syncPolicyText, { passive: true }); } catch (_) {}
    textNode.appendChild(message);
    textNode.appendChild(shortMessage);
    textNode.appendChild(link);
    var button = document.createElement("button");
    button.type = "button";
    button.textContent = cfg.acceptText;
    button.addEventListener("click", function (event) {
      event.preventDefault();
      event.stopPropagation();
      grantConsent("banner_accept");
    });
    banner.appendChild(textNode);
    banner.appendChild(button);
    document.body.appendChild(banner);
  }
  var appState = null;
  var started = false;
  function initTracking(reason) {
    if (started) return;
    started = true;
    appState = state();
    if (reason) {
      post(cfg.endpoint, basePayload("consent_granted", { confirmation_reason: reason }), function (data) { syncState(appState, data); });
    }
    post(cfg.endpoint, basePayload("pageview"), function (data) { syncState(appState, data); });
  }
  function grantConsent(reason) {
    if (!readConsent()) writeConsent(reason || "manual");
    removeBanner();
    initTracking(reason || "manual");
    return true;
  }
  function ensureConsent(reason, form) {
    if (!needsConsent()) {
      initTracking();
      return true;
    }
    if (form && cfg.formConsent && hasFormConsent(form)) return grantConsent(reason || "form_checkbox");
    showBanner();
    return false;
  }
  function bindAutoConsent() {
    if (cfg.consent !== "required" || (cfg.autoConsent !== "continue" && cfg.autoConsent !== "time") || readConsent()) return;
    var startedAt = Date.now();
    var startY = window.pageYOffset || document.documentElement.scrollTop || 0;
    var done = false;
    function auto(reason, event) {
      if (done || readConsent()) return;
      if (event && event.target && event.target.closest && event.target.closest("#nexus-tracker-consent")) return;
      if (Date.now() - startedAt < cfg.autoDelay) return;
      done = true;
      grantConsent(reason);
    }
    if (cfg.autoConsent === "continue") {
      window.addEventListener("scroll", function (event) {
        var y = window.pageYOffset || document.documentElement.scrollTop || 0;
        if (Math.abs(y - startY) >= cfg.autoScroll) auto("continue_scroll", event);
      }, { passive: true });
      document.addEventListener("click", function (event) { auto("continue_click", event); }, true);
      document.addEventListener("keydown", function (event) { auto("continue_key", event); }, true);
      document.addEventListener("touchstart", function (event) { auto("continue_touch", event); }, { passive: true, capture: true });
    }
    if (cfg.autoConsent === "time") setTimeout(function () { auto("continue_time"); }, cfg.autoDelay);
  }

  var cfg = config();
  if (needsConsent()) {
    showBanner();
    bindAutoConsent();
  } else {
    initTracking();
  }

  document.addEventListener("click", function (event) {
    var target = event.target && event.target.closest && event.target.closest("button, input[type='submit'], input[type='button']");
    if (target && (target.form || target.closest("form"))) remember(target.form || target.closest("form"), "click");
  }, true);
  document.addEventListener("input", function (event) {
    if (event.target && event.target.form) remember(event.target.form, "input");
  }, true);
  document.addEventListener("change", function (event) {
    if (event.target && event.target.form) remember(event.target.form, "change");
  }, true);
  document.addEventListener("submit", function (event) {
    if (event.target && event.target.tagName && String(event.target.tagName).toLowerCase() === "form") remember(event.target, "submit");
  }, true);
  try {
    var observer = new MutationObserver(checkSuccess);
    observer.observe(document.documentElement || document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ["class", "style"] });
  } catch (_) {}
  setInterval(checkSuccess, 1500);
  checkSuccess();

  window.NexusTracker = {
    hasConsent: function () { return !needsConsent(); },
    grantConsent: function (reason) { return grantConsent(reason || "manual_api"); },
    revokeConsent: function () {
      removeBanner();
      clearCookie(CONSENT_COOKIE);
      clearCookie(COOKIE_VISITOR);
      clearCookie(COOKIE_VISIT);
      removeLocal(CONSENT_KEY);
      removeLocal(STORAGE_KEY);
      removeSession(PENDING_KEY);
      appState = null;
      started = false;
      showBanner();
    },
    getVisitorId: function () { return appState && appState.visitorId || cookie(COOKIE_VISITOR) || ""; },
    getVisitId: function () { return appState && appState.visitId || cookie(COOKIE_VISIT) || ""; },
    track: function (eventType, payload) {
      if (!ensureConsent("api_track")) return false;
      post(cfg.endpoint, basePayload(eventType || "custom", payload || {}), function (data) { syncState(appState, data); });
      return true;
    },
    captureForm: function (form) {
      var node = typeof form === "string" ? document.querySelector(form) : form;
      if (!ensureConsent("api_capture", node)) return {};
      return formPayload(node, "form_submit", "manual");
    },
    confirmForm: function (form, meta) {
      var node = typeof form === "string" ? document.querySelector(form) : form;
      if (!ensureConsent("api_confirm", node)) return false;
      var payload = formPayload(node || document, "form_submit_success", "manual");
      payload.confirmed = true;
      if (meta && typeof meta === "object") Object.keys(meta).forEach(function (key) { payload[key] = meta[key]; });
      var sig = signature(payload) + "|manual";
      if (sent(sig)) return false;
      post(cfg.endpoint, payload, function (data) { syncState(appState, data); });
      mark(sig);
      return true;
    }
  };
})();
""".strip()
