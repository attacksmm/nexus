from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import sqlite3
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

try:
    from orchestrator.auth import can_access_module, require_admin, verify_token_from_request
except Exception:  # pragma: no cover - local import fallback for isolated tests
    can_access_module = None
    require_admin = None
    verify_token_from_request = None

router = APIRouter()

_ctx = None
_logger = None
_cache: dict[str, tuple[float, Any]] = {}

DEFAULT_SPREADSHEET_ID = "1zu1__XcKxJH8yC9ForDvibaUnKFCS1pxWHEjLgqlVXA"
DEFAULT_DEALS_LOOKBACK_DATE = "2024-01-01"
DEFAULT_CLUB_TG_URL = "https://t.me/+b4VYXVGM6ys1NWUy"
DEFAULT_CLUB_VK_URL = "https://vk.me/join/SFB3endlZWQ3N6hn6dbNhXGpeNaHdv7ksbQ="
CACHE_SCHEMA_VERSION = "v3"
PARTIAL_PAYMENT_MARKERS = ("частично опла", "part_payed", "part paid", "partial")
PARTIAL_PAYMENT_DONE_MARKERS = ("закрыт", "законч", "заверш", "выплачен", "оплачен полностью", "paid in full", "completed")
CHAT_SHEETS = {
    "dog": {
        "title": "Послушная собака",
        "tg": "304757615",
        "vk": "443062527",
    },
    "puppy": {
        "title": "Первые шаги к воспитанию",
        "tg": "1437498106",
        "vk": "65520414",
    },
}
COURSE_LABELS = {
    "dog": "Послушная собака",
    "puppy": "Первые шаги к воспитанию щенка",
}
SUCCESS_STATUSES = {
    "payed",
    "part_payed",
    "paid",
    "success",
    "done",
    "completed",
    "оплачен",
    "оплачено",
    "частично оплачен",
    "успешно",
    "завершен",
    "завершён",
}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"\D+")


class GetCourseRateLimitError(RuntimeError):
    pass


def setup(ctx):
    global _ctx, _logger
    _ctx = ctx
    _logger = getattr(ctx, "logger", None)
    _init_module_db()


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean(value).lower()).replace("ё", "е")


def _norm_email(value: Any) -> str:
    return _clean(value).lower()


def _norm_phone(value: Any) -> str:
    digits = PHONE_RE.sub("", _clean(value))
    if len(digits) == 11 and digits.startswith("8"):
        return "7" + digits[1:]
    if len(digits) == 10:
        return "7" + digits
    return digits


def _mask_email(value: Any) -> str:
    email = _norm_email(value)
    if not email or "@" not in email:
        return "-"
    local, domain = email.split("@", 1)
    if not local:
        return f"*@{domain}"
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: Any) -> str:
    phone = _norm_phone(value)
    if not phone:
        return "-"
    return "*" * max(0, len(phone) - 4) + phone[-4:]


def _phone_variants(value: Any) -> set[str]:
    normalized = _norm_phone(value)
    if not normalized:
        return set()
    variants = {normalized}
    if len(normalized) == 11 and normalized.startswith("7"):
        variants.add("8" + normalized[1:])
        variants.add(normalized[1:])
    return variants


def _cache_ttl() -> int:
    try:
        return max(30, int(os.environ.get("TILDA_CHAT_LINKS_CACHE_TTL_SECONDS", "300")))
    except Exception:
        return 300


def _cache_ttl_for(key: str) -> int:
    if key == "gc:groups":
        try:
            return max(300, int(os.environ.get("TILDA_CHAT_LINKS_GROUPS_CACHE_TTL_SECONDS", "21600")))
        except Exception:
            return 21600
    if key == "chats":
        try:
            return max(300, int(os.environ.get("TILDA_CHAT_LINKS_CHATS_CACHE_TTL_SECONDS", "3600")))
        except Exception:
            return 3600
    return _cache_ttl()


def _cache_get(key: str) -> Any | None:
    item = _cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > _cache_ttl_for(key):
        _cache.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any) -> Any:
    _cache[key] = (time.time(), value)
    return value


def _cors_origin(request: Request) -> str:
    configured = _clean(os.environ.get("TILDA_CHAT_LINKS_ALLOWED_ORIGINS") or "*")
    origin = request.headers.get("origin") or "*"
    if configured == "*":
        return "*"
    allowed = {item.strip() for item in configured.split(",") if item.strip()}
    return origin if origin in allowed else next(iter(allowed), "*")


def _cors_headers(request: Request) -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": _cors_origin(request),
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400",
    }


def _json(request: Request, payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code, headers=_cors_headers(request))


@router.options("/{path:path}")
async def options_any(path: str, request: Request):
    return Response(status_code=204, headers=_cors_headers(request))


def _getcourse_credentials() -> tuple[str, str]:
    return (
        _clean(os.environ.get("GETCOURSE_ACCOUNT_NAME")),
        _clean(os.environ.get("GETCOURSE_API_TOKEN")),
    )


def _getcourse_base_url() -> str:
    account, _ = _getcourse_credentials()
    return f"https://{account}.getcourse.ru"


def _spreadsheet_id() -> str:
    return _clean(os.environ.get("TILDA_CHAT_LINKS_SPREADSHEET_ID")) or DEFAULT_SPREADSHEET_ID


def _deals_lookback_date() -> str:
    value = _clean(os.environ.get("TILDA_CHAT_LINKS_DEALS_LOOKBACK_DATE")) or DEFAULT_DEALS_LOOKBACK_DATE
    return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else DEFAULT_DEALS_LOOKBACK_DATE


def _deal_scan_timeout() -> int:
    try:
        return max(5, min(45, int(os.environ.get("TILDA_CHAT_LINKS_DEAL_SCAN_TIMEOUT_SECONDS", "20"))))
    except Exception:
        return 20


def _deal_scan_enabled() -> bool:
    return _clean(os.environ.get("TILDA_CHAT_LINKS_ENABLE_DEAL_SCAN")).lower() in {"1", "true", "yes", "on"}


def _lookup_cache_ttl(payload: dict[str, Any]) -> int:
    if bool(payload.get("eligible")):
        try:
            return max(300, int(os.environ.get("TILDA_CHAT_LINKS_LOOKUP_CACHE_TTL_SECONDS", "21600")))
        except Exception:
            return 21600
    try:
        return max(60, int(os.environ.get("TILDA_CHAT_LINKS_NEGATIVE_CACHE_TTL_SECONDS", "900")))
    except Exception:
        return 900


def _cache_db_path() -> Path:
    configured = _clean(os.environ.get("TILDA_CHAT_LINKS_CACHE_DB_PATH"))
    if configured:
        return Path(configured)
    data_dir = getattr(_ctx, "data_dir", None)
    if data_dir:
        return Path(data_dir) / "tilda_chat_links_cache.db"
    return Path("data/tilda_chat_links_cache.db")


def _cache_db() -> sqlite3.Connection:
    path = _cache_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5)
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lookup_cache (
            cache_key TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL
        )
        """
    )
    return conn


def _lookup_cache_key(email: str, phone: str) -> str:
    emails = {_norm_email(email)} if email else set()
    phones = _phone_variants(phone) if phone else set()
    return "lookup:" + CACHE_SCHEMA_VERSION + ":" + json.dumps({"email": sorted(emails), "phone": sorted(phones)}, ensure_ascii=False)


def _persistent_lookup_get(cache_key: str, *, allow_stale: bool = False) -> dict[str, Any] | None:
    try:
        with _cache_db() as conn:
            row = conn.execute(
                "SELECT payload_json, expires_at FROM lookup_cache WHERE cache_key = ? LIMIT 1",
                (cache_key,),
            ).fetchone()
    except Exception as error:
        _log("warning", "lookup cache read failed: %s", error)
        return None
    if row is None:
        return None
    payload_json, expires_at = row
    if not allow_stale and float(expires_at) < time.time():
        return None
    try:
        return json.loads(str(payload_json))
    except Exception:
        return None


def _persistent_lookup_set(cache_key: str, payload: dict[str, Any]) -> None:
    now = time.time()
    try:
        with _cache_db() as conn:
            conn.execute(
                """
                INSERT INTO lookup_cache (cache_key, payload_json, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (
                    cache_key,
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    now,
                    now + _lookup_cache_ttl(payload),
                ),
            )
            conn.commit()
    except Exception as error:
        _log("warning", "lookup cache write failed: %s", error)


def _init_module_db() -> None:
    try:
        with _cache_db() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_link_bans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    value_norm TEXT NOT NULL,
                    raw_value TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    UNIQUE(kind, value_norm)
                )
                """
            )
            conn.commit()
    except Exception as error:
        _log("warning", "module db init failed: %s", error)


async def _require_panel_user(request: Request) -> dict[str, Any] | JSONResponse:
    if verify_token_from_request is None:
        return JSONResponse({"ok": False, "error": "auth_unavailable"}, status_code=403)
    user = await verify_token_from_request(request)
    is_admin = require_admin(user) if require_admin else False
    has_module = can_access_module(user, "tilda-chat-links") if user and can_access_module else False
    if not user or not (is_admin or (user.get("role") == "editor" and has_module)):
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    return user


def _clear_lookup_cache() -> None:
    _cache.clear()
    try:
        with _cache_db() as conn:
            conn.execute("DELETE FROM lookup_cache")
            conn.commit()
    except Exception as error:
        _log("warning", "lookup cache clear failed: %s", error)


def _ban_value(value: Any) -> tuple[str, str, str]:
    raw = _clean(value)
    email = _norm_email(raw)
    if EMAIL_RE.match(email):
        return "email", email, raw
    phone = _norm_phone(raw)
    if phone:
        return "phone", phone, raw
    raise ValueError("Введите email или телефон")


def _ban_candidates(email: str, phone: str) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    if email:
        result.append(("email", _norm_email(email)))
    for value in _phone_variants(phone):
        result.append(("phone", value))
    seen: set[tuple[str, str]] = set()
    unique: list[tuple[str, str]] = []
    for item in result:
        if item[1] and item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def _find_ban(email: str, phone: str) -> dict[str, Any] | None:
    candidates = _ban_candidates(email, phone)
    if not candidates:
        return None
    try:
        _init_module_db()
        with _cache_db() as conn:
            for kind, value_norm in candidates:
                row = conn.execute(
                    """
                    SELECT id, kind, value_norm, raw_value, note, created_at
                    FROM chat_link_bans
                    WHERE kind = ? AND value_norm = ?
                    LIMIT 1
                    """,
                    (kind, value_norm),
                ).fetchone()
                if row:
                    return {
                        "id": int(row[0]),
                        "kind": str(row[1]),
                        "value_norm": str(row[2]),
                        "raw_value": str(row[3]),
                        "note": str(row[4] or ""),
                        "created_at": float(row[5]),
                    }
    except Exception as error:
        _log("warning", "ban lookup failed: %s", error)
    return None


def _unavailable_payload(reason: str, email: str, phone: str, *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "eligible": False,
        "reason": reason,
        "message": "Ссылки недоступны",
        "courses": [],
        "contacts": {
            "email_used": _norm_email(email),
            "phone_used": _norm_phone(phone),
        },
        "meta": {
            "generated_at": int(time.time()),
            **(meta or {}),
        },
    }


def _lookup_log_summary(payload: dict[str, Any]) -> dict[str, str]:
    courses = [course for course in payload.get("courses") or [] if isinstance(course, dict)]
    keys = ",".join(_clean(course.get("key")) for course in courses if _clean(course.get("key")))
    sources = ",".join(_clean(course.get("source")) for course in courses if _clean(course.get("source")))
    return {
        "eligible": str(bool(payload.get("eligible"))).lower(),
        "reason": _clean(payload.get("reason")) or "-",
        "courses": keys or "-",
        "sources": sources or "-",
    }


async def _request_getcourse(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    _, token = _getcourse_credentials()
    payload = {"key": token}
    if params:
        payload.update({key: value for key, value in params.items() if _clean(value)})
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=35, follow_redirects=True, headers={"Accept": "application/json"}) as client:
        for attempt in range(1, 4):
            try:
                response = await client.get(f"{_getcourse_base_url()}{path}", params=payload)
                response.raise_for_status()
                data = response.json()
                if not bool(data.get("success", False)):
                    message = str(data.get("error_message") or "GetCourse API error")
                    if "слишком много" in message.lower() or "too many" in message.lower():
                        raise GetCourseRateLimitError(message)
                    raise RuntimeError(message)
                return data
            except GetCourseRateLimitError:
                raise
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError, RuntimeError) as error:
                last_error = error
                status_code = getattr(getattr(error, "response", None), "status_code", None)
                if status_code == 429 or "too many" in str(error).lower() or "слишком много" in str(error).lower():
                    raise GetCourseRateLimitError(str(error))
                if attempt < 3:
                    await asyncio.sleep(attempt)
                    continue
                raise RuntimeError(str(last_error))
    raise RuntimeError(str(last_error or "GetCourse API error"))


def _rows_from_info(info: Any) -> list[dict[str, Any]] | None:
    if isinstance(info, list):
        if all(isinstance(item, dict) for item in info):
            return [item for item in info if isinstance(item, dict)]
        return []
    if isinstance(info, dict):
        fields = info.get("fields") or []
        items = info.get("items") or []
        if fields and items is not None:
            rows: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, list):
                    continue
                rows.append({str(fields[i]): item[i] for i in range(min(len(fields), len(item)))})
            return rows
    return None


async def _poll_export(export_id: str) -> list[dict[str, Any]]:
    last_status = ""
    for attempt in range(1, 12):
        try:
            data = await _request_getcourse(f"/pl/api/account/exports/{export_id}")
        except RuntimeError as error:
            text = str(error).lower()
            if "файл еще не создан" in text or "file not created" in text or "not ready" in text:
                await asyncio.sleep(min(2 + attempt, 7))
                continue
            raise
        info = data.get("info") or {}
        rows = _rows_from_info(info)
        if rows is not None:
            return rows
        last_status = _clean((info or {}).get("status") or data.get("status") or data.get("error_message"))
        await asyncio.sleep(min(2 + attempt, 7))
    raise RuntimeError(f"GetCourse export {export_id} did not complete: {last_status}")


async def _export_getcourse(path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    data = await _request_getcourse(path, params=params)
    info = data.get("info")
    rows = _rows_from_info(info)
    if rows is not None:
        return rows
    export_id = info.get("export_id") if isinstance(info, dict) else None
    if not export_id:
        raise RuntimeError("GetCourse export_id missing")
    return await _poll_export(str(export_id))


async def _load_group_catalog() -> dict[str, str]:
    cached = _cache_get("gc:groups")
    if cached is not None:
        return cached
    data = await _request_getcourse("/pl/api/account/groups")
    groups = {}
    for item in data.get("info") or []:
        if not isinstance(item, dict):
            continue
        group_id = _clean(item.get("id") or item.get("group_id"))
        name = _clean(item.get("name") or item.get("title"))
        if group_id and name:
            groups[group_id] = name
    return _cache_set("gc:groups", groups)


async def _export_users(email: str, phone: str) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"idgrouplist": "id_date"}
    if email:
        params["email"] = email
    if phone:
        params["phone"] = phone
    return await _export_getcourse("/pl/api/account/users", params)


def _row_has_full_user_payload(row: dict[str, Any]) -> bool:
    keys = [_norm_text(key) for key in row.keys()]
    return any(
        "idgrouplist" in key
        or "id групп" in key
        or "ссылка на чат" in key
        or key in {"телефон", "phone", "создан"}
        for key in keys
    )


def _row_email(row: dict[str, Any]) -> str:
    value = _row_value(row, "email", "e-mail", "почта")
    return _norm_email(value)


async def _lookup_user_rows(email: str, phone: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if email:
        rows = await _export_users(email, "")
        if rows:
            return rows
    if phone:
        rows = await _export_users("", phone)
        if rows and not any(_row_has_full_user_payload(row) for row in rows):
            enriched: list[dict[str, Any]] = []
            for row in rows:
                found_email = _row_email(row)
                if found_email:
                    try:
                        enriched.extend(await _export_users(found_email, ""))
                    except Exception as error:
                        _log("warning", "email enrichment skipped for phone=%s email=%s: %s", _norm_phone(phone), found_email, error)
            if enriched:
                return enriched
    return rows


async def _export_recent_paid_deals() -> list[dict[str, Any]]:
    since = _deals_lookback_date()
    cache_key = f"gc:deals:{since}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    candidates: list[dict[str, Any]] = []
    for status in ("payed", "part_payed"):
        params = {
            "status": status,
            "payed_at[from]": since,
        }
        try:
            rows = await _export_getcourse("/pl/api/account/deals", params)
        except Exception as error:
            _log("warning", "paid deals export skipped for %s: %s", params, error)
            continue
        candidates.extend(rows)
    return _cache_set(cache_key, candidates)


def _row_value(row: dict[str, Any], *markers: str) -> str:
    marker_norms = [_norm_text(item) for item in markers]
    for key, value in row.items():
        key_norm = _norm_text(key)
        if any(marker in key_norm for marker in marker_norms):
            text = _clean(value)
            if text:
                return text
    return ""


def _row_contact_matches(row: dict[str, Any], emails: set[str], phones: set[str], user_ids: set[str]) -> bool:
    if not emails and not phones and not user_ids:
        return False
    row_text = "\n".join(_clean(value) for value in row.values())
    row_email = {_norm_email(item) for item in re.findall(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", row_text, re.I)}
    row_phones: set[str] = set()
    for raw in re.findall(r"(?:\+?7|8)?[\s\-()]*\d[\d\s\-()]{8,}\d", row_text):
        row_phones.update(_phone_variants(raw))
    if emails and row_email.intersection(emails):
        return True
    if phones and row_phones.intersection(phones):
        return True
    if user_ids:
        gc_id = _row_value(row, "id пользователя", "user id", "user_id", "gc_user_id", "пользователь")
        if _clean(gc_id) in user_ids:
            return True
    return False


def _extract_user_ids(rows: list[dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for row in rows:
        for key, value in row.items():
            key_norm = _norm_text(key)
            if key_norm in {"id", "user_id", "ид", "id пользователя"} or "user id" in key_norm:
                text = _clean(value)
                if text.isdigit():
                    ids.add(text)
    return ids


def _parse_group_date_payload(raw_value: Any) -> list[tuple[str, str]]:
    text = _clean(raw_value)
    if not text:
        return []
    result: list[tuple[str, str]] = []
    for chunk in text.split(","):
        part = chunk.strip()
        if not part:
            continue
        if ":" in part:
            group_id, added_at = part.split(":", 1)
        else:
            group_id, added_at = part, ""
        group_id = _clean(group_id)
        if group_id.isdigit():
            result.append((group_id, _clean(added_at)))
    return result


@dataclass
class GroupEntry:
    name: str
    added_at: str = ""


def _extract_user_groups(rows: list[dict[str, Any]], catalog: dict[str, str]) -> list[GroupEntry]:
    result: list[GroupEntry] = []
    seen: set[str] = set()
    for row in rows:
        for key, value in row.items():
            key_norm = _norm_text(key)
            if "груп" not in key_norm and "group" not in key_norm and "idgrouplist" not in key_norm:
                continue
            text = _clean(value)
            if not text:
                continue
            pairs = _parse_group_date_payload(text)
            if not pairs:
                pairs = [(group_id, "") for group_id in re.findall(r"\d+", text)]
            for group_id, added_at in pairs:
                name = catalog.get(group_id)
                unique = f"{name}:{added_at}" if name else ""
                if name and unique not in seen:
                    seen.add(unique)
                    result.append(GroupEntry(name=name, added_at=added_at))
            if not re.search(r"\d", text) and text not in seen:
                seen.add(text)
                result.append(GroupEntry(name=text))
    return result


def _course_from_text(text: str) -> str | None:
    value = _norm_text(text)
    if "первые шаги к воспитанию" in value or "щенок" in value or "шенок" in value:
        return "puppy"
    if "послушная собака" in value or "собака" in value:
        return "dog"
    return None


def _tariff_from_text(text: str) -> tuple[bool, str]:
    value = _norm_text(text)
    if "личн" in value or "наставнич" in value:
        return True, "Личное наставничество"
    if "vip" in value or "вип" in value:
        return True, "VIP"
    if "premium" in value or "премиум" in value:
        return True, "Премиум"
    if "стандарт" in value or "standard" in value:
        return False, "Стандарт"
    return False, ""


def _is_success_order(row: dict[str, Any]) -> bool:
    status = _norm_text(_row_value(row, "статус", "status", "deal_status", "оплата"))
    if any(item in status for item in SUCCESS_STATUSES):
        return True
    paid = _row_value(row, "payed", "оплач", "paid")
    try:
        number = float(str(paid).replace(",", ".").replace(" ", ""))
        return number > 0
    except Exception:
        return False


@dataclass
class AccessCourse:
    key: str
    title: str
    tariff: str
    source: str
    matched_text: str
    status: str = ""
    added_at: str = ""
    direct_links: dict[str, str] | None = None
    chat_kind: str = "training"


def _access_from_deals(rows: list[dict[str, Any]], emails: set[str], phones: set[str], user_ids: set[str]) -> tuple[list[AccessCourse], bool]:
    courses: dict[str, AccessCourse] = {}
    saw_standard = False
    for row in rows:
        if not _row_contact_matches(row, emails, phones, user_ids):
            continue
        text = "\n".join(_clean(value) for value in row.values())
        course_key = _course_from_text(text)
        if not course_key:
            continue
        tariff_ok, tariff = _tariff_from_text(text)
        if not tariff_ok:
            if tariff == "Стандарт":
                saw_standard = True
            continue
        if not _is_success_order(row):
            continue
        status = _row_value(row, "статус", "status", "deal_status")
        courses[course_key] = AccessCourse(course_key, COURSE_LABELS[course_key], tariff, "orders", text[:260], status)
    return list(courses.values()), saw_standard


def _access_from_groups(groups: list[GroupEntry]) -> tuple[list[AccessCourse], bool]:
    courses: dict[str, AccessCourse] = {}
    saw_standard = False
    for group in groups:
        name = group.name
        course_key = _course_from_text(name)
        if not course_key:
            continue
        tariff_ok, tariff = _tariff_from_text(name)
        if not tariff_ok:
            if tariff == "Стандарт":
                saw_standard = True
            continue
        current = courses.get(course_key)
        if current is None or (group.added_at and (not current.added_at or group.added_at < current.added_at)):
            courses[course_key] = AccessCourse(course_key, COURSE_LABELS[course_key], tariff, "groups", name, added_at=group.added_at)
    return list(courses.values()), saw_standard


def _direct_chat_links_from_users(rows: list[dict[str, Any]]) -> AccessCourse | None:
    for row in rows:
        vk = _row_value(row, "ссылка на чат вк", "vk chat", "chat vk")
        tg = _row_value(row, "ссылка на чат тг", "telegram chat", "tg chat", "chat tg")
        links = {"tg": tg, "vk": vk}
        links = {key: value for key, value in links.items() if value.startswith("http")}
        if links:
            return AccessCourse(
                key="club",
                title="Клуб «Современный собаковод»",
                tariff="Выпускник",
                source="getcourse_custom_fields",
                matched_text="Ссылки из допполей GetCourse",
                direct_links=links,
                chat_kind="club",
            )
    return None


def _club_links() -> dict[str, str]:
    tg = _clean(os.environ.get("TILDA_CHAT_LINKS_CLUB_TG_URL")) or DEFAULT_CLUB_TG_URL
    vk = _clean(os.environ.get("TILDA_CHAT_LINKS_CLUB_VK_URL")) or DEFAULT_CLUB_VK_URL
    return {key: value for key, value in {"tg": tg, "vk": vk}.items() if value.startswith("http")}


def _club_after_days() -> int:
    try:
        return max(1, int(os.environ.get("TILDA_CHAT_LINKS_CLUB_AFTER_DAYS", "60")))
    except Exception:
        return 60


def _age_days(date_text: str) -> int | None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", _clean(date_text)):
        return None
    try:
        parsed = time.strptime(date_text, "%Y-%m-%d")
        return int((time.time() - time.mktime(parsed)) // 86400)
    except Exception:
        return None


def _partial_grace_days() -> int:
    try:
        return max(1, int(os.environ.get("TILDA_CHAT_LINKS_PARTIAL_GRACE_DAYS", "30")))
    except Exception:
        return 30


def _parse_date_values(value: Any) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    dates: list[str] = []
    for year, month, day in re.findall(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", text):
        dates.append(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
    for day, month, year in re.findall(r"(?<!\d)(\d{1,2})[.](\d{1,2})[.](\d{4})(?!\d)", text):
        dates.append(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
    return dates


def _row_has_partial_payment(row: dict[str, Any]) -> bool:
    for key, value in row.items():
        key_norm = _norm_text(key)
        value_norm = _norm_text(value)
        if not value_norm:
            continue
        if any(marker in value_norm for marker in PARTIAL_PAYMENT_MARKERS):
            return True
        if "рассроч" in key_norm and not any(marker in value_norm for marker in PARTIAL_PAYMENT_DONE_MARKERS):
            return True
    return False


def _partial_payment_overdue(rows: list[dict[str, Any]], group_entries: list[GroupEntry]) -> dict[str, Any] | None:
    partial_rows = [row for row in rows if _row_has_partial_payment(row)]
    if not partial_rows:
        return None

    date_candidates: list[str] = []
    for row in partial_rows:
        for key, value in row.items():
            key_norm = _norm_text(key)
            value_norm = _norm_text(value)
            if not value_norm:
                continue
            relevant_key = any(marker in key_norm for marker in ("дата", "date", "расср", "плат", "оплат", "заказ", "deal", "order", "payment", "paid", "payed"))
            relevant_value = any(marker in value_norm for marker in PARTIAL_PAYMENT_MARKERS)
            if relevant_key or relevant_value:
                date_candidates.extend(_parse_date_values(value))
    if not date_candidates:
        date_candidates.extend(group.added_at for group in group_entries if group.added_at)

    ages = [(date_text, _age_days(date_text)) for date_text in date_candidates]
    valid_ages = [(date_text, age) for date_text, age in ages if age is not None]
    if not valid_ages:
        return None
    oldest_date, max_age = max(valid_ages, key=lambda item: item[1])
    if max_age is not None and max_age >= _partial_grace_days():
        return {
            "partial_payment": True,
            "partial_age_days": max_age,
            "partial_date": oldest_date,
            "partial_grace_days": _partial_grace_days(),
        }
    return None


def _club_course_from_old_access(courses: list[AccessCourse]) -> AccessCourse | None:
    links = _club_links()
    if not links:
        return None
    for course in courses:
        age = _age_days(course.added_at)
        if age is not None and age >= _club_after_days():
            return AccessCourse(
                key="club",
                title="Клуб «Современный собаковод»",
                tariff=course.tariff,
                source="group_age",
                matched_text=course.matched_text,
                status=f"{age} дней после покупки",
                added_at=course.added_at,
                direct_links=links,
                chat_kind="club",
            )
    return None


async def _fetch_csv(gid: str) -> str:
    url = f"https://docs.google.com/spreadsheets/d/{_spreadsheet_id()}/gviz/tq?tqx=out:csv&gid={gid}"
    last_error: Exception | None = None
    headers = {"User-Agent": "Mozilla/5.0 Nexus Tilda Chat Links"}

    def load_once() -> str:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=25) as response:
            raw = response.read()
        return raw.decode("utf-8-sig", errors="replace")

    for attempt in range(1, 4):
        try:
            return await asyncio.to_thread(load_once)
        except Exception as error:
            last_error = error
            if attempt < 3:
                await asyncio.sleep(attempt)
    raise RuntimeError(str(last_error or "Google Sheet request failed"))


def _parse_latest_chat(csv_text: str, fallback_course: str) -> dict[str, Any]:
    best: dict[str, Any] | None = None
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if len(row) < 2:
            continue
        title = _clean(row[0])
        link = _clean(row[1])
        if not link.startswith("http"):
            continue
        match = re.search(r"(\d+)", title)
        if not match:
            continue
        number = int(match.group(1))
        if best is None or number > best["number"]:
            best = {
                "course": fallback_course,
                "title": title,
                "number": number,
                "url": link,
            }
    if not best:
        raise RuntimeError("No chat link rows found")
    return best


async def _load_current_chats() -> dict[str, dict[str, Any]]:
    cached = _cache_get("chats")
    if cached is not None:
        return cached
    result: dict[str, dict[str, Any]] = {}
    for course_key, config in CHAT_SHEETS.items():
        result[course_key] = {"title": config["title"]}
        for channel in ("tg", "vk"):
            csv_text = await _fetch_csv(config[channel])
            result[course_key][channel] = _parse_latest_chat(csv_text, course_key)
    return _cache_set("chats", result)


def _serialize_course(course: AccessCourse, chats: dict[str, dict[str, Any]]) -> dict[str, Any]:
    course_chats = chats.get(course.key) or {}
    direct_links = course.direct_links or {}
    return {
        "key": course.key,
        "title": course.title,
        "tariff": course.tariff,
        "source": course.source,
        "status": course.status,
        "chat_kind": course.chat_kind,
        "added_at": course.added_at,
        "links": {
            "tg": direct_links.get("tg") or (course_chats.get("tg") or {}).get("url", ""),
            "vk": direct_links.get("vk") or (course_chats.get("vk") or {}).get("url", ""),
        },
        "chat_numbers": {
            "tg": (course_chats.get("tg") or {}).get("number"),
            "vk": (course_chats.get("vk") or {}).get("number"),
        },
    }


async def _lookup_access(email: str, phone: str) -> dict[str, Any]:
    emails = {_norm_email(email)} if email else set()
    phones = _phone_variants(phone) if phone else set()
    cache_key = _lookup_cache_key(email, phone)
    ban = _find_ban(email, phone)
    if ban:
        return _unavailable_payload("banned", email, phone, meta={"ban_id": ban["id"], "ban_kind": ban["kind"]})
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    persistent = _persistent_lookup_get(cache_key)
    if persistent is not None:
        return _cache_set(cache_key, persistent)

    users = await _lookup_user_rows(next(iter(emails), ""), phone)
    group_catalog = await _load_group_catalog()
    user_ids = _extract_user_ids(users)
    group_entries = _extract_user_groups(users, group_catalog)
    direct_course = _direct_chat_links_from_users(users)
    courses, saw_standard = _access_from_groups(group_entries)
    partial_overdue = _partial_payment_overdue(users, group_entries)
    if partial_overdue:
        payload = _unavailable_payload("partial_payment_overdue", email, phone, meta={**partial_overdue, "users_found": len(users)})
        _persistent_lookup_set(cache_key, payload)
        return _cache_set(cache_key, payload)

    deals: list[dict[str, Any]] = []
    if direct_course:
        courses = [direct_course]
    elif courses:
        club_course = _club_course_from_old_access(courses)
        if club_course:
            courses = [club_course]
    elif _deal_scan_enabled():
        try:
            deals = await asyncio.wait_for(_export_recent_paid_deals(), timeout=_deal_scan_timeout())
            deal_courses, deals_standard = _access_from_deals(deals, emails, phones, user_ids)
            courses = deal_courses
            saw_standard = saw_standard or deals_standard
        except Exception as error:
            _log("warning", "paid deals fallback skipped: %s", error)

    chats = await _load_current_chats()
    payload = {
        "ok": True,
        "eligible": bool(courses),
        "reason": "ok" if courses else ("standard" if saw_standard else "not_found"),
        "courses": [_serialize_course(course, chats) for course in courses],
        "contacts": {
            "email_used": next(iter(emails), ""),
            "phone_used": _norm_phone(phone),
        },
        "meta": {
            "users_found": len(users),
            "deals_checked": len(deals),
            "generated_at": int(time.time()),
        },
    }
    _persistent_lookup_set(cache_key, payload)
    return _cache_set(cache_key, payload)


@router.get("/status")
async def status(request: Request):
    account, token = _getcourse_credentials()
    chats_ready = False
    chat_error = ""
    try:
        await _load_current_chats()
        chats_ready = True
    except Exception as error:
        chat_error = str(error)
    return _json(
        request,
        {
            "ok": True,
            "env": {
                "GETCOURSE_ACCOUNT_NAME": bool(account),
                "GETCOURSE_API_TOKEN": bool(token),
                "TILDA_CHAT_LINKS_SPREADSHEET_ID": _spreadsheet_id(),
            },
            "ready": bool(account and token and chats_ready),
            "chats_ready": chats_ready,
            "chat_error": chat_error,
        },
    )


@router.get("/ban-list")
async def ban_list(request: Request):
    user = await _require_panel_user(request)
    if isinstance(user, JSONResponse):
        return user
    _init_module_db()
    with _cache_db() as conn:
        rows = conn.execute(
            """
            SELECT id, kind, value_norm, raw_value, note, created_at
            FROM chat_link_bans
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
    return {
        "ok": True,
        "items": [
            {
                "id": int(row[0]),
                "kind": str(row[1]),
                "value_norm": str(row[2]),
                "raw_value": str(row[3]),
                "note": str(row[4] or ""),
                "created_at": float(row[5]),
            }
            for row in rows
        ],
    }


@router.post("/ban-list")
async def ban_add(request: Request):
    user = await _require_panel_user(request)
    if isinstance(user, JSONResponse):
        return user
    data = await request.json()
    try:
        kind, value_norm, raw_value = _ban_value(data.get("value"))
    except ValueError as error:
        return JSONResponse({"ok": False, "error": str(error)}, status_code=400)
    note = _clean(data.get("note"))[:500]
    _init_module_db()
    with _cache_db() as conn:
        conn.execute(
            """
            INSERT INTO chat_link_bans (kind, value_norm, raw_value, note, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(kind, value_norm) DO UPDATE SET
                raw_value = excluded.raw_value,
                note = excluded.note
            """,
            (kind, value_norm, raw_value, note, time.time()),
        )
        conn.commit()
    _clear_lookup_cache()
    return {"ok": True}


@router.delete("/ban-list/{ban_id}")
async def ban_delete(ban_id: int, request: Request):
    user = await _require_panel_user(request)
    if isinstance(user, JSONResponse):
        return user
    _init_module_db()
    with _cache_db() as conn:
        conn.execute("DELETE FROM chat_link_bans WHERE id = ?", (ban_id,))
        conn.commit()
    _clear_lookup_cache()
    return {"ok": True}


@router.get("/lookup")
async def lookup(
    request: Request,
    email: str = Query("", max_length=200),
    phone: str = Query("", max_length=80),
):
    email = _norm_email(email)
    phone = _clean(phone)
    if email and not EMAIL_RE.match(email):
        email = ""
    if not email and not _norm_phone(phone):
        return _json(request, {"ok": False, "error": "email_or_phone_required"}, status_code=400)

    account, token = _getcourse_credentials()
    if not account or not token:
        return _json(request, {"ok": False, "error": "getcourse_not_configured"}, status_code=503)

    try:
        payload = await _lookup_access(email, phone)
    except GetCourseRateLimitError as error:
        _log("warning", "lookup rate limited email=%s phone=%s: %s", email, _norm_phone(phone), error)
        stale = _persistent_lookup_get(_lookup_cache_key(email, phone), allow_stale=True)
        if stale and stale.get("ok"):
            meta = dict(stale.get("meta") or {})
            meta["stale"] = True
            meta["stale_reason"] = "getcourse_rate_limited"
            stale["meta"] = meta
            summary = _lookup_log_summary(stale)
            _log(
                "info",
                "lookup stale email=%s phone=%s eligible=%s reason=%s courses=%s sources=%s",
                _mask_email(email),
                _mask_phone(phone),
                summary["eligible"],
                summary["reason"],
                summary["courses"],
                summary["sources"],
            )
            return _json(request, stale, status_code=200)
        return _json(
            request,
            {
                "ok": False,
                "error": "getcourse_rate_limited",
                "message": "GetCourse временно ограничил запросы. Попробуйте открыть ссылку чуть позже.",
            },
            status_code=200,
        )
    except Exception as error:
        _log("error", "lookup failed email=%s phone=%s: %s", email, _norm_phone(phone), error)
        return _json(request, {"ok": False, "error": "lookup_failed", "message": str(error)}, status_code=502)
    summary = _lookup_log_summary(payload)
    _log(
        "info",
        "lookup ok email=%s phone=%s eligible=%s reason=%s courses=%s sources=%s",
        _mask_email(email),
        _mask_phone(phone),
        summary["eligible"],
        summary["reason"],
        summary["courses"],
        summary["sources"],
    )
    return _json(request, payload)


@router.get("/script.js")
async def tilda_script(request: Request):
    src = r"""
(function () {
  "use strict";
  var currentScript = document.currentScript;
  var apiUrl = new URL("lookup", currentScript ? currentScript.src : window.location.href).toString();

  function readParam(name) {
    var query = window.location.search.replace(/^\?/, "");
    if (!query) return "";
    var parts = query.split("&");
    for (var i = 0; i < parts.length; i += 1) {
      var pair = parts[i].split("=");
      if (decodeURIComponent(pair[0] || "") === name) {
        try { return decodeURIComponent((pair.slice(1).join("=") || "")); }
        catch (_) { return pair.slice(1).join("=") || ""; }
      }
    }
    return "";
  }

  var email = readParam("email").trim();
  var phone = readParam("phone").trim();

  function cssText() {
    return [
      "#nexus-chat-links{min-height:100svh!important;width:100%!important;display:flex!important;align-items:center!important;justify-content:center!important;padding:20px 14px!important;box-sizing:border-box!important;text-align:center!important}",
      "#nexus-chat-links *{box-sizing:border-box!important;letter-spacing:0!important}",
      ".ncl-wrap{font-family:Inter,Arial,sans-serif!important;color:#111!important;background:#fff!important;border:1px solid #e6e6e6!important;border-radius:8px!important;padding:22px 18px!important;width:min(560px,100%)!important;margin:0 auto!important;box-shadow:0 12px 40px rgba(0,0,0,.08)!important;text-align:center!important}",
      ".ncl-title{font-size:22px!important;line-height:1.15!important;font-weight:750!important;margin:0 0 8px!important;color:#111!important;text-align:center!important}.ncl-text{font-size:15px!important;line-height:1.45!important;color:#555!important;margin:0 auto 16px!important;text-align:center!important}.ncl-muted{font-size:13px!important;line-height:1.4!important;color:#777!important;margin:14px auto 0!important;text-align:center!important}",
      ".ncl-loader{width:28px!important;height:28px!important;border-radius:50%!important;border:3px solid #ddd!important;border-top-color:#111!important;animation:ncl-spin .8s linear infinite!important;margin:0 0 14px!important}@keyframes ncl-spin{to{transform:rotate(360deg)}}",
      ".ncl-loading .ncl-loader{margin:0 auto 14px!important}.ncl-loading .ncl-title,.ncl-loading .ncl-text{text-align:center!important}.ncl-loading .ncl-text{margin-left:auto!important;margin-right:auto!important;max-width:420px!important}",
      ".ncl-course{border-top:1px solid #eee!important;padding-top:16px!important;margin-top:16px!important;text-align:center!important}.ncl-course:first-of-type{border-top:0!important;margin-top:0!important;padding-top:0!important}.ncl-course-title{font-size:17px!important;line-height:1.25!important;font-weight:700!important;margin:0 auto 10px!important;color:#111!important;text-align:center!important}",
      ".ncl-actions{display:grid!important;grid-template-columns:1fr!important;gap:10px!important}.ncl-btn,.ncl-btn:link,.ncl-btn:visited{display:flex!important;align-items:center!important;justify-content:center!important;min-height:48px!important;border-radius:8px!important;text-decoration:none!important;font-size:16px!important;line-height:1.15!important;font-weight:700!important;color:#fff!important;border:0!important;text-align:center!important;padding:12px 14px!important}.ncl-btn-tg{background:#229ED9!important}.ncl-btn-vk{background:#0077FF!important}",
      "@media(min-width:520px){.ncl-actions{grid-template-columns:1fr 1fr!important}.ncl-wrap{padding:26px 24px!important}}"
    ].join("");
  }

  function mount() {
    var host = document.getElementById("nexus-chat-links");
    if (!host) {
      host = document.createElement("div");
      host.id = "nexus-chat-links";
      if (currentScript && currentScript.parentNode) currentScript.parentNode.insertBefore(host, currentScript);
      else document.body.appendChild(host);
    }
    if (!document.getElementById("nexus-chat-links-style")) {
      var style = document.createElement("style");
      style.id = "nexus-chat-links-style";
      style.textContent = cssText();
      document.head.appendChild(style);
    }
    return host;
  }

  function renderLoading(host) {
    host.innerHTML = '<div class="ncl-wrap ncl-loading"><div class="ncl-loader"></div><h2 class="ncl-title">Проверяем доступ</h2><p class="ncl-text">Если загрузка занимает больше пары секунд, пожалуйста, подождите: мы проверяем покупку и актуальные чаты.</p></div>';
  }

  function esc(value) {
    return String(value || "").replace(/[&<>"']/g, function (ch) {
      return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch];
    });
  }

  function renderResult(host, data) {
    if (!data.ok) {
      var retryText = data.error === "getcourse_rate_limited"
        ? "GetCourse временно ограничил проверку. Попробуйте открыть ссылку через пару минут или напишите менеджеру."
        : "Напишите куратору или менеджеру, чтобы мы проверили покупку вручную.";
      host.innerHTML = '<div class="ncl-wrap"><h2 class="ncl-title">Не получилось проверить доступ</h2><p class="ncl-text">' + esc(retryText) + '</p></div>';
      return;
    }
    if (!data.eligible) {
      var text = (data.reason === "banned" || data.reason === "partial_payment_overdue")
        ? "Ссылки недоступны."
        : data.reason === "standard"
        ? "На тарифе Стандарт учебный чат не предусмотрен. Доступ открывается для Премиум, VIP и личного наставничества."
        : "Мы не нашли подходящую оплаченную покупку по этим данным. Проверьте ссылку или напишите менеджеру.";
      var title = (data.reason === "banned" || data.reason === "partial_payment_overdue") ? "Ссылки недоступны" : "Чат пока недоступен";
      host.innerHTML = '<div class="ncl-wrap"><h2 class="ncl-title">' + esc(title) + '</h2><p class="ncl-text">' + esc(text) + '</p></div>';
      return;
    }
    var courses = data.courses || [];
    var html = '<div class="ncl-wrap"><h2 class="ncl-title">Ваши учебные чаты</h2><p class="ncl-text">Откройте чат в удобном приложении.</p>';
    courses.forEach(function (course) {
      var links = course.links || {};
      html += '<div class="ncl-course"><p class="ncl-course-title">' + esc(course.title) + '</p><div class="ncl-actions">';
      if (links.tg) html += '<a class="ncl-btn ncl-btn-tg" target="_blank" rel="noopener" href="' + esc(links.tg) + '">Открыть Telegram</a>';
      if (links.vk) html += '<a class="ncl-btn ncl-btn-vk" target="_blank" rel="noopener" href="' + esc(links.vk) + '">Открыть VK</a>';
      html += '</div></div>';
    });
    html += '<p class="ncl-muted">Если приложение не открылось, нажмите кнопку еще раз после входа в аккаунт.</p></div>';
    host.innerHTML = html;
  }

  var host = mount();
  if (!email && !phone) {
    host.innerHTML = '<div class="ncl-wrap"><h2 class="ncl-title">Не хватает данных</h2><p class="ncl-text">В ссылке должен быть email или phone, чтобы мы нашли покупку.</p></div>';
    return;
  }
  renderLoading(host);
  var url = apiUrl + "?email=" + encodeURIComponent(email) + "&phone=" + encodeURIComponent(phone);
  fetch(url, { method: "GET", credentials: "omit" })
    .then(function (r) { return r.json().catch(function () { return {ok:false}; }); })
    .then(function (data) { renderResult(host, data); })
    .catch(function () { renderResult(host, {ok:false}); });
})();
"""
    return PlainTextResponse(
        src.strip() + "\n",
        media_type="application/javascript; charset=utf-8",
        headers={**_cors_headers(request), "Cache-Control": "public, max-age=120"},
    )


@router.get("/snippet")
async def snippet(request: Request):
    base = str(request.base_url).rstrip("/")
    script_url = f"{base}/tilda-chat-links/api/script.js"
    html = f'<div id="nexus-chat-links"></div>\n<script src="{script_url}" async></script>'
    return PlainTextResponse(html, media_type="text/plain; charset=utf-8")


@router.get("/debug/chats")
async def debug_chats(request: Request):
    try:
        return _json(request, {"ok": True, "chats": await _load_current_chats()})
    except Exception as error:
        return _json(request, {"ok": False, "error": str(error)}, status_code=502)


@router.get("/preview", response_class=HTMLResponse)
async def preview():
    return HTMLResponse(
        """<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Preview</title></head><body><div id="nexus-chat-links"></div><script src="./script.js"></script></body></html>"""
    )
