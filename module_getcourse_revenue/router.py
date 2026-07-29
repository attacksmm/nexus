from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "getcourse-revenue"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
ALLOWED_STATUSES = {"paid", "partial", "unpaid"}
AMO_INCOMING_PIPELINE = "1. входящая"
PROFILE_UTM_FIELDS = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")
ORDER_UTM_FIELDS = tuple(f"order_{field}" for field in PROFILE_UTM_FIELDS)
DATE_FORMATS = (
    "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
)
EXPORT_CALL_LIMIT_2H = 15
EXPORT_REFRESH_HOURS = 3

_module_dir: Path | None = None
_db_path: Path | None = None
_logger: logging.Logger | None = None
_sync_task: asyncio.Task | None = None
_sync_lock = asyncio.Lock()
_funnel_task: asyncio.Task | None = None
_funnel_lock = asyncio.Lock()
_funnel_cache: dict[str, Any] | None = None  # Explicit test override only.
_funnel_stop = threading.Event()
_funnel_state: dict[str, Any] = {
    "running": False,
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
    "rows": 0,
}
FUNNEL_CACHE_FILENAME = "funnel-cache.db"
FUNNEL_REFRESH_SECONDS = 900
FUNNEL_SCHEMA_VERSION = "2"


def setup(ctx) -> None:
    global _module_dir, _db_path, _logger, _sync_task, _funnel_task
    _module_dir = Path(ctx.module_dir)
    _db_path = Path(ctx.db_path)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-revenue"))
    _funnel_stop.clear()
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        _sync_task = loop.create_task(_sync_loop(), name="getcourse-revenue-finance-sync")
        _funnel_task = loop.create_task(_funnel_refresh_loop(), name="getcourse-revenue-identity-refresh")
    else:
        loop.run_until_complete(_init_db())
    _log("info", "getcourse revenue module ready")


async def shutdown() -> None:
    global _sync_task, _funnel_task
    _funnel_stop.set()
    tasks = [_sync_task, _funnel_task]
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
    _sync_task = None
    _funnel_task = None


def _log(level: str, message: str, *args: Any, **kwargs: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


async def _require_panel_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _first_path(env_name: str, candidates: list[Path]) -> Path:
    configured = os.environ.get(env_name, "").strip()
    if configured:
        return Path(configured)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _customer_db_path() -> Path:
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    return _first_path("GETCOURSE_REVENUE_CUSTOMER_DB_PATH", [
        _module_dir.parent / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent / "module_customer_db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        _module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ])


def _orders_db_path() -> Path:
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    return _first_path("GETCOURSE_REVENUE_ORDERS_DB_PATH", [
        _module_dir.parent / "getcourse-orders" / "data" / "getcourse-orders.db",
        _module_dir.parent / "module_getcourse_orders" / "data" / "getcourse-orders.db",
        _module_dir.parent.parent / "modules" / "getcourse-orders" / "data" / "getcourse-orders.db",
    ])


def _tracker_db_path() -> Path:
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    return _first_path("GETCOURSE_REVENUE_TRACKER_DB_PATH", [
        _module_dir.parent / "tracker" / "data" / "tracker.db",
        _module_dir.parent / "module_tracker" / "data" / "tracker.db",
        _module_dir.parent.parent / "modules" / "tracker" / "data" / "tracker.db",
        _module_dir.parent.parent / "module_tracker" / "data" / "tracker.db",
    ])


def _archive_db_path(customer_db_path: Path) -> Path:
    return customer_db_path.parent / "archive" / "customer-db-archive.db"


def _connect_readonly(path: Path) -> aiosqlite.Connection:
    return aiosqlite.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=20)


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    cur = await db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return bool(await cur.fetchone())


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _parse_datetime(value: Any, *, naive_tz: timezone | ZoneInfo = UTC) -> datetime | None:
    raw = _clean(value, 100)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=naive_tz) if parsed.tzinfo is None else parsed
    except ValueError:
        pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=naive_tz)
        except ValueError:
            continue
    return None


def _money(value: Any) -> Decimal:
    if isinstance(value, (int, float, Decimal)):
        raw = str(value)
    else:
        raw = _clean(value, 100).replace("\u00a0", "").replace(" ", "").replace(",", ".")
        match = re.search(r"-?\d+(?:\.\d+)?", raw)
        raw = match.group(0) if match else "0"
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def _updated_sort_key(row: dict[str, Any]) -> tuple[datetime, int]:
    parsed = _parse_datetime(row.get("updated_at")) or datetime.min.replace(tzinfo=UTC)
    return parsed.astimezone(UTC), 1 if not row.get("archived") else 0


async def _read_order_records() -> tuple[list[dict[str, Any]], dict[str, int]]:
    customer_path = _customer_db_path()
    rows: list[dict[str, Any]] = []
    counts = {"live": 0, "archive": 0, "malformed": 0, "duplicates": 0}
    sources = ((customer_path, "cdb_getcourse_orders", False), (_archive_db_path(customer_path), "archive_records", True))
    for path, table, archived in sources:
        if not path.exists():
            continue
        async with _connect_readonly(path) as db:
            db.row_factory = aiosqlite.Row
            if not await _table_exists(db, table):
                continue
            where = " WHERE table_name='getcourse_orders'" if archived else ""
            cur = await db.execute(
                f"SELECT id, platform_id, custom_fields, created_at, updated_at FROM {table}{where}"
            )
            for item in await cur.fetchall():
                rows.append({**dict(item), "archived": archived})
                counts["archive" if archived else "live"] += 1
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        platform_id = _clean(row.get("platform_id"), 200)
        fields = _parse_json_object(row.get("custom_fields"))
        if not platform_id or not fields:
            counts["malformed"] += 1
            continue
        normalized = {**row, "platform_id": platform_id, "fields": fields}
        previous = unique.get(platform_id)
        if previous is not None:
            counts["duplicates"] += 1
        if previous is None or _updated_sort_key(normalized) > _updated_sort_key(previous):
            unique[platform_id] = normalized
    return list(unique.values()), counts


async def _first_event_dates(platform_ids: list[str], states: set[str]) -> dict[tuple[str, str], datetime]:
    path = _orders_db_path()
    if not path.exists() or not platform_ids or not states:
        return {}
    result: dict[tuple[str, str], datetime] = {}
    state_values = sorted(states)
    async with _connect_readonly(path) as db:
        if not await _table_exists(db, "events"):
            return {}
        for index in range(0, len(platform_ids), 400):
            chunk = platform_ids[index:index + 400]
            cur = await db.execute(
                f"SELECT platform_id,payment_state,MIN(received_at) FROM events "
                f"WHERE payment_state IN ({','.join('?' for _ in state_values)}) "
                f"AND platform_id IN ({','.join('?' for _ in chunk)}) GROUP BY platform_id,payment_state",
                (*state_values, *chunk),
            )
            for platform_id, state, received_at in await cur.fetchall():
                parsed = _parse_datetime(received_at)
                if parsed:
                    result[(_clean(platform_id, 200), _clean(state, 20))] = parsed
    return result


def _flatten(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten(item) for item in value)
    return _clean(value, 5000)


def _order_product(fields: dict[str, Any]) -> str:
    return " ".join(part for part in (
        _clean(fields.get("title"), 2000), _flatten(fields.get("positions")), _flatten(fields.get("offers")),
    ) if part).strip()


def _order_state(fields: dict[str, Any]) -> str:
    state = _clean(fields.get("payment_state"), 30).casefold()
    return state if state in ALLOWED_STATUSES else ""


def _record_date(row: dict[str, Any], state: str, event_dates: dict[tuple[str, str], datetime]) -> tuple[date | None, bool]:
    event_date = event_dates.get((row["platform_id"], state))
    if event_date:
        return event_date.astimezone(MOSCOW_TZ).date(), False
    fields = row["fields"]
    if state in {"paid", "partial"}:
        value = (
            fields.get("paid_at") or fields.get("date_payment")
            or (fields.get("status_changed_at") if state == "partial" else "")
            or fields.get("date_updated")
        )
        paid = _parse_datetime(value, naive_tz=MOSCOW_TZ)
        if paid:
            return paid.astimezone(MOSCOW_TZ).date(), False
    if state == "unpaid":
        created = _parse_datetime(
            fields.get("date_creation") or fields.get("date_add") or fields.get("created"),
            naive_tz=MOSCOW_TZ,
        )
        if created:
            return created.astimezone(MOSCOW_TZ).date(), False
    fallback = _parse_datetime(fields.get("received_at")) or _parse_datetime(row.get("updated_at")) or _parse_datetime(row.get("created_at"))
    return (fallback.astimezone(MOSCOW_TZ).date(), True) if fallback else (None, True)


def _selection_list(value: Any) -> list[str]:
    source = value if isinstance(value, list) else [value] if value else []
    result: list[str] = []
    for item in source[:100]:
        cleaned = _clean(item, 500).casefold()
        if cleaned and cleaned not in result:
            result.append(cleaned)
    return result


def _filters_payload(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    profile = raw.get("profile_utm") if isinstance(raw.get("profile_utm"), dict) else {}
    order = raw.get("order_utm") if isinstance(raw.get("order_utm"), dict) else {}
    return {
        "profile_utm": {field: _selection_list(profile.get(field)) for field in PROFILE_UTM_FIELDS},
        "order_utm": {field: _selection_list(order.get(field)) for field in ORDER_UTM_FIELDS},
        "product": _clean(raw.get("product"), 500).casefold(),
    }


def _matches_filters(fields: dict[str, Any], filters: dict[str, Any]) -> bool:
    for group in ("profile_utm", "order_utm"):
        for field, selected in filters[group].items():
            if selected and _clean(fields.get(field), 500).casefold() not in selected:
                return False
    return not filters["product"] or filters["product"] in _order_product(fields).casefold()


def _period(payload: dict[str, Any], today: date | None = None) -> tuple[str, date, date]:
    local_today = today or datetime.now(MOSCOW_TZ).date()
    preset = _clean(payload.get("preset") or "30d", 20).casefold()
    days = {"7d": 7, "30d": 30, "365d": 365}
    if preset in days:
        return preset, local_today - timedelta(days=days[preset] - 1), local_today
    if preset != "custom":
        raise HTTPException(400, "Некорректный период")
    try:
        start = date.fromisoformat(_clean(payload.get("date_from"), 20))
        end = date.fromisoformat(_clean(payload.get("date_to"), 20))
    except ValueError as exc:
        raise HTTPException(400, "Для произвольного периода нужны даты YYYY-MM-DD") from exc
    if start > end:
        raise HTTPException(400, "Начальная дата позже конечной")
    if (end - start).days > 1095:
        raise HTTPException(400, "Максимальный диапазон — 1096 дней")
    return preset, start, end


def _option_list(counter: Counter[str], limit: int = 100) -> list[dict[str, Any]]:
    return [{"value": value, "count": count} for value, count in sorted(
        counter.items(), key=lambda item: (-item[1], item[0].casefold())
    )[:limit] if value]


def _source_family(value: Any) -> str:
    source = re.sub(r"[^a-zа-я0-9]+", "_", _clean(value, 300).casefold()).strip("_")
    tokens = set(filter(None, source.split("_")))
    if source.startswith("vk") or "vk" in tokens or "vkontakte" in source or "senler" in source:
        return "vk"
    if source.startswith("telegram") or "telegram" in source or source == "tg" or "tg" in tokens:
        return "tg"
    return ""


async def _registration_buckets(start: date, end: date) -> tuple[dict[date, dict[str, int]], int]:
    buckets = {start + timedelta(days=i): {"vk": 0, "tg": 0} for i in range((end - start).days + 1)}
    path = _customer_db_path()
    if not path.exists():
        return buckets, 0
    read = 0
    first_seen: dict[tuple[str, str], datetime] = {}
    async with _connect_readonly(path) as db:
        for table, family in (("cdb_vk_clients", "vk"), ("cdb_telegram_clients", "tg")):
            if not await _table_exists(db, table):
                continue
            cur = await db.execute(
                f"""
                SELECT platform_id,COALESCE(
                    NULLIF(json_extract(custom_fields,'$.first_contact_at'),''),
                    NULLIF(json_extract(custom_fields,'$.date_creation'),''),
                    NULLIF(json_extract(custom_fields,'$.created'),''),
                    NULLIF(json_extract(custom_fields,'$.date_add'),''),
                    NULLIF(json_extract(custom_fields,'$.created_at_ts'),''),
                    created_at,updated_at
                ) FROM {table}
                """
            )
            for platform_id, registered_at in await cur.fetchall():
                read += 1
                external_id = _strong_external_id(platform_id)
                if not external_id:
                    continue
                moment = _journey_date(registered_at)
                if not moment:
                    continue
                key = (family, external_id)
                normalized = moment.astimezone(MOSCOW_TZ)
                if key not in first_seen or normalized < first_seen[key]:
                    first_seen[key] = normalized
    for (family, _), moment in first_seen.items():
        if moment.date() in buckets:
            buckets[moment.date()][family] += 1
    return buckets, read


def _identity_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", _clean(value, 100))
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return digits if 10 <= len(digits) <= 15 and len(set(digits)) >= 5 else ""


def _identity_email(value: Any) -> str:
    email = _clean(value, 320).casefold()
    local = email.partition("@")[0]
    blocked = {"test", "none", "noemail", "noreply", "no-reply", "example"}
    return email if local not in blocked and re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email) else ""


def _strong_external_id(value: Any) -> str:
    cleaned = _clean(value, 200)
    return "" if cleaned.casefold() in {"", "0", "none", "null", "undefined", "unknown"} else cleaned


def _scalars(value: Any, parent_key: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, (dict, list)):
                yield from _scalars(child, _clean(key, 100).casefold())
            else:
                yield _clean(key, 100).casefold(), child
    elif isinstance(value, list):
        for child in value:
            if isinstance(child, (dict, list)):
                yield from _scalars(child, parent_key)
            elif parent_key:
                yield parent_key, child


def _identity_tokens(
    service: str,
    platform_id: Any,
    fields: dict[str, Any],
    *,
    known_vk_platform_ids: set[str] | None = None,
) -> set[str]:
    tokens: set[str] = set()
    platform = _clean(platform_id, 300)
    if platform:
        tokens.add(f"{service}_record:{platform}")
    for key, value in _scalars(fields):
        values = value if isinstance(value, list) else [value]
        for item in values:
            if key.startswith("manager_"):
                continue
            if key in {"phone", "phones", "telephone", "csv_phone"} or key.endswith("_phone"):
                if phone := _identity_phone(item):
                    tokens.add("phone:" + phone)
            elif key in {"email", "emails", "e_mail"} or key.endswith("_email"):
                if email := _identity_email(item):
                    tokens.add("email:" + email)
            elif key in {"salebot_id", "salebot_client_id"}:
                if clean := _strong_external_id(item):
                    tokens.add("salebot:" + clean)
            elif key in {"vk_id", "vkontakte_id"}:
                if clean := re.sub(r"\D", "", _clean(item, 100)):
                    tokens.add("vk:" + clean)
            elif key in {"platform_id", "vk_platform_id", "senler_id", "utm_term"}:
                clean = _strong_external_id(item)
                if clean and known_vk_platform_ids and clean in known_vk_platform_ids:
                    tokens.add("vk_platform:" + clean)
            elif key in {"ym_uid", "user_ym_uid", "_ym_uid", "pay_field_user_ym_uid", "reg_field_user_ym_uid"}:
                if clean := _strong_external_id(item):
                    tokens.add("ym:" + clean)
            elif key in {"visitor_id", "gc_user_id", "bizon_user_id", "chatuserid"}:
                if clean := _strong_external_id(item):
                    tokens.add(f"{key}:" + clean)
    for raw in fields.get("identity_tokens") or []:
        prefix, _, value = _clean(raw, 500).partition(":")
        prefix = prefix.casefold()
        if prefix == "phone" and (clean := _identity_phone(value)):
            tokens.add("phone:" + clean)
        elif prefix == "email" and (clean := _identity_email(value)):
            tokens.add("email:" + clean)
        elif prefix in {"bizon_user_id", "chatuserid"} and value:
            tokens.add(prefix + ":" + _clean(value, 200))
    if service == "vk" and platform:
        tokens.add("vk_platform:" + platform)
        if clean := re.sub(r"\D", "", platform):
            tokens.add("vk:" + clean)
    return tokens


def _journey_date(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) or (_clean(value, 30).isdigit() and len(_clean(value, 30)) >= 9):
        try:
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000
            return datetime.fromtimestamp(number, tz=UTC)
        except (ValueError, OSError, OverflowError):
            pass
    return _parse_datetime(value, naive_tz=MOSCOW_TZ)


def _service_event_date(
    service: str, fields: dict[str, Any], created_at: Any, updated_at: Any,
) -> datetime | None:
    if service == "bizon":
        # Historical Bizon exports may put the row/view creation timestamp into
        # `webinar_at`. `date_web` is the explicit business date of the webinar
        # and therefore must win whenever both values are present.
        for key in ("date_web", "webinar_date", "webinar_at"):
            if parsed := _journey_date(fields.get(key)):
                return parsed
        webinar_id = _clean(fields.get("webinarId") or fields.get("webinar_id"), 1000)
        match = re.search(r"\*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?)", webinar_id)
        return _journey_date(match.group(1)) if match else None
    value = (
        fields.get("first_contact_at") or fields.get("date_creation")
        or fields.get("created") or fields.get("date_add")
        or fields.get("created_at_ts") or created_at or updated_at
    )
    return _journey_date(value)


def _journey_source(service: str, fields: dict[str, Any]) -> str:
    nested_utms = fields.get("utms") if isinstance(fields.get("utms"), dict) else {}
    raw = _clean(
        fields.get("utm_source") or nested_utms.get("utm_source")
        or fields.get("user_source") or fields.get("order_utm_source"),
        120,
    )
    if raw:
        if raw.casefold() == "avito":
            return "Avito"
        return raw[:48]
    if service == "avito":
        return "Avito"
    return "Прямой / без UTM"


def _source_segment(source: str) -> str:
    normalized = _clean(source, 120).casefold()
    if re.search(r"(?:^|_)ai(?:_|$)", normalized):
        return "ai"
    if re.search(r"(?:^|_)baza(?:_|$)", normalized):
        return "baza"
    return ""


FUNNEL_CONTACT = 1
FUNNEL_REGISTRATION = 2
FUNNEL_LIVE = 4
FUNNEL_APPLICATION = 8
FUNNEL_SALE_PAID = 16
FUNNEL_SALE_PARTIAL = 32


def _service_funnel_flags(service: str) -> int:
    flags = FUNNEL_CONTACT
    if service in {"vk", "telegram"}:
        flags |= FUNNEL_REGISTRATION
    elif service == "bizon":
        flags |= FUNNEL_LIVE
    elif service in {"getcourse", "amo"}:
        flags |= FUNNEL_APPLICATION
    return flags
FUNNEL_SALE_MASK = FUNNEL_SALE_PAID | FUNNEL_SALE_PARTIAL


def _is_incoming_amo_deal(fields: dict[str, Any]) -> bool:
    """Keep amoCRM analytics scoped to the sales team's incoming pipeline."""
    pipeline = re.sub(r"\s+", " ", _clean(fields.get("pipeline_name"), 200)).casefold().replace("ё", "е")
    return pipeline == AMO_INCOMING_PIPELINE


def _is_full_tariff(fields: dict[str, Any]) -> bool:
    """Return true only for the three main-course tariff families.

    CSV backfills persist the audited decision explicitly. The fallback keeps
    fresh webhook orders classifiable before the next export arrives.
    """
    explicit = fields.get("is_full_tariff")
    if isinstance(explicit, bool):
        return explicit
    if isinstance(explicit, (int, float)):
        return bool(explicit)
    if isinstance(explicit, str) and explicit.strip().casefold() in {"да", "yes", "true", "1"}:
        return True
    title = _clean(fields.get("title") or fields.get("positions"), 4000).casefold().replace("ё", "е")
    if not title or _money(fields.get("cost_money")) < Decimal("10000"):
        return False
    if any(marker in title for marker in ("мини-курс", "доплата", "модуль", "за 15 минут", "запись эфира")):
        return False
    if re.search(r"\b\d+\s*/\s*\d+\b", title):
        return False
    main_course = any(marker in title for marker in (
        "первые шаги", "послушная собака", "современный собаковод", "курс щенок",
    ))
    tariff = any(marker in title for marker in ("тариф", "пакет", "курс щенок"))
    return main_course and tariff


def _snapshot_payment_dates() -> tuple[dict[tuple[str, str], datetime], dict[str, datetime]]:
    event_dates: dict[tuple[str, str], datetime] = {}
    finance_dates: dict[str, datetime] = {}
    orders_path = _orders_db_path()
    if orders_path.exists():
        with sqlite3.connect(f"file:{orders_path.as_posix()}?mode=ro", uri=True, timeout=30) as db:
            if "events" in _sync_table_names(db):
                for platform_id, state, received_at in db.execute(
                    "SELECT platform_id,payment_state,MIN(received_at) FROM events "
                    "WHERE payment_state IN ('paid','partial') GROUP BY platform_id,payment_state"
                ):
                    if parsed := _parse_datetime(received_at):
                        event_dates[(_clean(platform_id, 200), _clean(state, 20))] = parsed
    if _db_path and _db_path.exists():
        with sqlite3.connect(f"file:{_db_path.as_posix()}?mode=ro", uri=True, timeout=30) as db:
            if "finance_cache" in _sync_table_names(db):
                for order_id, paid_at in db.execute("SELECT order_id,paid_at FROM finance_cache WHERE paid_at<>''"):
                    if parsed := _parse_datetime(paid_at, naive_tz=MOSCOW_TZ):
                        finance_dates[_clean(order_id, 200)] = parsed
    return event_dates, finance_dates


def _sync_table_names(db: sqlite3.Connection) -> set[str]:
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


FUNNEL_SERVICES = {
    "avito": 1, "vk": 2, "telegram": 4, "bizon": 8,
    "getcourse_user": 16, "getcourse": 32, "amo": 64, "tracker": 128,
}
FUNNEL_CHANNELS = {"vk": 1, "telegram": 2}


class _FunnelBuildCancelled(Exception):
    pass


def _funnel_store_path() -> Path:
    if _module_dir is None:
        raise RuntimeError("module context is not initialized")
    return _module_dir / "data" / FUNNEL_CACHE_FILENAME


def _source_fingerprint() -> str:
    customer = _customer_db_path()
    paths = (customer, _archive_db_path(customer), _orders_db_path(), _tracker_db_path(), _db_path)
    values = []
    for path in paths:
        if path and path.exists():
            stat = path.stat()
            values.append((str(path), stat.st_size, stat.st_mtime_ns))
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


class _FunnelStoreBuilder:
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db
        self.rows = 0

    def _check_cancelled(self) -> None:
        self.rows += 1
        if self.rows % 1024 == 0 and _funnel_stop.is_set():
            raise _FunnelBuildCancelled()

    def _merge(self, keep: int, other: int) -> None:
        if keep == other:
            return
        left = self.db.execute(
            "SELECT first_day,source,source_at,flags,services,registration_channels FROM entities WHERE entity_id=?",
            (keep,),
        ).fetchone()
        right = self.db.execute(
            "SELECT first_day,source,source_at,flags,services,registration_channels FROM entities WHERE entity_id=?",
            (other,),
        ).fetchone()
        if not left or not right:
            return
        first_day = min(left[0], right[0])
        source, source_at = left[1], left[2]
        if right[2] and (not source_at or right[2] < source_at):
            source, source_at = right[1], right[2]
        self.db.execute(
            "UPDATE entities SET first_day=?,source=?,source_at=?,flags=?,services=?,registration_channels=? WHERE entity_id=?",
            (first_day, source, source_at, left[3] | right[3], left[4] | right[4], left[5] | right[5], keep),
        )
        self.db.execute("UPDATE tokens SET entity_id=? WHERE entity_id=?", (keep, other))
        self.db.execute(
            "INSERT OR IGNORE INTO stage_dates(entity_id,stage,day) "
            "SELECT ?,stage,day FROM stage_dates WHERE entity_id=?", (keep, other),
        )
        for kind, day, count in self.db.execute(
            "SELECT kind,day,count FROM record_dates WHERE entity_id=?", (other,),
        ).fetchall():
            self.db.execute(
                "INSERT INTO record_dates(entity_id,kind,day,count) VALUES(?,?,?,?) "
                "ON CONFLICT(entity_id,kind,day) DO UPDATE SET count=count+excluded.count",
                (keep, kind, day, count),
            )
        self.db.execute("DELETE FROM stage_dates WHERE entity_id=?", (other,))
        self.db.execute("DELETE FROM record_dates WHERE entity_id=?", (other,))
        self.db.execute("DELETE FROM entities WHERE entity_id=?", (other,))

    def add(
        self, tokens: set[str], moment: datetime, source: str, flags: int, service: str, kind: str = "",
    ) -> None:
        if not tokens:
            return
        self._check_cancelled()
        # Persist only fixed-size, non-reversible identity digests. The raw
        # phone/email/service identifiers exist only while one source row is
        # being normalized.
        ordered = sorted(hashlib.blake2b(token.encode("utf-8"), digest_size=16).hexdigest() for token in tokens)
        placeholders = ",".join("?" for _ in ordered)
        entity_ids = sorted({row[0] for row in self.db.execute(
            f"SELECT entity_id FROM tokens WHERE token IN ({placeholders})", ordered,
        )})
        day = moment.astimezone(MOSCOW_TZ).date().isoformat()
        if entity_ids:
            entity_id = entity_ids[0]
            for other in entity_ids[1:]:
                self._merge(entity_id, other)
        else:
            cursor = self.db.execute(
                "INSERT INTO entities(first_day,source,source_at,flags,services,registration_channels) "
                "VALUES(?,?,?,?,?,?)",
                (day, "Прямой / без UTM", "", 0, 0, 0),
            )
            entity_id = int(cursor.lastrowid)
        row = self.db.execute(
            "SELECT first_day,source,source_at,flags,services,registration_channels FROM entities WHERE entity_id=?",
            (entity_id,),
        ).fetchone()
        selected_source, source_at = row[1], row[2]
        source_moment = moment.astimezone(MOSCOW_TZ).isoformat()
        if source != "Прямой / без UTM" and (not source_at or source_moment < source_at):
            selected_source, source_at = source, source_moment
        self.db.execute(
            "UPDATE entities SET first_day=?,source=?,source_at=?,flags=?,services=?,registration_channels=? "
            "WHERE entity_id=?",
            (
                min(row[0], day), selected_source, source_at, row[3] | flags,
                row[4] | FUNNEL_SERVICES.get(service, 0),
                row[5] | FUNNEL_CHANNELS.get(service, 0), entity_id,
            ),
        )
        self.db.executemany(
            "INSERT OR IGNORE INTO tokens(token,entity_id) VALUES(?,?)",
            ((token, entity_id) for token in ordered),
        )
        for bit, stage in (
            (FUNNEL_REGISTRATION, "registrations"), (FUNNEL_LIVE, "live"),
            (FUNNEL_APPLICATION, "applications"), (FUNNEL_SALE_PAID, "sales_paid"),
            (FUNNEL_SALE_PARTIAL, "sales_partial"),
        ):
            if flags & bit:
                self.db.execute(
                    "INSERT OR IGNORE INTO stage_dates(entity_id,stage,day) VALUES(?,?,?)",
                    (entity_id, stage, day),
                )
        if kind:
            self.db.execute(
                "INSERT INTO record_dates(entity_id,kind,day,count) VALUES(?,?,?,1) "
                "ON CONFLICT(entity_id,kind,day) DO UPDATE SET count=count+1",
                (entity_id, kind, day),
            )


def _open_source(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    db.execute("PRAGMA cache_size=-4096")
    db.execute("PRAGMA temp_store=FILE")
    return db


def _build_funnel_store(fingerprint: str) -> dict[str, Any]:
    live_path = _funnel_store_path()
    live_path.parent.mkdir(parents=True, exist_ok=True)
    staging = live_path.with_name(f".{live_path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    customer_path, tracker_path = _customer_db_path(), _tracker_db_path()
    payment_dates, finance_dates = _snapshot_payment_dates()
    known_vk_platform_ids: set[str] = set()
    built_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with sqlite3.connect(staging, timeout=30) as store:
            store.executescript(
                """
                PRAGMA journal_mode=OFF;
                PRAGMA synchronous=OFF;
                PRAGMA temp_store=FILE;
                PRAGMA cache_size=-8192;
                CREATE TABLE entities (
                    entity_id INTEGER PRIMARY KEY,
                    first_day TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_at TEXT NOT NULL DEFAULT '',
                    flags INTEGER NOT NULL DEFAULT 0,
                    services INTEGER NOT NULL DEFAULT 0,
                    registration_channels INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE tokens (token TEXT PRIMARY KEY, entity_id INTEGER NOT NULL);
                CREATE INDEX idx_tokens_entity ON tokens(entity_id);
                CREATE TABLE stage_dates (
                    entity_id INTEGER NOT NULL, stage TEXT NOT NULL, day TEXT NOT NULL,
                    PRIMARY KEY(entity_id,stage,day)
                );
                CREATE INDEX idx_stage_day ON stage_dates(stage,day,entity_id);
                CREATE TABLE record_dates (
                    entity_id INTEGER NOT NULL, kind TEXT NOT NULL, day TEXT NOT NULL, count INTEGER NOT NULL,
                    PRIMARY KEY(entity_id,kind,day)
                );
                CREATE INDEX idx_record_day ON record_dates(kind,day,entity_id);
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                """
            )
            builder = _FunnelStoreBuilder(store)

            def add(service: str, platform_id: Any, fields: dict[str, Any], when: Any, source: str, flags: int, kind: str = "") -> None:
                moment = _journey_date(when)
                if not moment:
                    return
                builder.add(
                    _identity_tokens(service, platform_id, fields, known_vk_platform_ids=known_vk_platform_ids),
                    moment, source, flags, service, kind,
                )

            if customer_path.exists():
                with _open_source(customer_path) as db:
                    tables = _sync_table_names(db)
                    if "cdb_vk_clients" in tables:
                        known_vk_platform_ids = {
                            clean for row in db.execute("SELECT platform_id FROM cdb_vk_clients")
                            if (clean := _strong_external_id(row[0]))
                        }
                    specs = (
                        ("cdb_avito_clients", "avito"), ("cdb_vk_clients", "vk"),
                        ("cdb_telegram_clients", "telegram"), ("cdb_bizon365_attendance", "bizon"),
                        ("cdb_getcourse_users", "getcourse_user"), ("cdb_getcourse_orders", "getcourse"),
                        ("cdb_amo_deals", "amo"),
                    )
                    for table, service in specs:
                        if table not in tables:
                            continue
                        for platform_id, raw, created_at, updated_at in db.execute(
                            f"SELECT platform_id,custom_fields,created_at,updated_at FROM {table}"
                        ):
                            fields = _parse_json_object(raw)
                            if not fields or (service == "amo" and not _is_incoming_amo_deal(fields)):
                                continue
                            flags = _service_funnel_flags(service)
                            source = _journey_source(service, fields)
                            kind = (
                                f"{service}_registration" if service in {"vk", "telegram"}
                                else f"{service}_application" if service in {"getcourse", "amo"} else ""
                            )
                            add(service, platform_id, fields, _service_event_date(service, fields, created_at, updated_at), source, flags, kind)
                            if service == "getcourse":
                                state = _order_state(fields)
                                if state in {"paid", "partial"} and _money(fields.get("payed_money")) > 0 and _is_full_tariff(fields):
                                    sale_when = (
                                        payment_dates.get((_clean(platform_id, 200), state))
                                        or finance_dates.get(_clean(platform_id, 200))
                                        or fields.get("paid_at") or fields.get("date_payment")
                                        or fields.get("status_changed_at") or fields.get("date_updated") or updated_at
                                    )
                                    sale_flag = FUNNEL_SALE_PAID if state == "paid" else FUNNEL_SALE_PARTIAL
                                    add(service, platform_id, fields, sale_when, source, sale_flag, f"getcourse_sale_{state}")
                            elif service == "amo":
                                status = _clean(fields.get("status_name"), 200).casefold()
                                if "успешно реализовано" in status or "оплатили" in status:
                                    sale_when = fields.get("closed_at_ts") or fields.get("updated_at_ts") or updated_at
                                    add(service, platform_id, fields, sale_when, source, 0, "amo_sale_paid")

            if tracker_path.exists():
                with _open_source(tracker_path) as db:
                    db.row_factory = sqlite3.Row
                    tables = _sync_table_names(db)
                    if {"events", "profiles"} <= tables:
                        profile_columns = {row[1] for row in db.execute("PRAGMA table_info(profiles)")}
                        selected = [name for name in (
                            "visit_id", "first_seen_ts", "created_at", "first_phone", "last_phone",
                            "first_email", "last_email", "first_visitor_id", "last_visitor_id",
                            "first_utm_source", "last_utm_source", "attributes_json",
                        ) if name in profile_columns]
                        profiles: dict[str, dict[str, Any]] = {}
                        if "visit_id" in selected:
                            for row in db.execute(f"SELECT {','.join(selected)} FROM profiles"):
                                item = dict(row)
                                profiles[_clean(item.get("visit_id"), 300)] = item
                                fields = {
                                    "phone": item.get("first_phone") or item.get("last_phone"),
                                    "email": item.get("first_email") or item.get("last_email"),
                                    "visitor_id": item.get("first_visitor_id") or item.get("last_visitor_id"),
                                    "utm_source": item.get("first_utm_source") or item.get("last_utm_source"),
                                    **_parse_json_object(item.get("attributes_json")),
                                }
                                add("tracker", item.get("visit_id"), fields, item.get("first_seen_ts") or item.get("created_at"), _journey_source("tracker", fields), FUNNEL_CONTACT)
                        event_columns = {row[1] for row in db.execute("PRAGMA table_info(events)")}
                        if {"visit_id", "confirmed", "created_at"} <= event_columns:
                            source_expr = "utm_source" if "utm_source" in event_columns else "''"
                            for visit_id, created_at, utm_source in db.execute(
                                f"SELECT visit_id,MIN(created_at),{source_expr} FROM events WHERE confirmed=1 GROUP BY visit_id"
                            ):
                                profile = profiles.get(_clean(visit_id, 300), {})
                                fields = {
                                    "phone": profile.get("first_phone") or profile.get("last_phone"),
                                    "email": profile.get("first_email") or profile.get("last_email"),
                                    "visitor_id": profile.get("first_visitor_id") or profile.get("last_visitor_id"),
                                    "utm_source": utm_source or profile.get("first_utm_source") or profile.get("last_utm_source"),
                                    **_parse_json_object(profile.get("attributes_json")),
                                }
                                add("tracker", visit_id, fields, created_at, _journey_source("tracker", fields), FUNNEL_CONTACT)

            coverage: dict[str, dict[str, str]] = {}
            first = store.execute("SELECT MIN(first_day),MAX(first_day) FROM entities").fetchone()
            coverage["contacts"] = {"from": first[0] or "", "to": first[1] or ""}
            for label, stages in (
                ("registrations", ("registrations",)), ("live", ("live",)),
                ("applications", ("applications",)), ("sales", ("sales_paid", "sales_partial")),
            ):
                placeholders = ",".join("?" for _ in stages)
                row = store.execute(
                    f"SELECT MIN(day),MAX(day) FROM stage_dates WHERE stage IN ({placeholders})", stages,
                ).fetchone()
                coverage[label] = {"from": row[0] or "", "to": row[1] or ""}
            entity_count = int(store.execute("SELECT COUNT(*) FROM entities").fetchone()[0])
            metadata = {
                "schema_version": FUNNEL_SCHEMA_VERSION, "source_fingerprint": fingerprint,
                "built_at_iso": built_at, "rows": str(entity_count),
                "coverage": json.dumps(coverage, ensure_ascii=False, separators=(",", ":")),
            }
            store.executemany("INSERT INTO meta(key,value) VALUES(?,?)", metadata.items())
            store.commit()
            if store.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("funnel cache quick_check failed")
        os.replace(staging, live_path)
        return {"built_at_iso": built_at, "rows": entity_count, "coverage": coverage}
    finally:
        if staging.exists():
            staging.unlink()


def _funnel_store_meta() -> dict[str, Any]:
    path = _funnel_store_path()
    if not path.exists():
        return {}
    try:
        with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=10) as db:
            values = dict(db.execute("SELECT key,value FROM meta"))
        return {
            "built_at_iso": values.get("built_at_iso", ""),
            "source_fingerprint": values.get("source_fingerprint", ""),
            "rows": int(values.get("rows") or 0),
            "coverage": _parse_json_object(values.get("coverage")),
        }
    except (OSError, sqlite3.Error, ValueError):
        return {}


async def _refresh_funnel_store(*, force: bool = False) -> dict[str, Any]:
    async with _funnel_lock:
        fingerprint = await asyncio.to_thread(_source_fingerprint)
        current = await asyncio.to_thread(_funnel_store_meta)
        if not force and current.get("source_fingerprint") == fingerprint:
            return current
        _funnel_state.update({
            "running": True, "last_started_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_error": "",
        })
        try:
            result = await asyncio.to_thread(_build_funnel_store, fingerprint)
            _funnel_state.update({
                "running": False, "last_finished_at": result["built_at_iso"],
                "last_error": "", "rows": result["rows"],
            })
            _log("info", "disk funnel index refreshed: %s entities", result["rows"])
            return {**result, "source_fingerprint": fingerprint}
        except _FunnelBuildCancelled:
            _funnel_state["running"] = False
            raise asyncio.CancelledError
        except Exception as exc:
            _funnel_state.update({"running": False, "last_error": _clean(exc, 500)})
            _log("warning", "disk funnel index failed: %s", exc, exc_info=True)
            return current


def _load_funnel_snapshot(start: date, end: date, statuses: set[str], mode: str) -> dict[str, Any]:
    path = _funnel_store_path()
    meta = _funnel_store_meta()
    snapshot: dict[str, Any] = {
        "built_at_iso": meta.get("built_at_iso", ""), "coverage": meta.get("coverage", {}),
        "entities": [], "error": _funnel_state.get("last_error", ""),
    }
    if not path.exists():
        snapshot["error"] = snapshot["error"] or "Дисковый индекс воронки ещё строится"
        return snapshot
    start_iso, end_iso = start.isoformat(), end.isoformat()
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=20) as db:
        db.row_factory = sqlite3.Row
        if mode == "through":
            rows = db.execute(
                "SELECT entity_id,first_day,source,flags,services,registration_channels "
                "FROM entities WHERE first_day BETWEEN ? AND ?", (start_iso, end_iso),
            ).fetchall()
        else:
            stages = ["registrations", "live", "applications"]
            if "paid" in statuses:
                stages.append("sales_paid")
            if "partial" in statuses:
                stages.append("sales_partial")
            placeholders = ",".join("?" for _ in stages)
            rows = db.execute(
                "SELECT entity_id,first_day,source,flags,services,registration_channels FROM entities "
                "WHERE first_day BETWEEN ? AND ? OR entity_id IN ("
                f"SELECT entity_id FROM stage_dates WHERE stage IN ({placeholders}) AND day BETWEEN ? AND ?)",
                (start_iso, end_iso, *stages, start_iso, end_iso),
            ).fetchall()
        entities: dict[int, dict[str, Any]] = {}
        for row in rows:
            services = int(row["services"] or 0)
            channel_mask = int(row["registration_channels"] or 0)
            entities[int(row["entity_id"])] = {
                "date": row["first_day"], "source": row["source"], "flags": int(row["flags"] or 0),
                "segment": _source_segment(row["source"]), "linked": services.bit_count() >= 2,
                "registration_channels": [name for name, bit in FUNNEL_CHANNELS.items() if channel_mask & bit],
                "stage_dates": {}, "record_dates": {},
            }
        if mode == "quantitative" and entities:
            ids = list(entities)
            for index in range(0, len(ids), 400):
                chunk = ids[index:index + 400]
                placeholders = ",".join("?" for _ in chunk)
                for entity_id, stage, day in db.execute(
                    f"SELECT entity_id,stage,day FROM stage_dates WHERE entity_id IN ({placeholders}) AND day BETWEEN ? AND ?",
                    (*chunk, start_iso, end_iso),
                ):
                    entities[entity_id]["stage_dates"].setdefault(stage, []).append(day)
                for entity_id, kind, day, count in db.execute(
                    f"SELECT entity_id,kind,day,count FROM record_dates WHERE entity_id IN ({placeholders}) AND day BETWEEN ? AND ?",
                    (*chunk, start_iso, end_iso),
                ):
                    entities[entity_id]["record_dates"].setdefault(kind, []).extend([day] * int(count))
        snapshot["entities"] = list(entities.values())
    return snapshot


def _conversion(value: int, base: int) -> float:
    if not base:
        return 0.0
    result = value / base * 100
    return round(result, 2 if 0 < result < 0.1 else 1)


def _selected_sale_mask(statuses: set[str]) -> int:
    mask = 0
    if "paid" in statuses:
        mask |= FUNNEL_SALE_PAID
    if "partial" in statuses:
        mask |= FUNNEL_SALE_PARTIAL
    return mask


def _coverage_payload(snapshot: dict[str, Any], start: date, end: date) -> dict[str, Any]:
    coverage = snapshot.get("coverage") if isinstance(snapshot.get("coverage"), dict) else {}
    result: dict[str, Any] = {}
    incomplete: list[str] = []
    for stage in ("contacts", "registrations", "live", "applications", "sales"):
        item = coverage.get(stage) if isinstance(coverage.get(stage), dict) else {}
        date_from, date_to = _clean(item.get("from"), 10), _clean(item.get("to"), 10)
        result[stage] = {"from": date_from, "to": date_to}
        if not date_from or not date_to or start.isoformat() < date_from or end.isoformat() > date_to:
            incomplete.append(stage)
    result["incomplete"] = incomplete
    return result


def _through_funnel(
    snapshot: dict[str, Any], start: date, end: date, statuses: set[str],
) -> dict[str, Any]:
    rows = [row for row in snapshot.get("entities", []) if start.isoformat() <= row["date"] <= end.isoformat()]
    sale_mask = _selected_sale_mask(statuses)
    stages = {
        "contacts": len(rows), "registrations": 0, "live": 0,
        "applications": 0, "sales": 0, "sales_any": 0,
    }
    sources: dict[str, dict[str, int]] = {}
    sales_segments = {"ai": 0, "baza": 0}
    registration_channels = {"vk": 0, "telegram": 0, "overlap": 0}
    linked = 0
    for row in rows:
        flags = int(row.get("flags") or 0)
        has_registration = bool(flags & FUNNEL_REGISTRATION)
        has_live = has_registration and bool(flags & FUNNEL_LIVE)
        has_application = has_live and bool(flags & FUNNEL_APPLICATION)
        has_sale = has_application and bool(flags & sale_mask)
        has_sale_any = bool(flags & sale_mask)
        channels = set(row.get("registration_channels") or []) if has_registration else set()
        registration_channels["vk"] += int("vk" in channels)
        registration_channels["telegram"] += int("telegram" in channels)
        registration_channels["overlap"] += int({"vk", "telegram"} <= channels)
        stages["registrations"] += int(has_registration)
        stages["live"] += int(has_live)
        stages["applications"] += int(has_application)
        stages["sales"] += int(has_sale)
        stages["sales_any"] += int(has_sale_any)
        segment = _clean(row.get("segment"), 10)
        if has_sale_any and segment in sales_segments:
            sales_segments[segment] += 1
        linked += int(bool(row.get("linked")))
        source = _clean(row.get("source") or "Не определён", 80)
        item = sources.setdefault(source, {
            "contacts": 0, "registrations": 0, "live": 0, "applications": 0, "sales": 0,
        })
        item["contacts"] += 1
        item["registrations"] += int(has_registration)
        item["live"] += int(has_live)
        item["applications"] += int(has_application)
        item["sales"] += int(has_sale)

    source_rows = []
    for source, counts in sorted(
        sources.items(), key=lambda item: (-item[1]["contacts"], item[0].casefold())
    )[:20]:
        source_rows.append({
            "source": source, **counts,
            "percentages": {
                "contacts": _conversion(counts["contacts"], stages["contacts"]),
                "registrations": _conversion(counts["registrations"], counts["contacts"]),
                "live": _conversion(counts["live"], counts["contacts"]),
                "applications": _conversion(counts["applications"], counts["contacts"]),
                "sales": _conversion(counts["sales"], counts["contacts"]),
            },
        })
    return {
        "mode": "through",
        "stages": stages,
        "stage_percentages": {
            key: _conversion(value, stages["contacts"])
            for key, value in stages.items() if key != "sales_any"
        },
        "conversions": {
            "contact_to_registration": _conversion(stages["registrations"], stages["contacts"]),
            "registration_to_live": _conversion(stages["live"], stages["registrations"]),
            "live_to_application": _conversion(stages["applications"], stages["live"]),
            "application_to_sale": _conversion(stages["sales"], stages["applications"]),
            "contact_to_sale": _conversion(stages["sales"], stages["contacts"]),
        },
        "dropoffs": {
            "before_registration": max(0, stages["contacts"] - stages["registrations"]),
            "before_live": max(0, stages["registrations"] - stages["live"]),
            "before_application": max(0, stages["live"] - stages["applications"]),
            "before_sale": max(0, stages["applications"] - stages["sales"]),
        },
        "sales_segments": {
            "ai": {"count": sales_segments["ai"], "percent": _conversion(sales_segments["ai"], stages["sales_any"])},
            "baza": {"count": sales_segments["baza"], "percent": _conversion(sales_segments["baza"], stages["sales_any"])},
            "total": stages["sales_any"],
        },
        "sources": source_rows,
        "details": {"registration_vk": registration_channels["vk"],
                    "registration_telegram": registration_channels["telegram"],
                    "registration_overlap": registration_channels["overlap"]},
        "data_coverage": _coverage_payload(snapshot, start, end),
        "identity": {"linked": linked, "total": len(rows), "coverage": _conversion(linked, len(rows)),
                     "built_at": snapshot.get("built_at_iso", ""), "error": snapshot.get("error", "")},
    }


def _dates_in_period(values: Any, start_iso: str, end_iso: str) -> list[str]:
    return [value for value in values if isinstance(value, str) and start_iso <= value <= end_iso] if isinstance(values, list) else []


def _quantitative_funnel(
    snapshot: dict[str, Any], start: date, end: date, statuses: set[str],
) -> dict[str, Any]:
    start_iso, end_iso = start.isoformat(), end.isoformat()
    stages = {"contacts": 0, "registrations": 0, "live": 0, "applications": 0, "sales": 0, "sales_any": 0}
    sources: dict[str, dict[str, int]] = {}
    sales_segments = {"ai": 0, "baza": 0}
    details = {
        "registration_vk": 0, "registration_telegram": 0, "registration_overlap": 0,
        "getcourse_application_orders": 0, "getcourse_sale_orders": 0,
        "getcourse_sale_users": 0, "amo_sale_users": 0,
        "sales_union_users": 0, "sales_overlap_users": 0,
        "sales_getcourse_only_users": 0, "sales_amo_only_users": 0,
    }
    linked = relevant_total = 0
    selected_sale_stages = [stage for state, stage in (("paid", "sales_paid"), ("partial", "sales_partial")) if state in statuses]
    selected_gc_kinds = [kind for state, kind in (("paid", "getcourse_sale_paid"), ("partial", "getcourse_sale_partial")) if state in statuses]
    for row in snapshot.get("entities", []):
        stage_dates = row.get("stage_dates") if isinstance(row.get("stage_dates"), dict) else {}
        record_dates = row.get("record_dates") if isinstance(row.get("record_dates"), dict) else {}
        present = {
            "contacts": start_iso <= _clean(row.get("date"), 10) <= end_iso,
            "registrations": bool(_dates_in_period(stage_dates.get("registrations"), start_iso, end_iso)),
            "live": bool(_dates_in_period(stage_dates.get("live"), start_iso, end_iso)),
            "applications": bool(_dates_in_period(stage_dates.get("applications"), start_iso, end_iso)),
            "sales": any(_dates_in_period(stage_dates.get(stage), start_iso, end_iso) for stage in selected_sale_stages),
        }
        if not any(present.values()):
            continue
        relevant_total += 1
        linked += int(bool(row.get("linked")))
        for stage, value in present.items():
            stages[stage] += int(value)
        stages["sales_any"] += int(present["sales"])
        vk_registration = bool(_dates_in_period(record_dates.get("vk_registration"), start_iso, end_iso))
        telegram_registration = bool(_dates_in_period(record_dates.get("telegram_registration"), start_iso, end_iso))
        details["registration_vk"] += int(vk_registration)
        details["registration_telegram"] += int(telegram_registration)
        details["registration_overlap"] += int(vk_registration and telegram_registration)
        gc_application_orders = len(_dates_in_period(record_dates.get("getcourse_application"), start_iso, end_iso))
        gc_sale_orders = sum(len(_dates_in_period(record_dates.get(kind), start_iso, end_iso)) for kind in selected_gc_kinds)
        gc_sale_user = bool(gc_sale_orders)
        amo_sale_user = bool(_dates_in_period(record_dates.get("amo_sale_paid"), start_iso, end_iso)) and "paid" in statuses
        details["getcourse_application_orders"] += gc_application_orders
        details["getcourse_sale_orders"] += gc_sale_orders
        details["getcourse_sale_users"] += int(gc_sale_user)
        details["amo_sale_users"] += int(amo_sale_user)
        details["sales_union_users"] += int(gc_sale_user or amo_sale_user)
        details["sales_overlap_users"] += int(gc_sale_user and amo_sale_user)
        details["sales_getcourse_only_users"] += int(gc_sale_user and not amo_sale_user)
        details["sales_amo_only_users"] += int(amo_sale_user and not gc_sale_user)
        segment = _clean(row.get("segment"), 10)
        if present["sales"] and segment in sales_segments:
            sales_segments[segment] += 1
        source = _clean(row.get("source") or "Прямой / без UTM", 80)
        item = sources.setdefault(source, {"contacts": 0, "registrations": 0, "live": 0, "applications": 0, "sales": 0})
        for stage, value in present.items():
            item[stage] += int(value)
    source_rows = []
    for source, counts in sorted(
        sources.items(), key=lambda item: (-item[1]["contacts"], -sum(item[1].values()), item[0].casefold())
    )[:20]:
        source_rows.append({
            "source": source, **counts,
            "percentages": {key: _conversion(value, stages["contacts"]) for key, value in counts.items()},
        })
    return {
        "mode": "quantitative", "stages": stages,
        "stage_percentages": {key: _conversion(value, stages["contacts"]) for key, value in stages.items() if key != "sales_any"},
        "conversions": {},
        "dropoffs": {},
        "sales_segments": {
            "ai": {"count": sales_segments["ai"], "percent": _conversion(sales_segments["ai"], stages["sales"])},
            "baza": {"count": sales_segments["baza"], "percent": _conversion(sales_segments["baza"], stages["sales"])},
            "total": stages["sales"],
        },
        "sources": source_rows, "details": details,
        "data_coverage": _coverage_payload(snapshot, start, end),
        "identity": {"linked": linked, "total": relevant_total, "coverage": _conversion(linked, relevant_total),
                     "built_at": snapshot.get("built_at_iso", ""), "error": snapshot.get("error", "")},
    }


async def _funnel_for_period(
    start: date, end: date, statuses: set[str] | None = None, mode: str = "through",
) -> dict[str, Any]:
    selected = statuses or {"paid", "partial"}
    if _funnel_cache is not None:
        snapshot = _funnel_cache
    else:
        snapshot = await asyncio.to_thread(_load_funnel_snapshot, start, end, selected, mode)
    if mode == "quantitative":
        return _quantitative_funnel(snapshot, start, end, selected)
    return _through_funnel(snapshot, start, end, selected)


async def _funnel_refresh_loop() -> None:
    await asyncio.sleep(60 if _funnel_store_path().exists() else 5)
    while True:
        try:
            await _refresh_funnel_store()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "funnel refresh loop failed: %s", exc)
        await asyncio.sleep(FUNNEL_REFRESH_SECONDS)


async def _init_db() -> None:
    if _db_path is None:
        return
    _db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(_db_path, timeout=30) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS finance_cache (
                order_id TEXT PRIMARY KEY,
                paid_at TEXT NOT NULL DEFAULT '',
                paid REAL NOT NULL DEFAULT 0,
                payment_commission REAL NOT NULL DEFAULT 0,
                received REAL NOT NULL DEFAULT 0,
                tax REAL NOT NULL DEFAULT 0,
                rest_after_ps_tax REAL NOT NULL DEFAULT 0,
                other_commissions REAL NOT NULL DEFAULT 0,
                earned REAL,
                payment_system TEXT NOT NULL DEFAULT '',
                synced_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY CHECK(id=1),
                status TEXT NOT NULL DEFAULT 'idle',
                export_id TEXT NOT NULL DEFAULT '',
                date_from TEXT NOT NULL DEFAULT '',
                date_to TEXT NOT NULL DEFAULT '',
                last_synced_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                rows_synced INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT ''
            );
            INSERT OR IGNORE INTO sync_state(id,updated_at) VALUES(1,datetime('now'));
            CREATE TABLE IF NOT EXISTS export_api_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                requested_at TEXT NOT NULL DEFAULT (datetime('now')),
                purpose TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_export_api_calls_time ON export_api_calls(requested_at);
            """
        )
        await db.commit()


async def _ensure_db() -> None:
    if _db_path is not None and not _db_path.exists():
        await _init_db()


async def _finance_rows(order_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not order_ids or _db_path is None:
        return {}
    await _ensure_db()
    result: dict[str, dict[str, Any]] = {}
    async with aiosqlite.connect(_db_path, timeout=20) as db:
        db.row_factory = aiosqlite.Row
        for index in range(0, len(order_ids), 400):
            chunk = order_ids[index:index + 400]
            cur = await db.execute(
                f"SELECT * FROM finance_cache WHERE order_id IN ({','.join('?' for _ in chunk)})", chunk
            )
            for row in await cur.fetchall():
                item = dict(row)
                result[_clean(item.get("order_id"), 200)] = item
    return result


def _walk(value: Any):
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _extract_export_id(data: Any) -> str:
    for value in _walk(data):
        if isinstance(value, dict):
            for key in ("export_id", "exportId", "id"):
                found = _clean(value.get(key), 100)
                if found and re.fullmatch(r"\d+", found):
                    return found
    return ""


def _norm_key(value: Any) -> str:
    return re.sub(r"[^a-zа-я0-9]+", "", _clean(value, 500).casefold().replace("ё", "е"))


def _looks_like_export_header(values: list[Any]) -> bool:
    normalized = {_norm_key(value) for value in values}
    id_names = {_norm_key(value) for value in ("ID заказа", "Заказ ID", "order_id", "deal_id")}
    money_names = {_norm_key(value) for value in ("Заработано", "Оплачено", "earned", "paid")}
    return bool(normalized & id_names) and bool(normalized & money_names)


def _extract_export_rows(data: Any) -> list[dict[str, Any]]:
    tables: list[list[dict[str, Any]]] = []
    for value in _walk(data):
        if isinstance(value, dict):
            fields, items = value.get("fields"), value.get("items")
            if isinstance(fields, list) and isinstance(items, list):
                headers = [_clean(field, 300) for field in fields]
                rows = [dict(item) if isinstance(item, dict) else {
                    headers[i] if i < len(headers) else str(i): cell for i, cell in enumerate(item)
                } for item in items if isinstance(item, (dict, list))]
                if rows:
                    tables.append(rows)
        if isinstance(value, list) and len(value) >= 2 and isinstance(value[0], list) and _looks_like_export_header(value[0]):
            headers = [_clean(field, 300) for field in value[0]]
            rows = [{headers[i] if i < len(headers) else str(i): cell for i, cell in enumerate(item)}
                    for item in value[1:] if isinstance(item, list)]
            if rows:
                tables.append(rows)
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            if any(_row_value(item, ("ID заказа", "order_id", "deal_id")) for item in value):
                tables.append([dict(item) for item in value])
    return max(tables, key=len) if tables else []


def _row_value(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    wanted = {_norm_key(alias) for alias in aliases}
    flattened: list[tuple[str, Any]] = []
    for value in _walk(row):
        if isinstance(value, dict):
            flattened.extend((_norm_key(key), cell) for key, cell in value.items())
    for key, cell in flattened:
        if key in wanted and cell not in (None, ""):
            return cell
    for key, cell in flattened:
        if cell not in (None, "") and any(len(alias) >= 6 and alias in key for alias in wanted):
            return cell
    return ""


EXPORT_ALIASES = {
    "order_id": ("ID заказа", "Заказ ID", "order_id", "deal_id"),
    "paid_at": ("Дата оплаты", "paid_at", "payed_at"),
    "paid": ("Оплачено", "Оплачено руб", "paid", "payed"),
    "payment_commission": ("Комиссия платежной системы", "payment_system_commission", "payment_commission"),
    "received": ("Получено", "received"),
    "tax": ("Налог", "tax"),
    "rest_after_ps_tax": ("Осталось после вычета комиссии платежной системы и налога", "rest_after_payment_commission_and_tax"),
    "other_commissions": ("Другие комиссии", "other_commissions"),
    "earned": ("Заработано", "earned"),
    "payment_system": ("Платежная система", "payment_system"),
}


def _finance_from_export_row(row: dict[str, Any]) -> dict[str, Any] | None:
    order_id = _clean(_row_value(row, EXPORT_ALIASES["order_id"]), 200)
    earned_raw = _row_value(row, EXPORT_ALIASES["earned"])
    if not order_id or earned_raw in (None, ""):
        return None
    return {
        "order_id": order_id,
        "paid_at": _clean(_row_value(row, EXPORT_ALIASES["paid_at"]), 100),
        "paid": float(_money(_row_value(row, EXPORT_ALIASES["paid"]))),
        "payment_commission": float(_money(_row_value(row, EXPORT_ALIASES["payment_commission"]))),
        "received": float(_money(_row_value(row, EXPORT_ALIASES["received"]))),
        "tax": float(_money(_row_value(row, EXPORT_ALIASES["tax"]))),
        "rest_after_ps_tax": float(_money(_row_value(row, EXPORT_ALIASES["rest_after_ps_tax"]))),
        "other_commissions": float(_money(_row_value(row, EXPORT_ALIASES["other_commissions"]))),
        "earned": float(_money(earned_raw)),
        "payment_system": _clean(_row_value(row, EXPORT_ALIASES["payment_system"]), 300),
    }


def _gc_config() -> tuple[str, str]:
    account = os.environ.get("GETCOURSE_ACCOUNT_NAME", "").strip()
    token = os.environ.get("GETCOURSE_API_TOKEN", "").strip()
    if account.startswith(("http://", "https://")):
        base = account.rstrip("/")
    elif "." in account:
        base = "https://" + account.rstrip("/")
    else:
        base = f"https://{account}.getcourse.ru" if account else ""
    return base, token


async def _calls_used() -> int:
    if _db_path is None:
        return 0
    await _ensure_db()
    async with aiosqlite.connect(_db_path, timeout=20) as db:
        cur = await db.execute("SELECT COUNT(*) FROM export_api_calls WHERE datetime(requested_at)>=datetime('now','-2 hours')")
        row = await cur.fetchone()
        return int((row or [0])[0] or 0)


async def _api_get(path: str, params: dict[str, Any], purpose: str) -> Any:
    base, token = _gc_config()
    if not base or not token:
        raise RuntimeError("GETCOURSE_ACCOUNT_NAME/GETCOURSE_API_TOKEN не настроены")
    if await _calls_used() >= EXPORT_CALL_LIMIT_2H:
        raise RuntimeError("лимит синхронизации GetCourse исчерпан; повторим позже")
    assert _db_path is not None
    async with aiosqlite.connect(_db_path, timeout=20) as db:
        await db.execute("INSERT INTO export_api_calls(purpose) VALUES(?)", (_clean(purpose, 100),))
        await db.commit()
    query = {"key": token, **params}
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.get(base + path, params=query)
    if response.status_code >= 400:
        raise RuntimeError(f"GetCourse HTTP {response.status_code}")
    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError("GetCourse вернул некорректный JSON") from exc


async def _sync_state() -> dict[str, Any]:
    configured = all(_gc_config())
    if _db_path is None:
        return {"configured": configured, "status": "idle", "coverage_rows": 0, "budget_left": EXPORT_CALL_LIMIT_2H}
    await _ensure_db()
    async with aiosqlite.connect(_db_path, timeout=20) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM sync_state WHERE id=1")
        row = dict(await cur.fetchone() or {})
        cur = await db.execute("SELECT COUNT(*) FROM finance_cache WHERE earned IS NOT NULL")
        cached = int((await cur.fetchone() or [0])[0] or 0)
    row.update({"configured": configured, "coverage_rows": cached, "budget_left": max(0, EXPORT_CALL_LIMIT_2H - await _calls_used())})
    return row


async def _write_sync_state(**values: Any) -> None:
    if _db_path is None:
        return
    allowed = {"status", "export_id", "date_from", "date_to", "last_synced_at", "last_error", "rows_synced", "updated_at"}
    pairs = [(key, value) for key, value in values.items() if key in allowed]
    if not pairs:
        return
    pairs.append(("updated_at", datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")))
    async with aiosqlite.connect(_db_path, timeout=20) as db:
        await db.execute(f"UPDATE sync_state SET {','.join(f'{key}=?' for key, _ in pairs)} WHERE id=1", tuple(value for _, value in pairs))
        await db.commit()


def _export_ready(data: Any) -> bool:
    for value in _walk(data):
        if isinstance(value, dict):
            status = _clean(value.get("status") or value.get("state"), 100).casefold()
            if status in {"success", "done", "ready", "finished", "completed"}:
                return True
    return False


async def _save_finance(rows: list[dict[str, Any]]) -> int:
    parsed = [item for row in rows if (item := _finance_from_export_row(row))]
    if not parsed or _db_path is None:
        return 0
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiosqlite.connect(_db_path, timeout=30) as db:
        for item in parsed:
            await db.execute(
                """
                INSERT INTO finance_cache(order_id,paid_at,paid,payment_commission,received,tax,rest_after_ps_tax,other_commissions,earned,payment_system,synced_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(order_id) DO UPDATE SET paid_at=excluded.paid_at,paid=excluded.paid,
                payment_commission=excluded.payment_commission,received=excluded.received,tax=excluded.tax,
                rest_after_ps_tax=excluded.rest_after_ps_tax,other_commissions=excluded.other_commissions,
                earned=excluded.earned,payment_system=excluded.payment_system,synced_at=excluded.synced_at
                """,
                (item["order_id"], item["paid_at"], item["paid"], item["payment_commission"], item["received"],
                 item["tax"], item["rest_after_ps_tax"], item["other_commissions"], item["earned"], item["payment_system"], now),
            )
        await db.commit()
    return len(parsed)


async def _sync_step(*, force: bool = False) -> dict[str, Any]:
    async with _sync_lock:
        await _ensure_db()
        state = await _sync_state()
        if not state.get("configured"):
            return state
        try:
            if state.get("status") == "pending" and state.get("export_id"):
                export_id = urllib.parse.quote(_clean(state["export_id"], 100))
                data = await _api_get(f"/pl/api/account/exports/{export_id}", {}, "finance:poll")
                rows = _extract_export_rows(data)
                if rows or _export_ready(data):
                    saved = await _save_finance(rows)
                    await _write_sync_state(status="idle", export_id="", last_synced_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"), last_error="", rows_synced=saved)
                return await _sync_state()
            last = _parse_datetime(state.get("last_synced_at"))
            due = not last or datetime.now(UTC) - last >= timedelta(hours=EXPORT_REFRESH_HOURS)
            if not force and not due:
                return state
            end = datetime.now(MOSCOW_TZ).date()
            start = end - timedelta(days=364 if not last else 30)
            data = await _api_get("/pl/api/account/deals", {
                "payed_at[from]": start.isoformat(), "payed_at[to]": end.isoformat(),
            }, "finance:start")
            rows = _extract_export_rows(data)
            if rows:
                saved = await _save_finance(rows)
                await _write_sync_state(status="idle", export_id="", date_from=start.isoformat(), date_to=end.isoformat(), last_synced_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"), last_error="", rows_synced=saved)
            else:
                export_id = _extract_export_id(data)
                if not export_id:
                    raise RuntimeError("GetCourse не вернул идентификатор экспорта")
                await _write_sync_state(status="pending", export_id=export_id, date_from=start.isoformat(), date_to=end.isoformat(), last_error="")
        except Exception as exc:
            message = _clean(exc, 1000)
            retry_pending = state.get("status") == "pending" and not re.search(r"HTTP 4\d\d", message)
            await _write_sync_state(status="pending" if retry_pending else "error", last_error=message)
            _log("warning", "finance sync step failed: %s", exc)
        return await _sync_state()


async def _sync_loop() -> None:
    await asyncio.sleep(8)
    while True:
        try:
            await _sync_step()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("warning", "finance sync loop failed: %s", exc)
        await asyncio.sleep(60)


async def _build_analytics(payload: dict[str, Any], *, today: date | None = None) -> dict[str, Any]:
    preset, start, end = _period(payload, today=today)
    statuses = {_clean(item, 30).casefold() for item in (
        payload.get("statuses") if isinstance(payload.get("statuses"), list) else ["paid"]
    )} & ALLOWED_STATUSES
    if not statuses:
        raise HTTPException(400, "Выберите хотя бы одно состояние заказа")
    filters = _filters_payload(payload.get("filters"))
    records, source_counts = await _read_order_records()
    relevant = [row for row in records if _order_state(row["fields"]) in statuses]
    event_dates = await _first_event_dates([row["platform_id"] for row in relevant], statuses & {"paid", "partial"})
    finance = await _finance_rows([row["platform_id"] for row in relevant])
    registration_days, registrations_read = await _registration_buckets(start, end)
    buckets = {start + timedelta(days=i): {"orders": 0, "paid": Decimal("0"), "earned": Decimal("0"), "earned_orders": 0, "missing_earned": 0}
               for i in range((end - start).days + 1)}
    profile_options = {field: Counter() for field in PROFILE_UTM_FIELDS}
    order_options = {field: Counter() for field in ORDER_UTM_FIELDS}
    product_options: Counter[str] = Counter()
    approximate_dates = exact_dates = missing_dates = 0
    dated_rows: list[tuple[dict[str, Any], date, bool]] = []
    for row in relevant:
        state = _order_state(row["fields"])
        day, approximate = _record_date(row, state, event_dates)
        if day is None:
            missing_dates += 1
            continue
        if not start <= day <= end:
            continue
        dated_rows.append((row, day, approximate))
        fields = row["fields"]
        for field in PROFILE_UTM_FIELDS:
            if value := _clean(fields.get(field), 500):
                profile_options[field][value] += 1
        for field in ORDER_UTM_FIELDS:
            if value := _clean(fields.get(field), 500):
                order_options[field][value] += 1
        if product := _clean(fields.get("title"), 500):
            product_options[product] += 1
    for row, day, approximate in dated_rows:
        fields = row["fields"]
        if not _matches_filters(fields, filters):
            continue
        bucket = buckets[day]
        bucket["orders"] += 1
        bucket["paid"] += max(Decimal("0"), _money(fields.get("payed_money")))
        exact = finance.get(row["platform_id"])
        if exact and exact.get("earned") is not None:
            bucket["earned"] += max(Decimal("0"), _money(exact["earned"]))
            bucket["earned_orders"] += 1
        elif _order_state(fields) in {"paid", "partial"} and _money(fields.get("payed_money")) > 0:
            bucket["missing_earned"] += 1
        approximate_dates += int(approximate)
        exact_dates += int(not approximate)
    points = [{"date": day.isoformat(), "orders": b["orders"], "amount": float(b["paid"])} for day, b in buckets.items()]
    earned_points = [{"date": day.isoformat(), "value": float(b["earned"])} for day, b in buckets.items()]
    paid_points = [{"date": day.isoformat(), "value": float(b["paid"])} for day, b in buckets.items()]
    order_points = [{"date": day.isoformat(), "value": b["orders"]} for day, b in buckets.items()]
    registration_points = [{"date": day.isoformat(), "vk": registration_days[day]["vk"], "tg": registration_days[day]["tg"],
                            "total": registration_days[day]["vk"] + registration_days[day]["tg"]} for day in buckets]
    total_orders = sum(b["orders"] for b in buckets.values())
    total_paid = sum((b["paid"] for b in buckets.values()), Decimal("0"))
    total_earned = sum((b["earned"] for b in buckets.values()), Decimal("0"))
    earned_orders = sum(b["earned_orders"] for b in buckets.values())
    missing_earned = sum(b["missing_earned"] for b in buckets.values())
    total_vk = sum(item["vk"] for item in registration_points)
    total_tg = sum(item["tg"] for item in registration_points)
    warnings: list[str] = []
    if approximate_dates:
        warnings.append(f"У {approximate_dates} заказов дата рассчитана по резервному полю")
    if missing_dates:
        warnings.append(f"У {missing_dates} заказов нет пригодной даты")
    if missing_earned:
        warnings.append(f"Для {missing_earned} оплаченных заказов точное «Заработано» ещё не синхронизировано")
    if not _customer_db_path().exists():
        warnings.append("База customer-db не найдена")
    if not _tracker_db_path().exists():
        warnings.append("База tracker не найдена — часть касаний сквозной воронки недоступна")
    sync = await _sync_state()
    funnel_mode = _clean(payload.get("funnel_mode"), 30).casefold()
    if funnel_mode not in {"quantitative", "through"}:
        funnel_mode = "quantitative"
    funnel = await _funnel_for_period(start, end, statuses, funnel_mode)
    return {
        "timezone": "Europe/Moscow",
        "period": {"preset": preset, "date_from": start.isoformat(), "date_to": end.isoformat()},
        "selection": {"statuses": sorted(statuses)},
        "totals": {"orders": total_orders, "amount": float(total_paid), "earned": float(total_earned), "registrations": total_vk + total_tg},
        "points": points,
        "charts": {
            "earned": {"points": earned_points, "total": float(total_earned), "exact_orders": earned_orders, "missing_orders": missing_earned},
            "paid": {"points": paid_points, "total": float(total_paid)},
            "orders": {"points": order_points, "total": total_orders},
            "registrations": {"points": registration_points, "totals": {"vk": total_vk, "tg": total_tg, "total": total_vk + total_tg}},
        },
        "filter_options": {
            "profile_utm": {field: _option_list(profile_options[field]) for field in PROFILE_UTM_FIELDS},
            "order_utm": {field: _option_list(order_options[field]) for field in ORDER_UTM_FIELDS},
            "products": _option_list(product_options),
        },
        "data_quality": {"exact_dates": exact_dates, "approximate_dates": approximate_dates, "missing_dates": missing_dates,
                         "exact_earned_orders": earned_orders, "missing_earned_orders": missing_earned,
                         "registration_client_rows_scanned": registrations_read, **source_counts},
        "sync_status": sync,
        "funnel": funnel,
        "warnings": warnings,
    }


@router.get("/health")
async def health() -> dict[str, Any]:
    customer = _customer_db_path()
    funnel_meta = await asyncio.to_thread(_funnel_store_meta)
    return {"ok": customer.exists(), "module": MODULE_ID, "customer_db": customer.exists(),
            "orders_db": _orders_db_path().exists(), "tracker_db": _tracker_db_path().exists(),
            "archive_db": _archive_db_path(customer).exists(), "finance_sync": await _sync_state(),
            "funnel_index": {**funnel_meta, **_funnel_state, "exists": _funnel_store_path().exists()}}


@router.get("/sync-status")
async def sync_status(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    return await _sync_state()


@router.post("/sync")
async def sync_finance(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    return await _sync_step(force=True)


@router.post("/analytics")
async def analytics(request: Request) -> dict[str, Any]:
    await _require_panel_user(request)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(400, "Ожидается JSON-объект") from exc
    if not isinstance(payload, dict):
        raise HTTPException(400, "Ожидается JSON-объект")
    try:
        return await _build_analytics(payload)
    except HTTPException:
        raise
    except Exception as exc:
        _log("error", "analytics failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Не удалось построить аналитику") from exc
