from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

_ctx = None
_db_path: Path | None = None
_logger: logging.Logger | None = None

SENLER_API = "https://senler.ru/api"
SENLER_V = "2"
_EP_SUB_ADD = f"{SENLER_API}/subscribers/add"
_EP_SUB_DEL = f"{SENLER_API}/subscribers/del"
_EP_SUB_GET = f"{SENLER_API}/subscribers/get"
_EP_VAR_SET = f"{SENLER_API}/vars/set"

TABLE_NAME = "getcourse_orders"
TABLE_DISPLAY_NAME = "Заказы GetCourse"
PROCESSABLE_PAYMENT_STATES = {"paid", "partial", "unpaid"}
MODULE_ID = "getcourse-orders"

DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "paid_statuses": "Оплачен\nОплачено\npaid\nsuccess",
    "partial_statuses": "Частично оплачен\nЧастично оплачено\npartial",
    "unpaid_statuses": "Не оплачен\nНе оплачено\nНовый\nСоздан\nСоздан заказ\nnew\ncreated",
    "vk_fields": "utmT,utm_term,user_term",
    "request_timeout": "12",
}


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def setup(ctx):
    global _ctx, _db_path, _logger
    _ctx = ctx
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.getcourse-orders"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS rules (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                name             TEXT NOT NULL DEFAULT '',
                subscription_id  TEXT NOT NULL DEFAULT '',
                conditions_json  TEXT NOT NULL DEFAULT '{"mode":"and","conditions":[]}',
                exclusive_groups INTEGER NOT NULL DEFAULT 0,
                active           INTEGER NOT NULL DEFAULT 1,
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS events (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                method             TEXT NOT NULL DEFAULT '',
                order_id           TEXT NOT NULL DEFAULT '',
                platform_id        TEXT NOT NULL DEFAULT '',
                payment_state      TEXT NOT NULL DEFAULT '',
                status             TEXT NOT NULL DEFAULT '',
                vk_id              TEXT NOT NULL DEFAULT '',
                customer_record_id INTEGER,
                rules_matched_json TEXT NOT NULL DEFAULT '[]',
                success            INTEGER NOT NULL DEFAULT 0,
                ignored            INTEGER NOT NULL DEFAULT 0,
                error              TEXT NOT NULL DEFAULT '',
                details            TEXT NOT NULL DEFAULT '',
                raw_payload        TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_events_order ON events(order_id);
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_rules_active ON rules(active);
            """
        )
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                (key, value),
            )
        if not _env()["webhook_secret"]:
            cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
            row = await cur.fetchone()
            if not row or not _clean(row[0]):
                await db.execute(
                    "INSERT INTO settings(key,value) VALUES('webhook_secret',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (secrets.token_urlsafe(24),),
                )
        await db.commit()
    _log("info", "getcourse-orders DB initialized")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def _mask_secret(data: dict[str, Any]) -> dict[str, Any]:
    result = dict(data)
    for key in ("secret", "webhook_secret", "access_token"):
        if key in result and result[key]:
            result[key] = "***"
    return result


def _env() -> dict[str, str]:
    return {
        "senler_token": os.environ.get("SENLER_ACCESS_TOKEN", "").strip(),
        "senler_group_id": os.environ.get("SENLER_GROUP_ID", "").strip(),
        "webhook_secret": os.environ.get("GETCOURSE_ORDERS_WEBHOOK_SECRET", "").strip(),
        "customer_db_path": os.environ.get("GETCOURSE_CUSTOMER_DB_PATH", "").strip(),
    }


async def _settings_map() -> dict[str, str]:
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data = DEFAULT_SETTINGS.copy()
    data.update({row[0]: row[1] for row in rows})
    if _env()["webhook_secret"]:
        data["webhook_secret"] = _env()["webhook_secret"]
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, str]:
    allowed = {"webhook_secret", "paid_statuses", "partial_statuses", "unpaid_statuses", "vk_fields", "request_timeout"}
    async with aiosqlite.connect(_db_path) as db:
        for key in allowed:
            if key not in data:
                continue
            value = _clean(data.get(key), 5000)
            if key == "request_timeout":
                try:
                    value = str(max(5, min(45, int(value))))
                except Exception:
                    value = DEFAULT_SETTINGS[key]
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    return await _settings_map()


def _timeout(settings: dict[str, str]) -> int:
    try:
        return max(5, min(45, int(settings.get("request_timeout") or "12")))
    except Exception:
        return 12


def _split_values(value: str) -> list[str]:
    return [item.strip().casefold() for item in re.split(r"[\n,;]+", value or "") if item.strip()]


def _payment_state(status: str, settings: dict[str, str]) -> str:
    normalized = _clean(status, 300).casefold()
    if not normalized:
        return "unknown"
    if normalized in _split_values(settings.get("paid_statuses", "")):
        return "paid"
    if normalized in _split_values(settings.get("partial_statuses", "")):
        return "partial"
    if normalized in _split_values(settings.get("unpaid_statuses", "")):
        return "unpaid"
    return "unknown"


def _money(value: Any) -> float:
    raw = _clean(value, 80).replace(" ", "").replace("\u00a0", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except Exception:
        return 0.0


def _money_text(value: Any) -> str:
    amount = _number_value(value)
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def _phone_text(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value, 100))
    if not digits:
        return ""
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return "+" + digits


def _jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    raw = _clean(value, 20000)
    if not raw:
        return ""
    for candidate in (raw, raw.replace('\\"', '"')):
        try:
            return json.loads(candidate)
        except Exception:
            pass
    return raw


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def _deal_name_text(value: Any) -> str:
    text = re.sub(r"\s+", " ", _flatten_text(value)).strip()
    text = re.sub(r"(?i)(автооплата|autopay)\s+\d{5,}\s*$", r"\1", text).strip()
    text = re.sub(r"\s+\d{5,}\s*$", "", text).strip()
    return text


def _extract_vk_id(payload: dict[str, Any], settings: dict[str, str]) -> str:
    fields = [item.strip() for item in re.split(r"[\n,;]+", settings.get("vk_fields", "")) if item.strip()]
    fields.extend(["utmT", "utm_term", "user_term"])
    seen = set()
    for field in fields:
        if field in seen:
            continue
        seen.add(field)
        value = _clean(payload.get(field), 500)
        if not value:
            continue
        parsed = urlparse(value)
        if parsed.query:
            qs = parse_qs(parsed.query)
            for key in ("utm_term", "utmT", "user_term", field):
                if qs.get(key) and qs[key][0]:
                    return _clean(qs[key][0], 100)
        if "=" in value and "&" in value:
            qs = parse_qs(value)
            for key in ("utm_term", "utmT", "user_term", field):
                if qs.get(key) and qs[key][0]:
                    return _clean(qs[key][0], 100)
        return value
    return ""


def _normalize_order(payload: dict[str, Any], settings: dict[str, str], forced_payment_state: str = "") -> dict[str, Any]:
    positions = _jsonish(payload.get("positions", ""))
    offers = _jsonish(payload.get("offers", ""))
    status = _clean(payload.get("status"), 300)
    order_id = _clean(payload.get("order_id") or payload.get("object.id"), 100)
    number = _clean(payload.get("number"), 100)
    platform_id = order_id or number
    cost_money = _money(payload.get("costMoney") or payload.get("cost_money"))
    left_cost_money = _money(payload.get("leftCostMoney") or payload.get("left_cost_money"))
    payed_money = _money(payload.get("payedMoney") or payload.get("payed_money"))
    title = " ".join(part for part in (_flatten_text(positions), _flatten_text(offers)) if part).strip()
    payment_state = forced_payment_state if forced_payment_state in PROCESSABLE_PAYMENT_STATES else _payment_state(status, settings)
    vk_id = _extract_vk_id(payload, settings)

    fields = {
        "number": number,
        "order_id": order_id,
        "gc_user_id": _clean(payload.get("id"), 100),
        "status": status,
        "payment_state": payment_state,
        "payment_state_source": f"webhook/{forced_payment_state}" if forced_payment_state else "status",
        "positions": positions,
        "offers": offers,
        "title": title,
        "cost_money": cost_money,
        "left_cost_money": left_cost_money,
        "payed_money": payed_money,
        "payment_link": _clean(payload.get("paymentLink") or payload.get("payment_link"), 2000),
        "first_name": _clean(payload.get("firstName"), 300),
        "last_name": _clean(payload.get("lastName"), 300),
        "name": _clean(payload.get("name"), 500),
        "email": _clean(payload.get("email"), 500),
        "phone": _clean(payload.get("phone"), 100),
        "manager_name": _clean(payload.get("manager_name"), 500),
        "manager_email": _clean(payload.get("manager_email"), 500),
        "manager_phone": _clean(payload.get("manager_phone"), 100),
        "avatar_url": _clean(payload.get("avatarUrl") or payload.get("avatar_url"), 2000),
        "utm_source": _clean(payload.get("utmS") or payload.get("utm_source"), 500),
        "utm_medium": _clean(payload.get("utmM") or payload.get("utm_medium"), 500),
        "utm_campaign": _clean(payload.get("utmCa") or payload.get("utm_campaign"), 500),
        "utm_content": _clean(payload.get("utmCo") or payload.get("utm_content"), 500),
        "utm_term": _clean(payload.get("utmT") or payload.get("utm_term"), 500),
        "vk_id": vk_id,
        "user_yclid": _clean(payload.get("user_yclid"), 500),
        "user_ym_uid": _clean(payload.get("user_ym_uid"), 500),
        "user_source": _clean(payload.get("user_source"), 500),
        "user_content": _clean(payload.get("user_content"), 500),
        "user_campaign": _clean(payload.get("user_campaign"), 500),
        "user_term": _clean(payload.get("user_term"), 500),
        "user_medium": _clean(payload.get("user_medium"), 500),
        "received_at": _now(),
        "raw_payload": _mask_secret(payload),
    }
    return {
        "platform_id": platform_id,
        "order_id": order_id,
        "status": status,
        "payment_state": payment_state,
        "vk_id": vk_id,
        "custom_fields": fields,
    }


def _customer_db_path() -> Path:
    env_path = _env()["customer_db_path"]
    if env_path:
        return Path(env_path)
    if not _ctx:
        raise RuntimeError("module context is not initialized")
    module_dir = Path(_ctx.module_dir)
    candidates = [
        module_dir.parent / "customer-db" / "data" / "customer-db.db",
        module_dir.parent / "module_customer_db" / "data" / "customer-db.db",
        module_dir.parent.parent / "modules" / "customer-db" / "data" / "customer-db.db",
        module_dir.parent.parent / "module_customer_db" / "data" / "customer-db.db",
    ]
    for candidate in candidates:
        if candidate.exists() or candidate.parent.exists():
            return candidate
    return candidates[0]


async def _ensure_customer_table(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS _cdb_tables (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            description  TEXT DEFAULT '',
            schema_json  TEXT NOT NULL DEFAULT '[]',
            created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE TABLE IF NOT EXISTS cdb_getcourse_orders (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            platform_id   TEXT NOT NULL DEFAULT '',
            custom_fields TEXT NOT NULL DEFAULT '{}',
            created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        """
    )
    schema = [
        {"name": "status", "label": "Статус", "type": "text"},
        {"name": "payment_state", "label": "Оплата", "type": "text"},
        {"name": "title", "label": "Название", "type": "text"},
        {"name": "cost_money", "label": "Сумма", "type": "number"},
        {"name": "payed_money", "label": "Оплачено", "type": "number"},
        {"name": "vk_id", "label": "VK ID", "type": "text"},
    ]
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
            TABLE_NAME,
            TABLE_DISPLAY_NAME,
            "Заказы GetCourse, принятые webhook-модулем",
            json.dumps(schema, ensure_ascii=False),
        ),
    )


async def _upsert_customer_order(order: dict[str, Any]) -> dict[str, Any]:
    db_path = _customer_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        await _ensure_customer_table(db)
        platform_id = order["platform_id"]
        if not platform_id:
            raise ValueError("order_id или number обязателен для platform_id")
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT id FROM cdb_{TABLE_NAME} WHERE platform_id=? ORDER BY id DESC",
            (platform_id,),
        )
        rows = await cur.fetchall()
        payload = json.dumps(order["custom_fields"], ensure_ascii=False)
        duplicate_count = max(0, len(rows) - 1)
        if rows:
            record_id = int(rows[0]["id"])
            await db.execute(
                f"""
                UPDATE cdb_{TABLE_NAME}
                SET platform_id=?, custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (platform_id, payload, record_id),
            )
            action = "updated"
        else:
            cur = await db.execute(
                f"INSERT INTO cdb_{TABLE_NAME}(platform_id,custom_fields) VALUES(?,?)",
                (platform_id, payload),
            )
            record_id = int(cur.lastrowid)
            action = "created"
        await db.commit()
    return {
        "action": action,
        "record_id": record_id,
        "duplicate_count": duplicate_count,
        "db_path": str(db_path),
    }


def _field_value(order: dict[str, Any], field: str) -> Any:
    fields = order.get("custom_fields", {})
    aliases = {
        "order_name": "title",
        "name_order": "title",
        "payment": "payment_state",
        "paid": "payment_state",
        "costMoney": "cost_money",
        "payedMoney": "payed_money",
        "leftCostMoney": "left_cost_money",
    }
    key = aliases.get(field, field)
    if key in order:
        return order.get(key)
    return fields.get(key, "")


def _number_value(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return _money(value)


def _condition_ok(order: dict[str, Any], condition: dict[str, Any]) -> bool:
    field = _clean(condition.get("field"), 80)
    op = _clean(condition.get("op") or condition.get("operator"), 40)
    expected = condition.get("value", "")
    actual = _field_value(order, field)
    actual_text = _flatten_text(actual).casefold()
    expected_text = _clean(expected, 2000).casefold()

    if op == "exists":
        return bool(_flatten_text(actual).strip())
    if op == "not_exists":
        return not bool(_flatten_text(actual).strip())
    if op == "contains":
        return bool(expected_text and expected_text in actual_text)
    if op == "not_contains":
        return bool(expected_text and expected_text not in actual_text)
    if op == "equals":
        return actual_text == expected_text
    if op == "not_equals":
        return actual_text != expected_text
    if op == "greater_than":
        return _number_value(actual) > _number_value(expected)
    if op == "less_than":
        return _number_value(actual) < _number_value(expected)
    return False


def _conditions_payload(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"mode": "and", "conditions": []}
    mode = "or" if _clean(data.get("mode"), 10).lower() == "or" else "and"
    conditions = data.get("conditions")
    if not isinstance(conditions, list):
        conditions = []
    clean_conditions = []
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        field = _clean(condition.get("field"), 80)
        op = _clean(condition.get("op") or condition.get("operator"), 40)
        if not field or not op:
            continue
        clean_conditions.append({
            "field": field,
            "op": op,
            "value": _clean(condition.get("value"), 2000),
        })
    return {"mode": mode, "conditions": clean_conditions}


def _rule_matches(order: dict[str, Any], rule: dict[str, Any]) -> bool:
    try:
        payload = json.loads(rule.get("conditions_json") or "{}")
    except Exception:
        payload = {}
    payload = _conditions_payload(payload)
    conditions = payload["conditions"]
    if not conditions:
        return False
    checks = [_condition_ok(order, condition) for condition in conditions]
    return any(checks) if payload["mode"] == "or" else all(checks)


async def _active_rules() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM rules WHERE active=1 ORDER BY id")
        return [dict(row) for row in await cur.fetchall()]


async def _all_active_subscription_ids(except_ids: set[str] | None = None) -> list[str]:
    except_ids = except_ids or set()
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            "SELECT DISTINCT subscription_id FROM rules WHERE active=1 AND subscription_id<>'' ORDER BY subscription_id"
        )
        ids = [_clean(row[0], 80) for row in await cur.fetchall()]
    return [item for item in ids if item and item not in except_ids]


async def _senler_check(access_token: str, group_id: str, subscription_id: str, vk_id: str, timeout: int) -> tuple[bool | None, dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                _EP_SUB_GET,
                data={
                    "access_token": access_token,
                    "group_id": group_id,
                    "subscription_id": subscription_id,
                    "vk_user_id": vk_id,
                    "v": SENLER_V,
                },
            )
        body = resp.json()
        items = body.get("items") or body.get("users") or body.get("response") or []
        return bool(isinstance(items, list) and items), {"http_status": resp.status_code, "response": body}
    except Exception as exc:
        return None, {"error": str(exc)}


async def _senler_post(endpoint: str, data: dict[str, Any], timeout: int) -> tuple[bool, str, dict[str, Any]]:
    safe_params = {k: ("***" if k == "access_token" else v) for k, v in data.items()}
    raw = ""
    status_code = 0
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(endpoint, data=data)
        status_code = resp.status_code
        raw = resp.text[:2000]
        try:
            body = resp.json()
        except Exception:
            return False, f"ответ не JSON (HTTP {status_code})", {
                "http_status": status_code,
                "response": raw,
                "params": safe_params,
            }
        subscribers = body.get("subscribers")
        if isinstance(subscribers, list):
            failures = []
            for item in subscribers:
                if not isinstance(item, dict) or item.get("success") is not False:
                    continue
                msg = _clean(item.get("error") or item.get("error_message") or item, 300)
                low = msg.casefold()
                if endpoint == _EP_SUB_ADD and ("already" in low or "уже" in low):
                    continue
                if endpoint == _EP_SUB_DEL and (
                    "not found" in low or "not subscribed" in low or "не найден" in low or "не подпис" in low
                ):
                    continue
                failures.append(msg)
            if failures:
                return False, "; ".join(failures), {
                    "http_status": status_code,
                    "response": body,
                    "params": safe_params,
                }
        if body.get("success"):
            return True, "", {"http_status": status_code, "response": body, "params": safe_params}
        err = body.get("error", {})
        msg = (
            body.get("error_message")
            or (err.get("error_msg") if isinstance(err, dict) else "")
            or (str(err) if err else "")
            or str(body)
        )
        return False, msg, {"http_status": status_code, "response": body, "params": safe_params}
    except Exception as exc:
        return False, str(exc), {
            "http_status": status_code,
            "response": raw,
            "exception": str(exc),
            "params": safe_params,
        }


async def _senler_add(subscription_id: str, vk_id: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    timeout = _timeout(settings)
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN или SENLER_GROUP_ID не заданы", {}
    already, check_details = await _senler_check(
        env["senler_token"], env["senler_group_id"], subscription_id, vk_id, timeout
    )
    details: dict[str, Any] = {"check": check_details, "add": None}
    if already is True:
        details["add"] = {"skipped": True, "reason": "уже подписан"}
        return True, "", details
    ok, error, add_details = await _senler_post(
        _EP_SUB_ADD,
        {
            "access_token": env["senler_token"],
            "group_id": env["senler_group_id"],
            "subscription_id": subscription_id,
            "vk_user_id": vk_id,
            "v": SENLER_V,
        },
        timeout,
    )
    details["add"] = add_details
    return ok, error, details


async def _senler_del(subscription_id: str, vk_id: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    timeout = _timeout(settings)
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN или SENLER_GROUP_ID не заданы", {}
    return await _senler_post(
        _EP_SUB_DEL,
        {
            "access_token": env["senler_token"],
            "group_id": env["senler_group_id"],
            "subscription_id": subscription_id,
            "vk_user_id": vk_id,
            "v": SENLER_V,
        },
        timeout,
    )


async def _senler_set_order_vars(order: dict[str, Any], vk_id: str, settings: dict[str, str]) -> tuple[bool, str, list[dict[str, Any]]]:
    env = _env()
    timeout = _timeout(settings)
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN или SENLER_GROUP_ID не заданы", []
    fields = order.get("custom_fields", {})
    vars_to_set = [
        ("getcourse_deal_name", _clean(_deal_name_text(fields.get("title")), 2000)),
        ("getcourse_deal_number", _clean(fields.get("number"), 200)),
        ("getcourse_deal_cost", _money_text(fields.get("cost_money"))),
        ("getcourse_deal_payed", _money_text(fields.get("payed_money"))),
        ("getcourse_deal_id", _clean(fields.get("order_id") or order.get("order_id"), 200)),
        ("getcourse_deal_paylink", _clean(fields.get("payment_link"), 2000)),
        ("getcourse_user_email", _clean(fields.get("email"), 500)),
        ("getcourse_user_phone", _phone_text(fields.get("phone"))),
    ]
    results = []
    for name, value in vars_to_set:
        ok, error, details = await _senler_post(
            _EP_VAR_SET,
            {
                "access_token": env["senler_token"],
                "group_id": env["senler_group_id"],
                "vk_user_id": vk_id,
                "name": name,
                "value": value,
                "v": SENLER_V,
            },
            timeout,
        )
        results.append({"name": name, "value": value, "ok": ok, "error": error, "details": details})
        if not ok:
            return False, f"vars/set {name}: {error}", results
    return True, "", results


async def _apply_rules(order: dict[str, Any], settings: dict[str, str]) -> tuple[bool, str, list[dict[str, Any]]]:
    if order["payment_state"] not in PROCESSABLE_PAYMENT_STATES:
        return True, "", []
    vk_id = order.get("vk_id") or ""
    if not vk_id:
        return False, "VK ID не найден в utm_term/utmT/user_term", []
    rules = await _active_rules()
    matched = [rule for rule in rules if _rule_matches(order, rule)]
    matched_subscription_ids = {_clean(rule.get("subscription_id"), 80) for rule in matched if _clean(rule.get("subscription_id"), 80)}
    results = []
    ok_all = True
    errors = []
    for rule in matched:
        subscription_id = _clean(rule.get("subscription_id"), 80)
        if not subscription_id:
            continue
        item = {
            "rule_id": rule["id"],
            "name": rule["name"],
            "subscription_id": subscription_id,
            "exclusive_groups": int(rule.get("exclusive_groups") or 0),
            "vars": [],
            "add": {},
            "remove": [],
        }
        vars_ok, vars_error, vars_details = await _senler_set_order_vars(order, vk_id, settings)
        item["vars"] = vars_details
        if not vars_ok:
            ok_all = False
            errors.append(f"{rule['name'] or rule['id']}: {vars_error}")
            results.append(item)
            continue
        add_ok, add_error, add_details = await _senler_add(subscription_id, vk_id, settings)
        item["add"] = {"ok": add_ok, "error": add_error, "details": add_details}
        if not add_ok:
            ok_all = False
            errors.append(f"{rule['name'] or rule['id']}: {add_error}")
            results.append(item)
            continue
        if int(rule.get("exclusive_groups") or 0):
            other_ids = await _all_active_subscription_ids(matched_subscription_ids)
            for other_id in other_ids:
                del_ok, del_error, del_details = await _senler_del(other_id, vk_id, settings)
                item["remove"].append({
                    "subscription_id": other_id,
                    "ok": del_ok,
                    "error": del_error,
                    "details": del_details,
                })
                if not del_ok:
                    ok_all = False
                    errors.append(f"remove {other_id}: {del_error}")
        results.append(item)
    return ok_all, "; ".join(errors), results


async def _store_event(row: dict[str, Any]) -> int:
    keys = [
        "method", "order_id", "platform_id", "payment_state", "status", "vk_id", "customer_record_id",
        "rules_matched_json", "success", "ignored", "error", "details", "raw_payload",
    ]
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            f"INSERT INTO events({','.join(keys)}) VALUES({','.join(['?'] * len(keys))})",
            tuple(row.get(k, "") for k in keys),
        )
        await db.commit()
        return int(cur.lastrowid)


def _secret_ok(request: Request, settings: dict[str, str]) -> bool:
    secret = _clean(settings.get("webhook_secret"), 300)
    if not secret:
        return True
    supplied = (
        request.query_params.get("secret")
        or request.headers.get("X-Nexus-Secret")
        or request.headers.get("X-Webhook-Secret")
        or ""
    )
    return _clean(supplied, 300) == secret


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    if request.method == "GET":
        data = dict(request.query_params)
        return data, json.dumps(_mask_secret(data), ensure_ascii=False)
    body = await request.body()
    raw = body.decode("utf-8", errors="replace") if body else ""
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type and raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data, json.dumps(_mask_secret(data), ensure_ascii=False)
        except Exception:
            pass
    form = await request.form()
    if form:
        data = {k: v for k, v in form.items()}
        return data, json.dumps(_mask_secret(data), ensure_ascii=False)
    return {}, raw


@router.get("/health")
async def health():
    return {"ok": True, "module": "getcourse-orders"}


@router.get("/env-status")
async def env_status(request: Request):
    await _require_panel_user(request)
    env = _env()
    settings = await _settings_map()
    db_path = _customer_db_path()
    return {
        "SENLER_ACCESS_TOKEN": bool(env["senler_token"]),
        "SENLER_GROUP_ID": bool(env["senler_group_id"]),
        "webhook_secret": bool(settings.get("webhook_secret")),
        "webhook_secret_source": "env" if env["webhook_secret"] else "db",
        "customer_db_path": str(db_path),
        "customer_db_ready": db_path.exists() or db_path.parent.exists(),
        "ready": bool(env["senler_token"] and env["senler_group_id"]),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    return {
        **settings,
        "webhook_secret_source": "env" if _env()["webhook_secret"] else "db",
        "customer_db_path": str(_customer_db_path()),
    }


@router.post("/settings")
async def post_settings(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.get("/rules")
async def list_rules(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM rules ORDER BY id DESC")
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        try:
            row["conditions"] = json.loads(row.pop("conditions_json") or "{}")
        except Exception:
            row["conditions"] = {"mode": "and", "conditions": []}
    return rows


@router.post("/rules")
async def save_rule(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    if not isinstance(data, dict):
        return JSONResponse({"error": "JSON object required"}, status_code=400)
    rule_id = int(data.get("id") or 0)
    name = _clean(data.get("name"), 300)
    subscription_id = _clean(data.get("subscription_id"), 80)
    active = 1 if data.get("active", True) else 0
    exclusive_groups = 1 if data.get("exclusive_groups") else 0
    conditions = _conditions_payload(data.get("conditions") or data)
    if not name:
        return JSONResponse({"error": "Название правила обязательно"}, status_code=400)
    if not subscription_id:
        return JSONResponse({"error": "Группа Senler обязательна"}, status_code=400)
    if not conditions["conditions"]:
        return JSONResponse({"error": "Добавьте хотя бы одно условие"}, status_code=400)
    conditions_json = json.dumps(conditions, ensure_ascii=False)
    async with aiosqlite.connect(_db_path) as db:
        if rule_id:
            await db.execute(
                """
                UPDATE rules
                SET name=?, subscription_id=?, conditions_json=?, exclusive_groups=?, active=?,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (name, subscription_id, conditions_json, exclusive_groups, active, rule_id),
            )
            saved_id = rule_id
        else:
            cur = await db.execute(
                "INSERT INTO rules(name,subscription_id,conditions_json,exclusive_groups,active) VALUES(?,?,?,?,?)",
                (name, subscription_id, conditions_json, exclusive_groups, active),
            )
            saved_id = int(cur.lastrowid)
        await db.commit()
    return {"ok": True, "id": saved_id}


@router.put("/rules/{rule_id}/toggle")
async def toggle_rule(rule_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "UPDATE rules SET active=1-active, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (rule_id,),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM rules WHERE id=?", (rule_id,))
        await db.commit()
    return {"ok": True}


@router.get("/events")
async def list_events(request: Request, limit: int = 200, result: str = "all"):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit)))
    where = ""
    if result == "ok":
        where = "WHERE success=1"
    elif result == "error":
        where = "WHERE success=0 AND ignored=0"
    elif result == "ignored":
        where = "WHERE ignored=1"
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(row) for row in await cur.fetchall()]


@router.get("/events/{event_id}")
async def get_event(event_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Не найдено")
    data = dict(row)
    for key in ("rules_matched_json", "details", "raw_payload"):
        try:
            data[key] = json.loads(data[key]) if data[key] else ([] if key == "rules_matched_json" else {})
        except Exception:
            data[key] = {"raw": data[key]}
    return data


@router.get("/stats")
async def stats(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM events")).fetchone())[0]
        success = (await (await db.execute("SELECT COUNT(*) FROM events WHERE success=1")).fetchone())[0]
        ignored = (await (await db.execute("SELECT COUNT(*) FROM events WHERE ignored=1")).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM rules WHERE active=1")).fetchone())[0]
    return {"events": total, "success": success, "ignored": ignored, "active_rules": active}


def _forced_state_from_hook(hook_state: str) -> str:
    aliases = {
        "created": "unpaid",
        "create": "unpaid",
        "new": "unpaid",
        "unpaid": "unpaid",
        "partial": "partial",
        "part": "partial",
        "paid": "paid",
        "success": "paid",
    }
    return aliases.get(_clean(hook_state, 40).casefold(), "")


async def _process_webhook(request: Request, forced_payment_state: str = ""):
    settings = await _settings_map()
    if not _secret_ok(request, settings):
        _log("warning", "webhook rejected: invalid secret")
        return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=200)
    payload: dict[str, Any] = {}
    raw_payload = ""
    try:
        payload, raw_payload = await _read_payload(request)
        order = _normalize_order(payload, settings, forced_payment_state)
        storage = await _upsert_customer_order(order)
        senler_ok, senler_error, rule_results = await _apply_rules(order, settings)
        ignored = 0
        error = senler_error
        if order["payment_state"] not in PROCESSABLE_PAYMENT_STATES:
            ignored = 1
            error = f"payment_state={order['payment_state']}: Senler не запускался"
        elif not rule_results and not senler_error:
            ignored = 1
            error = "нет подходящих активных правил"
        details = {
            "storage": storage,
            "order": order,
            "senler": rule_results,
        }
        success = int(not ignored and senler_ok and not (order["payment_state"] in ("paid", "partial") and senler_error))
        event_id = await _store_event({
            "method": request.method,
            "order_id": order["order_id"],
            "platform_id": order["platform_id"],
            "payment_state": order["payment_state"],
            "status": order["status"],
            "vk_id": order["vk_id"],
            "customer_record_id": storage["record_id"],
            "rules_matched_json": json.dumps(rule_results, ensure_ascii=False),
            "success": success,
            "ignored": ignored,
            "error": error,
            "details": json.dumps(details, ensure_ascii=False),
            "raw_payload": raw_payload,
        })
        if storage.get("duplicate_count"):
            _log("warning", "order %s has %s duplicate customer-db rows", order["platform_id"], storage["duplicate_count"])
        return {
            "ok": senler_ok,
            "event_id": event_id,
            "storage": storage,
            "payment_state": order["payment_state"],
            "payment_state_source": order["custom_fields"].get("payment_state_source", ""),
            "rules_processed": len(rule_results),
            "ignored": bool(ignored),
            "error": error,
        }
    except Exception as exc:
        _log("error", "webhook error: %s", exc, exc_info=True)
        try:
            event_id = await _store_event({
                "method": request.method,
                "order_id": _clean(payload.get("order_id") if isinstance(payload, dict) else ""),
                "platform_id": _clean(payload.get("order_id") if isinstance(payload, dict) else ""),
                "success": 0,
                "ignored": 0,
                "error": str(exc),
                "details": json.dumps({"exception": str(exc)}, ensure_ascii=False),
                "raw_payload": raw_payload,
            })
            return JSONResponse({"ok": False, "event_id": event_id, "error": str(exc)}, status_code=200)
        except Exception:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=200)


@router.api_route("/webhook", methods=["GET", "POST"])
async def webhook(request: Request):
    return await _process_webhook(request)


@router.api_route("/webhook/{hook_state}", methods=["GET", "POST"])
async def webhook_for_state(hook_state: str, request: Request):
    forced_payment_state = _forced_state_from_hook(hook_state)
    if not forced_payment_state:
        return JSONResponse({"ok": False, "error": "unknown webhook state"}, status_code=200)
    return await _process_webhook(request, forced_payment_state)
