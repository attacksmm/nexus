from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

router = APIRouter()

_db_path = None
_logger: logging.Logger | None = None

SENLER_API = "https://senler.ru/api"
SENLER_V = "2"
_EP_SUB_ADD = f"{SENLER_API}/subscribers/add"
_EP_SUB_DEL = f"{SENLER_API}/subscribers/del"
_EP_SUB_GET = f"{SENLER_API}/subscribers/get"
_EP_VAR_SET = f"{SENLER_API}/vars/set"

DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "vk_field": "utm_term",
    "request_timeout": "12",
}

DEFAULT_BINDINGS = [
    ("created", "", "", "", "", "3713900", 1, "AMO. Сделка создана", "Создание сделки"),
    ("work", "", "", "", "Все статусы в работе", "3713899", 0, "AMO. Сделка в работе", "Выберите конкретные статусы amoCRM в панели"),
    ("closed_lost", "", "", "143", "Закрыто и не реализовано", "3713898", 1, "AMO. Закрыто и не реализованно", "Нереализованная сделка"),
    ("success", "", "", "142", "Успешно реализовано", "3713878", 1, "AMO. Успешно реализованные", "Успешно реализованная сделка"),
]

CATEGORY_LABELS = {
    "created": "Создана",
    "work": "В работе",
    "success": "Успех",
    "closed_lost": "Закрыто",
}


def setup(ctx):
    global _db_path, _logger
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.amocrm-senler"))
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
            CREATE TABLE IF NOT EXISTS status_bindings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                category        TEXT NOT NULL DEFAULT 'work',
                pipeline_id     TEXT NOT NULL DEFAULT '',
                pipeline_name   TEXT NOT NULL DEFAULT '',
                status_id       TEXT NOT NULL DEFAULT '',
                status_name     TEXT NOT NULL DEFAULT '',
                subscription_id TEXT NOT NULL DEFAULT '',
                exclusive_groups INTEGER NOT NULL DEFAULT 0,
                name            TEXT NOT NULL DEFAULT '',
                note            TEXT NOT NULL DEFAULT '',
                active          INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS responsible_cache (
                amo_user_id TEXT PRIMARY KEY,
                name        TEXT NOT NULL DEFAULT '',
                updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS events (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                action               TEXT NOT NULL DEFAULT '',
                category             TEXT NOT NULL DEFAULT '',
                deal_id              TEXT NOT NULL DEFAULT '',
                pipeline_id          TEXT NOT NULL DEFAULT '',
                status_id            TEXT NOT NULL DEFAULT '',
                old_status_id        TEXT NOT NULL DEFAULT '',
                responsible_user_id  TEXT NOT NULL DEFAULT '',
                responsible_name     TEXT NOT NULL DEFAULT '',
                vk_id                TEXT NOT NULL DEFAULT '',
                binding_id           INTEGER,
                subscription_id      TEXT NOT NULL DEFAULT '',
                success              INTEGER NOT NULL DEFAULT 0,
                ignored              INTEGER NOT NULL DEFAULT 0,
                error                TEXT NOT NULL DEFAULT '',
                details              TEXT NOT NULL DEFAULT '',
                raw_payload          TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_events_deal ON events(deal_id);
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_bindings_status ON status_bindings(category, pipeline_id, status_id, active);
            """
        )
        for column, ddl in (
            ("pipeline_name", "ALTER TABLE status_bindings ADD COLUMN pipeline_name TEXT NOT NULL DEFAULT ''"),
            ("status_name", "ALTER TABLE status_bindings ADD COLUMN status_name TEXT NOT NULL DEFAULT ''"),
            ("exclusive_groups", "ALTER TABLE status_bindings ADD COLUMN exclusive_groups INTEGER NOT NULL DEFAULT 0"),
        ):
            try:
                await db.execute(ddl)
            except Exception:
                pass
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute(
                "INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                (key, value),
            )
        for category, pipeline_id, pipeline_name, status_id, status_name, subscription_id, active, name, note in DEFAULT_BINDINGS:
            cur = await db.execute(
                "SELECT id FROM status_bindings WHERE category=? AND subscription_id=? AND name=?",
                (category, subscription_id, name),
            )
            if not await cur.fetchone():
                await db.execute(
                    """
                    INSERT INTO status_bindings(category,pipeline_id,pipeline_name,status_id,status_name,subscription_id,exclusive_groups,name,note,active)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (category, pipeline_id, pipeline_name, status_id, status_name, subscription_id, 0, name, note, active),
                )
        await db.commit()
    _log("info", "amocrm-senler DB initialized")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _mask(value: str, keep: int = 4) -> str:
    value = str(value or "")
    if not value:
        return ""
    return "***" + value[-keep:]


def _env() -> dict[str, str]:
    return {
        "senler_token": os.environ.get("SENLER_ACCESS_TOKEN", "").strip(),
        "senler_group_id": os.environ.get("SENLER_GROUP_ID", "").strip(),
        "amo_base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "amo_token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "webhook_secret": os.environ.get("AMO_SENLER_WEBHOOK_SECRET", "").strip(),
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
    allowed = {"webhook_secret", "vk_field", "request_timeout"}
    async with aiosqlite.connect(_db_path) as db:
        for key in allowed:
            if key not in data:
                continue
            value = _clean(data.get(key), 200)
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


def _flat_payload_to_nested(flat: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"leads": {}, "account": {}}
    item_re = re.compile(r"^([A-Za-z0-9_]+)\[([A-Za-z0-9_]+)\]\[(\d+)\]\[([^\]]+)\]$")
    account_re = re.compile(r"^account\[([^\]]+)\]$")
    for key, value in flat.items():
        key = str(key)
        account_match = account_re.match(key)
        if account_match:
            result["account"][account_match.group(1)] = value
            continue
        match = item_re.match(key)
        if not match:
            result[key] = value
            continue
        entity, action, idx, field = match.groups()
        bucket = result.setdefault(entity, {}).setdefault(action, {})
        bucket.setdefault(idx, {})[field] = value
    return result


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    ctype = request.headers.get("content-type", "").lower()
    if "application/json" in ctype:
        data = await request.json()
        return data if isinstance(data, dict) else {}, json.dumps(data, ensure_ascii=False)[:6000]
    form = await request.form()
    flat = {str(k): v for k, v in form.items()}
    return _flat_payload_to_nested(flat), json.dumps(flat, ensure_ascii=False)[:6000]


def _iter_lead_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    leads = payload.get("leads") or {}
    if not isinstance(leads, dict):
        return []
    events: list[dict[str, Any]] = []
    for action in ("add", "status"):
        bucket = leads.get(action)
        if not bucket:
            continue
        if isinstance(bucket, dict):
            items = bucket.values()
        elif isinstance(bucket, list):
            items = bucket
        else:
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            event = {str(k): v for k, v in item.items()}
            event["_action"] = action
            events.append(event)
    return events


def _custom_field_value(source: dict[str, Any], field_name: str) -> str:
    target = _clean(field_name).lower()
    if not target:
        return ""
    for key, value in source.items():
        if str(key).lower() == target:
            return _clean(value, 200)
    fields = source.get("custom_fields_values") or source.get("custom_fields") or []
    if not isinstance(fields, list):
        return ""
    for field in fields:
        if not isinstance(field, dict):
            continue
        candidates = [
            field.get("field_code"),
            field.get("field_name"),
            field.get("code"),
            field.get("name"),
        ]
        if not any(_clean(c).lower() == target or _clean(c).lower() == target.upper().lower() for c in candidates):
            continue
        values = field.get("values") or []
        if isinstance(values, list) and values:
            first = values[0]
            if isinstance(first, dict):
                return _clean(first.get("value"), 200)
            return _clean(first, 200)
        return _clean(field.get("value"), 200)
    return ""


def _category_for_action(action: str) -> str:
    return "created" if action == "add" else ""


async def _find_binding(event: dict[str, Any]) -> dict[str, Any] | None:
    action = _clean(event.get("_action"))
    pipeline_id = _clean(event.get("pipeline_id"), 64)
    status_id = _clean(event.get("status_id"), 64)
    category = _category_for_action(action)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        if category:
            cur = await db.execute(
                """
                SELECT * FROM status_bindings
                WHERE active=1
                  AND category=?
                  AND (pipeline_id='' OR pipeline_id=?)
                  AND (status_id='' OR status_id=?)
                ORDER BY CASE WHEN pipeline_id=? THEN 0 ELSE 1 END,
                         CASE WHEN status_id=? THEN 0 ELSE 1 END,
                         id
                LIMIT 1
                """,
                (category, pipeline_id, status_id, pipeline_id, status_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else None
        cur = await db.execute(
            """
            SELECT * FROM status_bindings
            WHERE active=1
              AND status_id=?
              AND (pipeline_id='' OR pipeline_id=?)
            ORDER BY CASE WHEN pipeline_id=? THEN 0 ELSE 1 END, id
            LIMIT 1
            """,
            (status_id, pipeline_id, pipeline_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def _amo_get(path: str, settings: dict[str, str]) -> tuple[dict[str, Any] | None, str]:
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            resp = await client.get(
                env["amo_base_url"] + path,
                headers={"Authorization": f"Bearer {env['amo_token']}"},
            )
        if resp.status_code >= 400:
            return None, f"amoCRM HTTP {resp.status_code}: {resp.text[:500]}"
        return resp.json(), ""
    except Exception as exc:
        return None, str(exc)


async def _load_deal(deal_id: str, settings: dict[str, str]) -> tuple[dict[str, Any], str]:
    if not deal_id:
        return {}, "deal_id пустой"
    body, error = await _amo_get(f"/api/v4/leads/{deal_id}", settings)
    return body or {}, error


async def _amo_status_catalog(settings: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    body, error = await _amo_get("/api/v4/leads/pipelines", settings)
    if error:
        return [], error
    raw_pipelines = ((body or {}).get("_embedded") or {}).get("pipelines") or []
    pipelines: list[dict[str, Any]] = []
    for pipeline in raw_pipelines:
        if not isinstance(pipeline, dict):
            continue
        statuses = []
        raw_statuses = ((pipeline.get("_embedded") or {}).get("statuses") or [])
        for status in raw_statuses:
            if not isinstance(status, dict):
                continue
            status_id = _clean(status.get("id"), 64)
            if not status_id:
                continue
            statuses.append({
                "id": status_id,
                "name": _clean(status.get("name"), 300) or status_id,
                "sort": status.get("sort"),
                "type": _clean(status.get("type"), 64),
            })
        pipelines.append({
            "id": _clean(pipeline.get("id"), 64),
            "name": _clean(pipeline.get("name"), 300) or _clean(pipeline.get("id"), 64),
            "sort": pipeline.get("sort"),
            "statuses": statuses,
        })
    return pipelines, ""


def _status_lookup(pipelines: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, str]]:
    lookup: dict[tuple[str, str], dict[str, str]] = {}
    wildcard: dict[str, dict[str, str]] = {}
    for pipeline in pipelines:
        pipeline_id = _clean(pipeline.get("id"), 64)
        pipeline_name = _clean(pipeline.get("name"), 300)
        for status in pipeline.get("statuses") or []:
            status_id = _clean(status.get("id"), 64)
            status_name = _clean(status.get("name"), 300)
            item = {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "status_id": status_id,
                "status_name": status_name,
            }
            lookup[(pipeline_id, status_id)] = item
            wildcard.setdefault(status_id, item)
    for status_id, item in wildcard.items():
        lookup.setdefault(("", status_id), item)
    return lookup


async def _responsible_name(user_id: str, settings: dict[str, str]) -> tuple[str, str]:
    user_id = _clean(user_id, 64)
    if not user_id:
        return "", "responsible_user_id пустой"
    body, error = await _amo_get(f"/api/v4/users/{user_id}", settings)
    if error:
        return "", error
    name = _clean((body or {}).get("name"), 300)
    if not name:
        return "", "amoCRM user name пустой"
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            """
            INSERT INTO responsible_cache(amo_user_id,name,updated_at) VALUES(?,?,?)
            ON CONFLICT(amo_user_id) DO UPDATE SET name=excluded.name, updated_at=excluded.updated_at
            """,
            (user_id, name, _now()),
        )
        await db.commit()
    return name, ""


async def _senler_check(access_token: str, group_id: str, subscription_id: str, vk_id: str, timeout: int) -> tuple[bool | None, Any]:
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
        if body.get("success"):
            return True, "", {"http_status": status_code, "response": body, "params": safe_params}
        err = body.get("error", {})
        msg = err.get("error_msg", str(body)) if isinstance(err, dict) else str(err)
        return False, msg, {"http_status": status_code, "response": body, "params": safe_params}
    except Exception as exc:
        return False, str(exc), {
            "http_status": status_code,
            "response": raw,
            "exception": str(exc),
            "params": safe_params,
        }


async def _senler_apply(subscription_id: str, vk_id: str, responsible_name: str, deal_id: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    env = _env()
    timeout = _timeout(settings)
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN или SENLER_GROUP_ID не заданы", {}
    base = {
        "access_token": env["senler_token"],
        "group_id": env["senler_group_id"],
        "vk_user_id": vk_id,
        "v": SENLER_V,
    }
    already, check_details = await _senler_check(
        env["senler_token"], env["senler_group_id"], subscription_id, vk_id, timeout
    )
    details: dict[str, Any] = {"check": check_details, "add": None, "vars": []}
    if already is True:
        add_ok, add_error = True, ""
        details["add"] = {"skipped": True, "reason": "уже подписан"}
    else:
        add_ok, add_error, add_details = await _senler_post(
            _EP_SUB_ADD,
            {**base, "subscription_id": subscription_id},
            timeout,
        )
        details["add"] = add_details
    if not add_ok:
        return False, add_error, details

    for name, value in (("amo_responsible", responsible_name), ("amo_deal_id", deal_id)):
        ok, error, var_details = await _senler_post(
            _EP_VAR_SET,
            {**base, "name": name, "value": value},
            timeout,
        )
        details["vars"].append({"name": name, "ok": ok, "error": error, "details": var_details})
        if not ok:
            return False, f"vars/set {name}: {error}", details
    return True, "", details


async def _other_subscription_ids(binding_id: int | None, current_subscription_id: str) -> list[str]:
    if not binding_id:
        return []
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            """
            SELECT DISTINCT subscription_id
            FROM status_bindings
            WHERE active=1
              AND id<>?
              AND category<>'created'
              AND subscription_id<>''
              AND subscription_id<>?
            ORDER BY subscription_id
            """,
            (binding_id, current_subscription_id),
        )
        return [_clean(row[0], 64) for row in await cur.fetchall() if _clean(row[0], 64)]


async def _senler_remove_from_groups(subscription_ids: list[str], vk_id: str, settings: dict[str, str]) -> tuple[bool, str, list[dict[str, Any]]]:
    env = _env()
    timeout = _timeout(settings)
    if not subscription_ids:
        return True, "", []
    if not env["senler_token"] or not env["senler_group_id"]:
        return False, "SENLER_ACCESS_TOKEN или SENLER_GROUP_ID не заданы", []
    results = []
    for subscription_id in subscription_ids:
        ok, error, details = await _senler_post(
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
        results.append({"subscription_id": subscription_id, "ok": ok, "error": error, "details": details})
        if not ok:
            return False, f"subscribers/del {subscription_id}: {error}", results
    return True, "", results


async def _store_event(row: dict[str, Any]) -> int:
    keys = [
        "action", "category", "deal_id", "pipeline_id", "status_id", "old_status_id",
        "responsible_user_id", "responsible_name", "vk_id", "binding_id", "subscription_id",
        "success", "ignored", "error", "details", "raw_payload",
    ]
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            f"INSERT INTO events({','.join(keys)}) VALUES({','.join(['?'] * len(keys))})",
            tuple(row.get(k, "") for k in keys),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _process_event(event: dict[str, Any], raw_payload: str, settings: dict[str, str]) -> dict[str, Any]:
    action = _clean(event.get("_action"), 32)
    deal_id = _clean(event.get("id"), 64)
    pipeline_id = _clean(event.get("pipeline_id"), 64)
    status_id = _clean(event.get("status_id"), 64)
    old_status_id = _clean(event.get("old_status_id"), 64)
    responsible_user_id = _clean(event.get("responsible_user_id"), 64)
    binding = await _find_binding(event)
    base_row = {
        "action": action,
        "category": binding["category"] if binding else "",
        "deal_id": deal_id,
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "old_status_id": old_status_id,
        "responsible_user_id": responsible_user_id,
        "responsible_name": "",
        "vk_id": "",
        "binding_id": binding["id"] if binding else None,
        "subscription_id": binding["subscription_id"] if binding else "",
        "success": 0,
        "ignored": 0,
        "error": "",
        "details": "",
        "raw_payload": raw_payload,
    }
    if not binding:
        base_row["ignored"] = 1
        base_row["error"] = "нет активной связки для события"
        base_row["details"] = json.dumps({"event": event}, ensure_ascii=False)
        event_id = await _store_event(base_row)
        return {"id": event_id, "ok": True, "ignored": True, "error": base_row["error"]}

    deal_data: dict[str, Any] = {}
    deal_error = ""
    vk_id = _custom_field_value(event, settings.get("vk_field", "utm_term"))
    if not vk_id or not responsible_user_id:
        deal_data, deal_error = await _load_deal(deal_id, settings)
        if not vk_id:
            vk_id = _custom_field_value(deal_data, settings.get("vk_field", "utm_term"))
        if not responsible_user_id:
            responsible_user_id = _clean(deal_data.get("responsible_user_id"), 64)
            base_row["responsible_user_id"] = responsible_user_id
    base_row["vk_id"] = vk_id

    if not vk_id:
        base_row["error"] = f"VK ID не найден в поле {settings.get('vk_field', 'utm_term')}"
        base_row["details"] = json.dumps({"event": event, "deal_error": deal_error, "deal": deal_data}, ensure_ascii=False)
        event_id = await _store_event(base_row)
        return {"id": event_id, "ok": False, "error": base_row["error"]}

    responsible_name, responsible_error = await _responsible_name(responsible_user_id, settings)
    base_row["responsible_name"] = responsible_name
    if not responsible_name:
        base_row["error"] = f"не удалось получить точное имя ответственного: {responsible_error}"
        base_row["details"] = json.dumps({"event": event, "deal_error": deal_error}, ensure_ascii=False)
        event_id = await _store_event(base_row)
        return {"id": event_id, "ok": False, "error": base_row["error"]}

    ok, error, senler_details = await _senler_apply(
        binding["subscription_id"], vk_id, responsible_name, deal_id, settings
    )
    if ok and binding.get("category") != "created" and int(binding.get("exclusive_groups") or 0):
        other_subscription_ids = await _other_subscription_ids(binding.get("id"), binding["subscription_id"])
        remove_ok, remove_error, remove_details = await _senler_remove_from_groups(other_subscription_ids, vk_id, settings)
        senler_details["exclusive_remove"] = {
            "enabled": True,
            "subscription_ids": other_subscription_ids,
            "results": remove_details,
        }
        if not remove_ok:
            ok = False
            error = remove_error
    elif ok:
        senler_details["exclusive_remove"] = {"enabled": False}
    base_row["success"] = int(ok)
    base_row["error"] = error
    base_row["details"] = json.dumps(
        {
            "event": event,
            "deal_error": deal_error,
            "responsible_error": responsible_error,
            "senler": senler_details,
        },
        ensure_ascii=False,
    )
    event_id = await _store_event(base_row)
    if ok:
        _log("info", "amo deal %s -> Senler %s OK", deal_id, binding["subscription_id"])
    else:
        _log("warning", "amo deal %s -> Senler %s FAIL: %s", deal_id, binding["subscription_id"], error)
    return {"id": event_id, "ok": ok, "ignored": False, "error": error}


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


@router.get("/health")
async def health():
    return {"ok": True, "module": "amocrm-senler"}


@router.get("/env-status")
async def env_status():
    env = _env()
    settings = await _settings_map()
    return {
        "SENLER_ACCESS_TOKEN": bool(env["senler_token"]),
        "SENLER_GROUP_ID": bool(env["senler_group_id"]),
        "AMO_BASE_URL": bool(env["amo_base_url"]),
        "AMO_ACCESS_TOKEN": bool(env["amo_token"]),
        "webhook_secret": bool(settings.get("webhook_secret")),
        "ready": bool(env["senler_token"] and env["senler_group_id"] and env["amo_base_url"] and env["amo_token"]),
    }


@router.get("/settings")
async def get_settings():
    settings = await _settings_map()
    env = _env()
    return {
        **settings,
        "webhook_secret_source": "env" if env["webhook_secret"] else "db",
        "amo_base_url": env["amo_base_url"],
        "has_amo_token": bool(env["amo_token"]),
        "has_senler_token": bool(env["senler_token"]),
        "senler_group_id": env["senler_group_id"],
    }


@router.post("/settings")
async def post_settings(request: Request):
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.get("/amo/statuses")
async def amo_statuses():
    settings = await _settings_map()
    pipelines, error = await _amo_status_catalog(settings)
    if error:
        return JSONResponse({"error": error, "pipelines": []}, status_code=502)
    return {"pipelines": pipelines}


@router.get("/bindings")
async def list_bindings():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM status_bindings ORDER BY category, id")
        rows = [dict(row) for row in await cur.fetchall()]
    settings = await _settings_map()
    pipelines, _ = await _amo_status_catalog(settings)
    lookup = _status_lookup(pipelines)
    for row in rows:
        row["category_label"] = CATEGORY_LABELS.get(row["category"], row["category"])
        found = lookup.get((_clean(row.get("pipeline_id"), 64), _clean(row.get("status_id"), 64)))
        if found:
            if row.get("pipeline_id") and not row.get("pipeline_name"):
                row["pipeline_name"] = found["pipeline_name"]
            if not row.get("status_name"):
                row["status_name"] = found["status_name"]
        if not row.get("pipeline_name"):
            row["pipeline_name"] = "Любая воронка"
        if not row.get("status_name"):
            if row["category"] == "created":
                row["status_name"] = "Создание сделки"
            elif not row.get("status_id"):
                row["status_name"] = "Все статусы в работе"
            elif row.get("status_id") == "142":
                row["status_name"] = "Успешно реализовано"
            elif row.get("status_id") == "143":
                row["status_name"] = "Закрыто и не реализовано"
            else:
                row["status_name"] = row.get("status_id") or "Не выбран"
    return rows


@router.post("/bindings")
async def save_binding(request: Request):
    data = await request.json()
    binding_id = int(data.get("id") or 0)
    category = _clean(data.get("category"), 32)
    if category not in CATEGORY_LABELS:
        return JSONResponse({"error": "неверная категория"}, status_code=400)
    subscription_id = _clean(data.get("subscription_id"), 64)
    name = _clean(data.get("name"), 300)
    note = _clean(data.get("note"), 1000)
    active = 1 if data.get("active", True) else 0
    exclusive_groups = 1 if data.get("exclusive_groups") and category != "created" else 0
    if not subscription_id:
        return JSONResponse({"error": "subscription_id обязателен"}, status_code=400)
    raw_statuses = data.get("statuses")
    if category == "created":
        statuses = [{
            "pipeline_id": _clean(data.get("pipeline_id"), 64),
            "pipeline_name": _clean(data.get("pipeline_name"), 300),
            "status_id": "",
            "status_name": "Создание сделки",
        }]
    elif isinstance(raw_statuses, list) and raw_statuses:
        statuses = []
        for item in raw_statuses:
            if not isinstance(item, dict):
                continue
            status_id = _clean(item.get("status_id") or item.get("id"), 64)
            if not status_id:
                continue
            statuses.append({
                "pipeline_id": _clean(item.get("pipeline_id"), 64),
                "pipeline_name": _clean(item.get("pipeline_name"), 300),
                "status_id": status_id,
                "status_name": _clean(item.get("status_name") or item.get("name"), 300) or status_id,
            })
        if not statuses:
            return JSONResponse({"error": "выберите хотя бы один статус amoCRM"}, status_code=400)
    else:
        status_id = _clean(data.get("status_id"), 64)
        if not status_id:
            return JSONResponse({"error": "выберите хотя бы один статус amoCRM"}, status_code=400)
        statuses = [{
            "pipeline_id": _clean(data.get("pipeline_id"), 64),
            "pipeline_name": _clean(data.get("pipeline_name"), 300),
            "status_id": status_id,
            "status_name": _clean(data.get("status_name"), 300) or status_id,
        }]
    async with aiosqlite.connect(_db_path) as db:
        saved_ids = []
        for idx, status in enumerate(statuses):
            row_name = name or f"{CATEGORY_LABELS[category]}: {status['status_name']}"
            if binding_id and idx == 0:
                saved_id = binding_id
                await db.execute(
                    """
                    UPDATE status_bindings
                    SET category=?, pipeline_id=?, pipeline_name=?, status_id=?, status_name=?,
                        subscription_id=?, exclusive_groups=?, name=?, note=?, active=?,
                        updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                    WHERE id=?
                    """,
                    (
                        category, status["pipeline_id"], status["pipeline_name"], status["status_id"],
                        status["status_name"], subscription_id, exclusive_groups, row_name, note, active, binding_id,
                    ),
                )
            else:
                cur = await db.execute(
                    """
                    INSERT INTO status_bindings(category,pipeline_id,pipeline_name,status_id,status_name,subscription_id,exclusive_groups,name,note,active)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        category, status["pipeline_id"], status["pipeline_name"], status["status_id"],
                        status["status_name"], subscription_id, exclusive_groups, row_name, note, active,
                    ),
                )
                saved_id = int(cur.lastrowid)
            saved_ids.append(saved_id)
        await db.commit()
    return {"ok": True, "id": saved_ids[0], "ids": saved_ids}


@router.put("/bindings/{binding_id}/toggle")
async def toggle_binding(binding_id: int):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "UPDATE status_bindings SET active=1-active, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (binding_id,),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/bindings/{binding_id}")
async def delete_binding(binding_id: int):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM status_bindings WHERE id=?", (binding_id,))
        await db.commit()
    return {"ok": True}


@router.get("/events")
async def list_events(limit: int = 200, result: str = "all"):
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
async def get_event(event_id: int):
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Не найдено")
    data = dict(row)
    for key in ("details", "raw_payload"):
        try:
            data[key] = json.loads(data[key]) if data[key] else {}
        except Exception:
            data[key] = {"raw": data[key]}
    return data


@router.get("/stats")
async def stats():
    async with aiosqlite.connect(_db_path) as db:
        total = (await (await db.execute("SELECT COUNT(*) FROM events")).fetchone())[0]
        success = (await (await db.execute("SELECT COUNT(*) FROM events WHERE success=1")).fetchone())[0]
        ignored = (await (await db.execute("SELECT COUNT(*) FROM events WHERE ignored=1")).fetchone())[0]
        active = (await (await db.execute("SELECT COUNT(*) FROM status_bindings WHERE active=1")).fetchone())[0]
    return {"events": total, "success": success, "ignored": ignored, "active_bindings": active}


@router.post("/webhook")
async def webhook(request: Request):
    settings = await _settings_map()
    if not _secret_ok(request, settings):
        return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=403)
    try:
        payload, raw_payload = await _read_payload(request)
        events = _iter_lead_events(payload)
        if not events:
            return {"ok": True, "processed": 0, "ignored": True, "error": "lead events not found"}
        results = [await _process_event(event, raw_payload, settings) for event in events]
        return {
            "ok": all(item.get("ok") for item in results),
            "processed": len(results),
            "results": results,
        }
    except Exception as exc:
        _log("error", "webhook error: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=200)


@router.get("/webhook")
async def webhook_get():
    return {"ok": True, "hint": "Use POST from amoCRM"}
