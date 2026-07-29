from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote_plus

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "vk-dialog-labels"
VK_API_BASE = "https://api.vk.com/method"
VK_API_VERSION = "5.199"
DEFAULT_GROUP_ID = "225075265"
TEST_GROUP_ID = "225075265"
TEST_PEER_ID = "1105209997"
AMO_WEBHOOK_SETTINGS = ["add_lead", "status_lead", "update_lead", "responsible_lead"]

DEFAULT_SETTINGS = {
    "webhook_secret": "",
    "vk_field": "utm_term",
    "request_timeout": "12",
}

OFFICIAL_ACTIONS = {
    "none": "Только сохранить метку Nexus",
    "important_on": "VK: отметить важным",
    "important_off": "VK: снять важное",
    "answered_on": "VK: отметить отвеченным",
    "answered_off": "VK: отметить неотвеченным",
}

DEFAULT_RULES = [
    ("created", "", "", "", "", "Заказ", "important_on", "Создана сделка", 1),
    ("work", "", "", "", "", "НА КУРСЕ", "none", "Сделка в работе", 0),
    ("success", "", "", "142", "Успешно реализовано", "после Веба", "answered_on", "Успешная сделка", 1),
    ("closed_lost", "", "", "143", "Закрыто и не реализовано", "А3", "important_off", "Закрытая сделка", 1),
]

CATEGORY_LABELS = {
    "created": "Создана",
    "work": "В работе",
    "success": "Успех",
    "closed_lost": "Закрыто",
}

_db_path: str | None = None
_logger: logging.Logger | None = None
_webhook_watchdog_task: asyncio.Task | None = None


def setup(ctx):
    global _db_path, _logger, _webhook_watchdog_task
    _db_path = ctx.db_path
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.vk-dialog-labels"))
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
        if _webhook_watchdog_enabled() and (_webhook_watchdog_task is None or _webhook_watchdog_task.done()):
            _webhook_watchdog_task = loop.create_task(_webhook_watchdog_loop())
    else:
        loop.run_until_complete(_init_db())


async def shutdown() -> None:
    global _webhook_watchdog_task
    if _webhook_watchdog_task and not _webhook_watchdog_task.done():
        _webhook_watchdog_task.cancel()
        try:
            await _webhook_watchdog_task
        except asyncio.CancelledError:
            pass
    _webhook_watchdog_task = None


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS label_rules (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                category        TEXT NOT NULL DEFAULT 'work',
                pipeline_id     TEXT NOT NULL DEFAULT '',
                pipeline_name   TEXT NOT NULL DEFAULT '',
                status_id       TEXT NOT NULL DEFAULT '',
                status_name     TEXT NOT NULL DEFAULT '',
                statuses_json   TEXT NOT NULL DEFAULT '[]',
                target_label    TEXT NOT NULL DEFAULT '',
                official_action TEXT NOT NULL DEFAULT 'none',
                name            TEXT NOT NULL DEFAULT '',
                note            TEXT NOT NULL DEFAULT '',
                active          INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS dialog_labels (
                peer_id         TEXT PRIMARY KEY,
                vk_user_id      TEXT NOT NULL DEFAULT '',
                target_label    TEXT NOT NULL DEFAULT '',
                rule_id         INTEGER,
                deal_id         TEXT NOT NULL DEFAULT '',
                official_action TEXT NOT NULL DEFAULT 'none',
                updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE TABLE IF NOT EXISTS events (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                source               TEXT NOT NULL DEFAULT 'webhook',
                action               TEXT NOT NULL DEFAULT '',
                category             TEXT NOT NULL DEFAULT '',
                deal_id              TEXT NOT NULL DEFAULT '',
                pipeline_id          TEXT NOT NULL DEFAULT '',
                status_id            TEXT NOT NULL DEFAULT '',
                old_status_id        TEXT NOT NULL DEFAULT '',
                responsible_user_id  TEXT NOT NULL DEFAULT '',
                vk_id                TEXT NOT NULL DEFAULT '',
                peer_id              TEXT NOT NULL DEFAULT '',
                rule_id              INTEGER,
                target_label         TEXT NOT NULL DEFAULT '',
                official_action      TEXT NOT NULL DEFAULT 'none',
                status               TEXT NOT NULL DEFAULT '',
                success              INTEGER NOT NULL DEFAULT 0,
                ignored              INTEGER NOT NULL DEFAULT 0,
                error                TEXT NOT NULL DEFAULT '',
                details              TEXT NOT NULL DEFAULT '',
                raw_payload          TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_rules_status ON label_rules(category, pipeline_id, status_id, active);
            CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_events_deal ON events(deal_id);
            CREATE INDEX IF NOT EXISTS idx_events_peer ON events(peer_id);
            """
        )
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        for category, pipeline_id, pipeline_name, status_id, status_name, target_label, official_action, name, active in DEFAULT_RULES:
            cur = await db.execute(
                "SELECT id FROM label_rules WHERE category=? AND target_label=? AND name=?",
                (category, target_label, name),
            )
            if not await cur.fetchone():
                await db.execute(
                    """
                    INSERT INTO label_rules(
                        category,pipeline_id,pipeline_name,status_id,status_name,statuses_json,
                        target_label,official_action,name,active
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        category,
                        pipeline_id,
                        pipeline_name,
                        status_id,
                        status_name,
                        _statuses_json([{
                            "pipeline_id": pipeline_id,
                            "pipeline_name": pipeline_name,
                            "status_id": status_id,
                            "status_name": status_name,
                        }] if category != "created" and status_id else []),
                        target_label,
                        official_action,
                        name,
                        active,
                    ),
                )
        await db.commit()
    _log("info", "vk-dialog-labels DB initialized")


def _must_db() -> str:
    if not _db_path:
        raise RuntimeError("vk-dialog-labels module is not initialized")
    return _db_path


def _log(level: str, message: str, *args: Any, **kwargs: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


async def _require_panel_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def _env() -> dict[str, str]:
    return {
        "vk_group_token": os.environ.get("VK_GROUP_TOKEN", "").strip(),
        "vk_user_token": os.environ.get("VK_USER_TOKEN", "").strip(),
        "vk_group_id": os.environ.get("VK_GROUP_ID", "").strip() or DEFAULT_GROUP_ID,
        "amo_base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "amo_token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "webhook_secret": os.environ.get("VK_LABELS_WEBHOOK_SECRET", "").strip(),
        "webhook_url": os.environ.get("VK_LABELS_WEBHOOK_URL", "").strip(),
        "webhook_public_base": os.environ.get("VK_LABELS_PUBLIC_BASE", "https://junior.sobakovod.pro/nexus").strip().rstrip("/"),
        "webhook_watchdog": os.environ.get("VK_LABELS_WEBHOOK_WATCHDOG", "").strip().lower(),
        "webhook_watchdog_interval": os.environ.get("VK_LABELS_WEBHOOK_WATCHDOG_INTERVAL", "").strip(),
    }


def _vk_token() -> str:
    env = _env()
    return env["vk_group_token"] or env["vk_user_token"]


def _webhook_watchdog_enabled() -> bool:
    return _env()["webhook_watchdog"] in {"1", "true", "yes", "on"}


def _webhook_watchdog_interval() -> int:
    try:
        return max(300, min(86400, int(_env()["webhook_watchdog_interval"] or "3600")))
    except Exception:
        return 3600


def _timeout(settings: dict[str, str]) -> int:
    try:
        return max(5, min(45, int(settings.get("request_timeout") or "12")))
    except Exception:
        return 12


def _statuses_json(statuses: list[dict[str, Any]]) -> str:
    clean_statuses = []
    seen = set()
    for raw in statuses:
        if not isinstance(raw, dict):
            continue
        item = _status_item(raw)
        if not item["status_id"]:
            continue
        key = (item["pipeline_id"], item["status_id"])
        if key in seen:
            continue
        seen.add(key)
        clean_statuses.append(item)
    return json.dumps(clean_statuses, ensure_ascii=False)


def _status_item(item: dict[str, Any]) -> dict[str, str]:
    status_id = _clean(item.get("status_id") or item.get("id"), 64)
    return {
        "pipeline_id": _clean(item.get("pipeline_id"), 64),
        "pipeline_name": _clean(item.get("pipeline_name"), 300),
        "status_id": status_id,
        "status_name": _clean(item.get("status_name") or item.get("name"), 300) or status_id,
    }


def _rule_statuses(row: dict[str, Any]) -> list[dict[str, str]]:
    try:
        parsed = json.loads(row.get("statuses_json") or "[]")
    except Exception:
        parsed = []
    statuses = [_status_item(item) for item in parsed if isinstance(item, dict)]
    statuses = [item for item in statuses if item["status_id"]]
    if statuses:
        return statuses
    status_id = _clean(row.get("status_id"), 64)
    if not status_id:
        return []
    return [_status_item(row)]


async def _settings_map() -> dict[str, str]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data = DEFAULT_SETTINGS.copy()
    data.update({row[0]: row[1] for row in rows})
    if _env()["webhook_secret"]:
        data["webhook_secret"] = _env()["webhook_secret"]
    return data


async def _save_settings(data: dict[str, Any]) -> dict[str, str]:
    allowed = {"webhook_secret", "vk_field", "request_timeout"}
    async with aiosqlite.connect(_must_db()) as db:
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


def _amo_webhook_destination(settings: dict[str, str]) -> str:
    env = _env()
    if env["webhook_url"]:
        return env["webhook_url"]
    url = f"{env['webhook_public_base']}/{MODULE_ID}/api/webhook"
    secret = _clean(settings.get("webhook_secret"), 200)
    if secret:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}secret={secret}"
    return url


def _safe_url(url: str) -> str:
    return re.sub(r"([?&]secret=)[^&]+", r"\1***", str(url or ""))


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
        return resp.json() if resp.text else {}, ""
    except Exception as exc:
        return None, str(exc)


async def _amo_post(path: str, payload: Any, settings: dict[str, str]) -> tuple[dict[str, Any] | None, str]:
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            resp = await client.post(
                env["amo_base_url"] + path,
                headers={"Authorization": f"Bearer {env['amo_token']}", "Content-Type": "application/json"},
                json=payload,
            )
        if resp.status_code >= 400:
            return None, f"amoCRM HTTP {resp.status_code}: {resp.text[:500]}"
        return resp.json() if resp.text else {}, ""
    except Exception as exc:
        return None, str(exc)


async def _ensure_amo_webhook(settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    destination = _amo_webhook_destination(settings)
    body, error = await _amo_get("/api/v4/webhooks", settings)
    if error:
        return False, error, {"destination": destination}
    webhooks = ((body or {}).get("_embedded") or {}).get("webhooks") or []
    current = next((item for item in webhooks if isinstance(item, dict) and _clean(item.get("destination"), 1000) == destination), None)
    current_settings = current.get("settings") if isinstance(current, dict) else []
    if isinstance(current_settings, dict):
        enabled_settings = {key for key, value in current_settings.items() if value}
    else:
        enabled_settings = set(current_settings or [])
    desired = set(AMO_WEBHOOK_SETTINGS)
    if current and not current.get("disabled") and desired.issubset(enabled_settings):
        return True, "", {"destination": destination, "id": current.get("id"), "disabled": False}
    registered, register_error = await _amo_post("/api/v4/webhooks", {"destination": destination, "settings": AMO_WEBHOOK_SETTINGS, "sort": 5}, settings)
    if register_error:
        return False, register_error, {"destination": destination, "current": current}
    return True, "", {"destination": destination, "current": current, "registered": registered}


async def _webhook_watchdog_loop() -> None:
    while True:
        try:
            settings = await _settings_map()
            ok, error, details = await _ensure_amo_webhook(settings)
            if ok:
                _log("info", "vk-labels amo webhook watchdog OK: %s", _safe_url(details.get("destination", "")))
            else:
                _log("warning", "vk-labels amo webhook watchdog FAIL: %s", error)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log("error", "vk-labels amo webhook watchdog error: %s", exc)
        await asyncio.sleep(_webhook_watchdog_interval())


async def _amo_status_catalog(settings: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    body, error = await _amo_get("/api/v4/leads/pipelines", settings)
    if error:
        return [], error
    pipelines = []
    for pipeline in ((body or {}).get("_embedded") or {}).get("pipelines") or []:
        if not isinstance(pipeline, dict):
            continue
        statuses = []
        for status in ((pipeline.get("_embedded") or {}).get("statuses") or []):
            if isinstance(status, dict) and _clean(status.get("id"), 64):
                statuses.append({
                    "id": _clean(status.get("id"), 64),
                    "name": _clean(status.get("name"), 300) or _clean(status.get("id"), 64),
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


def _flat_payload_to_nested(flat: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"leads": {}, "account": {}}
    account_re = re.compile(r"^account\[([^\]]+)\]$")
    for key, value in flat.items():
        key = str(key)
        account_match = account_re.match(key)
        if account_match:
            result["account"][account_match.group(1)] = value
            continue
        parts = re.findall(r"([^\[\]]+)", key)
        if len(parts) < 4:
            result[key] = value
            continue
        cursor = result
        for part in parts[:-1]:
            next_value = cursor.setdefault(part, {})
            if not isinstance(next_value, dict):
                next_value = {}
                cursor[part] = next_value
            cursor = next_value
        cursor[parts[-1]] = value
    return result


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    ctype = request.headers.get("content-type", "").lower()
    if "application/json" in ctype:
        data = await request.json()
        return data if isinstance(data, dict) else {}, json.dumps(data, ensure_ascii=False)[:8000]
    form = await request.form()
    flat = {str(k): v for k, v in form.items()}
    return _flat_payload_to_nested(flat), json.dumps(flat, ensure_ascii=False)[:8000]


def _is_creation_update(event: dict[str, Any]) -> bool:
    if _clean(event.get("old_status_id"), 64):
        return False
    date_create = _int_value(event.get("date_create") or event.get("created_at"))
    last_modified = _int_value(event.get("last_modified") or event.get("updated_at"))
    return bool(date_create and last_modified and 0 <= last_modified - date_create <= 180)


def _iter_lead_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    leads = payload.get("leads") or {}
    if not isinstance(leads, dict):
        return []
    events = []
    for action in ("add", "status", "responsible", "update"):
        bucket = leads.get(action)
        if not bucket:
            continue
        items = bucket.values() if isinstance(bucket, dict) else bucket if isinstance(bucket, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            event = {str(k): v for k, v in item.items()}
            event["_action"] = action
            if action == "update" and _is_creation_update(event):
                event["_action"] = "add"
                event["_source_action"] = "update"
            elif action == "update":
                event["_ignored_update"] = "1"
            events.append(event)
    return events


def _category_for_event(event: dict[str, Any]) -> str:
    if _clean(event.get("_action"), 32) == "add":
        return "created"
    status_id = _clean(event.get("status_id"), 64)
    if status_id == "142":
        return "success"
    if status_id == "143":
        return "closed_lost"
    return ""


async def _find_rule(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("_ignored_update"):
        return None
    category = _category_for_event(event)
    pipeline_id = _clean(event.get("pipeline_id"), 64)
    status_id = _clean(event.get("status_id"), 64)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        if category == "created":
            cur = await db.execute(
                "SELECT * FROM label_rules WHERE active=1 AND category='created' ORDER BY id LIMIT 1"
            )
            row = await cur.fetchone()
            return dict(row) if row else None
        cur = await db.execute("SELECT * FROM label_rules WHERE active=1 AND category<>'created' ORDER BY id")
        rows = [dict(row) for row in await cur.fetchall()]
    matches = []
    for row in rows:
        for status in _rule_statuses(row):
            if status["status_id"] != status_id:
                continue
            if status["pipeline_id"] and status["pipeline_id"] != pipeline_id:
                continue
            expected_category = _clean(row.get("category"), 32)
            if category and expected_category != category:
                continue
            score = 0 if status["pipeline_id"] == pipeline_id else 1
            matches.append((score, int(row["id"]), row))
            break
    return sorted(matches, key=lambda item: (item[0], item[1]))[0][2] if matches else None


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


def _recursive_query_value(value: Any, target: str) -> str:
    if isinstance(value, str):
        return _query_value(value, target)
    if isinstance(value, dict):
        for item in value.values():
            found = _recursive_query_value(item, target)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _recursive_query_value(item, target)
            if found:
                return found
    return ""


def _recursive_key_value(value: Any, target: str) -> str:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() == target:
                return _clean(item, 200)
            found = _recursive_key_value(item, target)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _recursive_key_value(item, target)
            if found:
                return found
    return ""


def _custom_field_value(source: dict[str, Any], field_name: str) -> str:
    target = _clean(field_name).lower()
    if not target:
        return ""
    for key, value in source.items():
        if str(key).lower() == target:
            return _clean(value, 200)
    fields = source.get("custom_fields_values") or source.get("custom_fields") or []
    if isinstance(fields, dict):
        fields = list(fields.values())
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            candidates = [field.get("field_code"), field.get("field_name"), field.get("code"), field.get("name")]
            if not any(_clean(c).lower() == target for c in candidates):
                continue
            values = field.get("values") or []
            if isinstance(values, list) and values:
                first = values[0]
                return _clean(first.get("value") if isinstance(first, dict) else first, 200)
            if isinstance(values, dict):
                for item in values.values():
                    return _clean(item.get("value") if isinstance(item, dict) else item, 200)
            return _clean(field.get("value"), 200)
    return _recursive_key_value(source, target) or _recursive_query_value(source, target)


async def _load_deal(deal_id: str, settings: dict[str, str]) -> tuple[dict[str, Any], str]:
    if not deal_id:
        return {}, "deal_id пустой"
    body, error = await _amo_get(f"/api/v4/leads/{deal_id}", settings)
    return body or {}, error


def _peer_id_from_vk_id(vk_id: str) -> str:
    text = _clean(vk_id, 80)
    match = re.search(r"(?:convo/|sel=)(-?\d+)", text)
    if match:
        return match.group(1)
    digits = re.sub(r"\D+", "", text)
    return digits[:80]


async def _vk_api_call(method: str, params: dict[str, Any], settings: dict[str, str]) -> tuple[dict[str, Any] | list[Any] | int | None, str, dict[str, Any]]:
    token = _vk_token()
    if not token:
        return None, "VK_GROUP_TOKEN или VK_USER_TOKEN не заданы", {}
    payload = {k: v for k, v in params.items() if v is not None and v != ""}
    payload["access_token"] = token
    payload.setdefault("v", VK_API_VERSION)
    payload.setdefault("group_id", _env()["vk_group_id"])
    safe_payload = {k: ("***" if k == "access_token" else v) for k, v in payload.items()}
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            resp = await client.post(f"{VK_API_BASE}/{method}", data=payload)
        body = resp.json()
    except Exception as exc:
        return None, f"VK API {method} transport error: {type(exc).__name__}: {exc}", {"params": safe_payload}
    details = {"http_status": resp.status_code, "params": safe_payload, "response": body}
    if isinstance(body, dict) and body.get("error"):
        err = body["error"]
        return None, f"VK API {method} error {err.get('error_code')}: {err.get('error_msg') or err}", details
    if not isinstance(body, dict) or "response" not in body:
        return None, f"VK API {method} returned unexpected response", details
    return body.get("response"), "", details


async def _apply_official_action(peer_id: str, official_action: str, settings: dict[str, str]) -> tuple[bool, str, dict[str, Any]]:
    if official_action == "none":
        return True, "", {"skipped": True, "reason": "official_action=none"}
    if official_action in {"important_on", "important_off"}:
        response, error, details = await _vk_api_call(
            "messages.markAsImportantConversation",
            {"peer_id": peer_id, "important": 1 if official_action == "important_on" else 0},
            settings,
        )
        return error == "", error, {"method": "messages.markAsImportantConversation", "response": response, **details}
    if official_action in {"answered_on", "answered_off"}:
        response, error, details = await _vk_api_call(
            "messages.markAsAnsweredConversation",
            {"peer_id": peer_id, "answered": 1 if official_action == "answered_on" else 0},
            settings,
        )
        return error == "", error, {"method": "messages.markAsAnsweredConversation", "response": response, **details}
    return False, f"неизвестное official_action: {official_action}", {}


async def _store_dialog_label(peer_id: str, vk_id: str, rule: dict[str, Any], deal_id: str) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO dialog_labels(peer_id,vk_user_id,target_label,rule_id,deal_id,official_action,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(peer_id) DO UPDATE SET
                vk_user_id=excluded.vk_user_id,
                target_label=excluded.target_label,
                rule_id=excluded.rule_id,
                deal_id=excluded.deal_id,
                official_action=excluded.official_action,
                updated_at=excluded.updated_at
            """,
            (
                peer_id,
                vk_id,
                _clean(rule.get("target_label"), 120),
                rule.get("id"),
                deal_id,
                _clean(rule.get("official_action"), 40) or "none",
                _now(),
            ),
        )
        await db.commit()


async def _store_event(row: dict[str, Any]) -> int:
    keys = [
        "source", "action", "category", "deal_id", "pipeline_id", "status_id", "old_status_id",
        "responsible_user_id", "vk_id", "peer_id", "rule_id", "target_label", "official_action",
        "status", "success", "ignored", "error", "details", "raw_payload",
    ]
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            f"INSERT INTO events({','.join(keys)}) VALUES({','.join(['?'] * len(keys))})",
            tuple(row.get(k, "") for k in keys),
        )
        await db.commit()
        return int(cur.lastrowid)


async def _process_event(event: dict[str, Any], raw_payload: str, settings: dict[str, str], source: str = "webhook") -> dict[str, Any]:
    rule = await _find_rule(event)
    deal_id = _clean(event.get("id"), 64)
    base_row = {
        "source": source,
        "action": _clean(event.get("_action"), 32),
        "category": rule["category"] if rule else "",
        "deal_id": deal_id,
        "pipeline_id": _clean(event.get("pipeline_id"), 64),
        "status_id": _clean(event.get("status_id"), 64),
        "old_status_id": _clean(event.get("old_status_id"), 64),
        "responsible_user_id": _clean(event.get("responsible_user_id"), 64),
        "vk_id": "",
        "peer_id": "",
        "rule_id": rule["id"] if rule else None,
        "target_label": _clean(rule.get("target_label"), 120) if rule else "",
        "official_action": _clean(rule.get("official_action"), 40) if rule else "none",
        "status": "",
        "success": 0,
        "ignored": 0,
        "error": "",
        "details": "",
        "raw_payload": raw_payload,
    }
    if not rule:
        base_row["ignored"] = 1
        base_row["status"] = "ignored"
        base_row["error"] = "нет активного правила для события" if not event.get("_ignored_update") else "update ignored"
        base_row["details"] = json.dumps({"event": event}, ensure_ascii=False)
        event_id = await _store_event(base_row)
        return {"id": event_id, "ok": True, "ignored": True, "error": base_row["error"]}

    deal_data: dict[str, Any] = {}
    deal_error = ""
    vk_id = _custom_field_value(event, settings.get("vk_field", "utm_term"))
    if not vk_id:
        deal_data, deal_error = await _load_deal(deal_id, settings)
        vk_id = _custom_field_value(deal_data, settings.get("vk_field", "utm_term"))
    peer_id = _peer_id_from_vk_id(vk_id)
    base_row["vk_id"] = vk_id
    base_row["peer_id"] = peer_id
    if not peer_id:
        base_row["status"] = "failed"
        base_row["error"] = f"VK ID не найден в поле {settings.get('vk_field', 'utm_term')}"
        base_row["details"] = json.dumps({"event": event, "deal_error": deal_error, "deal": deal_data}, ensure_ascii=False)
        event_id = await _store_event(base_row)
        return {"id": event_id, "ok": False, "error": base_row["error"]}

    await _store_dialog_label(peer_id, vk_id, rule, deal_id)
    ok, error, vk_details = await _apply_official_action(peer_id, base_row["official_action"], settings)
    base_row["success"] = int(ok)
    base_row["status"] = "applied" if ok and base_row["official_action"] != "none" else "stored_only" if ok else "failed"
    base_row["error"] = error
    base_row["details"] = json.dumps({"event": event, "deal_error": deal_error, "vk": vk_details}, ensure_ascii=False)
    event_id = await _store_event(base_row)
    if ok:
        _log("info", "vk label %s for peer %s via %s", base_row["target_label"], peer_id, base_row["official_action"])
    else:
        _log("warning", "vk label %s for peer %s failed: %s", base_row["target_label"], peer_id, error)
    return {"id": event_id, "ok": ok, "ignored": False, "error": error, "status": base_row["status"]}


def _secret_ok(request: Request, settings: dict[str, str]) -> bool:
    secret = _clean(settings.get("webhook_secret"), 200)
    if not secret:
        return True
    supplied = request.query_params.get("secret") or request.headers.get("X-Nexus-Secret") or request.headers.get("X-Webhook-Secret") or ""
    return _clean(supplied, 200) == secret


def _row_public(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    for key in ("success", "ignored", "active"):
        if key in row:
            row[key] = bool(row[key])
    return row


@router.get("/health")
async def health():
    return {"ok": True, "module": MODULE_ID}


@router.get("/env-status")
async def env_status(request: Request):
    await _require_panel_user(request)
    env = _env()
    settings = await _settings_map()
    return {
        "ok": True,
        "VK_GROUP_TOKEN": bool(env["vk_group_token"]),
        "VK_USER_TOKEN": bool(env["vk_user_token"]),
        "VK_GROUP_ID": env["vk_group_id"],
        "AMO_BASE_URL": bool(env["amo_base_url"]),
        "AMO_ACCESS_TOKEN": bool(env["amo_token"]),
        "webhook_secret": bool(settings.get("webhook_secret")),
        "ready": bool(_vk_token() and env["amo_base_url"] and env["amo_token"]),
        "test_group_id": TEST_GROUP_ID,
        "test_peer_id": TEST_PEER_ID,
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    env = _env()
    return {
        **settings,
        "webhook_secret_source": "env" if env["webhook_secret"] else "db",
        "webhook_url": _amo_webhook_destination(settings),
        "safe_webhook_url": _safe_url(_amo_webhook_destination(settings)),
        "amo_base_url": env["amo_base_url"],
        "vk_group_id": env["vk_group_id"],
        "has_vk_token": bool(_vk_token()),
        "has_amo_token": bool(env["amo_token"]),
    }


@router.post("/settings")
async def post_settings(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.get("/webhook/status")
async def webhook_status(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    ok, error, details = await _ensure_amo_webhook(settings)
    if "destination" in details:
        details["destination"] = _safe_url(details["destination"])
    return {"ok": ok, "error": error, "details": details}


@router.get("/amo/statuses")
async def amo_statuses(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    pipelines, error = await _amo_status_catalog(settings)
    if error:
        return JSONResponse({"ok": False, "error": error, "pipelines": []}, status_code=502)
    return {"ok": True, "pipelines": pipelines}


@router.get("/actions")
async def actions(request: Request):
    await _require_panel_user(request)
    return {"actions": [{"id": key, "name": label} for key, label in OFFICIAL_ACTIONS.items()]}


@router.get("/rules")
async def list_rules(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM label_rules ORDER BY active DESC, id")
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["category_label"] = CATEGORY_LABELS.get(row["category"], row["category"])
        row["statuses"] = _rule_statuses(row)
        row["statuses_count"] = len(row["statuses"])
        row["official_action_label"] = OFFICIAL_ACTIONS.get(row["official_action"], row["official_action"])
    return [_row_public(row) for row in rows]


@router.post("/rules")
async def save_rule(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    rule_id = int(data.get("id") or 0)
    category = _clean(data.get("category"), 32)
    if category not in CATEGORY_LABELS:
        return JSONResponse({"ok": False, "error": "неверная категория"}, status_code=400)
    target_label = _clean(data.get("target_label"), 120)
    official_action = _clean(data.get("official_action"), 40) or "none"
    name = _clean(data.get("name"), 300)
    note = _clean(data.get("note"), 1000)
    active = 1 if data.get("active", True) else 0
    if not target_label:
        return JSONResponse({"ok": False, "error": "target_label обязателен"}, status_code=400)
    if official_action not in OFFICIAL_ACTIONS:
        return JSONResponse({"ok": False, "error": "неверное official_action"}, status_code=400)
    statuses = []
    if category != "created":
        raw_statuses = data.get("statuses")
        if isinstance(raw_statuses, list):
            statuses = [_status_item(item) for item in raw_statuses if isinstance(item, dict)]
            statuses = [status for status in statuses if status["status_id"]]
        if not statuses:
            status_id = _clean(data.get("status_id"), 64)
            if status_id:
                statuses = [_status_item(data)]
        if not statuses:
            return JSONResponse({"ok": False, "error": "выберите хотя бы один статус amoCRM"}, status_code=400)
    first = statuses[0] if statuses else {"pipeline_id": "", "pipeline_name": "", "status_id": "", "status_name": ""}
    async with aiosqlite.connect(_must_db()) as db:
        if rule_id:
            await db.execute(
                """
                UPDATE label_rules SET category=?,pipeline_id=?,pipeline_name=?,status_id=?,status_name=?,
                    statuses_json=?,target_label=?,official_action=?,name=?,note=?,active=?,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (
                    category, first["pipeline_id"], first["pipeline_name"], first["status_id"], first["status_name"],
                    _statuses_json(statuses), target_label, official_action, name, note, active, rule_id,
                ),
            )
        else:
            await db.execute(
                """
                INSERT INTO label_rules(category,pipeline_id,pipeline_name,status_id,status_name,statuses_json,target_label,official_action,name,note,active)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    category, first["pipeline_id"], first["pipeline_name"], first["status_id"], first["status_name"],
                    _statuses_json(statuses), target_label, official_action, name, note, active,
                ),
            )
        await db.commit()
    return {"ok": True}


@router.put("/rules/{rule_id}/toggle")
async def toggle_rule(rule_id: int, request: Request):
    await _require_panel_user(request)
    data = await request.json()
    active = 1 if data.get("active") else 0
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("UPDATE label_rules SET active=?, updated_at=? WHERE id=?", (active, _now(), rule_id))
        await db.commit()
    return {"ok": True}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM label_rules WHERE id=?", (rule_id,))
        await db.commit()
    return {"ok": True}


@router.get("/events")
async def list_events(request: Request, status: str = "", limit: int = 100):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit or 100)))
    status = _clean(status, 40)
    where = ""
    params: list[Any] = []
    if status:
        where = "WHERE status=?"
        params.append(status)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT id,received_at,source,action,deal_id,pipeline_id,status_id,vk_id,peer_id,
                   rule_id,target_label,official_action,status,success,ignored,error
            FROM events {where}
            ORDER BY id DESC LIMIT ?
            """,
            (*params, limit),
        )
        rows = [dict(row) for row in await cur.fetchall()]
    return [_row_public(row) for row in rows]


@router.get("/events/{event_id}")
async def event_detail(event_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM events WHERE id=?", (event_id,))).fetchone()
    if not row:
        raise HTTPException(404, "event not found")
    data = _row_public(dict(row))
    try:
        data["details_json"] = json.loads(data.get("details") or "{}")
    except Exception:
        data["details_json"] = {}
    return data


@router.get("/labels")
async def list_labels(request: Request, limit: int = 100):
    await _require_panel_user(request)
    limit = max(1, min(500, int(limit or 100)))
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM dialog_labels ORDER BY updated_at DESC LIMIT ?", (limit,))
        rows = [dict(row) for row in await cur.fetchall()]
    return rows


@router.post("/probe/vk")
async def probe_vk(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    response, error, details = await _vk_api_call(
        "messages.getConversationsById",
        {"peer_ids": TEST_PEER_ID, "group_id": TEST_GROUP_ID, "extended": 0},
        settings,
    )
    return {"ok": not error, "error": error, "test_group_id": TEST_GROUP_ID, "test_peer_id": TEST_PEER_ID, "response": response, "details": details}


@router.post("/test/dialog")
async def test_dialog(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    peer_id = _clean(data.get("peer_id") or TEST_PEER_ID, 80)
    group_id = _clean(data.get("group_id") or TEST_GROUP_ID, 80)
    if peer_id != TEST_PEER_ID or group_id != TEST_GROUP_ID:
        return JSONResponse({"ok": False, "error": "test endpoint разрешен только для peer_id=1105209997 и group_id=225075265"}, status_code=400)
    official_action = _clean(data.get("official_action"), 40) or "none"
    target_label = _clean(data.get("target_label"), 120) or "Тест"
    if official_action not in OFFICIAL_ACTIONS:
        return JSONResponse({"ok": False, "error": "неверное official_action"}, status_code=400)
    settings = await _settings_map()
    fake_rule = {"id": None, "target_label": target_label, "official_action": official_action}
    await _store_dialog_label(peer_id, peer_id, fake_rule, "")
    ok, error, vk_details = await _apply_official_action(peer_id, official_action, settings)
    row = {
        "source": "test",
        "action": "test",
        "category": "",
        "deal_id": "",
        "pipeline_id": "",
        "status_id": "",
        "old_status_id": "",
        "responsible_user_id": "",
        "vk_id": peer_id,
        "peer_id": peer_id,
        "rule_id": None,
        "target_label": target_label,
        "official_action": official_action,
        "status": "applied" if ok and official_action != "none" else "stored_only" if ok else "failed",
        "success": int(ok),
        "ignored": 0,
        "error": error,
        "details": json.dumps({"vk": vk_details, "test_group_id": group_id}, ensure_ascii=False),
        "raw_payload": json.dumps(data, ensure_ascii=False),
    }
    event_id = await _store_event(row)
    return {"ok": ok, "error": error, "id": event_id, "status": row["status"], "test_url": f"https://vk.com/gim{TEST_GROUP_ID}/convo/{TEST_PEER_ID}"}


@router.post("/webhook")
async def webhook(request: Request):
    settings = await _settings_map()
    if not _secret_ok(request, settings):
        _log("warning", "vk-labels webhook invalid secret")
        return {"ok": False, "error": "invalid secret"}
    payload, raw_payload = await _read_payload(request)
    events = _iter_lead_events(payload)
    if not events:
        event_id = await _store_event({
            "source": "webhook",
            "action": "",
            "category": "",
            "deal_id": "",
            "pipeline_id": "",
            "status_id": "",
            "old_status_id": "",
            "responsible_user_id": "",
            "vk_id": "",
            "peer_id": "",
            "rule_id": None,
            "target_label": "",
            "official_action": "none",
            "status": "ignored",
            "success": 0,
            "ignored": 1,
            "error": "lead events not found",
            "details": json.dumps({"payload": payload}, ensure_ascii=False),
            "raw_payload": raw_payload,
        })
        return {"ok": True, "ignored": True, "id": event_id, "error": "lead events not found"}
    results = []
    for event in events:
        try:
            results.append(await _process_event(event, raw_payload, settings))
        except Exception as exc:
            _log("error", "vk-labels webhook event error: %s", exc, exc_info=True)
            results.append({"ok": False, "error": str(exc)})
    return {"ok": any(item.get("ok") for item in results), "results": results}
