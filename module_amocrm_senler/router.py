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

_db_path = None
_logger: logging.Logger | None = None
MODULE_ID = "amocrm-senler"

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


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


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
                statuses_json   TEXT NOT NULL DEFAULT '[]',
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
            ("statuses_json", "ALTER TABLE status_bindings ADD COLUMN statuses_json TEXT NOT NULL DEFAULT '[]'"),
            ("exclusive_groups", "ALTER TABLE status_bindings ADD COLUMN exclusive_groups INTEGER NOT NULL DEFAULT 0"),
        ):
            try:
                await db.execute(ddl)
            except Exception:
                pass
        await _migrate_binding_statuses(db)
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
                    INSERT INTO status_bindings(category,pipeline_id,pipeline_name,status_id,status_name,statuses_json,subscription_id,exclusive_groups,name,note,active)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        category, pipeline_id, pipeline_name, status_id, status_name,
                        _statuses_json([{
                            "pipeline_id": pipeline_id,
                            "pipeline_name": pipeline_name,
                            "status_id": status_id,
                            "status_name": status_name,
                        }] if category != "created" and status_id else []),
                        subscription_id, 0, name, note, active,
                    ),
                )
        await db.commit()
    _log("info", "amocrm-senler DB initialized")


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _status_item(item: dict[str, Any]) -> dict[str, str]:
    status_id = _clean(item.get("status_id") or item.get("id"), 64)
    return {
        "pipeline_id": _clean(item.get("pipeline_id"), 64),
        "pipeline_name": _clean(item.get("pipeline_name"), 300),
        "status_id": status_id,
        "status_name": _clean(item.get("status_name") or item.get("name"), 300) or status_id,
    }


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


def _binding_statuses(row: dict[str, Any]) -> list[dict[str, str]]:
    raw = row.get("statuses_json") or "[]"
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        parsed = []
    statuses = [_status_item(item) for item in parsed if isinstance(item, dict)]
    statuses = [item for item in statuses if item["status_id"]]
    if statuses:
        return statuses
    status_id = _clean(row.get("status_id"), 64)
    if not status_id:
        return []
    return [_status_item({
        "pipeline_id": row.get("pipeline_id"),
        "pipeline_name": row.get("pipeline_name"),
        "status_id": status_id,
        "status_name": row.get("status_name"),
    })]


async def _migrate_binding_statuses(db: aiosqlite.Connection) -> None:
    db.row_factory = aiosqlite.Row
    cur = await db.execute("SELECT * FROM status_bindings ORDER BY id")
    rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        try:
            existing = json.loads(row.get("statuses_json") or "[]")
        except Exception:
            existing = []
        if isinstance(existing, list) and existing:
            continue
        status_id = _clean(row.get("status_id"), 64)
        if not status_id:
            continue
        await db.execute(
            "UPDATE status_bindings SET statuses_json=? WHERE id=?",
            (_statuses_json([row]), row["id"]),
        )

    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("category") == "created":
            continue
        key = (
            _clean(row.get("category"), 32),
            _clean(row.get("subscription_id"), 64),
            _clean(row.get("name"), 300),
            _clean(row.get("note"), 1000),
        )
        groups.setdefault(key, []).append(row)

    for grouped in groups.values():
        if len(grouped) < 2:
            continue
        statuses: list[dict[str, Any]] = []
        for row in grouped:
            statuses.extend(_binding_statuses(row) or ([row] if _clean(row.get("status_id"), 64) else []))
        keeper = grouped[0]
        first = _binding_statuses({"statuses_json": _statuses_json(statuses)})[:1]
        first_status = first[0] if first else {"pipeline_id": "", "pipeline_name": "", "status_id": "", "status_name": ""}
        await db.execute(
            """
            UPDATE status_bindings
            SET pipeline_id=?, pipeline_name=?, status_id=?, status_name=?, statuses_json=?,
                exclusive_groups=?, active=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            WHERE id=?
            """,
            (
                first_status["pipeline_id"], first_status["pipeline_name"],
                first_status["status_id"], first_status["status_name"],
                _statuses_json(statuses),
                1 if any(int(row.get("exclusive_groups") or 0) for row in grouped) else 0,
                1 if any(int(row.get("active") or 0) for row in grouped) else 0,
                keeper["id"],
            ),
        )
        await db.execute(
            f"DELETE FROM status_bindings WHERE id IN ({','.join(['?'] * (len(grouped) - 1))})",
            tuple(row["id"] for row in grouped[1:]),
        )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


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
        cursor: dict[str, Any] = result
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
        return data if isinstance(data, dict) else {}, json.dumps(data, ensure_ascii=False)[:6000]
    form = await request.form()
    flat = {str(k): v for k, v in form.items()}
    return _flat_payload_to_nested(flat), json.dumps(flat, ensure_ascii=False)[:6000]


def _iter_lead_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    leads = payload.get("leads") or {}
    if not isinstance(leads, dict):
        return []
    events: list[dict[str, Any]] = []
    for action in ("add", "status", "responsible", "update"):
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
            if action == "update" and _is_creation_update(event):
                event["_action"] = "add"
                event["_source_action"] = "update"
            elif action == "update":
                event["_ignored_update"] = "1"
            events.append(event)
    return events


def _is_creation_update(event: dict[str, Any]) -> bool:
    if _clean(event.get("old_status_id"), 64):
        return False
    date_create = _int_value(event.get("date_create") or event.get("created_at"))
    last_modified = _int_value(event.get("last_modified") or event.get("updated_at"))
    if not date_create or not last_modified:
        return False
    return 0 <= last_modified - date_create <= 180


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
    if not isinstance(fields, list):
        fields = []
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
        if isinstance(values, dict):
            for item in values.values():
                if isinstance(item, dict) and item.get("value"):
                    return _clean(item.get("value"), 200)
                if item:
                    return _clean(item, 200)
        return _clean(field.get("value"), 200)
    direct = _recursive_key_value(source, target)
    if direct:
        return direct
    query_value = _recursive_query_value(source, target)
    if query_value:
        return query_value
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


def _category_for_action(action: str) -> str:
    return "created" if action == "add" else ""


async def _find_binding(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("_ignored_update"):
        return None
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
                ORDER BY id
                LIMIT 1
                """,
                (category,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None
        cur = await db.execute(
            """
            SELECT * FROM status_bindings
            WHERE active=1 AND category<>'created'
            ORDER BY id
            """,
        )
        rows = [dict(row) for row in await cur.fetchall()]
    matches = []
    for row in rows:
        for status in _binding_statuses(row):
            if status["status_id"] != status_id:
                continue
            if status["pipeline_id"] and status["pipeline_id"] != pipeline_id:
                continue
            score = 0 if status["pipeline_id"] == pipeline_id else 1
            matches.append((score, int(row["id"]), row))
            break
    if not matches:
        return None
    return sorted(matches, key=lambda item: (item[0], item[1]))[0][2]


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


async def _amo_post(path: str, payload: Any, settings: dict[str, str]) -> tuple[dict[str, Any] | None, str]:
    env = _env()
    if not env["amo_base_url"] or not env["amo_token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы"
    try:
        async with httpx.AsyncClient(timeout=_timeout(settings)) as client:
            resp = await client.post(
                env["amo_base_url"] + path,
                headers={
                    "Authorization": f"Bearer {env['amo_token']}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if resp.status_code >= 400:
            return None, f"amoCRM HTTP {resp.status_code}: {resp.text[:500]}"
        return resp.json() if resp.text else {}, ""
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


def _amo_success_note_text(vk_id: str, subscription_id: str, responsible_name: str, deal_id: str) -> str:
    lines = [
        "amoCRM -> Senler: подписчик обработан и отправлен в Senler.",
        f"Группа Senler: {subscription_id}",
        f"Сделка amoCRM: {deal_id}",
        f"VK диалог: https://vk.ru/gim225075265?sel={vk_id}",
    ]
    if responsible_name:
        lines.insert(3, f"Ответственный: {responsible_name}")
    return "\n".join(lines)


async def _amo_add_success_note(
    deal_id: str,
    vk_id: str,
    subscription_id: str,
    responsible_name: str,
    settings: dict[str, str],
) -> tuple[bool, str, dict[str, Any]]:
    text = _amo_success_note_text(vk_id, subscription_id, responsible_name, deal_id)
    body, error = await _amo_post(
        f"/api/v4/leads/{deal_id}/notes",
        [{"note_type": "common", "params": {"text": text}}],
        settings,
    )
    details = {"request": {"note_type": "common", "params": {"text": text}}, "response": body}
    return not error, error, details


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
        subscribers = body.get("subscribers")
        if isinstance(subscribers, list):
            failures = []
            for item in subscribers:
                if not isinstance(item, dict) or item.get("success") is not False:
                    continue
                msg = _clean(item.get("error") or item.get("error_message") or item, 300)
                low = msg.lower()
                if endpoint == _EP_SUB_ADD and ("already" in low or "уже" in low):
                    continue
                if endpoint == _EP_SUB_DEL and (
                    "not found" in low or "not subscribed" in low or "не найден" in low or "не подпис" in low
                ):
                    continue
                failures.append(msg)
            if failures:
                msg = "; ".join(failures)
                return False, msg or "ошибка подписчика Senler", {
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

    vars_to_set = [("amo_deal_id", deal_id)]
    if responsible_name:
        vars_to_set.insert(0, ("amo_responsible", responsible_name))
    else:
        details["vars"].append({"name": "amo_responsible", "ok": True, "skipped": True, "reason": "responsible_user_id пустой или amoCRM не вернула имя"})
    for name, value in vars_to_set:
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
        if event.get("_ignored_update"):
            base_row["error"] = "update ignored: событие изменения без создания или смены статуса"
        else:
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

    responsible_name = ""
    responsible_error = ""
    if responsible_user_id and responsible_user_id != "0":
        responsible_name, responsible_error = await _responsible_name(responsible_user_id, settings)
    else:
        responsible_error = "responsible_user_id пустой или 0"
    base_row["responsible_name"] = responsible_name

    ok, error, senler_details = await _senler_apply(
        binding["subscription_id"], vk_id, responsible_name, deal_id, settings
    )
    if ok and not responsible_name:
        error = f"amo_responsible не записан: {responsible_error}"
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
    if ok:
        note_ok, note_error, note_details = await _amo_add_success_note(
            deal_id, vk_id, binding["subscription_id"], responsible_name, settings
        )
        senler_details["amo_note"] = {"ok": note_ok, "error": note_error, "details": note_details}
        if not note_ok:
            note_warning = f"amo note не добавлен: {note_error}"
            error = f"{error}; {note_warning}" if error else note_warning
    else:
        senler_details["amo_note"] = {"ok": False, "skipped": True}
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
        if error:
            _log("warning", "amo deal %s -> Senler %s OK with warning: %s", deal_id, binding["subscription_id"], error)
        else:
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
async def env_status(request: Request):
    await _require_panel_user(request)
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
async def get_settings(request: Request):
    await _require_panel_user(request)
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
    await _require_panel_user(request)
    data = await request.json()
    return await _save_settings(data if isinstance(data, dict) else {})


@router.get("/amo/statuses")
async def amo_statuses(request: Request):
    await _require_panel_user(request)
    settings = await _settings_map()
    pipelines, error = await _amo_status_catalog(settings)
    if error:
        return JSONResponse({"error": error, "pipelines": []}, status_code=502)
    return {"pipelines": pipelines}


@router.get("/bindings")
async def list_bindings(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT * FROM status_bindings
            ORDER BY CASE category
                WHEN 'created' THEN 1
                WHEN 'work' THEN 2
                WHEN 'closed_lost' THEN 3
                WHEN 'success' THEN 4
                ELSE 5
            END, id
            """
        )
        rows = [dict(row) for row in await cur.fetchall()]
    settings = await _settings_map()
    pipelines, _ = await _amo_status_catalog(settings)
    lookup = _status_lookup(pipelines)
    for row in rows:
        row["category_label"] = CATEGORY_LABELS.get(row["category"], row["category"])
        statuses = []
        for status in _binding_statuses(row):
            found = lookup.get((_clean(status.get("pipeline_id"), 64), _clean(status.get("status_id"), 64)))
            if found:
                status = {**status}
                if status.get("pipeline_id") and not status.get("pipeline_name"):
                    status["pipeline_name"] = found["pipeline_name"]
                elif not status.get("pipeline_id") and not status.get("pipeline_name"):
                    status["pipeline_name"] = "Любая воронка"
                status["status_name"] = found["status_name"]
            statuses.append(status)
        row["statuses"] = statuses
        row["statuses_count"] = len(statuses)
        if row["category"] == "created":
            row["pipeline_name"] = "Любая воронка"
            row["status_name"] = "Событие создания сделки"
            row["statuses_label"] = "Событие создания сделки"
        elif len(statuses) == 1:
            status = statuses[0]
            row["pipeline_id"] = status["pipeline_id"]
            row["pipeline_name"] = status["pipeline_name"] or "Любая воронка"
            row["status_id"] = status["status_id"]
            row["status_name"] = status["status_name"] or status["status_id"]
            row["statuses_label"] = row["status_name"]
        elif statuses:
            pipeline_names = {_clean(status.get("pipeline_name"), 300) for status in statuses if _clean(status.get("pipeline_name"), 300)}
            row["pipeline_name"] = next(iter(pipeline_names)) if len(pipeline_names) == 1 else "Несколько воронок"
            row["status_name"] = f"Выбрано статусов: {len(statuses)}"
            row["statuses_label"] = row["status_name"]
        else:
            row["pipeline_name"] = row.get("pipeline_name") or "Любая воронка"
            row["status_name"] = "Не выбран"
            row["statuses_label"] = "Не выбран"
    return rows


@router.post("/bindings")
async def save_binding(request: Request):
    await _require_panel_user(request)
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
        statuses = []
    elif isinstance(raw_statuses, list) and raw_statuses:
        statuses = []
        for item in raw_statuses:
            if not isinstance(item, dict):
                continue
            status = _status_item(item)
            if not status["status_id"]:
                continue
            statuses.append(status)
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
    clean_statuses = json.loads(_statuses_json(statuses))
    first_status = clean_statuses[0] if clean_statuses else {
        "pipeline_id": "",
        "pipeline_name": "",
        "status_id": "",
        "status_name": "Событие создания сделки",
    }
    row_name = name or f"{CATEGORY_LABELS[category]}: {first_status['status_name']}"
    async with aiosqlite.connect(_db_path) as db:
        if binding_id:
            await db.execute(
                """
                UPDATE status_bindings
                SET category=?, pipeline_id=?, pipeline_name=?, status_id=?, status_name=?, statuses_json=?,
                    subscription_id=?, exclusive_groups=?, name=?, note=?, active=?,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                WHERE id=?
                """,
                (
                    category, first_status["pipeline_id"], first_status["pipeline_name"], first_status["status_id"],
                    first_status["status_name"], _statuses_json(clean_statuses), subscription_id, exclusive_groups,
                    row_name, note, active, binding_id,
                ),
            )
            saved_id = binding_id
        else:
            cur = await db.execute(
                """
                INSERT INTO status_bindings(category,pipeline_id,pipeline_name,status_id,status_name,statuses_json,subscription_id,exclusive_groups,name,note,active)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    category, first_status["pipeline_id"], first_status["pipeline_name"], first_status["status_id"],
                    first_status["status_name"], _statuses_json(clean_statuses), subscription_id, exclusive_groups,
                    row_name, note, active,
                ),
            )
            saved_id = int(cur.lastrowid)
        await db.commit()
    return {"ok": True, "id": saved_id}


@router.put("/bindings/{binding_id}/toggle")
async def toggle_binding(binding_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "UPDATE status_bindings SET active=1-active, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (binding_id,),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/bindings/{binding_id}")
async def delete_binding(binding_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM status_bindings WHERE id=?", (binding_id,))
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
    for key in ("details", "raw_payload"):
        try:
            data[key] = json.loads(data[key]) if data[key] else {}
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
        active = (await (await db.execute("SELECT COUNT(*) FROM status_bindings WHERE active=1")).fetchone())[0]
    return {"events": total, "success": success, "ignored": ignored, "active_bindings": active}


@router.post("/webhook")
async def webhook(request: Request):
    settings = await _settings_map()
    if not _secret_ok(request, settings):
        _log("warning", "webhook rejected: invalid secret")
        return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=200)
    try:
        payload, raw_payload = await _read_payload(request)
        events = _iter_lead_events(payload)
        _log("info", "webhook received: %s lead event(s)", len(events))
        if not events:
            event_id = await _store_event({
                "action": "webhook",
                "category": "",
                "deal_id": "",
                "pipeline_id": "",
                "status_id": "",
                "old_status_id": "",
                "responsible_user_id": "",
                "responsible_name": "",
                "vk_id": "",
                "binding_id": None,
                "subscription_id": "",
                "success": 0,
                "ignored": 1,
                "error": "lead events not found",
                "details": json.dumps({"payload_keys": list(payload.keys())}, ensure_ascii=False),
                "raw_payload": raw_payload,
            })
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
