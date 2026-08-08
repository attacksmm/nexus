from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlparse
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request

from orchestrator.auth import can_access_module, verify_token_from_request


router = APIRouter()

MODULE_ID = "amocrm-duplicates"
RETRY_DELAYS = (3, 7, 15, 30, 60)
MAX_EVENT_AGE_SECONDS = 120
FALLBACK_POLL_INTERVAL_SECONDS = 15
FALLBACK_POLL_LOOKBACK_SECONDS = 15 * 60
TERMINAL_STATES = {"completed", "ignored", "no_data", "failed"}
LEAD_KEY_RE = re.compile(r"^leads\[add\]\[(?P<idx>\d+)\]\[(?P<field>[^\]]+)\]$")
PLATFORM_ID_RE = re.compile(r"(?<!\d)-?\d{5,20}(?!\d)")
AI_NOTE_TITLE = "Общение с ИИ:"
AI_NOTE_TITLES = (AI_NOTE_TITLE, "Краткая сводка диалога")

_db_path: str | None = None
_module_dir: Path | None = None
_logger: logging.Logger | None = None
_tasks: dict[str, asyncio.Task] = {}
_poll_task: asyncio.Task | None = None
_poll_last_at = ""
_poll_last_error = ""
_poll_last_seen = 0
_poll_waiting_unsorted = 0
_poll_last_resumed = 0
_ai_lock = asyncio.Lock()


DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": False,
    "defaults_seeded": False,
    "request_timeout": 15,
    "source_scope": {"all": True, "statuses": []},
    "search": {
        "operator": "OR",
        "groups": [
            {
                "id": "phone",
                "name": "Телефон контакта",
                "operator": "AND",
                "conditions": [
                    {
                        "id": "phone",
                        "entity": "contacts",
                        "field_id": "",
                        "field_code": "PHONE",
                        "field_name": "Телефон",
                    }
                ],
            }
        ],
    },
    "base_tags": ["Дубль?"],
    "copy_responsible_from_latest_duplicate": False,
    "state_rules": [],
    "ai": {"openrouter_summary_enabled": False},
}


async def setup(ctx: Any) -> None:
    global _db_path, _module_dir, _logger, _poll_task
    _db_path = str(ctx.db_path)
    _module_dir = Path(ctx.module_dir)
    _logger = getattr(ctx, "logger", logging.getLogger("nexus.mod.amocrm-duplicates"))
    await _bootstrap()
    _poll_task = asyncio.create_task(_fallback_poll_loop())


async def shutdown() -> None:
    global _poll_task
    poll_task, _poll_task = _poll_task, None
    if poll_task and not poll_task.done():
        poll_task.cancel()
        await asyncio.gather(poll_task, return_exceptions=True)
    tasks = list(_tasks.values())
    _tasks.clear()
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> str:
    if not _db_path:
        raise RuntimeError("module is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean(value: Any, limit: int = 1000) -> str:
    return str(value or "").strip()[:limit]


def _int(value: Any) -> int:
    try:
        return int(float(str(value or "0")))
    except (TypeError, ValueError):
        return 0


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean(value, 20).lower() in {"1", "true", "yes", "on", "да"}


def _loads(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return fallback


def _env() -> dict[str, str]:
    return {
        "base_url": os.environ.get("AMO_BASE_URL", "").strip().rstrip("/"),
        "token": os.environ.get("AMO_ACCESS_TOKEN", "").strip(),
        "secret": os.environ.get("AMO_DUPLICATES_WEBHOOK_SECRET", "").strip(),
    }


async def _bootstrap() -> None:
    await _init_db()
    await _recover_events()
    _log("info", "amocrm-duplicates initialized")


async def _init_db() -> None:
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id TEXT NOT NULL UNIQUE,
                received_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                source_pipeline_id TEXT NOT NULL DEFAULT '',
                source_status_id TEXT NOT NULL DEFAULT '',
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                tags_json TEXT NOT NULL DEFAULT '[]',
                details_json TEXT NOT NULL DEFAULT '{}',
                raw_payload TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_amodup_events_received ON events(received_at);
            CREATE INDEX IF NOT EXISTS idx_amodup_events_state ON events(state);
            CREATE TABLE IF NOT EXISTS catalog_cache (
                id INTEGER PRIMARY KEY CHECK (id=1),
                payload TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings(key,value) VALUES('config_json',?)",
            (json.dumps(DEFAULT_CONFIG, ensure_ascii=False),),
        )
        env_secret = _env()["secret"]
        if not env_secret:
            cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
            row = await cur.fetchone()
            if not _clean(row[0] if row else "", 300):
                await db.execute(
                    "INSERT INTO settings(key,value) VALUES('webhook_secret',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (secrets.token_urlsafe(24),),
                )
        await db.commit()


async def _require_user(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _secret() -> str:
    env_secret = _env()["secret"]
    if env_secret:
        return env_secret
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key='webhook_secret'")
        row = await cur.fetchone()
    return _clean(row[0] if row else "", 300)


async def _secret_ok(request: Request) -> bool:
    expected = await _secret()
    if not expected:
        return True
    supplied = (
        request.query_params.get("secret")
        or request.headers.get("X-Nexus-Secret")
        or request.headers.get("X-Webhook-Secret")
        or ""
    )
    return secrets.compare_digest(expected, _clean(supplied, 300))


def _clean_operator(value: Any) -> str:
    return "AND" if _clean(value, 10).upper() == "AND" else "OR"


def _clean_status_pairs(value: Any, limit: int = 5000) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        pipeline_id = _clean(item.get("pipeline_id"), 64)
        status_id = _clean(item.get("status_id"), 64)
        if not pipeline_id or not status_id or (pipeline_id, status_id) in seen:
            continue
        seen.add((pipeline_id, status_id))
        rows.append({"pipeline_id": pipeline_id, "status_id": status_id})
        if len(rows) >= limit:
            break
    return rows


def _clean_tags(value: Any) -> list[str]:
    raw = value if isinstance(value, list) else re.split(r"[\n,;]+", str(value or ""))
    tags: list[str] = []
    seen: set[str] = set()
    for item in raw:
        tag = _clean(item, 200)
        key = tag.casefold()
        if tag and key not in seen:
            seen.add(key)
            tags.append(tag)
        if len(tags) >= 30:
            break
    return tags


def _clean_config(raw: Any) -> dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    ai = data.get("ai") if isinstance(data.get("ai"), dict) else {}
    source = data.get("source_scope") if isinstance(data.get("source_scope"), dict) else {}
    search = data.get("search") if isinstance(data.get("search"), dict) else {}
    groups: list[dict[str, Any]] = []
    condition_ids: set[str] = set()
    for group_index, raw_group in enumerate(search.get("groups") if isinstance(search.get("groups"), list) else []):
        if not isinstance(raw_group, dict) or len(groups) >= 20:
            continue
        conditions: list[dict[str, str]] = []
        for condition_index, raw_condition in enumerate(raw_group.get("conditions") if isinstance(raw_group.get("conditions"), list) else []):
            if not isinstance(raw_condition, dict) or len(conditions) >= 12:
                continue
            entity = "contacts" if raw_condition.get("entity") == "contacts" else "leads"
            field_id = _clean(raw_condition.get("field_id"), 64)
            field_code = _clean(raw_condition.get("field_code"), 120).upper()
            field_name = _clean(raw_condition.get("field_name"), 300)
            if not any((field_id, field_code, field_name)):
                continue
            condition_id = _clean(raw_condition.get("id"), 80) or f"c{group_index + 1}_{condition_index + 1}"
            if condition_id in condition_ids:
                condition_id = f"{condition_id}_{group_index + 1}_{condition_index + 1}"
            condition_ids.add(condition_id)
            conditions.append(
                {
                    "id": condition_id,
                    "entity": entity,
                    "field_id": field_id,
                    "field_code": field_code,
                    "field_name": field_name,
                }
            )
        if conditions:
            groups.append(
                {
                    "id": _clean(raw_group.get("id"), 80) or f"g{group_index + 1}",
                    "name": _clean(raw_group.get("name"), 200) or f"Группа {group_index + 1}",
                    "operator": _clean_operator(raw_group.get("operator")),
                    "conditions": conditions,
                }
            )

    state_rules: list[dict[str, Any]] = []
    for index, item in enumerate(data.get("state_rules") if isinstance(data.get("state_rules"), list) else []):
        if not isinstance(item, dict) or len(state_rules) >= 50:
            continue
        responsible = _clean(item.get("responsible"), 20)
        if responsible not in {"any", "assigned", "unassigned"}:
            responsible = "any"
        statuses = _clean_status_pairs(item.get("statuses"))
        tags = _clean_tags(item.get("tags"))
        if not statuses or not tags:
            continue
        state_rules.append(
            {
                "id": _clean(item.get("id"), 80) or f"r{index + 1}",
                "name": _clean(item.get("name"), 200) or f"Правило {index + 1}",
                "enabled": item.get("enabled") is not False,
                "responsible": responsible,
                "statuses": statuses,
                "tags": tags,
            }
        )

    timeout = max(5, min(60, _int(data.get("request_timeout") or DEFAULT_CONFIG["request_timeout"])))
    config = {
        "enabled": _bool(data.get("enabled")),
        "defaults_seeded": _bool(data.get("defaults_seeded")),
        "request_timeout": timeout,
        "source_scope": {"all": _bool(source.get("all", True)), "statuses": _clean_status_pairs(source.get("statuses"))},
        "search": {"operator": _clean_operator(search.get("operator")), "groups": groups},
        "base_tags": _clean_tags(data.get("base_tags")) or ["Дубль?"],
        "copy_responsible_from_latest_duplicate": _bool(
            data.get("copy_responsible_from_latest_duplicate")
        ),
        "state_rules": state_rules,
        "ai": {"openrouter_summary_enabled": _bool(ai.get("openrouter_summary_enabled"))},
    }
    if config["enabled"]:
        if not groups:
            raise ValueError("Добавьте хотя бы одну группу поиска")
        if not config["source_scope"]["all"] and not config["source_scope"]["statuses"]:
            raise ValueError("Для ограниченного охвата выберите хотя бы один статус")
    return config


async def _config() -> dict[str, Any]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key='config_json'")
        row = await cur.fetchone()
    try:
        return _clean_config(_loads(row[0] if row else "", DEFAULT_CONFIG))
    except ValueError:
        fallback = dict(DEFAULT_CONFIG)
        fallback["enabled"] = False
        return _clean_config(fallback)


async def _save_config(raw: dict[str, Any]) -> dict[str, Any]:
    config = _clean_config(raw)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "INSERT INTO settings(key,value) VALUES('config_json',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (json.dumps(config, ensure_ascii=False),),
        )
        supplied_secret = _clean(raw.get("webhook_secret"), 300)
        if supplied_secret and not _env()["secret"]:
            await db.execute(
                "INSERT INTO settings(key,value) VALUES('webhook_secret',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (supplied_secret,),
            )
        await db.commit()
    return config


async def _amo_request(
    method: str,
    path: str,
    config: dict[str, Any],
    payload: Any = None,
) -> tuple[Any, str, int]:
    env = _env()
    if not env["base_url"] or not env["token"]:
        return None, "AMO_BASE_URL или AMO_ACCESS_TOKEN не заданы", 0
    url = path if path.startswith("http://") or path.startswith("https://") else env["base_url"] + path
    timeout = max(5, min(60, _int(config.get("request_timeout") or 15)))
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method,
                url,
                headers={"Authorization": f"Bearer {env['token']}", "Content-Type": "application/json"},
                json=payload,
            )
        if response.status_code == 204:
            return {}, "", 204
        if response.status_code >= 400:
            retry = response.headers.get("Retry-After", "")
            suffix = f"; Retry-After={retry}" if retry else ""
            return None, f"amoCRM HTTP {response.status_code}: {response.text[:800]}{suffix}", response.status_code
        if not response.text.strip():
            return {}, "", response.status_code
        return response.json(), "", response.status_code
    except Exception as exc:
        return None, str(exc), 0


async def _paged_entities(entity: str, query: str, config: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    path = f"/api/v4/{entity}?query={quote(query)}&limit=250"
    if entity == "contacts":
        path += "&with=leads"
    items: list[dict[str, Any]] = []
    for _ in range(5):
        body, error, _ = await _amo_request("GET", path, config)
        if error:
            return [], error
        items.extend((((body or {}).get("_embedded") or {}).get(entity) or []))
        next_href = _clean((((body or {}).get("_links") or {}).get("next") or {}).get("href"), 3000)
        if not next_href:
            break
        parsed = urlparse(next_href)
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
    return [item for item in items if isinstance(item, dict)], ""


def _field_matches(field: dict[str, Any], condition: dict[str, Any]) -> bool:
    field_id = _clean(condition.get("field_id"), 64)
    field_code = _clean(condition.get("field_code"), 120).upper()
    field_name = _clean(condition.get("field_name"), 300).casefold()
    if field_id and _clean(field.get("field_id") or field.get("id"), 64) == field_id:
        return True
    if field_code and _clean(field.get("field_code") or field.get("code"), 120).upper() == field_code:
        return True
    return bool(field_name and _clean(field.get("field_name") or field.get("name"), 300).casefold() == field_name)


def _entity_values(entity: dict[str, Any], condition: dict[str, Any]) -> list[str]:
    values: list[str] = []
    field_name = _clean(condition.get("field_name"), 300).casefold()
    field_code = _clean(condition.get("field_code"), 120).upper()
    if not condition.get("field_id") and not field_code and field_name in {
        "id", "name", "название", "price", "responsible_user_id", "pipeline_id", "status_id"
    }:
        key = "name" if field_name in {"name", "название"} else field_name
        values.append(_clean(entity.get(key), 1000))
    for field in entity.get("custom_fields_values") or []:
        if not isinstance(field, dict) or not _field_matches(field, condition):
            continue
        for item in field.get("values") or []:
            value = item.get("value") if isinstance(item, dict) else item
            text = _clean(value, 1000)
            if text:
                values.append(text)
    return list(dict.fromkeys(value for value in values if value))


def _platform_ids_from_utm(lead: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field in lead.get("custom_fields_values") or []:
        if not isinstance(field, dict):
            continue
        code = _clean(field.get("field_code") or field.get("code"), 120).upper()
        name = _clean(field.get("field_name") or field.get("name"), 300).casefold()
        if code != "UTM_TERM" and name != "utm_term":
            continue
        for item in field.get("values") or []:
            value = item.get("value") if isinstance(item, dict) else item
            values.extend(PLATFORM_ID_RE.findall(_clean(value, 4000)))
    return list(dict.fromkeys(values))


def _openrouter_db() -> Path | None:
    if not _module_dir:
        return None
    return _module_dir.parent / "openrouter" / "data" / "openrouter.db"


async def _openrouter_summary(platform_ids: list[str]) -> tuple[str, str, str]:
    db_path = _openrouter_db()
    if not db_path or not db_path.is_file() or not platform_ids:
        return "", "", ""
    try:
        async with aiosqlite.connect(str(db_path)) as db:
            for platform_id in platform_ids:
                cur = await db.execute(
                    "SELECT summary FROM users WHERE platform_id=? AND trim(summary)<>''",
                    (platform_id,),
                )
                row = await cur.fetchone()
                summary = _clean(row[0] if row else "", 50000)
                if summary:
                    return platform_id, summary, ""
    except Exception as exc:
        return "", "", str(exc)
    return "", "", ""


def _ai_note_text(summary: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", _clean(summary, 50000)).strip()
    return f"{AI_NOTE_TITLE}\n\n{text}"


async def _ai_note_exists(lead_id: int, config: dict[str, Any]) -> tuple[bool, str]:
    path = f"/api/v4/leads/{lead_id}/notes?filter[note_type]=common&limit=250"
    for _ in range(20):
        body, error, _ = await _amo_request("GET", path, config)
        if error:
            return False, error
        for note in ((((body or {}).get("_embedded") or {}).get("notes")) or []):
            text = _clean(((note.get("params") or {}).get("text")), 60000)
            if text.startswith(AI_NOTE_TITLES):
                return True, ""
        next_href = _clean((((body or {}).get("_links") or {}).get("next") or {}).get("href"), 3000)
        if not next_href:
            return False, ""
        parsed = urlparse(next_href)
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
    return False, "Не удалось проверить все примечания сделки"


async def _apply_ai_summary(
    lead_id: int,
    lead: dict[str, Any],
    config: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    if not ((config.get("ai") or {}).get("openrouter_summary_enabled")):
        return {"state": "disabled"}
    platform_ids = _platform_ids_from_utm(lead)
    if not platform_ids:
        return {"state": "no_platform_id"}
    platform_id, summary, error = await _openrouter_summary(platform_ids)
    if error:
        return {"state": "error", "error": error}
    if not summary:
        return {"state": "no_summary"}
    async with _ai_lock:
        exists, error = await _ai_note_exists(lead_id, config)
        if error:
            return {"state": "error", "platform_id": platform_id, "error": error}
        if exists:
            return {"state": "already_added", "platform_id": platform_id}
        if dry_run:
            return {"state": "ready", "platform_id": platform_id}
        _body, error, _ = await _amo_request(
            "POST",
            f"/api/v4/leads/{lead_id}/notes",
            config,
            [{"note_type": "common", "params": {"text": _ai_note_text(summary)}}],
        )
        if error:
            return {"state": "error", "platform_id": platform_id, "error": error}
        return {"state": "created", "platform_id": platform_id}


def _is_phone(condition: dict[str, Any]) -> bool:
    code = _clean(condition.get("field_code"), 120).upper()
    name = _clean(condition.get("field_name"), 300).casefold()
    return code == "PHONE" or "телефон" in name or "phone" in name


def _is_email(condition: dict[str, Any]) -> bool:
    code = _clean(condition.get("field_code"), 120).upper()
    name = _clean(condition.get("field_name"), 300).casefold()
    return code == "EMAIL" or "email" in name or "почта" in name


def _normalized(value: Any, condition: dict[str, Any]) -> str:
    text = _clean(value, 1000)
    if _is_phone(condition):
        digits = re.sub(r"\D+", "", text)
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        elif len(digits) == 10:
            digits = "7" + digits
        return digits
    if _is_email(condition):
        return text.casefold()
    return " ".join(text.split()).casefold()


def _values_overlap(left: list[str], right: list[str], condition: dict[str, Any]) -> bool:
    expected = {_normalized(value, condition) for value in left}
    actual = {_normalized(value, condition) for value in right}
    expected.discard("")
    actual.discard("")
    return bool(expected & actual)


async def _contact_details(contact_stub: dict[str, Any], config: dict[str, Any]) -> tuple[dict[str, Any], str]:
    contact_id = _int(contact_stub.get("id"))
    if not contact_id:
        return contact_stub, ""
    body, error, _ = await _amo_request("GET", f"/api/v4/contacts/{contact_id}?with=leads", config)
    return (body if isinstance(body, dict) else contact_stub), error


async def _lead_bundle(lead_id: int, config: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    body, error, _ = await _amo_request("GET", f"/api/v4/leads/{lead_id}?with=contacts", config)
    if error or not isinstance(body, dict):
        return {}, [], error or "Сделка не найдена"
    contacts: list[dict[str, Any]] = []
    for stub in (((body.get("_embedded") or {}).get("contacts")) or []):
        if not isinstance(stub, dict):
            continue
        contact, contact_error = await _contact_details(stub, config)
        if contact_error:
            return body, contacts, contact_error
        contacts.append(contact)
    return body, contacts, ""


def _condition_values(
    lead: dict[str, Any], contacts: list[dict[str, Any]], condition: dict[str, Any]
) -> list[str]:
    if condition.get("entity") == "contacts":
        values: list[str] = []
        for contact in contacts:
            values.extend(_entity_values(contact, condition))
        return list(dict.fromkeys(values))
    return _entity_values(lead, condition)


def _expression_ready(source_values: dict[str, list[str]], search: dict[str, Any]) -> bool:
    group_ready: list[bool] = []
    for group in search.get("groups") or []:
        rows = [bool(source_values.get(_clean(item.get("id"), 80))) for item in group.get("conditions") or []]
        if not rows:
            continue
        group_ready.append(all(rows) if group.get("operator") == "AND" else any(rows))
    if not group_ready:
        return False
    return all(group_ready) if search.get("operator") == "AND" else any(group_ready)


def _expression_matches(
    source_values: dict[str, list[str]],
    lead: dict[str, Any],
    contacts: list[dict[str, Any]],
    search: dict[str, Any],
) -> tuple[bool, list[dict[str, Any]]]:
    group_results: list[bool] = []
    matched: list[dict[str, Any]] = []
    for group in search.get("groups") or []:
        results: list[bool] = []
        group_matched: list[dict[str, Any]] = []
        for condition in group.get("conditions") or []:
            condition_id = _clean(condition.get("id"), 80)
            candidate_values = _condition_values(lead, contacts, condition)
            hit = _values_overlap(source_values.get(condition_id, []), candidate_values, condition)
            results.append(hit)
            if hit:
                group_matched.append(
                    {
                        "group": _clean(group.get("name"), 200),
                        "entity": condition.get("entity"),
                        "field": condition.get("field_name") or condition.get("field_code") or condition.get("field_id"),
                    }
                )
        group_hit = bool(results) and (all(results) if group.get("operator") == "AND" else any(results))
        group_results.append(group_hit)
        if group_hit:
            matched.extend(group_matched)
    overall = bool(group_results) and (all(group_results) if search.get("operator") == "AND" else any(group_results))
    return overall, matched if overall else []


async def _lead_ids_for_contact(contact: dict[str, Any], config: dict[str, Any]) -> tuple[list[int], str]:
    ids = [_int(item.get("id")) for item in (((contact.get("_embedded") or {}).get("leads")) or [])]
    ids = [item for item in ids if item]
    if ids:
        return list(dict.fromkeys(ids)), ""
    contact_id = _int(contact.get("id"))
    if not contact_id:
        return [], ""
    body, error, _ = await _amo_request(
        "GET", f"/api/v4/contacts/{contact_id}/links?filter[to_entity_type]=leads", config
    )
    if error:
        return [], error
    ids = [
        _int(item.get("to_entity_id"))
        for item in ((((body or {}).get("_embedded") or {}).get("links")) or [])
        if _clean(item.get("to_entity_type"), 30) in {"", "leads"}
    ]
    return list(dict.fromkeys(item for item in ids if item)), ""


async def _candidate_ids(
    current_lead_id: int,
    source_values: dict[str, list[str]],
    search: dict[str, Any],
    config: dict[str, Any],
) -> tuple[list[int], str]:
    ids: set[int] = set()
    for group in search.get("groups") or []:
        for condition in group.get("conditions") or []:
            condition_id = _clean(condition.get("id"), 80)
            entity_name = "contacts" if condition.get("entity") == "contacts" else "leads"
            for value in source_values.get(condition_id, []):
                items, error = await _paged_entities(entity_name, value, config)
                if error:
                    return [], error
                for item in items:
                    if entity_name == "leads":
                        lead_id = _int(item.get("id"))
                        if lead_id:
                            ids.add(lead_id)
                    else:
                        linked_ids, link_error = await _lead_ids_for_contact(item, config)
                        if link_error:
                            return [], link_error
                        ids.update(linked_ids)
    ids.discard(current_lead_id)
    return sorted(ids), ""


def _scope_allows(lead: dict[str, Any], config: dict[str, Any]) -> bool:
    scope = config.get("source_scope") or {}
    if scope.get("all"):
        return True
    pair = (_clean(lead.get("pipeline_id"), 64), _clean(lead.get("status_id"), 64))
    return pair in {
        (_clean(item.get("pipeline_id"), 64), _clean(item.get("status_id"), 64))
        for item in scope.get("statuses") or []
    }


def _state_tags(duplicates: list[dict[str, Any]], config: dict[str, Any]) -> list[str]:
    tags = _clean_tags(config.get("base_tags"))
    seen = {tag.casefold() for tag in tags}
    for duplicate in duplicates:
        pipeline_id = _clean(duplicate.get("pipeline_id"), 64)
        status_id = _clean(duplicate.get("status_id"), 64)
        assigned = _int(duplicate.get("responsible_user_id")) > 0
        for rule in config.get("state_rules") or []:
            if not rule.get("enabled", True):
                continue
            pairs = {
                (_clean(item.get("pipeline_id"), 64), _clean(item.get("status_id"), 64))
                for item in rule.get("statuses") or []
            }
            if (pipeline_id, status_id) not in pairs and ("*", status_id) not in pairs:
                continue
            responsible = rule.get("responsible") or "any"
            if responsible == "assigned" and not assigned:
                continue
            if responsible == "unassigned" and assigned:
                continue
            for tag in _clean_tags(rule.get("tags")):
                if tag.casefold() not in seen:
                    seen.add(tag.casefold())
                    tags.append(tag)
    return tags


def _note_marker(lead_id: int) -> str:
    return f"[Nexus duplicate-check:{lead_id}]"


def _note_text(lead_id: int, duplicates: list[dict[str, Any]]) -> str:
    lines = [f"Найдено дублей: {len(duplicates)}"]
    lines.extend(_clean(item.get("url"), 1000) for item in duplicates if _clean(item.get("url"), 1000))
    return "\n".join(lines)[:50000]


def _latest_duplicate(duplicates: list[dict[str, Any]]) -> dict[str, Any] | None:
    rows = [item for item in duplicates if isinstance(item, dict) and _int(item.get("id"))]
    if not rows:
        return None
    return max(rows, key=lambda item: (_int(item.get("created_at")), _int(item.get("id"))))


async def _note_exists(lead_id: int, expected_text: str, config: dict[str, Any]) -> tuple[bool, str]:
    body, error, _ = await _amo_request(
        "GET", f"/api/v4/leads/{lead_id}/notes?filter[note_type]=common&limit=250", config
    )
    if error:
        return False, error
    for note in ((((body or {}).get("_embedded") or {}).get("notes")) or []):
        text = _clean(((note.get("params") or {}).get("text")), 60000)
        if _note_marker(lead_id) in text or text.strip() == expected_text.strip():
            return True, ""
    return False, ""


async def _apply_result(
    lead_id: int,
    lead: dict[str, Any],
    duplicates: list[dict[str, Any]],
    tags: list[str],
    config: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    result: dict[str, Any] = {"tags": tags, "note_created": False}
    patch: dict[str, Any] = {}
    if tags:
        patch["tags_to_add"] = [{"name": tag} for tag in tags]
    # A responsible chosen by a manager while accepting an Unsorted lead wins.
    # Copy from the latest duplicate only while the new lead is still unassigned.
    if config.get("copy_responsible_from_latest_duplicate") and not _int(lead.get("responsible_user_id")):
        latest = _latest_duplicate(duplicates)
        responsible_user_id = _int((latest or {}).get("responsible_user_id"))
        if responsible_user_id:
            patch["responsible_user_id"] = responsible_user_id
            result["responsible_user_id"] = responsible_user_id
            result["responsible_source_lead_id"] = _int((latest or {}).get("id"))

    # Preserve the exact source state explicitly. This is especially important for
    # amoCRM's "Unsorted" state: an update without the current status may be
    # interpreted by amoCRM as accepting the lead into the first pipeline stage.
    pipeline_id = _int(lead.get("pipeline_id"))
    status_id = _int(lead.get("status_id"))
    if patch and pipeline_id and status_id:
        patch["pipeline_id"] = pipeline_id
        patch["status_id"] = status_id

    if patch:
        body, error, _ = await _amo_request(
            "PATCH", f"/api/v4/leads/{lead_id}", config, patch
        )
        result["lead_patch"] = patch
        result["lead_response"] = body
        if error:
            return result, error
    note_text = _note_text(lead_id, duplicates)
    exists, error = await _note_exists(lead_id, note_text, config)
    if error:
        return result, error
    if not exists:
        body, error, _ = await _amo_request(
            "POST",
            f"/api/v4/leads/{lead_id}/notes",
            config,
            [{"note_type": "common", "params": {"text": note_text}}],
        )
        result["note_response"] = body
        result["note_created"] = not bool(error)
        if error:
            return result, error
    return result, ""


async def _catalog(config: dict[str, Any], refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        async with aiosqlite.connect(_must_db()) as db:
            cur = await db.execute("SELECT payload FROM catalog_cache WHERE id=1")
            row = await cur.fetchone()
        cached = _loads(row[0] if row else "", {})
        if isinstance(cached, dict) and cached.get("pipelines"):
            return cached

    endpoints = {
        "pipelines": "/api/v4/leads/pipelines",
        "lead_fields": "/api/v4/leads/custom_fields?limit=250",
        "contact_fields": "/api/v4/contacts/custom_fields?limit=250",
        "tags": "/api/v4/leads/tags?limit=250",
        "users": "/api/v4/users?limit=250",
    }
    raw: dict[str, Any] = {}
    errors: list[str] = []
    for key, path in endpoints.items():
        body, error, _ = await _amo_request("GET", path, config)
        raw[key] = body or {}
        if error:
            errors.append(f"{key}: {error}")

    pipelines: list[dict[str, Any]] = []
    for pipeline in (((raw["pipelines"].get("_embedded") or {}).get("pipelines")) or []):
        statuses = []
        for status in (((pipeline.get("_embedded") or {}).get("statuses")) or []):
            statuses.append(
                {
                    "id": _clean(status.get("id"), 64),
                    "name": _clean(status.get("name"), 300),
                    "color": _clean(status.get("color"), 30),
                    "sort": _int(status.get("sort")),
                    "type": _int(status.get("type")),
                }
            )
        pipelines.append(
            {
                "id": _clean(pipeline.get("id"), 64),
                "name": _clean(pipeline.get("name"), 300),
                "sort": _int(pipeline.get("sort")),
                "statuses": sorted(statuses, key=lambda row: (row["sort"], row["name"])),
            }
        )

    def fields(entity: str) -> list[dict[str, Any]]:
        return [
            {
                "id": _clean(item.get("id"), 64),
                "name": _clean(item.get("name"), 300),
                "code": _clean(item.get("code"), 120).upper(),
                "type": _clean(item.get("type"), 80),
            }
            for item in ((((raw[entity].get("_embedded") or {}).get("custom_fields")) or []))
        ]

    catalog = {
        "pipelines": sorted(pipelines, key=lambda row: (row["sort"], row["name"])),
        "lead_fields": fields("lead_fields"),
        "contact_fields": fields("contact_fields"),
        "tags": [
            {"id": _clean(item.get("id"), 64), "name": _clean(item.get("name"), 200), "color": _clean(item.get("color"), 30)}
            for item in ((((raw["tags"].get("_embedded") or {}).get("tags")) or []))
        ],
        "users": [
            {"id": _clean(item.get("id"), 64), "name": _clean(item.get("name"), 300), "active": bool((item.get("rights") or {}).get("is_active", True))}
            for item in ((((raw["users"].get("_embedded") or {}).get("users")) or []))
        ],
        "errors": errors,
        "updated_at": _now(),
    }
    if catalog["pipelines"]:
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute(
                "INSERT INTO catalog_cache(id,payload,updated_at) VALUES(1,?,?) "
                "ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
                (json.dumps(catalog, ensure_ascii=False), catalog["updated_at"]),
            )
            await db.commit()
    return catalog


def _catalog_lookup(catalog: dict[str, Any]) -> tuple[dict[tuple[str, str], tuple[str, str]], dict[str, str]]:
    statuses: dict[tuple[str, str], tuple[str, str]] = {}
    for pipeline in catalog.get("pipelines") or []:
        for status in pipeline.get("statuses") or []:
            statuses[(_clean(pipeline.get("id"), 64), _clean(status.get("id"), 64))] = (
                _clean(pipeline.get("name"), 300), _clean(status.get("name"), 300)
            )
    users = {_clean(user.get("id"), 64): _clean(user.get("name"), 300) for user in catalog.get("users") or []}
    return statuses, users


def _is_unsorted_lead(lead: dict[str, Any], catalog: dict[str, Any]) -> bool:
    pair = (_clean(lead.get("pipeline_id"), 64), _clean(lead.get("status_id"), 64))
    for pipeline in catalog.get("pipelines") or []:
        pipeline_id = _clean(pipeline.get("id"), 64)
        for status in pipeline.get("statuses") or []:
            if pair == (pipeline_id, _clean(status.get("id"), 64)):
                return _int(status.get("type")) == 1
    return False


async def _find_duplicates(lead_id: int, config: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    lead, contacts, error = await _lead_bundle(lead_id, config)
    if error:
        return "retry", {}, error
    if not _scope_allows(lead, config):
        return "ignored", {"reason": "source_scope", "lead": lead}, ""

    search = config.get("search") or {}
    source_values: dict[str, list[str]] = {}
    for group in search.get("groups") or []:
        for condition in group.get("conditions") or []:
            source_values[_clean(condition.get("id"), 80)] = _condition_values(lead, contacts, condition)
    if not _expression_ready(source_values, search):
        return "retry", {"reason": "source_data_missing", "source_values": source_values}, "Искомые данные ещё не появились"

    candidate_ids, error = await _candidate_ids(lead_id, source_values, search, config)
    if error:
        return "retry", {"source_values": source_values}, error

    catalog = await _catalog(config)
    status_lookup, users = _catalog_lookup(catalog)
    duplicates: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        candidate, candidate_contacts, candidate_error = await _lead_bundle(candidate_id, config)
        if candidate_error:
            return "retry", {"source_values": source_values, "candidate_id": candidate_id}, candidate_error
        matched, matches = _expression_matches(source_values, candidate, candidate_contacts, search)
        if not matched:
            continue
        pipeline_id = _clean(candidate.get("pipeline_id"), 64)
        status_id = _clean(candidate.get("status_id"), 64)
        if candidate.get("is_deleted"):
            continue
        pipeline_name, status_name = status_lookup.get((pipeline_id, status_id), ("", ""))
        responsible_id = _clean(candidate.get("responsible_user_id"), 64)
        duplicates.append(
            {
                "id": _clean(candidate.get("id"), 64),
                "name": _clean(candidate.get("name"), 500),
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "status_id": status_id,
                "status_name": status_name,
                "created_at": _int(candidate.get("created_at")),
                "responsible_user_id": responsible_id,
                "responsible_name": users.get(responsible_id, ""),
                "url": f"{_env()['base_url']}/leads/detail/{candidate_id}",
                "matched": matches,
            }
        )
    return "ready", {
        "lead": lead,
        "source_values": source_values,
        "duplicates": duplicates,
        "source_is_unsorted": _is_unsorted_lead(lead, catalog),
    }, ""


async def _event_update(lead_id: int, **values: Any) -> None:
    allowed = {
        "started_at", "finished_at", "state", "attempts", "source_pipeline_id", "source_status_id",
        "duplicate_count", "tags_json", "details_json", "error"
    }
    columns: list[str] = []
    params: list[Any] = []
    for key, value in values.items():
        if key not in allowed:
            continue
        columns.append(f"{key}=?")
        if key in {"tags_json", "details_json"} and not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, default=str)
        params.append(value)
    if not columns:
        return
    params.append(str(lead_id))
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(f"UPDATE events SET {', '.join(columns)} WHERE lead_id=?", params)
        await db.commit()


async def _process_lead(lead_id: int, received_at: str | None = None) -> None:
    config = await _config()
    started = _now()
    await _event_update(lead_id, state="processing", started_at=started, error="")
    if not config.get("enabled"):
        await _event_update(lead_id, state="ignored", finished_at=_now(), error="Модуль выключен")
        return

    try:
        received = datetime.fromisoformat((received_at or started).replace("Z", "+00:00"))
        if received.tzinfo is None:
            received = received.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        received = datetime.now(timezone.utc)
    deadline = received + timedelta(seconds=MAX_EVENT_AGE_SECONDS)
    delays = RETRY_DELAYS
    last_error = ""
    last_details: dict[str, Any] = {}
    ai_checked = False
    for attempt, delay in enumerate(delays, 1):
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= delay:
            break
        if delay:
            await asyncio.sleep(delay)
        await _event_update(lead_id, state="processing", attempts=attempt)
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            break
        try:
            outcome, details, error = await asyncio.wait_for(
                _find_duplicates(lead_id, config), timeout=remaining
            )
        except TimeoutError:
            last_error = "Истекло двухминутное окно обработки"
            break
        last_error, last_details = error, details
        lead = details.get("lead") if isinstance(details, dict) else {}
        if isinstance(lead, dict):
            await _event_update(
                lead_id,
                source_pipeline_id=_clean(lead.get("pipeline_id"), 64),
                source_status_id=_clean(lead.get("status_id"), 64),
            )
            if lead and not ai_checked:
                ai_result = await _apply_ai_summary(lead_id, lead, config)
                details["ai"] = ai_result
                ai_checked = ai_result.get("state") not in {"no_platform_id", "error"}
        if outcome == "ignored":
            await _event_update(lead_id, state="ignored", finished_at=_now(), details_json=details, error="")
            return
        if outcome != "ready":
            await _event_update(lead_id, state="retrying", details_json=details, error=error)
            continue
        duplicates = details.get("duplicates") or []
        if not duplicates:
            await _event_update(
                lead_id, state="completed", finished_at=_now(), duplicate_count=0,
                tags_json=[], details_json=details, error=""
            )
            return
        tags = _state_tags(duplicates, config)
        if details.get("source_is_unsorted"):
            await _event_update(
                lead_id,
                state="waiting_unsorted",
                finished_at="",
                duplicate_count=len(duplicates),
                tags_json=tags,
                details_json=details,
                error="",
            )
            return
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            last_error, last_details = "Истекло двухминутное окно обработки", details
            break
        try:
            apply_result, apply_error = await asyncio.wait_for(
                _apply_result(lead_id, lead, duplicates, tags, config), timeout=remaining
            )
        except TimeoutError:
            last_error, last_details = "Истекло двухминутное окно обработки", details
            break
        details["apply"] = apply_result
        if apply_error:
            last_error, last_details = apply_error, details
            await _event_update(lead_id, state="retrying", details_json=details, error=apply_error)
            continue
        await _event_update(
            lead_id, state="completed", finished_at=_now(), duplicate_count=len(duplicates),
            tags_json=tags, details_json=details, error=""
        )
        return

    final_state = "no_data" if (last_details or {}).get("reason") == "source_data_missing" else "failed"
    await _event_update(
        lead_id, state=final_state, finished_at=_now(), details_json=last_details,
        error=last_error or "Обработка не завершена за две минуты"
    )


def _task_done(lead_id: str, task: asyncio.Task) -> None:
    _tasks.pop(lead_id, None)
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        _log("exception", "duplicate processing failed for lead %s", lead_id)


def _schedule(lead_id: int, received_at: str | None = None) -> bool:
    key = str(lead_id)
    current = _tasks.get(key)
    if current and not current.done():
        return False
    task = asyncio.create_task(_process_lead(lead_id, received_at))
    _tasks[key] = task
    task.add_done_callback(lambda done, item=key: _task_done(item, done))
    return True


async def _recover_events() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=MAX_EVENT_AGE_SECONDS)).isoformat().replace("+00:00", "Z")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE events SET state='failed',finished_at=?,error='Окно обработки истекло во время остановки модуля' "
            "WHERE state IN ('queued','processing','retrying') AND received_at<?",
            (_now(), cutoff),
        )
        cur = await db.execute(
            "SELECT lead_id,received_at FROM events WHERE state IN ('queued','processing','retrying') AND received_at>=?",
            (cutoff,),
        )
        rows = await cur.fetchall()
        await db.commit()
    for lead_id, received_at in rows:
        _schedule(_int(lead_id), _clean(received_at, 80))


def _lead_added_event_rows(body: Any) -> list[dict[str, str]]:
    unique: dict[str, dict[str, str]] = {}
    events = ((((body or {}).get("_embedded") or {}).get("events")) or []) if isinstance(body, dict) else []
    for event in events:
        if not isinstance(event, dict) or _clean(event.get("type"), 80) != "lead_added":
            continue
        lead_id = _clean(event.get("entity_id"), 64)
        if not _int(lead_id):
            continue
        unique[lead_id] = {
            "id": lead_id,
            "pipeline_id": "",
            "status_id": "",
            "event_id": _clean(event.get("id"), 100),
            "created_at": _clean(event.get("created_at"), 30),
        }
    return list(unique.values())


async def _recent_created_leads(config: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    since = int(datetime.now(timezone.utc).timestamp()) - FALLBACK_POLL_LOOKBACK_SECONDS
    path = "/api/v4/events?" + urlencode(
        [
            ("filter[created_at][from]", str(since)),
            ("filter[entity][]", "lead"),
            ("filter[type][]", "lead_added"),
            ("limit", "100"),
        ]
    )
    rows: dict[str, dict[str, str]] = {}
    for _ in range(5):
        body, error, _status = await _amo_request("GET", path, config)
        if error:
            return [], error
        for item in _lead_added_event_rows(body):
            rows[item["id"]] = item
        next_href = _clean((((body or {}).get("_links") or {}).get("next") or {}).get("href"), 3000)
        if not next_href:
            break
        parsed = urlparse(next_href)
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
    return sorted(rows.values(), key=lambda item: (_int(item.get("created_at")), _int(item.get("id")))), ""


async def _resume_accepted_leads(config: dict[str, Any]) -> tuple[int, int, str]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "SELECT lead_id FROM events WHERE state='waiting_unsorted' ORDER BY id LIMIT 250"
        )
        lead_ids = [_int(row[0]) for row in await cur.fetchall() if _int(row[0])]
    if not lead_ids:
        return 0, 0, ""

    catalog = await _catalog(config)
    resumed = 0
    errors: list[str] = []
    for lead_id in lead_ids:
        lead, error, _status = await _amo_request("GET", f"/api/v4/leads/{lead_id}", config)
        if error or not isinstance(lead, dict):
            errors.append(f"{lead_id}: {error or 'Сделка не найдена'}")
            continue
        if _is_unsorted_lead(lead, catalog):
            continue
        now = _now()
        await _event_update(
            lead_id,
            state="queued",
            attempts=0,
            started_at="",
            finished_at="",
            source_pipeline_id=_clean(lead.get("pipeline_id"), 64),
            source_status_id=_clean(lead.get("status_id"), 64),
            error="",
        )
        resumed += int(_schedule(lead_id, now))
    return resumed, len(lead_ids), "; ".join(errors[:5])


async def _fallback_poll_once() -> int:
    global _poll_last_at, _poll_last_error, _poll_last_seen, _poll_waiting_unsorted, _poll_last_resumed
    config = await _config()
    _poll_last_at = _now()
    if not config.get("enabled"):
        _poll_last_error = ""
        _poll_last_seen = 0
        _poll_waiting_unsorted = 0
        _poll_last_resumed = 0
        return 0
    resumed, waiting, resume_error = await _resume_accepted_leads(config)
    _poll_waiting_unsorted = waiting
    _poll_last_resumed = resumed
    rows, error = await _recent_created_leads(config)
    _poll_last_seen = len(rows)
    _poll_last_error = _clean("; ".join(item for item in (resume_error, error) if item), 1000)
    if error:
        _log("warning", "amocrm-duplicates fallback poll failed: %s", error)
        return 0
    created = 0
    for item in rows:
        raw = json.dumps(
            {
                "source": "amo_events_fallback",
                "event_id": item.get("event_id"),
                "created_at": item.get("created_at"),
                "lead_id": item.get("id"),
            },
            ensure_ascii=False,
        )
        registered, _state = await _register_event(item, raw)
        created += int(registered)
    if created:
        _log("info", "amocrm-duplicates fallback recovered %s lead event(s)", created)
    return created + resumed


async def _fallback_poll_loop() -> None:
    while True:
        try:
            await _fallback_poll_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            _log("exception", "amocrm-duplicates fallback poll crashed")
        await asyncio.sleep(FALLBACK_POLL_INTERVAL_SECONDS)


async def _read_payload(request: Request) -> tuple[dict[str, Any], str]:
    body = await request.body()
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            data = json.loads(body.decode("utf-8") or "{}")
            payload = data if isinstance(data, dict) else {"raw": data}
            return payload, json.dumps(payload, ensure_ascii=False)
        except Exception:
            pass
    try:
        form = await request.form()
        if form:
            payload = {str(key): str(value) for key, value in form.items()}
            return payload, json.dumps(payload, ensure_ascii=False)
    except Exception:
        pass
    text = body.decode("utf-8", "replace")
    return {"raw": text}, json.dumps({"raw": text}, ensure_ascii=False)


def _lead_add_events(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    leads = payload.get("leads")
    if isinstance(leads, dict):
        items = leads.get("add")
        if isinstance(items, dict):
            items = list(items.values())
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict) and _int(item.get("id")):
                rows.append(
                    {
                        "id": _clean(item.get("id"), 64),
                        "pipeline_id": _clean(item.get("pipeline_id"), 64),
                        "status_id": _clean(item.get("status_id"), 64),
                    }
                )
    flat: dict[str, dict[str, str]] = {}
    for key, value in payload.items():
        match = LEAD_KEY_RE.match(str(key))
        if match:
            flat.setdefault(match.group("idx"), {})[match.group("field")] = _clean(value, 500)
    for item in flat.values():
        if _int(item.get("id")):
            rows.append(
                {
                    "id": _clean(item.get("id"), 64),
                    "pipeline_id": _clean(item.get("pipeline_id"), 64),
                    "status_id": _clean(item.get("status_id"), 64),
                }
            )
    unique: dict[str, dict[str, str]] = {}
    for row in rows:
        unique[row["id"]] = row
    return list(unique.values())


async def _register_event(item: dict[str, str], raw_payload: str) -> tuple[bool, str]:
    lead_id = _clean(item.get("id"), 64)
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute(
            "INSERT OR IGNORE INTO events(lead_id,received_at,state,source_pipeline_id,source_status_id,raw_payload) "
            "VALUES(?,?,?,?,?,?)",
            (
                lead_id,
                now,
                "queued",
                _clean(item.get("pipeline_id"), 64),
                _clean(item.get("status_id"), 64),
                _clean(raw_payload, 200000),
            ),
        )
        await db.commit()
        created = bool(cur.rowcount)
        if not created:
            state_cur = await db.execute("SELECT state FROM events WHERE lead_id=?", (lead_id,))
            existing = await state_cur.fetchone()
            return False, _clean(existing[0] if existing else "unknown", 30)
    _schedule(_int(lead_id), now)
    return True, "queued"


async def _today_leads(config: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    zone = ZoneInfo("Europe/Moscow")
    now = datetime.now(zone)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    path = "/api/v4/leads?" + urlencode(
        [
            ("filter[created_at][from]", str(int(start.timestamp()))),
            ("filter[created_at][to]", str(int(now.timestamp()))),
            ("limit", "250"),
        ]
    )
    leads: list[dict[str, Any]] = []
    for _ in range(20):
        body, error, _ = await _amo_request("GET", path, config)
        if error:
            return [], error
        leads.extend(
            item
            for item in ((((body or {}).get("_embedded") or {}).get("leads")) or [])
            if isinstance(item, dict)
        )
        next_href = _clean((((body or {}).get("_links") or {}).get("next") or {}).get("href"), 3000)
        if not next_href:
            break
        parsed = urlparse(next_href)
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
    return leads, ""


async def _backfill_ai_today(config: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    leads, error = await _today_leads(config)
    if error:
        raise HTTPException(502, error)
    counts: dict[str, int] = {"total": len(leads)}
    errors: list[dict[str, Any]] = []
    for lead in leads:
        lead_id = _int(lead.get("id"))
        result = await _apply_ai_summary(lead_id, lead, config, dry_run=dry_run)
        state = _clean(result.get("state"), 40) or "error"
        counts[state] = counts.get(state, 0) + 1
        if state == "error":
            errors.append({"lead_id": lead_id, "error": _clean(result.get("error"), 1000)})
        await asyncio.sleep(0.15)
    return {"ok": not errors, "dry_run": dry_run, "counts": counts, "errors": errors[:20]}


@router.get("/health")
async def health() -> dict[str, Any]:
    env = _env()
    config = await _config()
    return {
        "ok": True,
        "module": MODULE_ID,
        "enabled": bool(config.get("enabled")),
        "amo_base_url": bool(env["base_url"]),
        "amo_token": bool(env["token"]),
        "ready": bool(env["base_url"] and env["token"]),
        "active_tasks": len(_tasks),
        "fallback_polling": bool(_poll_task and not _poll_task.done()),
        "fallback_poll_interval_seconds": FALLBACK_POLL_INTERVAL_SECONDS,
        "fallback_poll_last_at": _poll_last_at,
        "fallback_poll_last_error": _poll_last_error,
        "fallback_poll_last_seen": _poll_last_seen,
        "waiting_unsorted": _poll_waiting_unsorted,
        "fallback_poll_last_resumed": _poll_last_resumed,
    }


@router.get("/settings")
async def get_settings(request: Request) -> dict[str, Any]:
    await _require_user(request)
    config = await _config()
    return {"config": config, "webhook_secret": await _secret(), "env_secret": bool(_env()["secret"])}


@router.put("/settings")
async def put_settings(data: dict[str, Any], request: Request) -> dict[str, Any]:
    await _require_user(request)
    try:
        config = await _save_config(data)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "config": config, "webhook_secret": await _secret(), "env_secret": bool(_env()["secret"])}


@router.get("/amo/catalog")
async def get_catalog(request: Request, refresh: int = 0) -> dict[str, Any]:
    await _require_user(request)
    return await _catalog(await _config(), bool(refresh))


@router.get("/events")
async def get_events(request: Request, limit: int = 150) -> dict[str, Any]:
    await _require_user(request)
    limit = max(1, min(500, limit))
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id,lead_id,received_at,started_at,finished_at,state,attempts,source_pipeline_id,"
            "source_status_id,duplicate_count,tags_json,error FROM events ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = [dict(row) async for row in cur]
    for row in rows:
        row["tags"] = _loads(row.pop("tags_json", "[]"), [])
    return {"items": rows}


@router.post("/ai/backfill-today")
async def backfill_ai_today(request: Request, dry_run: int = 0) -> dict[str, Any]:
    await _require_user(request)
    config = await _config()
    if not ((config.get("ai") or {}).get("openrouter_summary_enabled")):
        raise HTTPException(400, "Включите добавление сводки")
    return await _backfill_ai_today(config, bool(dry_run))


@router.get("/events/{event_id}")
async def get_event(event_id: int, request: Request) -> dict[str, Any]:
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM events WHERE id=?", (event_id,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "event not found")
    data = dict(row)
    data["tags"] = _loads(data.pop("tags_json", "[]"), [])
    data["details"] = _loads(data.pop("details_json", "{}"), {})
    data["raw_payload"] = _loads(data.get("raw_payload"), {})
    return data


@router.post("/webhook")
async def webhook(request: Request) -> dict[str, Any]:
    if not await _secret_ok(request):
        return {"ok": False, "error": "invalid secret"}
    payload, raw_payload = await _read_payload(request)
    events = _lead_add_events(payload)
    if not events:
        return {"ok": True, "ignored": True, "reason": "add_lead not found"}
    accepted = []
    for item in events:
        created, state = await _register_event(item, raw_payload)
        accepted.append({"lead_id": item["id"], "created": created, "state": state})
    return {"ok": True, "items": accepted}
