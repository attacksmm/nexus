from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

import httpx


_identity_db = os.getenv("NEXUS_GETCOURSE_WAZZUP_IDENTITY_DB", "").strip()
SYNC_CONTROL_DB = Path(_identity_db) if _identity_db else Path("/__nexus_no_identity_db__")
CACHE_SECONDS = 300
_cache: dict[str, tuple[float, dict[str, str]]] = {}
_sync_cache: dict[str, tuple[float, dict[str, str]]] = {}
_vk_cache: dict[str, tuple[float, dict[str, str]]] = {}


def _clean(value: Any, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def _phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return "+" + digits if 8 <= len(digits) <= 15 else ""


def _email(value: Any) -> str:
    text = _clean(value, 320).casefold()
    return text if "@" in text else ""


def _username(value: Any) -> str:
    return _clean(value, 200).lstrip("@").casefold()


def _json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _field_values(field: Any) -> list[str]:
    if not isinstance(field, dict):
        return []
    return [
        _clean(item.get("value"), 1000)
        for item in field.get("values") or []
        if isinstance(item, dict) and _clean(item.get("value"), 1000)
    ]


def _amo_identity_from_contacts(
    contacts: Any,
    *,
    phone: str,
    email: str,
    username_field_id: str,
    telegram_id_field_id: str,
) -> dict[str, str]:
    if not isinstance(contacts, list):
        return {}
    for contact in contacts:
        if not isinstance(contact, dict):
            continue
        fields = contact.get("custom_fields_values") or []
        phones = {
            _phone(value)
            for field in fields
            if isinstance(field, dict) and _clean(field.get("field_code"), 50).upper() == "PHONE"
            for value in _field_values(field)
        }
        emails = {
            _email(value)
            for field in fields
            if isinstance(field, dict) and _clean(field.get("field_code"), 50).upper() == "EMAIL"
            for value in _field_values(field)
        }
        if not ((phone and phone in phones) or (email and email in emails)):
            continue
        by_id = {
            _clean(field.get("field_id"), 80): _field_values(field)
            for field in fields
            if isinstance(field, dict)
        }
        return {
            "amo_contact_id": _clean(contact.get("id"), 100),
            "telegram_username": _username(next(iter(by_id.get(username_field_id, [])), "")),
            "telegram_id": _clean(next(iter(by_id.get(telegram_id_field_id, [])), ""), 200),
        }
    return {}


def _metadata_field_ids(conn: sqlite3.Connection) -> tuple[str, str]:
    username_id = telegram_id = ""
    rows = conn.execute(
        "SELECT item_key,label,payload_json FROM connector_metadata WHERE system='amocrm' AND category='field'"
    ).fetchall()
    for row in rows:
        label = _clean(row["label"], 500).casefold().replace(" ", "")
        payload = _json(row["payload_json"])
        field_id = _clean(payload.get("field_id"), 80) or _clean(row["item_key"], 100).split(":")[-1]
        if "telegramusername_wz" in label:
            username_id = field_id
        elif "telegramid_wz" in label:
            telegram_id = field_id
    return username_id, telegram_id


def _event_aliases(conn: sqlite3.Connection, *, phone: str, email: str, gc_user_id: str) -> dict[str, str]:
    result: dict[str, str] = {}
    rows = conn.execute(
        "SELECT scenario_id,identity_key,flat_json FROM webhook_events ORDER BY id DESC LIMIT 5000"
    ).fetchall()
    matched_links: set[tuple[int, str]] = set()
    for row in rows:
        flat = _json(row["flat_json"])
        row_phone = _phone(flat.get("phone") or flat.get("user_phone"))
        row_email = _email(flat.get("email") or flat.get("user_email"))
        row_gc_id = _clean(flat.get("gcid") or flat.get("getcourse_user_id") or flat.get("id"), 200)
        if not (
            (phone and row_phone == phone)
            or (email and row_email == email)
            or (gc_user_id and row_gc_id == gc_user_id)
        ):
            continue
        matched_links.add((int(row["scenario_id"]), _clean(row["identity_key"], 1000)))
        for key, source_keys, normalizer in (
            ("telegram_username", ("telegram_username", "tg_username"), _username),
            ("telegram_id", ("tg_id", "telegram_id"), lambda value: _clean(value, 200)),
            ("vk_id", ("vk_id", "senler_id"), lambda value: _clean(value, 200)),
            ("senler_id", ("senler_id",), lambda value: _clean(value, 200)),
            ("getcourse_user_id", ("gcid", "getcourse_user_id", "id"), lambda value: _clean(value, 200)),
        ):
            if result.get(key):
                continue
            for source_key in source_keys:
                value = normalizer(flat.get(source_key))
                if value:
                    result[key] = value
                    break
    for scenario_id, identity_key in matched_links:
        link = conn.execute(
            "SELECT amocrm_contact_id,getcourse_user_id FROM person_links WHERE scenario_id=? AND identity_key=?",
            (scenario_id, identity_key),
        ).fetchone()
        if link:
            result.setdefault("amo_contact_id", _clean(link["amocrm_contact_id"], 100))
            result.setdefault("getcourse_user_id", _clean(link["getcourse_user_id"], 100))
    return {key: value for key, value in result.items() if value}


async def _amo_identity(
    conn: sqlite3.Connection,
    *,
    phone: str,
    email: str,
) -> dict[str, str]:
    row = conn.execute(
        "SELECT config_json FROM connectors WHERE system='amocrm' AND enabled=1 ORDER BY id LIMIT 1"
    ).fetchone()
    if not row or not (phone or email):
        return {}
    config = _json(row["config_json"])
    token = _clean(config.get("long_lived_token") or config.get("access_token"), 8000)
    base_url = _clean(config.get("base_url"), 1000).rstrip("/")
    subdomain = _clean(config.get("subdomain"), 300)
    if not base_url and subdomain:
        base_url = f"https://{subdomain}.amocrm.ru"
    username_field_id, telegram_id_field_id = _metadata_field_ids(conn)
    if not token or not base_url or not username_field_id or not telegram_id_field_id:
        return {}
    try:
        async with httpx.AsyncClient(timeout=12, follow_redirects=False) as client:
            response = await client.get(
                f"{base_url}/api/v4/contacts",
                params={"query": phone[1:] if phone else email, "limit": 10},
                headers={"Authorization": f"Bearer {token}"},
            )
        if response.status_code != 200:
            return {}
        contacts = (response.json().get("_embedded") or {}).get("contacts") or []
        return _amo_identity_from_contacts(
            contacts,
            phone=phone,
            email=email,
            username_field_id=username_field_id,
            telegram_id_field_id=telegram_id_field_id,
        )
    except (httpx.HTTPError, ValueError, TypeError):
        return {}


async def resolve_client_identity(
    *,
    phone: Any = "",
    email: Any = "",
    getcourse_user_id: Any = "",
) -> dict[str, str]:
    normalized_phone = _phone(phone)
    normalized_email = _email(email)
    gc_user_id = _clean(getcourse_user_id, 200)
    cache_key = sha256(f"{normalized_phone}|{normalized_email}|{gc_user_id}".encode()).hexdigest()
    cached = _cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return dict(cached[1])
    result: dict[str, str] = {
        key: value
        for key, value in {
            "phone": normalized_phone,
            "email": normalized_email,
            "getcourse_user_id": gc_user_id,
        }.items()
        if value
    }
    if SYNC_CONTROL_DB.is_file():
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(f"file:{SYNC_CONTROL_DB}?mode=ro", uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            result.update({key: value for key, value in resolve_sync_identity(phone=normalized_phone, email=normalized_email, getcourse_user_id=gc_user_id).items() if value})
            amo_identity = await _amo_identity(conn, phone=normalized_phone, email=normalized_email)
            result.update({key: value for key, value in amo_identity.items() if value})
            amo_contact_id = _clean(amo_identity.get("amo_contact_id"), 100)
            if amo_contact_id and not result.get("getcourse_user_id"):
                link = conn.execute(
                    """SELECT getcourse_user_id FROM person_links
                       WHERE amocrm_contact_id=? AND COALESCE(getcourse_user_id,'')<>''
                       ORDER BY updated_at DESC,id DESC LIMIT 1""",
                    (amo_contact_id,),
                ).fetchone()
                if link:
                    result["getcourse_user_id"] = _clean(link["getcourse_user_id"], 200)
        except (sqlite3.Error, OSError, ValueError):
            pass
        finally:
            if conn is not None:
                conn.close()
    _cache[cache_key] = (time.monotonic() + CACHE_SECONDS, dict(result))
    return result


def resolve_sync_identity(
    *,
    phone: Any = "",
    email: Any = "",
    getcourse_user_id: Any = "",
) -> dict[str, str]:
    normalized_phone = _phone(phone)
    normalized_email = _email(email)
    gc_user_id = _clean(getcourse_user_id, 200)
    cache_key = sha256(f"{normalized_phone}|{normalized_email}|{gc_user_id}".encode()).hexdigest()
    cached = _sync_cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return dict(cached[1])
    result = {
        key: value
        for key, value in {
            "phone": normalized_phone,
            "email": normalized_email,
            "getcourse_user_id": gc_user_id,
        }.items()
        if value
    }
    if SYNC_CONTROL_DB.is_file():
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(f"file:{SYNC_CONTROL_DB}?mode=ro", uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            result.update(
                {
                    key: value
                    for key, value in _event_aliases(
                        conn,
                        phone=normalized_phone,
                        email=normalized_email,
                        gc_user_id=gc_user_id,
                    ).items()
                    if value
                }
            )
        except (sqlite3.Error, OSError, ValueError):
            pass
        finally:
            if conn is not None:
                conn.close()
    _sync_cache[cache_key] = (time.monotonic() + CACHE_SECONDS, dict(result))
    return result


def _event_vk_id(flat: dict[str, Any]) -> str:
    return _clean(flat.get("vk_id") or flat.get("senler_id"), 200)


def _event_gc_id(conn: sqlite3.Connection, scenario_id: int, identity_key: str) -> str:
    row = conn.execute(
        """SELECT getcourse_user_id FROM person_links
           WHERE scenario_id=? AND identity_key=? AND COALESCE(getcourse_user_id,'')<>''
           ORDER BY updated_at DESC,id DESC LIMIT 1""",
        (scenario_id, identity_key),
    ).fetchone()
    return _clean(row["getcourse_user_id"], 200) if row else ""


def resolve_vk_identity(vk_id: Any) -> dict[str, str]:
    peer_id = _clean(vk_id, 200)
    if not peer_id:
        return {}
    cached = _vk_cache.get(peer_id)
    if cached and cached[0] > time.monotonic():
        return dict(cached[1])
    result: dict[str, str] = {"vk_id": peer_id}
    if SYNC_CONTROL_DB.is_file():
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(f"file:{SYNC_CONTROL_DB}?mode=ro", uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT scenario_id,identity_key,flat_json FROM webhook_events ORDER BY id DESC LIMIT 10000"
            ).fetchall()
            for row in rows:
                flat = _json(row["flat_json"])
                if _event_vk_id(flat) != peer_id:
                    continue
                result.setdefault("phone", _phone(flat.get("phone") or flat.get("user_phone")))
                result.setdefault("email", _email(flat.get("email") or flat.get("user_email")))
                first = _clean(flat.get("first_name") or flat.get("firstName"), 150)
                last = _clean(flat.get("second_name") or flat.get("lastName"), 150)
                result.setdefault("name", " ".join(part for part in (first, last) if part).strip())
                gc_id = _event_gc_id(conn, int(row["scenario_id"]), _clean(row["identity_key"], 1000))
                if gc_id:
                    result["getcourse_user_id"] = gc_id
                    break
        except (sqlite3.Error, OSError, ValueError):
            pass
        finally:
            if conn is not None:
                conn.close()
    result = {key: value for key, value in result.items() if value}
    _vk_cache[peer_id] = (time.monotonic() + CACHE_SECONDS, dict(result))
    return result


def list_vk_identities() -> dict[str, dict[str, str]]:
    if not SYNC_CONTROL_DB.is_file():
        return {}
    result: dict[str, dict[str, str]] = {}
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{SYNC_CONTROL_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT scenario_id,identity_key,flat_json FROM webhook_events ORDER BY id DESC LIMIT 10000"
        ).fetchall()
        for row in rows:
            flat = _json(row["flat_json"])
            peer_id = _event_vk_id(flat)
            if not peer_id or peer_id in result:
                continue
            gc_id = _event_gc_id(conn, int(row["scenario_id"]), _clean(row["identity_key"], 1000))
            if not gc_id:
                continue
            first = _clean(flat.get("first_name") or flat.get("firstName"), 150)
            last = _clean(flat.get("second_name") or flat.get("lastName"), 150)
            result[peer_id] = {
                "vk_id": peer_id,
                "getcourse_user_id": gc_id,
                "phone": _phone(flat.get("phone") or flat.get("user_phone")),
                "email": _email(flat.get("email") or flat.get("user_email")),
                "name": " ".join(part for part in (first, last) if part).strip(),
            }
    except (sqlite3.Error, OSError, ValueError):
        return {}
    finally:
        if conn is not None:
            conn.close()
    return result
