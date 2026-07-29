from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any
from urllib.parse import urlparse

TABLE_NAME = "bizon365_clients"
TABLE_DISPLAY_NAME = "Клиенты Bizon365"
VAKAS_ALLOWED_PREFIX = "https://vakas-tools.ru/base/report/"

SECRET_KEYS = {"secret", "token", "api_token", "webhook_secret"}
VIEWER_LIST_KEYS = ("viewers", "items", "list", "users", "clients")
REPORT_ID_KEYS = ("webinarId", "webinar_id", "webinarID", "reportId", "report_id")
REPORT_META_KEYS = ("webinarId", "roomid", "created", "room_title", "name", "type", "group", "playFromRoom")
VIEWER_KEEP_KEYS = (
    "webinarId",
    "roomid",
    "created",
    "username",
    "name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "chatUserId",
    "uid",
    "sid",
    "finished",
    "view",
    "viewTill",
    "vi",
    "page",
    "partner",
    "referer",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "p1",
    "p2",
    "p3",
    "sup",
    "cu1",
    "c1",
    "cv",
    "buttons",
    "banners",
    "vizitForm",
    "newOrder",
    "orderDetails",
    "messages",
    "messagesTS",
    "ip",
    "city",
    "country",
    "country_code",
    "mob",
    "ban",
    "ignore",
    "ticket",
    "url",
)

MAX_WATCH_SECONDS = 24 * 60 * 60
WEBINAR_AT_RE = re.compile(r"\*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?)")


def clean_text(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", clean_text(value, 100))
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return digits


def normalize_email(value: Any) -> str:
    return clean_text(value, 500).casefold()


def webinar_at_from_values(*values: Any) -> str:
    for value in values:
        match = WEBINAR_AT_RE.search(clean_text(value, 2000))
        if match:
            return match.group(1)
    return ""


def _has_click(value: Any) -> bool:
    parsed = parse_jsonish(value)
    if isinstance(parsed, (list, dict)):
        return bool(parsed)
    return clean_text(parsed, 100).casefold() not in {"", "0", "false", "none", "null", "нет"}


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _merged_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    valid = sorted((start, end) for start, end in intervals if start >= 0 and end >= start)
    merged: list[list[float]] = []
    for start, end in valid:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def visit_intervals(viewer: dict[str, Any]) -> list[tuple[float, float]]:
    raw = parse_jsonish(viewer.get("vi"))
    intervals: list[tuple[float, float]] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            start = _finite_number(item.get("s"))
            end = _finite_number(item.get("e"))
            if start is not None and end is not None and 0 <= start <= end:
                intervals.append((start, end))
    return _merged_intervals(intervals)


def absolute_visit_interval(viewer: dict[str, Any]) -> tuple[float, float] | None:
    start = _finite_number(viewer.get("view"))
    end = _finite_number(viewer.get("viewTill"))
    if start is None or end is None or start < 0 or end < start:
        return None
    # Bizon documents view/viewTill as milliseconds. Tolerate seconds in old payloads.
    divisor = 1000.0 if max(start, end) > 10_000_000_000 else 1.0
    return start / divisor, end / divisor


def viewer_identity_tokens(viewer: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    phone = normalize_phone(viewer.get("phone"))
    email = normalize_email(viewer.get("email"))
    if phone:
        tokens.add(f"phone:{phone}")
    if email:
        tokens.add(f"email:{email}")
    for key in ("uid", "sid", "chatUserId", "bizon_user_id"):
        value = clean_text(viewer.get(key), 500)
        if value:
            tokens.add(f"{key}:{value}")
    return tokens


def is_allowed_forward_url(url: str) -> bool:
    text = clean_text(url, 3000)
    if not text.startswith(VAKAS_ALLOWED_PREFIX):
        return False
    parsed = urlparse(text)
    return parsed.scheme == "https" and parsed.netloc == "vakas-tools.ru" and parsed.path.startswith("/base/report/")


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in payload.items() if str(key) not in SECRET_KEYS}


def parse_jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = clean_text(value, 5_000_000)
    if not text:
        return value
    for candidate in (text, text.replace('\\"', '"')):
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, (dict, list)):
            return parsed
    return value


def extract_webinar_id(payload: dict[str, Any]) -> str:
    for key in REPORT_ID_KEYS:
        value = clean_text(payload.get(key), 1000)
        if value:
            return value
    report = payload.get("report")
    if isinstance(report, dict):
        for key in REPORT_ID_KEYS:
            value = clean_text(report.get(key), 1000)
            if value:
                return value
    return ""


def report_meta_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    for key in REPORT_META_KEYS:
        value = payload.get(key)
        if value not in (None, ""):
            meta[key] = value
    report = payload.get("report")
    if isinstance(report, dict):
        for key in REPORT_META_KEYS:
            value = report.get(key)
            if value not in (None, ""):
                meta[key] = value
    return meta


def _viewer_list_from_dict(data: dict[str, Any]) -> list[dict[str, Any]]:
    for key in VIEWER_LIST_KEYS:
        value = parse_jsonish(data.get(key))
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    report = parse_jsonish(data.get("report"))
    if isinstance(report, dict):
        for key in VIEWER_LIST_KEYS:
            value = parse_jsonish(report.get(key))
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        packed = parse_jsonish(report.get("report"))
        if isinstance(packed, dict):
            enriched = dict(packed)
            enriched["messages"] = report.get("messages", enriched.get("messages"))
            enriched["messagesTS"] = report.get("messagesTS", enriched.get("messagesTS"))
            return _viewer_list_from_bizon_report(enriched)
    if isinstance(report, str):
        packed = parse_jsonish(report)
        if isinstance(packed, dict):
            return _viewer_list_from_bizon_report(packed)
    return []


def _viewer_list_from_bizon_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    users_meta = report.get("usersMeta")
    if not isinstance(users_meta, dict):
        return []
    messages = parse_jsonish(report.get("messages"))
    messages_ts = parse_jsonish(report.get("messagesTS"))
    viewers: list[dict[str, Any]] = []
    for user_id, meta in users_meta.items():
        if not isinstance(meta, dict):
            continue
        viewer = dict(meta)
        viewer.setdefault("bizon_user_id", str(user_id))
        if isinstance(messages, dict) and user_id in messages:
            viewer["messages"] = messages[user_id]
        if isinstance(messages_ts, dict) and user_id in messages_ts:
            viewer["messagesTS"] = messages_ts[user_id]
        viewers.append(viewer)
    return viewers


def extract_viewers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _viewer_list_from_dict(payload)


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def platform_id_for_viewer(viewer: dict[str, Any], meta: dict[str, Any] | None = None) -> str:
    meta = meta or {}
    candidates = (
        ("bizon", viewer.get("chatUserId")),
        ("bizon_uid", viewer.get("uid")),
        ("bizon_sid", viewer.get("sid")),
        ("email", viewer.get("email")),
        ("phone", viewer.get("phone")),
        ("bizon_user", viewer.get("bizon_user_id")),
    )
    for prefix, value in candidates:
        text = clean_text(value, 500)
        if text:
            return f"{prefix}:{text}"
    return "bizon_hash:" + _stable_hash({"viewer": viewer, "webinarId": meta.get("webinarId")})


def normalize_viewer(viewer: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    meta = dict(meta or {})
    fields: dict[str, Any] = {
        "platform": "bizon365",
        "source": "bizon365_report",
    }
    for key in REPORT_META_KEYS:
        if meta.get(key) not in (None, ""):
            fields[key] = meta[key]
    for key in VIEWER_KEEP_KEYS:
        if viewer.get(key) not in (None, ""):
            fields[key] = viewer[key]
    for key, value in viewer.items():
        if key.startswith("utm_") or key.startswith("p") or key in {"sup", "cu1", "c1"}:
            if value not in (None, ""):
                fields[str(key)] = value
    fields["raw_viewer"] = viewer
    return {
        "platform_id": platform_id_for_viewer(viewer, fields),
        "custom_fields": fields,
    }


def normalize_viewers(viewers: list[dict[str, Any]], meta: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for viewer in viewers:
        record = normalize_viewer(viewer, meta)
        platform_id = record["platform_id"]
        if not platform_id:
            continue
        if platform_id in seen:
            records = [existing for existing in records if existing["platform_id"] != platform_id]
        seen.add(platform_id)
        records.append(record)
    return records


def _viewer_components(viewers: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Join repeated entries when any stable Bizon/contact identity overlaps."""
    parents = list(range(len(viewers)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        lroot, rroot = find(left), find(right)
        if lroot != rroot:
            parents[rroot] = lroot

    token_owner: dict[str, int] = {}
    for index, viewer in enumerate(viewers):
        tokens = viewer_identity_tokens(viewer)
        if not tokens:
            tokens = {"anonymous:" + _stable_hash(viewer)}
        for token in tokens:
            previous = token_owner.get(token)
            if previous is None:
                token_owner[token] = index
            else:
                union(index, previous)

    groups: dict[int, list[dict[str, Any]]] = {}
    for index, viewer in enumerate(viewers):
        groups.setdefault(find(index), []).append(viewer)
    return list(groups.values())


def _best_value(group: list[dict[str, Any]], *keys: str) -> Any:
    for viewer in group:
        for key in keys:
            value = viewer.get(key)
            if value not in (None, ""):
                return value
    return ""


def _preferred_person_key(tokens: set[str]) -> str:
    for prefix in ("chatUserId:", "uid:", "sid:", "email:", "phone:", "bizon_user_id:"):
        matches = sorted(token for token in tokens if token.startswith(prefix))
        if matches:
            return matches[0]
    return sorted(tokens)[0] if tokens else ""


def _watch_summary(group: list[dict[str, Any]]) -> dict[str, Any]:
    relative: list[tuple[float, float]] = []
    absolute: list[tuple[float, float]] = []
    for viewer in group:
        relative.extend(visit_intervals(viewer))
        item = absolute_visit_interval(viewer)
        if item:
            absolute.append(item)

    source = "vi"
    merged = _merged_intervals(relative)
    if not merged:
        source = "view_range"
        merged = _merged_intervals(absolute)
    seconds = sum(end - start for start, end in merged)
    valid = math.isfinite(seconds) and 0 <= seconds <= MAX_WATCH_SECONDS
    return {
        "watch_seconds": round(seconds, 3) if valid else None,
        "watch_minutes": round(seconds / 60.0, 3) if valid else None,
        "watch_valid": valid,
        "watch_source": source if merged else "missing",
        "watch_intervals": [{"start": start, "end": end} for start, end in merged],
        "watch_error": "" if valid else ("duration_out_of_range" if merged else "duration_missing"),
    }


def _chat_summary(group: list[dict[str, Any]]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for viewer in group:
        messages = parse_jsonish(viewer.get("messages"))
        timings = parse_jsonish(viewer.get("messagesTS"))
        if not isinstance(messages, list):
            continue
        timings = timings if isinstance(timings, list) else []
        for index, message in enumerate(messages[:100]):
            text = clean_text(message, 1000).replace("\r", " ").replace("\n", " ")
            if not text:
                continue
            raw_second = timings[index] if index < len(timings) else None
            second_number = _finite_number(raw_second)
            second = max(0, int(round(second_number))) if second_number is not None else None
            marker = (text, str(second))
            if marker in seen:
                continue
            seen.add(marker)
            if second is None:
                label = "--:--"
            else:
                hours, remainder = divmod(second, 3600)
                minutes, seconds = divmod(remainder, 60)
                label = f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"
            entries.append({"second": second, "time": label, "text": text})
    entries.sort(key=lambda item: (item["second"] is None, item["second"] or 0))
    rendered = "\n".join(f"[{item['time']}] {item['text']}" for item in entries)
    return {
        "chat_message_count": len(entries),
        "chat_messages": entries,
        "chat_messages_text": clean_text(rendered, 3500),
    }


def normalize_attendances(
    viewers: list[dict[str, Any]],
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    meta = dict(meta or {})
    webinar_id = clean_text(meta.get("webinarId") or meta.get("webinar_id"), 1000)
    if not webinar_id:
        webinar_id = "report:" + _stable_hash(meta)
    records: list[dict[str, Any]] = []
    for group in _viewer_components([item for item in viewers if isinstance(item, dict)]):
        tokens = sorted(set().union(*(viewer_identity_tokens(item) for item in group)))
        person_key = _preferred_person_key(set(tokens)) if tokens else "anonymous:" + _stable_hash(group)
        attendance_key = "attendance:" + _stable_hash({"webinar_id": webinar_id, "person_key": person_key})
        representative = dict(group[0])
        fields: dict[str, Any] = {
            "attendance_key": attendance_key,
            "person_key": person_key,
            "identity_tokens": tokens,
            "platform": "bizon365",
            "source": "bizon365_report",
            "webinarId": webinar_id,
            "username": clean_text(_best_value(group, "username", "name"), 500),
            "email": normalize_email(_best_value(group, "email")),
            "phone": normalize_phone(_best_value(group, "phone")),
            "city": clean_text(_best_value(group, "city"), 500),
            "finished": any(bool(item.get("finished")) for item in group),
            "profile_count": len(group),
            "profiles": group,
            "webinar_at": webinar_at_from_values(
                meta.get("webinarId"), meta.get("roomid"),
                _best_value(group, "webinarId", "roomid"),
            ),
            "clicked_button": any(_has_click(item.get("buttons")) for item in group),
            "clicked_banner": any(_has_click(item.get("banners")) for item in group),
        }
        for key in REPORT_META_KEYS:
            if meta.get(key) not in (None, ""):
                fields[key] = meta[key]
        for key in VIEWER_KEEP_KEYS:
            value = representative.get(key)
            if value not in (None, "") and key not in fields:
                fields[key] = value
        for key in ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "p1", "p2", "p3", "sup", "cu1", "c1", "cv"):
            value = _best_value(group, key)
            if value not in (None, ""):
                fields[key] = value
        fields.update(_watch_summary(group))
        fields.update(_chat_summary(group))
        records.append({"platform_id": attendance_key, "custom_fields": fields})
    return records
