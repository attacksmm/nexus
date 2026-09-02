from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, unquote_plus


TABLE_SERVICES = {
    "cdb_getcourse_users": "getcourse",
    "cdb_getcourse_orders": "getcourse_order",
    "cdb_amo_deals": "amo",
    "cdb_vk_clients": "vk",
    "cdb_telegram_clients": "telegram",
    "cdb_avito_clients": "avito",
    "cdb_bizon365_attendance": "bizon",
}
MAX_RELATED_RECORDS = 50
MAX_VARIABLE_VALUE = 2000
SENSITIVE_KEY = re.compile(
    r"(?:token|secret|password|passwd|cookie|authorization|api[_-]?key|raw|payload|access[_-]?key|refresh[_-]?key)",
    re.I,
)
VARIABLE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_.:-]{1,160})\s*\}\}")
IDENTITY_EXCLUDED_PARTS = {"possible_accounts", "raw_payload", "manager", "responsible"}


def clean(value: Any, limit: int = 2000) -> str:
    return str(value or "").strip()[:limit]


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", clean(value, 100))
    if len(digits) == 10:
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return "+" + digits if 8 <= len(digits) <= 15 and len(set(digits)) >= 5 else ""


def normalize_email(value: Any) -> str:
    text = clean(value, 320).casefold()
    return text if re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", text) else ""


def normalize_username(value: Any) -> str:
    return clean(value, 200).lstrip("@").casefold()


def strong_id(value: Any) -> str:
    text = clean(value, 300)
    return "" if text.casefold() in {"", "0", "none", "null", "undefined", "unknown"} else text


def numeric_id(value: Any) -> str:
    text = strong_id(value)
    return text if re.fullmatch(r"\d{3,20}", text) else ""


def ym_uid(value: Any) -> str:
    """Yandex Metrica user id is numeric; discard copied UI punctuation safely."""
    return re.sub(r"\D+", "", clean(value, MAX_VARIABLE_VALUE))[:64]


def parse_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def variable_key(value: Any) -> str:
    text = re.sub(r"[^a-zA-Z0-9_]+", "_", clean(value, 200).casefold()).strip("_")
    return text[:100]


def scalars(value: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_key = variable_key(key)
            path = ".".join(part for part in (prefix, child_key) if part)
            if isinstance(child, (dict, list)):
                yield from scalars(child, path)
            else:
                yield path, child
    elif isinstance(value, list):
        for item in value[:20]:
            if isinstance(item, (dict, list)):
                yield from scalars(item, prefix)
            elif prefix:
                yield prefix, item


def identity_scalars(fields: dict[str, Any]) -> Iterable[tuple[str, Any]]:
    for path, value in scalars(fields):
        parts = set(path.split("."))
        if parts & IDENTITY_EXCLUDED_PARTS or any(part.startswith(("manager_", "responsible_")) for part in parts):
            continue
        yield path, value


def contact_identity(fields: dict[str, Any]) -> tuple[set[str], set[str]]:
    phones: set[str] = set()
    emails: set[str] = set()
    for key, item in identity_scalars(fields):
        leaf = key.rsplit(".", 1)[-1]
        if leaf in {"phone", "phones", "telephone", "csv_phone"} or leaf.endswith("_phone"):
            if value := normalize_phone(item):
                phones.add(value)
        elif leaf in {"email", "emails", "e_mail"} or leaf.endswith("_email"):
            if value := normalize_email(item):
                emails.add(value)
    return phones, emails


def parse_utm_term(value: Any) -> list[tuple[str, str]]:
    text = unquote_plus(clean(value, 1000))
    if not text:
        return []
    explicit: list[tuple[str, str]] = []
    normalized = text.replace(":", "=", 1) if re.match(r"^(?:platform_id|salebot_id):", text, re.I) else text
    for key, item in parse_qsl(normalized.lstrip("?"), keep_blank_values=False):
        kind = variable_key(key)
        current = strong_id(item)
        if kind in {"platform_id", "vk_platform_id"} and current:
            explicit.append(("vk_platform", current))
        elif kind in {"salebot_id", "salebot_client_id"} and current:
            explicit.append(("salebot", current))
    if explicit:
        return explicit
    match = re.fullmatch(r"\s*(platform_id|vk_platform_id|salebot_id|salebot_client_id)\s*[=:]\s*(\S+)\s*", text, re.I)
    if match:
        kind = "salebot" if "salebot" in match.group(1).casefold() else "vk_platform"
        return [(kind, strong_id(match.group(2)))]
    current = strong_id(text)
    return [("candidate", current)] if current and len(current) <= 300 else []


def identity_tokens(service: str, platform_id: Any, fields: dict[str, Any], *, include_utm: bool = False) -> set[str]:
    tokens: set[str] = set()
    record_id = strong_id(platform_id)
    if record_id:
        tokens.add(f"{service}_record:{record_id}")
        if service == "vk":
            tokens.add(f"vk_platform:{record_id}")
            if record_id.isdigit():
                tokens.add(f"vk:{record_id}")
        elif service == "telegram" and record_id.isdigit():
            tokens.add(f"telegram:{record_id}")
    for key, item in identity_scalars(fields):
        leaf = key.rsplit(".", 1)[-1]
        if leaf.startswith("manager_"):
            continue
        if leaf in {"phone", "phones", "telephone", "csv_phone"} or leaf.endswith("_phone"):
            if value := normalize_phone(item):
                tokens.add("phone:" + value)
        elif leaf in {"email", "emails", "e_mail"} or leaf.endswith("_email"):
            if value := normalize_email(item):
                tokens.add("email:" + value)
        elif leaf in {"salebot_id", "salebot_client_id", "sb_id"}:
            if value := numeric_id(item):
                tokens.add("salebot:" + value)
        elif leaf in {"vk_id", "vkontakte_id", "senler_id"}:
            if value := re.sub(r"\D+", "", clean(item, 100)):
                tokens.add("vk:" + value)
        elif leaf in {"platform_id", "vk_platform_id"}:
            if value := strong_id(item):
                tokens.add("vk_platform:" + value)
        elif leaf in {"telegram_id", "tg_id"}:
            if value := numeric_id(item):
                tokens.add("telegram:" + value)
        elif leaf in {"telegram_username", "tg_username", "username"} and service == "telegram":
            if value := normalize_username(item):
                tokens.add("telegram_username:" + value)
        elif leaf in {"gc_user_id", "getcourse_user_id", "user_id"} and service.startswith("getcourse"):
            if value := numeric_id(item):
                tokens.add("getcourse_user:" + value)
        elif leaf in {"ym_uid", "user_ym_uid", "_ym_uid"}:
            if value := strong_id(item):
                tokens.add("ym:" + value)
        elif leaf in {"visitor_id", "yclid", "chatuserid"}:
            if value := strong_id(item):
                tokens.add(leaf + ":" + value)
        elif include_utm and leaf == "utm_term":
            for kind, value in parse_utm_term(item):
                if kind != "candidate" and value:
                    tokens.add(f"{kind}:{value}")
    return tokens


def token_hash(token: str) -> str:
    return hashlib.blake2b(token.encode("utf-8"), digest_size=16).hexdigest()


def source_fingerprint(path: Path) -> str:
    parts = []
    for candidate in (path, Path(str(path) + "-wal")):
        try:
            stat = candidate.stat()
            parts.append(f"{candidate.name}:{stat.st_size}:{stat.st_mtime_ns}")
        except OSError:
            parts.append(f"{candidate.name}:missing")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


class IdentityIndex:
    def __init__(self, customer_db: Path, index_db: Path):
        self.customer_db = Path(customer_db)
        self.index_db = Path(index_db)

    def _open_source(self) -> sqlite3.Connection:
        db = sqlite3.connect(f"file:{self.customer_db.as_posix()}?mode=ro", uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA cache_size=-4096")
        db.execute("PRAGMA temp_store=FILE")
        return db

    def cleanup_staging(self) -> None:
        current_pid = str(os.getpid())
        prefix = f".{self.index_db.name}."
        for stale in self.index_db.parent.glob(f"{prefix}*.tmp"):
            parts = stale.name[len(prefix):].split(".", 1)
            if parts and parts[0] == current_pid:
                continue
            try:
                stale.unlink()
            except OSError:
                pass

    @staticmethod
    def _tables(db: sqlite3.Connection) -> set[str]:
        return {str(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    @staticmethod
    def _entity_for_tokens(store: sqlite3.Connection, hashes: set[str]) -> int:
        ids: set[int] = set()
        if hashes:
            placeholders = ",".join("?" for _ in hashes)
            ids = {int(row[0]) for row in store.execute(
                f"SELECT DISTINCT entity_id FROM tokens WHERE token_hash IN ({placeholders})", tuple(hashes)
            )}
        if not ids:
            return int(store.execute("INSERT INTO entities DEFAULT VALUES").lastrowid)
        keep = min(ids)
        for other in sorted(ids - {keep}):
            store.execute("UPDATE OR IGNORE tokens SET entity_id=? WHERE entity_id=?", (keep, other))
            store.execute("DELETE FROM tokens WHERE entity_id=?", (other,))
            store.execute("UPDATE record_refs SET entity_id=? WHERE entity_id=?", (keep, other))
            store.execute("DELETE FROM entities WHERE entity_id=?", (other,))
        return keep

    @classmethod
    def _add_tokens(cls, store: sqlite3.Connection, ref_id: int, tokens: set[str]) -> int:
        hashes = {token_hash(token) for token in tokens if token}
        entity_id = cls._entity_for_tokens(store, hashes)
        store.execute("UPDATE record_refs SET entity_id=? WHERE ref_id=?", (entity_id, ref_id))
        store.executemany(
            "INSERT OR IGNORE INTO tokens(token_hash,entity_id) VALUES(?,?)",
            ((value, entity_id) for value in hashes),
        )
        return entity_id

    def build_if_changed(self, *, force: bool = False) -> dict[str, Any]:
        if not self.customer_db.is_file():
            return {"status": "missing", "records": 0, "fingerprint": ""}
        fingerprint = source_fingerprint(self.customer_db)
        if not force and self.index_db.is_file():
            try:
                with sqlite3.connect(self.index_db, timeout=5) as current:
                    row = current.execute("SELECT value FROM meta WHERE key='fingerprint'").fetchone()
                    if row and row[0] == fingerprint:
                        count = int(current.execute("SELECT COUNT(*) FROM record_refs").fetchone()[0])
                        return {"status": "current", "records": count, "fingerprint": fingerprint}
            except sqlite3.Error:
                pass
        self.index_db.parent.mkdir(parents=True, exist_ok=True)
        self.cleanup_staging()
        staging = self.index_db.with_name(f".{self.index_db.name}.{os.getpid()}.{time.time_ns()}.tmp")
        record_count = conflict_count = 0
        try:
            with self._open_source() as source, sqlite3.connect(staging, timeout=30) as store:
                store.executescript(
                    """
                    PRAGMA journal_mode=OFF;
                    PRAGMA synchronous=OFF;
                    PRAGMA temp_store=FILE;
                    PRAGMA cache_size=-8192;
                    CREATE TABLE entities(entity_id INTEGER PRIMARY KEY);
                    CREATE TABLE tokens(token_hash TEXT PRIMARY KEY,entity_id INTEGER NOT NULL);
                    CREATE INDEX ix_identity_tokens_entity ON tokens(entity_id);
                    CREATE TABLE record_refs(
                        ref_id INTEGER PRIMARY KEY,entity_id INTEGER NOT NULL DEFAULT 0,
                        table_name TEXT NOT NULL,record_id INTEGER NOT NULL,platform_id TEXT NOT NULL,
                        service TEXT NOT NULL,updated_at TEXT NOT NULL DEFAULT ''
                    );
                    CREATE INDEX ix_identity_refs_entity ON record_refs(entity_id,updated_at DESC,ref_id DESC);
                    CREATE UNIQUE INDEX ux_identity_refs_source ON record_refs(table_name,record_id);
                    CREATE TABLE pending_utm(ref_id INTEGER NOT NULL,kind TEXT NOT NULL,value TEXT NOT NULL);
                    CREATE TABLE conflicts(ref_id INTEGER NOT NULL,value_hash TEXT NOT NULL,kinds TEXT NOT NULL);
                    CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                    """
                )
                tables = self._tables(source)
                for table_name, service in TABLE_SERVICES.items():
                    if table_name not in tables:
                        continue
                    for row in source.execute(
                        f"SELECT id,platform_id,custom_fields,updated_at FROM {table_name} ORDER BY id"
                    ):
                        fields = parse_object(row["custom_fields"])
                        cursor = store.execute(
                            "INSERT INTO record_refs(table_name,record_id,platform_id,service,updated_at) VALUES(?,?,?,?,?)",
                            (table_name, int(row["id"]), clean(row["platform_id"], 300), service, clean(row["updated_at"], 80)),
                        )
                        ref_id = int(cursor.lastrowid)
                        self._add_tokens(store, ref_id, identity_tokens(service, row["platform_id"], fields))
                        for key, item in scalars(fields):
                            if key.rsplit(".", 1)[-1] != "utm_term":
                                continue
                            for kind, value in parse_utm_term(item):
                                if value:
                                    store.execute("INSERT INTO pending_utm(ref_id,kind,value) VALUES(?,?,?)", (ref_id, kind, value))
                        record_count += 1
                        if record_count % 2000 == 0:
                            store.commit()
                for ref_id, kind, value in store.execute("SELECT ref_id,kind,value FROM pending_utm"):
                    candidate_kinds = ("vk_platform", "salebot") if kind == "candidate" else (kind,)
                    matches: list[tuple[str, int]] = []
                    for candidate_kind in candidate_kinds:
                        row = store.execute(
                            "SELECT entity_id FROM tokens WHERE token_hash=?",
                            (token_hash(f"{candidate_kind}:{value}"),),
                        ).fetchone()
                        if row:
                            matches.append((candidate_kind, int(row[0])))
                    entity_ids = {item[1] for item in matches}
                    if len(entity_ids) > 1:
                        conflict_count += 1
                        store.execute(
                            "INSERT INTO conflicts(ref_id,value_hash,kinds) VALUES(?,?,?)",
                            (ref_id, token_hash(value), ",".join(item[0] for item in matches)),
                        )
                        continue
                    if not entity_ids:
                        continue
                    current = int(store.execute("SELECT entity_id FROM record_refs WHERE ref_id=?", (ref_id,)).fetchone()[0])
                    target = next(iter(entity_ids))
                    if current != target:
                        keep, other = min(current, target), max(current, target)
                        store.execute("UPDATE OR IGNORE tokens SET entity_id=? WHERE entity_id=?", (keep, other))
                        store.execute("DELETE FROM tokens WHERE entity_id=?", (other,))
                        store.execute("UPDATE record_refs SET entity_id=? WHERE entity_id=?", (keep, other))
                        store.execute("DELETE FROM entities WHERE entity_id=?", (other,))
                store.execute("DELETE FROM pending_utm")
                store.executemany(
                    "INSERT INTO meta(key,value) VALUES(?,?)",
                    (("fingerprint", fingerprint), ("built_at", str(int(time.time()))), ("records", str(record_count))),
                )
                store.commit()
            os.replace(staging, self.index_db)
            return {"status": "rebuilt", "records": record_count, "conflicts": conflict_count, "fingerprint": fingerprint}
        finally:
            if staging.exists():
                staging.unlink(missing_ok=True)

    def status(self) -> dict[str, Any]:
        if not self.index_db.is_file():
            return {"status": "missing", "records": 0, "entities": 0, "conflicts": 0}
        try:
            with sqlite3.connect(self.index_db, timeout=5) as db:
                return {
                    "status": "ready",
                    "records": int(db.execute("SELECT COUNT(*) FROM record_refs").fetchone()[0]),
                    "entities": int(db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]),
                    "conflicts": int(db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0]),
                    "built_at": (db.execute("SELECT value FROM meta WHERE key='built_at'").fetchone() or [""])[0],
                }
        except sqlite3.Error as exc:
            return {"status": "error", "error": clean(exc, 300), "records": 0, "entities": 0, "conflicts": 0}

    def platform_id_for_service(self, service: str, value: Any) -> str:
        current = strong_id(value)
        prefixes = {"vk": ("vk", "vk_platform"), "telegram": ("telegram",), "salebot": ("salebot",)}.get(service, ())
        if not current or not prefixes or not self.index_db.is_file():
            return ""
        try:
            with sqlite3.connect(f"file:{self.index_db.as_posix()}?mode=ro", uri=True, timeout=5) as index:
                entity_ids = {
                    int(row[0])
                    for prefix in prefixes
                    for row in index.execute("SELECT entity_id FROM tokens WHERE token_hash=?", (token_hash(f"{prefix}:{current}"),))
                }
                if len(entity_ids) != 1:
                    return ""
                rows = index.execute(
                    "SELECT DISTINCT platform_id FROM record_refs WHERE entity_id=? AND service=? AND platform_id=? LIMIT 2",
                    (next(iter(entity_ids)), service, current),
                ).fetchall()
                return clean(rows[0][0], 300) if len(rows) == 1 else ""
        except sqlite3.Error:
            return ""

    def platform_id_for_context(self, target_service: str, context: dict[str, Any]) -> str:
        if not self.index_db.is_file():
            return ""
        source_service = clean(context.get("service") or context.get("platform"), 40).casefold() or "context"
        source_service = {"amocrm": "amo", "getcourse_order": "getcourse_order"}.get(source_service, source_service)
        fields = context.get("fields") if isinstance(context.get("fields"), dict) else dict(context)
        tokens = identity_tokens(
            source_service,
            context.get("entity_id") or context.get("platform_id") or "",
            fields,
        )
        for key, prefix, normalizer in (
            ("phone", "phone", normalize_phone), ("email", "email", normalize_email),
            ("getcourse_user_id", "getcourse_user", strong_id),
            ("gc_user_id", "getcourse_user", strong_id),
        ):
            if value := normalizer(context.get(key)):
                tokens.add(f"{prefix}:{value}")
        hashes = {token_hash(value) for value in tokens}
        if not hashes:
            return ""
        try:
            with sqlite3.connect(f"file:{self.index_db.as_posix()}?mode=ro", uri=True, timeout=5) as index:
                placeholders = ",".join("?" for _ in hashes)
                entity_ids = [int(row[0]) for row in index.execute(
                    f"SELECT DISTINCT entity_id FROM tokens WHERE token_hash IN ({placeholders})", tuple(hashes)
                )]
                if not entity_ids:
                    return ""
                entity_placeholders = ",".join("?" for _ in entity_ids)
                rows = index.execute(
                    f"SELECT DISTINCT platform_id FROM record_refs WHERE entity_id IN ({entity_placeholders}) AND service=? LIMIT 2",
                    (*entity_ids, target_service),
                ).fetchall()
                if len(rows) == 1:
                    return clean(rows[0][0], 300)
                if rows or target_service != "getcourse":
                    return ""

                # Customer DB can receive an order before (or without) the
                # separate GetCourse user export.  The order still contains
                # the authoritative gc_user_id.  Recover it only from order
                # rows already attached to the exact matched identity, and
                # only when all those rows agree on one user.  This keeps a
                # duplicated phone/email from linking an amoCRM card to an
                # arbitrary GetCourse profile.
                order_ref_ids = [
                    int(row[0])
                    for row in index.execute(
                        f"SELECT DISTINCT record_id FROM record_refs "
                        f"WHERE entity_id IN ({entity_placeholders}) "
                        "AND table_name='cdb_getcourse_orders' LIMIT 200",
                        tuple(entity_ids),
                    )
                ]
                if not order_ref_ids:
                    return ""
                with self._open_source() as source:
                    if "cdb_getcourse_orders" not in self._tables(source):
                        return ""
                    order_placeholders = ",".join("?" for _ in order_ref_ids)
                    user_ids = {
                        numeric_id(value)
                        for row in source.execute(
                            f"SELECT custom_fields FROM cdb_getcourse_orders "
                            f"WHERE id IN ({order_placeholders})",
                            tuple(order_ref_ids),
                        )
                        for key, value in identity_scalars(parse_object(row[0]))
                        if key.rsplit(".", 1)[-1] in {"gc_user_id", "getcourse_user_id", "user_id"}
                    }
                    user_ids.discard("")
                    return next(iter(user_ids)) if len(user_ids) == 1 else ""
        except sqlite3.Error:
            return ""

    def telegram_target_for_utm_term(self, value: Any) -> dict[str, Any]:
        """Resolve an exact Telegram client from an order UTM value.

        ``platform_id`` is interpreted as the Senler Telegram user id for this
        workflow. ``salebot_id`` is only an identity bridge; no SaleBot network
        request is involved.
        """

        parsed = parse_utm_term(value)
        if not parsed:
            return {"ok": False, "status": "not_found", "platform_id": "", "source": "", "matches": []}
        direct: set[str] = set()
        salebot: set[str] = set()
        for kind, candidate in parsed:
            if not re.fullmatch(r"\d{1,24}", candidate or ""):
                continue
            if kind == "vk_platform":
                direct.add(candidate)
            elif kind == "salebot":
                salebot.add(candidate)
            elif kind == "candidate":
                direct.add(candidate)
                salebot.add(candidate)
        matches: dict[str, set[str]] = {}
        try:
            with self._open_source() as source:
                if "cdb_telegram_clients" not in self._tables(source):
                    return {"ok": False, "status": "unavailable", "platform_id": "", "source": "", "matches": []}
                for candidate in direct:
                    row = source.execute(
                        "SELECT DISTINCT platform_id FROM cdb_telegram_clients WHERE platform_id=? LIMIT 2",
                        (candidate,),
                    ).fetchall()
                    if len(row) == 1:
                        matches.setdefault(clean(row[0][0], 300), set()).add("senler_platform_id")
                for candidate in salebot:
                    rows = source.execute(
                        "SELECT platform_id,custom_fields FROM cdb_telegram_clients WHERE custom_fields LIKE ? LIMIT 50",
                        (f"%{candidate}%",),
                    ).fetchall()
                    for platform_id, raw_fields in rows:
                        exact = {
                            strong_id(item)
                            for key, item in scalars(parse_object(raw_fields))
                            if key.rsplit(".", 1)[-1] in {"salebot_id", "salebot_client_id", "sb_id"}
                        }
                        if candidate in exact:
                            matches.setdefault(clean(platform_id, 300), set()).add("salebot_id")
        except sqlite3.Error as exc:
            return {
                "ok": False,
                "status": "error",
                "platform_id": "",
                "source": "",
                "matches": [],
                "error": clean(exc, 300),
            }
        matches.pop("", None)
        if len(matches) != 1:
            return {
                "ok": False,
                "status": "conflict" if len(matches) > 1 else "not_found",
                "platform_id": "",
                "source": "",
                "matches": sorted(matches),
            }
        platform_id, sources = next(iter(matches.items()))
        return {
            "ok": True,
            "status": "resolved",
            "platform_id": platform_id,
            "source": "+".join(sorted(sources)),
            "matches": [platform_id],
        }

    def provider_id_for_exact_context(self, target_service: str, context: dict[str, Any]) -> str:
        """Resolve a provider from the exact source card without traversing merged identities."""
        source_service = clean(context.get("service") or context.get("platform"), 40).casefold()
        source_service = {"amocrm": "amo"}.get(source_service, source_service)
        table_name = {
            "getcourse": "cdb_getcourse_users",
            "getcourse_order": "cdb_getcourse_orders",
            "amo": "cdb_amo_deals",
        }.get(source_service, "")
        source_id = strong_id(context.get("entity_id") or context.get("platform_id"))
        field_sets = [context, context.get("fields") if isinstance(context.get("fields"), dict) else {}]
        try:
            with self._open_source() as source:
                tables = self._tables(source)
                if table_name in tables and source_id:
                    field_sets.extend(
                        parse_object(row[0])
                        for row in source.execute(
                            f"SELECT custom_fields FROM {table_name} WHERE platform_id=? ORDER BY updated_at DESC,id DESC LIMIT 3",
                            (source_id,),
                        )
                    )

                # A Streams student is a GetCourse user, but Customer DB may
                # know that person only through an order.  Resolve those
                # order rows by exact contact data as part of the same card
                # context.  This is deliberately an exact match (not a graph
                # traversal), so an unrelated merged identity cannot leak a
                # messenger id into the current card.
                if source_service == "getcourse" and "cdb_getcourse_orders" in tables:
                    context_fields = context.get("fields") if isinstance(context.get("fields"), dict) else {}
                    phones, emails = contact_identity({**context_fields, **context})
                    gc_ids = {
                        value
                        for value in (
                            numeric_id(context.get("getcourse_user_id")),
                            numeric_id(context.get("gc_user_id")),
                            numeric_id(context_fields.get("getcourse_user_id")),
                            numeric_id(context_fields.get("gc_user_id")),
                            numeric_id(source_id),
                        )
                        if value
                    }
                    tokens = {
                        *(f"email:{value}" for value in emails),
                        *(f"phone:{value}" for value in phones),
                        *(f"getcourse_user:{value}" for value in gc_ids),
                    }
                    order_ids: list[int] = []
                    if tokens and self.index_db.is_file():
                        try:
                            with sqlite3.connect(
                                f"file:{self.index_db.as_posix()}?mode=ro", uri=True, timeout=5,
                            ) as index:
                                hashes = tuple(token_hash(value) for value in tokens)
                                placeholders = ",".join("?" for _ in hashes)
                                entity_ids = [int(row[0]) for row in index.execute(
                                    f"SELECT DISTINCT entity_id FROM tokens WHERE token_hash IN ({placeholders})",
                                    hashes,
                                )]
                                if entity_ids:
                                    entity_placeholders = ",".join("?" for _ in entity_ids)
                                    order_ids = [int(row[0]) for row in index.execute(
                                        f"SELECT DISTINCT record_id FROM record_refs "
                                        f"WHERE entity_id IN ({entity_placeholders}) "
                                        "AND table_name='cdb_getcourse_orders' LIMIT 200",
                                        tuple(entity_ids),
                                    )]
                        except sqlite3.Error:
                            order_ids = []
                    if order_ids:
                        order_placeholders = ",".join("?" for _ in order_ids)
                        rows = source.execute(
                            f"SELECT custom_fields FROM cdb_getcourse_orders "
                            f"WHERE id IN ({order_placeholders}) "
                            "ORDER BY updated_at DESC,id DESC LIMIT 200",
                            tuple(order_ids),
                        ).fetchall()
                    elif not self.index_db.is_file():
                        clauses: list[str] = []
                        params: list[str] = []
                        for email in sorted(emails):
                            clauses.append("custom_fields LIKE ?")
                            params.append(f"%{email}%")
                        for phone in sorted(phones):
                            clauses.append("custom_fields LIKE ?")
                            params.append(f"%{phone.removeprefix('+')[-10:]}%")
                        for gc_id in sorted(gc_ids):
                            clauses.append("custom_fields LIKE ?")
                            params.append(f"%{gc_id}%")
                        rows = source.execute(
                            "SELECT custom_fields FROM cdb_getcourse_orders WHERE "
                            + " OR ".join(clauses)
                            + " ORDER BY updated_at DESC,id DESC LIMIT 200",
                            params,
                        ).fetchall() if clauses else []
                    else:
                        rows = []
                    if rows:
                        for row in rows:
                            fields = parse_object(row[0])
                            row_phones, row_emails = contact_identity(fields)
                            row_gc_ids = {
                                numeric_id(value)
                                for key, value in identity_scalars(fields)
                                if key.rsplit(".", 1)[-1] in {"gc_user_id", "getcourse_user_id", "user_id"}
                            }
                            row_gc_ids.discard("")
                            if phones & row_phones or emails & row_emails or gc_ids & row_gc_ids:
                                field_sets.append(fields)

                explicit: set[str] = set()
                salebot_explicit: set[str] = set()
                vk_dialog_explicit: set[str] = set()
                utm_values: set[tuple[str, str]] = set()
                direct_keys = {
                    "vk": {"vk_id", "vkontakte_id", "senler_id", "vk_platform_id"},
                    "telegram": {"telegram_id", "tg_id"},
                    "salebot": {"salebot_id", "salebot_client_id", "sb_id"},
                }.get(target_service, set())
                for fields in field_sets:
                    for key, value in scalars(fields):
                        leaf = key.rsplit(".", 1)[-1]
                        if leaf in direct_keys and (current := strong_id(value)):
                            explicit.add(current)
                        if leaf in {"salebot_id", "salebot_client_id", "sb_id"} and (current := strong_id(value)):
                            salebot_explicit.add(current)
                        if leaf == "utm_term":
                            utm_values.update(parse_utm_term(value))
                        text = clean(value, 4000)
                        for match in re.finditer(
                            r"https?://(?:www\.)?vk\.(?:com|ru)/gim\d+/convo/(\d+)", text, re.I,
                        ):
                            vk_dialog_explicit.add(match.group(1))
                        for match in re.finditer(
                            r"https?://(?:www\.)?vk\.(?:com|ru)/gim\d+\?[^\s#]*?\bsel=c?(\d+)",
                            text,
                            re.I,
                        ):
                            vk_dialog_explicit.add(match.group(1))

                if target_service == "vk":
                    explicit.update(value for kind, value in utm_values if kind == "vk_platform")
                    explicit.update(
                        value for kind, value in utm_values
                        if kind == "candidate" and value not in salebot_explicit
                    )
                    # amoCRM often stores a SaleBot client id in both utm_term
                    # and a community-dialog URL.  Even when the same number
                    # happens to exist as somebody else's VK id, it is not a
                    # verified VK identity for this deal.
                    explicit.difference_update(salebot_explicit - vk_dialog_explicit)
                    explicit.update(vk_dialog_explicit)
                    matches = {
                        candidate
                        for candidate in explicit
                        if source.execute(
                            "SELECT 1 FROM cdb_vk_clients WHERE platform_id=? LIMIT 1", (candidate,)
                        ).fetchone()
                    } if "cdb_vk_clients" in tables else set()
                    return next(iter(matches)) if len(matches) == 1 else ""

                salebot_candidates = set(salebot_explicit)
                salebot_candidates.update(value for kind, value in utm_values if kind in {"salebot", "candidate"})
                # amoCRM automations sometimes copy a VK dialog id or UTM
                # value into a field named ``salebot_id``.  A label alone is
                # therefore not proof that the person exists in SaleBot: the
                # id must be confirmed by the Telegram/SaleBot bridge below.
                telegram_ids = set(explicit) if target_service == "telegram" else set()
                if "cdb_telegram_clients" in tables:
                    for candidate in salebot_candidates:
                        rows = source.execute(
                            "SELECT platform_id,custom_fields FROM cdb_telegram_clients WHERE custom_fields LIKE ? LIMIT 20",
                            (f'%{candidate}%',),
                        )
                        linked = {
                            clean(row[0], 300)
                            for row in rows
                            if any(
                                key.rsplit(".", 1)[-1] in {"salebot_id", "salebot_client_id", "sb_id"}
                                and strong_id(value) == candidate
                                for key, value in scalars(parse_object(row[1]))
                            )
                        }
                        if target_service == "salebot" and len(linked) == 1:
                            telegram_ids.add(candidate)
                        elif target_service == "telegram":
                            telegram_ids.update(linked)
                    if target_service == "telegram":
                        telegram_ids = {
                            candidate for candidate in telegram_ids
                            if source.execute(
                                "SELECT 1 FROM cdb_telegram_clients WHERE platform_id=? LIMIT 1", (candidate,)
                            ).fetchone()
                        }
                return next(iter(telegram_ids)) if len(telegram_ids) == 1 else ""
        except sqlite3.Error:
            return ""

    def telegram_username_for_platform_id(self, platform_id: Any) -> str:
        """Return one verified public Telegram username for a Telegram client id."""
        candidate = numeric_id(platform_id)
        if not candidate:
            return ""
        try:
            with self._open_source() as source:
                if "cdb_telegram_clients" not in self._tables(source):
                    return ""
                usernames = {
                    username
                    for row in source.execute(
                        "SELECT custom_fields FROM cdb_telegram_clients "
                        "WHERE platform_id=? ORDER BY updated_at DESC,id DESC LIMIT 3",
                        (candidate,),
                    )
                    for key, value in identity_scalars(parse_object(row[0]))
                    if key.rsplit(".", 1)[-1] in {"telegram_username", "tg_username", "username"}
                    if (username := normalize_username(value))
                }
                return next(iter(usernames)) if len(usernames) == 1 else ""
        except sqlite3.Error:
            return ""

    def crm_utm_for_context(self, context: dict[str, Any]) -> dict[str, str]:
        """Return UTM values only from amoCRM records matching the exact current card."""
        service = clean(context.get("service") or context.get("platform"), 40).casefold()
        service = {"amocrm": "amo"}.get(service, service)
        table_name = {
            "getcourse": "cdb_getcourse_users",
            "getcourse_order": "cdb_getcourse_orders",
            "amo": "cdb_amo_deals",
        }.get(service, "")
        source_id = strong_id(context.get("entity_id") or context.get("platform_id"))
        field_sets = [context, context.get("fields") if isinstance(context.get("fields"), dict) else {}]
        try:
            with self._open_source() as source:
                tables = self._tables(source)
                if table_name in tables and source_id:
                    field_sets.extend(
                        parse_object(row[0])
                        for row in source.execute(
                            f"SELECT custom_fields FROM {table_name} WHERE platform_id=? ORDER BY updated_at DESC,id DESC LIMIT 3",
                            (source_id,),
                        )
                    )
                phones: set[str] = set()
                emails: set[str] = set()
                for fields in field_sets:
                    current_phones, current_emails = contact_identity(fields)
                    phones.update(current_phones)
                    emails.update(current_emails)
                if "cdb_amo_deals" not in tables or not (phones or emails):
                    return {}
                if service == "amo" and source_id:
                    rows = source.execute(
                        "SELECT id,custom_fields,updated_at FROM cdb_amo_deals WHERE platform_id=? ORDER BY updated_at DESC,id DESC",
                        (source_id,),
                    ).fetchall()
                else:
                    clauses: list[str] = []
                    params: list[str] = []
                    for email in sorted(emails):
                        clauses.append("custom_fields LIKE ?")
                        params.append(f"%{email}%")
                    for phone in sorted(phones):
                        clauses.append("custom_fields LIKE ?")
                        params.append(f"%{phone.removeprefix('+')[-10:]}%")
                    rows = source.execute(
                        "SELECT id,custom_fields,updated_at FROM cdb_amo_deals WHERE " + " OR ".join(clauses)
                        + " ORDER BY updated_at DESC,id DESC LIMIT 100",
                        params,
                    ).fetchall()
                matches: list[dict[str, Any]] = []
                for row in rows:
                    fields = parse_object(row[1])
                    row_phones, row_emails = contact_identity(fields)
                    if service == "amo" or phones & row_phones or emails & row_emails:
                        matches.append(fields)
                result: dict[str, str] = {}
                for suffix in ("source", "medium", "campaign", "content", "term"):
                    for fields in matches:
                        if value := first_safe_value(fields, (f"utm_{suffix}",)):
                            result[suffix] = value
                            break
                return result
        except sqlite3.Error:
            return {}

    def resolve(self, context: dict[str, Any]) -> dict[str, Any]:
        crm_utm = self.crm_utm_for_context(context) if self.customer_db.is_file() else {}
        if not self.index_db.is_file() or not self.customer_db.is_file():
            return {"status": "unavailable", "accounts": [], "variables": build_variables([], context, crm_utm), "conflicts": []}
        service = clean(context.get("service") or context.get("platform"), 40).casefold() or "context"
        service = {"amocrm": "amo", "getcourse_order": "getcourse_order"}.get(service, service)
        fields = context.get("fields") if isinstance(context.get("fields"), dict) else dict(context)
        platform_id = context.get("entity_id") or context.get("platform_id") or ""
        tokens = identity_tokens(service, platform_id, fields, include_utm=True)
        for key, prefix, normalizer in (
            ("phone", "phone", normalize_phone), ("email", "email", normalize_email),
            ("salebot_id", "salebot", strong_id), ("platform_id", "vk_platform", strong_id),
            ("vk_id", "vk", lambda value: re.sub(r"\D+", "", clean(value, 100))),
            ("telegram_id", "telegram", strong_id),
        ):
            if value := normalizer(context.get(key)):
                tokens.add(f"{prefix}:{value}")
        hashes = {token_hash(value) for value in tokens}
        if not hashes:
            return {"status": "not_found", "accounts": [], "variables": build_variables([], context, crm_utm), "conflicts": []}
        with sqlite3.connect(f"file:{self.index_db.as_posix()}?mode=ro", uri=True, timeout=10) as index:
            index.row_factory = sqlite3.Row
            placeholders = ",".join("?" for _ in hashes)
            entity_ids = {int(row[0]) for row in index.execute(
                f"SELECT DISTINCT entity_id FROM tokens WHERE token_hash IN ({placeholders})", tuple(hashes)
            )}
            if len(entity_ids) > 1:
                return {"status": "conflict", "accounts": [], "variables": build_variables([], context, crm_utm), "conflicts": sorted(entity_ids)}
            if not entity_ids:
                return {"status": "not_found", "accounts": [], "variables": build_variables([], context, crm_utm), "conflicts": []}
            entity_id = next(iter(entity_ids))
            refs = [dict(row) for row in index.execute(
                "SELECT * FROM record_refs WHERE entity_id=? ORDER BY updated_at DESC,ref_id DESC LIMIT ?",
                (entity_id, MAX_RELATED_RECORDS),
            )]
        records: list[dict[str, Any]] = []
        with self._open_source() as source:
            tables = self._tables(source)
            for ref in refs:
                table_name = ref["table_name"]
                if table_name not in tables:
                    continue
                row = source.execute(
                    f"SELECT id,platform_id,custom_fields,created_at,updated_at FROM {table_name} WHERE id=?",
                    (ref["record_id"],),
                ).fetchone()
                if not row:
                    continue
                records.append({
                    "service": ref["service"], "table": table_name, "record_id": int(row["id"]),
                    "platform_id": clean(row["platform_id"], 300), "fields": parse_object(row["custom_fields"]),
                    "created_at": clean(row["created_at"], 80), "updated_at": clean(row["updated_at"], 80),
                })
        return {
            "status": "resolved", "entity_id": entity_id,
            "accounts": account_views(records), "variables": build_variables(records, context, crm_utm), "conflicts": [],
            "truncated": len(refs) >= MAX_RELATED_RECORDS,
        }


def account_views(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for record in records:
        fields = record.get("fields") if isinstance(record.get("fields"), dict) else {}
        name = first_value(fields, ("name", "full_name", "contact_name"))
        if not name:
            name = " ".join(filter(None, (clean(fields.get("first_name"), 150), clean(fields.get("last_name") or fields.get("second_name"), 150))))
        result.append({
            "service": record.get("service", ""), "platform_id": record.get("platform_id", ""),
            "name": name, "updated_at": record.get("updated_at", ""),
        })
    return result


def first_value(fields: dict[str, Any], keys: tuple[str, ...]) -> str:
    wanted = {variable_key(key) for key in keys}
    for path, value in scalars(fields):
        if path.rsplit(".", 1)[-1] in wanted and (text := clean(value, MAX_VARIABLE_VALUE)):
            return text
    return ""


def first_safe_value(fields: dict[str, Any], keys: tuple[str, ...]) -> str:
    wanted = {variable_key(key) for key in keys}
    for path, value in identity_scalars(fields):
        if path.rsplit(".", 1)[-1] in wanted and (text := clean(value, MAX_VARIABLE_VALUE)):
            return text
    return ""


def build_variables(records: list[dict[str, Any]], context: dict[str, Any], crm_utm: dict[str, str] | None = None) -> dict[str, dict[str, str]]:
    variables: dict[str, dict[str, str]] = {}

    def put(key: str, value: Any, source: str, label: str = "") -> None:
        text = clean(value, MAX_VARIABLE_VALUE)
        if not key or not text or key in variables or SENSITIVE_KEY.search(key):
            return
        variables[key] = {"value": text, "source": source, "label": label or key}

    def put_amo_fields(fields: dict[str, Any], entity: str, source: str) -> None:
        for field in fields.get("custom_fields_values") or []:
            if not isinstance(field, dict):
                continue
            field_id = strong_id(field.get("field_id") or field.get("id"))
            values = field.get("values") if isinstance(field.get("values"), list) else []
            item = values[0] if values else None
            value = item.get("value") if isinstance(item, dict) else item
            if field_id:
                put(f"{entity}.cf.{field_id}", value, source)
                put(f"amo.{entity}.cf.{field_id}", value, source)
        for raw_key, value in fields.items():
            key = clean(raw_key, 200)
            match = re.fullmatch(r"(lead|contact)\.(\d+)", key)
            if match:
                put(f"{match.group(1)}.cf.{match.group(2)}", value, source)
                put(f"amo.{match.group(1)}.cf.{match.group(2)}", value, source)
            elif key.isdigit():
                put(f"{entity}.cf.{key}", value, source)
                put(f"amo.{entity}.cf.{key}", value, source)
        for path, value in scalars(fields):
            match = re.fullmatch(r"(lead|contact)\.(\d+)", path)
            if match:
                put(f"{match.group(1)}.cf.{match.group(2)}", value, source)
                put(f"amo.{match.group(1)}.cf.{match.group(2)}", value, source)
            elif path.isdigit():
                put(f"{entity}.cf.{path}", value, source)
                put(f"amo.{entity}.cf.{path}", value, source)

    context_fields = context.get("fields") if isinstance(context.get("fields"), dict) else {}
    context_name = clean(context.get("name"), 500) or first_value(context_fields, ("name", "full_name", "contact_name"))
    context_first = clean(context.get("first_name"), 200) or first_value(context_fields, ("first_name", "firstname"))
    context_last = clean(context.get("last_name"), 200) or first_value(context_fields, ("last_name", "second_name", "lastname"))
    if not context_name:
        context_name = " ".join(filter(None, (context_first, context_last)))
    put("contact.name", context_name, "current", "Имя контакта")
    put("name", context_name, "current", "Имя контакта")
    put("contact.first_name", context_first or context_name.split(" ", 1)[0], "current", "Имя")
    put("contact.last_name", context_last, "current", "Фамилия")
    put("contact.phone", context.get("phone"), "current", "Телефон")
    put("contact.email", context.get("email"), "current", "Email")
    put("manager.name", context.get("manager_name"), "manager", "Менеджер")
    put("yclid", context.get("yclid") or first_safe_value(context_fields, ("yclid",)), "current", "yclid")
    put("ym_uid", ym_uid(context.get("ym_uid") or first_safe_value(context_fields, ("ym_uid", "user_ym_uid", "_ym_uid"))), "current", "ym_uid")
    put("conversation_id", context.get("conversation_id") or first_safe_value(context_fields, ("conversation_id", "conversationid")), "current", "conversation_id")
    platform = variable_key(context.get("platform") or context.get("service"))
    entity_type = variable_key(context.get("entity_type"))
    if platform and context.get("entity_id"):
        put(f"{platform}.{entity_type + '.' if entity_type else ''}id", context.get("entity_id"), "current")
    if platform == "amocrm":
        entity = "contact" if entity_type == "contact" else "lead"
        put(f"{entity}.id", context.get("entity_id"), "current")
        put(f"amo.{entity}.id", context.get("entity_id"), "current")
        put("contact.id", context_fields.get("contact_id"), "current")
        put("amo.contact.id", context_fields.get("contact_id"), "current")
        responsible_id = context_fields.get("responsible_user_id")
        put(f"{entity}.responsible.id", responsible_id, "current")
        put(f"{entity}.responsible.name", context.get("manager_name"), "current")
        put_amo_fields(context_fields, entity, "current")
    for path, value in scalars(context_fields):
        if not path or SENSITIVE_KEY.search(path):
            continue
        put(f"{platform or 'current'}.{path}", value, "current")
    for record in records:
        namespace = {
            "getcourse_order": "getcourse.order", "getcourse": "getcourse",
            "amo": "amo.lead", "vk": "vk", "telegram": "telegram",
        }.get(clean(record.get("service"), 40), variable_key(record.get("service")) or "related")
        fields = record.get("fields") if isinstance(record.get("fields"), dict) else {}
        put(f"{namespace}.id", record.get("platform_id"), record.get("service", "related"))
        if clean(record.get("service"), 40) == "amo":
            put("lead.id", record.get("platform_id"), "amo")
            put("amo.lead.id", record.get("platform_id"), "amo")
            put("lead.responsible.id", first_value(fields, ("responsible_user_id",)), "amo")
            put_amo_fields(fields, "lead", "amo")
        for path, value in scalars(fields):
            if not path or SENSITIVE_KEY.search(path):
                continue
            put(f"{namespace}.{path}", value, record.get("service", "related"))
        if "contact.name" not in variables:
            put("contact.name", first_value(fields, ("contact_name", "full_name", "name")), record.get("service", "related"), "Имя контакта")
        if "contact.phone" not in variables:
            put("contact.phone", first_value(fields, ("phone", "telephone")), record.get("service", "related"), "Телефон")
        if "contact.email" not in variables:
            put("contact.email", first_value(fields, ("email", "e_mail")), record.get("service", "related"), "Email")
        if "yclid" not in variables:
            put("yclid", first_safe_value(fields, ("yclid",)), record.get("service", "related"), "yclid")
        if "ym_uid" not in variables:
            put("ym_uid", ym_uid(first_safe_value(fields, ("ym_uid", "user_ym_uid", "_ym_uid"))), record.get("service", "related"), "ym_uid")
        if "conversation_id" not in variables:
            put("conversation_id", first_safe_value(fields, ("conversation_id", "conversationid")), record.get("service", "related"), "conversation_id")
    for suffix, value in (crm_utm or {}).items():
        if suffix in {"source", "medium", "campaign", "content", "term"}:
            put(f"utm.{suffix}", value, "amoCRM", f"UTM {suffix}")
    return variables


def render_template(body: Any, variables: dict[str, Any]) -> dict[str, Any]:
    text = clean(body, 20_000)
    values = {
        key: clean(value.get("value") if isinstance(value, dict) else value, MAX_VARIABLE_VALUE)
        for key, value in variables.items()
    }
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = values.get(key, "")
        if not value:
            missing.append(key)
            return ""
        return value

    rendered = VARIABLE_PATTERN.sub(replace, text)
    return {"text": rendered, "missing": sorted(set(missing)), "ready": True}
