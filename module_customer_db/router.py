"""
customer-db v2.0.0
Структура: id, platform_id, custom_fields (JSON)
Поддерживает несколько именованных таблиц.
"""
import json
import re

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter()
_db_path = None

SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
PLACEHOLDER_VALUE = re.compile(r"^#\{[^{}]+\}$")


def setup(ctx):
    global _db_path
    _db_path = ctx.db_path
    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS _cdb_tables (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT DEFAULT '',
                schema_json TEXT NOT NULL DEFAULT '[]',
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
        """)
        # создаём таблицу по умолчанию если нет ни одной
        cur = await db.execute("SELECT COUNT(*) FROM _cdb_tables")
        (cnt,) = await cur.fetchone()
        if cnt == 0:
            await db.execute(
                "INSERT INTO _cdb_tables (name, display_name, description) VALUES (?, ?, ?)",
                ("default", "Основная", "Таблица клиентов по умолчанию"),
            )
            await db.execute(_create_table_sql("default"))
        cur = await db.execute("SELECT name FROM _cdb_tables ORDER BY id")
        for (name,) in await cur.fetchall():
            if SAFE_NAME.match(name):
                await db.execute(_create_table_sql(name))
                await db.execute(_create_index_sql(name))
        await db.commit()


def _create_table_sql(name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS cdb_{name} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            platform_id TEXT NOT NULL DEFAULT '',
            custom_fields TEXT NOT NULL DEFAULT '{{}}',
            created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
    """


def _create_index_sql(name: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS idx_cdb_{name}_platform_id ON cdb_{name} (platform_id);"


def _check_name(name: str):
    if not SAFE_NAME.match(name):
        raise HTTPException(400, "Имя таблицы: только латинские буквы, цифры, _. Начинается с буквы.")


def _is_empty_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or text.lower() in {"none", "null", "undefined"} or bool(PLACEHOLDER_VALUE.match(text))
    return False


def _clean_json_value(value):
    if _is_empty_value(value):
        return None
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if _is_empty_value(key):
                continue
            cleaned_item = _clean_json_value(item)
            if cleaned_item is not None:
                cleaned[str(key)] = cleaned_item
        return cleaned or None
    if isinstance(value, list):
        cleaned_items = []
        for item in value:
            cleaned_item = _clean_json_value(item)
            if cleaned_item is not None:
                cleaned_items.append(cleaned_item)
        return cleaned_items or None
    return value


def _clean_record(data: "RecordIn") -> "RecordIn":
    platform_id = str(data.platform_id or "").strip()
    cleaned = _clean_json_value(data.custom_fields or {})
    return RecordIn(platform_id=platform_id, custom_fields=cleaned if isinstance(cleaned, dict) else {})


async def _upsert_one(db, table: str, data: "RecordIn") -> tuple[int, str]:
    record = _clean_record(data)
    if not record.platform_id:
        raise HTTPException(400, "platform_id обязателен для upsert")

    tbl = f"cdb_{table}"
    await db.execute(_create_index_sql(table))
    cur = await db.execute(
        f"SELECT id FROM {tbl} WHERE platform_id = ? ORDER BY id ASC LIMIT 1",
        (record.platform_id,),
    )
    row = await cur.fetchone()
    custom_json = json.dumps(record.custom_fields, ensure_ascii=False)
    if row:
        record_id = int(row[0])
        await db.execute(
            f"UPDATE {tbl} SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (custom_json, record_id),
        )
        return record_id, "updated"

    cur = await db.execute(
        f"INSERT INTO {tbl} (platform_id, custom_fields) VALUES (?, ?)",
        (record.platform_id, custom_json),
    )
    return int(cur.lastrowid), "created"


# ── Tables management ─────────────────────────────────────────────────────────

@router.get("/tables")
async def list_tables():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM _cdb_tables ORDER BY id")
        rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        r["schema_json"] = json.loads(r["schema_json"])
    return rows


class TableIn(BaseModel):  # type: ignore[override]
    name: str
    display_name: str
    description: str = ""
    schema_json: list = []


@router.post("/tables", status_code=201)
async def create_table(data: TableIn):
    _check_name(data.name)
    async with aiosqlite.connect(_db_path) as db:
        try:
            await db.execute(
                "INSERT INTO _cdb_tables (name, display_name, description, schema_json) VALUES (?,?,?,?)",
                (data.name, data.display_name, data.description, json.dumps(data.schema_json)),
            )
            await db.execute(_create_table_sql(data.name))
            await db.execute(_create_index_sql(data.name))
            await db.commit()
        except Exception as e:
            raise HTTPException(409, f"Таблица уже существует: {e}")
    return {"ok": True, "name": data.name}


@router.delete("/tables/{name}")
async def delete_table(name: str):
    _check_name(name)
    if name == "default":
        raise HTTPException(400, "Нельзя удалить таблицу default")
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM _cdb_tables WHERE name = ?", (name,))
        await db.execute(f"DROP TABLE IF EXISTS cdb_{name}")
        await db.commit()
    return {"ok": True}


# ── Records ───────────────────────────────────────────────────────────────────

@router.get("/tables/{table}/records")
async def list_records(
    table: str,
    q: str = Query("", alias="q"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        tbl = f"cdb_{table}"
        if q:
            pat = f"%{q}%"
            cur = await db.execute(
                f"SELECT * FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (pat, pat, limit, offset),
            )
            cnt = await db.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE platform_id LIKE ? OR custom_fields LIKE ?",
                (pat, pat),
            )
        else:
            cur = await db.execute(f"SELECT * FROM {tbl} ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
            cnt = await db.execute(f"SELECT COUNT(*) FROM {tbl}")
        rows = [dict(r) for r in await cur.fetchall()]
        (total,) = await cnt.fetchone()

    for r in rows:
        try:
            r["custom_fields"] = json.loads(r["custom_fields"])
        except Exception:
            r["custom_fields"] = {}
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


class RecordIn(BaseModel):
    platform_id: str = ""
    custom_fields: dict = {}


class BatchRecordsIn(BaseModel):
    records: list[RecordIn] = Field(default_factory=list)
    dry_run: bool = False


@router.post("/tables/{table}/records", status_code=201)
async def create_record(table: str, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(_create_index_sql(table))
        cur = await db.execute(
            f"INSERT INTO cdb_{table} (platform_id, custom_fields) VALUES (?, ?)",
            (record.platform_id, json.dumps(record.custom_fields, ensure_ascii=False)),
        )
        await db.commit()
        rid = cur.lastrowid
    return {"id": rid, "platform_id": record.platform_id, "custom_fields": record.custom_fields}


@router.post("/tables/{table}/records/upsert")
async def upsert_record(table: str, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        record_id, status = await _upsert_one(db, table, record)
        await db.commit()
    return {
        "ok": True,
        "id": record_id,
        "status": status,
        "platform_id": record.platform_id,
        "custom_fields": record.custom_fields,
    }


@router.post("/tables/{table}/records/batch-upsert")
async def batch_upsert_records(table: str, data: BatchRecordsIn):
    _check_name(table)
    if len(data.records) > 1000:
        raise HTTPException(400, "batch-upsert принимает максимум 1000 записей за запрос")

    ordered_platform_ids: list[str] = []
    records_by_platform_id: dict[str, RecordIn] = {}
    skipped = 0
    for record in data.records:
        platform_id = str(record.platform_id or "").strip()
        if not platform_id:
            skipped += 1
            continue
        if platform_id not in records_by_platform_id:
            ordered_platform_ids.append(platform_id)
        records_by_platform_id[platform_id] = _clean_record(RecordIn(
            platform_id=platform_id,
            custom_fields=record.custom_fields or {},
        ))

    if not ordered_platform_ids:
        return {
            "ok": True,
            "total": len(data.records),
            "unique": 0,
            "created": 0,
            "updated": 0,
            "skipped": skipped,
        }

    tbl = f"cdb_{table}"
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(_create_index_sql(table))
        placeholders = ",".join("?" for _ in ordered_platform_ids)
        cur = await db.execute(
            f"SELECT id, platform_id FROM {tbl} WHERE platform_id IN ({placeholders}) ORDER BY id ASC",
            ordered_platform_ids,
        )
        existing: dict[str, int] = {}
        for row in await cur.fetchall():
            existing.setdefault(row["platform_id"], int(row["id"]))

        if data.dry_run:
            updated = sum(1 for platform_id in ordered_platform_ids if platform_id in existing)
            created = len(ordered_platform_ids) - updated
            return {
                "ok": True,
                "dry_run": True,
                "total": len(data.records),
                "unique": len(ordered_platform_ids),
                "created": created,
                "updated": updated,
                "skipped": skipped,
            }

        created = 0
        updated = 0
        ids: dict[str, int] = {}
        for platform_id in ordered_platform_ids:
            record = records_by_platform_id[platform_id]
            custom_json = json.dumps(record.custom_fields, ensure_ascii=False)
            record_id = existing.get(platform_id)
            if record_id is None:
                cur = await db.execute(
                    f"INSERT INTO {tbl} (platform_id, custom_fields) VALUES (?, ?)",
                    (platform_id, custom_json),
                )
                record_id = int(cur.lastrowid)
                existing[platform_id] = record_id
                created += 1
            else:
                await db.execute(
                    f"UPDATE {tbl} SET custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
                    (custom_json, record_id),
                )
                updated += 1
            ids[platform_id] = record_id

        await db.commit()

    return {
        "ok": True,
        "total": len(data.records),
        "unique": len(ordered_platform_ids),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "ids": ids,
    }


@router.get("/tables/{table}/records/{rid}")
async def get_record(table: str, rid: int):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"SELECT * FROM cdb_{table} WHERE id = ?", (rid,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Запись не найдена")
    r = dict(row)
    r["custom_fields"] = json.loads(r["custom_fields"])
    return r


@router.put("/tables/{table}/records/{rid}")
async def update_record(table: str, rid: int, data: RecordIn):
    _check_name(table)
    record = _clean_record(data)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            f"UPDATE cdb_{table} SET platform_id=?, custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (record.platform_id, json.dumps(record.custom_fields, ensure_ascii=False), rid),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/tables/{table}/records/{rid}")
async def delete_record(table: str, rid: int):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(f"DELETE FROM cdb_{table} WHERE id = ?", (rid,))
        await db.commit()
    return {"ok": True}


@router.get("/tables/{table}/stats")
async def table_stats(table: str):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{table}")).fetchone())
        (today,) = (await (await db.execute(
            f"SELECT COUNT(*) FROM cdb_{table} WHERE date(created_at)=date('now')"
        )).fetchone())
    return {"total": total, "today": today}


@router.get("/stats")
async def global_stats():
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT name, display_name FROM _cdb_tables ORDER BY id")
        tables = [dict(r) for r in await cur.fetchall()]
        result = []
        for t in tables:
            (total,) = (await (await db.execute(f"SELECT COUNT(*) FROM cdb_{t['name']}")).fetchone())
            result.append({**t, "total": total})
    return {"tables": result}
