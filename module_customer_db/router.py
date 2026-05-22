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
from pydantic import BaseModel

router = APIRouter()
_db_path = None

SAFE_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")


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


def _check_name(name: str):
    if not SAFE_NAME.match(name):
        raise HTTPException(400, "Имя таблицы: только латинские буквы, цифры, _. Начинается с буквы.")


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


class TableIn(BaseModel):
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


@router.post("/tables/{table}/records", status_code=201)
async def create_record(table: str, data: RecordIn):
    _check_name(table)
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            f"INSERT INTO cdb_{table} (platform_id, custom_fields) VALUES (?, ?)",
            (data.platform_id, json.dumps(data.custom_fields, ensure_ascii=False)),
        )
        await db.commit()
        rid = cur.lastrowid
    return {"id": rid, "platform_id": data.platform_id, "custom_fields": data.custom_fields}


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
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            f"UPDATE cdb_{table} SET platform_id=?, custom_fields=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (data.platform_id, json.dumps(data.custom_fields, ensure_ascii=False), rid),
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
