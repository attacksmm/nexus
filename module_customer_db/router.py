import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()
_db_path = None


def setup(ctx):
    global _db_path
    _db_path = ctx.db_path
    import asyncio
    asyncio.create_task(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS customers (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                name    TEXT NOT NULL,
                phone   TEXT DEFAULT '',
                email   TEXT DEFAULT '',
                source  TEXT DEFAULT '',
                tags    TEXT DEFAULT '',
                notes   TEXT DEFAULT '',
                created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_customers_name  ON customers(name);
            CREATE INDEX IF NOT EXISTS idx_customers_phone ON customers(phone);
            CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
        """)
        await db.commit()


class CustomerIn(BaseModel):
    name:   str
    phone:  str = ""
    email:  str = ""
    source: str = ""
    tags:   str = ""
    notes:  str = ""


@router.get("/customers")
async def list_customers(
    q: str = Query("", alias="q"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
):
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        if q:
            pattern = f"%{q}%"
            cur = await db.execute(
                "SELECT * FROM customers WHERE name LIKE ? OR phone LIKE ? OR email LIKE ? OR tags LIKE ?"
                " ORDER BY id DESC LIMIT ? OFFSET ?",
                (pattern, pattern, pattern, pattern, limit, offset),
            )
            cnt = await db.execute(
                "SELECT COUNT(*) FROM customers WHERE name LIKE ? OR phone LIKE ? OR email LIKE ? OR tags LIKE ?",
                (pattern, pattern, pattern, pattern),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM customers ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)
            )
            cnt = await db.execute("SELECT COUNT(*) FROM customers")
        rows = [dict(r) for r in await cur.fetchall()]
        (total,) = await cnt.fetchone()
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/customers/{cid}")
async def get_customer(cid: int):
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM customers WHERE id = ?", (cid,))
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Клиент не найден")
    return dict(row)


@router.post("/customers", status_code=201)
async def create_customer(data: CustomerIn):
    async with aiosqlite.connect(_db_path) as db:
        cur = await db.execute(
            "INSERT INTO customers (name, phone, email, source, tags, notes)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (data.name, data.phone, data.email, data.source, data.tags, data.notes),
        )
        await db.commit()
        cid = cur.lastrowid
    return {"id": cid, **data.dict()}


@router.put("/customers/{cid}")
async def update_customer(cid: int, data: CustomerIn):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute(
            "UPDATE customers SET name=?, phone=?, email=?, source=?, tags=?, notes=?,"
            " updated=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id=?",
            (data.name, data.phone, data.email, data.source, data.tags, data.notes, cid),
        )
        await db.commit()
    return {"id": cid, **data.dict()}


@router.delete("/customers/{cid}")
async def delete_customer(cid: int):
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM customers WHERE id = ?", (cid,))
        await db.commit()
    return {"ok": True}


@router.get("/stats")
async def stats():
    async with aiosqlite.connect(_db_path) as db:
        (total,) = (await (await db.execute("SELECT COUNT(*) FROM customers")).fetchone())
        (today,) = (await (await db.execute(
            "SELECT COUNT(*) FROM customers WHERE date(created)=date('now')"
        )).fetchone())
    return {"total": total, "today": today}
