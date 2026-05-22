import aiosqlite
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "nexus.db"


async def get_db():
    return await aiosqlite.connect(DB_PATH)


async def init_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS modules (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT NOT NULL DEFAULT '0.0.0',
                description TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'unloaded',
                installed_at TEXT NOT NULL,
                manifest_json TEXT NOT NULL DEFAULT '{}'
            );
        """)
        await db.commit()


async def get_modules_by_status(status: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            cur = await db.execute(
                "SELECT * FROM modules WHERE status = ? ORDER BY name", (status,)
            )
        else:
            cur = await db.execute("SELECT * FROM modules ORDER BY name")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def upsert_module(meta: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO modules (id, name, version, description, status, installed_at, manifest_json)
               VALUES (:id, :name, :version, :description, :status, :installed_at, :manifest_json)
               ON CONFLICT(id) DO UPDATE SET
                 name=excluded.name, version=excluded.version,
                 description=excluded.description, status=excluded.status,
                 installed_at=excluded.installed_at, manifest_json=excluded.manifest_json""",
            meta,
        )
        await db.commit()


async def update_module_status(module_id: str, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE modules SET status = ? WHERE id = ?", (status, module_id)
        )
        await db.commit()


async def delete_module(module_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM modules WHERE id = ?", (module_id,))
        await db.commit()
