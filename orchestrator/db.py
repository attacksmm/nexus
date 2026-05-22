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
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                module_access TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                active INTEGER NOT NULL DEFAULT 1
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
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        """)
        # migrations — add columns that may be missing in old DBs
        for col, ddl in [
            ("role",          "TEXT NOT NULL DEFAULT 'viewer'"),
            ("module_access", "TEXT NOT NULL DEFAULT '[]'"),
            ("created_at",    "TEXT NOT NULL DEFAULT ''"),
            ("active",        "INTEGER NOT NULL DEFAULT 1"),
        ]:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col} {ddl}")
            except Exception:
                pass
        await db.commit()


# ── Modules ───────────────────────────────────────────────────────────────────

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


# ── Users ─────────────────────────────────────────────────────────────────────

async def get_all_users() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, username, role, module_access, created_at, active FROM users ORDER BY id"
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_user_by_username(username: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE username = ? AND active = 1", (username,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def create_user(username: str, password_hash: str, role: str = "viewer", module_access: str = "[]") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO users (username, password_hash, role, module_access) VALUES (?, ?, ?, ?)",
            (username, password_hash, role, module_access),
        )
        await db.commit()
        return cur.lastrowid


async def update_user(user_id: int, role: str, module_access: str, active: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET role = ?, module_access = ?, active = ? WHERE id = ?",
            (role, module_access, active, user_id),
        )
        await db.commit()


async def update_user_password(user_id: int, password_hash: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
        await db.commit()


async def delete_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await db.commit()
