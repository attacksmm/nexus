import mimetypes
import os
import re
import secrets
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "file-storage"
ROOT_ID = 1
MAX_FILE_SIZE = 100 * 1024 * 1024
MAX_TEXT_EDIT_SIZE = 1 * 1024 * 1024
MAX_NAME_LEN = 140

ALLOWED_EXTENSIONS = {
    "jpg", "jpeg", "png", "gif", "webp", "bmp", "ico",
    "mp4", "webm", "mov", "m4v",
    "mp3", "wav", "ogg", "m4a",
    "txt", "md", "csv", "json", "jsonl", "log",
    "pdf", "docx", "xlsx", "zip",
}

TEXT_EXTENSIONS = {"txt", "md", "csv", "json", "jsonl", "log"}
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "webp", "bmp", "ico"}
VIDEO_EXTENSIONS = {"mp4", "webm", "mov", "m4v"}
AUDIO_EXTENSIONS = {"mp3", "wav", "ogg", "m4a"}

_db_path: Path | None = None
_blob_dir: Path | None = None
_tmp_dir: Path | None = None

SAFE_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def setup(ctx):
    global _db_path, _blob_dir, _tmp_dir
    _db_path = ctx.db_path
    _blob_dir = ctx.data_dir / "blobs"
    _tmp_dir = ctx.data_dir / "tmp"
    _blob_dir.mkdir(parents=True, exist_ok=True)
    _tmp_dir.mkdir(parents=True, exist_ok=True)

    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id   INTEGER REFERENCES items(id) ON DELETE CASCADE,
                kind        TEXT NOT NULL CHECK(kind IN ('folder', 'file')),
                name        TEXT NOT NULL,
                stored_name TEXT,
                ext         TEXT NOT NULL DEFAULT '',
                mime_type   TEXT NOT NULL DEFAULT 'application/octet-stream',
                size        INTEGER NOT NULL DEFAULT 0,
                token       TEXT UNIQUE,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                UNIQUE(parent_id, name)
            );
            CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent_id, kind, name);
            CREATE INDEX IF NOT EXISTS idx_items_token ON items(token);
        """)
        now = _now()
        await db.execute(
            """
            INSERT OR IGNORE INTO items
                (id, parent_id, kind, name, created_at, updated_at)
            VALUES
                (?, NULL, 'folder', '', ?, ?)
            """,
            (ROOT_ID, now, now),
        )
        await db.commit()


class FolderIn(BaseModel):
    parent_id: int = ROOT_ID
    name: str


class TextFileIn(BaseModel):
    folder_id: int = ROOT_ID
    name: str
    content: str = ""


class RenameIn(BaseModel):
    name: str


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("file-storage module is not initialized")
    return _db_path


def _must_blob_dir() -> Path:
    if _blob_dir is None:
        raise RuntimeError("file-storage module is not initialized")
    return _blob_dir


def _must_tmp_dir() -> Path:
    if _tmp_dir is None:
        raise RuntimeError("file-storage module is not initialized")
    return _tmp_dir


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_name(name: str, *, folder: bool = False) -> str:
    clean = (name or "").strip()
    if not clean:
        raise HTTPException(400, "Имя обязательно")
    if len(clean) > MAX_NAME_LEN:
        raise HTTPException(400, f"Имя длиннее {MAX_NAME_LEN} символов")
    if clean in {".", ".."} or "/" in clean or "\\" in clean or SAFE_CONTROL_RE.search(clean):
        raise HTTPException(400, "Недопустимое имя")
    if folder and clean.endswith("."):
        clean = clean.rstrip(".").strip()
        if not clean:
            raise HTTPException(400, "Недопустимое имя папки")
    return clean


def _file_parts(name: str) -> tuple[str, str, str]:
    clean = _safe_name(name)
    if "." not in clean:
        clean = f"{clean}.txt"
    ext = clean.rsplit(".", 1)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Расширение .{ext} запрещено")
    mime_type = mimetypes.guess_type(clean)[0] or "application/octet-stream"
    return clean, ext, mime_type


def _category(ext: str, kind: str) -> str:
    if kind == "folder":
        return "folder"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    if ext in AUDIO_EXTENSIONS:
        return "audio"
    if ext in TEXT_EXTENSIONS:
        return "text"
    if ext == "pdf":
        return "pdf"
    if ext in {"docx", "xlsx"}:
        return "document"
    return "archive"


async def _require_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _require_editor(request: Request) -> dict:
    user = await _require_user(request)
    if user["role"] not in ("admin", "editor"):
        raise HTTPException(403, "Недостаточно прав")
    return user


async def _ensure_folder(db: aiosqlite.Connection, folder_id: int):
    cur = await db.execute("SELECT id FROM items WHERE id=? AND kind='folder'", (folder_id,))
    row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Папка не найдена")


async def _path_for(db: aiosqlite.Connection, item_id: int) -> list[dict]:
    cur = await db.execute("SELECT id, parent_id, name, kind FROM items WHERE id=?", (item_id,))
    row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Объект не найден")
    chain = []
    current = dict(row)
    while current:
        if current["id"] != ROOT_ID:
            chain.append({"id": current["id"], "name": current["name"], "kind": current["kind"]})
        if current["parent_id"] is None:
            break
        cur = await db.execute("SELECT id, parent_id, name, kind FROM items WHERE id=?", (current["parent_id"],))
        next_row = await cur.fetchone()
        current = dict(next_row) if next_row else None
    chain.reverse()
    return [{"id": ROOT_ID, "name": "Хранилище", "kind": "folder"}, *chain]


def _item_dict(row: aiosqlite.Row) -> dict:
    data = dict(row)
    data["category"] = _category(data["ext"], data["kind"])
    data["is_text_editable"] = data["kind"] == "file" and data["ext"] in TEXT_EXTENSIONS and data["size"] <= MAX_TEXT_EDIT_SIZE
    return data


@router.get("/config")
async def config(request: Request):
    await _require_user(request)
    return {
        "root_id": ROOT_ID,
        "max_file_size": MAX_FILE_SIZE,
        "max_text_edit_size": MAX_TEXT_EDIT_SIZE,
        "allowed_extensions": sorted(ALLOWED_EXTENSIONS),
    }


@router.get("/folders/{folder_id}/items")
async def list_items(folder_id: int, request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_folder(db, folder_id)
        cur = await db.execute(
            """
            SELECT id, parent_id, kind, name, ext, mime_type, size, created_at, updated_at
            FROM items
            WHERE parent_id=?
            ORDER BY CASE kind WHEN 'folder' THEN 0 ELSE 1 END, lower(name)
            """,
            (folder_id,),
        )
        items = [_item_dict(r) for r in await cur.fetchall()]
        path = await _path_for(db, folder_id)
    return {"folder_id": folder_id, "path": path, "items": items}


@router.post("/folders", status_code=201)
async def create_folder(data: FolderIn, request: Request):
    await _require_editor(request)
    name = _safe_name(data.name, folder=True)
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, data.parent_id)
        try:
            cur = await db.execute(
                """
                INSERT INTO items(parent_id, kind, name, created_at, updated_at)
                VALUES (?, 'folder', ?, ?, ?)
                """,
                (data.parent_id, name, now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(409, "Папка или файл с таким именем уже существует")
    return {"id": cur.lastrowid, "name": name, "kind": "folder"}


@router.post("/text-files", status_code=201)
async def create_text_file(data: TextFileIn, request: Request):
    await _require_editor(request)
    name, ext, mime_type = _file_parts(data.name)
    if ext not in TEXT_EXTENSIONS:
        raise HTTPException(400, "Через редактор можно создавать только текстовые файлы")
    content = data.content.encode("utf-8")
    if len(content) > MAX_TEXT_EDIT_SIZE:
        raise HTTPException(400, "Текстовый файл слишком большой для редактора")
    stored_name = uuid.uuid4().hex
    blob_path = _must_blob_dir() / stored_name
    async with aiofiles.open(blob_path, "wb") as f:
        await f.write(content)

    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, data.folder_id)
        try:
            cur = await db.execute(
                """
                INSERT INTO items(parent_id, kind, name, stored_name, ext, mime_type, size, token, created_at, updated_at)
                VALUES (?, 'file', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (data.folder_id, name, stored_name, ext, mime_type, len(content), secrets.token_urlsafe(32), now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            blob_path.unlink(missing_ok=True)
            raise HTTPException(409, "Папка или файл с таким именем уже существует")
    return {"id": cur.lastrowid, "name": name, "kind": "file"}


@router.post("/uploads", status_code=201)
async def upload_file(request: Request, folder_id: int = Form(ROOT_ID), file: UploadFile = File(...)):
    await _require_editor(request)
    name, ext, mime_type = _file_parts(file.filename or "")
    stored_name = uuid.uuid4().hex
    tmp_path = _must_tmp_dir() / f"{stored_name}.upload"
    blob_path = _must_blob_dir() / stored_name
    size = 0

    try:
        async with aiofiles.open(tmp_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_FILE_SIZE:
                    raise HTTPException(413, "Файл больше 100 MB")
                await out.write(chunk)
        shutil.move(str(tmp_path), str(blob_path))
    finally:
        tmp_path.unlink(missing_ok=True)
        await file.close()

    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, folder_id)
        try:
            cur = await db.execute(
                """
                INSERT INTO items(parent_id, kind, name, stored_name, ext, mime_type, size, token, created_at, updated_at)
                VALUES (?, 'file', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (folder_id, name, stored_name, ext, mime_type, size, secrets.token_urlsafe(32), now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            blob_path.unlink(missing_ok=True)
            raise HTTPException(409, "Папка или файл с таким именем уже существует")
    return {"id": cur.lastrowid, "name": name, "kind": "file", "size": size}


@router.put("/items/{item_id}/rename")
async def rename_item(item_id: int, data: RenameIn, request: Request):
    await _require_editor(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM items WHERE id=? AND id<>?", (item_id, ROOT_ID))
        item = await cur.fetchone()
        if not item:
            raise HTTPException(404, "Объект не найден")
        if item["kind"] == "folder":
            name = _safe_name(data.name, folder=True)
            ext = item["ext"]
            mime_type = item["mime_type"]
        else:
            name, ext, mime_type = _file_parts(data.name)
        try:
            await db.execute(
                "UPDATE items SET name=?, ext=?, mime_type=?, updated_at=? WHERE id=?",
                (name, ext, mime_type, _now(), item_id),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(409, "Папка или файл с таким именем уже существует")
    return {"ok": True, "name": name}


@router.delete("/items/{item_id}")
async def delete_item(item_id: int, request: Request):
    await _require_editor(request)
    if item_id == ROOT_ID:
        raise HTTPException(400, "Корневую папку удалить нельзя")
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
        if not item:
            raise HTTPException(404, "Объект не найден")
        files = []
        item_ids = [item_id]
        if item["kind"] == "folder":
            files = await _collect_file_blobs(db, item_id)
            item_ids = await _collect_item_ids(db, item_id)
        elif item["stored_name"]:
            files = [item["stored_name"]]
        placeholders = ",".join("?" for _ in item_ids)
        await db.execute(f"DELETE FROM items WHERE id IN ({placeholders})", item_ids)
        await db.commit()
    for stored_name in files:
        (_must_blob_dir() / stored_name).unlink(missing_ok=True)
    return {"ok": True}


async def _collect_file_blobs(db: aiosqlite.Connection, folder_id: int) -> list[str]:
    cur = await db.execute("SELECT id, kind, stored_name FROM items WHERE parent_id=?", (folder_id,))
    rows = await cur.fetchall()
    result = []
    for row in rows:
        row = dict(row)
        if row["kind"] == "folder":
            result.extend(await _collect_file_blobs(db, row["id"]))
        elif row["stored_name"]:
            result.append(row["stored_name"])
    return result


async def _collect_item_ids(db: aiosqlite.Connection, folder_id: int) -> list[int]:
    cur = await db.execute("SELECT id, kind FROM items WHERE parent_id=?", (folder_id,))
    rows = await cur.fetchall()
    result = [folder_id]
    for row in rows:
        row = dict(row)
        if row["kind"] == "folder":
            result.extend(await _collect_item_ids(db, row["id"]))
        else:
            result.append(row["id"])
    return result


@router.get("/items/{item_id}/link")
async def public_link(item_id: int, request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, kind, name, token FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
        if not item or item["kind"] != "file":
            raise HTTPException(404, "Файл не найден")
    return {"url": str(request.url_for("serve_file", token=item["token"], filename=item["name"]))}


@router.post("/items/{item_id}/token")
async def regenerate_token(item_id: int, request: Request):
    await _require_editor(request)
    token = secrets.token_urlsafe(32)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT kind FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
        if not item or item[0] != "file":
            raise HTTPException(404, "Файл не найден")
        await db.execute("UPDATE items SET token=?, updated_at=? WHERE id=?", (token, _now(), item_id))
        await db.commit()
    return {"ok": True}


@router.get("/items/{item_id}/text")
async def read_text_file(item_id: int, request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
    if not item or item["kind"] != "file" or item["ext"] not in TEXT_EXTENSIONS:
        raise HTTPException(404, "Текстовый файл не найден")
    if item["size"] > MAX_TEXT_EDIT_SIZE:
        raise HTTPException(413, "Файл слишком большой для редактора")
    path = _must_blob_dir() / item["stored_name"]
    try:
        return {"content": path.read_text(encoding="utf-8")}
    except UnicodeDecodeError:
        raise HTTPException(400, "Файл не является UTF-8 текстом")


@router.put("/items/{item_id}/text")
async def update_text_file(item_id: int, data: TextFileIn, request: Request):
    await _require_editor(request)
    content = data.content.encode("utf-8")
    if len(content) > MAX_TEXT_EDIT_SIZE:
        raise HTTPException(400, "Текстовый файл слишком большой для редактора")
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
        if not item or item["kind"] != "file" or item["ext"] not in TEXT_EXTENSIONS:
            raise HTTPException(404, "Текстовый файл не найден")
        path = _must_blob_dir() / item["stored_name"]
        async with aiofiles.open(path, "wb") as f:
            await f.write(content)
        await db.execute("UPDATE items SET size=?, updated_at=? WHERE id=?", (len(content), _now(), item_id))
        await db.commit()
    return {"ok": True, "size": len(content)}


@router.get("/f/{token}/{filename}", name="serve_file")
async def serve_file(token: str, filename: str):
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT name, stored_name, mime_type FROM items WHERE token=? AND kind='file'",
            (token,),
        )
        item = await cur.fetchone()
    if not item:
        raise HTTPException(404, "Файл не найден")
    path = _must_blob_dir() / item["stored_name"]
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "Файл не найден")
    headers = {
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": "public, max-age=3600",
    }
    return FileResponse(str(path), media_type=item["mime_type"], headers=headers)

