import mimetypes
import os
import re
import secrets
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import aiofiles
import aiosqlite
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "file-storage"
ROOT_ID = 1
MAX_TEXT_EDIT_SIZE = 1 * 1024 * 1024
MAX_NAME_LEN = 140
UPLOAD_CHUNK_SIZE = 16 * 1024 * 1024

TEXT_EXTENSIONS = {"txt", "md", "csv", "json", "jsonl", "log"}
IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "webp", "bmp", "ico"}
VIDEO_EXTENSIONS = {"mp4", "webm", "mov", "m4v"}
AUDIO_EXTENSIONS = {"mp3", "wav", "ogg", "m4a"}
INLINE_EXTENSIONS = TEXT_EXTENSIONS | IMAGE_EXTENSIONS | VIDEO_EXTENSIONS | AUDIO_EXTENSIONS | {"pdf"}

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
                auth_required INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                UNIQUE(parent_id, name)
            );
            CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent_id, kind, name);
            CREATE INDEX IF NOT EXISTS idx_items_token ON items(token);
        """)
        cur = await db.execute("PRAGMA table_info(items)")
        columns = {row[1] for row in await cur.fetchall()}
        if "auth_required" not in columns:
            await db.execute("ALTER TABLE items ADD COLUMN auth_required INTEGER NOT NULL DEFAULT 0")
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


class FileAccessIn(BaseModel):
    auth_required: bool = False


class UploadSessionIn(BaseModel):
    folder_id: int = ROOT_ID
    name: str
    size: int | None = None


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


def _file_parts(name: str, *, default_text_ext: bool = False) -> tuple[str, str, str]:
    clean = _safe_name(name)
    if "." not in clean and default_text_ext:
        clean = f"{clean}.txt"
    ext = clean.rsplit(".", 1)[1].lower() if "." in clean else ""
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
    if ext in {"zip", "rar", "7z", "tar", "gz", "bz2", "xz"}:
        return "archive"
    return "file"


def _upload_meta_path(session_id: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{32}", session_id or ""):
        raise HTTPException(400, "Некорректная upload session")
    return _must_tmp_dir() / f"{session_id}.json"


def _upload_tmp_path(session_id: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{32}", session_id or ""):
        raise HTTPException(400, "Некорректная upload session")
    return _must_tmp_dir() / f"{session_id}.part"


async def _read_upload_meta(session_id: str) -> dict:
    path = _upload_meta_path(session_id)
    if not path.exists():
        raise HTTPException(404, "Upload session не найдена")
    try:
        import json
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            return json.loads(await f.read())
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(500, "Upload session повреждена")


async def _write_upload_meta(session_id: str, data: dict) -> None:
    import json
    data["updated_at"] = _now()
    async with aiofiles.open(_upload_meta_path(session_id), "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False, separators=(",", ":")))


async def _require_upload_owner(request: Request, session_id: str) -> tuple[dict, dict]:
    user = await _require_editor(request)
    meta = await _read_upload_meta(session_id)
    if meta.get("username") != user.get("username") and user.get("role") != "admin":
        raise HTTPException(403, "Нет доступа к этой загрузке")
    return user, meta


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
    data["auth_required"] = bool(data.get("auth_required", 0))
    data["is_text_editable"] = data["kind"] == "file" and data["ext"] in TEXT_EXTENSIONS and data["size"] <= MAX_TEXT_EDIT_SIZE
    return data


# Internal module service API. These functions deliberately bypass HTTP auth:
# callers are trusted Nexus modules running in the same process. Keeping blob
# writes here prevents other modules from depending on file-storage SQL details.
async def service_ensure_folder(name: str, parent_id: int = ROOT_ID) -> int:
    clean = _safe_name(name, folder=True)
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, parent_id)
        row = await (await db.execute(
            "SELECT id FROM items WHERE parent_id=? AND name=? AND kind='folder'",
            (parent_id, clean),
        )).fetchone()
        if row:
            return int(row[0])
        try:
            cur = await db.execute(
                "INSERT INTO items(parent_id,kind,name,created_at,updated_at) VALUES(?,'folder',?,?,?)",
                (parent_id, clean, now, now),
            )
            await db.commit()
            return int(cur.lastrowid)
        except aiosqlite.IntegrityError:
            row = await (await db.execute(
                "SELECT id FROM items WHERE parent_id=? AND name=? AND kind='folder'",
                (parent_id, clean),
            )).fetchone()
            if not row:
                raise
            return int(row[0])


async def service_write_file(
    name: str,
    content: bytes,
    *,
    folder_id: int = ROOT_ID,
    mime_type: str | None = None,
    item_id: int | None = None,
) -> int:
    clean, ext, guessed_mime = _file_parts(name)
    payload = bytes(content)
    mime = mime_type or guessed_mime
    now = _now()
    blob_dir = _must_blob_dir()
    blob_dir.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, folder_id)
        if item_id is not None:
            row = await (await db.execute(
                "SELECT stored_name FROM items WHERE id=? AND kind='file'",
                (int(item_id),),
            )).fetchone()
            if not row:
                raise HTTPException(404, "Файл не найден")
            stored_name = row[0] or uuid.uuid4().hex
            tmp_path = blob_dir / f".{stored_name}.{uuid.uuid4().hex}.tmp"
            async with aiofiles.open(tmp_path, "wb") as fh:
                await fh.write(payload)
            os.replace(tmp_path, blob_dir / stored_name)
            try:
                await db.execute(
                    "UPDATE items SET name=?,stored_name=?,ext=?,mime_type=?,size=?,updated_at=? WHERE id=?",
                    (clean, stored_name, ext, mime, len(payload), now, int(item_id)),
                )
                await db.commit()
            except aiosqlite.IntegrityError:
                raise HTTPException(409, "Папка или файл с таким именем уже существует")
            return int(item_id)

        stored_name = uuid.uuid4().hex
        blob_path = blob_dir / stored_name
        async with aiofiles.open(blob_path, "wb") as fh:
            await fh.write(payload)
        try:
            cur = await db.execute(
                "INSERT INTO items(parent_id,kind,name,stored_name,ext,mime_type,size,token,auth_required,created_at,updated_at) "
                "VALUES(?,'file',?,?,?,?,?,?,1,?,?)",
                (folder_id, clean, stored_name, ext, mime, len(payload), secrets.token_urlsafe(32), now, now),
            )
            await db.commit()
            return int(cur.lastrowid)
        except aiosqlite.IntegrityError:
            blob_path.unlink(missing_ok=True)
            raise HTTPException(409, "Папка или файл с таким именем уже существует")


async def service_read_file(item_id: int) -> tuple[dict, bytes]:
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute(
            "SELECT * FROM items WHERE id=? AND kind='file'",
            (int(item_id),),
        )).fetchone()
    if not row or not row["stored_name"]:
        raise HTTPException(404, "Файл не найден")
    blob_path = _must_blob_dir() / row["stored_name"]
    if not blob_path.exists():
        raise HTTPException(404, "Содержимое файла не найдено")
    async with aiofiles.open(blob_path, "rb") as fh:
        return dict(row), await fh.read()


async def service_delete_item(item_id: int) -> None:
    async with aiosqlite.connect(_must_db()) as db:
        row = await (await db.execute(
            "SELECT stored_name FROM items WHERE id=? AND kind='file'",
            (int(item_id),),
        )).fetchone()
        if not row:
            raise HTTPException(404, "Файл не найден")
        await db.execute("DELETE FROM items WHERE id=?", (int(item_id),))
        await db.commit()
    if row[0]:
        (_must_blob_dir() / row[0]).unlink(missing_ok=True)


@router.get("/config")
async def config(request: Request):
    await _require_user(request)
    return {
        "root_id": ROOT_ID,
        "max_file_size": None,
        "max_text_edit_size": MAX_TEXT_EDIT_SIZE,
        "upload_chunk_size": UPLOAD_CHUNK_SIZE,
        "allowed_extensions": [],
        "allow_any_file": True,
    }


@router.get("/folders/{folder_id}/items")
async def list_items(folder_id: int, request: Request):
    await _require_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        await _ensure_folder(db, folder_id)
        cur = await db.execute(
            """
            SELECT id, parent_id, kind, name, ext, mime_type, size, auth_required, created_at, updated_at
            FROM items
            WHERE parent_id=?
            ORDER BY CASE kind WHEN 'folder' THEN 0 ELSE 1 END, lower(name)
            """,
            (folder_id,),
        )
        items = [_item_dict(r) for r in await cur.fetchall()]
        path = await _path_for(db, folder_id)
    return {"folder_id": folder_id, "path": path, "items": items}


@router.get("/path")
async def resolve_path(request: Request, path: str = ""):
    await _require_user(request)
    parts = [part for part in str(path or "").strip("/").split("/") if part]
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        current_id = ROOT_ID
        current = None
        if not parts:
            cur = await db.execute(
                """
                SELECT id, parent_id, kind, name, ext, mime_type, size, auth_required, created_at, updated_at
                FROM items
                WHERE id=?
                """,
                (ROOT_ID,),
            )
            current = await cur.fetchone()
        for raw_name in parts:
            name = _safe_name(raw_name)
            cur = await db.execute(
                """
                SELECT id, parent_id, kind, name, ext, mime_type, size, auth_required, created_at, updated_at
                FROM items
                WHERE parent_id=? AND name=?
                """,
                (current_id, name),
            )
            current = await cur.fetchone()
            if not current:
                raise HTTPException(404, "Путь не найден")
            if raw_name != parts[-1] and current["kind"] != "folder":
                raise HTTPException(404, "Путь не найден")
            current_id = current["id"]
        if not current:
            raise HTTPException(404, "Путь не найден")
        item = _item_dict(current)
        path_chain = await _path_for(db, item["id"])
    return {"item": item, "path": path_chain}


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
    name, ext, mime_type = _file_parts(data.name, default_text_ext=True)
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


@router.post("/upload-sessions", status_code=201)
async def create_upload_session(data: UploadSessionIn, request: Request):
    user = await _require_editor(request)
    name, ext, mime_type = _file_parts(data.name)
    size = int(data.size) if data.size is not None else None
    if size is not None and size < 0:
        raise HTTPException(400, "Некорректный размер файла")
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, data.folder_id)
        cur = await db.execute("SELECT id FROM items WHERE parent_id=? AND name=?", (data.folder_id, name))
        if await cur.fetchone():
            raise HTTPException(409, "Папка или файл с таким именем уже существует")

    session_id = uuid.uuid4().hex
    meta = {
        "id": session_id,
        "folder_id": data.folder_id,
        "name": name,
        "ext": ext,
        "mime_type": mime_type,
        "size": size,
        "username": user.get("username", ""),
        "created_at": _now(),
        "updated_at": _now(),
    }
    await _write_upload_meta(session_id, meta)
    return {**meta, "uploaded": 0, "chunk_size": UPLOAD_CHUNK_SIZE}


@router.get("/upload-sessions/{session_id}")
async def get_upload_session(session_id: str, request: Request):
    await _require_editor(request)
    _, meta = await _require_upload_owner(request, session_id)
    uploaded = _upload_tmp_path(session_id).stat().st_size if _upload_tmp_path(session_id).exists() else 0
    return {**meta, "uploaded": uploaded, "chunk_size": UPLOAD_CHUNK_SIZE}


@router.put("/upload-sessions/{session_id}/chunk")
async def upload_session_chunk(session_id: str, request: Request, offset: int = 0):
    _, meta = await _require_upload_owner(request, session_id)
    tmp_path = _upload_tmp_path(session_id)
    current = tmp_path.stat().st_size if tmp_path.exists() else 0
    if offset != current:
        return {"ok": False, "uploaded": current, "expected_offset": current}

    total = meta.get("size")
    async with aiofiles.open(tmp_path, "ab") as out:
        async for chunk in request.stream():
            if not chunk:
                continue
            current += len(chunk)
            if total is not None and current > int(total):
                raise HTTPException(413, "Получено больше байт, чем заявлено")
            await out.write(chunk)
    await _write_upload_meta(session_id, meta)
    return {"ok": True, "uploaded": current, "complete": total is not None and current >= int(total)}


@router.post("/upload-sessions/{session_id}/complete", status_code=201)
async def complete_upload_session(session_id: str, request: Request):
    _, meta = await _require_upload_owner(request, session_id)
    tmp_path = _upload_tmp_path(session_id)
    if not tmp_path.exists():
        raise HTTPException(400, "Файл еще не загружен")
    size = tmp_path.stat().st_size
    expected = meta.get("size")
    if expected is not None and size != int(expected):
        raise HTTPException(400, f"Загружено {size} из {expected} байт")

    stored_name = uuid.uuid4().hex
    blob_path = _must_blob_dir() / stored_name
    shutil.move(str(tmp_path), str(blob_path))

    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        await _ensure_folder(db, int(meta["folder_id"]))
        try:
            cur = await db.execute(
                """
                INSERT INTO items(parent_id, kind, name, stored_name, ext, mime_type, size, token, created_at, updated_at)
                VALUES (?, 'file', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(meta["folder_id"]),
                    meta["name"],
                    stored_name,
                    meta.get("ext", ""),
                    meta.get("mime_type", "application/octet-stream"),
                    size,
                    secrets.token_urlsafe(32),
                    now,
                    now,
                ),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            blob_path.unlink(missing_ok=True)
            raise HTTPException(409, "Папка или файл с таким именем уже существует")
        finally:
            _upload_meta_path(session_id).unlink(missing_ok=True)
    return {"id": cur.lastrowid, "name": meta["name"], "kind": "file", "size": size}


@router.delete("/upload-sessions/{session_id}")
async def cancel_upload_session(session_id: str, request: Request):
    await _require_upload_owner(request, session_id)
    _upload_tmp_path(session_id).unlink(missing_ok=True)
    _upload_meta_path(session_id).unlink(missing_ok=True)
    return {"ok": True}


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


@router.put("/items/{item_id}/access")
async def update_file_access(item_id: int, data: FileAccessIn, request: Request):
    await _require_editor(request)
    auth_required = 1 if data.auth_required else 0
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT kind FROM items WHERE id=?", (item_id,))
        item = await cur.fetchone()
        if not item or item[0] != "file":
            raise HTTPException(404, "Файл не найден")
        await db.execute(
            "UPDATE items SET auth_required=?, updated_at=? WHERE id=?",
            (auth_required, _now(), item_id),
        )
        await db.commit()
    return {"ok": True, "auth_required": bool(auth_required)}


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
async def serve_file(token: str, filename: str, request: Request):
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT name, stored_name, ext, mime_type, auth_required FROM items WHERE token=? AND kind='file'",
            (token,),
        )
        item = await cur.fetchone()
    if not item:
        raise HTTPException(404, "Файл не найден")
    if item["auth_required"]:
        user = await verify_token_from_request(request)
        if not user or not can_access_module(user, MODULE_ID):
            raise HTTPException(401, "unauthorized")
    path = _must_blob_dir() / item["stored_name"]
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "Файл не найден")
    headers = {
        "X-Content-Type-Options": "nosniff",
        "Cache-Control": "private, no-store" if item["auth_required"] else "public, max-age=3600",
    }
    ext = str(item["ext"] or "").lower()
    media_type = item["mime_type"] if ext in INLINE_EXTENSIONS else "application/octet-stream"
    if ext not in INLINE_EXTENSIONS:
        headers["Content-Disposition"] = f"attachment; filename*=UTF-8''{quote(item['name'])}"
    return FileResponse(str(path), media_type=media_type, headers=headers)
