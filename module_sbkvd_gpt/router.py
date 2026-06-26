from __future__ import annotations

import asyncio
import json
import secrets
import sys
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from orchestrator.auth import require_admin, verify_token_from_request

router = APIRouter()

MODULE_ID = "sbkvd-gpt"
SESSION_COOKIE = "sbkvd_gpt_session"
SESSION_TTL_DAYS = 30
DEFAULT_MODEL = "openai/gpt-4.1-mini"
DEFAULT_HISTORY_LIMIT = 80

_db_path: Path | None = None
_module_dir: Path | None = None
_logger = None

LEGACY_ACCOUNTS = [
    "Никита Попов",
    "Кристина Рыжкова",
    "Дима Кошурин",
    "Татьяна Воробьева",
    "Евгений Норкин",
    "Артем Зайцев",
    "Наталья Абрамова",
    "Андрей Каракчиев",
]

DEFAULT_MODEL_GRANTS = [
    "openai/gpt-4.1-mini",
    "openai/gpt-4.1",
    "openai/gpt-5.1",
    "openai/gpt-5.2",
]

PROMPT_ALIASES = {
    "avito_gpt1.txt": ("Агенты бота", "Авито"),
    "dog_gpt2.txt": ("Агенты бота", "Собака. Перед анонсом"),
    "dog_gpt3.txt": ("Агенты бота", "Собака. Перед эфиром"),
    "dog_gpt5.txt": ("Агенты бота", "Собака. Агент дожима"),
    "puppy_gpt2.txt": ("Агенты бота", "Щенок. Перед анонсом"),
    "puppy_gpt3.txt": ("Агенты бота", "Щенок. Перед эфиром"),
    "puppy_gpt5.txt": ("Агенты бота", "Щенок. Дожим 1"),
    "puppy_gpt6.txt": ("Агенты бота", "Щенок. Дожим 2"),
    "site_gpt1.txt": ("Агенты бота", "Сайт"),
    "dog_gpt4-2.txt": ("Агенты эфиров", "Собака. ОП-агент"),
    "dog_gpt4.txt": ("Агенты эфиров", "Собака. Кинолог"),
    "puppy_gpt4-2.txt": ("Агенты эфиров", "Щенок. ОП-агент"),
    "puppy_gpt4.txt": ("Агенты эфиров", "Щенок. Кинолог"),
    "INSTAGRAM_DIRECT.txt": ("Переписка", "Инстаграм"),
    "VK_soobcheniya_rega.txt": ("Переписка", "ВКонтакте"),
    "YOUTUBE_kommenty.txt": ("Переписка", "Ютуб комментарии"),
}


class LoginIn(BaseModel):
    login: str


class AccountIn(BaseModel):
    login: str
    display_name: str = ""
    active: bool = True
    default_prompt: str = ""
    default_model: str = ""


class AccessIn(BaseModel):
    prompt_paths: list[str] = []
    models: list[str] = []
    default_prompt: str = ""
    default_model: str = ""
    active: bool | None = None


class AliasIn(BaseModel):
    prompt_path: str
    title: str = ""
    folder: str = ""


class MessageIn(BaseModel):
    message: str
    prompt_path: str
    model: str
    keep_context: bool = True
    thread_id: str | None = ""
    attachment_url: str = ""


class RegenerateIn(BaseModel):
    thread_id: str
    prompt_path: str
    model: str


def setup(ctx):
    global _db_path, _module_dir, _logger
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _logger = getattr(ctx, "logger", None)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


def _log(level: str, message: str, *args: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("sbkvd-gpt module is not initialized")
    return _db_path


def _must_module_dir() -> Path:
    if _module_dir is None:
        raise RuntimeError("sbkvd-gpt module is not initialized")
    return _module_dir


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _session_expires() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_thread_id() -> str:
    return "sgpt_" + uuid.uuid4().hex


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _normalize_login(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "").strip()).lower().replace("ё", "е")
    return " ".join(raw.split())


def _cookie_path(request: Request) -> str:
    root_path = request.scope.get("root_path", "") or ""
    return f"{root_path}/{MODULE_ID}".replace("//", "/")


def _file_storage_db_path() -> Path:
    return _must_module_dir().parent / "file-storage" / "data" / "file-storage.db"


def _file_storage_blob_dir() -> Path:
    return _must_module_dir().parent / "file-storage" / "data" / "blobs"


def _openrouter_db_path() -> Path:
    return _must_module_dir().parent / "openrouter" / "data" / "openrouter.db"


def _openrouter_module():
    mod = sys.modules.get("_nexus_mod_openrouter")
    if not mod or not hasattr(mod, "generate_direct_chat"):
        raise HTTPException(503, "OpenRouter module is not active or is too old")
    return mod


async def _init_db():
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                login_key TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                default_prompt TEXT NOT NULL DEFAULT '',
                default_model TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                account_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_prompts (
                account_id INTEGER NOT NULL,
                prompt_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(account_id, prompt_path)
            );
            CREATE TABLE IF NOT EXISTS account_models (
                account_id INTEGER NOT NULL,
                model TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(account_id, model)
            );
            CREATE TABLE IF NOT EXISTS prompt_aliases (
                prompt_path TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                folder TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS threads (
                id TEXT PRIMARY KEY,
                account_id INTEGER NOT NULL,
                title TEXT NOT NULL DEFAULT 'Новая беседа',
                prompt_path TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL DEFAULT '',
                prompt_path TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                usage_json TEXT NOT NULL DEFAULT '{}',
                attachment_url TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_threads_account ON threads(account_id, updated_at);
            CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id, id);
        """)
        await db.commit()
    await _seed_defaults()
    _log("info", "sbkvd-gpt DB initialized")


async def _list_file_prompts() -> list[dict[str, Any]]:
    db_path = _file_storage_db_path()
    if not db_path.exists():
        return []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id,parent_id,kind,name,ext,size,updated_at FROM items")
        rows = [dict(r) for r in await cur.fetchall()]
    by_parent: dict[int | None, list[dict[str, Any]]] = {}
    for row in rows:
        by_parent.setdefault(row["parent_id"], []).append(row)
    result: list[dict[str, Any]] = []

    def walk(folder_id: int, prefix: list[str]) -> None:
        for item in sorted(by_parent.get(folder_id, []), key=lambda x: (x["kind"] != "folder", x["name"].lower())):
            if item["kind"] == "folder":
                walk(item["id"], [*prefix, item["name"]])
            elif item.get("ext") == "txt":
                result.append({
                    "path": "/".join([*prefix, item["name"]]),
                    "name": item["name"],
                    "size": int(item.get("size") or 0),
                    "updated_at": item.get("updated_at") or "",
                })

    walk(1, [])
    return result


def _decorate_prompt(prompt: dict[str, Any], alias: dict[str, str] | None = None) -> dict[str, Any]:
    alias = alias or {}
    title = alias.get("title") or prompt["name"].removesuffix(".txt")
    folder = alias.get("folder") or ""
    return {
        **prompt,
        "title": title,
        "folder": folder,
        "label": f"{title} ({prompt['name']})" if title and title != prompt["name"].removesuffix(".txt") else prompt["name"],
    }


async def _prompt_aliases(db: aiosqlite.Connection) -> dict[str, dict[str, str]]:
    cur = await db.execute("SELECT prompt_path,title,folder FROM prompt_aliases")
    return {row[0]: {"title": row[1] or "", "folder": row[2] or ""} for row in await cur.fetchall()}


async def _seed_defaults():
    now = _now()
    prompts = await _list_file_prompts()
    prompt_paths = [p["path"] for p in prompts]
    async with aiosqlite.connect(_must_db()) as db:
        for path in prompt_paths:
            filename = path.rsplit("/", 1)[-1]
            if filename in PROMPT_ALIASES:
                folder, title = PROMPT_ALIASES[filename]
                await db.execute(
                    "INSERT OR IGNORE INTO prompt_aliases(prompt_path,title,folder,updated_at) VALUES(?,?,?,?)",
                    (path, title, folder, now),
                )
        for login in LEGACY_ACCOUNTS:
            login_key = _normalize_login(login)
            cur = await db.execute("SELECT id FROM accounts WHERE login_key=?", (login_key,))
            row = await cur.fetchone()
            created = False
            if row:
                account_id = int(row[0])
            else:
                cur = await db.execute(
                    """
                    INSERT INTO accounts(login,login_key,display_name,active,default_prompt,default_model,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (login, login_key, login, 1, prompt_paths[0] if prompt_paths else "", DEFAULT_MODEL, now, now),
                )
                account_id = int(cur.lastrowid)
                created = True
            if created:
                for path in prompt_paths:
                    await db.execute(
                        "INSERT OR IGNORE INTO account_prompts(account_id,prompt_path,created_at) VALUES(?,?,?)",
                        (account_id, path, now),
                    )
                for model in DEFAULT_MODEL_GRANTS:
                    await db.execute(
                        "INSERT OR IGNORE INTO account_models(account_id,model,created_at) VALUES(?,?,?)",
                        (account_id, model, now),
                    )
        await db.commit()


async def _require_gpt_account(request: Request) -> dict[str, Any]:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(401, "unauthorized")
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT a.*
            FROM sessions s
            JOIN accounts a ON a.id=s.account_id
            WHERE s.token=? AND s.expires_at>? AND a.active=1
            """,
            (token, now),
        )
        row = await cur.fetchone()
    if not row:
        raise HTTPException(401, "unauthorized")
    return dict(row)


async def _require_nexus_admin(request: Request) -> dict[str, Any]:
    user = await verify_token_from_request(request)
    if not require_admin(user):
        raise HTTPException(403, "admin required")
    return user


async def _account_prompt_set(db: aiosqlite.Connection, account_id: int) -> set[str]:
    cur = await db.execute("SELECT prompt_path FROM account_prompts WHERE account_id=?", (account_id,))
    return {row[0] for row in await cur.fetchall()}


async def _account_model_set(db: aiosqlite.Connection, account_id: int) -> set[str]:
    cur = await db.execute("SELECT model FROM account_models WHERE account_id=?", (account_id,))
    return {row[0] for row in await cur.fetchall()}


async def _ensure_prompt_access(account_id: int, prompt_path: str) -> str:
    prompt_path = _clean(prompt_path, 500)
    known = {p["path"] for p in await _list_file_prompts()}
    if prompt_path not in known:
        raise HTTPException(400, "prompt not found")
    async with aiosqlite.connect(_must_db()) as db:
        allowed = await _account_prompt_set(db, account_id)
    if prompt_path not in allowed:
        raise HTTPException(403, "prompt is not allowed")
    return prompt_path


async def _ensure_model_access(account_id: int, model: str) -> str:
    model = _clean(model, 200)
    if not model:
        raise HTTPException(400, "model is required")
    async with aiosqlite.connect(_must_db()) as db:
        allowed = await _account_model_set(db, account_id)
    if model not in allowed:
        raise HTTPException(403, "model is not allowed")
    return model


async def _service_models() -> list[dict[str, str]]:
    ids: set[str] = set(DEFAULT_MODEL_GRANTS)
    db_path = _openrouter_db_path()
    if db_path.exists():
        async with aiosqlite.connect(db_path) as db:
            try:
                cur = await db.execute("SELECT models_json FROM model_cache WHERE id=1")
                row = await cur.fetchone()
                for item in json.loads(row[0] if row else "[]"):
                    model_id = str(item.get("id") or "").strip()
                    if model_id:
                        ids.add(model_id)
            except Exception:
                pass
            try:
                cur = await db.execute("SELECT value FROM settings WHERE key IN ('default_model','summary_model')")
                ids.update(str(row[0]).strip() for row in await cur.fetchall() if str(row[0]).strip())
            except Exception:
                pass
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT model FROM account_models")
        ids.update(str(row[0]).strip() for row in await cur.fetchall() if str(row[0]).strip())
    return [{"id": model, "name": model} for model in sorted(ids)]


def _short_title(text: str) -> str:
    words = [part for part in " ".join(_clean(text, 200).replace("\n", " ").split()).split(" ") if part]
    title = " ".join(words[:5]).strip(".,!?;:")
    return title or "Новая беседа"


async def _thread_history(db: aiosqlite.Connection, account_id: int, thread_id: str, limit: int = DEFAULT_HISTORY_LIMIT) -> list[dict[str, Any]]:
    cur = await db.execute(
        """
        SELECT role,content,created_at,attachment_url
        FROM messages
        WHERE thread_id=? AND account_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (thread_id, account_id, limit),
    )
    rows = list(reversed(await cur.fetchall()))
    return [
        {"role": row[0], "content": row[1], "created_at": row[2], "attachment_url": row[3] or ""}
        for row in rows
        if row[0] in {"user", "assistant"} and row[1]
    ]


async def _get_or_create_thread(db: aiosqlite.Connection, account_id: int, thread_id: str, prompt_path: str, model: str, message: str) -> str:
    now = _now()
    clean_id = _clean(thread_id, 80)
    if clean_id:
        cur = await db.execute("SELECT id FROM threads WHERE id=? AND account_id=?", (clean_id, account_id))
        if not await cur.fetchone():
            raise HTTPException(404, "thread not found")
        await db.execute("UPDATE threads SET updated_at=?, prompt_path=?, model=? WHERE id=?", (now, prompt_path, model, clean_id))
        return clean_id
    clean_id = _new_thread_id()
    await db.execute(
        "INSERT INTO threads(id,account_id,title,prompt_path,model,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
        (clean_id, account_id, _short_title(message), prompt_path, model, now, now),
    )
    return clean_id


async def _save_message(
    db: aiosqlite.Connection,
    *,
    thread_id: str,
    account_id: int,
    role: str,
    content: str,
    prompt_path: str,
    model: str,
    usage: dict[str, Any] | None = None,
    attachment_url: str = "",
) -> None:
    await db.execute(
        """
        INSERT INTO messages(thread_id,account_id,role,content,prompt_path,model,usage_json,attachment_url,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (thread_id, account_id, role, content, prompt_path, model, json.dumps(usage or {}, ensure_ascii=False), attachment_url, _now()),
    )
    await db.execute("UPDATE threads SET updated_at=?, prompt_path=?, model=? WHERE id=?", (_now(), prompt_path, model, thread_id))


def _history_for_model(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [{"role": item["role"], "content": item["content"]} for item in items if item["role"] in {"user", "assistant"}]


@router.post("/login")
async def login(data: LoginIn, request: Request):
    login_key = _normalize_login(data.login)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM accounts WHERE login_key=? AND active=1", (login_key,))
        account = await cur.fetchone()
        if not account:
            raise HTTPException(401, "Логин не найден в списке доступа")
        token = secrets.token_urlsafe(40)
        await db.execute(
            "INSERT INTO sessions(token,account_id,expires_at,created_at) VALUES(?,?,?,?)",
            (token, account["id"], _session_expires(), _now()),
        )
        await db.commit()
    response = JSONResponse({"ok": True, "login": account["login"], "display_name": account["display_name"] or account["login"]})
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        samesite="lax",
        max_age=SESSION_TTL_DAYS * 24 * 3600,
        path=_cookie_path(request),
    )
    return response


@router.post("/logout")
async def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        async with aiosqlite.connect(_must_db()) as db:
            await db.execute("DELETE FROM sessions WHERE token=?", (token,))
            await db.commit()
    response = JSONResponse({"ok": True})
    response.delete_cookie(SESSION_COOKIE, path=_cookie_path(request))
    return response


@router.get("/me")
async def me(request: Request):
    admin_user = await verify_token_from_request(request)
    account = None
    try:
        account = await _require_gpt_account(request)
    except HTTPException:
        pass
    return {
        "authenticated": bool(account),
        "account": account and {
            "id": account["id"],
            "login": account["login"],
            "display_name": account["display_name"] or account["login"],
            "default_prompt": account["default_prompt"],
            "default_model": account["default_model"],
        },
        "nexus_admin": require_admin(admin_user),
        "nexus_user": admin_user and {"username": admin_user.get("username"), "role": admin_user.get("role")},
    }


@router.get("/prompts")
async def prompts(request: Request):
    account = await _require_gpt_account(request)
    all_prompts = await _list_file_prompts()
    async with aiosqlite.connect(_must_db()) as db:
        aliases = await _prompt_aliases(db)
        allowed = await _account_prompt_set(db, int(account["id"]))
    items = [_decorate_prompt(p, aliases.get(p["path"])) for p in all_prompts if p["path"] in allowed]
    return {"items": items, "default_prompt": account["default_prompt"]}


@router.get("/models")
async def models(request: Request):
    account = await _require_gpt_account(request)
    async with aiosqlite.connect(_must_db()) as db:
        allowed = await _account_model_set(db, int(account["id"]))
    service = await _service_models()
    by_id = {item["id"]: item for item in service}
    items = [by_id.get(model, {"id": model, "name": model}) for model in sorted(allowed)]
    return {"items": items, "default_model": account["default_model"] or DEFAULT_MODEL}


@router.get("/threads")
async def threads(request: Request):
    account = await _require_gpt_account(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT t.*, COUNT(m.id) AS messages
            FROM threads t
            LEFT JOIN messages m ON m.thread_id=t.id
            WHERE t.account_id=?
            GROUP BY t.id
            ORDER BY t.updated_at DESC
            LIMIT 200
            """,
            (account["id"],),
        )
        rows = [dict(r) for r in await cur.fetchall()]
    return {"items": rows}


@router.get("/threads/{thread_id}")
async def thread_detail(thread_id: str, request: Request):
    account = await _require_gpt_account(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM threads WHERE id=? AND account_id=?", (thread_id, account["id"]))
        thread = await cur.fetchone()
        if not thread:
            raise HTTPException(404, "thread not found")
        history = await _thread_history(db, int(account["id"]), thread_id, 500)
    return {"thread": dict(thread), "messages": history}


@router.post("/message")
async def message(data: MessageIn, request: Request):
    account = await _require_gpt_account(request)
    account_id = int(account["id"])
    text = _clean(data.message, 50000)
    if not text:
        raise HTTPException(400, "message is required")
    prompt_path = await _ensure_prompt_access(account_id, data.prompt_path)
    model = await _ensure_model_access(account_id, data.model)
    attachment_url = _clean(data.attachment_url, 2_500_000)
    history: list[dict[str, Any]] = []
    thread_id = ""
    if data.keep_context:
        async with aiosqlite.connect(_must_db()) as db:
            thread_id = await _get_or_create_thread(db, account_id, data.thread_id, prompt_path, model, text)
            history = await _thread_history(db, account_id, thread_id, DEFAULT_HISTORY_LIMIT)
            await db.commit()
    result = await _openrouter_module().generate_direct_chat(
        prompt=prompt_path,
        message=text,
        model=model,
        history=_history_for_model(history),
        attachment_url=attachment_url,
    )
    if data.keep_context:
        async with aiosqlite.connect(_must_db()) as db:
            await _save_message(
                db,
                thread_id=thread_id,
                account_id=account_id,
                role="user",
                content=text,
                prompt_path=prompt_path,
                model=model,
                attachment_url=attachment_url,
            )
            await _save_message(
                db,
                thread_id=thread_id,
                account_id=account_id,
                role="assistant",
                content=result["answer"],
                prompt_path=prompt_path,
                model=result["model"],
                usage=result.get("usage") or {},
            )
            await db.commit()
    return {
        "ok": True,
        "thread_id": thread_id,
        "answer": result["answer"],
        "message": result["answer"],
        "usage": result.get("usage") or {},
        "model": result.get("model") or model,
        "mode": "thread" if data.keep_context else "stateless",
    }


@router.post("/regenerate")
async def regenerate(data: RegenerateIn, request: Request):
    account = await _require_gpt_account(request)
    account_id = int(account["id"])
    thread_id = _clean(data.thread_id, 80)
    prompt_path = await _ensure_prompt_access(account_id, data.prompt_path)
    model = await _ensure_model_access(account_id, data.model)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM threads WHERE id=? AND account_id=?", (thread_id, account_id))
        if not await cur.fetchone():
            raise HTTPException(404, "thread not found")
        cur = await db.execute(
            "SELECT * FROM messages WHERE thread_id=? AND account_id=? ORDER BY id ASC",
            (thread_id, account_id),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        last_user_idx = max((idx for idx, row in enumerate(rows) if row["role"] == "user"), default=-1)
        if last_user_idx < 0:
            raise HTTPException(400, "thread has no user message")
        last_user = rows[last_user_idx]
        assistant_after = [row for row in rows[last_user_idx + 1:] if row["role"] == "assistant"]
        for row in assistant_after:
            await db.execute("DELETE FROM messages WHERE id=?", (row["id"],))
        history = [
            {"role": row["role"], "content": row["content"]}
            for row in rows[:last_user_idx]
            if row["role"] in {"user", "assistant"} and row["content"]
        ][-DEFAULT_HISTORY_LIMIT:]
        await db.commit()
    result = await _openrouter_module().generate_direct_chat(
        prompt=prompt_path,
        message=last_user["content"],
        model=model,
        history=history,
        attachment_url=last_user.get("attachment_url") or "",
    )
    async with aiosqlite.connect(_must_db()) as db:
        await _save_message(
            db,
            thread_id=thread_id,
            account_id=account_id,
            role="assistant",
            content=result["answer"],
            prompt_path=prompt_path,
            model=result["model"],
            usage=result.get("usage") or {},
        )
        await db.commit()
    return {"ok": True, "thread_id": thread_id, "answer": result["answer"], "message": result["answer"], "usage": result.get("usage") or {}, "model": result.get("model") or model}


@router.get("/admin/me")
async def admin_me(request: Request):
    user = await _require_nexus_admin(request)
    return {"ok": True, "username": user.get("username"), "role": user.get("role")}


@router.get("/admin/accounts")
async def admin_accounts(request: Request):
    await _require_nexus_admin(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT a.*,
                   COALESCE(p.cnt,0) AS prompt_count,
                   COALESCE(m.cnt,0) AS model_count
            FROM accounts a
            LEFT JOIN (SELECT account_id, COUNT(*) cnt FROM account_prompts GROUP BY account_id) p ON p.account_id=a.id
            LEFT JOIN (SELECT account_id, COUNT(*) cnt FROM account_models GROUP BY account_id) m ON m.account_id=a.id
            ORDER BY a.login
            """
        )
        rows = [dict(r) for r in await cur.fetchall()]
    return {"items": rows}


@router.post("/admin/accounts")
async def admin_create_account(data: AccountIn, request: Request):
    await _require_nexus_admin(request)
    login = _clean(data.login, 160)
    if not login:
        raise HTTPException(400, "login is required")
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        try:
            cur = await db.execute(
                """
                INSERT INTO accounts(login,login_key,display_name,active,default_prompt,default_model,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (login, _normalize_login(login), _clean(data.display_name, 160) or login, int(data.active), _clean(data.default_prompt, 500), _clean(data.default_model, 200), now, now),
            )
            await db.commit()
        except aiosqlite.IntegrityError as exc:
            raise HTTPException(400, "account already exists") from exc
    return {"ok": True, "id": cur.lastrowid}


@router.put("/admin/accounts/{account_id}")
async def admin_update_account(account_id: int, data: AccountIn, request: Request):
    await _require_nexus_admin(request)
    login = _clean(data.login, 160)
    if not login:
        raise HTTPException(400, "login is required")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            UPDATE accounts
            SET login=?, login_key=?, display_name=?, active=?, default_prompt=?, default_model=?, updated_at=?
            WHERE id=?
            """,
            (login, _normalize_login(login), _clean(data.display_name, 160) or login, int(data.active), _clean(data.default_prompt, 500), _clean(data.default_model, 200), _now(), account_id),
        )
        await db.commit()
    return {"ok": True}


@router.delete("/admin/accounts/{account_id}")
async def admin_delete_account(account_id: int, request: Request):
    await _require_nexus_admin(request)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute("DELETE FROM sessions WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM account_prompts WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM account_models WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM messages WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM threads WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM accounts WHERE id=?", (account_id,))
        await db.commit()
    return {"ok": True}


@router.get("/admin/prompts")
async def admin_prompts(request: Request):
    await _require_nexus_admin(request)
    all_prompts = await _list_file_prompts()
    async with aiosqlite.connect(_must_db()) as db:
        aliases = await _prompt_aliases(db)
    return {"items": [_decorate_prompt(p, aliases.get(p["path"])) for p in all_prompts]}


@router.put("/admin/prompt-aliases")
async def admin_prompt_alias(data: AliasIn, request: Request):
    await _require_nexus_admin(request)
    prompt_path = _clean(data.prompt_path, 500)
    if prompt_path not in {p["path"] for p in await _list_file_prompts()}:
        raise HTTPException(404, "prompt not found")
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            """
            INSERT INTO prompt_aliases(prompt_path,title,folder,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(prompt_path) DO UPDATE SET title=excluded.title, folder=excluded.folder, updated_at=excluded.updated_at
            """,
            (prompt_path, _clean(data.title, 160), _clean(data.folder, 160), _now()),
        )
        await db.commit()
    return {"ok": True}


@router.get("/admin/models")
async def admin_models(request: Request):
    await _require_nexus_admin(request)
    return {"items": await _service_models()}


@router.get("/admin/accounts/{account_id}/access")
async def admin_account_access(account_id: int, request: Request):
    await _require_nexus_admin(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM accounts WHERE id=?", (account_id,))
        account = await cur.fetchone()
        if not account:
            raise HTTPException(404, "account not found")
        prompts = sorted(await _account_prompt_set(db, account_id))
        models = sorted(await _account_model_set(db, account_id))
    return {"account": dict(account), "prompt_paths": prompts, "models": models}


@router.put("/admin/accounts/{account_id}/access")
async def admin_update_access(account_id: int, data: AccessIn, request: Request):
    await _require_nexus_admin(request)
    valid_prompts = {p["path"] for p in await _list_file_prompts()}
    prompt_paths = [p for p in dict.fromkeys(_clean(p, 500) for p in data.prompt_paths) if p in valid_prompts]
    models = [m for m in dict.fromkeys(_clean(m, 200) for m in data.models) if m]
    now = _now()
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT id FROM accounts WHERE id=?", (account_id,))
        if not await cur.fetchone():
            raise HTTPException(404, "account not found")
        await db.execute("DELETE FROM account_prompts WHERE account_id=?", (account_id,))
        await db.execute("DELETE FROM account_models WHERE account_id=?", (account_id,))
        for path in prompt_paths:
            await db.execute("INSERT INTO account_prompts(account_id,prompt_path,created_at) VALUES(?,?,?)", (account_id, path, now))
        for model in models:
            await db.execute("INSERT INTO account_models(account_id,model,created_at) VALUES(?,?,?)", (account_id, model, now))
        updates = ["default_prompt=?", "default_model=?", "updated_at=?"]
        params: list[Any] = [_clean(data.default_prompt, 500), _clean(data.default_model, 200), now]
        if data.active is not None:
            updates.insert(0, "active=?")
            params.insert(0, int(data.active))
        params.append(account_id)
        await db.execute(f"UPDATE accounts SET {', '.join(updates)} WHERE id=?", params)
        await db.commit()
    return {"ok": True, "prompt_paths": prompt_paths, "models": models}
