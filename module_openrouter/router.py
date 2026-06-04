from __future__ import annotations

import asyncio
import json
import os
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ValidationError, field_validator

from orchestrator.auth import ENV_PATH, _read_env_values, _write_env_values
from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()

MODULE_ID = "openrouter"
OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
DEFAULT_MODEL = "openai/gpt-4.1-mini"
DEFAULT_TIMEOUT = 90
MAX_HISTORY_MESSAGES = 80
SUMMARY_MAX_CHARS = 1800
MODULE_TOKEN_SETTING = "module_api_token"
LEGACY_SUMMARY_PROMPT = (
    "Сделай краткую сводку диалога с клиентом на русском языке. "
    "Сохрани факты о клиенте, собаке, проблемах, уже данных советах и текущем состоянии. "
    "Пиши структурно, без воды, не больше 10 пунктов."
)
SALES_SUMMARY_PROMPT = """Сделай краткую сводку диалога для отдела продаж на русском языке.
Цель сводки: менеджер должен за 20-30 секунд понять клиента и продолжить продажу без перечитывания всей переписки.

Пиши коротко, прикладно, без воды и без художественного пересказа. Если данных нет, пиши "нет данных".

Структура:
1. Статус лида: холодный / теплый / горячий и почему.
2. Кто клиент: имя, город, роль в покупке, важные личные детали.
3. Собака / ситуация: порода, возраст, проблема, срочность, контекст.
4. Главная боль клиента: что его реально беспокоит и какой результат он хочет.
5. Интерес к продукту: что заинтересовало, на что реагирует, какие форматы/услуги подходят.
6. Возражения и риски: цена, время, доверие, сомнения, негативный опыт, ограничения.
7. Что уже сказали/обещали: важные ответы, договоренности, упомянутые условия.
8. Следующий лучший шаг: что менеджеру сделать или спросить дальше одной конкретной фразой.
9. Тон общения: как с этим клиентом лучше говорить, что не давить, на чем сделать акцент.

Не придумывай факты. Не ставь диагнозы. Не добавляй внутренние рассуждения модели."""

_ctx = None
_db_path: Path | None = None
_module_dir: Path | None = None
_logger = None


def setup(ctx):
    global _ctx, _db_path, _module_dir, _logger
    _ctx = ctx
    _db_path = ctx.db_path
    _module_dir = ctx.module_dir
    _logger = getattr(ctx, "logger", None)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


def _log(level: str, message: str, *args: Any, **kwargs: Any) -> None:
    if _logger:
        getattr(_logger, level, _logger.info)(message, *args, **kwargs)


def _must_db() -> Path:
    if _db_path is None:
        raise RuntimeError("openrouter module is not initialized")
    return _db_path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_conversation_id() -> str:
    return "or_conv_" + uuid.uuid4().hex


def _new_pair_id() -> str:
    return "pair_" + uuid.uuid4().hex


def _clean(value: Any, limit: int = 10000) -> str:
    return str(value or "").strip()[:limit]


def _validation_detail(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", [])) or "body"
        parts.append(f"{loc}: {err.get('msg', 'invalid')}")
    return "; ".join(parts)[:500] or "invalid body"


def _coerce_text_input(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    raise ValueError("must be a string or number")


def _env() -> dict[str, str]:
    return {
        "openrouter_key": os.environ.get("OPENROUTER_API_KEY", "").strip(),
        "api_token": os.environ.get("NEXUS_OPENROUTER_API_TOKEN", "").strip(),
    }


async def _init_db():
    async with aiosqlite.connect(_must_db()) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS users (
                platform_id       TEXT PRIMARY KEY,
                summary           TEXT NOT NULL DEFAULT '',
                total_tokens_used INTEGER NOT NULL DEFAULT 0,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id TEXT PRIMARY KEY,
                platform_id     TEXT NOT NULL,
                active          INTEGER NOT NULL DEFAULT 1,
                prompt_path     TEXT NOT NULL DEFAULT '',
                model           TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                FOREIGN KEY(platform_id) REFERENCES users(platform_id)
            );
            CREATE INDEX IF NOT EXISTS idx_conversations_platform ON conversations(platform_id, updated_at);
            CREATE TABLE IF NOT EXISTS messages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                platform_id     TEXT NOT NULL,
                pair_id         TEXT NOT NULL,
                role            TEXT NOT NULL,
                content         TEXT NOT NULL DEFAULT '',
                source          TEXT NOT NULL DEFAULT 'api',
                prompt_path     TEXT NOT NULL DEFAULT '',
                model           TEXT NOT NULL DEFAULT '',
                usage_json      TEXT NOT NULL DEFAULT '{}',
                created_at      TEXT NOT NULL,
                FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, id);
            CREATE INDEX IF NOT EXISTS idx_messages_platform ON messages(platform_id, id);
            CREATE TABLE IF NOT EXISTS prompt_models (
                prompt_path TEXT PRIMARY KEY,
                model       TEXT NOT NULL DEFAULT '',
                updated_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS model_cache (
                id         INTEGER PRIMARY KEY CHECK(id = 1),
                models_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL DEFAULT ''
            );
        """)
        defaults = {
            "default_model": DEFAULT_MODEL,
            "summary_model": DEFAULT_MODEL,
            "request_timeout": str(DEFAULT_TIMEOUT),
            "history_limit": str(MAX_HISTORY_MESSAGES),
            "summary_prompt": SALES_SUMMARY_PROMPT,
        }
        for key, value in defaults.items():
            await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        await db.execute(
            "UPDATE settings SET value=? WHERE key='summary_prompt' AND value=?",
            (SALES_SUMMARY_PROMPT, LEGACY_SUMMARY_PROMPT),
        )
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (MODULE_TOKEN_SETTING,))
        row = await cur.fetchone()
        if not row or not row[0]:
            module_token = _env()["api_token"] or secrets.token_urlsafe(40)
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (MODULE_TOKEN_SETTING, module_token),
            )
        await db.commit()
    _log("info", "openrouter DB initialized")


class TextInputMixin(BaseModel):
    @field_validator(
        "platform_id",
        "conversation_id",
        "prompt",
        "message",
        "question",
        "answer",
        "answer_var",
        "conversation_id_var",
        "platform_id_var",
        "model_var",
        "summary_var",
        "summary_error_var",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def _normalize_text_input(cls, value: Any) -> str:
        return _coerce_text_input(value)


class ChatIn(TextInputMixin):
    platform_id: str = ""
    conversation_id: str | None = None
    prompt: str
    message: str
    context: int | bool = 2
    model: str | None = None


class TestChatIn(TextInputMixin):
    platform_id: str = ""
    conversation_id: str | None = None
    prompt: str
    message: str
    context: int | bool = 1
    model: str | None = None


class SenlerChatIn(ChatIn):
    answer_var: str = "ai_answer"
    conversation_id_var: str = "conversation_id"
    platform_id_var: str = "platform_id"
    model_var: str = ""
    summary_var: str = ""
    summary_error_var: str = ""


class AppendIn(TextInputMixin):
    platform_id: str = ""
    conversation_id: str | None = None
    question: str = ""
    answer: str = ""
    prompt: str = ""


class SettingsIn(BaseModel):
    default_model: str | None = None
    summary_model: str | None = None
    request_timeout: int | None = None
    history_limit: int | None = None
    summary_prompt: str | None = None
    openrouter_api_key: str | None = None


class PromptModelIn(BaseModel):
    prompt_path: str
    model: str


class SummaryIn(BaseModel):
    model: str | None = None


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _require_bearer(request: Request) -> None:
    expected = await _module_api_token()
    if not expected:
        raise HTTPException(503, "module API token is not configured")
    header = request.headers.get("authorization", "")
    prefix = "Bearer "
    if not header.startswith(prefix) or not secrets.compare_digest(header[len(prefix):].strip(), expected):
        raise HTTPException(401, "unauthorized")


async def _require_bearer_or_panel(request: Request) -> None:
    try:
        await _require_bearer(request)
        return
    except HTTPException as bearer_exc:
        user = await verify_token_from_request(request)
        if user and can_access_module(user, MODULE_ID):
            return
        raise bearer_exc


def _context_mode(value: int | bool) -> int:
    if isinstance(value, bool):
        return 2 if value else 0
    try:
        mode = int(value)
    except Exception:
        mode = 2
    if mode not in (0, 1, 2, 3, 4):
        raise HTTPException(400, "context должен быть 0, 1, 2, 3 или 4")
    return mode


async def _settings() -> dict[str, str]:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT key,value FROM settings")
        rows = await cur.fetchall()
    data = {
        "default_model": DEFAULT_MODEL,
        "summary_model": DEFAULT_MODEL,
        "request_timeout": str(DEFAULT_TIMEOUT),
        "history_limit": str(MAX_HISTORY_MESSAGES),
        "summary_prompt": SALES_SUMMARY_PROMPT,
    }
    data.update({row[0]: row[1] for row in rows})
    return data


async def _module_api_token() -> str:
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (MODULE_TOKEN_SETTING,))
        row = await cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        token = _env()["api_token"] or secrets.token_urlsafe(40)
        await db.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (MODULE_TOKEN_SETTING, token),
        )
        await db.commit()
        return token


async def _rotate_module_api_token() -> str:
    token = secrets.token_urlsafe(40)
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (MODULE_TOKEN_SETTING, token),
        )
        await db.commit()
    return token


def _timeout(settings: dict[str, str]) -> float:
    try:
        return float(max(10, min(180, int(settings.get("request_timeout") or DEFAULT_TIMEOUT))))
    except Exception:
        return float(DEFAULT_TIMEOUT)


def _history_limit(settings: dict[str, str]) -> int:
    try:
        return max(0, min(200, int(settings.get("history_limit") or MAX_HISTORY_MESSAGES)))
    except Exception:
        return MAX_HISTORY_MESSAGES


async def _save_settings(data: SettingsIn) -> dict[str, str]:
    updates: dict[str, str] = {}
    if data.default_model is not None:
        updates["default_model"] = _clean(data.default_model, 200) or DEFAULT_MODEL
    if data.summary_model is not None:
        updates["summary_model"] = _clean(data.summary_model, 200) or DEFAULT_MODEL
    if data.request_timeout is not None:
        updates["request_timeout"] = str(max(10, min(180, int(data.request_timeout))))
    if data.history_limit is not None:
        updates["history_limit"] = str(max(0, min(200, int(data.history_limit))))
    if data.summary_prompt is not None:
        updates["summary_prompt"] = _clean(data.summary_prompt, 4000)
    openrouter_api_key = _clean(data.openrouter_api_key, 2000) if data.openrouter_api_key is not None else None
    async with aiosqlite.connect(_must_db()) as db:
        for key, value in updates.items():
            await db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        await db.commit()
    if openrouter_api_key:
        values = _read_env_values()
        values["OPENROUTER_API_KEY"] = openrouter_api_key
        _write_env_values(values)
        os.environ["OPENROUTER_API_KEY"] = openrouter_api_key
    return await _settings()


def _file_storage_db_path() -> Path:
    if _module_dir is None:
        raise HTTPException(500, "module is not initialized")
    return _module_dir.parent / "file-storage" / "data" / "file-storage.db"


def _file_storage_blob_dir() -> Path:
    if _module_dir is None:
        raise HTTPException(500, "module is not initialized")
    return _module_dir.parent / "file-storage" / "data" / "blobs"


def _safe_prompt_parts(prompt_path: str) -> list[str]:
    parts = [p for p in str(prompt_path or "").strip("/").split("/") if p]
    if not parts:
        raise HTTPException(400, "prompt path is required")
    for part in parts:
        if part in {".", ".."} or "/" in part or "\\" in part:
            raise HTTPException(400, "invalid prompt path")
    return parts


async def _resolve_prompt(prompt_path: str) -> tuple[str, str]:
    parts = _safe_prompt_parts(prompt_path)
    db_path = _file_storage_db_path()
    if not db_path.exists():
        raise HTTPException(400, "file-storage DB not found")
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        current_id = 1
        item = None
        for idx, name in enumerate(parts):
            cur = await db.execute(
                "SELECT * FROM items WHERE parent_id=? AND name=?",
                (current_id, name),
            )
            item = await cur.fetchone()
            if not item:
                raise HTTPException(400, f"prompt not found: {prompt_path}")
            if idx < len(parts) - 1 and item["kind"] != "folder":
                raise HTTPException(400, f"prompt not found: {prompt_path}")
            current_id = item["id"]
    if not item or item["kind"] != "file" or item["ext"] != "txt":
        raise HTTPException(400, "prompt must be a .txt file in file-storage")
    blob_path = _file_storage_blob_dir() / item["stored_name"]
    if not blob_path.exists():
        raise HTTPException(400, "prompt blob not found")
    try:
        text = blob_path.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        raise HTTPException(400, "prompt file is not UTF-8 text")
    if not text:
        raise HTTPException(400, "prompt file is empty")
    return "/".join(parts), text


async def _list_prompt_paths() -> list[dict[str, Any]]:
    db_path = _file_storage_db_path()
    if not db_path.exists():
        return []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id,parent_id,kind,name,ext,size,updated_at FROM items")
        rows = [dict(r) for r in await cur.fetchall()]
    by_parent: dict[int | None, list[dict[str, Any]]] = {}
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        by_id[row["id"]] = row
        by_parent.setdefault(row["parent_id"], []).append(row)
    result: list[dict[str, Any]] = []

    def walk(folder_id: int, prefix: list[str]) -> None:
        for item in sorted(by_parent.get(folder_id, []), key=lambda x: (x["kind"] != "folder", x["name"].lower())):
            if item["kind"] == "folder":
                walk(item["id"], [*prefix, item["name"]])
            elif item.get("ext") == "txt":
                path = "/".join([*prefix, item["name"]])
                result.append({
                    "path": path,
                    "name": item["name"],
                    "size": item["size"],
                    "updated_at": item["updated_at"],
                })

    walk(1, [])
    return result


async def _ensure_user(db: aiosqlite.Connection, platform_id: str) -> None:
    now = _now()
    await db.execute(
        """
        INSERT INTO users(platform_id, created_at, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(platform_id) DO UPDATE SET updated_at=excluded.updated_at
        """,
        (platform_id, now, now),
    )


async def _resolve_conversation(
    db: aiosqlite.Connection,
    *,
    platform_id: str,
    conversation_id: str | None,
    prompt_path: str = "",
    model: str = "",
) -> str:
    await _ensure_user(db, platform_id)
    now = _now()
    if conversation_id:
        cur = await db.execute(
            "SELECT conversation_id, platform_id FROM conversations WHERE conversation_id=?",
            (conversation_id,),
        )
        row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "conversation_id not found")
        if row[1] != platform_id:
            raise HTTPException(403, "conversation_id belongs to another platform_id")
        await db.execute(
            "UPDATE conversations SET updated_at=?, prompt_path=COALESCE(NULLIF(?,''),prompt_path), model=COALESCE(NULLIF(?,''),model) WHERE conversation_id=?",
            (now, prompt_path, model, conversation_id),
        )
        return conversation_id
    cur = await db.execute(
        """
        SELECT conversation_id FROM conversations
        WHERE platform_id=? AND active=1
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (platform_id,),
    )
    row = await cur.fetchone()
    if row:
        cid = row[0]
        await db.execute(
            "UPDATE conversations SET updated_at=?, prompt_path=COALESCE(NULLIF(?,''),prompt_path), model=COALESCE(NULLIF(?,''),model) WHERE conversation_id=?",
            (now, prompt_path, model, cid),
        )
        return cid
    cid = _new_conversation_id()
    await db.execute(
        """
        INSERT INTO conversations(conversation_id, platform_id, active, prompt_path, model, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (cid, platform_id, 1, prompt_path, model, now, now),
    )
    return cid


async def _platform_for_conversation(db: aiosqlite.Connection, conversation_id: str) -> str:
    cur = await db.execute("SELECT platform_id FROM conversations WHERE conversation_id=?", (conversation_id,))
    row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "conversation_id not found")
    return row[0]


async def _context_target(
    db: aiosqlite.Connection,
    *,
    platform_id: str = "",
    conversation_id: str | None = None,
) -> tuple[str, str]:
    if conversation_id:
        cur = await db.execute("SELECT conversation_id, platform_id FROM conversations WHERE conversation_id=?", (conversation_id,))
        row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "conversation_id not found")
        if platform_id and row[1] != platform_id:
            raise HTTPException(403, "conversation_id belongs to another platform_id")
        return row[1], row[0]
    if not platform_id:
        raise HTTPException(400, "platform_id or conversation_id is required")
    cur = await db.execute(
        """
        SELECT conversation_id FROM conversations
        WHERE platform_id=? AND active=1
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (platform_id,),
    )
    row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "conversation not found")
    return platform_id, row[0]


async def _model_for_prompt(prompt_path: str, settings: dict[str, str], requested: str | None = None) -> str:
    if requested and requested.strip():
        return requested.strip()
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT model FROM prompt_models WHERE prompt_path=?", (prompt_path,))
        row = await cur.fetchone()
    if row and row[0]:
        return row[0]
    return settings.get("default_model") or DEFAULT_MODEL


async def _load_history(db: aiosqlite.Connection, conversation_id: str, limit: int) -> list[dict[str, str]]:
    if limit == 0:
        return []
    if limit < 0:
        cur = await db.execute(
            """
            SELECT role, content FROM messages
            WHERE conversation_id=?
            ORDER BY id ASC
            """,
            (conversation_id,),
        )
        rows = await cur.fetchall()
    else:
        cur = await db.execute(
            """
            SELECT role, content FROM messages
            WHERE conversation_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (conversation_id, limit),
        )
        rows = await cur.fetchall()
        rows = list(reversed(rows))
    result = []
    for role, content in rows:
        mapped = "assistant" if role in ("assistant", "manual_assistant") else "user" if role in ("user", "manual_user") else ""
        if mapped and content:
            result.append({"role": mapped, "content": content})
    return result


async def _user_summary(db: aiosqlite.Connection, platform_id: str) -> str:
    cur = await db.execute("SELECT summary FROM users WHERE platform_id=?", (platform_id,))
    row = await cur.fetchone()
    return (row[0] if row else "") or ""


def _messages_for_api(prompt_text: str, summary: str, history: list[dict[str, str]], message: str) -> list[dict[str, str]]:
    system_parts = []
    if summary.strip():
        system_parts.append("# СВОДКА ПО КЛИЕНТУ\n" + summary.strip())
    system_parts.append(prompt_text.strip())
    messages = [{"role": "system", "content": "\n\n---\n\n".join(system_parts)}]
    messages.extend(history)
    messages.append({"role": "user", "content": message.strip()})
    return messages


def _context_payload(prompt_text: str, summary: str, history: list[dict[str, str]], message: str, mode: int) -> list[dict[str, str]]:
    if mode in (1, 2) and summary.strip():
        return _messages_for_api(prompt_text, summary, [], message)
    return _messages_for_api(prompt_text, summary if mode in (1, 2) else "", history, message)


async def _call_openrouter(model: str, messages: list[dict[str, Any]], timeout: float) -> tuple[str, dict[str, int]]:
    api_key = _env()["openrouter_key"]
    if not api_key:
        _log("warning", "OpenRouter call blocked: OPENROUTER_API_KEY is not configured model=%s", model)
        raise HTTPException(503, "OPENROUTER_API_KEY is not configured")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://junior.sobakovod.pro/nexus/",
        "X-Title": "Nexus OpenRouter",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(OPENROUTER_CHAT_URL, headers=headers, json={"model": model, "messages": messages})
    if resp.status_code >= 400:
        _log("warning", "OpenRouter HTTP error status=%s model=%s body=%s", resp.status_code, model, resp.text[:500])
        raise HTTPException(502, f"OpenRouter HTTP {resp.status_code}: {resp.text[:1000]}")
    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
        _log("warning", "OpenRouter response missing choices model=%s body=%s", model, str(data)[:500])
        raise HTTPException(502, "OpenRouter response missing choices")
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, list):
        content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
    usage = data.get("usage") or {}
    return str(content or "").strip(), {
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


async def _save_turn(
    db: aiosqlite.Connection,
    *,
    conversation_id: str,
    platform_id: str,
    pair_id: str,
    question: str,
    answer: str,
    source: str,
    prompt_path: str = "",
    model: str = "",
    usage: dict[str, int] | None = None,
) -> None:
    now = _now()
    if question.strip():
        await db.execute(
            """
            INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (conversation_id, platform_id, pair_id, "manual_user" if source == "manual" else "user", question, source, prompt_path, model, "{}", now),
        )
    if answer.strip():
        await db.execute(
            """
            INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (conversation_id, platform_id, pair_id, "manual_assistant" if source == "manual" else "assistant", answer, source, prompt_path, model, json.dumps(usage or {}, ensure_ascii=False), now),
        )
    await db.execute("UPDATE conversations SET updated_at=?, prompt_path=COALESCE(NULLIF(?,''),prompt_path), model=COALESCE(NULLIF(?,''),model) WHERE conversation_id=?", (now, prompt_path, model, conversation_id))
    await db.execute("UPDATE users SET updated_at=?, total_tokens_used=total_tokens_used+? WHERE platform_id=?", (now, int((usage or {}).get("total_tokens") or 0), platform_id))


async def _conversation_transcript(db: aiosqlite.Connection, conversation_id: str) -> list[str]:
    cur = await db.execute("SELECT role,content FROM messages WHERE conversation_id=? ORDER BY id ASC", (conversation_id,))
    messages = await cur.fetchall()
    transcript = []
    for role, content in messages:
        if role in ("user", "manual_user"):
            transcript.append("Вопрос: " + content)
        elif role in ("assistant", "manual_assistant"):
            transcript.append("Ответ: " + content)
    return transcript


def _message_pairs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pairs: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = pairs.setdefault(
            row["pair_id"],
            {
                "pair_id": row["pair_id"],
                "question": "",
                "answer": "",
                "source": row["source"],
                "created_at": row["created_at"],
                "messages": [],
            },
        )
        if row["role"] in ("user", "manual_user"):
            entry["question"] = row["content"]
        elif row["role"] in ("assistant", "manual_assistant"):
            entry["answer"] = row["content"]
        entry["messages"].append(row)
    return list(pairs.values())


async def _generate_and_save_summary(conversation_id: str, model: str | None = None) -> dict[str, Any]:
    settings = await _settings()
    summary_model = model or settings.get("summary_model") or DEFAULT_MODEL
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,))
        conv = await cur.fetchone()
        if not conv:
            raise HTTPException(404, "conversation not found")
        transcript = await _conversation_transcript(db, conversation_id)
    if not transcript:
        raise HTTPException(400, "conversation has no messages")
    summary_prompt = settings.get("summary_prompt") or SALES_SUMMARY_PROMPT
    summary, usage = await _call_openrouter(
        summary_model,
        [{"role": "system", "content": summary_prompt}, {"role": "user", "content": "\n\n".join(transcript)[-60000:]}],
        _timeout(settings),
    )
    summary = summary[:SUMMARY_MAX_CHARS]
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "UPDATE users SET summary=?, updated_at=?, total_tokens_used=total_tokens_used+? WHERE platform_id=?",
            (summary, _now(), int(usage.get("total_tokens") or 0), conv["platform_id"]),
        )
        await db.commit()
    return {"platform_id": conv["platform_id"], "conversation_id": conversation_id, "model": summary_model, "summary": summary, "usage": usage}


async def _generate_summary_background(conversation_id: str) -> None:
    try:
        result = await _generate_and_save_summary(conversation_id)
        _log(
            "info",
            "chat auto-summary ok conversation_id=%s model=%s summary_chars=%s",
            conversation_id,
            result.get("model", ""),
            len(result.get("summary", "") or ""),
        )
    except HTTPException as exc:
        _log("warning", "chat auto-summary failed conversation_id=%s detail=%s", conversation_id, exc.detail)
    except Exception as exc:
        _log("error", "chat auto-summary crashed conversation_id=%s detail=%s", conversation_id, exc, exc_info=True)


@router.get("/env-status")
async def env_status(request: Request):
    await _require_panel_user(request)
    env = _env()
    fs_db = _file_storage_db_path()
    module_token = await _module_api_token()
    return {
        "ready": bool(env["openrouter_key"] and module_token and fs_db.exists()),
        "OPENROUTER_API_KEY": bool(env["openrouter_key"]),
        "NEXUS_OPENROUTER_API_TOKEN": bool(module_token),
        "MODULE_API_TOKEN": bool(module_token),
        "file_storage_db": fs_db.exists(),
        "env_path": str(ENV_PATH),
    }


@router.get("/settings")
async def get_settings(request: Request):
    await _require_panel_user(request)
    return await _settings()


@router.post("/settings")
async def post_settings(data: SettingsIn, request: Request):
    await _require_panel_user(request)
    return await _save_settings(data)


@router.put("/settings")
async def put_settings(data: SettingsIn, request: Request):
    await _require_panel_user(request)
    return await _save_settings(data)


@router.get("/api-token")
async def get_api_token(request: Request):
    await _require_panel_user(request)
    return {"token": await _module_api_token()}


@router.post("/api-token/rotate")
async def rotate_api_token(request: Request):
    await _require_panel_user(request)
    return {"token": await _rotate_module_api_token()}


@router.get("/me")
async def get_me(request: Request):
    user = await _require_panel_user(request)
    return {"username": user.get("username") or "", "role": user.get("role") or ""}


@router.get("/prompts")
async def list_prompts(request: Request):
    await _require_panel_user(request)
    prompts = await _list_prompt_paths()
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT prompt_path, model FROM prompt_models")
        overrides = {row[0]: row[1] for row in await cur.fetchall()}
    settings = await _settings()
    for p in prompts:
        p["model"] = overrides.get(p["path"]) or ""
        p["effective_model"] = p["model"] or settings.get("default_model") or DEFAULT_MODEL
    return {"items": prompts}


@router.get("/prompt-models")
async def get_prompt_models(request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        cur = await db.execute("SELECT prompt_path, model, updated_at FROM prompt_models ORDER BY prompt_path")
        rows = await cur.fetchall()
    return [{"prompt_path": r[0], "model": r[1], "updated_at": r[2]} for r in rows]


@router.put("/prompt-models")
async def put_prompt_model(data: PromptModelIn, request: Request):
    await _require_panel_user(request)
    prompt_path, _ = await _resolve_prompt(data.prompt_path)
    model = _clean(data.model, 200)
    async with aiosqlite.connect(_must_db()) as db:
        if model:
            await db.execute(
                "INSERT INTO prompt_models(prompt_path,model,updated_at) VALUES(?,?,?) ON CONFLICT(prompt_path) DO UPDATE SET model=excluded.model, updated_at=excluded.updated_at",
                (prompt_path, model, _now()),
            )
        else:
            await db.execute("DELETE FROM prompt_models WHERE prompt_path=?", (prompt_path,))
        await db.commit()
    return {"ok": True, "prompt_path": prompt_path, "model": model}


@router.get("/models")
async def get_models(request: Request, refresh: int = 0):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        if not refresh:
            cur = await db.execute("SELECT models_json FROM model_cache WHERE id=1")
            row = await cur.fetchone()
            if row:
                try:
                    return {"items": json.loads(row[0]), "cached": True}
                except Exception:
                    pass
    headers = {}
    if _env()["openrouter_key"]:
        headers["Authorization"] = f"Bearer {_env()['openrouter_key']}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(OPENROUTER_MODELS_URL, headers=headers)
        if resp.status_code >= 400:
            raise HTTPException(502, f"OpenRouter models HTTP {resp.status_code}")
    except Exception:
        settings = await _settings()
        fallback = sorted({DEFAULT_MODEL, settings.get("default_model") or "", settings.get("summary_model") or ""} - {""})
        return {"items": [{"id": model, "name": model} for model in fallback], "cached": False, "fallback": True}
    raw = resp.json().get("data") or []
    items = []
    for m in raw:
        model_id = str(m.get("id") or "").strip()
        if model_id:
            items.append({"id": model_id, "name": m.get("name") or model_id})
    async with aiosqlite.connect(_must_db()) as db:
        await db.execute(
            "INSERT INTO model_cache(id,models_json,updated_at) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET models_json=excluded.models_json, updated_at=excluded.updated_at",
            (json.dumps(items, ensure_ascii=False), _now()),
        )
        await db.commit()
    return {"items": items, "cached": False}


async def _run_chat(
    data: ChatIn | TestChatIn,
    *,
    allow_write: bool,
    source: str,
    defer_summary: bool = False,
    prefer_summary_context: bool = False,
) -> dict[str, Any]:
    platform_id = _clean(data.platform_id, 300)
    conversation_id = _clean(data.conversation_id, 200) or None
    message = _clean(data.message, 50000)
    if not message:
        raise HTTPException(400, "message is required")
    mode = _context_mode(data.context)
    read_mode = 2 if prefer_summary_context and mode == 4 else mode
    prompt_path, prompt_text = await _resolve_prompt(data.prompt)
    settings = await _settings()
    model = await _model_for_prompt(prompt_path, settings, data.model)
    async with aiosqlite.connect(_must_db()) as db:
        if not platform_id and conversation_id:
            platform_id = await _platform_for_conversation(db, conversation_id)
        if not platform_id:
            raise HTTPException(400, "platform_id is required when conversation_id is not provided")
        cid = await _resolve_conversation(db, platform_id=platform_id, conversation_id=conversation_id, prompt_path=prompt_path, model=model)
        summary = await _user_summary(db, platform_id) if read_mode in (1, 2) else ""
        if read_mode in (1, 2):
            history = [] if summary else await _load_history(db, cid, _history_limit(settings))
        elif read_mode in (3, 4):
            history = await _load_history(db, cid, -1)
        else:
            history = []
        await db.commit()
    _log(
        "info",
        "chat start source=%s write=%s platform_id=%s conversation_id=%s prompt=%s model=%s context=%s read_context=%s message_chars=%s",
        source,
        allow_write,
        platform_id,
        cid,
        prompt_path,
        model,
        mode,
        read_mode,
        len(message),
    )
    answer, usage = await _call_openrouter(model, _context_payload(prompt_text, summary, history, message, read_mode), _timeout(settings))
    summary_result = None
    summary_error = ""
    if allow_write and mode in (2, 3, 4):
        async with aiosqlite.connect(_must_db()) as db:
            await _save_turn(
                db,
                conversation_id=cid,
                platform_id=platform_id,
                pair_id=_new_pair_id(),
                question=message,
                answer=answer,
                source="api",
                prompt_path=prompt_path,
                model=model,
                usage=usage,
            )
            await db.commit()
        if mode == 4:
            if defer_summary:
                asyncio.create_task(_generate_summary_background(cid))
                _log("info", "chat auto-summary scheduled conversation_id=%s source=%s", cid, source)
            else:
                try:
                    summary_result = await _generate_and_save_summary(cid)
                except HTTPException as exc:
                    summary_error = str(exc.detail)
                    _log("warning", "chat auto-summary failed conversation_id=%s detail=%s", cid, summary_error)
    _log(
        "info",
        "chat ok source=%s write=%s platform_id=%s conversation_id=%s prompt=%s model=%s context=%s total_tokens=%s answer_chars=%s",
        source,
        allow_write,
        platform_id,
        cid,
        prompt_path,
        model,
        mode,
        int((usage or {}).get("total_tokens") or 0),
        len(answer or ""),
    )
    return {
        "ok": True,
        "platform_id": platform_id,
        "conversation_id": cid,
        "prompt": prompt_path,
        "model": model,
        "read_context": read_mode,
        "text": answer,
        "answer": answer,
        "usage": usage,
        "summary": summary_result["summary"] if summary_result else None,
        "summary_error": summary_error,
    }


@router.post("/chat")
async def chat(request: Request):
    try:
        await _require_bearer(request)
        try:
            raw = await request.json()
            data = ChatIn(**raw)
        except ValidationError as exc:
            raise HTTPException(400, f"invalid chat body: {_validation_detail(exc)}")
        except Exception as exc:
            raise HTTPException(400, "invalid JSON body")
        return await _run_chat(data, allow_write=True, source="api")
    except HTTPException as exc:
        _log("warning", "chat failed status=%s detail=%s", exc.status_code, exc.detail)
        raise
    except Exception as exc:
        _log("error", "chat crashed: %s", exc, exc_info=True)
        raise


@router.post("/test-chat")
async def test_chat(request: Request):
    try:
        await _require_panel_user(request)
        try:
            raw = await request.json()
            data = TestChatIn(**raw)
        except ValidationError as exc:
            raise HTTPException(400, f"invalid test body: {_validation_detail(exc)}")
        except Exception as exc:
            raise HTTPException(400, "invalid JSON body")
        return await _run_chat(data, allow_write=False, source="panel_test")
    except HTTPException as exc:
        _log("warning", "test-chat failed status=%s detail=%s", exc.status_code, exc.detail)
        raise
    except Exception as exc:
        _log("error", "test-chat crashed: %s", exc, exc_info=True)
        raise


def _senler_var(items: list[dict[str, str]], name: str, value: Any) -> None:
    clean_name = _clean(name, 120)
    if not clean_name or value is None:
        return
    items.append({"n": clean_name, "v": str(value)})


@router.post("/senler-chat")
async def senler_chat(request: Request):
    try:
        await _require_bearer(request)
        try:
            raw = await request.json()
            data = SenlerChatIn(**raw)
        except ValidationError as exc:
            raise HTTPException(400, f"invalid senler body: {_validation_detail(exc)}")
        except Exception:
            raise HTTPException(400, "invalid JSON body")
        result = await _run_chat(data, allow_write=True, source="senler", defer_summary=True, prefer_summary_context=True)
        vars_out: list[dict[str, str]] = []
        _senler_var(vars_out, data.answer_var, result.get("text", ""))
        _senler_var(vars_out, data.conversation_id_var, result.get("conversation_id", ""))
        _senler_var(vars_out, data.platform_id_var, result.get("platform_id", ""))
        _senler_var(vars_out, data.model_var, result.get("model", ""))
        _senler_var(vars_out, data.summary_var, result.get("summary", ""))
        _senler_var(vars_out, data.summary_error_var, result.get("summary_error", ""))
        return {"vars": vars_out, "glob_vars": []}
    except HTTPException as exc:
        _log("warning", "senler-chat failed status=%s detail=%s", exc.status_code, exc.detail)
        raise
    except Exception as exc:
        _log("error", "senler-chat crashed: %s", exc, exc_info=True)
        raise


@router.get("/schema")
async def api_schema(request: Request):
    await _require_bearer_or_panel(request)
    return {
        "chat": {
            "method": "POST",
            "path": "/nexus/openrouter/api/chat",
            "auth": "Authorization: Bearer <токен модуля из настроек>",
            "body_fields": {
                "platform_id": "string|number, обязательный если не передан conversation_id; number будет сохранен как строка",
                "conversation_id": "string|number|null, если передан без platform_id, platform_id будет найден по чату",
                "prompt": "string, путь к .txt prompt в file-storage, например prompts/avito_gpt1.txt",
                "message": "string, вопрос пользователя",
                "context": "0|1|2|3|4 или boolean; 0 без контекста, 1 краткий без записи, 2 краткий+запись, 3 полный+запись, 4 полный+запись+автосводка",
                "model": "string|null, необязательный override модели",
            },
            "response_fields": {
                "ok": "boolean",
                "platform_id": "string",
                "conversation_id": "string",
                "prompt": "string",
                "model": "string",
                "text": "string, текст ответа",
                "answer": "string, alias text",
                "usage": "object с token usage",
                "summary": "string|null, новая сводка при context=4",
                "summary_error": "string, ошибка автосводки если ответ был получен, но сводка не обновилась",
            },
            "example": {
                "platform_id": "vk_123",
                "conversation_id": None,
                "prompt": "prompts/avito_gpt1.txt",
                "message": "Вопрос клиента",
                "context": 2,
            },
        },
        "context": {
            "brief": "GET /nexus/openrouter/api/context/brief?platform_id=vk_123 или ?conversation_id=or_conv_...",
            "full": "GET /nexus/openrouter/api/context/full?platform_id=vk_123 или ?conversation_id=or_conv_...",
            "append": "POST /nexus/openrouter/api/context/append",
        },
        "senler_chat": {
            "method": "POST",
            "path": "/nexus/openrouter/api/senler-chat",
            "auth": "Authorization: Bearer <токен модуля из настроек>",
            "body": "как /chat; дополнительно answer_var, conversation_id_var, platform_id_var, model_var, summary_var, summary_error_var. При context=4 ответ строится по краткой сводке, а sales-сводка обновляется в фоне.",
            "response": {
                "vars": [
                    {"n": "ai_answer", "v": "текст ответа модели"},
                    {"n": "conversation_id", "v": "or_conv_..."},
                    {"n": "platform_id", "v": "vk_123"},
                ],
                "glob_vars": [],
            },
        },
        "panel_test": {
            "method": "POST",
            "path": "/nexus/openrouter/api/test-chat",
            "auth": "Nexus cookie, только из панели",
            "writes_context": False,
        },
    }


@router.post("/context/append")
async def append_context(data: AppendIn, request: Request):
    await _require_bearer_or_panel(request)
    platform_id = _clean(data.platform_id, 300)
    question = _clean(data.question, 50000)
    answer = _clean(data.answer, 50000)
    if not question and not answer:
        raise HTTPException(400, "question or answer is required")
    prompt_path = ""
    if data.prompt:
        prompt_path, _ = await _resolve_prompt(data.prompt)
    async with aiosqlite.connect(_must_db()) as db:
        if not platform_id and data.conversation_id:
            platform_id = await _platform_for_conversation(db, data.conversation_id)
        if not platform_id:
            raise HTTPException(400, "platform_id is required when conversation_id is not provided")
        cid = await _resolve_conversation(db, platform_id=platform_id, conversation_id=data.conversation_id, prompt_path=prompt_path)
        pair_id = _new_pair_id()
        await _save_turn(db, conversation_id=cid, platform_id=platform_id, pair_id=pair_id, question=question, answer=answer, source="manual", prompt_path=prompt_path)
        await db.commit()
    return {"ok": True, "platform_id": platform_id, "conversation_id": cid, "pair_id": pair_id}


@router.get("/context/brief")
async def brief_context(request: Request, platform_id: str = "", conversation_id: str = ""):
    await _require_bearer_or_panel(request)
    settings = await _settings()
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        resolved_platform, cid = await _context_target(
            db,
            platform_id=_clean(platform_id, 300),
            conversation_id=_clean(conversation_id, 200) or None,
        )
        summary = await _user_summary(db, resolved_platform)
        history = [] if summary else await _load_history(db, cid, _history_limit(settings))
    return {
        "ok": True,
        "platform_id": resolved_platform,
        "conversation_id": cid,
        "type": "summary" if summary else "history",
        "summary": summary,
        "messages": history,
    }


@router.get("/context/full")
async def full_context(request: Request, platform_id: str = "", conversation_id: str = ""):
    await _require_bearer_or_panel(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        resolved_platform, cid = await _context_target(
            db,
            platform_id=_clean(platform_id, 300),
            conversation_id=_clean(conversation_id, 200) or None,
        )
        cur = await db.execute("SELECT * FROM conversations WHERE conversation_id=?", (cid,))
        conv = await cur.fetchone()
        cur = await db.execute("SELECT * FROM messages WHERE conversation_id=? ORDER BY id ASC", (cid,))
        rows = [dict(r) for r in await cur.fetchall()]
        summary = await _user_summary(db, resolved_platform)
    return {
        "ok": True,
        "platform_id": resolved_platform,
        "conversation_id": cid,
        "conversation": dict(conv) if conv else None,
        "summary": summary,
        "items": _message_pairs(rows),
        "messages": rows,
    }


@router.get("/users")
async def list_users(request: Request, q: str = "", limit: int = 100):
    await _require_panel_user(request)
    pat = f"%{_clean(q, 200)}%"
    limit = max(1, min(500, int(limit or 100)))
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT u.platform_id, u.summary, u.total_tokens_used, u.created_at, u.updated_at,
                   COUNT(DISTINCT c.conversation_id) AS conversations,
                   COUNT(m.id) AS messages
            FROM users u
            LEFT JOIN conversations c ON c.platform_id=u.platform_id
            LEFT JOIN messages m ON m.platform_id=u.platform_id
            WHERE u.platform_id LIKE ?
               OR u.platform_id IN (
                    SELECT platform_id FROM conversations WHERE conversation_id LIKE ?
               )
            GROUP BY u.platform_id
            ORDER BY u.updated_at DESC
            LIMIT ?
            """,
            (pat, pat, limit),
        )
        rows = [dict(r) for r in await cur.fetchall()]
    return {"items": rows}


@router.get("/users/{platform_id}/conversations")
async def user_conversations(platform_id: str, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT c.*, COUNT(m.id) AS messages
            FROM conversations c
            LEFT JOIN messages m ON m.conversation_id=c.conversation_id
            WHERE c.platform_id=?
            GROUP BY c.conversation_id
            ORDER BY c.updated_at DESC
            """,
            (platform_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute("SELECT summary FROM users WHERE platform_id=?", (platform_id,))
        user = await cur.fetchone()
    return {"platform_id": platform_id, "summary": user["summary"] if user else "", "items": rows}


@router.get("/conversations/{conversation_id}/messages")
async def conversation_messages(conversation_id: str, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_must_db()) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,))
        conv = await cur.fetchone()
        if not conv:
            raise HTTPException(404, "conversation not found")
        cur = await db.execute(
            "SELECT * FROM messages WHERE conversation_id=? ORDER BY id ASC",
            (conversation_id,),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        cur = await db.execute("SELECT summary FROM users WHERE platform_id=?", (conv["platform_id"],))
        user = await cur.fetchone()
    return {"conversation": dict(conv), "summary": user["summary"] if user else "", "items": _message_pairs(rows)}


@router.post("/conversations/{conversation_id}/summary")
async def conversation_summary(conversation_id: str, data: SummaryIn, request: Request):
    await _require_panel_user(request)
    result = await _generate_and_save_summary(conversation_id, data.model)
    return {"ok": True, **result}
