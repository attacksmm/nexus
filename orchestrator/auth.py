import json
import os
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from jose import JWTError, jwt
from passlib.context import CryptContext

from orchestrator.db import (
    create_user, delete_user, get_all_users, get_user_by_username,
    update_user, update_user_password,
)

_FALLBACK_SECRET = "change-me-in-production-please-use-env"
ALGORITHM = "HS256"
TOKEN_TTL_HOURS = 24 * 7


def _secret() -> str:
    """Читает NEXUS_SECRET из окружения каждый раз — обновляется без перезапуска."""
    return os.environ.get("NEXUS_SECRET") or _FALLBACK_SECRET

pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
router = APIRouter()

_tpl_dir = str(__import__("pathlib").Path(__file__).parent.parent / "templates")
templates = Jinja2Templates(directory=_tpl_dir)


# ── Token helpers ─────────────────────────────────────────────────────────────

def _make_token(username: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=TOKEN_TTL_HOURS)
    return jwt.encode({"sub": username, "exp": exp}, _secret(), algorithm=ALGORITHM)


def _verify_token(token: str) -> str | None:
    try:
        data = jwt.decode(token, _secret(), algorithms=[ALGORITHM])
        return data.get("sub")
    except JWTError:
        return None


async def verify_token_value(token: str | None) -> dict | None:
    """Returns user dict (with role, module_access) or None."""
    if not token:
        return None
    username = _verify_token(token)
    if not username:
        return None
    return await get_user_by_username(username)


async def verify_token_from_request(request: Request) -> dict | None:
    return await verify_token_value(request.cookies.get("nexus_token"))


def can_access_module(user: dict, module_id: str) -> bool:
    if user["role"] == "admin":
        return True
    try:
        access = json.loads(user.get("module_access") or "[]")
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(access, list):
        return False
    return not access or module_id in access


def require_admin(user: dict | None) -> bool:
    return user is not None and user["role"] == "admin"


# ── Default user ──────────────────────────────────────────────────────────────

async def ensure_default_users():
    """Создаёт admin только при самом первом запуске (bootstrap).
    После удаления admin — не восстанавливает его."""
    from orchestrator.db import DB_PATH
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key='bootstrap_done'")
        row = await cur.fetchone()
        if row:
            return  # bootstrap уже выполнялся — ничего не делаем
        cur2 = await db.execute("SELECT COUNT(*) FROM users")
        (count,) = await cur2.fetchone()
        if count == 0:
            await create_user("admin", pwd_ctx.hash("admin"), role="admin", module_access="[]")
        # помечаем что bootstrap выполнен
        await db.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('bootstrap_done', '1')"
        )
        await db.commit()


# ── Auth pages ────────────────────────────────────────────────────────────────

def _rp(request: Request) -> str:
    return request.scope.get("root_path", "")


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = await verify_token_from_request(request)
    if user:
        return RedirectResponse(_rp(request) + "/")
    return templates.TemplateResponse("login.html", {"request": request, "error": None, "rp": _rp(request)})


@router.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    row = await get_user_by_username(username)
    if row and pwd_ctx.verify(password, row["password_hash"]):
        token = _make_token(username)
        resp = RedirectResponse(_rp(request) + "/", status_code=303)
        resp.set_cookie("nexus_token", token, httponly=True, samesite="lax", max_age=TOKEN_TTL_HOURS * 3600)
        return resp
    return templates.TemplateResponse(
        "login.html", {"request": request, "error": "Неверный логин или пароль", "rp": _rp(request)}
    )


@router.post("/logout")
async def logout(request: Request):
    resp = RedirectResponse(_rp(request) + "/login", status_code=303)
    resp.delete_cookie("nexus_token")
    return resp


# ── Settings API (admin only) ──────────────────────────────────────────────────

@router.get("/api/settings/users")
async def api_users_list(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    return await get_all_users()


@router.post("/api/settings/users")
async def api_user_create(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    data = await request.json()
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    role = data.get("role", "viewer")
    module_access = _normalize_module_access(data.get("module_access", []))
    if module_access is None:
        return _err("module_access должен быть списком модулей")
    if not username or not password:
        return _err("username и password обязательны")
    if role not in ("admin", "editor", "viewer"):
        return _err("Недопустимая роль")
    try:
        uid = await create_user(username, pwd_ctx.hash(password), role, json.dumps(module_access))
        return {"id": uid, "username": username, "role": role}
    except Exception as e:
        return _err(str(e))


@router.put("/api/settings/users/{uid}")
async def api_user_update(uid: int, request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    data = await request.json()
    role = data.get("role", "viewer")
    module_access = _normalize_module_access(data.get("module_access", []))
    if module_access is None:
        return _err("module_access должен быть списком модулей")
    active = int(data.get("active", 1))
    if role not in ("admin", "editor", "viewer"):
        return _err("Недопустимая роль")
    await update_user(uid, role, json.dumps(module_access), active)
    return {"ok": True}


@router.put("/api/settings/users/{uid}/password")
async def api_user_password(uid: int, request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    data = await request.json()
    pwd = data.get("password", "").strip()
    if len(pwd) < 8:
        return _err("Минимум 8 символов")
    await update_user_password(uid, pwd_ctx.hash(pwd))
    return {"ok": True}


@router.delete("/api/settings/users/{uid}")
async def api_user_delete(uid: int, request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    # нельзя удалить себя
    if user["username"] == (await _get_user_by_id(uid)):
        return _err("Нельзя удалить собственный аккаунт")
    await delete_user(uid)
    return {"ok": True}


# ── ENV API (admin only) ───────────────────────────────────────────────────────

ENV_PATH = __import__("pathlib").Path(__file__).parent.parent / ".env"
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@router.get("/api/settings/env")
async def api_env_list(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    keys = _parse_env_keys()
    last_updated = None
    if ENV_PATH.exists():
        import datetime
        mtime = ENV_PATH.stat().st_mtime
        last_updated = datetime.datetime.fromtimestamp(
            mtime, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC")
    return {"keys": keys, "last_updated": last_updated}


@router.post("/api/settings/env/upload")
async def api_env_upload(request: Request):
    """Safely merges a text .env upload into the stored environment.

    Values are never returned to the client. Empty template lines are ignored,
    and omitted existing keys are preserved so module installs cannot wipe
    already configured secrets.
    """
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    form = await request.form()
    file = form.get("file")
    if not file:
        return _err("Файл не передан")

    content = (await file.read()).decode("utf-8", errors="replace")
    parsed = _parse_env_content(content)

    if not parsed:
        return _err("Файл не содержит переменных формата KEY=value")

    invalid = [k for k in parsed if not ENV_KEY_RE.match(k)]
    if invalid:
        return _err("Некорректные ENV ключи: " + ", ".join(invalid))

    existing = _read_env_values()
    merged = dict(existing)
    added = [k for k in parsed if k not in existing]
    updated = [k for k, v in parsed.items() if k in existing and existing[k] != v]
    unchanged = [k for k, v in parsed.items() if k in existing and existing[k] == v]
    changed_keys = added + updated
    merged.update(parsed)

    _write_env_values(merged)

    # применяем к процессу без перезапуска
    for k, v in parsed.items():
        os.environ[k] = v

    return {
        "ok": True,
        "count": len(parsed),
        "keys": list(parsed.keys()),
        "changed_keys": changed_keys,
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "retained_count": max(0, len(existing) - len(updated) - len(unchanged)),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_env_content(content: str) -> dict[str, str]:
    """Парсит .env файл любого формата:
    - Игнорирует строки-комментарии (начинаются с #)
    - Убирает инлайн-комментарии: KEY=value  # описание → value
    - Обрабатывает кавычки: KEY="value" → value
    - Пропускает строки без '='
    - Пропускает ключи с пустым значением
    """
    parsed = {}
    for raw_line in content.splitlines():
        line = raw_line.strip().lstrip("\ufeff")
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, rest = line.partition("=")
        key = key.strip()
        if not key:
            continue
        # убираем инлайн-комментарий (не внутри кавычек)
        value = _strip_inline_comment(rest)
        # убираем кавычки
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        # пропускаем пустые значения (незаполненные строки шаблона)
        if not value:
            continue
        parsed[key] = value
    return parsed


def _normalize_module_access(value) -> list[str] | None:
    if value is None:
        return []
    if not isinstance(value, list):
        return None
    result = []
    seen = set()
    for item in value:
        module_id = str(item).strip()
        if module_id and module_id not in seen:
            result.append(module_id)
            seen.add(module_id)
    return result


def _read_env_values() -> dict[str, str]:
    if not ENV_PATH.exists():
        return {}
    return _parse_env_content(ENV_PATH.read_text(encoding="utf-8"))


def _write_env_values(values: dict[str, str]) -> None:
    if ENV_PATH.exists():
        backup_path = ENV_PATH.parent / ".env.bak"
        backup_path.write_text(ENV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    tmp_path = ENV_PATH.parent / ".env.tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        for k, v in values.items():
            f.write(f"{k}={v}\n")
    tmp_path.replace(ENV_PATH)


def _strip_inline_comment(s: str) -> str:
    """Убирает # комментарий в конце значения если он вне кавычек."""
    s = s.strip()
    in_q = None
    for i, ch in enumerate(s):
        if ch in ('"', "'"):
            if in_q is None:
                in_q = ch
            elif in_q == ch:
                in_q = None
        elif ch == "#" and in_q is None:
            return s[:i].strip()
    return s


def _parse_env_keys() -> list[str]:
    return list(_read_env_values().keys())


async def _get_user_by_id(uid: int) -> str | None:
    from orchestrator.db import DB_PATH
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT username FROM users WHERE id = ?", (uid,))
        row = await cur.fetchone()
        return row[0] if row else None


from fastapi.responses import JSONResponse

def _forbidden():
    return JSONResponse({"error": "Недостаточно прав"}, status_code=403)

def _err(msg: str):
    return JSONResponse({"error": msg}, status_code=400)
