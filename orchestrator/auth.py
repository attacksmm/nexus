import json
import os
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


async def verify_token_from_request(request: Request) -> dict | None:
    """Returns user dict (with role, module_access) or None."""
    token = request.cookies.get("nexus_token")
    if not token:
        return None
    username = _verify_token(token)
    if not username:
        return None
    return await get_user_by_username(username)


def can_access_module(user: dict, module_id: str) -> bool:
    if user["role"] == "admin":
        return True
    access = json.loads(user.get("module_access") or "[]")
    return not access or module_id in access


def require_admin(user: dict | None) -> bool:
    return user is not None and user["role"] == "admin"


# ── Default user ──────────────────────────────────────────────────────────────

async def ensure_default_users():
    from orchestrator.db import DB_PATH
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        (count,) = await cur.fetchone()
    if count == 0:
        await create_user("admin", pwd_ctx.hash("admin"), role="admin", module_access="[]")


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
    module_access = json.dumps(data.get("module_access", []))
    if not username or not password:
        return _err("username и password обязательны")
    if role not in ("admin", "editor", "viewer"):
        return _err("Недопустимая роль")
    try:
        uid = await create_user(username, pwd_ctx.hash(password), role, module_access)
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
    module_access = json.dumps(data.get("module_access", []))
    active = int(data.get("active", 1))
    if role not in ("admin", "editor", "viewer"):
        return _err("Недопустимая роль")
    await update_user(uid, role, module_access, active)
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


@router.get("/api/settings/env")
async def api_env_list(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    keys = _parse_env_keys()
    return {"keys": keys}


@router.post("/api/settings/env/upload")
async def api_env_upload(request: Request):
    """Upload a .env file — values never returned to client."""
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return _forbidden()
    from fastapi import File, UploadFile
    form = await request.form()
    file = form.get("file")
    if not file:
        return _err("Файл не передан")
    content = (await file.read()).decode("utf-8", errors="replace")
    lines = [l for l in content.splitlines() if l.strip() and not l.startswith("#")]
    parsed = {}
    for line in lines:
        if "=" in line:
            k, _, v = line.partition("=")
            parsed[k.strip()] = v.strip().strip('"').strip("'")
    if not parsed:
        return _err("Файл пустой или не содержит переменных")
    # write
    with open(ENV_PATH, "w") as f:
        for k, v in parsed.items():
            f.write(f"{k}={v}\n")
    # применяем все переменные без перезапуска
    # NEXUS_SECRET тоже применяется — _secret() читает os.environ динамически
    import os
    for k, v in parsed.items():
        os.environ[k] = v
    return {"ok": True, "count": len(parsed), "keys": list(parsed.keys())}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_env_keys() -> list[str]:
    if not ENV_PATH.exists():
        return []
    keys = []
    for line in ENV_PATH.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            keys.append(line.partition("=")[0].strip())
    return keys


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
