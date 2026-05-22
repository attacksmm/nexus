import os
from datetime import datetime, timedelta, timezone

import aiosqlite
from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from jose import JWTError, jwt
from passlib.context import CryptContext

from orchestrator.db import DB_PATH

SECRET_KEY = os.environ.get("NEXUS_SECRET", "change-me-in-production-please-use-env")
ALGORITHM = "HS256"
TOKEN_TTL_HOURS = 24 * 7

pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
templates = Jinja2Templates(directory=str(__import__("pathlib").Path(__file__).parent.parent / "templates"))
router = APIRouter()


def _make_token(username: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=TOKEN_TTL_HOURS)
    return jwt.encode({"sub": username, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)


def _verify_token(token: str) -> str | None:
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return data.get("sub")
    except JWTError:
        return None


async def verify_token_from_request(request: Request) -> str | None:
    token = request.cookies.get("nexus_token")
    if not token:
        return None
    return _verify_token(token)


async def ensure_default_user():
    """Create admin/admin if no users exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        (count,) = await cur.fetchone()
        if count == 0:
            h = pwd_ctx.hash("admin")
            await db.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)", ("admin", h)
            )
            await db.commit()


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = await verify_token_from_request(request)
    if user:
        return RedirectResponse("/")
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    response: Response,
    username: str = Form(...),
    password: str = Form(...),
):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = await cur.fetchone()

    if row and pwd_ctx.verify(password, row["password_hash"]):
        token = _make_token(username)
        resp = RedirectResponse("/", status_code=303)
        resp.set_cookie("nexus_token", token, httponly=True, samesite="lax", max_age=TOKEN_TTL_HOURS * 3600)
        return resp

    return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный логин или пароль"})


@router.post("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie("nexus_token")
    return resp
