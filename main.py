from contextlib import asynccontextmanager
from pathlib import Path

import aiofiles
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from orchestrator.auth import (
    ensure_default_users, require_admin,
    router as auth_router, verify_token_from_request,
)
from orchestrator.core import ModuleManager, UPLOADS_DIR
from orchestrator.db import init_db, update_module_status

BASE_DIR = Path(__file__).parent
UPLOADS_DIR.mkdir(exist_ok=True)

manager = ModuleManager(BASE_DIR)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await ensure_default_users()
    await manager.restore_active_modules(app)
    yield


app = FastAPI(lifespan=lifespan, title="Nexus Orchestrator")
app.include_router(auth_router)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _rp(request: Request) -> str:
    return request.scope.get("root_path", "")


def _auth_redirect(request: Request):
    return RedirectResponse(_rp(request) + "/login", status_code=303)


def _unauth_json():
    return JSONResponse({"error": "unauthorized"}, status_code=401)


# ── Pages ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _auth_redirect(request)
    modules = await manager.list_modules()
    return templates.TemplateResponse("shell.html", {
        "request": request, "user": user, "modules": modules, "rp": _rp(request),
    })


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _auth_redirect(request)
    if not require_admin(user):
        return RedirectResponse(_rp(request) + "/", status_code=303)
    return templates.TemplateResponse("settings.html", {
        "request": request, "user": user, "rp": _rp(request),
    })


# ── Modules API ─────────────────────────────────────────────────────────────────

@app.get("/api/modules")
async def api_list(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _unauth_json()
    return await manager.list_modules()


@app.post("/api/modules/upload")
async def api_upload(request: Request, file: UploadFile = File(...)):
    user = await verify_token_from_request(request)
    if not user or user["role"] not in ("admin", "editor"):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    if not file.filename.endswith(".zip"):
        return JSONResponse({"error": "Только .zip файлы"}, status_code=400)

    zip_path = UPLOADS_DIR / file.filename
    async with aiofiles.open(zip_path, "wb") as f:
        content = await file.read()
        await f.write(content)

    try:
        meta = await manager.install_from_zip(zip_path, app)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    finally:
        zip_path.unlink(missing_ok=True)

    return meta


@app.post("/api/modules/{module_id}/unload")
async def api_unload(module_id: str, request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    try:
        await manager.unload(module_id, app)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"ok": True}


@app.post("/api/modules/{module_id}/pause")
async def api_pause(module_id: str, request: Request):
    user = await verify_token_from_request(request)
    if not user or user["role"] not in ("admin", "editor"):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    try:
        await manager.pause(module_id, app)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"ok": True}


@app.post("/api/modules/{module_id}/resume")
async def api_resume(module_id: str, request: Request):
    user = await verify_token_from_request(request)
    if not user or user["role"] not in ("admin", "editor"):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    try:
        await manager.resume(module_id, app)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"ok": True}
