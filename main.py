import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import psutil
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from orchestrator.auth import (
    ENV_PATH, ensure_default_users, require_admin,
    _read_env_values,
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


@app.get("/api/env/check")
async def api_env_check(request: Request, keys: str = ""):
    """Проверяет наличие переменных в os.environ. Значения не возвращаются."""
    user = await verify_token_from_request(request)
    if not user:
        return _unauth_json()
    import os
    key_list = [k.strip() for k in keys.split(",") if k.strip()]
    return {k: bool(os.environ.get(k)) for k in key_list}


@app.get("/api/settings/env/template")
async def api_env_template(request: Request):
    """Генерирует безопасный .env шаблон только для недостающих ключей."""
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)

    modules = await manager.list_modules()
    configured = {k for k, v in _read_env_values().items() if v}
    configured.update(k for k, v in os.environ.items() if v)

    required: dict[str, dict] = {
        "NEXUS_SECRET": {
            "desc": "JWT секрет, минимум 32 символа. Смена требует повторного входа.",
            "modules": ["Nexus"],
        }
    }

    for m in modules:
        try:
            manifest = json.loads(m.get("manifest_json", "{}"))
            env_vars = manifest.get("env_vars", {})
        except Exception:
            env_vars = {}
        required_keys = manifest.get("env_required")
        if isinstance(required_keys, list):
            env_items = [(key, env_vars.get(key, "")) for key in required_keys]
        else:
            env_items = list(env_vars.items())
        for key, desc in env_items:
            entry = required.setdefault(key, {"desc": desc, "modules": []})
            if desc and not entry.get("desc"):
                entry["desc"] = desc
            entry["modules"].append(m["name"])

    missing = {k: v for k, v in required.items() if k not in configured}
    lines = [
        f"# Nexus Orchestrator — шаблон недостающих переменных окружения",
        f"# Сгенерировано: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "# Заполните только пустые значения и загрузите через «Применить».",
        "# Уже настроенные ключи не включены: Nexus сохранит их при загрузке этого файла.",
        f"# Источник текущих значений: {ENV_PATH}",
        "",
    ]

    if not missing:
        lines.extend([
            "# Все обязательные ENV ключи установленных модулей уже есть.",
            "# Добавьте сюда KEY=value вручную, если нужно изменить или добавить ключ.",
        ])
    else:
        for key, meta in missing.items():
            modules_text = ", ".join(dict.fromkeys(meta["modules"]))
            lines.append(f"# Модули: {modules_text}")
            if meta.get("desc"):
                lines.append(f"# {meta['desc']}")
            lines.append(f"{key}=")
            lines.append("")

    content = "\n".join(lines)
    return PlainTextResponse(
        content,
        headers={"Content-Disposition": "attachment; filename=\"nexus.env.template\""},
        media_type="text/plain; charset=utf-8",
    )


@app.get("/api/server/stats")
async def api_server_stats(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _unauth_json()
    boot = psutil.boot_time()
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    modules = await manager.list_modules()
    return {
        "cpu_percent": psutil.cpu_percent(interval=0.3),
        "cpu_count": psutil.cpu_count(),
        "ram_total": vm.total,
        "ram_used": vm.used,
        "ram_percent": vm.percent,
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_percent": disk.percent,
        "uptime": int(time.time() - boot),
        "load_avg": list(psutil.getloadavg()),
        "modules": [{"id": m["id"], "name": m["name"], "status": m["status"], "version": m["version"]} for m in modules],
    }


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
