import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import psutil
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from orchestrator.auth import (
    ENV_PATH, can_access_module, ensure_default_users, require_admin,
    _read_env_values, enforce_rate_limit,
    router as auth_router, verify_token_from_request,
)
from orchestrator.core import ModuleManager, UPLOADS_DIR
from orchestrator.db import init_db, update_module_status
from orchestrator.vk_poll import shared_vk_poll_hub

BASE_DIR = Path(__file__).parent
UPLOADS_DIR.mkdir(exist_ok=True)
MAX_MODULE_ZIP_BYTES = 100 * 1024 * 1024
LOGIN_BODY_LIMIT_BYTES = 64 * 1024
ADMIN_BODY_LIMIT_BYTES = 2 * 1024 * 1024
MODULE_UPLOAD_BODY_LIMIT_BYTES = 105 * 1024 * 1024

manager = ModuleManager(BASE_DIR)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await ensure_default_users()
    await manager.restore_active_modules(app)
    try:
        yield
    finally:
        await shared_vk_poll_hub.shutdown()


app = FastAPI(lifespan=lifespan, title="Nexus Orchestrator")
app.include_router(auth_router)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _rp(request: Request) -> str:
    return request.scope.get("root_path", "")


def _auth_redirect(request: Request):
    return RedirectResponse(_rp(request) + "/login", status_code=303)


def _unauth_json():
    return JSONResponse({"error": "unauthorized"}, status_code=401)


def _request_body_limit(path: str) -> int | None:
    if path in {"/login", "/sales-chats/api/login", "/sbkvd-gpt/api/login"}:
        return LOGIN_BODY_LIMIT_BYTES
    if path == "/api/modules/upload":
        return MODULE_UPLOAD_BODY_LIMIT_BYTES
    if path.startswith("/api/settings/"):
        return ADMIN_BODY_LIMIT_BYTES
    return None


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    try:
        return os.uname().machine
    except Exception:
        return ""


async def _visible_modules_for(user: dict) -> list[dict]:
    modules = await manager.list_modules()
    if require_admin(user):
        return modules
    return [m for m in modules if can_access_module(user, m["id"])]


def _can_manage_module(user: dict | None, module_id: str) -> bool:
    if not user or user["role"] not in ("admin", "editor"):
        return False
    return can_access_module(user, module_id)


@app.middleware("http")
async def module_panel_access_middleware(request: Request, call_next):
    request_path = request.scope.get("path", "")
    root_path = request.scope.get("root_path", "") or ""
    if root_path and request_path.startswith(root_path):
        request_path = request_path[len(root_path):] or "/"
    body_limit = _request_body_limit(request_path)
    content_length = request.headers.get("content-length", "").strip()
    if body_limit is not None and content_length:
        try:
            if int(content_length) > body_limit:
                return JSONResponse({"error": "Тело запроса слишком большое"}, status_code=413)
        except ValueError:
            return JSONResponse({"error": "Некорректный Content-Length"}, status_code=400)
    parts = [p for p in request.scope.get("path", "").strip("/").split("/") if p]
    if "panel" in parts:
        panel_idx = parts.index("panel")
        module_id = parts[panel_idx - 1] if panel_idx > 0 else ""
        panel_tail = parts[panel_idx + 1 :]
        if module_id == "sbkvd-gpt" and panel_tail[:1] == ["chat"]:
            return await call_next(request)
        if module_id == "sales-chats" and panel_tail[:1] == ["chat"]:
            if len(panel_tail) > 1 and panel_tail[-1] != "index.html":
                root_path = request.scope.get("root_path", "") or ""
                rewritten = f"{root_path}/sales-chats/panel/chat/index.html"
                request.scope["path"] = rewritten
                request.scope["raw_path"] = rewritten.encode()
            return await call_next(request)
        modules = await manager.list_modules()
        if any(m["id"] == module_id for m in modules):
            user = await verify_token_from_request(request)
            if not user:
                return _auth_redirect(request)
            if not can_access_module(user, module_id):
                return PlainTextResponse("Недостаточно прав", status_code=403)
    return await call_next(request)


# ── Pages ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _auth_redirect(request)
    modules = await _visible_modules_for(user)
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


@app.get("/sales-chats/panel/chat/{chat_path:path}")
async def sales_chats_panel_chat_deep_link(chat_path: str):
    runtime = BASE_DIR / "modules" / "sales-chats" / "panel" / "chat" / "index.html"
    source = BASE_DIR / "module_sales_chats" / "panel" / "chat" / "index.html"
    path = runtime if runtime.exists() else source
    if not path.exists():
        return PlainTextResponse("sales-chats chat panel not found", status_code=404)
    return FileResponse(str(path), media_type="text/html; charset=utf-8")


# ── Modules API ─────────────────────────────────────────────────────────────────

@app.get("/api/modules")
async def api_list(request: Request):
    user = await verify_token_from_request(request)
    if not user:
        return _unauth_json()
    return await _visible_modules_for(user)


@app.post("/api/modules/upload")
async def api_upload(request: Request, file: UploadFile | None = File(None)):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    enforce_rate_limit(request, "nexus-module-upload", limit=20, window_seconds=3600, subject=user["username"])
    if not file:
        return JSONResponse({"error": "Файл не передан"}, status_code=400)
    raw_name = (file.filename or "").replace("\\", "/")
    safe_name = Path(raw_name).name
    if not safe_name or safe_name in {".", ".."} or not safe_name.lower().endswith(".zip"):
        return JSONResponse({"error": "Только .zip файлы"}, status_code=400)

    zip_path = UPLOADS_DIR / f"{int(time.time() * 1000)}-{safe_name}"
    size = 0
    too_large = False
    try:
        async with aiofiles.open(zip_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_MODULE_ZIP_BYTES:
                    too_large = True
                    break
                await f.write(chunk)
    finally:
        await file.close()
    if too_large:
        zip_path.unlink(missing_ok=True)
        return JSONResponse({"error": "ZIP файл слишком большой"}, status_code=413)

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
    if not _can_manage_module(user, module_id):
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

    modules = await _visible_modules_for(user)
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
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "cpu_model": _cpu_model(),
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


@app.get("/healthz", include_in_schema=False)
async def healthz():
    """Minimal readiness probe for nginx/systemd; does not expose module details."""
    try:
        modules = await manager.list_modules()
    except Exception:
        return JSONResponse({"ok": False}, status_code=503)
    if any(module.get("status") == "error" for module in modules):
        return JSONResponse({"ok": False}, status_code=503)
    return {"ok": True}


@app.post("/api/modules/{module_id}/resume")
async def api_resume(module_id: str, request: Request):
    user = await verify_token_from_request(request)
    if not _can_manage_module(user, module_id):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    try:
        await manager.resume(module_id, app)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"ok": True}
