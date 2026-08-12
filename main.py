import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import aiofiles
import psutil
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from orchestrator.auth import (
    ENV_PATH, can_access_module, ensure_default_users, require_admin,
    _read_env_values, _write_env_values, enforce_rate_limit,
    router as auth_router, verify_token_from_request,
)
from orchestrator.core import ModuleManager, UPLOADS_DIR
from orchestrator.credentials import (
    ENV_KEY_RE,
    clean as clean_credential,
    current_value as current_credential_value,
    inventory as credential_inventory,
    now as credentials_now,
    provider_for_key,
    save_value as save_credential_value,
    status_item as credential_status_item,
    validate_known as validate_credential,
)
from orchestrator.db import init_db
from orchestrator.telegram_proxy import (
    BOT_API_BASE_KEYS,
    BOT_API_PROXY_KEYS,
    MTPROTO_PROXY_KEYS,
    masked_proxy,
    telegram_bot_api_base,
    telegram_bot_api_proxy_url,
    telegram_mtproto_proxy_url,
    test_bot_api_route,
    test_mtproto_route,
    validate_bot_api_base,
    validate_bot_api_proxy,
    validate_mtproto_proxy,
)
from orchestrator.vk_poll import shared_vk_poll_hub

BASE_DIR = Path(__file__).parent
UPLOADS_DIR.mkdir(exist_ok=True)
MAX_MODULE_ZIP_BYTES = 100 * 1024 * 1024
LOGIN_BODY_LIMIT_BYTES = 64 * 1024
ADMIN_BODY_LIMIT_BYTES = 2 * 1024 * 1024
MODULE_UPLOAD_BODY_LIMIT_BYTES = 105 * 1024 * 1024

MODULE_LOAD_BRIDGE_MARKER = "data-nexus-module-load-bridge"
MODULE_THEME_STYLE_MARKER = "data-nexus-module-theme"
MODULE_THEME_STYLE = r"""<style data-nexus-module-theme>
:root[data-nexus-theme="dark"]{color-scheme:dark;filter:none;background:#060708!important;--bg:#060708!important;--panel:#060708!important}
:root[data-nexus-theme="dark"] body{background:#060708!important}
:root[data-nexus-theme="gray"]{color-scheme:dark;filter:invert(.18);background:#03060d!important;--bg:#03060d!important;--panel:#03060d!important}
:root[data-nexus-theme="gray"] body{background:#03060d!important}
:root[data-nexus-theme="gray"] img,:root[data-nexus-theme="gray"] video,:root[data-nexus-theme="gray"] canvas{filter:invert(1)}
:root[data-nexus-theme="light"]{color-scheme:dark;filter:invert(1) hue-rotate(180deg);background:#0c0a08!important;--bg:#0c0a08!important;--panel:#0c0a08!important}
:root[data-nexus-theme="light"] body{background:#0c0a08!important}
:root[data-nexus-theme="light"] img,:root[data-nexus-theme="light"] video,:root[data-nexus-theme="light"] canvas{filter:invert(1) hue-rotate(180deg)}
</style>"""
MODULE_LOAD_BRIDGE = r"""<script data-nexus-module-load-bridge>(function(){
  if(window.parent===window)return;
  var themes=["light","gray","dark"];
  function readTheme(){try{var value=window.parent.localStorage.getItem("nexus-theme")||window.parent.localStorage.getItem("nexus-streams-theme");return themes.indexOf(value)>=0?value:"dark"}catch(error){return"dark"}}
  function applyTheme(theme){var value=themes.indexOf(theme)>=0?theme:"dark";document.documentElement.setAttribute("data-nexus-theme",value);document.documentElement.setAttribute("data-theme",value)}
  applyTheme(readTheme());
  var pending=0,domReady=document.readyState!=="loading",ready=false,idleTimer=0;
  function isNonCredentialInput(input){
    if(!input||input.tagName!=="INPUT"||input.disabled)return false;
    if(input.readOnly&&input.getAttribute("data-form-type")!=="other")return false;
    if(input.type!=="text"&&input.type!=="search")return false;
    if(input.defaultValue)return false;
    if(input.classList.contains("no-autofill")||input.classList.contains("search")||input.type==="search")return true;
    var clue=[input.id,input.name,input.placeholder,input.getAttribute("aria-label")].join(" ").toLowerCase();
    return /search|filter|поиск|фильтр|найти/.test(clue);
  }
  function guardInput(input){
    if(!isNonCredentialInput(input)||input.getAttribute("data-nexus-autofill-guard")==="1")return;
    input.setAttribute("data-nexus-autofill-guard","1");
    input.setAttribute("autocomplete","one-time-code");
    input.setAttribute("data-lpignore","true");
    input.setAttribute("data-1p-ignore","true");
    input.setAttribute("data-form-type","other");
    input.readOnly=true;
    function clearAutofill(){if(input.readOnly&&input.value)input.value=""}
    function unlock(event){if(event.isTrusted)input.readOnly=false}
    input.addEventListener("pointerdown",unlock,{once:true});
    input.addEventListener("keydown",unlock,{once:true});
    input.addEventListener("input",clearAutofill);
    input.addEventListener("change",clearAutofill);
    clearAutofill();
  }
  function guardInputs(root){
    if(root.nodeType===1&&root.matches("input"))guardInput(root);
    if(root.querySelectorAll)root.querySelectorAll("input").forEach(guardInput);
  }
  function installAutofillGuard(){
    guardInputs(document);
    new MutationObserver(function(records){records.forEach(function(record){record.addedNodes.forEach(guardInputs)})})
      .observe(document.documentElement,{childList:true,subtree:true});
  }
  function post(state){window.parent.postMessage({type:"nexus:module-load",state:state,path:location.pathname},location.origin)}
  function settle(){
    clearTimeout(idleTimer);
    if(ready||!domReady||pending)return;
    idleTimer=setTimeout(function(){if(!ready&&domReady&&!pending){ready=true;post("ready")}},140);
  }
  function begin(){if(ready)return false;pending+=1;post("busy");return true}
  function end(tracked){if(!tracked)return;pending=Math.max(0,pending-1);settle()}
  var nativeFetch=window.fetch;
  if(typeof nativeFetch==="function")window.fetch=function(){
    var tracked=begin(),result;
    try{result=nativeFetch.apply(this,arguments)}catch(error){end(tracked);throw error}
    return Promise.resolve(result).finally(function(){end(tracked)});
  };
  if(window.XMLHttpRequest){
    var nativeSend=XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send=function(){
      var request=this,tracked=begin();
      if(tracked)request.addEventListener("loadend",function(){end(true)},{once:true});
      try{return nativeSend.apply(request,arguments)}catch(error){end(tracked);throw error}
    };
  }
  window.addEventListener("message",function(event){
    if(event.origin!==location.origin||event.source!==window.parent)return;
    var data=event.data||{};
    if(data.type==="nexus:theme")applyTheme(data.theme);
  });
  document.addEventListener("DOMContentLoaded",function(){domReady=true;installAutofillGuard();settle()},{once:true});
  if(domReady){installAutofillGuard();settle()}
})();</script>"""

manager = ModuleManager(BASE_DIR)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager.install_lifecycle_tracking()
    try:
        await init_db()
        await ensure_default_users()
        await manager.restore_active_modules(app)
        yield
    finally:
        try:
            await shared_vk_poll_hub.shutdown(stop_timeout=2)
        finally:
            try:
                await manager.shutdown_all(app, timeout=10)
            finally:
                manager.uninstall_lifecycle_tracking()


app = FastAPI(
    lifespan=lifespan,
    title="Nexus Orchestrator",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["junior.sobakovod.pro", "127.0.0.1", "localhost", "testserver"],
)
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


def _app_request_path(request: Request) -> str:
    path = request.scope.get("path", "")
    root_path = request.scope.get("root_path", "") or ""
    if root_path and path.startswith(root_path):
        return path[len(root_path):] or "/"
    return path


def _is_module_panel_index(path: str) -> bool:
    parts = [part for part in path.strip("/").split("/") if part]
    return len(parts) == 3 and parts[1] == "panel" and parts[2] == "index.html"


def _inject_module_load_bridge(html: str) -> str:
    lower = html.lower()
    head_start = lower.find("<head")
    head_end = lower.find(">", head_start) if head_start >= 0 else -1
    if MODULE_LOAD_BRIDGE_MARKER in html or head_end < 0:
        return html
    index = head_end + 1
    return html[:index] + MODULE_LOAD_BRIDGE + MODULE_THEME_STYLE + html[index:]


async def _bridge_module_panel_response(response: Response) -> Response:
    if response.status_code != 200 or "text/html" not in response.headers.get("content-type", "").lower():
        return response
    body = b"".join([chunk async for chunk in response.body_iterator])
    injected = _inject_module_load_bridge(body.decode("utf-8", errors="replace")).encode("utf-8")
    headers = dict(response.headers)
    headers.pop("content-length", None)
    headers.pop("etag", None)
    headers.pop("last-modified", None)
    headers["cache-control"] = "no-store"
    return Response(
        content=injected,
        status_code=response.status_code,
        headers=headers,
        background=response.background,
    )


def _cross_origin_cookie_request(request: Request) -> bool:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False
    if not request.cookies.get("nexus_token"):
        return False
    if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
        return True
    origin = request.headers.get("origin", "").strip()
    if not origin:
        return False
    parsed = urlsplit(origin)
    return parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != request.headers.get("host", "").lower()


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
async def browser_security_middleware(request: Request, call_next):
    if _cross_origin_cookie_request(request):
        return JSONResponse({"error": "cross-origin request rejected"}, status_code=403)
    response = await call_next(request)
    path = _app_request_path(request)
    if path in {"/login", "/settings"} or path.startswith("/api/settings/") or path.startswith("/token-vault/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.middleware("http")
async def module_lifecycle_scope_middleware(request: Request, call_next):
    request_path = _app_request_path(request)
    parts = [part for part in request_path.strip("/").split("/") if part]
    lifecycle = manager.lifecycle_for(parts[0]) if parts else None
    if lifecycle is not None:
        with lifecycle.activate():
            response = await call_next(request)
    else:
        response = await call_next(request)
    if request.method == "GET" and _is_module_panel_index(request_path):
        return await _bridge_module_panel_response(response)
    return response


@app.middleware("http")
async def module_panel_access_middleware(request: Request, call_next):
    request_path = _app_request_path(request)
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


@app.get("/api/lifecycle")
async def api_lifecycle(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    return {
        "modules": manager.lifecycle_snapshot(),
        "vk_poll": shared_vk_poll_hub.snapshot(),
    }


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


async def _credential_items(*, validate: bool = False) -> list[dict]:
    rows = await manager.list_modules()
    items = credential_inventory(rows, read_env=_read_env_values)
    return [
        await credential_status_item(
            item,
            base_dir=BASE_DIR,
            validate=validate,
            read_env=_read_env_values,
        )
        for item in items
    ]


@app.get("/api/settings/credentials")
async def api_credentials(request: Request, validate: int = 0):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    return {
        "ok": True,
        "items": await _credential_items(validate=bool(validate)),
        "env_path": str(ENV_PATH),
        "updated_at": credentials_now(),
    }


@app.post("/api/settings/credentials/validate")
async def api_credentials_validate(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    enforce_rate_limit(
        request,
        "nexus-credentials-validate",
        limit=60,
        window_seconds=3600,
        subject=user["username"],
    )
    data = await request.json()
    key = clean_credential(data.get("key"), 120)
    if not ENV_KEY_RE.fullmatch(key):
        return JSONResponse({"error": "Некорректный ENV ключ"}, status_code=400)
    value = str(data.get("value") or "").strip() or current_credential_value(
        key, read_env=_read_env_values
    )
    validation = await validate_credential(
        key,
        value,
        base_dir=BASE_DIR,
        read_env=_read_env_values,
    )
    return {"ok": True, "key": key, "validation": validation}


@app.post("/api/settings/credentials")
async def api_credentials_save(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    enforce_rate_limit(
        request,
        "nexus-credentials-save",
        limit=30,
        window_seconds=3600,
        subject=user["username"],
    )
    data = await request.json()
    key = clean_credential(data.get("key"), 120)
    value = str(data.get("value") or "").strip()
    if not ENV_KEY_RE.fullmatch(key):
        return JSONResponse({"error": "Некорректный ENV ключ"}, status_code=400)
    if not value:
        return JSONResponse({"error": "Значение пустое"}, status_code=400)
    validation = (
        await validate_credential(
            key,
            value,
            base_dir=BASE_DIR,
            read_env=_read_env_values,
        )
        if data.get("validate", True)
        else {"status": "unchecked", "message": "проверка пропущена"}
    )
    if validation["status"] in {"invalid", "error"} and not data.get("force"):
        return {"ok": False, "key": key, "validation": validation, "saved": False}

    result = await save_credential_value(
        key,
        value,
        validation=validation,
        read_env=_read_env_values,
        write_env=_write_env_values,
        restart=lambda changed_key: manager.restart_modules_for_env(changed_key, app),
    )
    result["item"] = await credential_status_item(
        {
            "key": key,
            "description": "",
            "required": False,
            "modules": [],
            "provider": provider_for_key(key),
        },
        base_dir=BASE_DIR,
        read_env=_read_env_values,
    )
    return result


def _telegram_test_token() -> str:
    for key in (
        "SBKVD_LETTER_TELEGRAM_BOT_TOKEN",
        "TELEGRAM_BOT_TOKEN_MODERATOR",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_BOT_TOKEN_ERROR_ALERT",
    ):
        value = str(os.environ.get(key) or "").strip()
        if value:
            return value
    return ""


def _telegram_settings_status() -> dict:
    bot_proxy = telegram_bot_api_proxy_url()
    mtproto_proxy = telegram_mtproto_proxy_url()
    env = _read_env_values()
    bot_alias_values = {env.get(key, "") for key in BOT_API_PROXY_KEYS if env.get(key, "")}
    mt_alias_values = {env.get(key, "") for key in MTPROTO_PROXY_KEYS if env.get(key, "")}
    return {
        "bot_api_base": telegram_bot_api_base(),
        "bot_api_proxy": {
            "configured": bool(bot_proxy),
            "masked": masked_proxy(bot_proxy, kind="bot"),
            "aliases_synced": len(bot_alias_values) <= 1,
        },
        "mtproto_proxy": {
            "configured": bool(mtproto_proxy),
            "masked": masked_proxy(mtproto_proxy, kind="mtproto"),
            "aliases_synced": len(mt_alias_values) <= 1,
        },
    }


async def _telegram_proxy_changed_hooks() -> list[dict]:
    results: list[dict] = []
    for module_id, module in list(manager._loaded.items()):
        hook = getattr(module, "on_telegram_proxy_changed", None)
        if not callable(hook):
            continue
        try:
            value = hook()
            if hasattr(value, "__await__"):
                value = await asyncio.wait_for(value, timeout=30)
            results.append({"module": module_id, "ok": True, "result": value})
        except Exception as exc:
            results.append({"module": module_id, "ok": False, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
    return results


@app.get("/api/settings/telegram")
async def api_telegram_settings(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    return {"ok": True, **_telegram_settings_status()}


async def _test_telegram_candidates(*, bot_api_base: str, bot_api_proxy: str, mtproto_proxy: str) -> dict:
    bot_result, mtproto_result = await asyncio.gather(
        test_bot_api_route(base=bot_api_base, proxy=bot_api_proxy, token=_telegram_test_token()),
        test_mtproto_route(proxy=mtproto_proxy) if mtproto_proxy else asyncio.sleep(
            0, result={"ok": True, "duration_ms": 0, "message": "MTProto proxy отключён"}
        ),
    )
    return {"bot_api": bot_result, "mtproto": mtproto_result, "ok": bool(bot_result.get("ok") and mtproto_result.get("ok"))}


@app.post("/api/settings/telegram/test")
async def api_telegram_settings_test(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    enforce_rate_limit(request, "nexus-telegram-proxy-test", limit=30, window_seconds=3600, subject=user["username"])
    data = await request.json()
    try:
        bot_api_base = validate_bot_api_base(data.get("bot_api_base") or telegram_bot_api_base())
        bot_api_proxy = validate_bot_api_proxy(data.get("bot_api_proxy") or telegram_bot_api_proxy_url())
        mtproto_proxy = validate_mtproto_proxy(data.get("mtproto_proxy") or telegram_mtproto_proxy_url())
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    tests = await _test_telegram_candidates(
        bot_api_base=bot_api_base,
        bot_api_proxy=bot_api_proxy,
        mtproto_proxy=mtproto_proxy,
    )
    return {"ok": tests["ok"], "tests": tests}


@app.post("/api/settings/telegram")
async def api_telegram_settings_save(request: Request):
    user = await verify_token_from_request(request)
    if not require_admin(user):
        return JSONResponse({"error": "Недостаточно прав"}, status_code=403)
    enforce_rate_limit(request, "nexus-telegram-proxy-save", limit=10, window_seconds=3600, subject=user["username"])
    data = await request.json()
    current_base = telegram_bot_api_base()
    current_bot_proxy = telegram_bot_api_proxy_url()
    current_mtproto = telegram_mtproto_proxy_url()
    try:
        bot_api_base = validate_bot_api_base(data.get("bot_api_base") or current_base)
        bot_api_proxy = validate_bot_api_proxy(data.get("bot_api_proxy") or current_bot_proxy)
        mtproto_proxy = validate_mtproto_proxy(data.get("mtproto_proxy") or current_mtproto)
        if not bot_api_proxy or not mtproto_proxy:
            raise ValueError("Для production Nexus должны быть заданы Bot API и MTProto proxy")
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    tests = await _test_telegram_candidates(
        bot_api_base=bot_api_base,
        bot_api_proxy=bot_api_proxy,
        mtproto_proxy=mtproto_proxy,
    )
    if not tests["ok"] and not data.get("force"):
        return JSONResponse({"error": "Новые маршруты не прошли проверку", "tests": tests}, status_code=422)

    env = _read_env_values()
    updates: dict[str, str] = {}
    for key in BOT_API_BASE_KEYS:
        updates[key] = bot_api_base
    for key in BOT_API_PROXY_KEYS:
        updates[key] = bot_api_proxy
    for key in MTPROTO_PROXY_KEYS:
        updates[key] = mtproto_proxy
    changed = [key for key, value in updates.items() if env.get(key, "") != value]
    for key, value in updates.items():
        if value:
            env[key] = value
            os.environ[key] = value
        else:
            env.pop(key, None)
            os.environ.pop(key, None)
    _write_env_values(env)
    hooks = await _telegram_proxy_changed_hooks() if changed else []
    return {
        "ok": True,
        "changed_keys": changed,
        "tests": tests,
        "hooks": hooks,
        **_telegram_settings_status(),
    }


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
