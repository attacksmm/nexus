import asyncio
import re
import tempfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from orchestrator.auth import can_access_module, verify_token_from_request, verify_token_value

router = APIRouter()
_modules_dir: Path = None
_log_dir: Path = None   # data/logs/ самого логгера (не используется для чтения)
SAFE_MODULE_ID = re.compile(r"^[A-Za-z0-9_-]+$")
MODULE_ID = "logger"


def setup(ctx):
    global _modules_dir, _log_dir
    _modules_dir = ctx.module_dir.parent   # .../nexus/modules/
    _log_dir = ctx.module_dir / "data" / "logs"
    _log_dir.mkdir(parents=True, exist_ok=True)


# ── Module list ──────────────────────────────────────────────────────────────

@router.get("/modules")
async def list_modules(request: Request):
    await _require_panel_user(request)
    result = [{"id": "nexus", "name": "Nexus (system)", "size": 0, "has_logs": True}]
    if not _modules_dir:
        return result
    for module in await _list_real_modules():
        log_file = _modules_dir / module["id"] / "data" / "logs" / "module.log"
        result.append({
            "id": module["id"],
            "name": module["name"],
            "has_logs": log_file.exists(),
            "size": log_file.stat().st_size if log_file.exists() else 0,
        })
    return result


async def _list_real_modules() -> list[dict]:
    """Return modules registered in Nexus, not every backup directory on disk."""
    try:
        from orchestrator.db import get_modules_by_status
        rows = await get_modules_by_status()
        result = []
        for row in rows:
            module_id = row.get("id", "")
            if _valid_module_id(module_id):
                result.append({"id": module_id, "name": row.get("name") or module_id})
        return result
    except Exception:
        # Fallback for early startup or DB errors: still hide backup folders.
        if not _modules_dir:
            return []
        return [
            {"id": d.name, "name": d.name}
            for d in sorted(_modules_dir.iterdir())
            if d.is_dir() and _valid_module_id(d.name)
        ]


async def _module_log_file(module_id: str) -> Path | None:
    if not _modules_dir or not _valid_module_id(module_id):
        return None
    real_ids = {m["id"] for m in await _list_real_modules()}
    if module_id not in real_ids:
        return None
    return _modules_dir / module_id / "data" / "logs" / "module.log"


def _valid_module_id(module_id: str) -> bool:
    return bool(module_id and SAFE_MODULE_ID.match(module_id))


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


async def _require_ws_user(websocket: WebSocket) -> dict | None:
    user = await verify_token_value(websocket.cookies.get("nexus_token"))
    if not user or not can_access_module(user, MODULE_ID):
        await websocket.close(code=1008)
        return None
    return user


# ── Read logs ────────────────────────────────────────────────────────────────

@router.get("/logs/nexus")
async def get_nexus_logs(request: Request, lines: int = 300):
    await _require_panel_user(request)
    try:
        proc = await asyncio.create_subprocess_exec(
            "journalctl", "-u", "nexus", "-n", str(lines),
            "--no-pager", "--output=short-iso",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        return PlainTextResponse(stdout.decode("utf-8", errors="replace"))
    except Exception as e:
        return PlainTextResponse(f"[ошибка journalctl: {e}]")


@router.get("/logs/{module_id}")
async def get_module_logs(module_id: str, request: Request, lines: int = 300):
    await _require_panel_user(request)
    log_file = await _module_log_file(module_id)
    if not log_file:
        return PlainTextResponse("[модуль не найден]", status_code=404)
    if not log_file.exists():
        return PlainTextResponse(f"[лог файл отсутствует: {log_file.name}]")
    text = log_file.read_text(errors="replace")
    log_lines = text.splitlines()
    return PlainTextResponse("\n".join(log_lines[-lines:]))


# ── Clear logs ───────────────────────────────────────────────────────────────

@router.delete("/logs/nexus")
async def clear_nexus_logs(request: Request):
    await _require_panel_user(request)
    return JSONResponse({"error": "Системные логи очищаются через journalctl на сервере"}, status_code=400)


@router.delete("/logs/{module_id}")
async def clear_module_logs(module_id: str, request: Request):
    await _require_panel_user(request)
    log_file = await _module_log_file(module_id)
    if not log_file:
        return JSONResponse({"error": "модуль не найден"}, status_code=404)
    return {
        "ok": True,
        "mode": "view-only",
        "message": "Файл лога не изменен. Очистка окна выполняется в интерфейсе.",
    }


@router.post("/logs/{module_id}/rotate")
async def rotate_module_logs(module_id: str, request: Request):
    await _require_panel_user(request)
    log_file = await _module_log_file(module_id)
    if not log_file:
        return JSONResponse({"error": "модуль не найден"}, status_code=404)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = log_file.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    archive_file = archive_dir / f"{module_id}-{stamp}.log"
    size = log_file.stat().st_size if log_file.exists() else 0
    if log_file.exists() and size > 0:
        archive_file.write_bytes(log_file.read_bytes())
    else:
        archive_file.write_text("", encoding="utf-8")
    log_file.write_text("", encoding="utf-8")
    return {
        "ok": True,
        "module_id": module_id,
        "archived": archive_file.name,
        "archive_path": str(archive_file),
        "size": size,
    }


# ── Download logs ────────────────────────────────────────────────────────────

@router.get("/logs/nexus/download")
async def download_nexus_logs(request: Request):
    await _require_panel_user(request)
    try:
        proc = await asyncio.create_subprocess_exec(
            "journalctl", "-u", "nexus", "--no-pager",
            stdout=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        tmp = Path(tempfile.mktemp(suffix=".log"))
        tmp.write_bytes(stdout)
        return FileResponse(str(tmp), filename="nexus-system.log", media_type="text/plain")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/logs/{module_id}/download")
async def download_module_logs(module_id: str, request: Request):
    await _require_panel_user(request)
    log_file = await _module_log_file(module_id)
    if not log_file:
        return JSONResponse({"error": "модуль не найден"}, status_code=404)
    if not log_file.exists():
        return JSONResponse({"error": "файл не найден"}, status_code=404)
    return FileResponse(str(log_file), filename=f"{module_id}.log", media_type="text/plain")


# ── WebSocket live tail ──────────────────────────────────────────────────────

@router.websocket("/ws/{module_id}")
async def ws_tail(websocket: WebSocket, module_id: str):
    if not await _require_ws_user(websocket):
        return
    await websocket.accept()
    proc = None
    try:
        if module_id == "nexus":
            proc = await asyncio.create_subprocess_exec(
                "journalctl", "-u", "nexus", "-f", "-n", "80", "--no-pager", "--output=short-iso",
                stdout=asyncio.subprocess.PIPE,
            )
        else:
            log_file = await _module_log_file(module_id)
            if not log_file:
                await websocket.send_text("[модуль не найден]\n")
                return
            log_file.parent.mkdir(parents=True, exist_ok=True)
            if not log_file.exists():
                log_file.touch()
            proc = await asyncio.create_subprocess_exec(
                "tail", "-f", "-n", "80", str(log_file),
                stdout=asyncio.subprocess.PIPE,
            )

        while True:
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=25)
            except asyncio.TimeoutError:
                await websocket.send_text("")   # keepalive
                continue
            if not line:
                break
            await websocket.send_text(line.decode("utf-8", errors="replace"))

    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if proc and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
