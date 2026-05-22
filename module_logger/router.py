import asyncio
import tempfile
from pathlib import Path

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

router = APIRouter()
_modules_dir: Path = None
_log_dir: Path = None   # data/logs/ самого логгера (не используется для чтения)


def setup(ctx):
    global _modules_dir, _log_dir
    _modules_dir = ctx.module_dir.parent   # .../nexus/modules/
    _log_dir = ctx.module_dir / "data" / "logs"
    _log_dir.mkdir(parents=True, exist_ok=True)


# ── Module list ──────────────────────────────────────────────────────────────

@router.get("/modules")
async def list_modules():
    result = [{"id": "nexus", "name": "Nexus (system)", "size": 0, "has_logs": True}]
    if not _modules_dir:
        return result
    for d in sorted(_modules_dir.iterdir()):
        if not d.is_dir():
            continue
        log_file = d / "data" / "logs" / "module.log"
        result.append({
            "id": d.name,
            "name": d.name,
            "has_logs": log_file.exists(),
            "size": log_file.stat().st_size if log_file.exists() else 0,
        })
    return result


# ── Read logs ────────────────────────────────────────────────────────────────

@router.get("/logs/nexus")
async def get_nexus_logs(lines: int = 300):
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
async def get_module_logs(module_id: str, lines: int = 300):
    if not _modules_dir:
        return PlainTextResponse("[логгер не инициализирован]")
    log_file = _modules_dir / module_id / "data" / "logs" / "module.log"
    if not log_file.exists():
        return PlainTextResponse(f"[лог файл отсутствует: {log_file.name}]")
    text = log_file.read_text(errors="replace")
    log_lines = text.splitlines()
    return PlainTextResponse("\n".join(log_lines[-lines:]))


# ── Clear logs ───────────────────────────────────────────────────────────────

@router.delete("/logs/nexus")
async def clear_nexus_logs():
    return JSONResponse({"error": "Системные логи очищаются через journalctl на сервере"}, status_code=400)


@router.delete("/logs/{module_id}")
async def clear_module_logs(module_id: str):
    if not _modules_dir:
        return JSONResponse({"error": "не инициализировано"}, status_code=500)
    log_file = _modules_dir / module_id / "data" / "logs" / "module.log"
    if log_file.exists():
        log_file.write_text("", encoding="utf-8")
    return {"ok": True}


# ── Download logs ────────────────────────────────────────────────────────────

@router.get("/logs/nexus/download")
async def download_nexus_logs():
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
async def download_module_logs(module_id: str):
    if not _modules_dir:
        return JSONResponse({"error": "не инициализировано"}, status_code=500)
    log_file = _modules_dir / module_id / "data" / "logs" / "module.log"
    if not log_file.exists():
        return JSONResponse({"error": "файл не найден"}, status_code=404)
    return FileResponse(str(log_file), filename=f"{module_id}.log", media_type="text/plain")


# ── WebSocket live tail ──────────────────────────────────────────────────────

@router.websocket("/ws/{module_id}")
async def ws_tail(websocket: WebSocket, module_id: str):
    await websocket.accept()
    proc = None
    try:
        if module_id == "nexus":
            proc = await asyncio.create_subprocess_exec(
                "journalctl", "-u", "nexus", "-f", "-n", "80", "--no-pager", "--output=short-iso",
                stdout=asyncio.subprocess.PIPE,
            )
        else:
            if not _modules_dir:
                await websocket.send_text("[логгер не инициализирован]\n")
                return
            log_file = _modules_dir / module_id / "data" / "logs" / "module.log"
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
