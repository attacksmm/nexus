import asyncio
import importlib.util
import json
import logging
import logging.handlers
import re
import shutil
import sys
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from types import ModuleType

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from orchestrator.db import get_modules_by_status, update_module_status, upsert_module
from orchestrator.lifecycle import LifecycleSupervisor

MODULES_DIR = Path(__file__).parent.parent / "modules"
UPLOADS_DIR = Path(__file__).parent.parent / "uploads"

REQUIRED_MANIFEST_KEYS = {"id", "name", "version"}
MAX_ZIP_FILES = 500
MAX_ZIP_UNCOMPRESSED_BYTES = 200 * 1024 * 1024
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_module_logger(module_id: str, log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "module.log"
    logger = logging.getLogger(f"nexus.mod.{module_id}")
    if not logger.handlers:
        h = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h)
    logger.setLevel(logging.DEBUG)
    return logger


class ModuleContext:
    def __init__(self, module_id: str, module_dir: Path):
        self.module_id = module_id
        self.module_dir = module_dir
        self.data_dir = module_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / f"{module_id}.db"
        self.lifecycle = None
        self.restart_modules_for_env = None


class ModuleManager:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self._loaded: dict[str, ModuleType] = {}
        self._contexts: dict[str, ModuleContext] = {}
        self._supervisor = LifecycleSupervisor()
        self._env_restart_lock = asyncio.Lock()

    def install_lifecycle_tracking(self) -> None:
        self._supervisor.install()

    def uninstall_lifecycle_tracking(self) -> None:
        self._supervisor.uninstall()

    def lifecycle_snapshot(self) -> list[dict]:
        return self._supervisor.snapshot()

    def lifecycle_for(self, module_id: str):
        return self._supervisor.get(module_id)

    # ── Public API ─────────────────────────────────────────────────────────────

    async def install_from_zip(self, zip_path: Path, app: FastAPI) -> dict:
        manifest, staging_dir = self._extract_zip_to_staging(zip_path)
        module_id = manifest["id"]
        module_dir = MODULES_DIR / module_id
        rollback_dir = MODULES_DIR / f".rollback-{module_id}-{uuid.uuid4().hex}"
        old_meta = next((row for row in await get_modules_by_status() if row["id"] == module_id), None)
        old_exists = module_dir.exists()
        meta = {
            "id": module_id,
            "name": manifest["name"],
            "version": manifest.get("version", "0.0.0"),
            "description": manifest.get("description", ""),
            "status": "active",
            "installed_at": datetime.now(timezone.utc).isoformat(),
            "manifest_json": json.dumps(manifest, ensure_ascii=False),
        }
        try:
            await self._unmount_module(module_id, app)
            if old_exists:
                module_dir.rename(rollback_dir)
                old_data = rollback_dir / "data"
                if old_data.exists():
                    staged_data = staging_dir / "data"
                    if staged_data.exists():
                        shutil.rmtree(staged_data)
                    old_data.rename(staged_data)
            staging_dir.rename(module_dir)
            await self._mount_module(module_id, module_dir, app)
            await upsert_module(meta)
        except Exception:
            await self._unmount_module(module_id, app)
            failed_dir = MODULES_DIR / f".failed-{module_id}-{uuid.uuid4().hex}"
            if module_dir.exists():
                module_dir.rename(failed_dir)
            if old_exists and rollback_dir.exists():
                failed_data = failed_dir / "data"
                staged_data = staging_dir / "data"
                recovered_data = failed_data if failed_data.exists() else staged_data
                if recovered_data.exists():
                    rollback_data = rollback_dir / "data"
                    if rollback_data.exists():
                        shutil.rmtree(rollback_data)
                    recovered_data.rename(rollback_data)
                rollback_dir.rename(module_dir)
                if old_meta and old_meta.get("status") == "active":
                    await self._mount_module(module_id, module_dir, app)
            if failed_dir.exists():
                shutil.rmtree(failed_dir, ignore_errors=True)
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            raise
        else:
            if rollback_dir.exists():
                shutil.rmtree(rollback_dir, ignore_errors=True)
            return meta

    async def pause(self, module_id: str, app: FastAPI):
        await self._unmount_module(module_id, app)
        await update_module_status(module_id, "paused")

    async def resume(self, module_id: str, app: FastAPI):
        module_dir = MODULES_DIR / module_id
        if not module_dir.exists():
            raise RuntimeError(f"Module dir not found: {module_dir}")
        await self._mount_module(module_id, module_dir, app)
        await update_module_status(module_id, "active")

    async def list_modules(self) -> list[dict]:
        return await get_modules_by_status()

    async def restart_modules_for_env(
        self,
        key: str,
        app: FastAPI,
        *,
        exclude: set[str] | None = None,
    ) -> dict:
        """Restart active modules that declare an ENV dependency.

        Restarts are deliberately sequential so shared transports can detach
        cleanly before a replacement module instance is mounted. Paused and
        unloaded modules keep their state.
        """

        clean_key = str(key or "").strip()
        if not ENV_KEY_RE.fullmatch(clean_key):
            raise ValueError("Invalid ENV key")
        excluded = {str(module_id) for module_id in (exclude or set())}
        active_rows = await get_modules_by_status("active")
        targets = []
        for row in active_rows:
            if row["id"] in excluded:
                continue
            try:
                manifest = json.loads(row.get("manifest_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                manifest = {}
            env_vars = manifest.get("env_vars") or {}
            required = manifest.get("env_required") or []
            declared = set(env_vars) | (set(required) if isinstance(required, list) else set())
            if clean_key in declared:
                targets.append(row)

        results = []
        async with self._env_restart_lock:
            for row in targets:
                module_id = row["id"]
                started = asyncio.get_running_loop().time()
                try:
                    module_dir = MODULES_DIR / module_id
                    if not module_dir.exists():
                        raise RuntimeError(f"Module dir not found: {module_dir}")
                    await self._unmount_module(module_id, app)
                    await self._mount_module(module_id, module_dir, app)
                    await update_module_status(module_id, "active")
                    status = "active"
                    error = ""
                except Exception as exc:
                    await update_module_status(module_id, "error")
                    status = "error"
                    error = f"{type(exc).__name__}: {str(exc)[:300]}"
                    logging.getLogger("nexus.core").exception(
                        "ENV dependent module restart failed key=%s module=%s",
                        clean_key,
                        module_id,
                    )
                results.append(
                    {
                        "id": module_id,
                        "name": row.get("name") or module_id,
                        "status": status,
                        "duration_ms": round(
                            (asyncio.get_running_loop().time() - started) * 1000
                        ),
                        "error": error,
                    }
                )
        return {
            "ok": all(item["status"] == "active" for item in results),
            "key": clean_key,
            "modules": results,
            "restarted": sum(item["status"] == "active" for item in results),
            "failed": sum(item["status"] == "error" for item in results),
        }

    async def restore_active_modules(self, app: FastAPI):
        for row in await get_modules_by_status("active"):
            module_dir = MODULES_DIR / row["id"]
            if module_dir.exists():
                try:
                    await self._mount_module(row["id"], module_dir, app)
                except Exception as e:
                    await update_module_status(row["id"], "error")
                    print(f"[nexus] Failed to restore {row['id']}: {e}")
            else:
                await update_module_status(row["id"], "error")

    async def shutdown_all(self, app: FastAPI, timeout: float = 10.0) -> None:
        module_ids = reversed(list(self._loaded))

        async def stop_modules() -> None:
            for module_id in module_ids:
                await self._unmount_module(module_id, app)

        try:
            await asyncio.wait_for(stop_modules(), timeout=max(0.1, timeout))
        except TimeoutError:
            remaining = sorted(self._loaded)
            logging.getLogger("nexus.core").error(
                "Global module shutdown deadline exceeded remaining=%s",
                remaining,
            )

    # ── Internals ──────────────────────────────────────────────────────────────

    def _extract_zip_to_staging(self, zip_path: Path) -> tuple[dict, Path]:
        with zipfile.ZipFile(zip_path) as zf:
            if "manifest.json" not in zf.namelist():
                raise ValueError("manifest.json missing in ZIP")
            self._validate_zip_members(zf)
            manifest = json.loads(zf.read("manifest.json"))

        missing = REQUIRED_MANIFEST_KEYS - manifest.keys()
        if missing:
            raise ValueError(f"manifest.json missing keys: {missing}")

        module_id = manifest["id"]
        if not module_id.replace("-", "_").isidentifier():
            raise ValueError(f"Invalid module id: {module_id!r}")

        MODULES_DIR.mkdir(parents=True, exist_ok=True)
        staging_dir = MODULES_DIR / f".staging-{module_id}-{uuid.uuid4().hex}"
        staging_dir.mkdir(parents=True)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(staging_dir)
            router_file = staging_dir / "router.py"
            if router_file.exists():
                compile(router_file.read_bytes(), str(router_file), "exec")
        except Exception:
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise
        return manifest, staging_dir

    @staticmethod
    def _validate_zip_members(zf: zipfile.ZipFile) -> None:
        infos = zf.infolist()
        if len(infos) > MAX_ZIP_FILES:
            raise ValueError(f"Too many files in ZIP: {len(infos)}")
        total_size = 0
        for info in infos:
            name = info.filename
            if not name:
                raise ValueError("ZIP contains empty filename")
            path = PurePosixPath(name)
            if path.is_absolute() or "\\" in name or any(part in {"", ".", ".."} for part in path.parts):
                raise ValueError(f"Unsafe ZIP path: {name!r}")
            mode = (info.external_attr >> 16) & 0o170000
            if mode == 0o120000:
                raise ValueError(f"ZIP symlinks are not allowed: {name!r}")
            total_size += int(info.file_size or 0)
            if total_size > MAX_ZIP_UNCOMPRESSED_BYTES:
                raise ValueError("ZIP uncompressed size is too large")

    async def _mount_module(self, module_id: str, module_dir: Path, app: FastAPI):
        await self._unmount_module(module_id, app)

        router_file = module_dir / "router.py"
        if router_file.exists():
            mod = self._import_module_file(module_id, router_file)
            ctx = ModuleContext(module_id, module_dir)
            ctx.restart_modules_for_env = lambda key: self.restart_modules_for_env(
                key,
                app,
                exclude={module_id},
            )
            ctx.logger = get_module_logger(module_id, module_dir / "data" / "logs")
            lifecycle = self._supervisor.register(module_id, ctx.logger)
            ctx.lifecycle = lifecycle
            ctx.logger.info(f"Module {module_id} mounting")
            try:
                self._loaded[module_id] = mod
                self._contexts[module_id] = ctx
                with lifecycle.activate():
                    if hasattr(mod, "setup"):
                        result = mod.setup(ctx)
                        if hasattr(result, "__await__"):
                            await result
                lifecycle.mark_running()
            except Exception as e:
                ctx.logger.error(f"setup() failed: {e}", exc_info=True)
                await self._unmount_module(module_id, app)
                raise
            if hasattr(mod, "router"):
                app.include_router(mod.router, prefix=f"/{module_id}/api")
            ctx.logger.info(f"Module {module_id} active")

        for d, suffix in [(module_dir / "panel", "panel"), (module_dir / "static", "static")]:
            if d.exists():
                try:
                    app.mount(
                        f"/{module_id}/{suffix}",
                        StaticFiles(directory=str(d), html=True),
                        name=f"mod_{module_id}_{suffix}",
                    )
                except Exception:
                    pass

    async def _unmount_module(self, module_id: str, app: FastAPI):
        mod = self._loaded.pop(module_id, None)
        ctx = self._contexts.pop(module_id, None)
        lifecycle = ctx.lifecycle if ctx is not None else self._supervisor.get(module_id)
        if mod is not None and hasattr(mod, "shutdown"):
            try:
                if lifecycle is not None:
                    with lifecycle.activate():
                        result = mod.shutdown()
                        if hasattr(result, "__await__"):
                            await asyncio.wait_for(result, timeout=20)
                else:
                    result = mod.shutdown()
                    if hasattr(result, "__await__"):
                        await asyncio.wait_for(result, timeout=20)
            except TimeoutError:
                logging.getLogger("nexus.core").error("Module %s shutdown() timed out", module_id)
            except Exception:
                logging.getLogger("nexus.core").exception("Module %s shutdown() failed", module_id)
        if lifecycle is not None:
            await self._supervisor.unregister(module_id, lifecycle)
        sys.modules.pop(f"_nexus_mod_{module_id}", None)

        prefixes = (f"/{module_id}/api", f"/{module_id}/panel", f"/{module_id}/static")
        app.routes[:] = [r for r in app.routes if not (hasattr(r, "path") and r.path.startswith(prefixes))]
        app.router.routes[:] = [r for r in app.router.routes if not (hasattr(r, "path") and r.path.startswith(prefixes))]

    @staticmethod
    def _import_module_file(module_id: str, file_path: Path) -> ModuleType:
        mod_name = f"_nexus_mod_{module_id}"
        sys.modules.pop(mod_name, None)
        spec = importlib.util.spec_from_file_location(mod_name, file_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
        return mod
