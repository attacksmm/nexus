import importlib.util
import json
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from orchestrator.db import delete_module, get_modules_by_status, update_module_status, upsert_module

MODULES_DIR = Path(__file__).parent.parent / "modules"
UPLOADS_DIR = Path(__file__).parent.parent / "uploads"

REQUIRED_MANIFEST_KEYS = {"id", "name", "version"}


class ModuleContext:
    def __init__(self, module_id: str, module_dir: Path):
        self.module_id = module_id
        self.module_dir = module_dir
        self.data_dir = module_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / f"{module_id}.db"


class ModuleManager:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self._loaded: dict[str, ModuleType] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    async def install_from_zip(self, zip_path: Path, app: FastAPI) -> dict:
        manifest, module_dir = self._extract_zip(zip_path)
        module_id = manifest["id"]
        meta = {
            "id": module_id,
            "name": manifest["name"],
            "version": manifest.get("version", "0.0.0"),
            "description": manifest.get("description", ""),
            "status": "active",
            "installed_at": datetime.now(timezone.utc).isoformat(),
            "manifest_json": json.dumps(manifest, ensure_ascii=False),
        }
        await upsert_module(meta)
        await self._mount_module(module_id, module_dir, app)
        return meta

    async def unload(self, module_id: str, app: FastAPI):
        self._unmount_module(module_id, app)
        module_dir = MODULES_DIR / module_id
        if module_dir.exists():
            shutil.rmtree(module_dir)
        await delete_module(module_id)

    async def pause(self, module_id: str, app: FastAPI):
        self._unmount_module(module_id, app)
        await update_module_status(module_id, "paused")

    async def resume(self, module_id: str, app: FastAPI):
        module_dir = MODULES_DIR / module_id
        if not module_dir.exists():
            raise RuntimeError(f"Module dir not found: {module_dir}")
        await self._mount_module(module_id, module_dir, app)
        await update_module_status(module_id, "active")

    async def list_modules(self) -> list[dict]:
        return await get_modules_by_status()

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

    # ── Internals ──────────────────────────────────────────────────────────────

    def _extract_zip(self, zip_path: Path) -> tuple[dict, Path]:
        with zipfile.ZipFile(zip_path) as zf:
            if "manifest.json" not in zf.namelist():
                raise ValueError("manifest.json missing in ZIP")
            manifest = json.loads(zf.read("manifest.json"))

        missing = REQUIRED_MANIFEST_KEYS - manifest.keys()
        if missing:
            raise ValueError(f"manifest.json missing keys: {missing}")

        module_id = manifest["id"]
        if not module_id.replace("-", "_").isidentifier():
            raise ValueError(f"Invalid module id: {module_id!r}")

        module_dir = MODULES_DIR / module_id
        if module_dir.exists():
            shutil.rmtree(module_dir)
        module_dir.mkdir(parents=True)

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(module_dir)

        return manifest, module_dir

    async def _mount_module(self, module_id: str, module_dir: Path, app: FastAPI):
        self._unmount_module(module_id, app)

        router_file = module_dir / "router.py"
        if router_file.exists():
            mod = self._import_module_file(module_id, router_file)
            ctx = ModuleContext(module_id, module_dir)
            if hasattr(mod, "setup"):
                result = mod.setup(ctx)
                # поддерживаем и async setup
                if hasattr(result, "__await__"):
                    await result
            if hasattr(mod, "router"):
                app.include_router(mod.router, prefix=f"/{module_id}/api")
            self._loaded[module_id] = mod

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

    def _unmount_module(self, module_id: str, app: FastAPI):
        self._loaded.pop(module_id, None)
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
