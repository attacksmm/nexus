import asyncio
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from fastapi import FastAPI

import orchestrator.core as core


ROUTER = """
import asyncio
from fastapi import APIRouter

router = APIRouter()
worker = None

async def _worker():
    await asyncio.Event().wait()

def setup(ctx):
    global worker
    worker = asyncio.create_task(_worker(), name='demo-worker-{version}')

@router.get('/version')
async def version():
    return {{'version': '{version}'}}
"""


class ModuleLifecycleAtomicTest(unittest.TestCase):
    def test_update_and_pause_cancel_unowned_module_workers(self):
        asyncio.run(self._run_case())

    async def _run_case(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            modules = root / "modules"
            modules.mkdir()
            registry = {}

            async def get_modules(status=None):
                rows = list(registry.values())
                return [row for row in rows if status is None or row["status"] == status]

            async def upsert(meta):
                registry[meta["id"]] = dict(meta)

            async def update_status(module_id, status):
                if module_id in registry:
                    registry[module_id]["status"] = status

            original_modules_dir = core.MODULES_DIR
            original_get_modules = core.get_modules_by_status
            original_upsert = core.upsert_module
            original_update = core.update_module_status
            core.MODULES_DIR = modules
            core.get_modules_by_status = get_modules
            core.upsert_module = upsert
            core.update_module_status = update_status
            manager = core.ModuleManager(root)
            manager.install_lifecycle_tracking()
            app = FastAPI()
            try:
                first_zip = root / "first.zip"
                second_zip = root / "second.zip"
                self._write_zip(first_zip, "1.0.0")
                self._write_zip(second_zip, "2.0.0")

                await manager.install_from_zip(first_zip, app)
                first_task = manager._loaded["demo"].worker
                await asyncio.sleep(0)
                self.assertFalse(first_task.done())
                self.assertEqual(manager.lifecycle_snapshot()[0]["active_count"], 1)

                await manager.install_from_zip(second_zip, app)
                second_task = manager._loaded["demo"].worker
                await asyncio.sleep(0)
                self.assertTrue(first_task.cancelled())
                self.assertFalse(second_task.done())
                self.assertEqual(manager.lifecycle_snapshot()[0]["active_count"], 1)
                self.assertEqual(
                    manager.lifecycle_snapshot()[0]["active_tasks"][0]["name"],
                    "demo-worker-2.0.0",
                )

                await manager.pause("demo", app)
                await asyncio.sleep(0)
                self.assertTrue(second_task.cancelled())
                self.assertEqual(manager.lifecycle_snapshot(), [])
            finally:
                await manager.shutdown_all(app)
                manager.uninstall_lifecycle_tracking()
                core.MODULES_DIR = original_modules_dir
                core.get_modules_by_status = original_get_modules
                core.upsert_module = original_upsert
                core.update_module_status = original_update

    @staticmethod
    def _write_zip(path: Path, version: str):
        manifest = {"id": "demo", "name": "Demo", "version": version}
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("router.py", ROUTER.format(version=version))


if __name__ == "__main__":
    unittest.main()
