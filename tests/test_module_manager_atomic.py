import asyncio
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from fastapi import FastAPI

import orchestrator.core as core


GOOD_ROUTER = """
from fastapi import APIRouter
router = APIRouter()
@router.get('/version')
async def version():
    return {'version': 'good'}
def setup(ctx):
    (ctx.data_dir / 'setup.txt').write_text('good', encoding='utf-8')
"""

BROKEN_ROUTER = """
from fastapi import APIRouter
router = APIRouter()
def setup(ctx):
    raise RuntimeError('intentional setup failure')
"""

GOOD_ROUTER_V2 = GOOD_ROUTER.replace("'good'", "'good-v2'")


class AtomicModuleInstallTest(unittest.TestCase):
    def test_failed_update_restores_previous_module_and_data(self):
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
            try:
                good_zip = root / "good.zip"
                broken_zip = root / "broken.zip"
                good_v2_zip = root / "good-v2.zip"
                self._write_zip(good_zip, "1.0.0", GOOD_ROUTER)
                self._write_zip(broken_zip, "2.0.0", BROKEN_ROUTER)
                self._write_zip(good_v2_zip, "2.0.0", GOOD_ROUTER_V2)
                app = FastAPI()
                manager = core.ModuleManager(root)

                await manager.install_from_zip(good_zip, app)
                data_file = modules / "demo" / "data" / "state.txt"
                data_file.write_text("preserved", encoding="utf-8")
                self.assertEqual(registry["demo"]["version"], "1.0.0")

                with self.assertRaisesRegex(RuntimeError, "intentional setup failure"):
                    await manager.install_from_zip(broken_zip, app)

                self.assertEqual(registry["demo"]["version"], "1.0.0")
                self.assertEqual(data_file.read_text(encoding="utf-8"), "preserved")
                self.assertIn("'good'", (modules / "demo" / "router.py").read_text(encoding="utf-8"))
                self.assertIn("/demo/api/version", [getattr(route, "path", "") for route in app.routes])
                self.assertFalse(any(path.name.startswith((".staging-", ".rollback-", ".failed-")) for path in modules.iterdir()))

                await manager.install_from_zip(good_v2_zip, app)
                self.assertEqual(registry["demo"]["version"], "2.0.0")
                self.assertEqual(data_file.read_text(encoding="utf-8"), "preserved")
                self.assertIn("'good-v2'", (modules / "demo" / "router.py").read_text(encoding="utf-8"))
                self.assertEqual([getattr(route, "path", "") for route in app.routes].count("/demo/api/version"), 1)
            finally:
                core.MODULES_DIR = original_modules_dir
                core.get_modules_by_status = original_get_modules
                core.upsert_module = original_upsert
                core.update_module_status = original_update

    @staticmethod
    def _write_zip(path: Path, version: str, router_text: str):
        manifest = {"id": "demo", "name": "Demo", "version": version}
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("router.py", router_text)


if __name__ == "__main__":
    unittest.main()
