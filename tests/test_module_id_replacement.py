import asyncio
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from fastapi import FastAPI

import orchestrator.core as core


OLD_ROUTER = """
from fastapi import APIRouter
router = APIRouter()
@router.get('/value')
async def value(): return {'module': 'old'}
def setup(ctx):
    if not ctx.db_path.exists(): ctx.db_path.write_text('persistent', encoding='utf-8')
"""

NEW_ROUTER = """
from fastapi import APIRouter
router = APIRouter()
@router.get('/value')
async def value(): return {'module': 'new'}
def setup(ctx):
    if ctx.db_path.read_text(encoding='utf-8') != 'persistent': raise RuntimeError('data missing')
"""


class ModuleIdReplacementTest(unittest.TestCase):
    def test_replaces_module_id_and_preserves_data(self):
        asyncio.run(self._run_case())

    async def _run_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            modules = root / "modules"
            modules.mkdir()
            registry = {}

            async def get_modules(status=None):
                rows = list(registry.values())
                return [row for row in rows if status is None or row["status"] == status]

            async def upsert(meta):
                registry[meta["id"]] = dict(meta)

            async def replace(old_id, meta):
                registry.pop(old_id, None)
                registry[meta["id"]] = dict(meta)

            originals = (core.MODULES_DIR, core.get_modules_by_status, core.upsert_module, core.replace_module)
            core.MODULES_DIR, core.get_modules_by_status, core.upsert_module, core.replace_module = modules, get_modules, upsert, replace
            try:
                old_zip = root / "old.zip"
                new_zip = root / "new.zip"
                self._write_zip(old_zip, {"id": "old-module", "name": "Old", "version": "1.0.0"}, OLD_ROUTER)
                self._write_zip(new_zip, {"id": "new-module", "name": "New", "version": "2.0.0", "replaces": "old-module"}, NEW_ROUTER)
                app = FastAPI()
                manager = core.ModuleManager(root)
                await manager.install_from_zip(old_zip, app)
                await manager.install_from_zip(new_zip, app)

                self.assertNotIn("old-module", registry)
                self.assertIn("new-module", registry)
                self.assertFalse((modules / "old-module").exists())
                self.assertEqual((modules / "new-module" / "data" / "new-module.db").read_text(encoding="utf-8"), "persistent")
                paths = [getattr(route, "path", "") for route in app.routes]
                self.assertIn("/old-module/api/value", paths)
                self.assertIn("/new-module/api/value", paths)
                self.assertFalse(any(path.name.startswith((".staging-", ".rollback-", ".failed-")) for path in modules.iterdir()))
            finally:
                core.MODULES_DIR, core.get_modules_by_status, core.upsert_module, core.replace_module = originals

    @staticmethod
    def _write_zip(path, manifest, router):
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest))
            archive.writestr("router.py", router)


if __name__ == "__main__":
    unittest.main()
