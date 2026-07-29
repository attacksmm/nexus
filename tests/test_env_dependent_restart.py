import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI

import orchestrator.core as core


class EnvDependentRestartTest(unittest.TestCase):
    def test_only_active_declared_modules_restart_sequentially(self):
        asyncio.run(self._run_case())

    async def _run_case(self):
        with tempfile.TemporaryDirectory() as td:
            modules_dir = Path(td) / "modules"
            for module_id in ("dep-a", "dep-b", "other", "paused", "token-vault"):
                (modules_dir / module_id).mkdir(parents=True)

            rows = [
                self._row("dep-a", "active", env_vars={"VK_USER_TOKEN": "token"}),
                self._row("dep-b", "active", env_required=["VK_USER_TOKEN"]),
                self._row("other", "active", env_vars={"SOME_KEY": "other"}),
                self._row("paused", "paused", env_vars={"VK_USER_TOKEN": "token"}),
                self._row("token-vault", "active", env_vars={"VK_USER_TOKEN": "self"}),
            ]
            events = []
            statuses = {}

            async def get_modules(status=None):
                return [row for row in rows if status is None or row["status"] == status]

            async def update_status(module_id, status):
                statuses[module_id] = status

            original_modules_dir = core.MODULES_DIR
            original_get_modules = core.get_modules_by_status
            original_update = core.update_module_status
            core.MODULES_DIR = modules_dir
            core.get_modules_by_status = get_modules
            core.update_module_status = update_status
            manager = core.ModuleManager(Path(td))

            async def unmount(module_id, app):
                events.append(("unmount", module_id))

            async def mount(module_id, module_dir, app):
                events.append(("mount", module_id))
                if module_id == "dep-b":
                    raise RuntimeError("expected mount failure")

            manager._unmount_module = unmount
            manager._mount_module = mount
            try:
                result = await manager.restart_modules_for_env(
                    "VK_USER_TOKEN",
                    FastAPI(),
                    exclude={"token-vault"},
                )
            finally:
                core.MODULES_DIR = original_modules_dir
                core.get_modules_by_status = original_get_modules
                core.update_module_status = original_update

            self.assertEqual(
                events,
                [
                    ("unmount", "dep-a"),
                    ("mount", "dep-a"),
                    ("unmount", "dep-b"),
                    ("mount", "dep-b"),
                ],
            )
            self.assertEqual(statuses, {"dep-a": "active", "dep-b": "error"})
            self.assertEqual(result["restarted"], 1)
            self.assertEqual(result["failed"], 1)
            self.assertFalse(result["ok"])
            self.assertEqual([item["id"] for item in result["modules"]], ["dep-a", "dep-b"])
            self.assertIn("expected mount failure", result["modules"][1]["error"])

    def test_rejects_invalid_env_key(self):
        async def scenario():
            manager = core.ModuleManager(Path("."))
            with self.assertRaises(ValueError):
                await manager.restart_modules_for_env("BAD-KEY", FastAPI())

        asyncio.run(scenario())

    @staticmethod
    def _row(module_id, status, *, env_vars=None, env_required=None):
        manifest = {"id": module_id, "name": module_id, "version": "1.0.0"}
        if env_vars is not None:
            manifest["env_vars"] = env_vars
        if env_required is not None:
            manifest["env_required"] = env_required
        return {
            "id": module_id,
            "name": module_id,
            "status": status,
            "manifest_json": json.dumps(manifest),
        }


if __name__ == "__main__":
    unittest.main()
