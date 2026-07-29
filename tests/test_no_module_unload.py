from pathlib import Path
import unittest

import orchestrator.core as core
import orchestrator.db as db
from main import app


ROOT = Path(__file__).resolve().parents[1]


class NoModuleUnloadTests(unittest.TestCase):
    def test_destructive_unload_capability_does_not_exist(self):
        routes = {(route.path, method) for route in app.routes for method in getattr(route, "methods", set())}

        self.assertNotIn(("/api/modules/{module_id}/unload", "POST"), routes)
        self.assertFalse(hasattr(core.ModuleManager, "unload"))
        self.assertFalse(hasattr(db, "delete_module"))

    def test_shell_and_documentation_do_not_offer_unload(self):
        shell = (ROOT / "templates" / "shell.html").read_text(encoding="utf-8")
        script = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        settings = (ROOT / "templates" / "settings.html").read_text(encoding="utf-8")

        self.assertNotIn("mtUnload", shell + script)
        self.assertNotIn("/unload", settings)
        self.assertNotIn("Выгрузить", shell + settings)


if __name__ == "__main__":
    unittest.main()
