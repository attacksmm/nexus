import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class BizonMobilePanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
        cls.docs = (ROOT / "panel" / "docs.html").read_text(encoding="utf-8")
        cls.router = (ROOT / "router.py").read_text(encoding="utf-8")
        cls.runtime = (ROOT / "static" / "moderator_openrouter_pm.js").read_text(encoding="utf-8")
        cls.manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))

    def test_release_version_is_consistent(self):
        self.assertEqual(self.manifest["version"], "1.1.23")
        self.assertIn("Bizon365 1.1.23", self.docs)
        self.assertIn("moderator_openrouter_pm.js?v=1.1.23", self.router)
        self.assertIn("Runtime 1.1.23 loaded", self.runtime)

    def test_runner_has_mobile_sections(self):
        for name in ("status", "settings", "logs"):
            self.assertIn(f'data-runner-section="{name}"', self.panel)
            self.assertIn(f'data-runner-panel="{name}"', self.panel)
        self.assertIn("setRunnerSection", self.panel)

    def test_scripts_have_mobile_list_editor_flow(self):
        for marker in (
            'id="scriptRoomList"',
            'id="backToRoomsBtn"',
            'id="mobileSaveScriptsBtn"',
            "openMobileScript",
            "showScriptRoomList",
        ):
            self.assertIn(marker, self.panel)

    def test_operational_interruptions_require_confirmation(self):
        for prompt in (
            "Установить зависимости runner?",
            "Остановить runner?",
            "Перезапустить runner?",
        ):
            self.assertIn(prompt, self.panel)

    def test_polling_preserves_logs_and_script_edits(self):
        self.assertIn("if (text === state.logText) return", self.panel)
        self.assertIn("nearBottom", self.panel)
        self.assertIn("reloadRoomScripts = false", self.panel)
        self.assertIn("state.scriptsDirty", self.panel)

    def test_mobile_code_is_viewport_bounded(self):
        self.assertIn("overflow-x:hidden;overflow-wrap:anywhere", self.panel)
        self.assertIn("overflow-wrap:anywhere;word-break:break-word", self.docs)


if __name__ == "__main__":
    unittest.main()
