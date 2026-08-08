import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AmoCrmSenlerPanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
        cls.manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))

    def test_release_and_mobile_touch_targets(self):
        self.assertEqual(self.manifest["version"], "1.0.3")
        for marker in (
            ".tab{height:40px",
            ".f-input,.select-btn{min-height:40px}",
            ".btn,.btn--sm{min-height:40px",
            "withButtonBusy",
            "Загрузка…",
        ):
            self.assertIn(marker, self.panel)

    def test_mutating_binding_actions_use_busy_guard(self):
        self.assertIn("withButtonBusy(event.currentTarget, 'Сохранение…'", self.panel)
        self.assertIn("toggleBinding(${b.id},this)", self.panel)
        self.assertIn("deleteBinding(${b.id},this)", self.panel)


if __name__ == "__main__":
    unittest.main()
