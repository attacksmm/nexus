import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SenlerMobilePanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.panel = (ROOT / "panel" / "index.html").read_text(encoding="utf-8")
        cls.docs = (ROOT / "panel" / "docs.html").read_text(encoding="utf-8")
        cls.manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))

    def test_release_version_is_consistent(self):
        self.assertEqual(self.manifest["version"], "1.3.2")
        self.assertIn("Списки Сенлер v1.3.2", self.docs)

    def test_mobile_list_editor_navigation_exists(self):
        for marker in (
            'id="mobileNewBindingBtn"',
            'id="bCancelBtn"',
            'id="mobileNewChannelBtn"',
            'id="chCancelBtn"',
            "setMobileEditor('viewBindings'",
            "setMobileEditor('viewSettings'",
        ):
            self.assertIn(marker, self.panel)

    def test_mobile_cards_and_journal_filter_exist(self):
        for label in ("Страница", "Канал", "Список Senler", "Результат"):
            self.assertIn(f'data-label="{label}"', self.panel)
        self.assertIn('id="visitFilterSelect"', self.panel)

    def test_mutations_have_busy_guard(self):
        self.assertIn("async function withBusy", self.panel)
        for action in ("bAddBtn", "chAddBtn", "deleteBinding", "deleteChannel", "deletePage"):
            self.assertIn(action, self.panel)

    def test_docs_are_mobile_bounded(self):
        self.assertIn('name="viewport"', self.docs)
        self.assertIn("overflow-x:hidden", self.docs)
        self.assertIn("@media(max-width:640px)", self.docs)


if __name__ == "__main__":
    unittest.main()
