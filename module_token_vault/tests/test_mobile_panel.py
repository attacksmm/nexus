import unittest
from pathlib import Path


PANEL = Path(__file__).resolve().parents[1] / "panel" / "index.html"


class MobilePanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = PANEL.read_text(encoding="utf-8")

    def test_mobile_list_and_editor_are_separate(self):
        self.assertIn("body.mobile-editing .list{display:none}", self.html)
        self.assertIn("body.mobile-editing .side{display:block}", self.html)
        self.assertIn('id="backToList"', self.html)

    def test_token_rows_become_mobile_cards(self):
        self.assertIn("tr[data-key]{padding:10px;border:1px solid var(--line)", self.html)
        self.assertIn('class="mono token-key"', self.html)

    def test_mobile_actions_are_touch_sized(self):
        self.assertIn("header button{min-height:40px", self.html)
        self.assertIn(".actions button{min-height:40px", self.html)

    def test_force_save_requires_confirmation(self):
        self.assertIn('confirm("Сохранить без проверки?")', self.html)

    def test_empty_filter_has_an_empty_state(self):
        self.assertIn("rows?`<table>", self.html)
        self.assertIn("Нет ключей", self.html)


if __name__ == "__main__":
    unittest.main()
