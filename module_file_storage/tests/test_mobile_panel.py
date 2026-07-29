import unittest
from pathlib import Path


PANEL = Path(__file__).resolve().parents[1] / "panel" / "index.html"


class MobilePanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = PANEL.read_text(encoding="utf-8")

    def test_mobile_actions_are_touch_sized(self):
        self.assertIn(".row-actions .btn{width:100%;min-height:40px", self.html)
        self.assertIn(".crumb{min-height:40px", self.html)

    def test_every_item_has_explicit_open_action(self):
        self.assertIn('data-act="open"', self.html)
        self.assertIn("item.kind === 'folder' ? loadFolder(item.id) : previewItem(item)", self.html)

    def test_mobile_rows_are_cards(self):
        self.assertIn("tr.item-row{padding:10px;border:1px solid var(--border)", self.html)
        self.assertIn("grid-template-columns:repeat(2,minmax(0,1fr))", self.html)

    def test_create_forms_prevent_duplicate_submits(self):
        self.assertGreaterEqual(self.html.count("if(submit)submit.disabled=true"), 2)
        self.assertIn("Папка создана", self.html)
        self.assertIn("Файл сохранён", self.html)


if __name__ == "__main__":
    unittest.main()
