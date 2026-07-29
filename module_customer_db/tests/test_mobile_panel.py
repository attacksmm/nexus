from pathlib import Path
import unittest


MODULE_DIR = Path(__file__).resolve().parents[1]


class MobilePanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.panel = (MODULE_DIR / "panel" / "index.html").read_text(encoding="utf-8")
        cls.docs = (MODULE_DIR / "panel" / "docs.html").read_text(encoding="utf-8")

    def test_mobile_toolbar_keeps_table_tabs_and_actions_reachable(self):
        self.assertIn("@media(max-width:760px)", self.panel)
        self.assertIn(".table-tabs{grid-column:1/-1", self.panel)
        self.assertIn("#newTableBtn{grid-column:1", self.panel)
        self.assertIn("#refreshBtn{grid-column:5", self.panel)
        self.assertIn(".table-tab,.table-tab__del{min-height:40px", self.panel)

    def test_records_render_as_mobile_cards_with_touch_actions(self):
        for class_name in (
            "record-platform",
            "record-fields",
            "record-id",
            "record-created",
            "record-actions",
        ):
            self.assertIn(class_name, self.panel)
        self.assertIn('aria-label="Изменить запись"', self.panel)
        self.assertIn('aria-label="Удалить запись"', self.panel)
        self.assertIn("#tableBody .record-actions .btn{width:40px", self.panel)

    def test_dialogs_are_mobile_sized_and_keyboard_closeable(self):
        self.assertIn('role="dialog" aria-modal="true"', self.panel)
        self.assertIn(".overlay{place-items:end center;padding:0", self.panel)
        self.assertIn('event.key !== "Escape"', self.panel)
        self.assertIn("if (saveBtn.disabled) return", self.panel)

    def test_async_lists_reject_stale_responses(self):
        self.assertIn("tablesRequest = 0, recordsRequest = 0", self.panel)
        self.assertIn("requestId !== tablesRequest", self.panel)
        self.assertIn("requestId !== recordsRequest", self.panel)

    def test_docs_wrap_content_without_document_overflow(self):
        self.assertIn('name="viewport"', self.docs)
        self.assertIn("overflow-x:hidden", self.docs)
        self.assertIn("white-space:pre-wrap", self.docs)
        self.assertIn("overflow-wrap:anywhere", self.docs)
        self.assertIn("База клиентов v2.1.6", self.docs)


if __name__ == "__main__":
    unittest.main()
