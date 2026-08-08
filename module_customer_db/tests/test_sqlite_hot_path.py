from pathlib import Path
import unittest


class SqliteHotPathTests(unittest.TestCase):
    def test_read_connections_do_not_switch_journal_mode(self):
        source = (Path(__file__).resolve().parents[1] / "router.py").read_text(encoding="utf-8")
        connect = source[source.index("async def _connect_db"):source.index("def setup")]
        self.assertNotIn("journal_mode", connect)
        self.assertIn("_archive_initialized", source)

    def test_default_list_uses_indexable_per_table_page(self):
        source = (Path(__file__).resolve().parents[1] / "router.py").read_text(encoding="utf-8")
        self.assertIn("idx_cdb_{name}_created_id", source)
        self.assertIn("if not q:\n        return await _aggregate_records_fallback", source)


if __name__ == "__main__":
    unittest.main()
