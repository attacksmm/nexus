import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from module_scanner import router


class OpenRouterLookupTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_senler_retry_counts_as_answer(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "openrouter.db"
            with sqlite3.connect(db_path) as db:
                db.execute(
                    "CREATE TABLE messages (platform_id TEXT, source TEXT, role TEXT, created_at TEXT, conversation_id TEXT)"
                )
                db.execute(
                    "INSERT INTO messages VALUES (?,?,?,?,?)",
                    ("123", "senler_retry_delivered", "assistant", "2026-07-29T14:00:00Z", "or_conv_retry"),
                )
            with patch.dict("os.environ", {"SCANNER_OPENROUTER_DB_PATH": str(db_path)}):
                self.assertEqual(
                    await router._openrouter_answer_after("123", "2026-07-29T13:59:50Z"),
                    ("2026-07-29T14:00:00Z", "or_conv_retry"),
                )


if __name__ == "__main__":
    unittest.main()
