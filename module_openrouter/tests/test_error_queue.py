import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from module_openrouter import router


class ErrorQueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name) / "modules"
        self.module_dir = root / "openrouter"
        self.db_path = self.module_dir / "data" / "openrouter.db"
        self.db_path.parent.mkdir(parents=True)
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """CREATE TABLE outbound_jobs (
                    id INTEGER PRIMARY KEY, source TEXT, status TEXT, request_hash TEXT,
                    payload_json TEXT, result_json TEXT, error_text TEXT, attempts INTEGER,
                    created_at TEXT, updated_at TEXT, next_attempt_at TEXT
                )"""
            )
            rows = [
                (1, "senler_failed", "completed"),
                (2, "senler_failed", "failed"),
                (3, "salebot", "pending"),
            ]
            for job_id, source, status in rows:
                db.execute(
                    "INSERT INTO outbound_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (job_id, source, status, str(job_id), '{"platform_id":"123","message":"Вопрос"}', "{}", "", 1, "2026-01-01T00:00:00Z", f"2026-01-01T00:00:0{job_id}Z", ""),
                )
        self.old_db_path = router._db_path
        self.old_module_dir = router._module_dir
        router._db_path = self.db_path
        router._module_dir = self.module_dir

    def tearDown(self):
        router._db_path = self.old_db_path
        router._module_dir = self.old_module_dir
        self.tmp.cleanup()

    async def test_only_openrouter_problems_stay_first_until_completed(self):
        with patch.object(router, "_require_panel_user", new=AsyncMock(return_value={"id": 1})):
            problems = await router.list_error_jobs(object(), status="problem")
            all_jobs = await router.list_error_jobs(object(), status="all")

        self.assertEqual([item["id"] for item in problems["items"]], [3, 2])
        self.assertEqual(problems["total"], 2)
        self.assertEqual([item["id"] for item in all_jobs["items"]], [3, 2, 1])
        self.assertTrue(problems["items"][0]["retryable"])
