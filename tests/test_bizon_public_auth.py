import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path

from starlette.requests import Request

import module_bizon.router as bizon


def _request(key: str = "") -> Request:
    headers = []
    if key:
        headers.append((b"x-nexus-bizon-key", key.encode("utf-8")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


class BizonPublicAuthTest(unittest.TestCase):
    def test_observe_to_enforce_migration(self):
        asyncio.run(self._run_case())

    async def _run_case(self):
        with tempfile.TemporaryDirectory() as td:
            original_db = bizon._db_path
            original_logger = bizon._logger
            bizon._db_path = Path(td) / "bizon.db"
            bizon._logger = None
            try:
                await bizon._init_db()
                with sqlite3.connect(bizon._db_path) as db:
                    db.execute("UPDATE settings SET value='test-secret' WHERE key='sec_key'")
                    db.commit()

                self.assertTrue(await bizon._public_key_allowed(_request(), endpoint="test"))
                self.assertTrue(await bizon._public_key_allowed(_request("test-secret"), endpoint="test"))
                self.assertFalse(await bizon._public_key_allowed(_request("wrong"), endpoint="test"))

                with sqlite3.connect(bizon._db_path) as db:
                    db.execute("UPDATE settings SET value='enforce' WHERE key='public_auth_mode'")
                    db.commit()

                self.assertFalse(await bizon._public_key_allowed(_request(), endpoint="test"))
                self.assertTrue(await bizon._public_key_allowed(_request("test-secret"), endpoint="test"))
                self.assertFalse(
                    await bizon._public_key_allowed(
                        _request(), endpoint="strict-test", allow_legacy=False
                    )
                )
            finally:
                bizon._db_path = original_db
                bizon._logger = original_logger


if __name__ == "__main__":
    unittest.main()
