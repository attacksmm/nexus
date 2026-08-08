from __future__ import annotations

import importlib.util
import asyncio
import sys
import types
import unittest
from pathlib import Path


def load_router():
    aiosqlite = types.ModuleType("aiosqlite")
    aiosqlite.connect = lambda *args, **kwargs: None
    sys.modules.setdefault("aiosqlite", aiosqlite)
    httpx = types.ModuleType("httpx")
    httpx.AsyncClient = object
    sys.modules.setdefault("httpx", httpx)
    fastapi = types.ModuleType("fastapi")
    class APIRouter:
        def __getattr__(self, _name):
            return lambda *args, **kwargs: (lambda func: func)
    class HTTPException(Exception):
        pass
    fastapi.APIRouter = APIRouter
    fastapi.HTTPException = HTTPException
    fastapi.Query = lambda default=None, **kwargs: default
    fastapi.Request = object
    sys.modules.setdefault("fastapi", fastapi)
    pydantic = types.ModuleType("pydantic")
    pydantic.BaseModel = type("BaseModel", (), {})
    sys.modules.setdefault("pydantic", pydantic)
    auth = types.ModuleType("orchestrator.auth")
    auth.can_access_module = lambda *args: True
    async def verify(*args): return {"role": "admin"}
    auth.verify_token_from_request = verify
    sys.modules.setdefault("orchestrator.auth", auth)
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location("bizon_google_sheets_router", Path(__file__).resolve().parents[1] / "router.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


router = load_router()


class SheetRowsTest(unittest.TestCase):
    def test_retry_batch_is_small_and_recovers_interrupted_events(self):
        self.assertEqual(router.DEFAULT_RETRY_LIMIT, 10)
        self.assertEqual(router.RETRY_STATUSES, ("failed", "received"))

    def test_write_throttle_stays_above_two_seconds(self):
        self.assertGreaterEqual(router.WRITE_THROTTLE_SECONDS, 2.0)

    def test_sheet_schema_check_is_cached_for_batch(self):
        calls = []
        async def fake_settings():
            return {
                "spreadsheet_id": "sheet-1",
                "worksheet_title": "Bizon365 Nexus",
                "vakas_worksheet_title": "Vakas",
                "vakas_mirror_enabled": "1",
            }
        async def fake_request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            if kwargs.get("params", {}).get("fields") == "sheets.properties":
                return {"sheets": [{"properties": {"title": "Bizon365 Nexus"}}, {"properties": {"title": "Vakas"}}]}
            if "A1%3AAD1" in url:
                return {"values": [router.HEADERS]}
            return {"values": [router.VAKAS_HEADERS]}
        original_settings, original_request = router._settings, router._google_request
        router._settings, router._google_request = fake_settings, fake_request
        router._sheet_check_key, router._sheet_check_until = None, 0.0
        try:
            async def scenario():
                await router._ensure_sheet()
                await router._ensure_sheet()
            asyncio.run(scenario())
        finally:
            router._settings, router._google_request = original_settings, original_request
            router._sheet_check_key, router._sheet_check_until = None, 0.0
        self.assertEqual(len(calls), 3)

    def test_row_matches_header_and_has_one_web_min(self):
        row = router.attendance_row({
            "attendance_key": "attendance:1",
            "person_key": "phone:79990000000",
            "username": "Анна",
            "phone": "79990000000",
            "watch_minutes": 81.5,
            "watch_seconds": 4890,
            "watch_valid": True,
            "webinarId": "97242:puppy*2026-07-13T12:00:00",
            "roomid": "97242:puppy",
        }, "2026-07-13T13:00:00Z")
        self.assertEqual(len(row), len(router.HEADERS))
        self.assertEqual(router.HEADERS.count("web_min"), 1)
        self.assertEqual(row[router.HEADERS.index("attendance_key")], "attendance:1")
        self.assertEqual(row[router.HEADERS.index("web_min")], 81.5)

    def test_updated_range_parser(self):
        self.assertEqual(router._row_number("'Bizon365 Nexus'!A42:AD42"), 42)

    def test_vakas_row_preserves_exact_legacy_column_order(self):
        attendance = {
            "username": "Анна",
            "email": "a@example.test",
            "phone": "79990000000",
            "city": "Москва",
            "watch_minutes": 81.5,
            "view": 100,
            "viewTill": 200,
            "roomid": "97242:master-klass",
            "utm_term": "vk-42",
            "p1": "one",
            "p2": "two",
        }
        row = router.vakas_attendance_row(attendance)
        self.assertEqual(len(row), 23)
        self.assertEqual(len(row), len(router.VAKAS_HEADERS))
        self.assertEqual(router.VAKAS_HEADERS[3], "sity")
        self.assertEqual(row[3], "Москва")
        self.assertEqual(router.VAKAS_HEADERS.count("web_min"), 2)
        web_min_indexes = [i for i, value in enumerate(router.VAKAS_HEADERS) if value == "web_min"]
        self.assertEqual([row[i] for i in web_min_indexes], [81.5, 81.5])
        self.assertEqual(row[router.VAKAS_HEADERS.index("utm_term")], "vk-42")


if __name__ == "__main__":
    unittest.main()
