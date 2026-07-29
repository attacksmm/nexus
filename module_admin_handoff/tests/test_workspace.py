from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

def load_router():
    try:
        import aiosqlite  # noqa: F401
        import fastapi  # noqa: F401
        import httpx  # noqa: F401
    except ModuleNotFoundError:
        aiosqlite = types.ModuleType("aiosqlite")

        class Cursor:
            def __init__(self, cursor):
                self.cursor = cursor
                self.rowcount = cursor.rowcount
                self.lastrowid = cursor.lastrowid

            async def fetchone(self):
                return self.cursor.fetchone()

            async def fetchall(self):
                return self.cursor.fetchall()

        class Connection:
            def __init__(self, path):
                self.connection = sqlite3.connect(path)

            @property
            def row_factory(self):
                return self.connection.row_factory

            @row_factory.setter
            def row_factory(self, value):
                self.connection.row_factory = value

            async def execute(self, sql, params=()):
                return Cursor(self.connection.execute(sql, params))

            async def executemany(self, sql, params):
                return Cursor(self.connection.executemany(sql, params))

            async def executescript(self, sql):
                self.connection.executescript(sql)

            async def commit(self):
                self.connection.commit()

            async def close(self):
                self.connection.close()

        async def connect(path, **_kwargs):
            return Connection(path)

        aiosqlite.connect = connect
        aiosqlite.Connection = Connection
        aiosqlite.Row = sqlite3.Row
        sys.modules["aiosqlite"] = aiosqlite

        class APIRouter:
            def get(self, *_args, **_kwargs):
                return lambda fn: fn

            post = get

        class HTTPException(Exception):
            def __init__(self, status_code, detail):
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        fastapi = types.ModuleType("fastapi")
        fastapi.APIRouter = APIRouter
        fastapi.HTTPException = HTTPException
        fastapi.Request = object
        sys.modules["fastapi"] = fastapi

        httpx = types.ModuleType("httpx")
        httpx.AsyncClient = object
        sys.modules["httpx"] = httpx

        orchestrator = types.ModuleType("orchestrator")
        auth = types.ModuleType("orchestrator.auth")
        auth.can_access_module = lambda *_args: True

        async def verify_token_from_request(_request):
            return {"id": 1}

        auth.verify_token_from_request = verify_token_from_request
        sys.modules["orchestrator"] = orchestrator
        sys.modules["orchestrator.auth"] = auth

    spec = importlib.util.spec_from_file_location(
        "admin_handoff_workspace_router",
        Path(__file__).resolve().parents[1] / "router.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


router = load_router()


class AdminHandoffWorkspaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        router._db_path = self.root / "admin-handoff.db"
        router._module_dir = self.root
        self.old_scanner_path = os.environ.get("ADMIN_HANDOFF_SCANNER_DB_PATH")
        self.old_retention = os.environ.get("ADMIN_HANDOFF_RETENTION_DAYS")
        os.environ["ADMIN_HANDOFF_SCANNER_DB_PATH"] = str(self.root / "scanner.db")
        os.environ["ADMIN_HANDOFF_RETENTION_DAYS"] = "7"
        await router._init_db()

    async def asyncTearDown(self) -> None:
        if self.old_scanner_path is None:
            os.environ.pop("ADMIN_HANDOFF_SCANNER_DB_PATH", None)
        else:
            os.environ["ADMIN_HANDOFF_SCANNER_DB_PATH"] = self.old_scanner_path
        if self.old_retention is None:
            os.environ.pop("ADMIN_HANDOFF_RETENTION_DAYS", None)
        else:
            os.environ["ADMIN_HANDOFF_RETENTION_DAYS"] = self.old_retention
        self.tmp.cleanup()

    async def _insert_action(self, scanner_event_id: int, admin_at: str) -> None:
        now = router._now()
        async with router._connect() as db:
            await db.execute(
                """
                INSERT INTO actions(
                    scanner_event_id,scanner_vk_message,vk_user_id,source,target_type,target_id,
                    external_client_id,status,error,details_json,attempts,admin_message_at,
                    created_at,updated_at,processed_at
                ) VALUES(?,?,'101','senler','senler_subscription','3748755','101',
                         'success','','{}',1,?,?,?,?)
                """,
                (scanner_event_id, str(scanner_event_id), admin_at, now, now, now),
            )
            await db.commit()

    async def test_restart_backfill_does_not_reactivate_expired_membership(self) -> None:
        admin_at = "2026-06-01T10:00:00Z"
        await self._insert_action(1, admin_at)
        key = router._membership_key("senler", "senler_subscription", "3748755", "101", "101")
        async with router._connect() as db:
            await db.execute("DELETE FROM settings WHERE key='memberships_seed_version'")
            await db.execute(
                """
                INSERT INTO memberships(
                    member_key,vk_user_id,source,target_type,target_id,external_client_id,
                    last_scanner_event_id,last_admin_message_at,expires_at,status,remove_error,
                    remove_details_json,remove_attempts,created_at,updated_at,removed_at
                ) VALUES(?,?,?,?,?,?,?,?,?,'expired','','{}',7,?,?,?)
                """,
                (
                    key,"101","senler","senler_subscription","3748755","101",1,admin_at,
                    "2026-06-08T10:00:00Z",admin_at,admin_at,"2026-06-08T10:01:00Z",
                ),
            )
            await router._seed_memberships_from_actions(db)
            await db.commit()
            cur = await db.execute("SELECT status,remove_attempts,removed_at FROM memberships WHERE member_key=?", (key,))
            row = await cur.fetchone()
        self.assertEqual("expired", row["status"])
        self.assertEqual(7, row["remove_attempts"])
        self.assertEqual("2026-06-08T10:01:00Z", row["removed_at"])

    async def test_backfill_creates_only_latest_missing_membership(self) -> None:
        await self._insert_action(1, "2026-07-01T10:00:00Z")
        await self._insert_action(2, "2026-07-02T10:00:00Z")
        async with router._connect() as db:
            await db.execute("DELETE FROM settings WHERE key='memberships_seed_version'")
            await router._seed_memberships_from_actions(db)
            await db.commit()
            cur = await db.execute("SELECT COUNT(*) AS total,MAX(last_scanner_event_id) AS event_id FROM memberships")
            row = await cur.fetchone()
        self.assertEqual(1, row["total"])
        self.assertEqual(2, row["event_id"])

    async def test_workspace_counts_people_separately_from_external_stops(self) -> None:
        scanner_path = Path(os.environ["ADMIN_HANDOFF_SCANNER_DB_PATH"])
        async with router._connect(scanner_path) as db:
            await db.executescript(
                """
                CREATE TABLE scan_events(
                    id INTEGER PRIMARY KEY,vk_user_id TEXT,peer_id TEXT,profile_name TEXT,
                    message_at TEXT,message_text_preview TEXT,status TEXT,reason TEXT,
                    admin_before_at TEXT,admin_before_author_id TEXT,
                    admin_after_at TEXT,admin_after_author_id TEXT,
                    read_at TEXT,opened_at TEXT,updated_at TEXT
                );
                """
            )
            await db.executemany(
                "INSERT INTO scan_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1,"101","101","Анна","2026-07-15T06:00:00Z","Нужна помощь","admin_conversation","","2026-07-15T05:00:00Z","9001","","","","","2026-07-15T06:00:00Z"),
                    (2,"202","202","Борис","2026-07-14T06:00:00Z","Спасибо","resolved_not_unanswered","","2026-07-14T05:00:00Z","9002","","","2026-07-14T07:00:00Z","2026-07-14T07:00:00Z","2026-07-14T07:00:00Z"),
                    (3,"303","303","Вера","2026-07-15T07:00:00Z","Где ответ?","missing_webhook","","","","","","","","2026-07-15T07:00:00Z"),
                ],
            )
            await db.commit()

        now = datetime.now(timezone.utc)
        expires_today = (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        expires_later = (now + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        memberships = [
            ("senler:1","101","senler","senler_subscription","3748755","101",1,expires_today),
            ("salebot:1","101","salebot","salebot_list","2427562","501",1,expires_today),
            ("senler:2","202","senler","senler_subscription","3748755","202",2,expires_later),
        ]
        async with router._connect() as db:
            await db.executemany(
                """
                INSERT INTO memberships(
                    member_key,vk_user_id,source,target_type,target_id,external_client_id,
                    last_scanner_event_id,last_admin_message_at,expires_at,status,remove_error,
                    remove_details_json,remove_attempts,created_at,updated_at,removed_at
                ) VALUES(?,?,?,?,?,?,?,'2026-07-15T05:00:00Z',?,'active','','{}',0,?,?,'')
                """,
                [(*row, created, created) for row in memberships],
            )
            await db.commit()

        original_admin_names = router._admin_names

        async def fake_admin_names(ids):
            return {"9001": "Админ 1", "9002": "Админ 2"}

        router._admin_names = fake_admin_names
        try:
            payload = await router._workspace_payload()
        finally:
            router._admin_names = original_admin_names

        self.assertEqual(2, payload["counts"]["protected_people"])
        self.assertEqual(3, payload["counts"]["active_stops"])
        self.assertEqual(2, payload["counts"]["senler_stops"])
        self.assertEqual(1, payload["counts"]["salebot_stops"])
        self.assertEqual(1, payload["counts"]["waiting_admin_people"])
        self.assertEqual(1, payload["counts"]["waiting_admin_unread"])
        self.assertEqual(1, payload["counts"]["problem_people"])
        anna = next(item for item in payload["people"] if item["vk_user_id"] == "101")
        self.assertEqual(2, anna["active_stops"])
        self.assertEqual(["salebot", "senler"], anna["services"])
        self.assertEqual("Анна", anna["profile_name"])

    async def test_admin_names_are_cached_between_panel_payloads(self) -> None:
        calls = []
        original_vk_api_call = router._vk_api_call
        router._admin_name_cache.clear()

        async def fake_vk_api_call(method, params, timeout=15.0):
            calls.append((method, params, timeout))
            return {"response": [{"id": 9001, "first_name": "Анна", "last_name": "Админ"}]}

        router._vk_api_call = fake_vk_api_call
        try:
            first = await router._admin_names(["9001"])
            second = await router._admin_names(["9001"])
        finally:
            router._vk_api_call = original_vk_api_call
            router._admin_name_cache.clear()

        self.assertEqual({"9001": "Анна Админ"}, first)
        self.assertEqual(first, second)
        self.assertEqual(1, len(calls))


if __name__ == "__main__":
    unittest.main()
