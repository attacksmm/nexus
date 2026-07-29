import json
import logging
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
from starlette.requests import Request

import router


def request_for(body: bytes, secret: str = "source-secret") -> Request:
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/webhook/source-uuid",
        "raw_path": b"/webhook/source-uuid",
        "query_string": b"",
        "server": ("example.test", 443),
        "client": ("127.0.0.1", 1),
        "headers": [
            (b"x-nexus-senler-secret", secret.encode()),
            (b"content-type", b"application/json"),
        ],
    }
    return Request(scope, receive)


class TelegramEventWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="senler-telegram-events-")
        router._db_path = Path(self.tmp.name) / "events.db"
        router._logger = logging.getLogger("senler-telegram-events-tests")
        await router._init_db()
        now = router._now()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO sources(uuid,name,secret_hash,secret_hint,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (
                    "source-uuid",
                    "Тестовый бот",
                    router._secret_hash("source-uuid", "source-secret"),
                    "••••cret",
                    now,
                    now,
                ),
            )
            await db.commit()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_webhook_stores_exact_json_and_deduplicates_update(self):
        payload = {
            "update_id": 501,
            "message": {
                "message_id": 7,
                "from": {"id": 42, "username": "tester"},
                "chat": {"id": 42, "type": "private"},
                "text": "<script>alert(1)</script>",
            },
        }
        body = json.dumps(payload, ensure_ascii=False).encode()
        first = await router.webhook("source-uuid", request_for(body))
        duplicate = await router.webhook("source-uuid", request_for(body))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(duplicate.status_code, 200)
        self.assertFalse(json.loads(first.body)["duplicate"])
        self.assertTrue(json.loads(duplicate.body)["duplicate"])

        async with aiosqlite.connect(router._must_db()) as db:
            event = await (
                await db.execute(
                    "SELECT update_id,summary,raw_payload,duplicate_count,parse_status FROM events"
                )
            ).fetchone()
            source = await (
                await db.execute(
                    "SELECT received_count,unique_count,duplicate_count,rejected_count FROM sources"
                )
            ).fetchone()
        self.assertEqual(event[0], "501")
        self.assertIn("<script>alert(1)</script>", event[1])
        self.assertEqual(json.loads(event[2]), payload)
        self.assertEqual(event[3:], (1, "ok"))
        self.assertEqual(source, (2, 1, 1, 0))

    async def test_wrong_secret_is_rejected_without_database_write(self):
        response = await router.webhook("source-uuid", request_for(b'{"update_id":502}', "wrong"))
        self.assertEqual(response.status_code, 401)
        async with aiosqlite.connect(router._must_db()) as db:
            count = (await (await db.execute("SELECT COUNT(*) FROM events")).fetchone())[0]
        self.assertEqual(count, 0)

    async def test_invalid_json_is_recorded_once_and_returns_200(self):
        response = await router.webhook("source-uuid", request_for(b"not-json"))
        duplicate = await router.webhook("source-uuid", request_for(b"not-json"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(duplicate.status_code, 200)
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (
                await db.execute(
                    "SELECT event_type,parse_status,raw_payload,duplicate_count FROM events"
                )
            ).fetchone()
        self.assertEqual(row, ("invalid_json", "invalid_json", "not-json", 1))

    async def test_oversized_event_is_rejected_without_body_and_not_collapsed(self):
        original_limit = router.MAX_BODY_BYTES
        router.MAX_BODY_BYTES = 16
        try:
            first = await router.webhook("source-uuid", request_for(b"a" * 30))
            second = await router.webhook("source-uuid", request_for(b"b" * 30))
        finally:
            router.MAX_BODY_BYTES = original_limit
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (
                await db.execute(
                    "SELECT parse_status,raw_payload,body_size FROM events ORDER BY id"
                )
            ).fetchall()
        self.assertEqual(rows, [("rejected", "", 17), ("rejected", "", 17)])

    async def test_cleanup_respects_retention_setting(self):
        old = (datetime.now(timezone.utc) - timedelta(days=31)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """
                INSERT INTO events(source_id,body_hash,event_type,summary,received_at,last_received_at)
                VALUES(1,'old-hash','message','Старое событие',?,?)
                """,
                (old, old),
            )
            await db.commit()
        self.assertEqual(await router._cleanup_events(), 1)

    async def test_admin_source_creation_never_stores_plain_secret(self):
        original_auth = router._require_admin

        async def allow_admin(_request):
            return {"role": "admin"}

        router._require_admin = allow_admin
        try:
            created = await router.create_source(
                request_for(json.dumps({"name": "Второй бот"}, ensure_ascii=False).encode())
            )
            source = created["source"]
            secret = created["secret"]
            self.assertNotIn("secret_hash", source)
            self.assertEqual(source["header_name"], router.WEBHOOK_HEADER)
            self.assertTrue(source["webhook_path"].endswith(source["uuid"]))

            async with aiosqlite.connect(router._must_db()) as db:
                stored_hash, hint = await (
                    await db.execute(
                        "SELECT secret_hash,secret_hint FROM sources WHERE id=?", (source["id"],)
                    )
                ).fetchone()
            self.assertEqual(stored_hash, router._secret_hash(source["uuid"], secret))
            self.assertNotIn(secret, stored_hash)
            self.assertEqual(hint, f"••••{secret[-4:]}")

            rotated = await router.rotate_source_secret(source["id"], request_for(b"{}"))
            self.assertNotEqual(rotated["secret"], secret)
            async with aiosqlite.connect(router._must_db()) as db:
                new_hash = (
                    await (
                        await db.execute(
                            "SELECT secret_hash FROM sources WHERE id=?", (source["id"],)
                        )
                    ).fetchone()
                )[0]
            self.assertEqual(new_hash, router._secret_hash(source["uuid"], rotated["secret"]))
        finally:
            router._require_admin = original_auth


if __name__ == "__main__":
    unittest.main()
