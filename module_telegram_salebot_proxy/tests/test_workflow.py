import json
import logging
import os
import tempfile
import unittest
from pathlib import Path

import aiosqlite
from starlette.requests import Request

os.environ.setdefault("NEXUS_SECRET", "workflow-test-secret-at-least-32-characters")
os.environ.pop("TG_SALEBOT_PROXY_PUBLIC_BASE", None)

import router


class FakeResponse:
    status_code = 200
    content = b'{"ok":true}'
    headers = {"content-type": "application/json"}


class FakeClient:
    posts = 0

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, *args, **kwargs):
        type(self).posts += 1
        return FakeResponse()


class ProxyWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="telegram-proxy-tests-")
        router._db_path = Path(self.tmp.name) / "proxy.db"
        router._logger = logging.getLogger("telegram-proxy-tests")
        await router._init_db()
        now = router._now()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """
                INSERT INTO bots(
                    telegram_id,username,token_enc,token_hint,path_secret,telegram_secret,state,
                    auto_activate,auto_recover,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,'waiting',1,1,?,?)
                """,
                (
                    "123456", "workflow_bot", router._encrypt("123456:test-token"),
                    "123456:••••", "path-secret", "header-secret", now, now,
                ),
            )
            await db.commit()

    async def asyncTearDown(self):
        router._bot_locks.clear()
        self.tmp.cleanup()

    async def test_auto_capture_activation_delivery_and_duplicate(self):
        telegram = {"url": "https://chatter.salebot.pro/telegram/closed-target", "pending_update_count": 0}
        original_tg_call = router._tg_call

        async def fake_tg_call(token, method, payload=None):
            self.assertEqual(token, "123456:test-token")
            if method == "getWebhookInfo":
                return dict(telegram)
            if method == "setWebhook":
                telegram["url"] = payload["url"]
                return {"value": True}
            raise AssertionError(method)

        router._tg_call = fake_tg_call
        try:
            first = await router._observe_once(1)
            second = await router._observe_once(1)
        finally:
            router._tg_call = original_tg_call

        self.assertEqual(first["action"], "salebot_observed")
        self.assertEqual(second["action"], "captured_and_activated")
        bot = await router._bot_row(1)
        self.assertEqual(bot["state"], "active")
        self.assertEqual(router._decrypt(bot["upstream_url_enc"]), "https://chatter.salebot.pro/telegram/closed-target")
        self.assertIn("/telegram-salebot-proxy/api/webhook/123456/path-secret", telegram["url"])

        payload = {"update_id": 9001, "message": {"chat": {"id": 10}, "from": {"id": 20}, "text": "private"}}
        body = json.dumps(payload).encode()

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        scope = {
            "type": "http", "http_version": "1.1", "method": "POST",
            "scheme": "https", "path": "/webhook", "raw_path": b"/webhook",
            "query_string": b"", "server": ("example.test", 443), "client": ("127.0.0.1", 1),
            "headers": [(b"x-telegram-bot-api-secret-token", b"header-secret"), (b"content-type", b"application/json")],
        }
        original_client = router.httpx.AsyncClient
        FakeClient.posts = 0
        router.httpx.AsyncClient = FakeClient
        try:
            response = await router.telegram_webhook("123456", "path-secret", Request(scope, receive))

            async def receive_duplicate():
                return {"type": "http.request", "body": body, "more_body": False}

            duplicate = await router.telegram_webhook("123456", "path-secret", Request(scope, receive_duplicate))
        finally:
            router.httpx.AsyncClient = original_client

        self.assertEqual(response.status_code, 200)
        self.assertEqual(duplicate.status_code, 200)
        self.assertEqual(FakeClient.posts, 1)
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute("SELECT status,raw_payload,attempts FROM events WHERE update_id='9001'")).fetchone()
            counters = await (await db.execute("SELECT received_count,delivered_count,failed_count FROM bots WHERE id=1")).fetchone()
        self.assertEqual(row, ("delivered", "", 1))
        self.assertEqual(counters, (1, 1, 0))

    async def test_polling_fallback_keeps_and_delivers_pending_update(self):
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "UPDATE bots SET state='active',upstream_url_enc=?,upstream_masked=? WHERE id=1",
                (
                    router._encrypt("https://chatter.salebot.pro/telegram/closed-target"),
                    "https://chatter.salebot.pro/telegram/••••",
                ),
            )
            await db.commit()
        original_tg_call = router._tg_call
        original_client = router.httpx.AsyncClient
        payload = {"update_id": 9010, "message": {"chat": {"id": 10}, "from": {"id": 20}, "text": "pending"}}

        async def fake_tg_call(token, method, data=None):
            self.assertEqual(token, "123456:test-token")
            if method == "deleteWebhook":
                self.assertFalse(data["drop_pending_updates"])
                return {"value": True}
            if method == "getUpdates":
                return {"value": [payload]}
            raise AssertionError(method)

        FakeClient.posts = 0
        router._tg_call = fake_tg_call
        router.httpx.AsyncClient = FakeClient
        try:
            bot = await router._bot_row(1)
            await router._switch_to_polling(bot, reason="Connection timed out")
            bot = await router._bot_row(1)
            result = await router._poll_once(bot)
        finally:
            router._tg_call = original_tg_call
            router.httpx.AsyncClient = original_client

        self.assertEqual(result["action"], "polling_active")
        self.assertEqual(result["delivered"], 1)
        self.assertEqual(FakeClient.posts, 1)
        bot = await router._bot_row(1)
        self.assertEqual(bot["transport"], "polling")
        self.assertEqual(bot["poll_offset"], 9011)
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute("SELECT status,raw_payload FROM events WHERE update_id='9010'")).fetchone()
        self.assertEqual(row, ("delivered", ""))


if __name__ == "__main__":
    unittest.main()
