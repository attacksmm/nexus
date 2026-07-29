import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiosqlite
from starlette.requests import Request

import router


def request_for(
    path: str,
    body: dict | None = None,
    *,
    origin: str = router.ALLOWED_ORIGIN,
    token: str = "",
    test_mode: bool = False,
) -> Request:
    raw = json.dumps(body or {}).encode()
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": raw, "more_body": False}

    headers = [(b"origin", origin.encode()), (b"content-type", b"application/json"), (b"host", b"junior.sobakovod.pro")]
    if token:
        headers.append((b"authorization", ("Bearer " + token).encode()))
    if test_mode:
        headers.append((b"x-nexus-wazzup-test", b"1"))
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "server": ("junior.sobakovod.pro", 443),
        "client": ("203.0.113.9", 50100),
        "headers": headers,
    }
    return Request(scope, receive)


class GetCourseWazzupWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="getcourse-wazzup-")
        router._db_path = Path(self.tmp.name) / "module.db"
        router._logger = logging.getLogger("getcourse-wazzup-tests")
        await router._init_db()
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cur = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)",
                ("wazzup-user-7", "Анна", now, now),
            )
            self.admin_id = int(cur.lastrowid)
            await db.commit()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _code(self) -> str:
        code = router._activation_code()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO activation_codes(admin_id,code_hash,expires_at,created_at) VALUES(?,?,?,?)",
                (self.admin_id, router._hash(router._normalize_code(code)), "2999-01-01T00:00:00Z", router._iso()),
            )
            await db.commit()
        return code

    async def test_one_time_activation_and_global_iframe_flow(self):
        code = await self._code()
        activated = await router.widget_activate(request_for("/widget/activate", {"code": code}))
        payload = json.loads(activated.body)
        self.assertEqual(activated.status_code, 200)
        self.assertIn("device_token", payload)

        replay = await router.widget_activate(request_for("/widget/activate", {"code": code}))
        self.assertEqual(replay.status_code, 401)

        captured = []
        original = router._wazzup_request

        async def fake_wazzup(method, path, body=None):
            captured.append((method, path, body))
            if method == "GET" and path == "/channels":
                return [{"channelId": "wa-1", "transport": "whatsapp", "state": "active", "name": "Служба заботы"}]
            return {"url": "https://web.wazzup24.com/iframe/temporary-secret"}

        router._wazzup_request = fake_wazzup
        try:
            response = await router.widget_iframe_link(
                request_for(
                    "/widget/iframe-link",
                    {
                        "phone": "+7 (911) 447-40-13",
                        "name": "Клиент",
                        "source_url": "https://club.sobakovod.pro/user/control/user/update/id/394523316",
                    },
                    token=payload["device_token"],
                )
            )
        finally:
            router._wazzup_request = original

        result = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["phone"], "+79114474013")
        self.assertEqual(captured[0], ("GET", "/channels", None))
        self.assertEqual(captured[1], ("POST", "/contacts", [{"id": "getcourse-user-394523316", "responsibleUserId": "wazzup-user-7", "name": "Клиент", "contactData": [{"chatType": "whatsapp", "chatId": "79114474013"}], "uri": "https://club.sobakovod.pro/user/control/user/update/id/394523316"}]))
        self.assertEqual(captured[2], ("POST", "/iframe", {"user": {"id": "wazzup-user-7", "name": "Анна"}, "scope": "card", "filter": [{"chatType": "whatsapp", "chatId": "79114474013", "name": "Клиент"}], "activeChat": {"channelId": "wa-1", "chatType": "whatsapp", "chatId": "79114474013"}, "options": {"clientType": "GetCourse"}}))

        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (await db.execute("SELECT action,status,phone_mask,error FROM events ORDER BY id")).fetchall()
            dump = "\n".join(str(row) for row in await (await db.execute("SELECT * FROM events")).fetchall())
        self.assertEqual(rows[0][0:2], ("activate", "ok"))
        self.assertEqual(rows[1], ("sync_contact", "ok", "+79*****4013", ""))
        self.assertEqual(rows[2], ("open_iframe", "ok", "+79*****4013", ""))
        self.assertNotIn("temporary-secret", dump)
        self.assertNotIn("+79114474013", dump)

    async def test_provisions_getcourse_staff_as_wazzup_users(self):
        captured = []

        async def fake_wazzup(method, path, body=None):
            captured.append((method, path, body))
            return []

        original = router._wazzup_request
        router._wazzup_request = fake_wazzup
        try:
            count = await router._upsert_admins([
                {"id": "getcourse-42", "name": "Менеджер", "phone": "+7 (999) 123-45-67"}
            ], "test")
        finally:
            router._wazzup_request = original
        self.assertEqual(count, 1)
        self.assertEqual(captured, [("POST", "/users", [{"id": "getcourse-42", "name": "Менеджер", "phone": "79991234567"}])])
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute("SELECT wazzup_user_id,name,phone FROM admins WHERE wazzup_user_id='getcourse-42'")).fetchone()
        self.assertEqual(row, ("getcourse-42", "Менеджер", "+79991234567"))

    async def test_widget_syncs_visible_getcourse_staff(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        captured = []

        async def fake_wazzup(method, path, body=None):
            captured.append((method, path, body))
            return []

        original = router._wazzup_request
        router._wazzup_request = fake_wazzup
        try:
            response = await router.widget_staff_sync(
                request_for("/widget/staff-sync", {"staff": [{"id": "42", "name": "Менеджер", "phone": "8 999 123 45 67"}]}, token=token)
            )
        finally:
            router._wazzup_request = original
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["synced"], 1)
        self.assertEqual(captured, [("POST", "/users", [{"id": "getcourse-42", "name": "Менеджер", "phone": "79991234567"}])])

    async def test_widget_lists_only_active_wazzup_channels(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]

        async def fake_wazzup(method, path, body=None):
            self.assertEqual((method, path, body), ("GET", "/channels", None))
            return [
                {"channelId": "wa-old", "transport": "whatsapp", "state": "blocked", "name": "Old WhatsApp"},
                {"channelId": "max-1", "transport": "max", "state": "active", "name": "Служба заботы"},
            ]

        original = router._wazzup_request
        router._wazzup_request = fake_wazzup
        try:
            response = await router.widget_channels(request_for("/widget/channels", {}, token=token))
        finally:
            router._wazzup_request = original
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["channels"], [{"channel_id": "max-1", "transport": "max", "channel_transport": "max", "name": "Служба заботы", "plain_id": ""}])

    async def test_webhook_stores_incoming_message_without_sending(self):
        payload = {
            "messages": [{
                "messageId": "msg-in-1",
                "channelId": "max-1",
                "chatType": "max",
                "chatId": "79108758427",
                "incoming": True,
                "text": "Входящее тестовое сообщение",
                "contact": {"name": "Елена", "phone": "79108758427"},
                "dateTime": "2026-07-25T09:00:00Z",
            }]
        }
        async with aiosqlite.connect(router._must_db()) as db:
            secret = (await (await db.execute("SELECT value FROM module_settings WHERE key='webhook_secret'")).fetchone())[0]
        response = await router.inbound_webhook(secret, request_for("/webhook/inbound/test", payload))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["messages"], 1)
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute("SELECT direction,text,chat_type,chat_id FROM wazzup_messages WHERE external_id='msg-in-1'")).fetchone()
        self.assertEqual(row, ("incoming", "Входящее тестовое сообщение", "max", "79108758427"))

    async def test_webhook_registration_subscribes_only_to_messages_and_statuses(self):
        captured = []

        async def fake_wazzup(method, path, body=None):
            captured.append((method, path, body))
            return {"ok": True}

        with (
            patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})),
            patch.object(router, "_wazzup_request", new=fake_wazzup),
            patch.object(router, "enforce_rate_limit"),
        ):
            result = await router.register_webhook(request_for("/webhook/register", {}))

        self.assertTrue(result["configured"])
        self.assertEqual(captured[0][0:2], ("PATCH", "/webhooks"))
        self.assertEqual(captured[0][2]["subscriptions"], {"messagesAndStatuses": True})
        self.assertRegex(captured[0][2]["webhooksUri"], r"^https://junior\.sobakovod\.pro/nexus/getcourse-wazzup/api/webhook/inbound/[A-Za-z0-9_-]+$")

    async def test_stores_imported_history_and_real_max_chat_id(self):
        candidate = {
            "channel_id": "max-1", "chat_type": "max", "chat_id": "62837516", "contact_name": "Елена"
        }
        imported = await router._store_history(candidate, "+79108758427", [{
            "id": "history-in-1",
            "channelId": "max-1",
            "chatType": "max",
            "chatId": "62837516",
            "incoming": True,
            "text": "Сообщение из истории",
            "datetime": "2026-07-23T12:17:00Z",
        }])
        self.assertEqual(imported, 1)
        chat_id, has_chat, messages = await router._conversation_rows("max-1", "max", "+79108758427")
        self.assertEqual(chat_id, "62837516")
        self.assertTrue(has_chat)
        self.assertEqual(messages[0]["text"], "Сообщение из истории")

    async def test_own_chat_send_uses_public_message_api_only_after_explicit_call(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        captured = []

        async def fake_wazzup(method, path, body=None):
            captured.append((method, path, body))
            if (method, path) == ("GET", "/channels"):
                return [{"channelId": "max-1", "transport": "max", "state": "active", "name": "Служба заботы"}]
            if (method, path) == ("POST", "/message"):
                return {"messageId": "msg-out-1"}
            raise AssertionError((method, path, body))

        original = router._wazzup_request
        router._wazzup_request = fake_wazzup
        try:
            response = await router.widget_send(request_for(
                "/widget/send",
                {
                    "phone": "+79108758427",
                    "name": "Елена",
                    "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
                    "channel_id": "max-1",
                    "transport": "max",
                    "text": "Сообщение только для mock-теста",
                },
                token=token,
            ))
        finally:
            router._wazzup_request = original
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured[-1][0:2], ("POST", "/message"))
        message = captured[-1][2]
        self.assertEqual(message["channelId"], "max-1")
        self.assertEqual(message["chatType"], "max")
        self.assertEqual(message["phone"], "79108758427")
        self.assertNotIn("chatId", message)
        self.assertEqual(message["crmUserId"], "wazzup-user-7")
        self.assertRegex(message["crmMessageId"], r"^nexus-[0-9a-f]{32}$")
        self.assertEqual(message["text"], "Сообщение только для mock-теста")
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute("SELECT direction,status,text FROM wazzup_messages WHERE external_id='msg-out-1'")).fetchone()
        self.assertEqual(row, ("outgoing", "accepted", "Сообщение только для mock-теста"))

    async def test_rejects_foreign_origin(self):
        code = await self._code()
        response = await router.widget_activate(request_for("/widget/activate", {"code": code}, origin="https://evil.test"))
        self.assertEqual(response.status_code, 403)
        self.assertNotIn("access-control-allow-origin", response.headers)

    async def test_test_page_requires_nexus_admin_session(self):
        code = await self._code()
        request = request_for(
            "/widget/activate",
            {"code": code},
            origin="https://junior.sobakovod.pro",
            test_mode=True,
        )
        response = await router.widget_activate(request)
        self.assertEqual(response.status_code, 403)

    async def test_authorized_same_origin_test_page_can_activate(self):
        code = await self._code()
        request = request_for(
            "/widget/activate",
            {"code": code},
            origin="https://junior.sobakovod.pro",
            test_mode=True,
        )
        with patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})):
            response = await router.widget_activate(request)
        self.assertEqual(response.status_code, 200)
        self.assertIn("device_token", json.loads(response.body))
        self.assertNotIn("access-control-allow-origin", response.headers)

    async def test_revoked_device_requires_reactivation(self):
        code = await self._code()
        activated = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute("UPDATE devices SET revoked_at=?", (router._iso(),))
            await db.commit()
        response = await router.widget_iframe_link(
            request_for(
                "/widget/iframe-link",
                {"source_url": "https://club.sobakovod.pro/user/control/user/update/id/1"},
                token=activated["device_token"],
            )
        )
        self.assertEqual(response.status_code, 401)
        self.assertTrue(json.loads(response.body)["reauth"])


if __name__ == "__main__":
    unittest.main()
