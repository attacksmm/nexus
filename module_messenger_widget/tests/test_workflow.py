import asyncio
import json
import logging
import tempfile
import time
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
    origin: str = router.DEFAULT_ALLOWED_ORIGIN,
    token: str = "",
    test_mode: bool = False,
    platform: str = "",
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
    if platform:
        headers.append((b"x-nexus-messenger-platform", platform.encode()))
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
        router._telegram_state_cache = (time.monotonic() + 3600, {"api": False, "authorized": False, "account": {}})
        router._telegram_history_cache.clear()
        router._wazzup_history_inflight.clear()
        router._telegram_auth_pending.clear()
        router._card_link_cache.clear()
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

    async def test_persistent_activation_and_global_iframe_flow(self):
        code = await self._code()
        activated = await router.widget_activate(request_for("/widget/activate", {"code": code}))
        payload = json.loads(activated.body)
        self.assertEqual(activated.status_code, 200)
        self.assertIn("device_token", payload)

        replay = await router.widget_activate(request_for("/widget/activate", {"code": code}))
        self.assertEqual(replay.status_code, 200)

        async with aiosqlite.connect(router._must_db()) as db:
            used_at = (await (await db.execute("SELECT used_at FROM activation_codes LIMIT 1")).fetchone())[0]
        self.assertEqual(used_at, "")

        captured = []
        original = router._wazzup_request

        async def fake_wazzup(method, path, body=None, **_kwargs):
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
        self.assertEqual([row[0] for row in rows[:2]], ["activate", "activate"])
        self.assertEqual(rows[-2], ("sync_contact", "ok", "+79*****4013", ""))
        self.assertEqual(rows[-1], ("open_iframe", "ok", "+79*****4013", ""))
        self.assertNotIn("temporary-secret", dump)
        self.assertNotIn("+79114474013", dump)

    async def test_reissue_revokes_every_active_device(self):
        code = await self._code()
        activated = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)
        with patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})):
            result = await router.create_activation_code(self.admin_id, request_for("/admins/1/activation-code", {}))
        self.assertTrue(result["reissued"])
        self.assertEqual(result["revoked_devices"], 1)
        self.assertNotEqual(result["code"], code)
        self.assertIsNone(await router._device(request_for("/widget/context", {}, token=activated["device_token"])))

    async def test_widget_logout_revokes_current_device(self):
        code = await self._code()
        activated = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)
        token = activated["device_token"]
        response = await router.widget_logout(request_for("/widget/logout", {}, token=token))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.body)["ok"])
        self.assertIsNone(await router._device(request_for("/widget/context", {}, token=token)))

    async def test_wazzup_history_is_scheduled_without_blocking_the_request(self):
        started = asyncio.Event()

        async def history(*_args, **_kwargs):
            started.set()
            return {"status": "imported", "imported": 0, "complete": True}

        channel = {"channel_id": "max-1", "transport": "max"}
        with (
            patch.object(router, "_record_history_sync", new=AsyncMock()),
            patch.object(router, "_import_wazzup_history", new=history),
        ):
            router._schedule_wazzup_history(
                {"id": 1, "admin_id": self.admin_id}, channel, "+79990000000", name="Клиент",
            )
            await asyncio.wait_for(started.wait(), timeout=1)
            for _ in range(20):
                if not router._wazzup_history_inflight:
                    break
                await asyncio.sleep(0)
        self.assertFalse(router._wazzup_history_inflight)

    async def test_salebot_history_runs_only_after_explicit_channel_open(self):
        channel = {
            "channel_id": "salebot:project", "transport": "salebot", "channel_transport": "salebot",
            "provider": "salebot", "name": "Проект", "plain_id": "project", "label": "SaleBot · Проект",
        }
        history = AsyncMock(return_value=[])
        with (
            patch.object(router, "_cached_active_channels", new=AsyncMock(return_value=[])),
            patch.object(router, "_vk_channel", new=AsyncMock(return_value=None)),
            patch.object(router, "_telegram_channel", new=AsyncMock(return_value=None)),
            patch.object(router, "_salebot_channel", return_value=channel),
            patch.object(router, "_salebot_history", new=history),
        ):
            self.assertEqual(await router._all_channels(), [channel])
            history.assert_not_awaited()
            with (
                patch.object(router, "_widget_request_mode", new=AsyncMock(return_value="getcourse")),
                patch.object(router, "_device", new=AsyncMock(return_value={"id": 1, "admin_id": self.admin_id, "admin_name": "Анна"})),
                patch.object(router, "_provider_card_link", new=AsyncMock(return_value={"external_user_id": "998877"})),
            ):
                response = await router.widget_conversation(request_for("/widget/conversation", {
                    "channel_id": "salebot:project", "transport": "salebot", "provider": "salebot",
                }))
            self.assertEqual(response.status_code, 200)
            history.assert_awaited_once_with("998877")

    async def test_schema_contains_inbox_and_external_identity_tables(self):
        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (await db.execute("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
            admin_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(admins)")).fetchall()}
            chat_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(wazzup_chats)")).fetchall()}
            link_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(client_links)")).fetchall()}
            template_columns = {row[1] for row in await (await db.execute("PRAGMA table_info(message_templates)")).fetchall()}
        names = {row[0] for row in rows}
        self.assertTrue({"client_links", "inbox_devices", "inbox_reads", "external_identity_links", "template_favorites"} <= names)
        self.assertIn("role", admin_columns)
        self.assertIn("responsible_admin_id", chat_columns)
        self.assertIn("responsible_admin_id", link_columns)
        self.assertIn("folder", template_columns)

    async def test_template_favorites_are_private_to_each_module_user(self):
        token = "favorite-device-token-000000000000000"
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            other = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)",
                ("other-user", "Другой сотрудник", now, now),
            )
            other_admin_id = int(other.lastrowid)
            await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (self.admin_id, router._hash(token), "favorite", now, now, "2999-01-01T00:00:00Z"),
            )
            shared = await db.execute(
                """INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at)
                   VALUES(NULL,'Собака','Общий','Текст',1,0,?,?)""",
                (now, now),
            )
            personal = await db.execute(
                """INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at)
                   VALUES(?,'Личные','Мой','Текст',1,0,?,?)""",
                (self.admin_id, now, now),
            )
            foreign = await db.execute(
                """INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at)
                   VALUES(?,'Личные','Чужой','Текст',1,0,?,?)""",
                (other_admin_id, now, now),
            )
            shared_id, personal_id, foreign_id = int(shared.lastrowid), int(personal.lastrowid), int(foreign.lastrowid)
            await db.commit()

        for template_id in (shared_id, personal_id):
            response = await router.widget_templates(request_for(
                "/widget/templates", {"action": "favorite", "id": template_id, "favorite": True}, token=token,
            ))
            self.assertEqual(response.status_code, 200)

        listed = await router.widget_templates(request_for(
            "/widget/templates", {"action": "list"}, token=token,
        ))
        rows = json.loads(listed.body)["templates"]
        favorites = {row["id"]: row for row in rows if row["favorite"]}
        self.assertEqual(set(favorites), {shared_id, personal_id})
        self.assertEqual([favorites[shared_id]["favorite_order"], favorites[personal_id]["favorite_order"]], [0, 1])
        self.assertFalse((await router._template_rows(other_admin_id))[0]["favorite"])

        denied = await router.widget_templates(request_for(
            "/widget/templates", {"action": "favorite", "id": foreign_id, "favorite": True}, token=token,
        ))
        self.assertEqual(denied.status_code, 404)

        removed = await router.widget_templates(request_for(
            "/widget/templates", {"action": "favorite", "id": shared_id, "favorite": False}, token=token,
        ))
        self.assertFalse(json.loads(removed.body)["favorite"])
        self.assertFalse(next(row for row in await router._template_rows(self.admin_id) if row["id"] == shared_id)["favorite"])

    async def test_admin_manages_one_employees_personal_templates_and_shared_favorites(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            other = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)",
                ("employee-templates", "Наталья Абрамова", now, now),
            )
            employee_id = int(other.lastrowid)
            shared = await db.execute(
                """INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at)
                   VALUES(NULL,'amoCRM','Общий','Здравствуйте, {{contact.name}}!',1,0,?,?)""",
                (now, now),
            )
            foreign = await db.execute(
                """INSERT INTO message_templates(owner_admin_id,folder,title,body,enabled,sort_order,created_at,updated_at)
                   VALUES(?,'Личные','Чужой','Не менять',1,0,?,?)""",
                (self.admin_id, now, now),
            )
            shared_id, foreign_id = int(shared.lastrowid), int(foreign.lastrowid)
            await db.commit()

        admin_user = AsyncMock(return_value={"username": "nikita"})
        with patch.object(router, "_require_admin", new=admin_user):
            created = await router.create_admin_template(employee_id, request_for(
                f"/admins/{employee_id}/templates",
                {"folder": "Личные", "title": "Перезвон", "body": "Перезвоню {{contact.name}}!"},
            ))
            personal_id = int(created["template"]["id"])
            listed = await router.list_admin_templates(employee_id, request_for(f"/admins/{employee_id}/templates"))
            rows = {row["id"]: row for row in listed["templates"]}
            self.assertEqual(listed["admin"]["name"], "Наталья Абрамова")
            self.assertTrue(rows[personal_id]["editable"])
            self.assertFalse(rows[shared_id]["editable"])
            self.assertNotIn(foreign_id, rows)

            favorite = await router.set_admin_template_favorite(employee_id, shared_id, request_for(
                f"/admins/{employee_id}/templates/{shared_id}/favorite", {"favorite": True},
            ))
            self.assertTrue(favorite["favorite"])
            updated = await router.update_admin_template(employee_id, personal_id, request_for(
                f"/admins/{employee_id}/templates/{personal_id}",
                {"folder": "Личные", "title": "Перезвон", "body": "Добрый день, {{contact.name}}!", "enabled": True},
            ))
            self.assertEqual(updated["template"]["body"], "Добрый день, {{contact.name}}!")
            await router.delete_admin_template(employee_id, personal_id, request_for(
                f"/admins/{employee_id}/templates/{personal_id}",
            ))

        rows = await router._template_rows(employee_id)
        self.assertTrue(next(row for row in rows if row["id"] == shared_id)["favorite"])
        self.assertFalse(any(row["id"] == personal_id for row in rows))

    async def test_amocrm_admin_imports_shared_templates_without_deal_routes(self):
        token = "amo-template-import-token-000000000000"
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute("UPDATE admins SET role='admin' WHERE id=?", (self.admin_id,))
            await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at,platform,platform_user_id,platform_user_email)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (self.admin_id, router._hash(token), "import", now, now, "2999-01-01T00:00:00Z", "amocrm", "6269974", "manager@example.test"),
            )
            await db.commit()
        context = {"platform_user_id": "6269974", "platform_user_email": "manager@example.test"}
        body = {
            **context, "action": "import", "folder": "amoCRM",
            "templates": [
                {"id": 7, "name": "Приветствие", "content": "Здравствуйте, {{contact.name}}!"},
                {"id": 7, "name": "Приветствие", "content": "Здравствуйте, {{contact.name}}!"},
            ],
        }
        imported = await router.widget_templates(request_for(
            "/widget/templates", body, token=token, platform="amocrm",
            origin="https://junior.sobakovod.pro",
        ))
        result = json.loads(imported.body)
        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["skipped"], 1)
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT owner_admin_id,folder,title,body FROM message_templates"
            )).fetchone()
        self.assertEqual(tuple(row), (None, "amoCRM", "Приветствие", "Здравствуйте, {{contact.name}}!"))

        repeated = await router.widget_templates(request_for(
            "/widget/templates", body, token=token, platform="amocrm",
            origin="https://junior.sobakovod.pro",
        ))
        repeated_result = json.loads(repeated.body)
        self.assertEqual(repeated_result["imported"], 0)
        self.assertEqual(repeated_result["skipped"], 2)

    async def test_nikita_is_migrated_to_administrator(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at) VALUES(?,?,1,?,?)",
                ("nikita", "Никита", now, now),
            )
            await db.commit()
        await router._init_db()
        async with aiosqlite.connect(router._must_db()) as db:
            role = (await (await db.execute("SELECT role FROM admins WHERE wazzup_user_id='nikita'")).fetchone())[0]
        self.assertEqual(role, "admin")

    async def test_employee_inbox_is_limited_to_owned_clients(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            device_ids = []
            for suffix in ("employee", "administrator"):
                cursor = await db.execute(
                    """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                       VALUES(?,?,?,?,?,?)""",
                    (self.admin_id, suffix, suffix, now, now, "2999-01-01T00:00:00Z"),
                )
                device_ids.append(int(cursor.lastrowid))
            for suffix, owner in (("owned", self.admin_id), ("other", None)):
                await db.execute(
                    """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,contact_name,last_message_at,
                       last_message_preview,responsible_admin_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    ("max-1", "max", suffix, suffix, now, suffix, owner, now, now),
                )
                await db.execute(
                    """INSERT INTO wazzup_messages(external_id,channel_id,chat_type,chat_id,direction,text,sent_at,created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (suffix, "max-1", "max", suffix, "incoming", suffix, now, now),
                )
            await db.commit()
        channel = {"channel_id": "max-1", "transport": "max", "provider": "wazzup", "label": "MAX"}
        employee = await router._inbox_items(
            {"id": device_ids[0], "admin_id": self.admin_id, "admin_role": "employee"}, [channel]
        )
        administrator = await router._inbox_items(
            {"id": device_ids[1], "admin_id": self.admin_id, "admin_role": "admin"}, [channel]
        )
        administrator_with_stale_filter = await router._inbox_items(
            {"id": device_ids[1], "admin_id": self.admin_id, "admin_role": "admin"},
            [channel],
            selected_channel_ids=["retired-channel"],
        )
        self.assertEqual([row["chat_id"] for row in employee["items"]], ["owned"])
        self.assertEqual({row["chat_id"] for row in administrator["items"]}, {"owned", "other"})
        self.assertEqual(
            {row["chat_id"] for row in administrator_with_stale_filter["items"]},
            {"owned", "other"},
        )

    async def test_snippet_uses_stable_widget_url(self):
        with patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})):
            result = await router.snippet(request_for("/snippet", {}))
        self.assertTrue(result["static_url"].endswith("/static/widget.js"))
        self.assertNotIn("?v=", result["snippet"])

    async def test_external_links_keep_vk_and_telegram_separate(self):
        identity = {"getcourse_user_id": "42", "phone": "+79991234567", "name": "Клиент"}
        await router._remember_external_link(identity, "test", provider="vk", external_user_id="100")
        await router._remember_external_link(identity, "test", provider=router.TELEGRAM_PROVIDER, external_user_id="200")
        vk = await router._external_link(gc_id="42", provider="vk")
        telegram = await router._external_link(gc_id="42", provider=router.TELEGRAM_PROVIDER)
        self.assertEqual(vk["external_user_id"], "100")
        self.assertEqual(telegram["external_user_id"], "200")

    async def test_telegram_login_code_flow_keeps_password_out_of_settings(self):
        class Sent:
            phone_code_hash = "hash-1"

        class Client:
            authorized = False

            async def is_user_authorized(self):
                return self.authorized

            async def send_code_request(self, phone):
                self.phone = phone
                return Sent()

            async def sign_in(self, **kwargs):
                self.authorized = True

            async def get_me(self):
                return type("User", (), {"id": 77, "phone": "79991234567", "username": "operator", "first_name": "", "last_name": ""})()

        client = Client()

        async def run(callback):
            return await callback(client)

        with (
            patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})),
            patch.object(router, "_telegram_run", new=run),
            patch.object(router, "enforce_rate_limit"),
        ):
            sent = await router.telegram_send_code(request_for("/telegram/auth/send-code", {"phone": "+79991234567"}))
            confirmed = await router.telegram_confirm(request_for(
                "/telegram/auth/confirm", {"phone": "+79991234567", "code": "12345", "password": "not-stored"}
            ))
        self.assertEqual(sent["status"], "code_required")
        self.assertEqual(confirmed["status"], "ready")
        self.assertNotIn("+79991234567", router._telegram_auth_pending)
        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (await db.execute("SELECT value FROM module_settings")).fetchall()
        self.assertNotIn("not-stored", "\n".join(row[0] for row in rows))

    async def test_provisions_getcourse_staff_as_wazzup_users(self):
        captured = []

        async def fake_wazzup(method, path, body=None, **_kwargs):
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

        async def fake_wazzup(method, path, body=None, **_kwargs):
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

        async def fake_wazzup(method, path, body=None, **_kwargs):
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
        channels = [
            row for row in json.loads(response.body)["channels"]
            if row.get("provider") == "wazzup"
        ]
        self.assertEqual(channels, [{
            "channel_id": "max-1", "provider": "wazzup", "transport": "max",
            "channel_transport": "max", "name": "Служба заботы", "plain_id": "",
            "label": "MAX · Служба заботы",
            "available": False, "can_send": False, "has_chat": False,
            "send_reason": "Телефон не найден",
        }])

    async def test_widget_initial_channels_never_wait_for_live_provider_discovery(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        max_channel = {
            "channel_id": "max-1", "provider": "wazzup", "transport": "max",
            "channel_transport": "max", "name": "Служба заботы", "plain_id": "",
            "label": "MAX · Служба заботы",
        }
        telegram_channel = {
            "channel_id": "telegram-personal:5601500901", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "5601500901", "label": "Telegram Personal · operator",
        }

        slow_card_link = AsyncMock(side_effect=AssertionError("live provider lookup must not run"))

        started = time.monotonic()
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[max_channel, telegram_channel])),
            patch.object(router, "_has_conversation", new=AsyncMock(return_value=False)),
            patch.object(router, "_provider_card_link", new=slow_card_link),
            patch.object(router, "_assign_client_threads", new=AsyncMock()),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels", {"phone": "+79991234567"}, token=token,
            ))
            repeated = await router.widget_channels(request_for(
                "/widget/channels", {"phone": "+79991234567"}, token=token,
            ))
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.15)
        slow_card_link.assert_not_awaited()
        views = json.loads(response.body)["channels"]
        self.assertTrue(views[0]["can_send"])
        self.assertNotIn("pending", views[0])
        self.assertFalse(views[1]["can_send"])
        self.assertNotIn("pending", views[1])
        self.assertEqual(views[1]["send_reason"], "Пользователь Telegram не найден")
        self.assertFalse(json.loads(repeated.body)["channels"][1]["can_send"])

    async def test_amocrm_salebot_channel_uses_the_same_exact_card_identity_as_profile(self):
        code = await self._code()
        amo_origin = "https://junior.sobakovod.pro"
        activated = await router.widget_activate(request_for(
            "/widget/activate",
            {"code": code, "platform_user_id": "6269974"},
            origin=amo_origin,
            platform="amocrm",
        ))
        token = json.loads(activated.body)["device_token"]
        channel = {
            "channel_id": "salebot:project", "provider": router.SALEBOT_PROVIDER,
            "transport": "salebot", "channel_transport": "salebot",
            "name": "Проект", "plain_id": "project", "label": "SaleBot · Проект",
        }
        exact_link = AsyncMock(return_value={
            "external_user_id": "998417306", "name": "Амина Тесаева",
        })
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=exact_link),
            patch.object(router, "_conversation_rows", new=AsyncMock(return_value=("", False, []))),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels",
                {
                    "platform_user_id": "6269974", "entity_type": "lead",
                    "entity_id": "18101847", "phone": "+79297762777",
                },
                origin=amo_origin,
                token=token,
                platform="amocrm",
            ))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["channels"], [{
            **channel, "label": "SaleBot: Амина Тесаева",
            "available": True, "can_send": True, "has_chat": False, "send_reason": "",
        }])
        exact_link.assert_awaited_once()

    async def test_amocrm_salebot_raw_card_field_does_not_bypass_exact_verification(self):
        code = await self._code()
        amo_origin = "https://junior.sobakovod.pro"
        activated = await router.widget_activate(request_for(
            "/widget/activate",
            {"code": code, "platform_user_id": "6269974"},
            origin=amo_origin,
            platform="amocrm",
        ))
        token = json.loads(activated.body)["device_token"]
        channel = {
            "channel_id": "salebot:project", "provider": router.SALEBOT_PROVIDER,
            "transport": "salebot", "channel_transport": "salebot",
            "name": "Проект", "plain_id": "project", "label": "SaleBot · Проект",
        }
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=AsyncMock(return_value={})),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels",
                {
                    "platform_user_id": "6269974", "entity_type": "lead",
                    "entity_id": "17894711", "phone": "+79991234567",
                    "fields": {"salebot_id": "215204074", "utm_term": "215204074"},
                },
                origin=amo_origin,
                token=token,
                platform="amocrm",
            ))

        view = json.loads(response.body)["channels"][0]
        self.assertFalse(view["can_send"])
        self.assertEqual(view["send_reason"], "SaleBot клиента не найден")

    async def test_widget_keeps_verified_telegram_personal_when_wazzup_key_fails(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        channel = {
            "channel_id": "telegram-personal:5601500901", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "5601500901", "label": "Telegram Personal · operator",
        }
        await router._remember_external_link(
            {"getcourse_user_id": "42", "phone": "+79108758427", "name": "Елена"},
            "test", provider=router.TELEGRAM_PROVIDER, external_user_id="700",
        )
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,phone_hash,contact_name,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (channel["channel_id"], "telegram", "700", router._phone_hash("+79108758427"), "Елена", now, now),
            )
            await db.commit()
        with (
            patch.object(router, "_cached_active_channels", new=AsyncMock(side_effect=router.HTTPException(502, "bad key"))),
            patch.object(router, "_vk_channel", new=AsyncMock(return_value=None)),
            patch.object(router, "_telegram_channel", new=AsyncMock(return_value=channel)),
            patch.object(router, "_provider_card_link", new=AsyncMock(return_value={"external_user_id": "700"})),
            patch.object(router, "_sync_telegram_history", new=AsyncMock(return_value=(0, False))),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels",
                {"source_url": "https://club.sobakovod.pro/user/control/user/update/id/42", "phone": "+79108758427"},
                token=token,
            ))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["channels"], [{
            **channel, "label": "TG Personal: Елена",
            "available": True, "can_send": True, "has_chat": True, "send_reason": "",
        }])

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

        async def fake_wazzup(method, path, body=None, **_kwargs):
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
        self.assertRegex(captured[0][2]["webhooksUri"], r"^https://junior\.sobakovod\.pro/nexus/messenger-widget/api/webhook/inbound/[A-Za-z0-9_-]+$")

    async def test_webhook_status_returns_connection_error_without_http_failure(self):
        with (
            patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})),
            patch.object(router, "_wazzup_request", new=AsyncMock(side_effect=router.HTTPException(502, "Wazzup HTTP 401: INVALID_APIKEY"))),
        ):
            result = await router.webhook_status(request_for("/webhook/status", {}))
        self.assertFalse(result["ok"])
        self.assertFalse(result["configured"])
        self.assertEqual(result["error"], "Wazzup HTTP 401: INVALID_APIKEY")

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

        async def fake_wazzup(method, path, body=None, **_kwargs):
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

    async def test_max_absent_is_stored_as_not_delivered(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]

        async def fake_wazzup(method, path, body=None, **_kwargs):
            if (method, path) == ("GET", "/channels"):
                return [{"channelId": "max-1", "transport": "max", "state": "active", "name": "Служба заботы"}]
            raise router.HTTPException(502, "Wazzup HTTP 400: CHANNEL_MAX_PHONE_NOT_OCCUPIED")

        with patch.object(router, "_wazzup_request", new=fake_wazzup):
            response = await router.widget_send(request_for(
                "/widget/send",
                {
                    "phone": "+79108758427", "name": "Елена",
                    "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
                    "channel_id": "max-1", "transport": "max", "text": "Проверка MAX",
                },
                token=token,
            ))
        body = json.loads(response.body)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(body["sent"])
        self.assertEqual(body["message"]["status"], "failed")
        self.assertEqual(body["notice"], "У клиента не найден MAX. Сообщение не доставлено.")
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT status,text FROM wazzup_messages WHERE text='Проверка MAX'"
            )).fetchone()
        self.assertEqual(row, ("failed", "Проверка MAX"))

    async def test_temporary_channel_error_is_queued_with_idempotency(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]

        async def fake_wazzup(method, path, body=None, **_kwargs):
            if (method, path) == ("GET", "/channels"):
                return [{"channelId": "max-1", "transport": "max", "state": "active", "name": "MAX"}]
            raise router.HTTPException(429, "Wazzup HTTP 429: Too Many Requests")

        payload = {
            "request_id": "batch-1:max", "phone": "+79108758427", "name": "Елена",
            "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
            "channel_id": "max-1", "transport": "max", "text": "Позвоните мне",
        }
        with patch.object(router, "_wazzup_request", new=fake_wazzup):
            response = await router.widget_send(request_for("/widget/send", payload, token=token))
            duplicate = await router.widget_send(request_for("/widget/send", payload, token=token))
        self.assertEqual(response.status_code, 202)
        self.assertTrue(json.loads(response.body)["queued"])
        self.assertTrue(json.loads(duplicate.body)["queued"])
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT request_key,status,attempts FROM outbound_jobs WHERE request_key='batch-1:max'"
            )).fetchone()
        self.assertEqual(row, ("batch-1:max", "retry", 1))

    async def test_empty_max_chat_history_is_scoped_to_the_current_phone(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            for external_id, phone, text in (
                ("failed-nikita", "+79964158537", "Никита MAX"),
                ("failed-tatyana", "+79163134287", "Татьяна MAX"),
            ):
                await db.execute(
                    """INSERT INTO wazzup_messages(
                       external_id,channel_id,chat_type,chat_id,phone_hash,direction,status,text,sent_at,created_at
                       ) VALUES(?,'max-1','max','',?,'outgoing','failed',?,?,?)""",
                    (external_id, router._phone_hash(phone), text, now, now),
                )
            await db.commit()
        chat_id, has_chat, messages = await router._conversation_rows("max-1", "max", "+79163134287")
        self.assertEqual(chat_id, "")
        self.assertFalse(has_chat)
        self.assertEqual([row["text"] for row in messages], ["Татьяна MAX"])

    async def test_vk_conversation_hides_legacy_duplicate_and_reuses_global_message_id(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,contact_name,created_at,updated_at) VALUES('vk:225075265','vk','700','Клиент',?,?)",
                (now, now),
            )
            for external_id, conversation_id in (("vk:225075265:700:990", None), ("vk:225075265:700:77", 77)):
                raw = {"id": 990, "peer_id": 700, "text": "Один раз"}
                if conversation_id:
                    raw["conversation_message_id"] = conversation_id
                await db.execute(
                    """INSERT INTO wazzup_messages(
                       external_id,channel_id,chat_type,chat_id,direction,status,text,author_name,sent_at,raw_json,created_at
                       ) VALUES(?,'vk:225075265','vk','700','outgoing','delivered','Один раз','Сообщество',?,?,?)""",
                    (external_id, now, json.dumps(raw), now),
                )
            await db.commit()
        _, _, messages = await router._conversation_rows("vk:225075265", "vk", "", exact_chat_id="700")
        self.assertEqual([row["text"] for row in messages], ["Один раз"])

        row = {"id": 991, "conversation_message_id": 78, "from_id": -225075265, "date": 1_785_500_000, "text": "Новый ключ"}
        with (
            patch.object(router, "_vk_channel_id", return_value="vk:225075265"),
            patch.object(router, "_vk_group_id", return_value="225075265"),
        ):
            await router._store_vk_messages("700", [{key: value for key, value in row.items() if key != "conversation_message_id"}], {"name": "Клиент"})
            await router._store_vk_messages("700", [row], {"name": "Клиент"})
        async with aiosqlite.connect(router._must_db()) as db:
            count = (await (await db.execute(
                "SELECT count(*) FROM wazzup_messages WHERE external_id='vk:225075265:700:991'"
            )).fetchone())[0]
        self.assertEqual(count, 1)

    async def test_vk_manager_message_uses_out_flag_and_repairs_cached_direction(self):
        now = router._iso()
        external_id = "vk:225075265:700:992"
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,direction,status,text,author_name,sent_at,raw_json,created_at
                   ) VALUES(?,'vk:225075265','vk','700','incoming','delivered','Ответ менеджера','Клиент',?,?,?)""",
                (external_id, now, json.dumps({"id": 992, "from_id": 555}), now),
            )
            await db.commit()
        row = {
            "id": 992,
            "conversation_message_id": 79,
            "from_id": 555,
            "out": 1,
            "date": 1_785_500_100,
            "text": "Ответ менеджера",
        }
        with (
            patch.object(router, "_vk_channel_id", return_value="vk:225075265"),
            patch.object(router, "_vk_group_id", return_value="225075265"),
        ):
            await router._store_vk_messages("700", [row], {"name": "Клиент"})
        async with aiosqlite.connect(router._must_db()) as db:
            stored = await (await db.execute(
                "SELECT direction,author_name FROM wazzup_messages WHERE external_id=?", (external_id,)
            )).fetchone()
        self.assertEqual(stored, ("outgoing", "Сообщество"))

    async def test_vk_channel_can_start_with_an_exact_card_identity(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        channel = {
            "channel_id": "vk:225075265", "provider": "vk", "transport": "vk",
            "channel_transport": "vk", "name": "Кинолог", "plain_id": "225075265", "label": "VK · Кинолог",
        }
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=AsyncMock(return_value={"external_user_id": "215204074", "name": "Иван Иванов"})),
            patch.object(router, "_conversation_rows", new=AsyncMock(return_value=("", False, []))),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels", {"phone": "+79270916946", "fields": {"vk_id": "215204074"}}, token=token,
            ))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body)["channels"], [{
            **channel,
            "available": True, "can_send": True, "has_chat": False, "send_reason": "",
        }])

    async def test_direct_channel_names_come_from_the_exact_provider_chat(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.executemany(
                """INSERT INTO wazzup_chats(
                   channel_id,chat_type,chat_id,contact_name,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?)""",
                [
                    ("telegram-personal:5601500901", "telegram", "700", "Екатерина Петрова", now, now),
                    ("salebot:project", "salebot", "99001", "Наталья Абрамова", now, now),
                ],
            )
            await db.commit()
        self.assertEqual(
            await router._provider_profile_name(router.TELEGRAM_PROVIDER, "700", "Имя из сделки"),
            "Екатерина Петрова",
        )
        self.assertEqual(
            await router._provider_profile_name(router.SALEBOT_PROVIDER, "99001", "Имя из сделки"),
            "Наталья Абрамова",
        )

    async def test_telegram_personal_initial_channels_do_not_run_live_phone_lookup(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        channel = {
            "channel_id": "telegram-personal:5601500901", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "5601500901", "label": "Telegram Personal · operator",
        }
        resolver = AsyncMock(return_value={"pending": "1"})
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=resolver),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels", {"phone": "+79991234567", "entity_type": "lead", "entity_id": "17759125"}, token=token,
            ))
        view = json.loads(response.body)["channels"][0]
        self.assertFalse(view["can_send"])
        self.assertNotIn("pending", view)
        self.assertEqual(view["send_reason"], "Пользователь Telegram не найден")
        resolver.assert_not_awaited()

    async def test_telegram_personal_is_unavailable_when_phone_has_no_telegram_user(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        channel = {
            "channel_id": "telegram-personal:5601500901", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "5601500901", "label": "Telegram Personal · operator",
        }
        resolver = AsyncMock(return_value={})
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=resolver),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels", {"phone": "+79091440995", "entity_type": "lead", "entity_id": "18232123"}, token=token,
            ))
        view = json.loads(response.body)["channels"][0]
        self.assertFalse(view["can_send"])
        self.assertNotIn("pending", view)
        self.assertEqual(view["send_reason"], "Пользователь Telegram не найден")
        resolver.assert_not_awaited()

    async def test_vk_card_link_uses_the_resolved_vk_account(self):
        data = {
            "entity_type": "lead", "entity_id": "18057331", "name": "Лана Волкова",
            "phone": "+79270916946", "fields": {"utm_term": "215204074"},
        }
        with patch.object(
            router, "_resolve_widget_context",
            new=AsyncMock(return_value={"accounts": [{"service": "vk", "platform_id": "215204074"}]}),
        ):
            link = await router._provider_card_link(
                data, "test", {"admin_id": self.admin_id, "admin_name": "Анна"}, "vk",
            )
        self.assertEqual(link["external_user_id"], "215204074")

    async def test_telegram_personal_send_runs_only_after_explicit_call(self):
        code = await self._code()
        token = json.loads((await router.widget_activate(request_for("/widget/activate", {"code": code}))).body)["device_token"]
        channel = {
            "channel_id": "telegram-personal:5601500901", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "5601500901", "label": "Telegram Personal · operator",
        }
        await router._remember_external_link(
            {"getcourse_user_id": "42", "phone": "+79108758427", "name": "Елена"},
            "test", provider=router.TELEGRAM_PROVIDER, external_user_id="700",
        )
        sender = AsyncMock(return_value={"external_id": "tg:1", "direction": "outgoing", "text": "mock"})
        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=[channel])),
            patch.object(router, "_provider_card_link", new=AsyncMock(return_value={"external_user_id": "700"})),
            patch.object(router, "_telegram_send_text", new=sender),
        ):
            response = await router.widget_send(request_for(
                "/widget/send",
                {
                    "phone": "+79108758427", "name": "Елена",
                    "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
                    "channel_id": channel["channel_id"], "transport": "telegram",
                    "provider": router.TELEGRAM_PROVIDER, "text": "mock",
                },
                token=token,
            ))
        self.assertEqual(response.status_code, 200)
        sender.assert_awaited_once_with("700", "mock", author_name="Анна")

    async def test_amocrm_telegram_requires_live_phone_resolution_and_persists_entity_link(self):
        class User:
            id = 700
            phone = "79108758427"
            username = "client"
            first_name = "Елена"
            last_name = ""

        class Client:
            async def is_user_authorized(self):
                return True

            async def get_entity(self, reference):
                raise AssertionError("stale telegram_id must not be trusted for an amoCRM card")

            async def __call__(self, request):
                self.phone = request.phone
                return type("Resolved", (), {"users": [User()]})()

        client = Client()

        async def run(callback):
            return await callback(client)

        device = {"admin_id": self.admin_id, "admin_name": "Анна"}
        data = {
            "entity_type": "lead", "entity_id": "9001", "name": "Елена",
            "phone": "+79108758427", "fields": {"telegram_id": "700", "salebot_id": "sb-1"},
        }
        with (
            patch.object(router, "resolve_client_identity", new=AsyncMock(return_value={})),
            patch.object(router, "_telegram_run", new=run),
        ):
            link = await router._provider_card_link(data, "amocrm", device, router.TELEGRAM_PROVIDER)
        self.assertEqual(link["external_user_id"], "700")
        self.assertEqual(client.phone, "79108758427")
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT external_user_id FROM entity_identity_links WHERE platform='amocrm' AND entity_type='lead' AND entity_id='9001'"
            )).fetchone()
        self.assertEqual(row[0], "700")

    async def test_test_card_keeps_explicit_platform_id_without_persisting_link(self):
        class User:
            id = 701
            phone = "79108758427"
            username = "client"
            first_name = "Елена"
            last_name = ""

        class Client:
            async def is_user_authorized(self):
                return True

            async def get_entity(self, reference):
                return User()

        async def run(callback):
            return await callback(Client())

        data = {
            "entity_type": "lead", "entity_id": "9002", "name": "Елена",
            "phone": "+79108758427", "fields": {"platform_id": "701"},
        }
        with patch.object(router, "_telegram_run", new=run):
            link = await router._provider_card_link(data, "test", {"admin_id": self.admin_id}, router.TELEGRAM_PROVIDER)
        self.assertEqual(link["external_user_id"], "701")
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT 1 FROM entity_identity_links WHERE platform='getcourse' AND entity_type='lead' AND entity_id='9002'"
            )).fetchone()
        self.assertIsNone(row)

    async def test_test_card_reuses_exact_direct_link_for_the_next_request(self):
        class User:
            id = 702
            phone = ""
            username = "client"
            first_name = "Елена"
            last_name = ""

        class Client:
            async def is_user_authorized(self):
                return True

            async def get_entity(self, reference):
                return User()

        calls = 0

        async def run(callback):
            nonlocal calls
            calls += 1
            return await callback(Client())

        data = {"entity_type": "lead", "entity_id": "9003", "fields": {"platform_id": "702"}}
        device = {"id": "cache-test", "admin_id": self.admin_id}
        with patch.object(router, "_telegram_run", new=run):
            first = await router._provider_card_link(data, "test", device, router.TELEGRAM_PROVIDER)
            second = await router._provider_card_link(data, "test", device, router.TELEGRAM_PROVIDER)
        self.assertEqual((first["external_user_id"], second["external_user_id"], calls), ("702", "702", 1))

    async def test_telegram_history_cache_skips_remote_read(self):
        router._telegram_history_cache.clear()
        router._telegram_history_cache[("700", 0)] = (time.monotonic() + 60, True)
        with patch.object(router, "_telegram_run", new=AsyncMock(side_effect=AssertionError("remote read"))):
            result = await router._sync_telegram_history("700")
        self.assertEqual(result, (0, True))

    async def test_wazzup_history_retry_uses_persistent_cache_windows(self):
        self.assertEqual(router._history_retry_minutes("imported"), 720)
        self.assertEqual(router._history_retry_minutes("not_found"), 60)
        self.assertEqual(router._history_retry_minutes("no_access"), 60)
        self.assertEqual(router._history_retry_minutes("error"), 10)

    async def test_vk_sync_keeps_unmatched_community_dialogs(self):
        payload = {
            "items": [{
                "conversation": {"peer": {"id": 701, "type": "user"}},
                "last_message": {"id": 9, "from_id": 701, "date": 1_785_300_000, "text": "VK входящее"},
            }],
            "profiles": [{"id": 701, "first_name": "Елена", "last_name": "Иванова"}],
        }
        with (
            patch.object(router, "_vk_token", return_value="token"),
            patch.object(router, "_vk_group_id", return_value="225075265"),
            patch.object(router, "_refresh_vk_links", new=AsyncMock(return_value=0)),
            patch.object(router, "_vk_request", new=AsyncMock(return_value=payload)),
        ):
            result = await router._sync_vk_conversations()
        self.assertEqual(result["conversations"], 1)
        link = await router._external_link(peer_id="701", provider="vk")
        self.assertEqual(link["name"], "Елена Иванова")
        async with aiosqlite.connect(router._must_db()) as db:
            chat = await (await db.execute("SELECT contact_name FROM wazzup_chats WHERE chat_type='vk' AND chat_id='701'" )).fetchone()
        self.assertEqual(chat[0], "Елена Иванова")

    async def test_telegram_full_sync_keeps_unmatched_personal_dialogs(self):
        class User:
            id = 702
            phone = "79108758427"
            username = "client"
            first_name = "Елена"
            last_name = ""

        class Message:
            id = 10
            message = "Telegram входящее"
            date = None
            out = False
            file = None
            sender_id = 702

        class Dialog:
            entity = User()
            message = Message()

        class Client:
            async def is_user_authorized(self):
                return True

            async def iter_dialogs(self, limit):
                self.limit = limit
                yield Dialog()

        client = Client()

        async def run(callback):
            return await callback(client)

        channel = {
            "channel_id": "telegram-personal:1", "provider": router.TELEGRAM_PROVIDER,
            "transport": "telegram", "channel_transport": "personal", "name": "operator",
            "plain_id": "1", "label": "Telegram Personal · operator",
        }
        with (
            patch.object(router, "_telegram_channel", new=AsyncMock(return_value=channel)),
            patch.object(router, "_telegram_run", new=run),
        ):
            result = await router._sync_telegram_dialogs(full=True)
        self.assertEqual(result["dialogs"], 1)
        self.assertEqual(client.limit, router.TELEGRAM_DIALOG_LIMIT)
        link = await router._external_link(peer_id="702", provider=router.TELEGRAM_PROVIDER)
        self.assertEqual(link["phone"], "+79108758427")

    async def test_salebot_related_fields_supply_telegram_id(self):
        accounts = [{"service": "salebot", "platform_id": "sb-1", "fields": {"telegram_id": "703"}}]
        self.assertEqual(router._account_identity_value(accounts, router.TELEGRAM_PROVIDER), "703")

    async def test_admin_sync_pushes_local_employee_to_current_wazzup_project(self):
        captured = []

        async def fake_wazzup(method, path, body=None, **_kwargs):
            captured.append((method, path, body))
            if (method, path) == ("GET", "/users"):
                return [{"id": "wazzup-user-7", "name": "Анна"}]
            return []

        with (
            patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})),
            patch.object(router, "_wazzup_request", new=fake_wazzup),
            patch.object(router, "enforce_rate_limit"),
        ):
            result = await router.sync_admins(request_for("/admins/sync", {}))
        self.assertEqual(result["pushed"], 1)
        self.assertEqual(captured[0][0:2], ("POST", "/users"))
        self.assertEqual(captured[1], ("GET", "/users", None))

    async def test_admin_bindings_map_getcourse_and_amocrm_employees(self):
        payload = {"bindings": [
            {"platform": "getcourse", "platform_user_id": "42"},
            {"platform": "amocrm", "platform_user_id": "84"},
        ]}
        with patch.object(router, "_require_admin", new=AsyncMock(return_value={"username": "admin"})):
            await router.save_admin_bindings(self.admin_id, request_for("/admins/1/bindings", payload))
            result = await router.list_admins(request_for("/admins", {}))
        bindings = result["admins"][0]["bindings"]
        self.assertEqual({(row["platform"], row["platform_user_id"]) for row in bindings}, {
            ("getcourse", "42"), ("amocrm", "84"),
        })

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

    async def test_telegram_phone_uses_direct_resolve_before_dialog_scan(self):
        class User:
            id = 859949704
            phone = "79880952576"
            username = "fish_dory7"
            first_name = "Рыба"
            last_name = "Дори"

        class Result:
            users = [User()]

        class Client:
            def __init__(self):
                self.phone = ""
                self.scanned = False

            async def __call__(self, request):
                self.phone = request.phone
                return Result()

            async def iter_dialogs(self, **_kwargs):
                self.scanned = True
                if False:
                    yield None

        client = Client()
        entity = await router._telegram_entity(client, {"phone": "+7 (988) 095-25-76"})
        self.assertEqual(router._telegram_user_view(entity)["id"], "859949704")
        self.assertEqual(client.phone, "79880952576")
        self.assertFalse(client.scanned)

    async def test_vk_sync_replaces_placeholder_id_with_profile_name(self):
        await router._remember_external_link(
            {"name": "VK 135738842"}, "callback", provider="vk", external_user_id="135738842",
        )
        response = {
            "items": [{
                "conversation": {"peer": {"id": 135738842, "type": "user"}},
                "last_message": {"id": 17, "from_id": 135738842, "date": 1_722_800_000, "text": "Здравствуйте"},
            }],
            "profiles": [{"id": 135738842, "first_name": "Марина", "last_name": "Иванова"}],
        }
        with (
            patch.object(router, "_vk_token", return_value="token"),
            patch.object(router, "_vk_group_id", return_value="123"),
            patch.object(router, "_refresh_vk_links", new=AsyncMock(return_value=0)),
            patch.object(router, "_vk_request", new=AsyncMock(return_value=response)),
        ):
            await router._sync_vk_conversations()
        link = await router._external_link(peer_id="135738842", provider="vk")
        async with aiosqlite.connect(router._must_db()) as db:
            chat = await (await db.execute(
                "SELECT contact_name FROM wazzup_chats WHERE chat_type='vk' AND chat_id='135738842'"
            )).fetchone()
        self.assertEqual(link["name"], "Марина Иванова")
        self.assertEqual(chat[0], "Марина Иванова")

    async def test_vk_placeholder_names_are_refreshed_in_one_batch(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,contact_name,last_message_at,created_at,updated_at) VALUES('vk:123','vk','282292714','VK 282292714',?,?,?)",
                (now, now, now),
            )
            await db.commit()
        with patch.object(
            router,
            "_vk_request",
            new=AsyncMock(return_value=[{"id": 282292714, "first_name": "Анна", "last_name": "Смирнова"}]),
        ) as request:
            updated = await router._refresh_vk_placeholder_names()
        self.assertEqual(updated, 1)
        request.assert_awaited_once_with("users.get", {"user_ids": "282292714"})
        link = await router._external_link(peer_id="282292714", provider="vk")
        async with aiosqlite.connect(router._must_db()) as db:
            chat = await (await db.execute(
                "SELECT contact_name FROM wazzup_chats WHERE chat_type='vk' AND chat_id='282292714'"
            )).fetchone()
        self.assertEqual(link["name"], "Анна Смирнова")
        self.assertEqual(chat[0], "Анна Смирнова")


if __name__ == "__main__":
    unittest.main()
