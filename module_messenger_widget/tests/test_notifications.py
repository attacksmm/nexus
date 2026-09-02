import json
import logging
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiosqlite
from starlette.requests import Request

import router


def request_for(path: str, body: dict | None = None) -> Request:
    raw = json.dumps(body or {}).encode()
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request({
        "type": "http", "http_version": "1.1", "method": "POST", "scheme": "https",
        "path": path, "raw_path": path.encode(), "query_string": b"",
        "server": ("junior.sobakovod.pro", 443), "client": ("203.0.113.10", 50100),
        "headers": [(b"content-type", b"application/json"), (b"host", b"junior.sobakovod.pro")],
    }, receive)


def browser_request(path: str, token: str, body: dict | None = None) -> Request:
    request = request_for(path, body)
    request.scope["headers"].append((b"authorization", f"Bearer {token}".encode()))
    return request


class NotificationWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="messenger-notify-")
        router._db_path = Path(self.tmp.name) / "module.db"
        router._logger = logging.getLogger("messenger-notify-tests")
        await router._init_db()
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cursor = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at) VALUES(?,?,'admin',1,?,?)",
                ("manager-1", "Анна Менеджер", now, now),
            )
            self.admin_id = int(cursor.lastrowid)
            await db.execute(
                "UPDATE module_settings SET value='2000-01-01T00:00:00Z' WHERE key='notification_live_since'"
            )
            await db.commit()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_email_task_source_migration_is_once_only_and_preserves_choices(self):
        key = router._admin_amo_task_sources_setting_key(self.admin_id)
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute("DELETE FROM module_settings WHERE key='amo_task_email_default_enabled_v1'")
            await db.execute(
                "INSERT OR REPLACE INTO module_settings(key,value,updated_at) VALUES(?,?,?)",
                (key, '["max"]', now),
            )
            await db.commit()

        await router._init_db()
        async with aiosqlite.connect(router._must_db()) as db:
            migrated = await (await db.execute(
                "SELECT value FROM module_settings WHERE key=?", (key,),
            )).fetchone()
            marker = await (await db.execute(
                "SELECT value FROM module_settings WHERE key='amo_task_email_default_enabled_v1'"
            )).fetchone()
        self.assertEqual(json.loads(migrated[0]), ["max", "email"])
        self.assertEqual(marker[0], "1")

        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute("UPDATE module_settings SET value='[\"max\"]' WHERE key=?", (key,))
            await db.commit()
        await router._init_db()
        async with aiosqlite.connect(router._must_db()) as db:
            unchanged = await (await db.execute(
                "SELECT value FROM module_settings WHERE key=?", (key,),
            )).fetchone()
        self.assertEqual(json.loads(unchanged[0]), ["max"])

    async def test_pairing_is_single_use_and_binds_exact_manager(self):
        code = "ABCDEFGH23"
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO notification_pairings(admin_id,provider,code_hash,expires_at,created_at) VALUES(?,?,?,?,?)",
                (self.admin_id, "vk", router._hash(code), "2999-01-01T00:00:00Z", now),
            )
            await db.commit()

        result = await router._consume_notification_pairing("vk", "NEXUS-" + code, "551122", "Анна VK")
        self.assertEqual(result["admin_id"], self.admin_id)
        self.assertIsNone(await router._consume_notification_pairing("vk", "NEXUS-" + code, "551122", "Анна VK"))
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT provider,recipient_id,label,enabled FROM notification_destinations"
            )).fetchone()
        self.assertEqual(row, ("vk", "551122", "Анна VK", 1))

    async def test_delayed_telegram_start_uses_original_message_time(self):
        code = "telegram-delay-token-123456789"
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO notification_pairings(admin_id,provider,code_hash,expires_at,created_at) VALUES(?,?,?,?,?)",
                (self.admin_id, "telegram", router._hash(code), "2026-08-20T08:10:00Z", "2026-08-20T08:00:00Z"),
            )
            await db.commit()
        paired = await router._consume_notification_pairing(
            "telegram", "/start nx_" + code, "9911", "Анна TG", received_at="2026-08-20T08:05:00Z",
        )
        self.assertEqual(paired["admin_id"], self.admin_id)

    async def test_cleanup_keeps_recently_expired_pairing_for_delayed_bot_update(self):
        now = router._now_dt()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO notification_pairings(admin_id,provider,code_hash,expires_at,created_at) VALUES(?,?,?,?,?)",
                (self.admin_id, "telegram", router._hash("late"), router._iso(now - router.timedelta(minutes=5)), router._iso(now - router.timedelta(minutes=10))),
            )
            await db.commit()
        await router._cleanup()
        async with aiosqlite.connect(router._must_db()) as db:
            count = (await (await db.execute("SELECT COUNT(*) FROM notification_pairings")).fetchone())[0]
        self.assertEqual(count, 1)

    async def test_enqueue_deduplicates_and_keeps_responsible_manager(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,responsible_admin_id,created_at,updated_at)
                   VALUES('max-1','max','chat-7',?,?,?)""",
                (self.admin_id, now, now),
            )
            await db.commit()
        payload = {
            "external_id": "max-message-1", "channel_id": "max-1", "chat_type": "max",
            "chat_id": "chat-7", "client_name": "Клиент", "text": "Здравствуйте", "sent_at": now,
        }
        self.assertTrue(await router._enqueue_notification_message(**payload))
        self.assertFalse(await router._enqueue_notification_message(**payload))
        async with aiosqlite.connect(router._must_db()) as db:
            row = await (await db.execute(
                "SELECT source,target_admin_id,status FROM notification_events"
            )).fetchone()
        self.assertEqual(row, ("max", self.admin_id, "pending"))

    async def test_short_reply_to_automated_funnel_message_is_silent(self):
        now = router._iso()
        before = router._iso(router._now_dt() - timedelta(minutes=1))
        automated = "Приходите на занятие сегодня. " * 6
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,direction,status,text,author_name,sent_at,raw_json,created_at
                   ) VALUES('out-auto','vk:225','vk','client-auto','outgoing','delivered',?,'Сообщество',?,? ,?)""",
                (automated, before, json.dumps({"id": 700}), before),
            )
            await db.commit()
        inserted = await router._enqueue_notification_message(
            external_id="in-auto", channel_id="vk:225", chat_type="vk", chat_id="client-auto",
            provider="vk", client_name="Клиент", text="Да.", sent_at=now,
            raw_payload={"reply_message": {"id": 700}},
        )
        self.assertFalse(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            count = (await (await db.execute("SELECT COUNT(*) FROM notification_events")).fetchone())[0]
        self.assertEqual(count, 0)

    async def test_reply_to_manager_message_keeps_notification_and_task_flow(self):
        now = router._iso()
        before = router._iso(router._now_dt() - timedelta(minutes=1))
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,responsible_admin_id,created_at,updated_at)
                   VALUES('vk:225','vk','client-manager',?,?,?)""",
                (self.admin_id, before, now),
            )
            await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,direction,status,text,author_name,sent_at,raw_json,created_at
                   ) VALUES('out-manager','vk:225','vk','client-manager','outgoing','delivered',
                            'Вам удобно созвониться?','Анна Менеджер',?,? ,?)""",
                (before, json.dumps({"id": 701}), before),
            )
            await db.commit()
        inserted = await router._enqueue_notification_message(
            external_id="in-manager", channel_id="vk:225", chat_type="vk", chat_id="client-manager",
            provider="vk", client_name="Клиент", text="Да.", sent_at=now,
            raw_payload={"reply_message": {"id": 701}},
        )
        self.assertTrue(inserted)

    async def test_meaningful_sales_intent_is_never_suppressed_by_short_reply_heuristic(self):
        now = router._iso()
        before = router._iso(router._now_dt() - timedelta(minutes=1))
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_chats(channel_id,chat_type,chat_id,responsible_admin_id,created_at,updated_at)
                   VALUES('max-1','max','client-sale',?,?,?)""",
                (self.admin_id, before, now),
            )
            await db.execute(
                """INSERT INTO wazzup_messages(
                   external_id,channel_id,chat_type,chat_id,direction,status,text,author_name,sent_at,raw_json,created_at
                   ) VALUES('out-funnel','max-1','max','client-sale','outgoing','delivered',?,'',?,'{}',?)""",
                ("Запишитесь на занятие. " * 8, before, before),
            )
            await db.commit()
        inserted = await router._enqueue_notification_message(
            external_id="in-sale", channel_id="max-1", chat_type="max", chat_id="client-sale",
            provider="max", client_name="Клиент", text="Да, хочу купить курс", sent_at=now,
        )
        self.assertTrue(inserted)

    async def test_incoming_message_records_hidden_history_and_queues_amo_task(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES('max','chat-amo',?,'amocrm','lead','18244969','https://example.amocrm.ru/leads/detail/18244969',?)""",
                (self.admin_id, now),
            )
            await db.commit()
        original = router._amo_deal_delivery_details
        router._amo_deal_delivery_details = lambda _lead_id: {
            "responsible_user_id": "7461291", "client_name": "Ирина Скуратова",
            "entity_url": "https://example.amocrm.ru/leads/detail/18244969",
        }
        try:
            inserted = await router._enqueue_notification_message(
                external_id="max-amo-1", channel_id="max-1", chat_type="max", chat_id="chat-amo",
                provider="max", client_name="Ирина Скуратова", text="Перезвоните мне", sent_at=now,
            )
        finally:
            router._amo_deal_delivery_details = original
        self.assertTrue(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            communication = await (await db.execute(
                "SELECT direction,provider,amo_lead_id,text FROM communication_messages"
            )).fetchone()
            task = await (await db.execute(
                "SELECT amo_lead_id,responsible_user_id,messenger,message_text,status FROM amo_task_jobs"
            )).fetchone()
        self.assertEqual(communication, ("incoming", "max", "18244969", "Перезвоните мне"))
        self.assertEqual(task, ("18244969", "7461291", "MAX", "Перезвоните мне", "pending"))

    async def test_employee_can_disable_amo_task_without_disabling_notification_or_history(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)",
                (router._admin_amo_task_setting_key(self.admin_id), "0", now),
            )
            await db.execute(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES('max','chat-no-amo-task',?,'amocrm','lead','18244970',
                            'https://example.amocrm.ru/leads/detail/18244970',?)""",
                (self.admin_id, now),
            )
            await db.commit()
        with patch.object(router, "_amo_deal_delivery_details", return_value={
            "responsible_user_id": "7461291", "client_name": "Ирина Скуратова",
            "entity_url": "https://example.amocrm.ru/leads/detail/18244970",
        }):
            inserted = await router._enqueue_notification_message(
                external_id="max-no-amo-task-1", channel_id="max-1", chat_type="max",
                chat_id="chat-no-amo-task", provider="max", client_name="Ирина Скуратова",
                text="Нужна консультация", sent_at=now,
            )
        self.assertTrue(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            notification_count = (await (await db.execute(
                "SELECT COUNT(*) FROM notification_events"
            )).fetchone())[0]
            communication_count = (await (await db.execute(
                "SELECT COUNT(*) FROM communication_messages"
            )).fetchone())[0]
            task_count = (await (await db.execute(
                "SELECT COUNT(*) FROM amo_task_jobs"
            )).fetchone())[0]
        self.assertEqual((notification_count, communication_count, task_count), (1, 1, 0))

    async def test_employee_can_create_amo_tasks_only_for_selected_sources(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)",
                (router._admin_amo_task_sources_setting_key(self.admin_id), '["max"]', now),
            )
            await db.executemany(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES(?,?,?,'amocrm','lead',?,?,?)""",
                [
                    ("max", "selected-max", self.admin_id, "18244971", "https://example.amocrm.ru/leads/detail/18244971", now),
                    ("vk", "filtered-vk", self.admin_id, "18244972", "https://example.amocrm.ru/leads/detail/18244972", now),
                ],
            )
            await db.commit()
        with patch.object(router, "_amo_deal_delivery_details", return_value={
            "responsible_user_id": "7461291", "client_name": "Клиент",
        }):
            self.assertTrue(await router._enqueue_notification_message(
                external_id="selected-max-1", channel_id="max-1", chat_type="max",
                chat_id="selected-max", provider="max", client_name="Клиент",
                text="MAX сообщение", sent_at=now,
            ))
            self.assertTrue(await router._enqueue_notification_message(
                external_id="filtered-vk-1", channel_id="vk:225", chat_type="vk",
                chat_id="filtered-vk", provider="vk", client_name="Клиент",
                text="VK сообщение", sent_at=now,
            ))
        async with aiosqlite.connect(router._must_db()) as db:
            tasks = await (await db.execute(
                "SELECT amo_lead_id,messenger FROM amo_task_jobs ORDER BY id"
            )).fetchall()
            communications = (await (await db.execute(
                "SELECT COUNT(*) FROM communication_messages"
            )).fetchone())[0]
        self.assertEqual(tasks, [("18244971", "MAX")])
        self.assertEqual(communications, 2)

    async def test_conflicting_exact_identity_cannot_route_or_queue_amo_task(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO entity_identity_links(
                   platform,entity_type,entity_id,provider,external_user_id,confirmed_by,created_at,updated_at
                   ) VALUES('amocrm','lead','18262251','vk','583266787',?,?,?)""",
                (self.admin_id, now, now),
            )
            await db.execute(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES('vk','329938523',?,'amocrm','lead','18262251',
                            'https://example.amocrm.ru/leads/detail/18262251',?)""",
                (self.admin_id, now),
            )
            await db.commit()
        with patch.object(router, "_identity_amo_notification_context", new=AsyncMock(return_value={
            "platform": "amocrm", "entity_type": "lead", "entity_id": "18262251",
            "entity_url": "https://example.amocrm.ru/leads/detail/18262251",
        })):
            inserted = await router._enqueue_notification_message(
                external_id="vk-marina-1", channel_id="vk:225", chat_type="vk",
                chat_id="329938523", provider="vk", client_name="Марина Боровкова",
                text="Здравствуйте", sent_at=now,
            )
        self.assertFalse(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            counts = [
                (await (await db.execute(f"SELECT COUNT(*) FROM {table}")).fetchone())[0]
                for table in ("notification_events", "communication_messages", "amo_task_jobs")
            ]
        self.assertEqual(counts, [0, 0, 0])

    async def test_exact_link_update_removes_orphan_and_rejects_late_stale_context(self):
        now = router._iso()
        context = {
            "platform": "amocrm", "entity_type": "lead", "entity_id": "18262252",
            "entity_url": "https://example.amocrm.ru/leads/detail/18262252",
        }
        await router._remember_entity_external_link(context, "vk", "old-vk", self.admin_id)
        await router._remember_notification_context(context, "vk", "old-vk", self.admin_id)
        await router._remember_entity_external_link(context, "vk", "new-vk", self.admin_id)
        await router._remember_notification_context(context, "vk", "old-vk", self.admin_id)
        async with aiosqlite.connect(router._must_db()) as db:
            exact = await (await db.execute(
                """SELECT external_user_id FROM entity_identity_links
                   WHERE platform='amocrm' AND entity_type='lead' AND entity_id='18262252' AND provider='vk'"""
            )).fetchone()
            contexts = await (await db.execute(
                "SELECT external_user_id FROM conversation_contexts WHERE entity_id='18262252'"
            )).fetchall()
        self.assertEqual(exact[0], "new-vk")
        self.assertEqual(contexts, [])

    async def test_pending_amo_task_rechecks_employee_sources_before_delivery(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO manager_bindings(
                   platform,platform_user_id,platform_user_email,admin_id,created_at,updated_at
                   ) VALUES('amocrm','7461291','',?,?,?)""",
                (self.admin_id, now, now),
            )
            await db.commit()
        context = {
            "platform": "amocrm", "entity_type": "lead", "entity_id": "18262253",
            "amo_lead_id": "18262253", "external_user_id": "vk-client",
        }
        await router._remember_entity_external_link(context, "vk", "vk-client", self.admin_id)
        with patch.object(router, "_amo_deal_delivery_details", return_value={"responsible_user_id": "7461291"}):
            queued = await router._enqueue_amo_task_for_message(
                message_key="vk-pending-setting-change", communication_id=0, context=context,
                source="vk", messenger="VK", client_name="Клиент", message_text="Сообщение",
                admin_id=self.admin_id,
            )
        self.assertTrue(queued)
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO module_settings(key,value,updated_at) VALUES(?,?,?)",
                (router._admin_amo_task_sources_setting_key(self.admin_id), '["max"]', now),
            )
            db.row_factory = aiosqlite.Row
            job = dict(await (await db.execute("SELECT * FROM amo_task_jobs")).fetchone())
            await db.commit()
        sender = AsyncMock(return_value=("task-id", []))
        with patch.object(router, "_send_amo_task", new=sender):
            await router._process_amo_task_job(job)
        sender.assert_not_awaited()
        async with aiosqlite.connect(router._must_db()) as db:
            status = await (await db.execute(
                "SELECT status,error FROM amo_task_jobs WHERE id=?", (job["id"],),
            )).fetchone()
        self.assertEqual(status[0], "cancelled")
        self.assertIn("Канал отключён", status[1])

    async def test_five_minute_debounce_groups_messages_into_one_amo_task(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES('vk','551199',?,'amocrm','lead','18250000','https://example.amocrm.ru/leads/detail/18250000',?)""",
                (self.admin_id, now),
            )
            await db.commit()
        original = router._amo_deal_delivery_details
        router._amo_deal_delivery_details = lambda _lead_id: {
            "responsible_user_id": "7461291", "client_name": "Клиент",
            "entity_url": "https://example.amocrm.ru/leads/detail/18250000",
        }
        try:
            for message_id, text in (("vk-batch-1", "Первое"), ("vk-batch-2", "Второе")):
                self.assertTrue(await router._enqueue_notification_message(
                    external_id=message_id, channel_id="vk:225", chat_type="vk", chat_id="551199",
                    provider="vk", client_name="Клиент", text=text, sent_at=now,
                ))
        finally:
            router._amo_deal_delivery_details = original
        async with aiosqlite.connect(router._must_db()) as db:
            events = await (await db.execute(
                "SELECT available_at FROM notification_events ORDER BY external_id"
            )).fetchall()
            tasks = await (await db.execute(
                "SELECT messenger,message_text,status FROM amo_task_jobs"
            )).fetchall()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0][0], events[1][0])
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0][0], "VK")
        self.assertIn("Первое", tasks[0][1])
        self.assertIn("Второе", tasks[0][1])
        self.assertEqual(tasks[0][2], "pending")

    async def test_amo_task_delivery_merges_open_tasks_and_resets_deadline(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.executemany(
                """INSERT INTO amo_task_jobs(
                   message_key,amo_lead_id,responsible_user_id,messenger,client_name,message_text,
                   status,attempts,next_attempt_at,amo_task_id,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,'sent',1,'',?,?,?)""",
                [
                    ("old-1", "18250000", "7461291", "VK", "Клиент", "Первое", "101", now, now),
                    ("old-2", "18250000", "7461291", "VK", "Клиент", "Второе", "102", now, now),
                ],
            )
            await db.commit()
        current = {
            "id": 3, "amo_lead_id": "18250000", "responsible_user_id": "7461291",
            "messenger": "VK", "client_name": "Клиент", "message_text": "Третье",
        }
        calls = []
        original_env = router._read_env_values
        original_request = router._amo_task_api_request

        async def fake_request(_client, method, _url, _token, *, payload=None, params=None):
            calls.append((method, payload, params))
            if method == "GET":
                return {"_embedded": {"tasks": [
                    {"id": 101, "entity_id": 18250000, "entity_type": "leads", "responsible_user_id": 7461291,
                     "created_at": 10, "is_completed": False, "text": "Новое сообщение · VK\nКлиент: Первое"},
                    {"id": 102, "entity_id": 18250000, "entity_type": "leads", "responsible_user_id": 7461291,
                     "created_at": 20, "is_completed": False, "text": "Новое сообщение · VK\nКлиент: Второе"},
                ]}}
            return {"_embedded": {"tasks": [{"id": 102}]}}

        router._read_env_values = lambda: {"AMO_BASE_URL": "https://example.amocrm.ru", "AMO_ACCESS_TOKEN": "token"}
        router._amo_task_api_request = fake_request
        before = int(router.time.time())
        try:
            task_id, replaced = await router._send_amo_task(current)
        finally:
            router._read_env_values = original_env
            router._amo_task_api_request = original_request
        self.assertEqual(task_id, "102")
        self.assertEqual(replaced, ["101", "102"])
        self.assertEqual([call[0] for call in calls], ["GET", "PATCH"])
        patch_payload = calls[1][1]
        self.assertEqual(patch_payload[0]["id"], 102)
        self.assertFalse(patch_payload[0]["is_completed"])
        self.assertGreaterEqual(patch_payload[0]["complete_till"], before + 24 * 60 * 60)
        text = patch_payload[0]["text"]
        self.assertLess(text.index("Первое"), text.index("Второе"))
        self.assertLess(text.index("Второе"), text.index("Третье"))
        self.assertEqual(
            patch_payload[1],
            {"id": 101, "is_completed": True, "result": {"text": "Объединено Nexus в задачу #102"}},
        )

    async def test_amo_task_retry_does_not_append_same_batch_twice(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO amo_task_jobs(
                   message_key,amo_lead_id,responsible_user_id,messenger,client_name,message_text,
                   status,attempts,next_attempt_at,amo_task_id,created_at,updated_at
                   ) VALUES('old','18250001','7461291','VK','Клиент','Первое','sent',1,'','201',?,?)""",
                (now, now),
            )
            await db.commit()
        current = {
            "id": 2, "amo_lead_id": "18250001", "responsible_user_id": "7461291",
            "messenger": "VK", "client_name": "Клиент", "message_text": "Одинаковый пакет",
        }
        expected = router._compose_amo_task_text(
            ["VK", "VK"], ["Клиент: Первое", "Клиент: Одинаковый пакет"],
        )
        patch_payloads = []
        original_env = router._read_env_values
        original_request = router._amo_task_api_request

        async def fake_request(_client, method, _url, _token, *, payload=None, params=None):
            if method == "GET":
                return {"_embedded": {"tasks": [{
                    "id": 201, "entity_id": 18250001, "entity_type": "leads", "responsible_user_id": 7461291,
                    "created_at": 10, "is_completed": False, "text": expected,
                }]}}
            patch_payloads.append(payload)
            return {"_embedded": {"tasks": [{"id": 201}]}}

        router._read_env_values = lambda: {"AMO_BASE_URL": "https://example.amocrm.ru", "AMO_ACCESS_TOKEN": "token"}
        router._amo_task_api_request = fake_request
        try:
            await router._send_amo_task(current)
        finally:
            router._read_env_values = original_env
            router._amo_task_api_request = original_request
        merged = patch_payloads[0][0]["text"]
        self.assertEqual(merged, expected)
        self.assertEqual(merged.count("Одинаковый пакет"), 1)

    async def test_successful_merge_relinks_previous_local_jobs_to_canonical_task(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.executemany(
                """INSERT INTO amo_task_jobs(
                   message_key,amo_lead_id,responsible_user_id,messenger,client_name,message_text,
                   status,attempts,next_attempt_at,amo_task_id,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,'sent',1,'',?,?,?)""",
                [
                    ("old-a", "18250002", "7461291", "VK", "Клиент", "Первое", "301", now, now),
                    ("old-b", "18250002", "7461291", "VK", "Клиент", "Второе", "302", now, now),
                ],
            )
            cursor = await db.execute(
                """INSERT INTO amo_task_jobs(
                   message_key,amo_lead_id,responsible_user_id,messenger,client_name,message_text,
                   status,attempts,next_attempt_at,created_at,updated_at
                   ) VALUES('current','18250002','7461291','VK','Клиент','Третье','processing',1,'',?,?)""",
                (now, now),
            )
            current_id = int(cursor.lastrowid)
            await db.commit()
        original_send = router._send_amo_task

        async def fake_send(_job):
            return "302", ["301", "302"]

        router._send_amo_task = fake_send
        try:
            await router._process_amo_task_job({"id": current_id, "attempts": 1})
        finally:
            router._send_amo_task = original_send
        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (await db.execute(
                "SELECT amo_task_id,status FROM amo_task_jobs ORDER BY id"
            )).fetchall()
        self.assertEqual(rows, [("302", "sent"), ("302", "sent"), ("302", "sent")])

    async def test_pairing_command_never_creates_client_notification_or_task(self):
        inserted = await router._enqueue_notification_message(
            external_id="vk-pairing-command", channel_id="vk:225", chat_type="vk",
            chat_id="1105209997", provider="vk", client_name="Тест",
            text="NEXUS-WZBTRYHYAR", sent_at=router._iso(),
        )
        self.assertFalse(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            event_count = (await (await db.execute("SELECT COUNT(*) FROM notification_events")).fetchone())[0]
            task_count = (await (await db.execute("SELECT COUNT(*) FROM amo_task_jobs")).fetchone())[0]
        self.assertEqual((event_count, task_count), (0, 0))

    async def test_batch_is_sent_once_to_each_enabled_destination(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.executemany(
                """INSERT INTO notification_destinations(
                   admin_id,provider,recipient_id,label,connected_at,verified_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?)""",
                [
                    (self.admin_id, "telegram", "1001", "Анна TG", now, now, now),
                    (self.admin_id, "vk", "2002", "Анна VK", now, now, now),
                ],
            )
            await db.executemany(
                """INSERT INTO notification_events(
                   external_id,source,thread_key,channel_id,chat_type,chat_id,target_admin_id,
                   client_name,text,sent_at,available_at,created_at,updated_at
                   ) VALUES(?,'max','max:max-1:chat-7','max-1','max','chat-7',?,'Клиент',?,?,?,?,?)""",
                [
                    ("event-1", self.admin_id, "Первое", now, "2000-01-01T00:00:00Z", now, now),
                    ("event-2", self.admin_id, "Второе", now, "2000-01-01T00:00:00Z", now, now),
                ],
            )
            await db.commit()
        calls = []
        original = router._send_notification_destination

        async def fake_send(provider, recipient_id, text_value, links):
            calls.append((provider, recipient_id, text_value, links))
            return provider + "-message"

        router._send_notification_destination = fake_send
        try:
            rows = await router._next_notification_group()
            await router._deliver_notification_group(rows)
        finally:
            router._send_notification_destination = original
        self.assertEqual({(row[0], row[1]) for row in calls}, {("telegram", "1001"), ("vk", "2002")})
        self.assertTrue(all("Первое" in row[2] and "Второе" in row[2] for row in calls))
        async with aiosqlite.connect(router._must_db()) as db:
            statuses = [row[0] for row in await (await db.execute(
                "SELECT status FROM notification_events ORDER BY external_id"
            )).fetchall()]
            deliveries = int((await (await db.execute("SELECT COUNT(*) FROM notification_deliveries")).fetchone())[0])
        self.assertEqual(statuses, ["delivered", "delivered"])
        self.assertEqual(deliveries, 4)

    async def test_configured_manager_route_can_notify_multiple_other_employees(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            recipients = []
            for key, name in (("manager-2", "Борис"), ("manager-3", "Вера")):
                cursor = await db.execute(
                    "INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at) VALUES(?,?,'employee',1,?,?)",
                    (key, name, now, now),
                )
                recipients.append(int(cursor.lastrowid))
            await db.execute(
                "INSERT INTO notification_route_policies(source_admin_id,configured,updated_at) VALUES(?,1,?)",
                (self.admin_id, now),
            )
            await db.executemany(
                "INSERT INTO notification_routes(source_admin_id,recipient_admin_id,created_at,updated_at) VALUES(?,?,?,?)",
                ((self.admin_id, recipient, now, now) for recipient in recipients),
            )
            await db.commit()
        self.assertEqual(set(await router._notification_targets(self.admin_id)), set(recipients))

    async def test_course_chat_preference_matches_cyrillic_curator_to_employee_name(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cursor = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at) VALUES(?,?,'employee',1,?,?)",
                ("slava", "Slava Reineke", now, now),
            )
            slava_id = int(cursor.lastrowid)
            await db.execute(
                "INSERT INTO notification_preferences(admin_id,fallback_unassigned,course_chats,updated_at) VALUES(?,0,1,?)",
                (slava_id, now),
            )
            await db.commit()
        self.assertEqual(
            await router._course_chat_target({"curator_name": "Слава"}), slava_id,
        )
        self.assertEqual(await router._notification_targets(slava_id, direct=True), [slava_id])

    async def test_course_chat_requires_curator_mention_or_reply(self):
        context = {
            "curator_vk_id": "45930434",
            "curator_vk_ref": "aflameryan",
            "curator_telegram": "slava_curator",
        }
        self.assertTrue(router._course_chat_addressed(
            context, "[id45930434|@aflameryan], подскажите, пожалуйста",
        ))
        self.assertTrue(router._course_chat_addressed(
            context, "Спасибо", reply_sender_id="45930434",
        ))
        self.assertTrue(router._course_chat_addressed(
            context, "Подскажите", reply_sender_ref="@slava_curator",
        ))
        self.assertFalse(router._course_chat_addressed(
            context, "Как приучить собаку к туалету?",
        ))

    async def test_course_chat_uses_configured_multiple_recipients(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cursor = await db.execute(
                "INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at) VALUES(?,?,'employee',1,?,?)",
                ("nikita-copy", "Никита", now, now),
            )
            nikita_id = int(cursor.lastrowid)
            await db.execute(
                "INSERT INTO notification_route_policies(source_admin_id,configured,updated_at) VALUES(?,1,?)",
                (self.admin_id, now),
            )
            await db.executemany(
                "INSERT INTO notification_routes(source_admin_id,recipient_admin_id,created_at,updated_at) VALUES(?,?,?,?)",
                [
                    (self.admin_id, self.admin_id, now, now),
                    (self.admin_id, nikita_id, now, now),
                ],
            )
            await db.commit()
        self.assertEqual(
            set(await router._notification_targets(self.admin_id, direct=True)),
            {self.admin_id, nikita_id},
        )

    async def test_notification_names_responsible_manager(self):
        row = {
            "source": "vk", "chat_id": "515207214", "target_admin_id": self.admin_id,
            "client_name": "Светлана Демина", "text": "Завтра удобно", "content_type": "text",
        }
        original_context = router._notification_context

        async def amo_context(_source, _chat_id):
            return {"platform": "amocrm", "entity_url": "https://example.amocrm.ru/leads/detail/1"}

        router._notification_context = amo_context
        try:
            text_value, links = await router._notification_text([row])
        finally:
            router._notification_context = original_context
        self.assertIn("Ответственный: Анна Менеджер", text_value)
        self.assertIn(("Открыть сделку amoCRM", "https://example.amocrm.ru/leads/detail/1"), links)

    async def test_course_notification_uses_real_chat_title_and_link(self):
        row = {
            "source": "vk", "chat_id": "2000000024", "target_admin_id": self.admin_id,
            "client_name": "Участник · Старое название", "text": "@aflameryan подскажите",
            "content_type": "text",
        }
        original_context = router._notification_context
        original_course_context = router._course_chat_context

        async def stored_context(_source, _chat_id):
            return {"platform": "course_chat", "entity_id": "Старое название"}

        async def live_context(_source, _chat_id, *args, **kwargs):
            return {
                "title": "55. 03.08.2026 - Современный Собаковод - закрытый чат",
                "chat_url": "https://vk.com/gim225075265?sel=c24",
                "curator_name": "Слава",
            }

        router._notification_context = stored_context
        router._course_chat_context = live_context
        try:
            text_value, links = await router._notification_text([row])
        finally:
            router._notification_context = original_context
            router._course_chat_context = original_course_context
        self.assertIn("Учебный чат: 55. 03.08.2026 - Современный Собаковод - закрытый чат", text_value)
        self.assertIn("Куратор: Слава", text_value)
        self.assertNotIn("Профиль VK", text_value)
        self.assertEqual(links, [("Открыть учебный чат", "https://vk.com/gim225075265?sel=c24")])

    async def test_unknown_profile_is_not_enqueued_for_fallback_admin(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                "INSERT INTO notification_preferences(admin_id,fallback_unassigned,course_chats,updated_at) VALUES(?,1,0,?)",
                (self.admin_id, now),
            )
            await db.commit()
        inserted = await router._enqueue_notification_message(
            external_id="vk-unknown", channel_id="vk:225", chat_type="vk", chat_id="251541432",
            provider="vk", client_name="Неизвестный профиль", text="Здравствуйте", sent_at=now,
        )
        self.assertFalse(inserted)
        async with aiosqlite.connect(router._must_db()) as db:
            count = (await (await db.execute("SELECT COUNT(*) FROM notification_events")).fetchone())[0]
        self.assertEqual(count, 0)

    async def test_routing_api_persists_multiple_recipients(self):
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            recipients = []
            for key, name in (("route-2", "Галина"), ("route-3", "Дарья")):
                cursor = await db.execute(
                    "INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at) VALUES(?,?,'employee',1,?,?)",
                    (key, name, now, now),
                )
                recipients.append(int(cursor.lastrowid))
            await db.commit()
        original = router._require_admin

        async def allow(_request):
            return {"username": "admin", "role": "admin"}

        router._require_admin = allow
        try:
            result = await router.save_notification_routing(
                self.admin_id,
                request_for("/notification-routing/1", {"recipient_admin_ids": recipients}),
            )
        finally:
            router._require_admin = original
        self.assertTrue(result["configured"])
        self.assertEqual(result["recipient_admin_ids"], recipients)
        self.assertEqual(set(await router._notification_targets(self.admin_id)), set(recipients))

    async def test_personal_templates_ignore_folders_and_keep_per_user_order(self):
        first = await router._save_template(
            {"folder": "Не должна сохраниться", "title": "Первый", "body": "Текст 1"}, self.admin_id,
        )
        second = await router._save_template(
            {"folder": "Тоже нет", "title": "Второй", "body": "Текст 2"}, self.admin_id,
        )
        self.assertEqual(first["folder"], "")
        self.assertEqual(second["folder"], "")
        ordered = await router._set_template_order(self.admin_id, [second["id"], first["id"]])
        self.assertEqual(ordered, [second["id"], first["id"]])
        async with aiosqlite.connect(router._must_db()) as db:
            rows = await (await db.execute(
                "SELECT template_id FROM template_user_order WHERE admin_id=? ORDER BY sort_order",
                (self.admin_id,),
            )).fetchall()
        self.assertEqual([row[0] for row in rows], ordered)

    async def test_salebot_minimal_callback_is_idempotent_with_message_id(self):
        secret = await router._setting("notification_salebot_secret")
        body = {"client_id": "99001", "text": "Можно подробнее?", "message_id": "sb-77"}
        first = await router.notification_salebot_callback(secret, request_for("/notifications/salebot/x", body))
        second = await router.notification_salebot_callback(secret, request_for("/notifications/salebot/x", body))
        self.assertTrue(json.loads(first.body)["inserted"])
        self.assertFalse(json.loads(second.body)["inserted"])
        async with aiosqlite.connect(router._must_db()) as db:
            message = await (await db.execute(
                "SELECT direction,text FROM wazzup_messages WHERE external_id='salebot-hook:99001:sb-77'"
            )).fetchone()
            event_count = int((await (await db.execute("SELECT COUNT(*) FROM notification_events")).fetchone())[0])
        self.assertEqual(message, ("incoming", "Можно подробнее?"))
        # The message is kept in history, but an unknown SaleBot id without
        # an amoCRM deal must not leak into the admin's fallback notifications.
        self.assertEqual(event_count, 0)

    async def test_salebot_minimal_callback_queues_notification_without_batch_delay(self):
        secret = await router._setting("notification_salebot_secret")
        captured = {}

        async def enqueue(**kwargs):
            captured.update(kwargs)
            return True

        body = {"client_id": "99002", "text": "Когда начало?", "message_id": "sb-immediate"}
        with patch.object(router, "_enqueue_notification_message", new=enqueue):
            response = await router.notification_salebot_callback(
                secret, request_for("/notifications/salebot/x", body),
            )
        self.assertTrue(json.loads(response.body)["inserted"])
        self.assertEqual(captured["chat_id"], "99002")
        self.assertEqual(captured["text"], "Когда начало?")
        self.assertEqual(captured["delay_seconds"], 0)

    async def test_salebot_button_and_service_events_are_acknowledged_but_ignored(self):
        secret = await router._setting("notification_salebot_secret")
        for body in (
            {"client_id": "99001", "text": "Кнопка", "message_id": "sb-button", "payload": {"button": "yes"}},
            {"client_id": "99001", "text": "callback_amoCRM", "message_id": "sb-service", "message_from_outside": 3},
        ):
            response = await router.notification_salebot_callback(
                secret, request_for("/notifications/salebot/x", body),
            )
            self.assertTrue(json.loads(response.body)["ok"])
            self.assertFalse(json.loads(response.body)["inserted"])
        async with aiosqlite.connect(router._must_db()) as db:
            self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM notification_events")).fetchone())[0], 0)

    async def test_registration_refuses_to_overwrite_foreign_webhook(self):
        calls = []
        original_call = router._notification_tg_call
        original_admin = router._require_admin

        async def fake_call(method, payload=None):
            calls.append(method)
            if method == "getMe":
                return {"username": router.NOTIFY_TELEGRAM_USERNAME}
            if method == "getWebhookInfo":
                return {"url": "https://example.test/existing"}
            raise AssertionError("setWebhook must not be called")

        async def fake_admin(_request):
            return {"username": "admin"}

        router._notification_tg_call = fake_call
        router._require_admin = fake_admin
        try:
            with self.assertRaises(router.HTTPException) as error:
                await router.notification_telegram_register(request_for("/notification-system/telegram/register"))
        finally:
            router._notification_tg_call = original_call
            router._require_admin = original_admin
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(calls, ["getMe", "getWebhookInfo"])

    async def test_browser_feed_targets_manager_and_opens_amo_deal(self):
        raw = "browser-token-" + "x" * 40
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cursor = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,'2999-01-01T00:00:00Z')""",
                (self.admin_id, router._hash("device-token-" + "y" * 40), "hint", now, now),
            )
            device_id = int(cursor.lastrowid)
            cursor = await db.execute(
                """INSERT INTO browser_notification_subscriptions(
                   admin_id,device_id,token_hash,token_hint,label,enabled,enabled_at,last_seen_at,created_at,updated_at
                   ) VALUES(?,?,?,?,?,1,?,?,?,?)""",
                (self.admin_id, device_id, router._hash(raw), "hint", "Chrome", now, now, now, now),
            )
            await db.execute(
                """INSERT INTO conversation_contexts(
                   provider,external_user_id,admin_id,platform,entity_type,entity_id,entity_url,updated_at
                   ) VALUES('max','chat-browser',?,'amocrm','lead','1822','https://example.amocrm.ru/leads/detail/1822',?)""",
                (self.admin_id, now),
            )
            await db.execute(
                """INSERT INTO notification_events(
                   external_id,source,thread_key,channel_id,chat_type,chat_id,target_admin_id,
                   client_name,text,sent_at,available_at,created_at,updated_at
                   ) VALUES('browser-event','max','max:c:chat-browser','c','max','chat-browser',?,
                   'Ирина','Перезвоните мне',?,?,?,?)""",
                (self.admin_id, now, now, now, now),
            )
            await db.commit()
        response = await router.browser_notification_feed(browser_request("/browser-notifications/feed", raw))
        data = json.loads(response.body)
        self.assertEqual(data["notifications"][0]["title"], "MAX · Ирина")
        self.assertEqual(data["notifications"][0]["url"], "https://example.amocrm.ru/leads/detail/1822")
        ack = await router.browser_notification_ack(browser_request(
            "/browser-notifications/ack", raw, {"event_ids": ["browser-event"]},
        ))
        self.assertTrue(json.loads(ack.body)["ok"])
        async with aiosqlite.connect(router._must_db()) as db:
            status = (await (await db.execute(
                "SELECT status FROM browser_notification_deliveries WHERE event_id='browser-event'"
            )).fetchone())[0]
        self.assertEqual(status, "shown")


class NotificationSurfaceTests(unittest.TestCase):
    def test_both_widget_surfaces_expose_notifications_and_loading_copy(self):
        root = Path(__file__).resolve().parents[1]
        getcourse = (root / "static" / "widget.js").read_text(encoding="utf-8")
        amocrm = (root / "static" / "amocrm.html").read_text(encoding="utf-8")
        panel = (root / "panel" / "index.html").read_text(encoding="utf-8")
        browser_page = (root / "static" / "notifications.html").read_text(encoding="utf-8")
        browser_script = (root / "static" / "browser-notifications.js").read_text(encoding="utf-8")
        service_worker = (root / "static" / "notification-sw.js").read_text(encoding="utf-8")
        for source in (getcourse, amocrm):
            self.assertIn("Уведомления", source)
            self.assertIn("Загружаем подключения уведомлений", source)
            self.assertIn("/notifications", source)
            self.assertIn("/pair", source)
            self.assertIn("spinner", source)
            self.assertIn("Как Nexus выбирает нужного менеджера", source)
            self.assertIn("browser/open", source)
            self.assertIn("notify-code", source)
        self.assertIn("NEXUS_MESSENGER_NOTIFY_TELEGRAM_BOT_TOKEN", panel)
        self.assertIn("Проверяем Telegram и SaleBot", panel)
        self.assertIn("Разрешить уведомления", browser_page)
        self.assertIn("эту страницу можно закрыть", browser_page)
        self.assertIn("serviceWorker.register", browser_script)
        self.assertIn("pushManager.subscribe", browser_script)
        self.assertIn("showNotification", service_worker)
        self.assertIn("notificationclick", service_worker)
        self.assertIn("spinner", browser_page)


if __name__ == "__main__":
    unittest.main()
