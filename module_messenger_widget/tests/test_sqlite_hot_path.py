import asyncio
import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiosqlite
from starlette.requests import Request

import router


def request_for(path: str, body: dict, token: str) -> Request:
    raw = json.dumps(body).encode()
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
        "server": ("junior.sobakovod.pro", 443), "client": ("203.0.113.20", 50100),
        "headers": [
            (b"origin", router.DEFAULT_ALLOWED_ORIGIN.encode()),
            (b"content-type", b"application/json"),
            (b"authorization", f"Bearer {token}".encode()),
            (b"host", b"junior.sobakovod.pro"),
        ],
    }, receive)


class SQLiteHotPathTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="messenger-sqlite-hot-path-")
        router._db_path = Path(self.tmp.name) / "module.db"
        router._logger = logging.getLogger("messenger-sqlite-hot-path-tests")
        await router._init_db()
        self.token = "employee-device-token-" + "x" * 32
        now = router._iso()
        async with aiosqlite.connect(router._must_db()) as db:
            cursor = await db.execute(
                """INSERT INTO admins(wazzup_user_id,name,role,enabled,created_at,updated_at)
                   VALUES('employee-1','Анна','employee',1,?,?)""",
                (now, now),
            )
            self.admin_id = int(cursor.lastrowid)
            cursor = await db.execute(
                """INSERT INTO devices(
                   admin_id,token_hash,token_hint,platform,created_at,last_used_at,expires_at
                   ) VALUES(?,?,?,'getcourse',?,?,?)""",
                (self.admin_id, router._hash(self.token), "test", now, now, "2999-01-01T00:00:00Z"),
            )
            self.device_id = int(cursor.lastrowid)
            await db.commit()

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def _seed_direct_channels(self):
        now = router._iso()
        phone = "+79991234567"
        channels = [{
            "channel_id": "max-1", "transport": "max", "provider": "wazzup",
            "channel_transport": "max", "label": "MAX",
        }]
        direct = (
            ("vk", "vk:group", "vk", "vk-user"),
            (router.TELEGRAM_PROVIDER, "telegram-personal:account", "telegram", "tg-user"),
            (router.SALEBOT_PROVIDER, "salebot:project", "salebot", "sb-user"),
        )
        async with aiosqlite.connect(router._must_db()) as db:
            await db.execute(
                """INSERT INTO wazzup_chats(
                   channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,
                   responsible_admin_id,created_at,updated_at
                   ) VALUES('max-1','max','79991234567',?,'Клиент',?,?,?,?)""",
                (router._phone_hash(phone), now, self.admin_id, now, now),
            )
            for provider, channel_id, transport, peer_id in direct:
                channels.append({
                    "channel_id": channel_id, "transport": transport, "provider": provider,
                    "channel_transport": transport, "label": provider,
                })
                await db.execute(
                    """INSERT INTO external_identity_links(
                       provider,external_user_id,getcourse_user_id,phone,name,source,updated_at
                       ) VALUES(?,?,?,?,?,'test',?)""",
                    (provider, peer_id, "42", phone, "Клиент", now),
                )
                await db.execute(
                    """INSERT INTO entity_identity_links(
                       platform,entity_type,entity_id,provider,external_user_id,confirmed_by,created_at,updated_at
                       ) VALUES('getcourse','user','42',?,?,?,?,?)""",
                    (provider, peer_id, self.admin_id, now, now),
                )
                await db.execute(
                    """INSERT INTO wazzup_chats(
                       channel_id,chat_type,chat_id,contact_name,last_message_at,
                       responsible_admin_id,created_at,updated_at
                       ) VALUES(?,?,?,?,?,?,?,?)""",
                    (channel_id, transport, peer_id, "Клиент", now, self.admin_id, now, now),
                )
            await db.commit()
        return phone, channels

    async def test_channel_card_reuses_one_sqlite_reader_for_all_providers(self):
        phone, channels = await self._seed_direct_channels()
        real_connect = router._connect
        opened = 0

        async def counted_connect():
            nonlocal opened
            opened += 1
            return await real_connect()

        with (
            patch.object(router, "_connect", new=counted_connect),
            patch.object(router, "_all_channels", new=AsyncMock(return_value=channels)),
            patch.object(router, "_streams_vk_profile_name", new=AsyncMock(return_value="Клиент VK")),
        ):
            response = await router.widget_channels(request_for(
                "/widget/channels",
                {
                    "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
                    "phone": phone, "name": "Клиент",
                },
                self.token,
            ))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(json.loads(response.body)["channels"]), len(channels))
        # One authentication lookup and one card-local reader. Before the
        # optimization each direct provider opened several extra connections.
        self.assertEqual(opened, 2)

    async def test_simultaneous_employees_keep_independent_card_connections(self):
        phone, channels = await self._seed_direct_channels()
        real_presence = router._conversation_presence
        card_connections: set[int] = set()
        both_entered = asyncio.Event()

        async def synchronized_presence(active_channels, active_phone, *, db=None):
            card_connections.add(id(db))
            if len(card_connections) == 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), timeout=1)
            return await real_presence(active_channels, active_phone, db=db)

        with (
            patch.object(router, "_all_channels", new=AsyncMock(return_value=channels)),
            patch.object(router, "_conversation_presence", new=synchronized_presence),
            patch.object(router, "_streams_vk_profile_name", new=AsyncMock(return_value="Клиент VK")),
        ):
            responses = await asyncio.gather(*(
                router.widget_channels(request_for(
                    "/widget/channels",
                    {
                        "source_url": "https://club.sobakovod.pro/user/control/user/update/id/42",
                        "phone": phone, "name": "Клиент",
                    },
                    self.token,
                ))
                for _ in range(2)
            ))

        self.assertEqual([response.status_code for response in responses], [200, 200])
        self.assertEqual(len(card_connections), 2)

    async def test_direct_inbox_links_are_joined_without_per_row_connections(self):
        now = router._iso()
        channel = {
            "channel_id": "vk:group", "transport": "vk", "provider": "vk", "label": "VK",
        }
        async with aiosqlite.connect(router._must_db()) as db:
            for index in range(20):
                peer_id = f"vk-{index}"
                phone = f"+7999000{index:04d}"
                phone_hash = router._phone_hash(phone)
                await db.execute(
                    "INSERT INTO client_links(phone_hash,phone,getcourse_user_id,name,source,updated_at) VALUES(?,?,?,?,?,?)",
                    (phone_hash, phone, str(index), f"Клиент {index}", "test", now),
                )
                await db.execute(
                    """INSERT INTO external_identity_links(
                       provider,external_user_id,getcourse_user_id,phone,name,source,updated_at
                       ) VALUES('vk',?,?,?,?,?,?)""",
                    (peer_id, str(index), phone, f"Клиент {index}", "test", now),
                )
                await db.execute(
                    """INSERT INTO wazzup_chats(
                       channel_id,chat_type,chat_id,phone_hash,contact_name,last_message_at,
                       responsible_admin_id,created_at,updated_at
                       ) VALUES('vk:group','vk',?,?,?,?,?,?,?)""",
                    (peer_id, phone_hash, f"Клиент {index}", now, self.admin_id, now, now),
                )
                await db.execute(
                    """INSERT INTO wazzup_messages(
                       external_id,channel_id,chat_type,chat_id,direction,text,sent_at,created_at
                       ) VALUES(?,'vk:group','vk',?,'incoming','Привет',?,?)""",
                    (f"message-{index}", peer_id, now, now),
                )
            await db.commit()

        real_connect = router._connect
        opened = 0

        async def counted_connect():
            nonlocal opened
            opened += 1
            return await real_connect()

        with patch.object(router, "_connect", new=counted_connect):
            result = await router._inbox_items(
                {"id": self.device_id, "admin_id": self.admin_id, "admin_role": "employee"},
                [channel],
            )

        self.assertEqual(len(result["items"]), 20)
        self.assertEqual(opened, 2)

    async def test_inbox_identity_enrichment_uses_one_batch_transaction(self):
        real_connect = router._connect
        opened = 0

        async def counted_connect():
            nonlocal opened
            opened += 1
            return await real_connect()

        links = [
            {
                "phone": f"+7999111{index:04d}", "getcourse_user_id": str(index),
                "name": f"Клиент {index}", "source": "inbox",
            }
            for index in range(20)
        ]
        with patch.object(router, "_connect", new=counted_connect):
            await router._remember_client_links(links)

        async with aiosqlite.connect(router._must_db()) as db:
            stored = int((await (await db.execute(
                "SELECT COUNT(*) FROM client_links"
            )).fetchone())[0])
        self.assertEqual(stored, 20)
        self.assertEqual(opened, 1)


if __name__ == "__main__":
    unittest.main()
