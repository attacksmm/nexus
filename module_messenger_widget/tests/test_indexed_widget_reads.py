import unittest
from unittest.mock import AsyncMock, patch

import aiosqlite
import router


class IndexedWidgetReads(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = await aiosqlite.connect(':memory:')
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript('''
            CREATE TABLE wazzup_chats (channel_id TEXT, chat_type TEXT, chat_id TEXT,
                phone_hash TEXT, contact_name TEXT, responsible_admin_id INTEGER,
                updated_at TEXT, UNIQUE(channel_id,chat_type,chat_id));
            CREATE INDEX phone_idx ON wazzup_chats(phone_hash,updated_at DESC);
            CREATE TABLE client_links(phone_hash TEXT PRIMARY KEY,responsible_admin_id INTEGER);
            CREATE TABLE wazzup_messages(channel_id TEXT,chat_type TEXT,chat_id TEXT,
                direction TEXT,author_name TEXT,sent_at TEXT,id INTEGER);
            CREATE INDEX message_idx ON wazzup_messages(channel_id,chat_type,chat_id,sent_at,id);
        ''')
        await self.db.executemany('INSERT INTO wazzup_chats VALUES(?,?,?,?,?,?,?)',
            [('vk:10', 'vk', str(i), 'noise', '', 1, '') for i in range(20000)])
        await self.db.executemany('INSERT INTO wazzup_chats VALUES(?,?,?,?,?,?,?)', [
            ('max:1', 'max', '79990000000', '', 'Макс', 1, ''),
            ('vk:10', 'vk', 'target', 'target-phone', 'Катерина', 1, ''),
            ('salebot:project', 'salebot', 'target', '', 'Другой клиент', 1, ''),
        ])
        await self.db.commit()
        self.steps = 0
        def progress():
            self.steps += 1
            return self.steps > 30  # Abort any accidental whole-table scan.
        await self.db.set_progress_handler(progress, 100)

    async def asyncTearDown(self):
        await self.db.close()

    async def test_presence_checks_phone_and_exact_route_without_scan(self):
        with patch.object(router, '_phone_hash', return_value='target-phone'):
            result = await router._conversation_presence([
                {'channel_id': 'max:1', 'transport': 'max', 'provider': 'wazzup'},
                {'channel_id': 'vk:10', 'transport': 'vk', 'provider': 'vk'},
            ], '+79990000000', db=self.db)
        self.assertEqual(result, {('max:1', 'max')})
        self.assertLess(self.steps, 30)

    async def test_owner_update_is_exact_and_does_not_rewrite_unchanged_rows(self):
        with patch.object(router, '_direct_provider_routes', return_value=[('vk:10', 'vk')]):
            await router._assign_client_threads(7, direct_links=[('vk', 'target')], db=self.db)
            changes = self.db.total_changes
            await router._assign_client_threads(7, direct_links=[('vk', 'target')], db=self.db)
            self.assertEqual(self.db.total_changes, changes)
        rows = await (await self.db.execute(
            'SELECT responsible_admin_id FROM wazzup_chats WHERE channel_id=? AND chat_type=? AND chat_id=?',
            ('salebot:project', 'salebot', 'target'))).fetchone()
        self.assertEqual(rows[0], 1)

    async def test_profile_name_uses_exact_provider_index(self):
        with patch.object(router, '_direct_provider_routes', return_value=[('vk:10', 'vk')]), \
                patch.object(router, '_streams_vk_profile_name', AsyncMock(return_value='')):
            name = await router._provider_profile_name('vk', 'target', db=self.db)
        self.assertEqual(name, 'Катерина')
        self.assertLess(self.steps, 30)

    async def test_missing_profile_does_not_scan_other_clients(self):
        with patch.object(router, '_direct_provider_routes', return_value=[('vk:10', 'vk')]), \
                patch.object(router, '_streams_vk_profile_name', AsyncMock(return_value='')):
            name = await router._provider_profile_name('vk', 'absent', 'Клиент', db=self.db)
        self.assertEqual(name, 'Клиент')
        self.assertLess(self.steps, 30)
