import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import router
from test_workflow import request_for


class ConversationLatencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        router._vk_history_cache.clear()
        router._vk_history_inflight.clear()
        self.tasks = []

    def create_task(self, coroutine, *, name):
        task = asyncio.create_task(coroutine, name=name)
        self.tasks.append(task)
        return task

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_saved_vk_conversation_opens_while_provider_is_still_waiting(self):
        provider_started, release_provider = asyncio.Event(), asyncio.Event()

        async def provider(*args, **kwargs):
            provider_started.set()
            await release_provider.wait()
            return {"items": [], "count": 0}

        channel = {"channel_id": "vk:10", "transport": "vk", "provider": "vk"}
        messages = [{"external_id": "vk:1", "text": "Сохранённое сообщение"}]
        catalogue = AsyncMock(return_value=[channel])
        with (
            patch.object(router, "_module_lifecycle", self),
            patch.object(router, "_device", AsyncMock(return_value={"id": 1})),
            patch.object(router, "_validate_device_context"),
            patch.object(router, "_all_channels", catalogue),
            patch.object(router, "_external_link", AsyncMock(return_value={"external_user_id": "700"})),
            patch.object(router, "_conversation_rows", AsyncMock(return_value=("700", True, messages))),
            patch.object(router, "_mark_thread_read", AsyncMock()),
            patch.object(router, "_vk_request", side_effect=provider),
            patch.object(router, "_store_vk_messages", AsyncMock()),
        ):
            response = await asyncio.wait_for(router.widget_conversation(request_for(
                "/widget/conversation", {**channel, "vk_id": "700"},
            )), timeout=0.5)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(json.loads(response.body)["messages"], messages)
            self.assertFalse(release_provider.is_set())
            catalogue.assert_awaited_once()
            await asyncio.wait_for(provider_started.wait(), timeout=0.5)
            release_provider.set()
            await asyncio.gather(*self.tasks)

    async def test_vk_refresh_is_shared_and_survives_one_disconnected_request(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def provider(*args, **kwargs):
            started.set()
            await release.wait()
            return {"items": [{"id": 1}], "count": 1}

        with (
            patch.object(router, "_module_lifecycle", self),
            patch.object(router, "_external_link", AsyncMock(return_value={"external_user_id": "700"})),
            patch.object(router, "_vk_request", side_effect=provider) as remote,
            patch.object(router, "_store_vk_messages", AsyncMock()),
        ):
            readers = [asyncio.create_task(router._load_vk_history("700")) for _ in range(8)]
            await asyncio.wait_for(started.wait(), timeout=0.5)
            readers[0].cancel()
            await asyncio.gather(readers[0], return_exceptions=True)
            release.set()
            results = await asyncio.gather(*readers[1:])
            self.assertEqual(results, [(1, False)] * 7)
            remote.assert_awaited_once()
            self.assertFalse(router._vk_history_inflight)

    async def test_explicit_empty_catalogue_does_not_rediscover_or_accept_channel(self):
        with patch.object(router, "_all_channels", AsyncMock()) as discover:
            with self.assertRaises(router.HTTPException) as failure:
                await router._requested_channel("vk:10", "vk", "vk", channels=[])
            self.assertEqual(failure.exception.status_code, 409)
            discover.assert_not_awaited()

    async def test_saved_wazzup_history_does_not_resolve_identity_again(self):
        channel = {"channel_id": "max-1", "transport": "max", "provider": "wazzup"}
        with (
            patch.object(router, "_device", AsyncMock(return_value={"id": 1})),
            patch.object(router, "_validate_device_context"),
            patch.object(router, "_all_channels", AsyncMock(return_value=[channel])) as catalogue,
            patch.object(router, "_conversation_rows", AsyncMock(return_value=("700", True, [{"text": "История"}]))),
            patch.object(router, "_history_sync_info", AsyncMock(return_value={"status": "imported", "complete": True})),
            patch.object(router, "_resolved_client_identity", AsyncMock(side_effect=AssertionError("unnecessary identity lookup"))) as identity,
            patch.object(router, "_mark_thread_read", AsyncMock()),
            patch.object(router, "_responsible_admin_id", AsyncMock(return_value=None)),
            patch.object(router, "_assign_client_threads", AsyncMock()),
            patch.object(router, "_remember_notification_context", AsyncMock()),
            patch.object(router, "_widget_context", return_value={}),
        ):
            response = await router.widget_conversation(request_for(
                "/widget/conversation", {**channel, "phone": "+79991234567"},
            ))
            self.assertEqual(response.status_code, 200)
            identity.assert_not_awaited()
            catalogue.assert_awaited_once()
