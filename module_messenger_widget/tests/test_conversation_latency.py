import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import router
from test_workflow import request_for


class ConversationLatencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_catalogue_renders_without_waiting_for_provider(self):
        rows = [{"channel_id": "email", "transport": "email", "provider": "email"}]
        gate = asyncio.Event()
        async def slow(**kwargs):
            await gate.wait()
            return []
        with patch.object(router, '_all_channels_cache_owner', str(router._db_path or '')), \
                patch.object(router, '_all_channels_cache', (time.monotonic()-1, rows)), \
                patch.object(router, '_all_channels_inflight', None), \
                patch.object(router, '_cached_active_channels', side_effect=slow), \
                patch.object(router, '_vk_channel', AsyncMock(return_value=None)), \
                patch.object(router, '_telegram_channel', AsyncMock(return_value=None)), \
                patch.object(router, '_email_channel', AsyncMock(return_value=None)):
            result = await asyncio.wait_for(router._all_channels(), timeout=0.1)
            self.assertEqual(result, rows)
            self.assertFalse(gate.is_set())
            task = router._all_channels_inflight
            gate.set()
            await task

    async def test_active_trial_is_distinguished_from_paid_course(self):
        student = AsyncMock(return_value={'found': True, 'paid_access': False,
            'item': {'enrollment_id': 'gc:7', 'course_display': 'Доступ ещё не куплен'}})
        trial = AsyncMock(return_value={'status': 'active', 'expires_at': '2026-09-10T00:00:00Z'})
        def service(module, name):
            return trial if name == 'service_widget_test_period' else student
        with patch.object(router, '_identity_index', None), \
                patch.object(router, '_apply_identity_rules', AsyncMock()), \
                patch.object(router, '_module_service', side_effect=service):
            result = await router._widget_getcourse_card_data(
                {'entity_type': 'lead', 'entity_id': '7'}, 'amocrm', {}, summary_only=True)
        self.assertEqual(result['item']['course_display'], 'Тестовый доступ (не покупка)')
        self.assertFalse(result['paid_access'])

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
            patch.object(router, "_recipient_unavailable_reason", AsyncMock(return_value="")),
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
