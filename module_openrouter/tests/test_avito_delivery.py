import unittest
from unittest.mock import AsyncMock, patch

from module_openrouter import router


class _Response:
    status_code = 200
    text = "ok"

    def raise_for_status(self):
        return None


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _HistoryResponse(_Response):
    def json(self):
        return {"result": []}


class _HistoryClient:
    def __init__(self):
        self.params = None

    async def get(self, _url, *, params):
        self.params = params
        return _HistoryResponse()


class AvitoDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_history_requests_explicit_max_limit(self):
        client = _HistoryClient()
        self.assertEqual(await router._salebot_history_payload(client, "history", "123", attempts=1), {"result": []})
        self.assertEqual(client.params, {"client_id": "123", "limit": 2000})

    def test_history_parser_ignores_callbacks_and_incoming_messages(self):
        payload = {"result": [
            {"id": 12, "text": "callback openai_answer", "client_replica": True, "message_from_outside": 3},
            {"id": 13, "text": "вопрос", "client_replica": True, "message_from_outside": 0},
            {"id": 14, "text": "ответ", "client_replica": False, "message_from_outside": 0, "delivered": True},
        ]}
        self.assertEqual(router._salebot_history_outbound_id(payload, after_id=10), 14)
        self.assertIsNone(router._salebot_history_outbound_id(payload, after_id=14))

    async def _send(self, polls):
        retry_post = AsyncMock(return_value=_Response())
        once_post = AsyncMock(return_value=_Response())
        history = AsyncMock(return_value={"result": [{"id": 10}]})
        with (
            patch.object(router, "_env", return_value={
                "salebot_key": "key",
                "salebot_avito_first_answer_block_id": "60812489",
            }),
            patch.object(router.httpx, "AsyncClient", return_value=_Client()),
            patch.object(router, "_salebot_post_json_with_retry", retry_post),
            patch.object(router, "_salebot_post_json_once", once_post),
            patch.object(router, "_salebot_history_payload", history),
            patch.object(router, "_wait_for_salebot_outbound", AsyncMock(side_effect=polls)),
        ):
            result = await router._send_salebot_avito_callback(
                salebot_id="123",
                avito_id="avito",
                message="вопрос",
                answer="ответ",
                conversation_id="or_conv_test",
                callback_message="callback openai_answer",
                split_size=800,
            )
        return result, retry_post, once_post

    async def test_callback_delivery_does_not_run_answer_block(self):
        result, retry_post, once_post = await self._send([(20, True)])
        self.assertTrue(result["delivery_verified"])
        self.assertEqual(result["delivery_method"], "callback")
        self.assertEqual(retry_post.await_count, 1)
        self.assertEqual(once_post.await_count, 1)

    async def test_missing_callback_delivery_runs_only_first_answer_block(self):
        result, retry_post, once_post = await self._send([(None, True), (21, True)])
        self.assertTrue(result["delivery_verified"])
        self.assertEqual(result["delivery_method"], "message_block")
        self.assertEqual(retry_post.await_count, 1)
        self.assertEqual(once_post.await_count, 2)
        self.assertEqual(once_post.await_args_list[-1].args[2], {"client_id": "123", "message_id": "60812489"})

    async def test_unavailable_final_poll_skips_fallback(self):
        result, retry_post, once_post = await self._send([(None, False)])
        self.assertFalse(result["delivery_verified"])
        self.assertEqual(retry_post.await_count, 1)
        self.assertEqual(once_post.await_count, 1)
        self.assertIn("fallback skipped", result["delivery_error"])

    async def test_callback_timeout_is_not_retried_and_fallback_is_checked(self):
        retry_post = AsyncMock(return_value=_Response())
        once_post = AsyncMock(side_effect=[TimeoutError("ambiguous"), _Response()])
        with (
            patch.object(router, "_env", return_value={
                "salebot_key": "key",
                "salebot_avito_first_answer_block_id": "60812489",
            }),
            patch.object(router.httpx, "AsyncClient", return_value=_Client()),
            patch.object(router, "_salebot_post_json_with_retry", retry_post),
            patch.object(router, "_salebot_post_json_once", once_post),
            patch.object(router, "_salebot_history_payload", AsyncMock(return_value={"result": [{"id": 10}]})),
            patch.object(router, "_wait_for_salebot_outbound", AsyncMock(side_effect=[(None, True), (21, True)])),
        ):
            result = await router._send_salebot_avito_callback(
                salebot_id="123", avito_id="avito", message="вопрос", answer="ответ",
                conversation_id="or_conv_test", callback_message="callback openai_answer", split_size=800,
            )
        self.assertTrue(result["delivery_verified"])
        self.assertEqual(once_post.await_count, 2)

    async def test_avito_review_request_is_ignored(self):
        data = router.AvitoChatIn(
            salebot_id="123",
            platform_id="avito",
            prompt="prompts/avito_gpt1.txt",
            message="Оставьте отзыв об исполнителе — это поможет другим",
        )
        with patch.object(router, "_run_chat_with_stale_retry", AsyncMock()) as generate:
            result = await router._deliver_avito_job(data, job_id=1)
        self.assertEqual(result["reason"], "avito_review_request")
        generate.assert_not_awaited()

    async def test_avito_system_review_request_is_ignored(self):
        data = router.AvitoChatIn(
            salebot_id="123",
            platform_id="avito",
            prompt="prompts/avito_gpt1.txt",
            message=(
                "[Системное сообщение] 🖊 Поделитесь впечатлениями: отзыв поможет другим "
                "понять, стоит ли иметь дело со специалистом"
            ),
        )
        with patch.object(router, "_run_chat_with_stale_retry", AsyncMock()) as generate:
            result = await router._deliver_avito_job(data, job_id=1)
        self.assertEqual(result["reason"], "avito_review_request")
        generate.assert_not_awaited()

    def test_normal_client_review_question_is_not_ignored(self):
        self.assertEqual(router._avito_ignored_reason("Где можно оставить вам отзыв?"), "")


if __name__ == "__main__":
    unittest.main()
