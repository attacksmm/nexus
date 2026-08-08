import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from module_openrouter import router


class ContextAndRetryTests(unittest.IsolatedAsyncioTestCase):
    def test_senler_template_values_cover_all_supported_marker_forms(self):
        values = {
            "airtime": "19:00",
            "web_date": "8 августа",
            "campaign_id": "42",
            "banner_id": "7",
        }
        text = "{%airtime%} [%web_date%] #{campaign_id} {{campaign_id}} %7B%7Bbanner_id%7D%7D {{missing}}"
        self.assertEqual(
            router._render_senler_template_text(text, values),
            "19:00 8 августа 42 42 7 {{missing}}",
        )

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "openrouter.db"
        with sqlite3.connect(self.db_path) as db:
            db.executescript(
                """
                CREATE TABLE users (
                    platform_id TEXT PRIMARY KEY,
                    summary TEXT NOT NULL DEFAULT '',
                    total_tokens_used INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE conversations (
                    conversation_id TEXT PRIMARY KEY,
                    platform_id TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    prompt_path TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL,
                    platform_id TEXT NOT NULL,
                    pair_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'api',
                    prompt_path TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT '',
                    usage_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '');
                CREATE TABLE usage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    conversation_id TEXT NOT NULL DEFAULT '',
                    platform_id TEXT NOT NULL DEFAULT '',
                    prompt_path TEXT NOT NULL DEFAULT '',
                    model TEXT NOT NULL DEFAULT '',
                    usage_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                """
            )
            for key, value in {
                "history_limit": "80",
                "request_timeout": "90",
                "summary_model": "test/summary",
                "summary_prompt": router.CLIENT_STORY_SUMMARY_PROMPT,
                "budget_daily_warn": "0",
                "budget_monthly_warn": "0",
            }.items():
                db.execute("INSERT INTO settings(key,value) VALUES(?,?)", (key, value))
            db.execute(
                "INSERT INTO users VALUES (?,?,?,?,?)",
                ("884568514", "Собака: доберман.\n\nКоротко, о чем говорили:\n- Обсуждали курс.", 0, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            )
            db.execute(
                "INSERT INTO conversations VALUES (?,?,?,?,?,?,?)",
                ("or_conv_test", "884568514", 1, "prompts/test.txt", "test/model", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            )
            for index in range(10):
                db.execute(
                    "INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    ("or_conv_test", "884568514", f"pair-{index}", "assistant" if index % 2 else "user", f"history-{index}", "senler", "prompts/test.txt", "test/model", "{}", "2026-01-01T00:00:00Z"),
                )
        self.old_db_path = router._db_path
        router._db_path = self.db_path

    def tearDown(self):
        for task in router._summary_workers.values():
            task.cancel()
        router._summary_workers.clear()
        router._summary_pending.clear()
        router._db_path = self.old_db_path
        self.tmp.cleanup()

    async def test_context_four_is_summary_plus_last_four_pairs_for_every_source(self):
        captured = []

        async def call_openrouter(model, messages, timeout, settings):
            captured.append(messages)
            return "answer", {"total_tokens": 1}

        data = router.SenlerChatIn(
            platform_id="884568514",
            conversation_id="or_conv_test",
            prompt="prompts/test.txt",
            message="Да",
            context=4,
        )
        with (
            patch.object(router, "_resolve_prompt", new=AsyncMock(return_value=("prompts/test.txt", "PROMPT"))),
            patch.object(router, "_settings", new=AsyncMock(return_value={"history_limit": "80", "request_timeout": "90"})),
            patch.object(router, "_model_for_prompt", new=AsyncMock(return_value="test/model")),
            patch.object(router, "_call_openrouter", side_effect=call_openrouter),
            patch.object(router, "_customer_db_vk_client_name", new=AsyncMock(return_value="")),
        ):
            for source in ("api", "senler", "avito", "salebot"):
                result = await router._run_chat(data, allow_write=False, source=source, defer_summary=True)
                self.assertEqual(result["read_context"], 4)

        for messages in captured:
            self.assertIn("Коротко, о чем говорили", messages[0]["content"])
            self.assertEqual([item["content"] for item in messages[1:-1]], [f"history-{i}" for i in range(2, 10)])
            self.assertEqual(messages[-1], {"role": "user", "content": "Да"})

    async def test_undelivered_legacy_retry_is_not_context_or_summary_input(self):
        async with router.aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("or_conv_test", "884568514", "retry-user", "user", "retry question", "senler_retry", "prompts/test.txt", "test/model", "{}", "2026-01-01T00:00:01Z"),
            )
            await db.execute(
                "INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("or_conv_test", "884568514", "retry-answer", "assistant", "undelivered answer", "senler_retry", "prompts/test.txt", "test/model", "{}", "2026-01-01T00:00:01Z"),
            )
            await db.execute(
                "INSERT INTO messages(conversation_id,platform_id,pair_id,role,content,source,prompt_path,model,usage_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("or_conv_test", "884568514", "fallback-answer", "assistant", "undelivered fallback", "senler_fallback_undelivered", "prompts/test.txt", "test/model", "{}", "2026-01-01T00:00:01Z"),
            )
            await db.commit()
            history = await router._load_history(db, "or_conv_test", -1)
            transcript = await router._conversation_transcript(db, "or_conv_test")

        self.assertNotIn("retry question", [item["content"] for item in history])
        self.assertNotIn("undelivered answer", [item["content"] for item in history])
        self.assertNotIn("undelivered fallback", [item["content"] for item in history])
        self.assertNotIn("Вопрос: retry question", transcript)
        self.assertNotIn("Ответ: undelivered answer", transcript)
        self.assertNotIn("Ответ: undelivered fallback", transcript)

    async def test_senler_retry_delivers_before_persisting(self):
        result = {
            "platform_id": "123",
            "conversation_id": "or_conv_test",
            "prompt": "prompts/test.txt",
            "model": "test/model",
            "text": "Готовый ответ",
            "usage": {"total_tokens": 3},
        }
        save_turn = AsyncMock()
        with (
            patch.object(router, "_run_chat", new=AsyncMock(return_value=result)),
            patch.object(router, "_senler_set_var_and_add_ai_bot", new=AsyncMock(return_value={"ok": True})) as deliver,
            patch.object(router, "_save_turn", new=save_turn),
        ):
            returned = await router._retry_failed_chat_job(
                {"platform_id": "123", "prompt": "prompts/test.txt", "message": "Вопрос", "context": 2},
                source="senler_retry",
                model_cls=router.SenlerChatIn,
                job_id=7,
            )

        deliver.assert_awaited_once_with("123", "ai_answer", "Готовый ответ")
        self.assertEqual(save_turn.await_args.kwargs["source"], "senler_retry_delivered")
        self.assertTrue(returned["senler_ai_bot"]["ok"])

    def test_summary_tracks_explicit_sales_state(self):
        self.assertIn("Коротко, о чем говорили:", router.CLIENT_STORY_SUMMARY_PROMPT)
        self.assertIn("на что клиент явно согласился", router.CLIENT_STORY_SUMMARY_PROMPT)
        self.assertIn("от чего клиент явно отказался", router.CLIENT_STORY_SUMMARY_PROMPT)
        self.assertIn("согласился получить ссылку", router.CLIENT_STORY_SUMMARY_PROMPT)
        self.assertIn("до фактического выполнения", router.CLIENT_STORY_SUMMARY_PROMPT)
        for label in ("- Обсудили:", "- Согласился:", "- Отказался:", "- Ожидает:"):
            self.assertIn(label, router.CLIENT_STORY_SUMMARY_PROMPT)
        self.assertIn("Все четыре строки обязательны", router.CLIENT_STORY_SUMMARY_PROMPT)

    def test_unknown_parenthetical_name_is_removed(self):
        answer = "Ох, Наталья (или как вас зовут?), я прочитала внимательно."

        self.assertEqual(
            router._rewrite_direct_client_address(answer, ""),
            "Ох, я прочитала внимательно.",
        )

    def test_unknown_parenthetical_name_is_replaced_with_trusted_latin_name(self):
        answer = "Ох, Наталья (или как вас зовут?), я прочитала внимательно."

        self.assertEqual(
            router._rewrite_direct_client_address(answer, "Tatyana"),
            "Ох, Tatyana, я прочитала внимательно.",
        )

    async def test_customer_db_accepts_vkontakte_latin_name(self):
        customer_db = Path(self.tmp.name) / "customer-db.db"
        with sqlite3.connect(customer_db) as db:
            db.execute(
                "CREATE TABLE cdb_vk_clients (id INTEGER PRIMARY KEY, platform_id TEXT, custom_fields TEXT, updated_at TEXT)"
            )
            db.execute(
                "INSERT INTO cdb_vk_clients(platform_id,custom_fields,updated_at) VALUES (?,?,?)",
                ("691903984", '{"name":"Tatyana","second_name":"Vin"}', "2026-07-29T00:00:00Z"),
            )
        with patch.object(router, "_customer_db_path", return_value=customer_db):
            self.assertEqual(await router._customer_db_vk_client_name("691903984"), "Tatyana")

    async def test_background_summaries_are_coalesced_per_platform(self):
        active = 0
        peak = 0

        calls = []

        async def generate(conversation_id, *, incremental=True):
            nonlocal active, peak
            calls.append(conversation_id)
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {"model": "test/model", "summary": "ok"}

        router._summary_workers.clear()
        router._summary_pending.clear()
        with patch.object(router, "_generate_and_save_summary", side_effect=generate):
            router._schedule_summary("or_conv_old", "884568514")
            router._schedule_summary("or_conv_test", "884568514")
            await asyncio.gather(*list(router._summary_workers.values()))

        self.assertEqual(peak, 1)
        self.assertEqual(calls, ["or_conv_test"])
        self.assertFalse(router._summary_workers)

    async def test_incremental_summary_uses_previous_summary_and_recent_messages(self):
        captured = {}

        async def call_openrouter(model, messages, timeout, settings):
            captured["source"] = messages[-1]["content"]
            return "Новая сводка", {"total_tokens": 4, "cost": 0.001}

        with (
            patch.object(router, "_call_openrouter", side_effect=call_openrouter),
            patch.object(router, "_record_usage_event", new=AsyncMock()),
        ):
            await router._generate_and_save_summary("or_conv_test", incremental=True)

        self.assertIn("ПРЕДЫДУЩАЯ СВОДКА", captured["source"])
        self.assertNotIn("history-0", captured["source"])
        self.assertIn("history-9", captured["source"])

    async def test_spend_report_uses_exact_cost_and_soft_warning(self):
        now = router._now()
        with sqlite3.connect(self.db_path) as db:
            db.execute("UPDATE settings SET value='0.5' WHERE key='budget_daily_warn'")
            db.execute(
                "INSERT INTO usage_events(kind,source,prompt_path,model,usage_json,created_at) VALUES(?,?,?,?,?,?)",
                ("answer", "senler", "prompts/test.txt", "test/model", '{"prompt_tokens":100,"completion_tokens":20,"cached_tokens":80,"cost":0.01}', now),
            )
        with patch.object(router, "_openrouter_key_usage", new=AsyncMock(return_value={"ok": True, "daily": 0.6, "weekly": 1.0, "monthly": 3.0, "usage": 5.0})):
            report = await router._spend_report(30)

        self.assertEqual(report["local"]["periods"]["today"]["cost"], 0.01)
        self.assertEqual(report["local"]["models"][0]["model"], "test/model")
        self.assertTrue(next(item for item in report["warnings"] if item["period"] == "day")["exceeded"])

    async def test_openrouter_usage_keeps_cost_cache_provider_and_generation(self):
        class Response:
            status_code = 200
            text = ""

            @staticmethod
            def json():
                return {
                    "id": "gen-1",
                    "provider": "DeepSeek",
                    "choices": [{"message": {"content": "Готово"}}],
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 20,
                        "total_tokens": 120,
                        "cost": 0.0002,
                        "prompt_tokens_details": {"cached_tokens": 80, "cache_write_tokens": 5},
                        "cost_details": {"upstream_inference_cost": 0.0001},
                    },
                }

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def post(self, *args, **kwargs):
                return Response()

        with (
            patch.object(router, "_env", return_value={"openrouter_key": "key", "openrouter_proxy": ""}),
            patch.object(router.httpx, "AsyncClient", return_value=Client()),
        ):
            answer, usage = await router._call_openrouter("test/model", [{"role": "user", "content": "x"}], 10, {})

        self.assertEqual(answer, "Готово")
        self.assertEqual(usage["cost"], 0.0002)
        self.assertEqual(usage["cached_tokens"], 80)
        self.assertEqual(usage["provider"], "DeepSeek")
        self.assertEqual(usage["generation_id"], "gen-1")


if __name__ == "__main__":
    unittest.main()
