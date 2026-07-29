from datetime import timezone
import unittest
import asyncio
import os
from pathlib import Path
import sqlite3
import tempfile

from module_sbkvd_letter import router


def record(**custom_fields):
    return {
        "table": "clients",
        "id": 1,
        "platform_id": "42",
        "custom_fields": custom_fields,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }


class LogicTests(unittest.TestCase):
    def test_segment_creation_uses_committing_write_context(self):
        source = Path(router.__file__).read_text(encoding="utf-8")
        create_segment_source = source.split('async def create_segment', 1)[1].split('@router.put("/segments/{segment_id}")', 1)[0]
        self.assertIn('async with _write_db(operation="create segment") as db:', create_segment_source)

    def test_compose_preview_ignores_stale_segment_responses(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("composeAudienceRequest:0", panel)
        self.assertIn("const requestId=++state.composeAudienceRequest", panel)
        self.assertGreaterEqual(
            panel.count("if(requestId!==state.composeAudienceRequest)return"),
            2,
        )
        self.assertIn("$('audienceSheets').textContent=selectedSegmentIds().length", panel)
        self.assertIn("<span>сегментов</span>", panel)

    def test_mobile_segment_editor_has_list_navigation(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="mobileSegmentBackBtn"', panel)
        self.assertIn(".segment-layout:not(.mobile-editor)>.inspector", panel)
        self.assertIn(".segment-layout.mobile-editor>.list-main", panel)
        self.assertIn("function openMobileSegmentEditor()", panel)
        self.assertIn("function closeMobileSegmentEditor()", panel)

    def test_mobile_segment_editor_imports_large_recipient_files(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="recipientImportFile"', panel)
        self.assertIn('accept=".csv,.txt,text/csv,text/plain"', panel)
        self.assertIn("'мессенджер','канал','channel','platform','платформа'", panel)
        self.assertIn("'идентификатор внутри мессенджера','platform_id','recipient_id'", panel)
        self.assertIn("const MANUAL_RENDER_LIMIT=20", panel)
        self.assertIn("const RECIPIENT_IMPORT_MAX=10000", panel)
        self.assertIn("channel==='telegram'?/^-?\\d+$/", panel)
        self.assertIn("state.manualRecipients.slice(0,MANUAL_RENDER_LIMIT)", panel)
        self.assertIn("const unique=[...new Set(tokens)]", panel)
        self.assertIn("importRecipientFile(file)", panel)
        self.assertIn("clearManualRecipientsBtn", panel)

    def test_mobile_history_has_stable_list_navigation(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("grid-template-columns:minmax(0,1fr)", panel)
        self.assertIn(".tabs{height:100%;min-width:0;flex:1", panel)
        self.assertIn('id="mobileHistoryBackBtn"', panel)
        self.assertIn(".history-layout:not(.mobile-detail)>.inspector", panel)
        self.assertIn(".history-layout.mobile-detail>.list-main", panel)
        self.assertIn("function openMobileHistoryDetail()", panel)
        self.assertIn("requestAnimationFrame(()=>layout?.scrollTo({top:0}))", panel)
        self.assertIn("function closeMobileHistoryDetail()", panel)
        self.assertIn("row.dataset.signature!==html", panel)
        self.assertIn("if(!open&&state.campaignLoadingId)return", panel)
        self.assertNotIn("body.innerHTML=state.campaigns.map", panel)

    def test_paused_campaign_detail_exposes_read_only_deletion_progress(self):
        original_db_path = router._db_path
        original_require_user = router._require_user

        async def allow_user(request, **kwargs):
            return {"role": "admin"}

        async def run(module_db):
            router._db_path = module_db
            router._require_user = allow_user
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json) VALUES(?,?,?,?,?)",
                    ("campaign", "Пауза", "paused", "{}", "{}"),
                )
                recipient_ids = []
                for recipient_id in ("kept", "deleted"):
                    cur = await db.execute(
                        "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) "
                        "VALUES(?,?,?,?,?,?,?)",
                        ("campaign", "vk", recipient_id, "manual", "{}", "message", "sent"),
                    )
                    recipient_ids.append(int(cur.lastrowid))
                await db.execute(
                    "INSERT INTO sent_messages(campaign_id,recipient_row_id,channel,recipient_id,external_message_id,sent_at) "
                    "VALUES(?,?,?,?,?,?)",
                    ("campaign", recipient_ids[0], "vk", "kept", "100", router._now()),
                )
                await db.execute(
                    "INSERT INTO sent_messages(campaign_id,recipient_row_id,channel,recipient_id,external_message_id,sent_at,deleted_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    ("campaign", recipient_ids[1], "vk", "deleted", "101", router._now(), router._now()),
                )
                await db.commit()
            return await router.campaign_detail("campaign", object())

        with tempfile.TemporaryDirectory() as tmp:
            try:
                detail = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._require_user = original_require_user

        self.assertEqual(detail["campaign"]["status"], "paused")
        self.assertEqual(
            detail["message_deletion"],
            {"total": 2, "deleted": 1, "remaining": 1, "errors": 0, "active": False, "percent_deleted": 50.0},
        )
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("deletion.remaining&&!deletion.active&&!paused", panel)

    def test_launch_restores_ready_state(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("if(result.id)await loadCampaign(result.id)", panel)
        self.assertIn("$('launchBtn').disabled=false;busy('Готово')", panel)

    def test_mobile_template_flow_separates_list_editor_and_preview(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        for element_id in (
            "mobileTemplateBackBtn",
            "mobileTemplatePreviewBtn",
            "mobilePreviewBackBtn",
        ):
            self.assertIn(f'id="{element_id}"', panel)
        self.assertIn("function openMobileTemplateEditor()", panel)
        self.assertIn("function closeMobileTemplateEditor()", panel)
        self.assertIn("function openMobileTemplatePreview()", panel)
        self.assertIn("function closeMobileTemplatePreview()", panel)
        self.assertIn(".template-layout.mobile-editor>.template-preview-pane", panel)
        self.assertIn(".template-layout.mobile-preview>.template-editor", panel)

    def test_mobile_settings_cards_expose_all_sheet_fields(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("sheet-settings-table", panel)
        self.assertIn('data-label="Использовать"', panel)
        self.assertIn('data-label="Поле ID"', panel)
        self.assertIn(".sheet-settings-table thead{display:none}", panel)
        self.assertIn(".sheet-settings-table td::before{content:attr(data-label)", panel)

    def test_deletion_polling_does_not_overlap(self):
        panel = (Path(router.__file__).with_name("panel") / "index.html").read_text(encoding="utf-8")
        self.assertIn("deletionLoading:false", panel)
        self.assertIn("if(state.deletionLoading)return", panel)
        self.assertIn("state.deletionLoading=false", panel)

    def test_nested_filters_and_modes(self):
        item = record(contact={"name": "Анна", "age": 31}, tags=["buyer", "webinar"])
        audience = {
            "mode": "and",
            "conditions": [
                {"field": "contact.name", "op": "contains", "value": "ан"},
                {"field": "contact.age", "op": "gt", "value": "18"},
                {"field": "tags", "op": "eq", "value": "buyer"},
            ],
        }
        self.assertTrue(router._matches(item, audience))
        audience["conditions"].append({"field": "contact.name", "op": "eq", "value": "Борис"})
        self.assertFalse(router._matches(item, audience))
        audience["mode"] = "or"
        self.assertTrue(router._matches(item, audience))

    def test_negative_filter_requires_all_values_to_miss(self):
        item = record(tags=["buyer", "webinar"])
        self.assertFalse(router._condition_matches(item, {"field": "tags", "op": "not_contains", "value": "buyer"}))
        self.assertTrue(router._condition_matches(item, {"field": "tags", "op": "not_contains", "value": "spam"}))
        self.assertFalse(router._condition_matches(item, {"field": "tags", "op": "neq", "value": "buyer"}))

    def test_empty_and_missing_fields(self):
        item = record(contact={"phone": ""})
        self.assertTrue(router._condition_matches(item, {"field": "contact.phone", "op": "empty"}))
        self.assertTrue(router._condition_matches(item, {"field": "contact.email", "op": "empty"}))
        self.assertFalse(router._condition_matches(item, {"field": "contact.email", "op": "not_empty"}))

    def test_personalization_reports_missing_values(self):
        text, missing = router._render(
            "Здравствуйте, {{ contact.name }}! Ваш город: {{contact.city}}.",
            record(contact={"name": "Анна"}),
        )
        self.assertEqual(text, "Здравствуйте, Анна! Ваш город: .")
        self.assertEqual(missing, ["contact.city"])

    def test_vk_html_is_sent_as_plain_text_while_telegram_keeps_markup(self):
        source = "<b>Главное &amp; важное</b><br>Вторая строка"
        template = {"parse_mode": "HTML"}
        self.assertEqual(
            router._content_for_channel(source, template, "vk"),
            "Главное & важное\nВторая строка",
        )
        self.assertEqual(router._content_for_channel(source, template, "telegram"), source)

        original_token = os.environ.get("SBKVD_LETTER_VK_TOKEN")
        calls = []

        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {"response": 77}

        class Client:
            async def post(self, url, data=None):
                calls.append((url, data))
                return Response()

        try:
            os.environ["SBKVD_LETTER_VK_TOKEN"] = "test-token"
            message_id, _ = asyncio.run(router._send_vk({
                "campaign_id": "campaign",
                "recipient_id": "42",
                "rendered_content": source,
            }, template, Client()))
        finally:
            if original_token is None:
                os.environ.pop("SBKVD_LETTER_VK_TOKEN", None)
            else:
                os.environ["SBKVD_LETTER_VK_TOKEN"] = original_token

        self.assertEqual(message_id, "77")
        self.assertEqual(calls[0][1]["message"], "Главное & важное\nВторая строка")

    def test_vk_markdown_v2_is_sent_as_plain_text(self):
        source = "*Жирный* и _курсив_\\. [Регистрация](https://example.com/form)"
        self.assertEqual(
            router._content_for_channel(source, {"parse_mode": "MarkdownV2"}, "vk"),
            "Жирный и курсив. Регистрация (https://example.com/form)",
        )

    def test_random_id_is_stable_and_nonzero(self):
        first = router._stable_random_id("campaign", "42")
        self.assertEqual(first, router._stable_random_id("campaign", "42"))
        self.assertNotEqual(first, router._stable_random_id("campaign", "43"))
        self.assertTrue(0 < first <= 0x7FFFFFFF)

    def test_empty_delivery_exception_has_visible_message(self):
        self.assertEqual(
            router._delivery_error_text(TimeoutError()),
            "Таймаут сети при обращении к API канала",
        )

    def test_network_retry_uses_long_backoff(self):
        config = {"network_retry_base_sec": 300, "network_retry_max_sec": 1800}
        self.assertEqual(router._network_retry_delay(1, config), 300)
        self.assertEqual(router._network_retry_delay(2, config), 600)
        self.assertEqual(router._network_retry_delay(3, config), 1200)
        self.assertEqual(router._network_retry_delay(4, config), 1800)
        self.assertEqual(router._network_retry_delay(20, config), 1800)

    def test_transient_delivery_errors_are_classified(self):
        self.assertTrue(router._is_transient_delivery_error(router.TransientDeliveryError("proxy")))
        self.assertFalse(router._is_transient_delivery_error(router.PermanentDeliveryError("blocked")))

    def test_channel_refusals_are_not_delivery_errors(self):
        exc = router._api_error({"error": {"error_code": 901, "error_msg": "not allowed"}}, "vk")
        self.assertIsInstance(exc, router.NotAllowedDeliveryError)
        self.assertEqual(exc.api_code, "901")
        unavailable_vk = router._api_error(
            {"error": {"error_code": 936, "error_msg": "contact not found"}}, "vk"
        )
        self.assertIsInstance(unavailable_vk, router.PermanentDeliveryError)
        self.assertNotIsInstance(unavailable_vk, router.NotAllowedDeliveryError)
        self.assertIsInstance(
            router._api_error({"error": {"error_code": 6, "error_msg": "too many requests"}}, "vk"),
            router.TransientDeliveryError,
        )
        self.assertIsInstance(
            router._api_error({"error_code": 403, "description": "Forbidden: bot was blocked by the user"}, "telegram"),
            router.NotAllowedDeliveryError,
        )
        unavailable_telegram = router._api_error(
            {"error_code": 400, "description": "Bad Request: chat not found"}, "telegram"
        )
        self.assertIsInstance(unavailable_telegram, router.PermanentDeliveryError)
        self.assertNotIsInstance(unavailable_telegram, router.NotAllowedDeliveryError)
        self.assertIsInstance(
            router._api_error({"error_code": 429, "description": "Too Many Requests"}, "telegram"),
            router.TransientDeliveryError,
        )
        self.assertIsInstance(
            router._api_error({"error_code": 401, "description": "Unauthorized"}, "telegram"),
            router.PermanentDeliveryError,
        )

    def test_rate_limit_metadata_is_parsed(self):
        telegram = router._api_error(
            {
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 7},
            },
            "telegram",
        )
        self.assertTrue(telegram.rate_limited)
        self.assertEqual(telegram.retry_after, 7)
        vk_limit = router._api_error(
            {"error": {"error_code": 6, "error_msg": "Too many requests per second"}},
            "vk",
        )
        self.assertTrue(vk_limit.rate_limited)
        vk_internal = router._api_error(
            {"error": {"error_code": 10, "error_msg": "Internal server error"}},
            "vk",
        )
        self.assertFalse(vk_internal.rate_limited)

    def test_adaptive_gate_coalesces_parallel_rate_limits(self):
        async def run():
            gate = router.AdaptiveRateGate("telegram", 20)
            error = router.TransientDeliveryError(
                "Telegram 429", rate_limited=True, retry_after=3,
            )
            first_cooldown = await gate.on_rate_limit(error)
            first = await gate.snapshot()
            await gate.on_rate_limit(error)
            second = await gate.snapshot()
            return first_cooldown, first, second

        cooldown, first, second = asyncio.run(run())
        self.assertEqual(cooldown, 3)
        self.assertEqual(first["effective_rate"], 12)
        self.assertEqual(second["effective_rate"], 12)
        self.assertEqual(second["limit_events"], 1)

    def test_vk_delete_confirmation_is_parsed_per_message(self):
        self.assertEqual(router._vk_deleted_message_ids(1, [10, 20]), {10, 20})
        self.assertEqual(router._vk_deleted_message_ids({"10": 1, "20": 0}, [10, 20]), {10})
        self.assertEqual(router._vk_deleted_message_ids([1, 0], [10, 20]), {10})

    def test_vk_delete_is_batched_with_delete_for_all(self):
        original_client = router.httpx.AsyncClient
        original_gate = router.RateGate
        original_token = os.environ.get("SBKVD_LETTER_VK_TOKEN")
        calls = []

        class FakeResponse:
            def __init__(self, ids):
                self.ids = ids

            def raise_for_status(self):
                return None

            def json(self):
                return {"response": {str(value): 1 for value in self.ids}}

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, url, data):
                ids = [int(value) for value in data["message_ids"].split(",")]
                calls.append((url, dict(data), ids))
                return FakeResponse(ids)

        class ImmediateGate:
            def __init__(self, rate):
                self.rate = rate

            async def wait(self):
                return None

        messages = [
            {"id": index, "external_message_id": str(1000 + index)}
            for index in range(1, 206)
        ]
        try:
            os.environ["SBKVD_LETTER_VK_TOKEN"] = "test-token"
            router.httpx.AsyncClient = FakeClient
            router.RateGate = ImmediateGate
            deleted, errors = asyncio.run(router._delete_vk_messages(messages))
        finally:
            router.httpx.AsyncClient = original_client
            router.RateGate = original_gate
            if original_token is None:
                os.environ.pop("SBKVD_LETTER_VK_TOKEN", None)
            else:
                os.environ["SBKVD_LETTER_VK_TOKEN"] = original_token

        self.assertEqual(deleted, set(range(1, 206)))
        self.assertEqual(errors, {})
        self.assertEqual([len(call[2]) for call in calls], [100, 100, 5])
        self.assertTrue(all(call[0].endswith("/messages.delete") for call in calls))
        self.assertTrue(all(call[1]["delete_for_all"] == 1 for call in calls))

    def test_vk_history_search_persists_only_exact_outgoing_matches(self):
        original_db_path = router._db_path
        original_client = router.httpx.AsyncClient
        original_token = os.environ.get("SBKVD_LETTER_VK_TOKEN")
        content = "Уникальный полный текст сообщения длиной больше двадцати символов"

        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "response": {
                        "count": 3,
                        "items": [
                            {"id": 101, "peer_id": 42, "out": 1, "date": 1, "text": content},
                            {"id": 102, "peer_id": 43, "out": 0, "date": 1, "text": content},
                            {"id": 103, "peer_id": 44, "out": 1, "date": 1, "text": "другой текст"},
                        ],
                    }
                }

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def post(self, *args, **kwargs):
                return Response()

        async def run(module_db):
            router._db_path = module_db
            await router._init_db()
            result = await router._discover_vk_deletion_matches(content)
            summary = await router._content_deletion_summary(content)
            async with router.aiosqlite.connect(module_db) as db:
                rows = await (await db.execute(
                    "SELECT channel,recipient_id,external_message_id FROM deletion_matches"
                )).fetchall()
            return result, summary, [tuple(row) for row in rows]

        with tempfile.TemporaryDirectory() as tmp:
            try:
                os.environ["SBKVD_LETTER_VK_TOKEN"] = "token-for-test"
                router.httpx.AsyncClient = Client
                result, summary, rows = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router.httpx.AsyncClient = original_client
                if original_token is None:
                    os.environ.pop("SBKVD_LETTER_VK_TOKEN", None)
                else:
                    os.environ["SBKVD_LETTER_VK_TOKEN"] = original_token

        self.assertEqual(result["found"], 1)
        self.assertEqual(rows, [("vk", "42", "101")])
        self.assertEqual(summary["sources"], {"module": 0, "vk_history": 1})
        self.assertEqual(summary["remaining_by_channel"], {"vk": 1})

    def test_external_vk_deletion_updates_reverse_progress_storage(self):
        original_db_path = router._db_path
        original_delete = router._delete_vk_messages
        content = "Ещё один уникальный текст сообщения для удаления"

        async def delete(messages, on_batch=None):
            deleted = {int(row["id"]) for row in messages}
            if on_batch:
                await on_batch(deleted, {})
            return deleted, {}

        async def run(module_db):
            router._db_path = module_db
            router._delete_vk_messages = delete
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                cur = await db.execute(
                    """
                    INSERT INTO deletion_matches(
                        content_hash,search_content,channel,recipient_id,external_message_id
                    ) VALUES(?,?,'vk','42','101')
                    """,
                    (router._content_hash(content), content),
                )
                await db.commit()
                row_id = int(cur.lastrowid)
            before = await router._content_deletion_summary(content)
            result = await router._delete_message_rows([{
                "id": row_id,
                "channel": "vk",
                "recipient_id": "42",
                "external_message_id": "101",
                "_tracking": "deletion_matches",
            }])
            after = await router._content_deletion_summary(content)
            return before, result, after

        with tempfile.TemporaryDirectory() as tmp:
            try:
                before, result, after = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._delete_vk_messages = original_delete

        self.assertEqual((before["remaining"], before["deleted"]), (1, 0))
        self.assertEqual(result["deleted"], 1)
        self.assertEqual((after["remaining"], after["deleted"]), (0, 1))

    def test_telegram_proxy_prefers_shared_env_then_letter_fallback(self):
        keys = ["SBKVD_LETTER_TELEGRAM_PROXY_URL", "TELEGRAM_BOT_API_PROXY_URL", "TELEGRAM_HTTPS_PROXY_URL"]
        original = {key: os.environ.get(key) for key in keys}
        try:
            for key in keys:
                os.environ.pop(key, None)
            self.assertEqual(router._telegram_proxy_url(), "")
            os.environ["SBKVD_LETTER_TELEGRAM_PROXY_URL"] = "http://letter:3129"
            self.assertEqual(router._telegram_proxy_url(), "http://letter:3129")
            os.environ["TELEGRAM_BOT_API_PROXY_URL"] = "http://shared:3128"
            self.assertEqual(router._telegram_proxy_url(), "http://shared:3128")
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_schedule_is_normalized_to_utc(self):
        value = router._parse_schedule("2026-06-19T15:00:00+03:00")
        self.assertEqual(value, "2026-06-19T12:00:00Z")
        parsed = router.datetime.fromisoformat(value.replace("Z", "+00:00"))
        self.assertEqual(parsed.tzinfo, timezone.utc)

    def test_naive_schedule_is_always_interpreted_as_moscow_time(self):
        self.assertEqual(
            router._parse_schedule("2026-07-26T15:00"),
            "2026-07-26T12:00:00Z",
        )

    def test_manual_recipients_are_deduplicated_without_customer_db(self):
        original_config = router._get_config
        original_path = router._customer_db_path

        async def config():
            return {"sheets": []}

        router._get_config = config
        router._customer_db_path = lambda: router.Path("/missing/customer.db")
        try:
            rows = asyncio.run(router._audience_records({
                "tables": [], "conditions": [], "exclude_ids": [],
                "manual_recipients": [
                    {"channel": "telegram", "recipient_id": "5601500901", "label": "Тест"},
                    {"channel": "telegram", "recipient_id": "5601500901", "label": "Дубль"},
                ],
            }))
        finally:
            router._get_config = original_config
            router._customer_db_path = original_path
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["recipient_id"], "5601500901")
        self.assertEqual(rows[0]["table"], "manual")

    def test_empty_tables_do_not_expand_to_all_configured_sheets(self):
        original_config = router._get_config
        original_path = router._customer_db_path

        with tempfile.TemporaryDirectory() as tmp:
            db_path = router.Path(tmp) / "customer.db"
            con = sqlite3.connect(db_path)
            con.executescript(
                """
                CREATE TABLE _cdb_tables(name TEXT, display_name TEXT, description TEXT, schema_json TEXT);
                INSERT INTO _cdb_tables VALUES('vk_clients','VK','','[]');
                CREATE TABLE cdb_vk_clients(
                    id INTEGER PRIMARY KEY,
                    platform_id TEXT,
                    custom_fields TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                INSERT INTO cdb_vk_clients(platform_id,custom_fields,created_at,updated_at)
                VALUES('100','{"contact":{"name":"Анна"}}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                """
            )
            con.commit()
            con.close()

            async def config():
                return {"sheets": [{"name": "vk_clients", "enabled": True, "channel": "vk", "recipient_field": "platform_id"}]}

            router._get_config = config
            router._customer_db_path = lambda: db_path
            try:
                manual_only = asyncio.run(router._audience_records({
                    "tables": [],
                    "conditions": [],
                    "manual_recipients": [{"channel": "vk", "recipient_id": "42", "label": "Тест"}],
                }))
                explicit_table = asyncio.run(router._audience_records({
                    "tables": ["vk_clients"],
                    "conditions": [],
                    "manual_recipients": [],
                }))
            finally:
                router._get_config = original_config
                router._customer_db_path = original_path

        self.assertEqual([(row["table"], row["recipient_id"]) for row in manual_only], [("manual", "42")])
        self.assertEqual([(row["table"], row["recipient_id"]) for row in explicit_table], [("vk_clients", "100")])

    def test_campaign_segments_are_union_deduplicated(self):
        original_config = router._get_config
        original_customer_path = router._customer_db_path
        original_db_path = router._db_path

        with tempfile.TemporaryDirectory() as tmp:
            module_db = router.Path(tmp) / "letter.db"
            customer_db = router.Path(tmp) / "customer.db"
            con = sqlite3.connect(customer_db)
            con.executescript(
                """
                CREATE TABLE _cdb_tables(name TEXT, display_name TEXT, description TEXT, schema_json TEXT);
                INSERT INTO _cdb_tables VALUES('vk_clients','VK','','[]');
                CREATE TABLE cdb_vk_clients(
                    id INTEGER PRIMARY KEY,
                    platform_id TEXT,
                    custom_fields TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                INSERT INTO cdb_vk_clients(platform_id,custom_fields,created_at,updated_at)
                VALUES
                    ('100','{"contact":{"name":"Анна"}}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
                    ('200','{"contact":{"name":"Борис"}}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                """
            )
            con.commit()
            con.close()

            async def run():
                async def config():
                    return {"sheets": [{"name": "vk_clients", "enabled": True, "channel": "vk", "recipient_field": "platform_id"}]}

                router._db_path = module_db
                router._get_config = config
                router._customer_db_path = lambda: customer_db
                await router._init_db()
                async with router.aiosqlite.connect(module_db) as db:
                    await db.execute(
                        "INSERT INTO segments(name,description,audience_json) VALUES(?,?,?)",
                        ("manual", "", router._dump({
                            "tables": [],
                            "conditions": [],
                            "manual_recipients": [{"channel": "vk", "recipient_id": "100", "label": ""}],
                        })),
                    )
                    await db.execute(
                        "INSERT INTO segments(name,description,audience_json) VALUES(?,?,?)",
                        ("table", "", router._dump({
                            "tables": ["vk_clients"],
                            "conditions": [],
                            "manual_recipients": [],
                        })),
                    )
                    await db.commit()
                records, snapshot, stored_segment_id = await router._resolve_campaign_records(
                    router.CampaignIn(segment_ids=[1, 2])
                )
                return records, snapshot, stored_segment_id

            try:
                records, snapshot, stored_segment_id = asyncio.run(run())
            finally:
                router._get_config = original_config
                router._customer_db_path = original_customer_path
                router._db_path = original_db_path

        self.assertEqual([(row["channel"], row["recipient_id"]) for row in records], [("vk", "100"), ("vk", "200")])
        self.assertEqual(snapshot["segment_ids"], [1, 2])
        self.assertIsNone(stored_segment_id)

    def test_universal_keyboard_compiles_for_both_channels(self):
        keyboard = {"universal": {"inline": True, "rows": [[
            {"label": "Сайт", "type": "link", "value": "https://example.com", "color": "primary"},
            {"label": "Ответ", "type": "callback", "value": "answer", "color": "positive"},
        ]]}}
        vk = router._compile_universal_keyboard(keyboard, "vk")
        telegram = router._compile_universal_keyboard(keyboard, "telegram")
        self.assertEqual(vk["buttons"][0][0]["action"]["type"], "open_link")
        self.assertEqual(vk["buttons"][0][1]["action"]["type"], "callback")
        self.assertEqual(telegram["inline_keyboard"][0][0]["url"], "https://example.com")
        self.assertEqual(telegram["inline_keyboard"][0][1]["callback_data"], "answer")

    def test_continue_requeues_pending_and_skipped_but_not_failed_recipients(self):
        original_db_path = router._db_path
        original_write_lock = router._db_write_lock

        async def run(module_db):
            router._db_path = module_db
            router._db_write_lock = None
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json) VALUES(?,?,?,?,?)",
                    ("campaign", "Тест", "cancelled", "{}", "{}"),
                )
                for recipient_id, status in (
                    ("sent", "sent"), ("failed", "failed"), ("skipped", "skipped"),
                    ("refused", "not_allowed"), ("blocked", "skipped"),
                ):
                    await db.execute(
                        "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) VALUES(?,?,?,?,?,?,?)",
                        ("campaign", "vk", recipient_id, "manual", "{}", "message", status),
                    )
                sent_row = (await (await db.execute(
                    "SELECT id FROM recipients WHERE recipient_id='sent'"
                )).fetchone())[0]
                await db.execute(
                    "INSERT INTO sent_messages(campaign_id,recipient_row_id,channel,recipient_id,external_message_id,sent_at) VALUES(?,?,?,?,?,?)",
                    ("campaign", sent_row, "vk", "sent", "100", router._now()),
                )
                await db.execute("INSERT INTO stop_list(channel,recipient_id) VALUES('vk','blocked')")
                await db.execute(
                    "INSERT INTO not_allow(channel,recipient_id,reason,api_code) VALUES('vk','refused','refused','901')"
                )
                await db.commit()
            count = await router._requeue_unsent_campaign("campaign")
            async with router.aiosqlite.connect(module_db) as db:
                statuses = dict(await (await db.execute(
                    "SELECT recipient_id,status FROM recipients ORDER BY recipient_id"
                )).fetchall())
                campaign = await (await db.execute(
                    "SELECT status,sent,failed,skipped,not_allowed FROM campaigns WHERE id='campaign'"
                )).fetchone()
            return count, statuses, tuple(campaign)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                count, statuses, campaign = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._db_write_lock = original_write_lock

        self.assertEqual(count, 1)
        self.assertEqual(statuses["failed"], "failed")
        self.assertEqual(statuses["skipped"], "pending")
        self.assertEqual(statuses["sent"], "sent")
        self.assertEqual(statuses["refused"], "not_allowed")
        self.assertEqual(statuses["blocked"], "skipped")
        self.assertEqual(campaign, ("queued", 1, 1, 1, 1))

    def test_explicit_retry_can_include_failed_recipients(self):
        original_db_path = router._db_path
        original_write_lock = router._db_write_lock

        async def run(module_db):
            router._db_path = module_db
            router._db_write_lock = None
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json) VALUES(?,?,?,?,?)",
                    ("campaign", "Тест", "failed", "{}", "{}"),
                )
                await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) "
                    "VALUES('campaign','vk','failed','manual','{}','message','failed')"
                )
                await db.commit()
            count = await router._requeue_unsent_campaign("campaign", include_failed=True)
            async with router.aiosqlite.connect(module_db) as db:
                status = (await (await db.execute(
                    "SELECT status FROM recipients WHERE recipient_id='failed'"
                )).fetchone())[0]
            return count, status

        with tempfile.TemporaryDirectory() as tmp:
            try:
                count, status = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._db_write_lock = original_write_lock

        self.assertEqual((count, status), (1, "pending"))

    def test_concurrent_module_writes_are_serialized_without_lock_errors(self):
        original_db_path = router._db_path
        original_write_lock = router._db_write_lock

        async def run(module_db):
            router._db_path = module_db
            router._db_write_lock = None
            await router._init_db()
            async with router._write_db(operation="create lock probe") as db:
                await db.execute("CREATE TABLE lock_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")

            async def write_one(value):
                async with router._write_db(operation="stress probe") as db:
                    await db.execute("INSERT INTO lock_probe(value) VALUES(?)", (str(value),))

            await asyncio.gather(*(write_one(value) for value in range(160)))
            async with router.aiosqlite.connect(module_db) as db:
                return (await (await db.execute("SELECT COUNT(*) FROM lock_probe")).fetchone())[0]

        with tempfile.TemporaryDirectory() as tmp:
            try:
                count = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._db_write_lock = original_write_lock

        self.assertEqual(count, 160)

    def test_delivery_outcomes_are_group_committed_and_update_live_progress(self):
        original_db_path = router._db_path
        original_write_lock = router._db_write_lock
        original_queue = router._delivery_write_queue
        original_flush_task = router._delivery_flush_task
        original_write_db = router._write_db
        original_window = router.DELIVERY_WRITE_BATCH_WINDOW_SEC
        write_transactions = 0

        async def run(module_db):
            nonlocal write_transactions
            router._db_path = module_db
            router._db_write_lock = None
            router._delivery_write_queue = []
            router._delivery_flush_task = None
            router.DELIVERY_WRITE_BATCH_WINDOW_SEC = 0.01
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) "
                    "VALUES('campaign','Тест','running','{}','{}','owner')"
                )
                await db.executemany(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) "
                    "VALUES('campaign','vk',?,'manual','{}','message','sending')",
                    [(str(value),) for value in range(160)],
                )
                await db.commit()
                db.row_factory = router.aiosqlite.Row
                rows = [dict(row) for row in await (await db.execute(
                    "SELECT * FROM recipients ORDER BY id"
                )).fetchall()]

            @router.asynccontextmanager
            async def counted_write_db(*, operation="write"):
                nonlocal write_transactions
                write_transactions += 1
                async with original_write_db(operation=operation) as db:
                    yield db

            router._write_db = counted_write_db
            await asyncio.gather(*(
                router._queue_delivery_outcome({
                    "kind": "sent", "row": row, "attempt": 1,
                    "external_id": f"message-{row['id']}", "response": {"ok": True},
                    "now": router._now(),
                })
                for row in rows
            ))
            async with router.aiosqlite.connect(module_db) as db:
                campaign = tuple(await (await db.execute(
                    "SELECT sent,failed,not_allowed FROM campaigns WHERE id='campaign'"
                )).fetchone())
                recipient_counts = dict(await (await db.execute(
                    "SELECT status,COUNT(*) FROM recipients GROUP BY status"
                )).fetchall())
                sent_messages = (await (await db.execute(
                    "SELECT COUNT(*) FROM sent_messages"
                )).fetchone())[0]
                attempts = (await (await db.execute(
                    "SELECT COUNT(*) FROM delivery_attempts"
                )).fetchone())[0]
            return campaign, recipient_counts, sent_messages, attempts

        with tempfile.TemporaryDirectory() as tmp:
            try:
                result = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._db_write_lock = original_write_lock
                router._delivery_write_queue = original_queue
                router._delivery_flush_task = original_flush_task
                router._write_db = original_write_db
                router.DELIVERY_WRITE_BATCH_WINDOW_SEC = original_window

        campaign, recipient_counts, sent_messages, attempts = result
        self.assertEqual(campaign, (160, 0, 0))
        self.assertEqual(recipient_counts, {"sent": 160})
        self.assertEqual((sent_messages, attempts), (160, 160))
        self.assertEqual(write_transactions, 1)

    def test_claim_sets_progress_baseline_before_first_new_delivery(self):
        original_db_path = router._db_path
        original_write_lock = router._db_write_lock

        async def run(module_db):
            router._db_path = module_db
            router._db_write_lock = None
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,sent) "
                    "VALUES('campaign','Тест','queued','{}','{}',17)"
                )
                await db.commit()
            claimed = await router._claim_campaign("owner")
            async with router.aiosqlite.connect(module_db) as db:
                state = tuple(await (await db.execute(
                    "SELECT status,sent,run_sent_baseline FROM campaigns WHERE id='campaign'"
                )).fetchone())
            return claimed, state

        with tempfile.TemporaryDirectory() as tmp:
            try:
                claimed, state = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._db_write_lock = original_write_lock

        self.assertEqual(claimed, "campaign")
        self.assertEqual(state, ("running", 17, 17))

    def test_refusal_is_persisted_as_not_allow_without_failed_count(self):
        original_db_path = router._db_path
        original_worker_generation = router._worker_generation
        original_send_remote = router._send_remote

        async def refused_send(row, template):
            raise router.NotAllowedDeliveryError("VK 901: can't send", api_code=901)

        async def run(module_db):
            router._db_path = module_db
            router._worker_generation = "owner"
            router._send_remote = refused_send
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) VALUES(?,?,?,?,?,?)",
                    ("campaign", "Тест", "running", "{}", "{}", "owner"),
                )
                await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) VALUES(?,?,?,?,?,?,?)",
                    ("campaign", "vk", "42", "manual", "{}", "message", "sending"),
                )
                await db.commit()
                db.row_factory = router.aiosqlite.Row
                row = dict(await (await db.execute("SELECT * FROM recipients")).fetchone())
            await router._deliver_recipient(
                row, {}, router.DEFAULT_CONFIG, router.AdaptiveRateGate("vk", 1000),
            )
            await router._refresh_counts("campaign")
            async with router.aiosqlite.connect(module_db) as db:
                recipient = await (await db.execute("SELECT status FROM recipients")).fetchone()
                not_allow = await (await db.execute("SELECT channel,recipient_id,api_code FROM not_allow")).fetchone()
                counts = await (await db.execute("SELECT failed,not_allowed FROM campaigns WHERE id='campaign'")).fetchone()
            return recipient[0], tuple(not_allow), tuple(counts)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                status, not_allow, counts = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._worker_generation = original_worker_generation
                router._send_remote = original_send_remote

        self.assertEqual(status, "not_allowed")
        self.assertEqual(not_allow, ("vk", "42", "901"))
        self.assertEqual(counts, (0, 1))

    def test_paused_campaign_returns_claimed_recipient_to_pending(self):
        original_db_path = router._db_path
        original_worker_generation = router._worker_generation
        original_send_remote = router._send_remote
        send_called = False

        async def unexpected_send(row, template):
            nonlocal send_called
            send_called = True
            return "1", {}

        async def run(module_db):
            router._db_path = module_db
            router._worker_generation = "owner"
            router._send_remote = unexpected_send
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) VALUES(?,?,?,?,?,?)",
                    ("campaign", "Тест", "paused", "{}", "{}", None),
                )
                await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) VALUES(?,?,?,?,?,?,?)",
                    ("campaign", "vk", "42", "manual", "{}", "message", "sending"),
                )
                await db.commit()
                db.row_factory = router.aiosqlite.Row
                row = dict(await (await db.execute("SELECT * FROM recipients")).fetchone())
            await router._deliver_recipient(
                row, {}, router.DEFAULT_CONFIG, router.AdaptiveRateGate("vk", 1000),
            )
            async with router.aiosqlite.connect(module_db) as db:
                return (await (await db.execute("SELECT status FROM recipients")).fetchone())[0]

        with tempfile.TemporaryDirectory() as tmp:
            try:
                status = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._worker_generation = original_worker_generation
                router._send_remote = original_send_remote

        self.assertFalse(send_called)
        self.assertEqual(status, "pending")

    def test_vk_and_telegram_campaign_queues_run_independently(self):
        original_db_path = router._db_path
        original_worker_generation = router._worker_generation
        original_module_instance = router._module_instance
        original_get_config = router._get_config
        original_deliver = router._deliver_recipient
        entered = {"vk": asyncio.Event(), "telegram": asyncio.Event()}
        overlapped = set()

        async def config():
            return {**router.DEFAULT_CONFIG, "send_concurrency": 1}

        async def deliver(row, template, config_data, gate, client=None):
            channel = row["channel"]
            other = "telegram" if channel == "vk" else "vk"
            entered[channel].set()
            await asyncio.wait_for(entered[other].wait(), timeout=1.0)
            overlapped.add(channel)
            async with router.aiosqlite.connect(router._must_db()) as db:
                await db.execute(
                    "UPDATE recipients SET status='sent',sent_at=?,updated_at=? WHERE id=?",
                    (router._now(), router._now(), row["id"]),
                )
                await db.commit()

        async def run(module_db):
            router._db_path = module_db
            router._worker_generation = "owner"
            router._module_instance = __import__("sys").modules[router.__name__]
            router._get_config = config
            router._deliver_recipient = deliver
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) VALUES(?,?,?,?,?,?)",
                    ("campaign", "Тест", "running", "{}", "{}", "owner"),
                )
                for channel, recipient_id in (("telegram", "100"), ("vk", "200")):
                    await db.execute(
                        "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content) VALUES(?,?,?,?,?,?)",
                        ("campaign", channel, recipient_id, "manual", "{}", "message"),
                    )
                await db.commit()
            await asyncio.wait_for(router._run_campaign("campaign", "owner"), timeout=5.0)
            async with router.aiosqlite.connect(module_db) as db:
                campaign = await (await db.execute(
                    "SELECT status,sent,failed FROM campaigns WHERE id='campaign'"
                )).fetchone()
                statuses = await (await db.execute(
                    "SELECT channel,status FROM recipients ORDER BY channel"
                )).fetchall()
            return tuple(campaign), [tuple(row) for row in statuses]

        with tempfile.TemporaryDirectory() as tmp:
            try:
                campaign, statuses = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._worker_generation = original_worker_generation
                router._module_instance = original_module_instance
                router._get_config = original_get_config
                router._deliver_recipient = original_deliver

        self.assertEqual(overlapped, {"vk", "telegram"})
        self.assertEqual(campaign, ("completed", 2, 0))
        self.assertEqual(statuses, [("telegram", "sent"), ("vk", "sent")])

    def test_buyers_refresh_maps_getcourse_payments_to_vk_and_salebot_telegram(self):
        original_db_path = router._db_path
        original_customer_path = router._customer_db_path

        async def run(module_db, customer_db):
            router._db_path = module_db
            router._customer_db_path = lambda: customer_db
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO buyer_terms(term,source) VALUES('700','deal_export.csv')"
                )
                await db.commit()
            result = await router._refresh_buyers_cache()
            async with router.aiosqlite.connect(module_db) as db:
                rows = await (await db.execute(
                    "SELECT channel,recipient_id,match_value,source FROM buyers ORDER BY channel,recipient_id"
                )).fetchall()
            return result, [tuple(row) for row in rows]

        with tempfile.TemporaryDirectory() as tmp:
            customer_db = router.Path(tmp) / "customer.db"
            con = sqlite3.connect(customer_db)
            con.executescript(
                """
                CREATE TABLE cdb_getcourse_orders(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT);
                CREATE TABLE cdb_vk_clients(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT);
                CREATE TABLE cdb_telegram_clients(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT);
                """
            )
            orders = [
                ("1", {"payment_state": "paid", "payed_money": "100", "utm_term": "100"}),
                ("2", {"payment_state": "partial", "payed_money": "50", "utm_term": "900"}),
                ("3", {"payment_state": "unpaid", "payed_money": "0", "utm_term": "200"}),
                ("4", {"payment_state": "paid", "payed_money": "100", "utm_term": "999"}),
            ]
            con.executemany(
                "INSERT INTO cdb_getcourse_orders(platform_id,custom_fields,created_at,updated_at) VALUES(?,?,?,?)",
                [(order_id, router._dump(fields), "", "") for order_id, fields in orders],
            )
            con.execute(
                "INSERT INTO cdb_vk_clients(platform_id,custom_fields,created_at,updated_at) VALUES('100','{}','','')"
            )
            con.execute(
                "INSERT INTO cdb_vk_clients(platform_id,custom_fields,created_at,updated_at) VALUES('700','{}','','')"
            )
            con.execute(
                "INSERT INTO cdb_telegram_clients(platform_id,custom_fields,created_at,updated_at) VALUES('300',?,'','')",
                (router._dump({"salebot_id": "900"}),),
            )
            con.commit()
            con.close()
            try:
                result, rows = asyncio.run(run(router.Path(tmp) / "letter.db", customer_db))
            finally:
                router._db_path = original_db_path
                router._customer_db_path = original_customer_path

        self.assertEqual(
            rows,
            [
                ("telegram", "300", "900", "getcourse_orders"),
                ("vk", "100", "100", "getcourse_orders"),
                ("vk", "700", "700", "deal_export.csv"),
            ],
        )
        self.assertEqual(result["qualifying_orders"], 3)
        self.assertEqual(result["live_paid_terms"], 3)
        self.assertEqual(result["imported_paid_terms"], 1)
        self.assertEqual(result["imported_sources"], {"deal_export.csv": 1})
        self.assertEqual(result["paid_terms"], 4)
        self.assertEqual(result["matched_terms"], 3)
        self.assertEqual(result["unmatched_terms"], 1)

    def test_campaign_exclusions_are_optional_and_counted(self):
        original_db_path = router._db_path

        async def run(module_db):
            router._db_path = module_db
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute("INSERT INTO buyers(channel,recipient_id) VALUES('vk','100')")
                await db.execute("INSERT INTO stop_list(channel,recipient_id) VALUES('vk','200')")
                await db.execute("INSERT INTO not_allow(channel,recipient_id) VALUES('telegram','300')")
                await db.commit()
            records = [
                {"channel": "vk", "recipient_id": "100"},
                {"channel": "vk", "recipient_id": "200"},
                {"channel": "telegram", "recipient_id": "300"},
                {"channel": "telegram", "recipient_id": "400"},
            ]
            filtered, excluded = await router._apply_exclusions(
                records, {"buyers": True, "stop_list": True, "not_allow": True}
            )
            unfiltered, none_excluded = await router._apply_exclusions(
                records, {"buyers": False, "stop_list": False, "not_allow": False}
            )
            return filtered, excluded, unfiltered, none_excluded

        with tempfile.TemporaryDirectory() as tmp:
            try:
                filtered, excluded, unfiltered, none_excluded = asyncio.run(
                    run(router.Path(tmp) / "letter.db")
                )
            finally:
                router._db_path = original_db_path

        self.assertEqual([(x["channel"], x["recipient_id"]) for x in filtered], [("telegram", "400")])
        self.assertEqual(excluded, {"buyers": 1, "stop_list": 1, "not_allow": 1, "total": 3})
        self.assertEqual(len(unfiltered), 4)
        self.assertEqual(none_excluded["total"], 0)

    def test_static_campaign_content_is_stored_once_in_snapshot(self):
        original_db_path = router._db_path
        original_require_user = router._require_user
        original_resolve = router._resolve_campaign_records

        records = [
            {
                "table": "vk_clients",
                "id": index,
                "channel": "vk",
                "recipient_id": str(1000 + index),
                "custom_fields": {"large": "x" * 1000},
            }
            for index in range(7)
        ]

        async def require_user(request, **kwargs):
            return {"username": "tester", "role": "admin"}

        async def resolve(data):
            return records, {"segment_ids": [1], "segment_names": ["test"]}, 1

        async def run(module_db):
            router._db_path = module_db
            router._require_user = require_user
            router._resolve_campaign_records = resolve
            await router._init_db()
            result = await router.create_campaign(
                router.CampaignIn(
                    name="Static",
                    content="Один общий текст",
                    channels=["vk"],
                    exclude_buyers=False,
                    exclude_stop_list=False,
                    exclude_not_allow=False,
                ),
                object(),
            )
            async with router.aiosqlite.connect(module_db) as db:
                campaign = await (await db.execute(
                    "SELECT status,total,template_snapshot_json,audience_snapshot_json FROM campaigns WHERE id=?",
                    (result["id"],),
                )).fetchone()
                compact = await (await db.execute(
                    "SELECT COUNT(*),MIN(source_json),MAX(source_json),MIN(rendered_content),MAX(rendered_content) "
                    "FROM recipients WHERE campaign_id=?",
                    (result["id"],),
                )).fetchone()
            return result, tuple(campaign), tuple(compact)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                result, campaign, compact = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._require_user = original_require_user
                router._resolve_campaign_records = original_resolve

        self.assertEqual(result["total"], 7)
        self.assertEqual(campaign[:2], ("queued", 7))
        self.assertEqual(router._loads(campaign[2], {})["content"], "Один общий текст")
        self.assertFalse(router._loads(campaign[3], {})["personalized"])
        self.assertEqual(compact, (7, "{}", "{}", "", ""))

    def test_empty_compact_recipient_uses_snapshot_content_for_delivery(self):
        original_db_path = router._db_path
        original_worker_generation = router._worker_generation
        original_send_remote = router._send_remote
        delivered = []

        async def send(row, template):
            delivered.append(row["rendered_content"])
            return "777", {"ok": True}

        async def run(module_db):
            router._db_path = module_db
            router._worker_generation = "owner"
            router._send_remote = send
            await router._init_db()
            template = {"content": "Текст из snapshot"}
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) VALUES(?,?,?,?,?,?)",
                    ("campaign", "Тест", "running", router._dump(template), "{}", "owner"),
                )
                await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) VALUES(?,?,?,?,?,?,?)",
                    ("campaign", "vk", "42", "manual", "{}", "", "sending"),
                )
                await db.commit()
                db.row_factory = router.aiosqlite.Row
                row = dict(await (await db.execute("SELECT * FROM recipients")).fetchone())
            await router._deliver_recipient(
                row, template, router.DEFAULT_CONFIG, router.AdaptiveRateGate("vk", 1000),
            )

        with tempfile.TemporaryDirectory() as tmp:
            try:
                asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._worker_generation = original_worker_generation
                router._send_remote = original_send_remote

        self.assertEqual(delivered, ["Текст из snapshot"])

    def test_not_allow_can_be_rechecked_when_campaign_exclusion_is_disabled(self):
        original_db_path = router._db_path
        original_worker_generation = router._worker_generation
        original_send_remote = router._send_remote

        async def send(row, template):
            return "777", {"ok": True}

        async def run(module_db):
            router._db_path = module_db
            router._worker_generation = "owner"
            router._send_remote = send
            await router._init_db()
            snapshot = {"exclusions": {"buyers": False, "stop_list": False, "not_allow": False}}
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json,lease_owner) VALUES(?,?,?,?,?,?)",
                    ("campaign", "Тест", "running", "{}", router._dump(snapshot), "owner"),
                )
                await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,rendered_content,status) VALUES(?,?,?,?,?,?,?)",
                    ("campaign", "vk", "42", "manual", "{}", "message", "sending"),
                )
                await db.execute(
                    "INSERT INTO not_allow(channel,recipient_id,reason) VALUES('vk','42','old refusal')"
                )
                await db.commit()
                db.row_factory = router.aiosqlite.Row
                row = dict(await (await db.execute("SELECT * FROM recipients")).fetchone())
            await router._deliver_recipient(
                row, {}, router.DEFAULT_CONFIG, router.AdaptiveRateGate("vk", 1000),
            )
            async with router.aiosqlite.connect(module_db) as db:
                recipient = tuple(await (await db.execute(
                    "SELECT status,external_message_id FROM recipients WHERE recipient_id='42'"
                )).fetchone())
                remaining_not_allow = (await (await db.execute(
                    "SELECT COUNT(*) FROM not_allow WHERE channel='vk' AND recipient_id='42'"
                )).fetchone())[0]
                return recipient, remaining_not_allow

        with tempfile.TemporaryDirectory() as tmp:
            try:
                result, remaining_not_allow = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._worker_generation = original_worker_generation
                router._send_remote = original_send_remote

        self.assertEqual(result, ("sent", "777"))
        self.assertEqual(remaining_not_allow, 0)

    def test_bookkeeping_lock_returns_only_unsent_claim_to_pending(self):
        original_db_path = router._db_path
        original_deliver = router._deliver_recipient

        async def explode(*args, **kwargs):
            raise router.aiosqlite.OperationalError("database is locked")

        async def run(module_db):
            router._db_path = module_db
            await router._init_db()
            async with router.aiosqlite.connect(module_db) as db:
                await db.execute(
                    "INSERT INTO campaigns(id,name,status,template_snapshot_json,audience_snapshot_json) "
                    "VALUES('campaign','Тест','running','{}','{}')"
                )
                cur = await db.execute(
                    "INSERT INTO recipients(campaign_id,channel,recipient_id,source_table,source_json,"
                    "rendered_content,status) VALUES('campaign','vk','42','manual','{}','message','sending')"
                )
                await db.commit()
                row_id = cur.lastrowid
                db.row_factory = router.aiosqlite.Row
                row = dict(await (await db.execute(
                    "SELECT * FROM recipients WHERE id=?", (row_id,)
                )).fetchone())
            router._deliver_recipient = explode
            await router._deliver_recipient_safe(
                row, {}, router.DEFAULT_CONFIG, router.AdaptiveRateGate("vk", 10),
            )
            async with router.aiosqlite.connect(module_db) as db:
                return tuple(await (await db.execute(
                    "SELECT status,last_error FROM recipients WHERE id=?", (row_id,)
                )).fetchone())

        with tempfile.TemporaryDirectory() as tmp:
            try:
                status, error = asyncio.run(run(router.Path(tmp) / "letter.db"))
            finally:
                router._db_path = original_db_path
                router._deliver_recipient = original_deliver

        self.assertEqual(status, "pending")
        self.assertIn("database is locked", error)


if __name__ == "__main__":
    unittest.main()
