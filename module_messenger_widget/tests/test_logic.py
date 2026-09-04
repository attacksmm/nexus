import asyncio
import gc
import json
import os
import sqlite3
import shutil
import subprocess
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import router


class GetCourseWazzupLogicTests(unittest.TestCase):
    def test_resolved_widget_context_exposes_contact_fields_to_email_channel(self):
        class Index:
            def resolve(self, context):
                self_assert.assertEqual(context["entity_id"], "17696535")
                return {
                    "status": "resolved",
                    "entity_id": 912,
                    "accounts": [],
                    "variables": {
                        "contact.email": {"value": "Ovrklhdr@Gmail.com"},
                        "contact.phone": {"value": "8 (900) 999-22-33"},
                        "contact.name": {"value": "Никита Попов"},
                    },
                    "conflicts": [],
                }

        async def no_rules(_context):
            return None

        self_assert = self
        previous = router._identity_index, router._apply_identity_rules
        router._identity_index, router._apply_identity_rules = Index(), no_rules
        try:
            result = asyncio.run(router._resolve_widget_context(
                {"entity_type": "lead", "entity_id": "17696535"},
                "amocrm", {"admin_name": "Татьяна"},
            ))
        finally:
            router._identity_index, router._apply_identity_rules = previous
        self.assertEqual(result["platform"], "amocrm")
        self.assertEqual(result["entity_type"], "lead")
        self.assertEqual(result["entity_id"], "17696535")
        self.assertEqual(result["identity_entity_id"], 912)
        self.assertEqual(result["email"], "ovrklhdr@gmail.com")
        self.assertEqual(result["phone"], "+79009992233")
        self.assertEqual(result["name"], "Никита Попов")

    def test_getcourse_widget_opens_composer_before_history_finishes(self):
        module_dir = Path(__file__).resolve().parents[1]
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        amo = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn('history_status: "loading"', widget)
        self.assertIn('fetchConversation(channel, feed, true, 0, { timeoutMs: 8000 })', widget)
        self.assertIn("CHANNEL_STORAGE_KEY", widget)
        self.assertIn('setState("Подключаем каналы"', widget)
        self.assertIn("if (error.retryable)", widget)
        self.assertNotIn('setState("Повторяем загрузку"', widget)
        self.assertIn("requestWithRetry('/conversation'", amo)
        self.assertIn("Повторяем загрузку", amo)

    def test_email_staff_service_returns_only_active_managers(self):
        previous_db = router._db_path
        try:
            with tempfile.TemporaryDirectory() as directory:
                router._db_path = Path(directory) / "messenger-widget.db"
                with sqlite3.connect(router._db_path) as db:
                    db.execute("CREATE TABLE admins(id INTEGER PRIMARY KEY,name TEXT,enabled INTEGER)")
                    db.executemany(
                        "INSERT INTO admins(id,name,enabled) VALUES(?,?,?)",
                        [(2, "Татьяна Истратова", 1), (1, "Анна", 1), (3, "Скрытый", 0)],
                    )
                self.assertEqual(asyncio.run(router.service_email_staff()), [
                    {"id": "1", "name": "Анна"},
                    {"id": "2", "name": "Татьяна Истратова"},
                ])
        finally:
            router._db_path = previous_db

    def test_salebot_image_uses_native_image_attachment_type(self):
        captured = {}

        class Response:
            status_code = 200

            @staticmethod
            def json():
                return {"status": "success"}

        class Client:
            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, url, json):
                captured.update({"url": url, "body": json})
                return Response()

        with patch.dict(os.environ, {"SALEBOT_API_KEY": "secret"}, clear=False), patch.object(
            router.httpx, "AsyncClient", Client,
        ):
            asyncio.run(router._salebot_send(
                "771116046", "", "https://cdn.example.test/photo.png", "image/png",
            ))

        self.assertEqual(captured["body"], {
            "client_id": "771116046",
            "message": "\u2060",
            "attachment_url": "https://cdn.example.test/photo.png",
            "attachment_type": "image",
        })

    def test_widget_image_type_accepts_only_supported_raster_images(self):
        self.assertEqual(router._widget_image_type(b"\x89PNG\r\n\x1a\nrest"), (".png", "image/png"))
        self.assertEqual(router._widget_image_type(b"\xff\xd8\xffrest"), (".jpg", "image/jpeg"))
        self.assertEqual(router._widget_image_type(b"GIF89arest"), (".gif", "image/gif"))
        self.assertEqual(router._widget_image_type(b"RIFF\x00\x00\x00\x00WEBPrest"), (".webp", "image/webp"))
        with self.assertRaises(router.HTTPException):
            router._widget_image_type(b"<svg></svg>")

    def test_salutation_guard_blocks_name_from_another_client(self):
        self.assertEqual(
            router._salutation_name_mismatch("Здравствуйте, Екатерина! Чем помочь?", "Ирина Скуратова"),
            ("Екатерина", "Ирина"),
        )
        self.assertIsNone(router._salutation_name_mismatch("Здравствуйте, Ирина!", "Ирина Скуратова"))
        self.assertIsNone(router._salutation_name_mismatch("Расскажу подробнее о курсе", "Ирина Скуратова"))

    def test_salutation_guard_reads_the_contact_name_value_not_variable_metadata(self):
        resolved = {"variables": {
            "contact.name": {"value": "Галина Бутенко", "source": "current"},
        }}
        expected = router._resolved_contact_name(resolved, {"name": "Имя из карточки"})
        self.assertEqual(expected, "Галина Бутенко")
        self.assertIsNone(router._salutation_name_mismatch("Доброе утро, Галина!", expected))

    def test_salutation_guard_does_not_stringify_malformed_name_objects(self):
        self.assertEqual(
            router._resolved_contact_name({"variables": {}}, {"name": {"unexpected": "object"}}),
            "",
        )

    def test_delivery_retry_honors_telegram_flood_wait(self):
        error = router.HTTPException(502, "Telegram: A wait of 37 seconds is required")
        self.assertTrue(router._delivery_error_is_transient(error))
        self.assertEqual(router._delivery_retry_delay(error, 1), 39)

    def test_delivery_retry_treats_provider_upload_502_as_transient(self):
        request = router.httpx.Request("POST", "https://upload.example.test/image")
        response = router.httpx.Response(502, request=request)
        error = router.httpx.HTTPStatusError("502 Bad Gateway", request=request, response=response)
        self.assertTrue(router._delivery_error_is_transient(error))

    def test_vk_image_upload_refreshes_incomplete_upload_response_once(self):
        class Response:
            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        class Client:
            responses = iter((
                {"server": 1, "hash": "first-without-photo"},
                {"server": 2, "photo": "[{\"photo\":\"ok\"}]", "hash": "second"},
            ))

            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def post(self, *_args, **_kwargs):
                return Response(next(self.responses))

        with tempfile.TemporaryDirectory() as temp_dir:
            image = Path(temp_dir) / "test.png"
            image.write_bytes(b"\x89PNG\r\n\x1a\nimage")
            vk = AsyncMock(side_effect=(
                {"upload_url": "https://upload.example.test/one"},
                {"upload_url": "https://upload.example.test/two"},
                [{"owner_id": -225075265, "id": 42, "access_key": "key"}],
            ))
            with (
                patch.object(router, "_widget_media_path", AsyncMock(return_value=(image, "image/png"))),
                patch.object(router, "_vk_request", vk),
                patch.object(router.httpx, "AsyncClient", Client),
                patch.object(router.asyncio, "sleep", AsyncMock()) as sleep,
            ):
                result = asyncio.run(router._vk_upload_widget_image("1105209997", "https://example.test/image.png"))

        self.assertEqual(result, "photo-225075265_42_key")
        self.assertEqual(vk.await_count, 3)
        self.assertEqual(vk.await_args_list[0].args[1]["group_id"], router._vk_group_id())
        sleep.assert_awaited_once_with(0.2)

    def test_auto_markup_completes_allow_listed_urls_without_replacing_existing_values(self):
        domains = "club.sobakovod.pro;sobakovod.pro;start.bizon365.ru"
        tail = "?utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}"
        text = (
            "https://club.sobakovod.pro/lesson "
            "https://sobakovod.pro/a?foo=1#part "
            "https://start.bizon365.ru/room/7?utm_source=vk "
            "https://example.org/nope."
        )
        result = router._apply_auto_markup(text, domains, tail)
        self.assertIn("https://club.sobakovod.pro/lesson?utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}", result)
        self.assertIn("https://sobakovod.pro/a?foo=1&utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}#part", result)
        self.assertEqual(
            router._apply_auto_markup("https://sobakovod.pro/dog#program", domains, tail),
            "https://sobakovod.pro/dog?utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}#program",
        )
        self.assertIn(
            "https://start.bizon365.ru/room/7?utm_source=vk&utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}",
            result,
        )
        self.assertIn("https://example.org/nope.", result)
        self.assertEqual(result.count("utm_term={{utm.term}}"), 3)

    def test_auto_markup_adds_every_missing_configured_parameter_before_fragment(self):
        tail = (
            "?utm_term={{utm.term}}&utm_source={{utm.source}}&utm_medium={{utm.medium}}"
            "&utm_campaign={{utm.campaign}}&utm_content={{utm.content}}"
            "&param1={{ym_uid}}&param2={{conversation_id}}"
        )
        result = router._apply_auto_markup(
            "https://sobakovod.pro/dog?utm_source=1&utm_medium=2#program",
            "sobakovod.pro",
            tail,
        )
        self.assertEqual(
            result,
            "https://sobakovod.pro/dog?utm_source=1&utm_medium=2"
            "&utm_term={{utm.term}}&utm_campaign={{utm.campaign}}"
            "&utm_content={{utm.content}}&param1={{ym_uid}}"
            "&param2={{conversation_id}}#program",
        )
        self.assertEqual(result.count("utm_source="), 1)
        self.assertEqual(result.count("utm_medium="), 1)

    def test_auto_markup_accepts_ampersand_tail_and_rejects_invalid_tail(self):
        self.assertEqual(
            router._apply_auto_markup("https://club.sobakovod.pro/a", "club.sobakovod.pro", "&utm_source=x"),
            "https://club.sobakovod.pro/a?utm_source=x",
        )
        with self.assertRaises(router.HTTPException):
            router._auto_markup_tail("?utm_source=x#fragment")

    def test_send_auto_markup_renders_only_the_configured_tail_variables(self):
        async def setting(key):
            return {
                "auto_markup_domains": "sobakovod.pro",
                "auto_markup_tail": "?utm_term={{utm.term}}&param1={{ym_uid}}&param2={{conversation_id}}",
            }[key]

        original = router._setting
        router._setting = setting
        try:
            result = asyncio.run(router._auto_markup_for_send(
                "https://sobakovod.pro/course_tour",
                {"utm.term": {"value": "152867794"}, "ym_uid": {"value": "1786735599256964564"}},
            ))
        finally:
            router._setting = original
        self.assertEqual(
            result,
            "https://sobakovod.pro/course_tour?utm_term=152867794&param1=1786735599256964564&param2=",
        )

    def test_send_auto_markup_completes_partial_tail_with_rendered_values(self):
        async def setting(key):
            return {
                "auto_markup_domains": "sobakovod.pro",
                "auto_markup_tail": router.AUTO_MARKUP_DEFAULT_TAIL,
            }[key]

        original = router._setting
        router._setting = setting
        try:
            result = asyncio.run(router._auto_markup_for_send(
                "https://sobakovod.pro/course_tour?utm_source=1&utm_medium=2#program",
                {
                    "utm.term": {"value": "term"},
                    "utm.source": {"value": "source-from-card"},
                    "utm.medium": {"value": "medium-from-card"},
                    "utm.campaign": {"value": "campaign"},
                    "utm.content": {"value": "content"},
                    "ym_uid": {"value": "ym-1"},
                    "conversation_id": {"value": "conversation-1"},
                },
            ))
        finally:
            router._setting = original
        self.assertEqual(
            result,
            "https://sobakovod.pro/course_tour?utm_source=1&utm_medium=2"
            "&utm_term=term&utm_campaign=campaign&utm_content=content"
            "&param1=ym-1&param2=conversation-1#program",
        )

    def test_channels_are_prioritized_by_delivery_confidence(self):
        channels = [
            {"provider": "vk", "label": "VK недоступен", "can_send": False, "has_chat": False},
            {"provider": "wazzup", "label": "MAX", "can_send": True, "has_chat": False},
            {"provider": "email", "label": "Email", "can_send": True, "has_chat": False},
            {"provider": "email", "label": "Email в очереди", "can_send": True, "has_chat": True, "confirmed_chat": False},
            {"provider": "vk", "label": "VK подтверждён", "can_send": True, "has_chat": True},
            {"provider": "telegram-personal", "label": "TG · найти по номеру", "can_send": True, "has_chat": False},
        ]
        ordered = router._prioritize_channels(channels)
        self.assertEqual(
            [channel["label"] for channel in ordered],
            ["VK подтверждён", "Email", "Email в очереди", "MAX", "TG · найти по номеру", "VK недоступен"],
        )

    def test_vk_callback_handles_disconnected_request_body(self):
        class Request:
            async def body(self):
                raise router.ClientDisconnect()

        previous = dict(router._vk_callback_config)
        router._vk_callback_config["key"] = "callback-key"
        try:
            response = asyncio.run(router.vk_callback("callback-key", Request()))
        finally:
            router._vk_callback_config.update(previous)
        self.assertEqual(response.status_code, 400)

    def test_telegram_session_is_copied_to_module_private_storage(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_dir = root / "modules" / "messenger-widget" / "data"
            shared = root / "modules" / "course-chat-creator" / "data" / "telegram.session"
            data_dir.mkdir(parents=True)
            shared.parent.mkdir(parents=True)
            with sqlite3.connect(shared) as db:
                db.execute("CREATE TABLE sessions(id INTEGER PRIMARY KEY, value TEXT)")
                db.execute("INSERT INTO sessions(value) VALUES('auth')")

            previous_db = router._db_path
            previous_reader = router._read_env_values
            previous_env = {
                key: os.environ.pop(key, None)
                for key in (
                    router.TELEGRAM_SESSION_ENV_KEY,
                    router.LEGACY_TELEGRAM_SESSION_ENV_KEY,
                    "TELEGRAM_SESSION_FILE",
                )
            }
            router._db_path = data_dir / "messenger-widget.db"
            router._read_env_values = lambda: {}
            try:
                private = router._telegram_session_file()
                self.assertEqual(private, data_dir / "telegram-personal.session")
                self.assertTrue(private.is_file())
                with sqlite3.connect(f"file:{private.as_posix()}?mode=ro", uri=True) as db:
                    self.assertEqual(db.execute("SELECT value FROM sessions").fetchone()[0], "auth")
                self.assertEqual(router._telegram_session_file(), private)
            finally:
                router._db_path = previous_db
                router._read_env_values = previous_reader
                for key, value in previous_env.items():
                    if value is not None:
                        os.environ[key] = value

    def test_vk_transfer_reuses_deterministic_random_id(self):
        calls = []

        async def send(method, payload):
            self.assertEqual(method, "messages.send")
            calls.append(payload)
            return 991

        previous = router._vk_request
        router._vk_request = send
        try:
            first = asyncio.run(router.service_send_transfer_message(
                provider="vk", recipient_id="123", content="Проверка", operation_id="order-stage-chunk",
            ))
            second = asyncio.run(router.service_send_transfer_message(
                provider="vk", recipient_id="123", content="Проверка", operation_id="order-stage-chunk",
            ))
        finally:
            router._vk_request = previous
        self.assertTrue(first["ok"] and second["ok"])
        self.assertEqual(calls[0]["random_id"], calls[1]["random_id"])
        self.assertGreater(calls[0]["random_id"], 0)

    def test_onboarding_target_prefers_direct_telegram_and_falls_back_to_vk(self):
        async def telegram_ready(**_kwargs):
            return {"ok": True, "platform_id": "5601500901", "source": "salebot_id"}

        async def telegram_missing(**_kwargs):
            return {"ok": False, "status": "not_found", "error": "missing"}

        async def vk_ready(**_kwargs):
            return {"ok": True, "status": "ready", "provider": "vk", "recipient_id": "1105209997"}

        original_tg = router.service_resolve_onboarding_telegram_target
        original_fallback = router.service_transfer_delivery_target
        try:
            router.service_resolve_onboarding_telegram_target = telegram_ready
            router.service_transfer_delivery_target = vk_ready
            result = asyncio.run(router.service_resolve_onboarding_target(utm_term="salebot_id=1"))
            self.assertEqual((result["provider"], result["recipient_id"]), ("telegram", "5601500901"))

            router.service_resolve_onboarding_telegram_target = telegram_missing
            result = asyncio.run(router.service_resolve_onboarding_target(utm_term="platform_id=1105209997"))
            self.assertEqual((result["provider"], result["recipient_id"]), ("vk", "1105209997"))
        finally:
            router.service_resolve_onboarding_telegram_target = original_tg
            router.service_transfer_delivery_target = original_fallback

    def test_onboarding_targets_keep_vk_when_telegram_is_unavailable(self):
        async def telegram_missing(**_kwargs):
            return {"ok": False, "status": "not_found", "error": "telegram missing"}

        async def recipients(**_kwargs):
            return {"ok": True, "telegram": "", "vk": "1105209997"}

        async def fallback(**_kwargs):
            return {"ok": False, "status": "not_found"}

        async def vk(method, params):
            self.assertEqual(method, "messages.isMessagesFromGroupAllowed")
            self.assertEqual(params["user_id"], "1105209997")
            return {"is_allowed": 1}

        originals = (
            router.service_resolve_onboarding_telegram_target,
            router.service_transfer_recipients,
            router.service_transfer_delivery_target,
            router._vk_request,
        )
        try:
            router.service_resolve_onboarding_telegram_target = telegram_missing
            router.service_transfer_recipients = recipients
            router.service_transfer_delivery_target = fallback
            router._vk_request = vk
            result = asyncio.run(router.service_resolve_onboarding_targets(
                utm_term="", email="student@example.test", phone="+79990000000",
            ))
        finally:
            (
                router.service_resolve_onboarding_telegram_target,
                router.service_transfer_recipients,
                router.service_transfer_delivery_target,
                router._vk_request,
            ) = originals
        self.assertTrue(result["ok"])
        self.assertEqual(result["candidates"], [{
            "provider": "vk", "recipient_id": "1105209997", "source": "identity",
        }])

    def test_transfer_delivery_uses_verified_utm_recipient(self):
        class Index:
            def provider_id_for_exact_context(self, provider, _context):
                return "268030521" if provider == "vk" else ""

        async def allowed(method, params):
            self.assertEqual(method, "messages.isMessagesFromGroupAllowed")
            self.assertEqual(params["user_id"], "268030521")
            return {"is_allowed": 1}

        previous_index, previous_request = router._identity_index, router._vk_request
        router._identity_index, router._vk_request = Index(), allowed
        try:
            result = asyncio.run(router.service_transfer_delivery_target(
                email="student@example.com", gc_user_id="1", phone="+79991112233",
                utm_term="platform_id=268030521",
            ))
        finally:
            router._identity_index, router._vk_request = previous_index, previous_request
        self.assertTrue(result["ok"])
        self.assertEqual((result["provider"], result["recipient_id"]), ("vk", "268030521"))

    def test_transfer_delivery_resolves_verified_vk_page_url(self):
        class Index:
            def provider_id_for_exact_context(self, provider, context):
                if provider == "vk" and context.get("fields", {}).get("vk_platform_id") == "1105209997":
                    return "1105209997"
                return ""

        async def peer_id(reference):
            self.assertEqual(reference, "https://vk.ru/tehpod_sobakovodpro")
            return "1105209997"

        async def allowed(method, params):
            self.assertEqual(method, "messages.isMessagesFromGroupAllowed")
            self.assertEqual(params["user_id"], "1105209997")
            return {"is_allowed": 1}

        previous = router._identity_index, router._vk_peer_id, router._vk_request
        router._identity_index, router._vk_peer_id, router._vk_request = Index(), peer_id, allowed
        try:
            result = asyncio.run(router.service_transfer_delivery_target(
                email="student@example.com", gc_user_id="1", phone="+79991112233",
                utm_term="https://vk.ru/tehpod_sobakovodpro",
            ))
        finally:
            router._identity_index, router._vk_peer_id, router._vk_request = previous
        self.assertTrue(result["ok"])
        self.assertEqual((result["provider"], result["recipient_id"]), ("vk", "1105209997"))

    def test_normalizes_russian_and_international_phones(self):
        self.assertEqual(router._normalize_phone("8 (911) 447-40-13"), "+79114474013")
        self.assertEqual(router._normalize_phone("911 447 40 13"), "+79114474013")
        self.assertEqual(router._normalize_phone("+49 151 23456789"), "+4915123456789")
        self.assertEqual(router._normalize_phone("123"), "")

    def test_masks_phone_for_audit(self):
        self.assertEqual(router._mask_phone("+7 911 447-40-13"), "+79*****4013")
        self.assertNotIn("11447", router._mask_phone("+7 911 447-40-13"))

    def test_recognizes_only_supported_card_paths(self):
        self.assertEqual(
            router._page_context("https://club.sobakovod.pro/user/control/user/update/id/394523316"),
            ("user", "394523316"),
        )
        self.assertEqual(
            router._page_context("https://club.sobakovod.pro/sales/control/deal/update/id/551?from=list"),
            ("order", "551"),
        )
        self.assertEqual(router._page_context("https://club.sobakovod.pro/user/control/user/list"), ("", ""))

    def test_activation_code_is_unambiguous(self):
        code = router._activation_code()
        self.assertRegex(code, r"^[2-9A-HJ-NP-Z]{4}-[2-9A-HJ-NP-Z]{4}-[2-9A-HJ-NP-Z]{4}$")
        self.assertEqual(len(router._normalize_code(code)), 12)

    def test_getcourse_origins_include_custom_and_account_domains(self):
        env = {
            "NEXUS_MESSENGER_WIDGET_GETCOURSE_ORIGIN": "https://club.sobakovod.pro/",
            "GETCOURSE_ACCOUNT_NAME": "sobakovod",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertEqual(router._getcourse_origins(), {
                "https://club.sobakovod.pro",
                "https://sobakovod.getcourse.ru",
            })

    def test_getcourse_origin_rejects_unsafe_account_name(self):
        env = {
            "NEXUS_MESSENGER_WIDGET_GETCOURSE_ORIGIN": "https://club.sobakovod.pro",
            "GETCOURSE_ACCOUNT_NAME": "sobakovod.example.com",
        }
        with patch.dict(os.environ, env, clear=False):
            self.assertEqual(router._getcourse_origins(), {"https://club.sobakovod.pro"})

    def test_utm_term_supports_platform_and_salebot_ids(self):
        self.assertEqual(router._graph.parse_utm_term("platform_id=abc-123"), [("vk_platform", "abc-123")])
        self.assertEqual(router._graph.parse_utm_term("salebot_id:998877"), [("salebot", "998877")])
        self.assertEqual(router._graph.parse_utm_term("998877"), [("candidate", "998877")])

    def test_salebot_history_result_normalizes_directions_and_attachment(self):
        messages = router._salebot_messages({"result": [
            {"id": 1, "client_replica": True, "message_from_outside": 0, "text": "Вопрос"},
            {"id": 2, "client_replica": False, "message_from_outside": 0, "text": "Ответ", "attachment_url": "https://example.test/file.pdf"},
            {"id": 3, "client_replica": False, "message_from_outside": 3, "text": "callback_amoCRM"},
            {"id": 4, "client_replica": False, "message_from_outside": 2, "text": "Комментарий CRM"},
            {"id": 5, "client_replica": True, "message_from_outside": 0, "answered": False, "text": "instagram"},
        ]})
        self.assertEqual([row["direction"] for row in messages], ["incoming", "outgoing"])
        self.assertEqual(messages[1]["attachments"][0]["content_uri"], "https://example.test/file.pdf")

    def test_salebot_telegram_attachment_is_proxied_without_exposing_bot_token(self):
        previous = router._salebot_key
        router._salebot_key = lambda: "test-signing-secret"
        try:
            messages = router._salebot_messages({"result": [{
                "id": 7, "client_replica": True, "message_from_outside": 0, "text": "",
                "attachments": ["https://api.telegram.org/file/botSECRET/photos/client.jpg"],
            }]}, "99001")
            url = messages[0]["attachments"][0]["content_uri"]
            self.assertTrue(url.startswith(router.PUBLIC_API_BASE + "/streams/salebot-attachment/"))
            self.assertNotIn("SECRET", url)
            self.assertEqual(messages[0]["attachments"][0]["content_type"], "image")
            token = url.rsplit("/", 1)[-1]
            self.assertEqual(router._salebot_attachment_claims(token), ("99001", "7", 0))
        finally:
            router._salebot_key = previous

    def test_salebot_external_attachment_is_proxied_for_streams_and_private_hosts_are_rejected(self):
        previous = router._salebot_key
        router._salebot_key = lambda: "test-signing-secret"
        try:
            messages = router._salebot_messages({"result": [{
                "id": 8, "client_replica": False, "message_from_outside": 0, "text": "",
                "attachments": [{
                    "attachment_url": "https://cdn.example.test/photos/client.jpg",
                    "attachment_type": "image",
                }],
            }]}, "99001")
            url = messages[0]["attachments"][0]["content_uri"]
            self.assertTrue(url.startswith(router.PUBLIC_API_BASE + "/streams/salebot-attachment/"))
            self.assertNotIn("cdn.example.test", url)
            self.assertEqual(router._salebot_remote_attachment_url("https://127.0.0.1/private.jpg"), "")
            self.assertEqual(router._salebot_remote_attachment_url("https://localhost/private.jpg"), "")
            self.assertEqual(router._salebot_remote_attachment_url("https://user:pass@example.test/a.jpg"), "")
            self.assertEqual(
                router._salebot_remote_attachment_url("https://cdn.example.test/a.jpg#secret"),
                "https://cdn.example.test/a.jpg",
            )
        finally:
            router._salebot_key = previous

    def test_salebot_attachment_fetch_uses_shared_telegram_proxy(self):
        source = Path(router.__file__).read_text(encoding="utf-8")
        block = source.split("async def streams_salebot_attachment", 1)[1].split("@router", 1)[0]
        self.assertIn("httpx_client_kwargs", block)
        self.assertIn("except httpx.HTTPError", block)
        self.assertIn("_salebot_attachment_claims(token)", block)
        self.assertIn("_require_public_attachment_host", block)
        self.assertIn("SALEBOT_SAFE_MEDIA_TYPES", block)
        self.assertIn('"trust_env": False', block)
        self.assertNotIn("verify_token_from_request(request)", block)

    def test_streams_recipient_does_not_use_graph_fallback_for_vk(self):
        class Index:
            def provider_id_for_exact_context(self, provider, _context):
                return {"salebot": "99001"}.get(provider, "")

            def platform_id_for_context(self, provider, _context):
                return {"telegram": "778899", "vk": "unrelated-vk"}.get(provider, "")

            def telegram_username_for_platform_id(self, platform_id):
                self_assert.assertEqual(platform_id, "778899")
                return "exact_user"

        self_assert = self
        previous = router._identity_index
        router._identity_index = Index()
        try:
            result = asyncio.run(router.service_transfer_recipients(
                email="exact@example.test", gc_user_id="123", phone="+79990001122",
            ))
        finally:
            router._identity_index = previous
        self.assertEqual(result["telegram"], "778899")
        self.assertEqual(result["vk"], "")
        self.assertEqual(result["salebot"], "99001")

    def test_salebot_history_keeps_voice_placeholder_without_public_url(self):
        messages = router._salebot_messages({"result": [{
            "id": 1, "client_replica": True, "message_from_outside": 0, "text": "",
            "attachments": [{"attachment_url": "", "attachment_type": "audio"}],
        }]})
        self.assertEqual(messages[0]["attachments"], [{
            "content_uri": "", "content_type": "audio", "filename": "", "unavailable": True,
        }])

    def test_template_variables_include_exact_contact_name(self):
        variables = router.build_context_variables([], {
            "platform": "amocrm", "entity_type": "contact", "entity_id": "42",
            "name": "Анна Петрова", "phone": "+79990001122", "manager_name": "Евгений",
            "fields": {"utm_source": "vk", "utm_term": "platform_id=abc"},
        }, {"source": "vk", "term": "platform_id=abc"})
        self.assertEqual(variables["contact.name"]["value"], "Анна Петрова")
        self.assertEqual(variables["manager.name"]["value"], "Евгений")
        self.assertEqual(variables["utm.source"]["value"], "vk")
        rendered = router.render_message_template("Здравствуйте, {{contact.name}}! {{missing.value}}", variables)
        self.assertEqual(rendered["text"], "Здравствуйте, Анна Петрова! ")
        self.assertEqual(rendered["missing"], ["missing.value"])

    def test_extracts_wazzup_users_from_both_response_shapes(self):
        expected = [{"id": "u-1", "name": "Анна"}]
        self.assertEqual(router._users_from_response([{"id": "u-1", "name": "Анна"}]), expected)
        self.assertEqual(router._users_from_response({"data": [{"id": "u-1", "name": "Анна"}]}), expected)
        self.assertEqual(router._users_from_response({"unexpected": []}), [])

    def test_uses_only_active_supported_wazzup_transports(self):
        channels = router._active_chat_channels([
            {"channelId": "wa-old", "transport": "whatsapp", "state": "blocked", "name": "Старый WhatsApp"},
            {"channelId": "max-1", "transport": "max", "state": "active", "name": "Служба заботы", "plainId": "79990000000"},
            {"channelId": "max-1", "transport": "max", "state": "active", "name": "Дубликат"},
            {"channelId": "email-1", "transport": "email", "state": "active", "name": "Не чат Wazzup"},
        ])
        self.assertEqual(channels, [{
            "channel_id": "max-1", "provider": "wazzup", "transport": "max",
            "channel_transport": "max", "name": "Служба заботы", "plain_id": "79990000000",
            "label": "MAX · Служба заботы",
        }])

    def test_maps_wazzup_channel_transports_to_chat_types(self):
        channels = router._active_chat_channels([
            {"channelId": "wapi-1", "transport": "wapi", "state": "active", "name": "WABA"},
            {"channelId": "tgapi-1", "transport": "tgapi", "state": "active", "name": "Telegram"},
            {"channelId": "maxbot-1", "transport": "maxbot", "state": "active", "name": "MAX Bot"},
        ])
        self.assertEqual([row["transport"] for row in channels], ["whatsapp", "telegram", "max"])

    def test_hidden_personal_telegram_accounts_match_stable_identifiers(self):
        self.assertTrue(router._telegram_dialog_hidden(peer_id="943871493"))
        self.assertTrue(router._telegram_dialog_hidden(peer_id="328268937"))
        self.assertTrue(router._telegram_dialog_hidden(phone="8 (999) 730-19-59"))
        self.assertTrue(router._telegram_dialog_hidden(username="@PapaProduser"))
        self.assertTrue(router._telegram_dialog_hidden({"telegram_username": "Rareru"}))
        self.assertFalse(router._telegram_dialog_hidden({
            "external_user_id": "702", "phone": "+79108758427", "username": "client",
        }))

    def test_finds_existing_wazzup_chat_by_phone_and_channel(self):
        rows = [{
            "chatId": "62837516",
            "chatType": "max",
            "userPhone": "+7 910 875-84-27",
            "contactName": "Елена",
            "chats": [{"channelId": "max-1", "chatId": "62837516", "chatType": "max"}],
        }]
        self.assertEqual(
            router._history_chat_candidate(rows, "max-1", "max", "+79108758427"),
            {"channel_id": "max-1", "chat_type": "max", "chat_id": "62837516", "contact_name": "Елена"},
        )
        self.assertIsNone(router._history_chat_candidate(rows, "max-other", "max", "+79108758427"))

    def test_converts_read_only_history_message_without_sending(self):
        record = router._history_message_record(
            {
                "id": "history-1", "channelId": "max-1", "chatType": "max", "chatId": "62837516",
                "incoming": True, "text": "Старая история", "datetime": "2026-07-23T12:17:00Z",
            },
            "max-1", "max", "62837516", "+79108758427",
        )
        self.assertEqual(record["external_id"], "history-1")
        self.assertEqual(record["direction"], "incoming")
        self.assertEqual(record["text"], "Старая история")
        self.assertNotIn("+79108758427", record["phone_hash"])

    def test_telegram_bot_requires_existing_chat(self):
        self.assertEqual(
            router._channel_send_state({"channel_transport": "telegram"}, False),
            (False, "Клиент ещё не написал Telegram-боту."),
        )
        self.assertEqual(router._channel_send_state({"channel_transport": "tgapi"}, False), (True, ""))
        self.assertEqual(router._channel_send_state({"channel_transport": "max"}, False), (True, ""))

    def test_first_message_uses_phone_for_max_and_telegram_personal(self):
        self.assertEqual(
            router._first_message_recipient({"channel_transport": "max"}, "max", "+7 (999) 123-45-67", {}),
            {"phone": "79991234567"},
        )
        self.assertEqual(
            router._first_message_recipient({"channel_transport": "tgapi"}, "telegram", "+7 (999) 123-45-67", {}),
            {"phone": "79991234567"},
        )
        self.assertEqual(
            router._first_message_recipient(
                {"channel_transport": "tgapi"}, "telegram", "+7 (999) 123-45-67", {"telegram_username": "@client"},
            ),
            {"username": "client"},
        )

    def test_channel_sources_are_loaded_in_parallel(self):
        original = router._cached_active_channels, router._vk_channel, router._telegram_channel

        async def run():
            started = 0
            all_started = asyncio.Event()

            async def source(row):
                nonlocal started
                started += 1
                if started == 3:
                    all_started.set()
                await all_started.wait()
                return row

            router._cached_active_channels = lambda **_: source([{"channel_id": "wazzup"}])
            router._vk_channel = lambda: source({"channel_id": "vk"})
            router._telegram_channel = lambda **_: source({"channel_id": "telegram"})
            return await asyncio.wait_for(router._all_channels(), timeout=0.2)

        try:
            self.assertEqual(
                [row["channel_id"] for row in asyncio.run(run())],
                ["wazzup", "vk", "telegram"],
            )
        finally:
            router._cached_active_channels, router._vk_channel, router._telegram_channel = original

    def test_active_channels_have_provider_and_distinct_telegram_labels(self):
        channels = router._active_chat_channels([
            {"channelId": "bot", "transport": "telegram", "state": "active", "name": "SystemBot"},
            {"channelId": "personal", "transport": "tgapi", "state": "active", "name": "79990001122"},
            {"channelId": "wa", "transport": "whatsapp", "state": "notEnoughMoney", "name": "79990001122"},
        ])
        self.assertEqual([row["channel_id"] for row in channels], ["bot", "personal"])
        self.assertEqual(channels[0]["label"], "Telegram Bot · SystemBot")
        self.assertEqual(channels[1]["label"], "Telegram Personal · 79990001122")
        self.assertTrue(all(row["provider"] == "wazzup" for row in channels))

    def test_channel_lookup_has_a_short_provider_deadline_and_stored_fallback(self):
        self.assertEqual(router.CHANNEL_REQUEST_TIMEOUT_SECONDS, 5)
        self.assertTrue(callable(router._stored_wazzup_channels))

    def test_vk_identifiers_and_callback_secret(self):
        self.assertEqual(router._vk_reference("123456"), "123456")
        self.assertEqual(router._vk_reference("https://vk.com/id654321"), "654321")
        self.assertEqual(router._vk_reference("https://vk.com/client.name"), "client.name")
        self.assertEqual(router._vk_reference("https://vk.ru/tehpod_sobakovodpro"), "tehpod_sobakovodpro")
        self.assertEqual(router._vk_callback_secret("Abc123"), "Abc123")
        generated = router._vk_callback_secret("bad-secret_with-symbols")
        self.assertTrue(generated.isalnum())
        self.assertLessEqual(len(generated), 50)

    def test_vk_callback_queue_is_durable_and_deduplicated(self):
        previous_db = router._db_path
        processed = []
        previous_processor = router._process_vk_callback_payload

        async def processor(payload):
            processed.append(payload["type"])

        try:
            with tempfile.TemporaryDirectory() as directory:
                router._db_path = Path(directory) / "messenger-widget.db"
                router._process_vk_callback_payload = processor

                async def scenario():
                    await router._init_vk_callback_queue()
                    body = b'{"type":"message_new","event_id":"same"}'
                    payload = json.loads(body)
                    await router._enqueue_vk_callback(body, payload)
                    await router._enqueue_vk_callback(body, payload)
                    with sqlite3.connect(router._vk_callback_queue_path()) as db:
                        self.assertEqual(db.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0], 1)
                    self.assertEqual(await router._drain_vk_callback_queue(), 1)
                    with sqlite3.connect(router._vk_callback_queue_path()) as db:
                        self.assertEqual(db.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0], 0)

                asyncio.run(scenario())
        finally:
            router._db_path = previous_db
            router._process_vk_callback_payload = previous_processor
        self.assertEqual(processed, ["message_new"])

    def test_cancelled_quick_profile_lookup_does_not_leak_coroutines(self):
        async def probe(*_args, **_kwargs):
            return {}

        async def cancel_before_children_start(*awaitables):
            for awaitable in awaitables:
                awaitable.close()
            raise asyncio.CancelledError

        async def scenario():
            with (
                patch.object(router, "_apply_identity_rules", new=AsyncMock()),
                patch.object(router, "_run_identity_lookup", new=probe),
                patch.object(router, "_exact_provider_identity", new=probe),
                patch.object(router, "_entity_external_link", new=probe),
                patch.object(router, "_successful_card_delivery_link", new=probe),
                patch.object(router.asyncio, "gather", new=cancel_before_children_start),
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await router._quick_widget_profile_links(
                        {"entity_type": "lead", "entity_id": "1", "platform_user_id": "2"},
                        "amocrm",
                        {"id": 1, "admin_id": 1},
                    )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(scenario())
            gc.collect()
        self.assertFalse(any("was never awaited" in str(item.message) for item in caught))

    def test_vk_callback_writer_batches_concurrent_durable_acks(self):
        previous_db = router._db_path
        previous_queue = router._vk_callback_write_queue
        previous_task = router._vk_callback_writer_task
        try:
            with tempfile.TemporaryDirectory() as directory:
                router._db_path = Path(directory) / "messenger-widget.db"

                async def scenario():
                    await router._init_vk_callback_queue()
                    router._vk_callback_write_queue = asyncio.Queue(maxsize=64)
                    writer = asyncio.create_task(router.vk_callback_writer_loop())
                    router._vk_callback_writer_task = writer
                    try:
                        await asyncio.gather(*(
                            router._enqueue_vk_callback(
                                json.dumps({"type": "message_new", "event_id": index}).encode(),
                                {"type": "message_new", "event_id": index},
                            )
                            for index in range(50)
                        ))
                    finally:
                        writer.cancel()
                        await asyncio.gather(writer, return_exceptions=True)
                    with sqlite3.connect(router._vk_callback_queue_path()) as db:
                        return db.execute("SELECT COUNT(*) FROM callback_events").fetchone()[0]

                self.assertEqual(asyncio.run(scenario()), 50)
        finally:
            router._db_path = previous_db
            router._vk_callback_write_queue = previous_queue
            router._vk_callback_writer_task = previous_task

    def test_vk_callback_enqueue_fails_if_writer_cannot_open_database(self):
        previous_queue = router._vk_callback_write_queue
        previous_task = router._vk_callback_writer_task

        async def fail_connect():
            raise sqlite3.OperationalError("database is locked")

        async def scenario():
            router._vk_callback_write_queue = asyncio.Queue(maxsize=1)
            with patch.object(router, "_connect_vk_callback_queue", new=fail_connect):
                writer = asyncio.create_task(router.vk_callback_writer_loop())
                router._vk_callback_writer_task = writer
                with self.assertRaisesRegex(RuntimeError, "VK callback writer stop"):
                    await asyncio.wait_for(
                        router._enqueue_vk_callback(b"callback", {"type": "message_new"}),
                        timeout=1,
                    )
                await asyncio.gather(writer, return_exceptions=True)

        try:
            asyncio.run(scenario())
        finally:
            router._vk_callback_write_queue = previous_queue
            router._vk_callback_writer_task = previous_task

    def test_vk_photo_uses_largest_image(self):
        files = router._vk_attachment_views([
            {"type": "photo", "photo": {"sizes": [
                {"width": 100, "height": 100, "url": "https://example.test/s.jpg"},
                {"width": 1200, "height": 800, "url": "https://example.test/l.jpg"},
            ]}},
        ])
        self.assertEqual(files, [{"content_uri": "https://example.test/l.jpg", "content_type": "image", "filename": ""}])

    def test_message_attachments_and_inbox_paging(self):
        content = router._message_content({
            "contentSha": "e66e3b7704be1146891e7f684b413a2d142826a1",
            "contentType": "image/webp",
            "filename": "photo name.webp",
        })
        self.assertEqual(content["content_type"], "image/webp")
        self.assertEqual(
            content["content_uri"],
            "https://store.wazzup24.com/e66e3b7704be1146891e7f684b413a2d142826a1/?filename=photo%20name.webp",
        )
        self.assertEqual(router._inbox_preview({"content_type": "image/webp"}), "Изображение")
        self.assertEqual(router.INBOX_LIMIT, 50)
        self.assertEqual(router.VK_HISTORY_PAGE_SIZE, 12)
        self.assertEqual(router.CONVERSATION_PAGE_SIZE, 12)

    def test_unread_threads_are_first_and_newest_within_each_group(self):
        rows = [
            {"name": "new-read", "unread": 0, "sent_at": "2026-08-04T19:00:00Z"},
            {"name": "old-unread", "unread": 1, "sent_at": "2026-08-04T17:00:00Z"},
            {"name": "new-unread", "unread": 2, "sent_at": "2026-08-04T18:00:00Z"},
            {"name": "old-read", "unread": 0, "sent_at": "2026-08-04T16:00:00Z"},
        ]
        self.assertEqual(
            [row["name"] for row in router._sort_inbox_items(rows)],
            ["new-unread", "old-unread", "new-read", "old-read"],
        )

    def test_direct_history_cache_is_bounded(self):
        cache = {}
        for value in range(router.DIRECT_HISTORY_CACHE_LIMIT + 20):
            router._remember_direct_history(cache, (str(value), 0), False, 60)
        self.assertEqual(len(cache), router.DIRECT_HISTORY_CACHE_LIMIT)
        self.assertNotIn(("0", 0), cache)

    def test_widget_contains_inbox_vk_and_lazy_history(self):
        module_dir = Path(__file__).resolve().parents[1]
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        panel = (module_dir / "panel" / "index.html").read_text(encoding="utf-8")
        backend = (module_dir / "router.py").read_text(encoding="utf-8")
        self.assertIn('request("/inbox"', widget)
        self.assertIn("function isAdminShell()", widget)
        self.assertIn('if (!isAdminShell()) return;', widget)
        self.assertIn("function wheelScrollX(node)", widget)
        self.assertIn("wheelScrollX(drawer.channels)", widget)
        self.assertIn('pair.host.style.width = Math.ceil(targetRect.width) + "px"', widget)
        self.assertIn("REQUEST_TIMEOUT_MS = 15000", widget)
        self.assertIn('"Сервер не ответил за " + Math.round(timeoutMs / 1000)', widget)
        self.assertIn('timeoutMs:60000', widget)
        self.assertNotIn('deferred_card = not thread and provider == TELEGRAM_PROVIDER', backend)
        self.assertIn('can_send = bool(peer_id or attemptable)', backend)
        self.assertIn("if not state and not refresh:\n        db = await _connect()", backend)
        self.assertNotIn("if not state and not refresh and _telegram_lock.locked()", backend)
        self.assertIn("conversation_presence = await asyncio.wait_for(", backend)
        self.assertIn("async def _conversation_presence(", backend)
        self.assertEqual(widget.count('await request("/link"'), 1)
        self.assertIn("function enableHistoryScroll(feed, loader)", widget)
        self.assertIn("var conversationCache = new Map();", widget)
        self.assertIn("var channelCache = new Map();", widget)
        self.assertIn("function loadChannelsForContext(source, token)", widget)
        self.assertIn("window.requestIdleCallback(prefetchCardChannels", widget)
        self.assertIn("function deliveryStatus(status)", widget)
        self.assertIn('label: "Прочитано"', widget)
        self.assertIn("appendMessageMeta(meta, message)", widget)
        self.assertNotIn("message.status || \"\"].filter(Boolean).join", widget)
        self.assertIn("loadInbox(true, true)", widget)
        self.assertNotIn('await request("/inbox/read"', widget)
        self.assertIn("function findVkId()", widget)
        self.assertIn("function visibleSourceFields()", widget)
        self.assertIn('document.querySelectorAll("tr")', widget)
        self.assertIn('document.querySelectorAll("label,dt,th,td,div,span")', widget)
        self.assertIn("function getCoursePageIdentity()", widget)
        self.assertIn("fields.getcourse_user_id = page.entity_id", widget)
        self.assertIn('request("/profile-links"', widget)
        self.assertIn('aria-label="Профили клиента"', widget)
        self.assertIn('class="gc-card-action"', widget)
        self.assertIn("function openGetCourseCard()", widget)
        self.assertIn(".attachment-draft[hidden]{display:none}", widget)
        self.assertIn(".compose-error:empty{display:none}", widget)
        self.assertNotIn('nativeButton.textContent = channel.provider', widget)
        self.assertNotIn('back.textContent = "Каналы"', widget)
        self.assertIn('direct_label = "VK · найти по utm_term"', backend)
        self.assertIn("attemptable = bool(phone)", backend)
        self.assertIn('loadInbox(false);', widget)
        self.assertIn("vk_id: findVkId()", widget)
        self.assertIn('image.className = "message-image"', widget)
        self.assertIn('>Написать</button>', widget)
        self.assertNotIn('>Написать через Wazzup</button>', widget)
        self.assertIn('inputWrap.className = "composer-input"', widget)
        self.assertIn('inputWrap.appendChild(menu)', widget)
        self.assertIn("function bindComposerTextarea(input)", widget)
        self.assertIn('input.setRangeText(" ", start, end, "end")', widget)
        self.assertIn('["keypress", "keyup", "beforeinput"]', widget)
        self.assertIn("function resizeComposerTextarea(input)", widget)
        self.assertIn('input.style.overflowY = input.scrollHeight > maximum + 1 ? "auto" : "hidden"', widget)
        self.assertGreaterEqual(widget.count("resizeComposerTextarea(input)"), 6)
        self.assertNotIn("resize:vertical;overflow-y:auto", widget)
        self.assertIn('function placeMenu()', widget)
        self.assertIn('composer-menu.open-up', widget)
        self.assertIn('.channel-option input{width:14px', widget)
        self.assertIn('id="vkMessage"', panel)
        self.assertIn("Синхронизировано ·", panel)
        self.assertIn("function findTelegramIdentity()", widget)
        self.assertIn("telegram_username: telegram.telegram_username", widget)
        self.assertIn("Telegram Personal", panel)
        self.assertIn("/telegram/auth/send-code", panel)
        self.assertIn("/telegram/auth/confirm", panel)
        self.assertIn('request("/templates"', widget)
        self.assertIn('className = "composer-more"', widget)
        self.assertIn('<span>Отправить везде</span>', widget)
        self.assertEqual(widget.count('<span>Отправить везде</span>'), 1)
        self.assertNotIn("inbox-send-all", widget)
        self.assertIn("Загружаем диалоги…", widget)
        self.assertIn('refreshButton.setAttribute("aria-busy", "true")', widget)
        self.assertIn("WITH visible(channel_id,chat_type,chat_id) AS (VALUES", backend)
        self.assertIn("known_selected = selected.intersection(channel_map)", backend)
        self.assertIn('async function sendComposerText(', widget)
        self.assertIn('body: JSON.stringify(Object.assign({}, payloadFor(targets[0]), { body: rawText }))', widget)
        self.assertIn('await Promise.all(targets.map(async function (channel, index)', widget)
        self.assertIn('request_id: batchId + ":" + index', widget)
        self.assertIn('event.stopPropagation();', widget)
        self.assertIn('Тема<div class="themes"', widget)
        self.assertIn('Палитра<div class="palettes"', widget)
        self.assertIn('showDrawerTemplateSettings', widget)
        self.assertIn('.chat-shell{min-width:0;', widget)
        self.assertIn('grid-template-columns:minmax(0,1fr);grid-template-rows:', widget)
        self.assertIn('.layer[data-drawer-size] .drawer{width:100%;', widget)
        self.assertIn('.head{min-height:56px;display:grid;grid-template-columns:minmax(130px,.45fr) minmax(120px,1fr)', widget)
        self.assertIn('.drawer-send-all{grid-column:3;grid-row:1}.copy{grid-column:4;grid-row:1}.gc-card-action{grid-column:5;grid-row:1}.drawer-settings{grid-column:6;grid-row:1}', widget)
        self.assertIn('.channels{grid-column:1/-1;grid-row:2;', widget)
        self.assertIn('.drawer-send-all{grid-column:1;grid-row:3}', widget)
        self.assertIn('folder || "Без папки"', widget)
        self.assertIn('menuButton("★ Избранное", showFavorites)', widget)
        self.assertIn('action: "favorite"', widget)
        self.assertIn('className = "template-star"', widget)
        self.assertIn("Виджет мессенжеров", panel)
        self.assertIn('id="templatesView"', panel)
        self.assertIn('id="identityView"', panel)
        self.assertIn('>Сотрудники <span id="adminCount">', panel)
        self.assertIn('class="input role-select"', panel)
        self.assertIn('class="input amo-select"', panel)
        self.assertIn('class="amo-task-enabled"', panel)
        self.assertIn('Ставить задачу при новом сообщении', panel)
        self.assertIn('amo_task_enabled:amoTaskEnabled', panel)
        self.assertIn('class="amo-task-source"', panel)
        self.assertIn("['max','MAX']", panel)
        self.assertIn('amo_task_sources:amoTaskSources', panel)
        self.assertIn("button.textContent='Сохраняю…'", panel)
        self.assertIn("api('/staff/catalog')", panel)
        self.assertIn('id="employeeTemplatesModal"', panel)
        self.assertIn("employee-templates", panel)
        self.assertIn("/admins/${employeeTemplateState.admin.id}/templates", panel)
        self.assertIn("Метки для личного шаблона", panel)
        self.assertNotIn("prompt('ID сотрудника GetCourse:'", panel)

    def test_telegram_personal_user_view_is_exact(self):
        class User:
            id = 123456
            phone = "8 (999) 123-45-67"
            username = "Exact_User"
            first_name = "Анна"
            last_name = "Петрова"

        self.assertEqual(router._telegram_user_view(User()), {
            "id": "123456",
            "phone": "+79991234567",
            "username": "Exact_User",
            "name": "Анна Петрова",
        })

    def test_profile_links_accept_only_direct_allowlisted_profiles(self):
        links = router._profile_links_from_values([{
            "vk": "https://vk.com/id123456",
            "telegram": "t.me/Exact_User",
            "telegram_phone": "https://t.me/+79991234567?profile",
            "max": "https://max.ru/u/abcdefghijklmnop",
            "salebot": "https://salebot.pro/projects/397724/clients/99001",
            "salebot_bad": "https://salebot.pro/projects/397724/clients/tg/user",
            "unsafe": "https://example.test/id123456",
            "vk_group": "https://vk.com/club123456",
            "telegram_join": "https://t.me/+invite",
        }])
        self.assertEqual(links, {
            "vk": "https://vk.com/id123456",
            "telegram_personal": "https://t.me/Exact_User",
            "salebot": "https://salebot.pro/projects/397724/clients/99001",
            "max": "https://max.ru/u/abcdefghijklmnop",
        })

    def test_telegram_profile_url_prefers_username_and_supports_private_phone(self):
        self.assertEqual(
            router._telegram_profile_url("@Exact_User", "+79991234567"),
            "https://t.me/Exact_User?profile",
        )
        self.assertEqual(
            router._telegram_profile_url("", "8 (999) 123-45-67"),
            "https://t.me/+79991234567?profile",
        )
        self.assertEqual(router._telegram_profile_url("", "123"), "")

    def test_amocrm_telegram_profile_requires_live_personal_account_verification(self):
        class Index:
            def provider_id_for_exact_context(self, provider, _context):
                return {"salebot": "99001", "telegram": "123456789"}.get(provider, "")

            def telegram_username_for_platform_id(self, platform_id):
                self_assert.assertEqual(platform_id, "123456789")
                return ""

        async def no_rules(_context):
            return None

        async def resolved(_data, _mode, _device):
            return {"accounts": [], "variables": {}}

        async def no_entity_link(*_args):
            return {}

        async def no_successful_delivery(*_args):
            return {}

        telegram_result = {"value": {}}

        async def telegram_check(_data, _mode, _device, context):
            self.assertEqual(context.get("platform"), "amocrm")
            return dict(telegram_result["value"])

        self_assert = self
        previous = (
            router._identity_index,
            router._apply_identity_rules,
            router._resolve_widget_context,
            router._entity_external_link,
            router._amocrm_telegram_profile_link,
            router._successful_card_delivery_link,
        )
        router._identity_index = Index()
        router._apply_identity_rules = no_rules
        router._resolve_widget_context = resolved
        router._entity_external_link = no_entity_link
        router._amocrm_telegram_profile_link = telegram_check
        router._successful_card_delivery_link = no_successful_delivery
        try:
            request_data = {
                "entity_type": "lead",
                "entity_id": "18222875",
                "phone": "8 (999) 123-45-67",
            }
            device = {"admin_name": "Татьяна"}
            missing_links = asyncio.run(router._widget_profile_links(
                request_data, "amocrm", device,
            ))
            telegram_result["value"] = {"pending": "1"}
            pending_links = asyncio.run(router._widget_profile_links(
                request_data, "amocrm", device,
            ))
            telegram_result["value"] = {"external_user_id": "123456789"}
            verified_links = asyncio.run(router._widget_profile_links(
                {
                    "entity_type": "lead",
                    "entity_id": "18222875",
                    "phone": "8 (999) 123-45-67",
                },
                "amocrm",
                {"admin_name": "Татьяна"},
            ))
        finally:
            (
                router._identity_index,
                router._apply_identity_rules,
                router._resolve_widget_context,
                router._entity_external_link,
                router._amocrm_telegram_profile_link,
                router._successful_card_delivery_link,
            ) = previous
        self.assertEqual(missing_links, [
            {
                "kind": "salebot",
                "label": "SaleBot",
                "url": "https://salebot.pro/projects/397724/clients/99001",
            },
        ])
        self.assertEqual(pending_links, [
            {
                "kind": "salebot",
                "label": "SaleBot",
                "url": "https://salebot.pro/projects/397724/clients/99001",
            },
            {
                "kind": "telegram_personal",
                "label": "",
                "url": "",
                "verification": "pending",
            },
        ])
        self.assertEqual(verified_links, [
            {
                "kind": "telegram_personal",
                "label": "TG Personal",
                "url": "https://t.me/+79991234567?profile",
                "verification": "verified",
            },
            {
                "kind": "salebot",
                "label": "SaleBot",
                "url": "https://salebot.pro/projects/397724/clients/99001",
            },
        ])
        amo = (Path(__file__).resolve().parents[1] / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("['pending','unverified'].includes(row.verification)", amo)
        self.assertIn("profileRefreshTimer=setTimeout(()=>loadProfileLinks(expectedGeneration,true),2000)", amo)
        self.assertNotIn("aria-label','Уточняем остальные профили'", amo)
        self.assertIn("if(result.sent.length){await refreshActive(true);loadProfileLinks(bootGeneration,true)}", amo)
        self.assertIn("else if(result.queued.length)setTimeout(()=>refreshActive(true),2500)", amo)

    def test_amocrm_telegram_profile_reuses_an_exact_verified_card_link(self):
        class Index:
            def provider_id_for_exact_context(self, _provider, _context):
                return ""

            def telegram_username_for_platform_id(self, platform_id):
                self_assert.assertEqual(platform_id, "6055344033")
                return ""

        async def no_rules(_context):
            return None

        async def resolved(_data, _mode, _device):
            return {"accounts": [], "variables": {}}

        async def exact_link(_platform, _entity_type, _entity_id, provider):
            if provider == router.TELEGRAM_PROVIDER:
                return {
                    "external_user_id": "6055344033", "phone": "+79050497320",
                    "name": "Екатерина Поликарпова",
                }
            return {}

        self_assert = self
        with (
            patch.object(router, "_identity_index", Index()),
            patch.object(router, "_apply_identity_rules", new=no_rules),
            patch.object(router, "_resolve_widget_context", new=resolved),
            patch.object(router, "_entity_external_link", new=exact_link),
            patch.object(
                router, "_successful_card_delivery_link",
                new=AsyncMock(side_effect=AssertionError("exact link must avoid history lookup")),
            ),
            patch.object(
                router, "_amocrm_telegram_profile_link",
                new=AsyncMock(side_effect=AssertionError("exact link must avoid live lookup")),
            ),
        ):
            links = asyncio.run(router._widget_profile_links(
                {
                    "entity_type": "lead", "entity_id": "18240573",
                    "phone": "+79050497320",
                },
                "amocrm", {"admin_name": "Евгения"},
            ))
        telegram = next(row for row in links if row["kind"] == router.TELEGRAM_PROVIDER)
        self.assertEqual(telegram["verification"], "verified")
        self.assertEqual(telegram["label"], "TG Personal: Екатерина Поликарпова")

    def test_amocrm_telegram_profile_verification_runs_in_background_and_caches_miss(self):
        async def scenario():
            context = {
                "platform": "amocrm", "entity_type": "lead", "entity_id": "18232123",
                "phone": "+79991234567", "email": "",
            }
            device = {"id": 17}
            cache_key = router._card_link_cache_key(
                context, device, router.TELEGRAM_PROVIDER, "",
            )
            gate = asyncio.Event()

            async def slow_check(_data, _mode, _device, provider, **kwargs):
                self.assertEqual(provider, router.TELEGRAM_PROVIDER)
                self.assertEqual(kwargs.get("resolution_timeout"), 30)
                await gate.wait()
                router._remember_card_link(cache_key, {})
                return {}

            previous = router._provider_card_link
            router._provider_card_link = slow_check
            router._card_link_cache.clear()
            router._telegram_profile_inflight.clear()
            try:
                pending = await router._amocrm_telegram_profile_link(
                    {}, "amocrm", device, context,
                )
                self.assertEqual(pending, {"pending": "1"})
                self.assertIn(cache_key, router._telegram_profile_inflight)
                gate.set()
                await router._telegram_profile_inflight[cache_key]
                self.assertEqual(
                    await router._amocrm_telegram_profile_link({}, "amocrm", device, context),
                    {},
                )
                self.assertNotIn(cache_key, router._telegram_profile_inflight)
            finally:
                router._provider_card_link = previous
                router._card_link_cache.clear()
                router._telegram_profile_inflight.clear()

        asyncio.run(scenario())

    def test_amocrm_profile_ignores_unverified_salebot_import_url_and_stale_link(self):
        class Index:
            def provider_id_for_exact_context(self, _provider, _context):
                return ""

        async def no_rules(_context):
            return None

        async def resolved(_data, _mode, _device):
            return {
                "accounts": [],
                "variables": {
                    "amo.lead.csv_import.fields.dialog_salebot": {
                        "value": "https://salebot.pro/projects/397724/clients/765266654",
                    },
                },
            }

        async def stale_entity_link(_platform, _entity_type, _entity_id, provider):
            if provider == router.SALEBOT_PROVIDER:
                return {"external_user_id": "765266654"}
            return {}

        async def no_successful_delivery(*_args):
            return {}

        previous = (
            router._identity_index,
            router._apply_identity_rules,
            router._resolve_widget_context,
            router._entity_external_link,
            router._successful_card_delivery_link,
        )
        router._identity_index = Index()
        router._apply_identity_rules = no_rules
        router._resolve_widget_context = resolved
        router._entity_external_link = stale_entity_link
        router._successful_card_delivery_link = no_successful_delivery
        try:
            links = asyncio.run(router._widget_profile_links(
                {
                    "entity_type": "lead",
                    "entity_id": "17894711",
                    "fields": {"salebot_id": "765266654", "utm_term": "765266654"},
                },
                "amocrm",
                {"admin_name": "Евгения"},
            ))
        finally:
            (
                router._identity_index,
                router._apply_identity_rules,
                router._resolve_widget_context,
                router._entity_external_link,
                router._successful_card_delivery_link,
            ) = previous
        self.assertNotIn("salebot", {row["kind"] for row in links})

    def test_profile_link_labels_have_stable_sales_order(self):
        self.assertEqual(router.PROFILE_LINK_ORDER, ("getcourse", "vk", "telegram_personal", "salebot", "max"))
        self.assertEqual(router.PROFILE_LINK_LABELS["getcourse"], "GetCourse")
        self.assertEqual(router.PROFILE_LINK_LABELS["telegram_personal"], "TG Personal")

    def test_getcourse_card_separates_calls_and_deduplicates_review(self):
        widget = (Path(__file__).resolve().parents[1] / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("function gcLessonGroups", widget)
        self.assertIn("Созвоны · только просмотр", widget)
        self.assertIn("seen[key].value=Boolean(seen[key].value||row.value)", widget)
        self.assertNotIn("Откройте карточку после", widget)

    def test_protected_test_card_loads_the_real_widget_in_test_mode(self):
        module_dir = Path(__file__).resolve().parents[1]
        page = (module_dir / "panel" / "test.html").read_text(encoding="utf-8")
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        self.assertIn('class="user-call-to-phone"', page)
        self.assertIn('+7 (996) 415-85-37', page)
        self.assertIn("entity_id:'15462823'", page)
        self.assertIn('data-nexus-wazzup-test="1"', page)
        self.assertIn('src="../static/widget.js?v=5143"', page)
        self.assertIn('"X-Nexus-Wazzup-Test": "1"', widget)
        self.assertIn("TEST_SOURCE_URL", widget)
        self.assertIn("if (!target && TEST_MODE) target = actions[actions.length - 1]", widget)
        self.assertIn('request("/channels"', widget)
        self.assertIn('id="amocrmView"', page)
        self.assertIn('id="amoFrame"', page)
        self.assertIn("type:'nexus-messenger-context'", page)
        self.assertIn("$('amoFrame').onload=sendAmoContext", page)
        self.assertIn("platform:'amocrm'", page)
        self.assertIn("fields:{utm_term:v.utm,utm_source:'yandex_dk_NW_ai'", page)
        self.assertIn("responsible_user_id:'6269974'", page)

    def test_public_user_guide_is_responsive_and_linked_from_widgets(self):
        module_dir = Path(__file__).resolve().parents[1]
        guide = (module_dir / "panel" / "docs.html").read_text(encoding="utf-8")
        guide_js = (module_dir / "static" / "guide.js").read_text(encoding="utf-8")
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        amo = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("Как пользоваться виджетом сообщений", guide)
        self.assertIn("Первый вход", guide)
        self.assertIn("Галочка «Отправить везде»", guide)
        self.assertIn("Как добавить шаблон в избранное", guide)
        self.assertIn("Как добавить вложение", guide)
        self.assertNotIn("HTML", guide)
        self.assertNotIn("для бабуш", guide.casefold())
        self.assertNotIn("Короткая памятка", guide)
        self.assertIn('id="articleSearchInput"', guide)
        self.assertIn('id="articleSearchResults"', guide)
        self.assertIn('<script src="../static/guide.js?v=5927"></script>', guide)
        self.assertNotIn('<script>', guide)
        self.assertIn('main section[id]', guide_js)
        self.assertIn('history.pushState(null,"","#"+article.id)', guide_js)
        self.assertIn('className="heading-anchor"', guide_js)
        self.assertIn('const snippetFor=', guide_js)
        self.assertIn('const appendHighlighted=', guide_js)
        self.assertIn('className="search-result-snippet"', guide_js)
        self.assertIn('words.every(word=>article.search.includes(word))', guide_js)
        self.assertIn('.search-results{position:absolute;top:100%;left:0;width:100%', guide)
        for internal_term in ("разметк", "callback", "message_from_outside", "utm_term"):
            self.assertNotIn(internal_term, guide.casefold())
        self.assertIn("@media(max-width:860px)", guide)
        self.assertNotIn("overflow-x:auto", guide)
        self.assertIn('API.replace(/\\/widget$/, "/guide")', widget)
        self.assertIn("Открыть инструкцию", widget)
        self.assertNotIn("прост", (guide + widget + amo).casefold())
        self.assertIn("GUIDE=API.replace(/\\/widget$/,'/guide')", amo)
        self.assertIn('id="help"', amo)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "data").mkdir()
            (root / "panel").mkdir()
            (root / "panel" / "docs.html").write_text(guide, encoding="utf-8")
            previous = router._db_path
            router._db_path = root / "data" / "messenger-widget.db"
            try:
                response = asyncio.run(router.user_guide())
            finally:
                router._db_path = previous
            self.assertEqual(response.status_code, 200)
            self.assertIn("no-cache, must-revalidate", response.headers["cache-control"])
            self.assertIn("script-src 'self'", response.headers["content-security-policy"])
            self.assertIn("Как пользоваться виджетом сообщений", response.body.decode("utf-8"))

    def test_amocrm_widget_has_one_size_setting_and_valid_manifest_settings(self):
        module_dir = Path(__file__).resolve().parents[1]
        page = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        manifest = json.loads((module_dir / "amocrm_widget" / "manifest.json").read_text(encoding="utf-8"))
        backend = (module_dir / "router.py").read_text(encoding="utf-8")
        self.assertIn("Размер виджета", page)
        self.assertIn("more-wrap.open-up", page)
        self.assertIn("composer.hidden=true", page)
        self.assertIn("Повторить загрузку", page)
        self.assertEqual(manifest["settings"], {
            "nexus_url": {
                "name": "settings.nexus_url",
                "type": "text",
                "required": False,
            }
        })
        amo = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("[hidden]{display:none!important}", amo)
        self.assertNotIn("<aside>", amo)
        self.assertNotIn("placeholder=\"Имя, телефон или ID\"", amo)
        self.assertIn('id="profileLinks"', amo)
        self.assertIn("request('/profile-links'", amo)
        self.assertIn("function appendRichText", amo)
        self.assertIn("function appendAttachment", amo)
        self.assertIn("media=document.createElement('audio')", amo)
        self.assertIn("Голосовое сообщение — файл недоступен в истории SaleBot", amo)
        self.assertIn("TG Personal", router.PROFILE_LINK_LABELS.values())
        self.assertIn("PAGE=12", amo)
        self.assertIn('id="more"', amo)
        self.assertIn('id="templateSettings"', amo)
        self.assertIn("Новый личный шаблон", amo)
        self.assertIn("requestAnimationFrame", amo)
        self.assertNotIn("keepTemplatesReady", amo)
        self.assertIn('id="authClose"', amo)
        self.assertIn("nexus-messenger-close", amo)
        self.assertIn("setInterval(refreshActive,5000)", amo)
        self.assertIn('id="sendAll" type="checkbox">Отправить везде', amo)
        self.assertNotIn('id="sendAll" type="checkbox" checked', amo)
        self.assertIn("if(!contextReady){if(completeness==='enriched')scheduleContextBoot(0);return}", amo)
        self.assertIn("if(routingChanged||!snapshotMatches)enrichCardContext(context)", amo)
        self.assertIn("scheduleContextBoot(completeness==='enriched'?0:800)", amo)
        self.assertIn("contextBootTimer=setTimeout(()=>{contextBootTimer=0;contextReady=true;boot()},delay)", amo)
        self.assertIn("async function enrichCardContext(next)", amo)
        self.assertIn("setRefreshTask('channels');setRefreshTask('cached');setRefreshTask('context','Уточняем данные клиента…')", amo)
        self.assertIn("finally{if(expectedGeneration===bootGeneration){setRefreshTask('channels');setRefreshTask('cached');setRefreshTask('context')}}", amo)
        self.assertNotIn("Уточняем остальные профили", amo)
        self.assertIn(".profile-link.unverified::before{display:none}", amo)
        profile_header = amo.split('id="profileLinks" class="profile-links">', 1)[1].split('</div>', 1)[0]
        self.assertNotIn("spinner", profile_header)
        self.assertIn("async function sendText(raw,rows,attachment)", amo)
        self.assertIn("const preview=raw?await request('/template-preview',{body:raw,...threadFields()}):{text:''}", amo)
        self.assertIn("Promise.all(targets.map", amo)
        self.assertIn("request_id:batchId+':'+index", amo)
        self.assertIn("event.stopPropagation()", amo)
        self.assertIn('Тема<div class="choices themes"', amo)
        self.assertIn('Палитра<div class="palettes"', amo)
        self.assertIn("del.type='button'", amo)
        self.assertIn("function placeMenu()", amo)
        self.assertIn("menuButton('★ Избранное',favoriteMenu)", amo)
        self.assertIn("action:'favorite'", amo)
        self.assertIn("className='template-star'", amo)
        self.assertIn(".shell{grid-template-rows:minmax(0,1fr)}", amo)
        self.assertIn(".shell .main{min-height:0;grid-template-rows:50px auto minmax(0,1fr) auto}", amo)
        self.assertIn("@media(max-width:680px){.composer{grid-template-columns:minmax(0,1fr) auto}", amo)
        self.assertIn("function showActiveChannel", amo)
        self.assertIn("function wheelX(node)", amo)
        self.assertIn("wheelX($('channels'))", amo)
        self.assertIn("REQUEST_TIMEOUT=15000", amo)
        self.assertIn("Сервер отвечает дольше ${Math.round(timeout/1000)} секунд", amo)
        self.assertIn("timeout:60000", amo)
        self.assertIn("const conversationCache=new Map()", amo)
        self.assertIn("conversationCache.get(key)", amo)
        self.assertIn("CARD_CACHE_TTL=30*60*1000", amo)
        self.assertIn("function restoreCardSnapshot(snapshot=readCardSnapshot())", amo)
        self.assertIn("function restoreTemplateCache()", amo)
        self.assertIn("Загружаем шаблоны…", amo)
        self.assertIn("Изображение с компьютера", amo)
        self.assertIn("row.content_uri,row.content_type,row.attachments", amo)
        self.assertIn("async function uploadImage(file)", amo)
        self.assertIn("event.clipboardData?.files", amo)
        self.assertIn('id="file" type="file" accept="image/jpeg,image/png,image/gif,image/webp"', amo)
        self.assertIn("writeCardSnapshot({channels:cardChannels,context_routing_signature:contextRoutingSignature(context)})", amo)
        self.assertIn("Показываем сохранённые данные · обновляем…", amo)
        self.assertIn('id="refreshState" class="cache-refresh" hidden', amo)
        self.assertIn("localStorage.removeItem(cardCacheStoreKey())", amo)
        self.assertNotIn("channelRefreshAttempts>=3", amo)
        self.assertIn("while(expectedGeneration===bootGeneration&&token&&widgetVisible)", amo)
        self.assertIn("Каналы отвечают дольше обычного · попытка ${attempt}", amo)
        self.assertIn("Nexus продолжает загрузку", amo)
        self.assertNotIn("Профили загрузятся после повторной попытки", amo)
        self.assertIn("clearTimeout(channelRefreshTimer)", amo)
        self.assertNotIn("},3500)}", amo)
        self.assertNotIn("await request('/inbox/read'", amo)
        self.assertNotIn("await openChannel(active)", amo)
        self.assertIn("insertAdjacentHTML('beforeend'", amo)
        self.assertIn("nexus-messenger-painted", amo)
        self.assertNotIn("mobile-inbox", amo)
        self.assertIn(".channels button:disabled::after", amo)
        self.assertIn("Загружаем историю переписки…", amo)
        self.assertIn("data.history_status!=='syncing'", amo)
        self.assertIn("function deliveryStatus(status)", amo)
        self.assertIn("label:'Отправлено'", amo)
        self.assertIn("label:'Ошибка доставки'", amo)
        self.assertIn("label:'Прочитано'", amo)
        self.assertIn("appendMessageMeta(meta,row)", amo)
        self.assertNotIn("row.status].filter(Boolean).join(' · ')", amo)
        self.assertIn("Выйти из аккаунта", amo)
        self.assertIn("request('/logout')", amo)
        self.assertIn("html[data-theme=\"dark\"] .auth", amo)
        self.assertIn(".profile-links{flex:1 1 0;width:0}", amo)
        script = (module_dir / "amocrm_widget" / "script.js").read_text(encoding="utf-8")
        self.assertEqual(manifest["widget"]["version"], "1.9.5")
        self.assertIn("static/amocrm.html'", script)
        self.assertIn("REMOTE_CACHE_WINDOW_MS = 5 * 60 * 1000", script)
        self.assertIn("Math.floor(Date.now() / REMOTE_CACHE_WINDOW_MS)", script)
        self.assertIn("WIDGET_BOOTSTRAP_VERSION + '-' + cacheSlot", script)
        self.assertNotIn("WIDGET_CACHE_VERSION", script)
        self.assertIn("background:'#111c25'", script)
        self.assertIn("opacity:0", script)
        self.assertIn("height:'100dvh'", script)
        self.assertIn("const AMO_REQUEST_TIMEOUT = 6000", script)
        self.assertIn("const CONTEXT_TIMEOUT = 20000", script)
        self.assertIn("postContext(basicContext(), 'basic')", script)
        self.assertIn("enrichmentPromise = cardContext()", script)
        self.assertIn("context:value", script)
        self.assertIn("Получаем данные клиента…", script)
        self.assertIn("spinner[0].animate", script)
        self.assertIn("setTimeout(paint, 1200)", script)
        self.assertIn("function armFrameDeadline()", script)
        self.assertIn("Виджет не загрузился", script)
        self.assertIn("}, 30000);", script)
        self.assertIn("addEventListener('resize',()=>{showActiveChannel();sizeMessageInput()})", amo)
        self.assertIn("resize:'both'", script)
        self.assertIn("variable-list", amo)
        self.assertIn("setRangeText", amo)
        self.assertNotIn("scrollIntoView", amo)
        panel = (module_dir / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn("compactRecipientPicker", panel)
        self.assertIn("data-recipient", panel)
        self.assertIn("Уведомления ещё не подключены", panel)
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        self.assertIn("function optimisticTemplate", amo)
        self.assertIn("function optimisticTemplate", widget)
        self.assertIn("var templateCacheKey", widget)
        self.assertIn("function ensureTemplates(next)", widget)
        self.assertIn("async function uploadImage(imageFile)", widget)
        self.assertIn("event.clipboardData && event.clipboardData.files", widget)
        self.assertIn('file.accept = "image/jpeg,image/png,image/gif,image/webp"', widget)
        self.assertIn('render_message_template(message_text, variables)["text"]', backend)
        self.assertIn("CREATE TABLE IF NOT EXISTS widget_media", backend)
        self.assertIn("height:76px;min-height:76px;max-height:min(58vh,460px);resize:none", amo)
        self.assertIn("function sizeMessageInput()", amo)
        self.assertIn("$('message').addEventListener('input',sizeMessageInput)", amo)
        self.assertIn("input.value=optimistic;sizeMessageInput()", amo)
        self.assertIn("$('message').value='';sizeMessageInput()", amo)
        self.assertIn("$('send').classList.add('busy')", amo)
        self.assertIn("max-height:min(45vh,360px)", widget)
        self.assertIn("scrollbar-width:thin", amo)
        self.assertIn("scrollbar-width:thin", widget)
        self.assertIn('request("/logout"', widget)
        self.assertIn("Выйти из аккаунта", widget)
        self.assertIn("function logoutDevice", widget)
        self.assertIn("_schedule_wazzup_history(", backend)
        self.assertLess(amo.index("input.value=optimistic"), amo.index("await request('/template-preview',{id:Number(row.id)})"))
        self.assertLess(widget.index("input.value = optimistic"), widget.index('await request("/template-preview"'))

    def test_admin_panel_has_a_bounded_scroll_workspace(self):
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn("main{min-width:0;min-height:0;overflow:hidden}", panel)
        self.assertIn(".view{display:none;width:100%;height:100%", panel)
        self.assertIn("-webkit-overflow-scrolling:touch", panel)

    def test_amocrm_widget_reads_the_current_lead_and_main_contact(self):
        if not shutil.which("node"):
            self.skipTest("node is not installed")
        module_dir = Path(__file__).resolve().parents[1]
        subprocess.run(
            ["node", str(module_dir / "tests" / "test_amocrm_context.js"), str(module_dir / "amocrm_widget" / "script.js")],
            check=True,
        )

    def test_card_link_requires_exact_card_identity(self):
        context = {"phone": "+79964158537", "email": "nikita@example.test"}
        good = {"phone": "+7 (996) 415-85-37", "email": "nikita@example.test", "getcourse_user_id": "42"}
        self.assertTrue(router._card_link_matches_context(good, context, "42"))
        self.assertFalse(router._card_link_matches_context({**good, "phone": "+79990000000"}, context, "42"))
        self.assertFalse(router._card_link_matches_context({**good, "getcourse_user_id": "43"}, context, "42"))

    def test_streams_conversations_exposes_exact_salebot_history(self):
        async def recipients(**_kwargs):
            return {"ok": True, "telegram": "", "vk": "123456", "salebot": "965776230"}

        async def channels(**_kwargs):
            return [{
                "channel_id": "salebot:project", "transport": "salebot",
                "provider": "salebot", "label": "SaleBot · Проект",
            }]

        async def history(client_id):
            self.assertEqual(client_id, "965776230")
            return [{
                "external_id": "salebot:1", "direction": "incoming", "status": "delivered",
                "text": "Здравствуйте", "author_name": "Алина в SaleBot",
                "sent_at": "2026-08-17T05:00:00Z",
            }]

        async def vk_request(method, params):
            self.assertEqual((method, params), ("users.get", {"user_ids": "123456"}))
            return [{"id": 123456, "first_name": "Алина", "last_name": "Соколова"}]

        async def admin(_operator_name):
            return {"id": 1, "name": "Никита Попов"}

        async def templates(_admin_id, *, can_edit_shared):
            self.assertFalse(can_edit_shared)
            return []

        originals = (
            router.service_transfer_recipients, router._all_channels, router._salebot_history,
            router._streams_admin, router._template_rows, router._vk_request,
        )
        router.service_transfer_recipients = recipients
        router._all_channels = channels
        router._salebot_history = history
        router._streams_admin = admin
        router._template_rows = templates
        router._vk_request = vk_request
        try:
            result = asyncio.run(router.service_streams_conversations(
                email="mail.ru789@mail.ru", gc_user_id="505433216",
                name="Соколова Алина", phone="79819793382", operator_name="Никита Попов",
            ))
        finally:
            (
                router.service_transfer_recipients, router._all_channels, router._salebot_history,
                router._streams_admin, router._template_rows, router._vk_request,
            ) = originals

        self.assertEqual(len(result["channels"]), 1)
        self.assertEqual(result["channels"][0]["chat_id"], "965776230")
        self.assertTrue(result["channels"][0]["has_chat"])
        self.assertTrue(result["channels"][0]["can_send"])
        self.assertEqual(result["channels"][0]["messages"][0]["text"], "Здравствуйте")
        self.assertEqual(result["profile_links"], [
            {"kind": "vk", "label": "VK: Алина Соколова", "url": "https://vk.com/id123456"},
            {
                "kind": "salebot", "label": "SaleBot: Алина в SaleBot",
                "url": "https://salebot.pro/projects/397724/clients/965776230",
            },
        ])

    def test_streams_conversations_can_open_before_history_is_loaded(self):
        history_called = False

        async def recipients(**_kwargs):
            return {"ok": True, "telegram": "", "vk": "", "salebot": "965776230"}

        async def channels(**_kwargs):
            return [{
                "channel_id": "salebot:project", "transport": "salebot",
                "provider": "salebot", "label": "SaleBot · Проект",
            }]

        async def history(_client_id):
            nonlocal history_called
            history_called = True
            return []

        async def admin(_operator_name):
            return {"id": 1, "name": "Никита Попов"}

        async def templates(_admin_id, *, can_edit_shared):
            self.assertFalse(can_edit_shared)
            return []

        async def presence(_channels, _phone):
            return set()

        with patch.object(router, "service_transfer_recipients", new=recipients), patch.object(
            router, "_all_channels", new=channels,
        ), patch.object(router, "_salebot_history", new=history), patch.object(
            router, "_streams_admin", new=admin,
        ), patch.object(router, "_template_rows", new=templates), patch.object(
            router, "_conversation_presence", new=presence,
        ):
            result = asyncio.run(router.service_streams_conversations(
                email="mail@example.test", gc_user_id="505433216", name="Алина",
                phone="79819793382", operator_name="Никита Попов", include_history=False,
            ))

        self.assertFalse(history_called)
        self.assertEqual(result["channels"][0]["messages"], [])
        self.assertFalse(result["channels"][0]["history_loaded"])
        self.assertTrue(result["channels"][0]["can_send"])

    def test_streams_conversations_hides_excluded_personal_telegram(self):
        async def recipients(**_kwargs):
            return {
                "ok": True, "telegram": "328268937",
                "telegram_username": "Rareru", "vk": "", "salebot": "",
            }

        async def channels(**_kwargs):
            return [{
                "channel_id": "telegram-personal:1", "transport": "telegram",
                "provider": router.TELEGRAM_PROVIDER, "label": "Telegram Personal",
            }]

        async def admin(_operator_name):
            return {"id": 1, "name": "Никита Попов"}

        async def templates(_admin_id, *, can_edit_shared):
            return []

        with patch.object(router, "service_transfer_recipients", new=recipients), patch.object(
            router, "_all_channels", new=channels,
        ), patch.object(router, "_streams_admin", new=admin), patch.object(
            router, "_template_rows", new=templates,
        ):
            result = asyncio.run(router.service_streams_conversations(
                gc_user_id="42", name="Служебный контакт", phone="",
                operator_name="Никита Попов",
            ))

        self.assertEqual(result["channels"], [])
        self.assertEqual(result["profile_links"], [])

    def test_streams_email_preview_and_service_complete_url_attribution(self):
        variables = {
            "utm.term": {"value": "term-42"},
            "utm.source": {"value": "getcourse"},
            "utm.medium": {"value": "manager-email"},
            "utm.campaign": {"value": "follow-up"},
            "utm.content": {"value": "personal"},
            "ym_uid": {"value": "ym-7"},
            "conversation_id": {"value": "gc-514110600"},
        }
        captured = {}

        async def admin(_operator_name):
            return {"id": 1, "name": "Татьяна Истратова", "role": "employee"}

        async def resolved(_context):
            return {"variables": variables}

        async def setting(key):
            return {
                "auto_markup_domains": "sobakovod.pro;club.sobakovod.pro",
                "auto_markup_tail": router.AUTO_MARKUP_DEFAULT_TAIL,
            }[key]

        async def channel(channel_id, transport, provider):
            return {"channel_id": channel_id, "transport": transport, "provider": provider}

        async def email_send(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "queued": True}

        with patch.object(router, "_streams_admin", new=admin), patch.object(
            router, "_resolve_identity_context", new=resolved,
        ), patch.object(router, "_setting", new=setting), patch.object(
            router, "_requested_channel", new=channel,
        ), patch.object(router, "_identity_index", object()), patch.object(
            router, "_module_service", return_value=email_send,
        ):
            preview = asyncio.run(router.service_streams_template_preview(
                body="Подробнее: https://sobakovod.pro/course_tour",
                email="client@example.com", gc_user_id="514110600",
                name="Клиент", operator_name="Татьяна Истратова",
            ))
            result = asyncio.run(router.service_streams_send(
                channel_id="email:info", transport="email", provider="email",
                chat_id="", phone="+79990000000",
                text="Подробнее: https://sobakovod.pro/course_tour",
                operator_name="Татьяна Истратова", email="client@example.com",
                gc_user_id="514110600", name="Клиент", subject="Информация",
                idempotency_key="email-attribution-test",
                email_guidelines_confirmed=True, email_guidelines_version="2026-09-01",
            ))

        expected_url = (
            "https://sobakovod.pro/course_tour?utm_term=term-42"
            "&utm_source=getcourse&utm_medium=manager-email&utm_campaign=follow-up"
            "&utm_content=personal&param1=ym-7&param2=gc-514110600"
        )
        self.assertTrue(result["ok"])
        self.assertIn(expected_url, preview["text"])
        self.assertIn(expected_url, captured["text"])
        self.assertEqual(captured["text"].count("utm_term="), 1)
        self.assertEqual(
            captured["signature_url"],
            "https://sobakovod.pro/?utm_term=term-42&utm_source=getcourse"
            "&utm_medium=manager-email&utm_campaign=follow-up&utm_content=personal"
            "&param1=ym-7&param2=gc-514110600",
        )
        self.assertIs(captured["email_guidelines_confirmed"], True)
        self.assertEqual(captured["email_guidelines_version"], "2026-09-01")

    def test_streams_send_uses_salebot_client_id(self):
        calls = []

        async def channel(channel_id, transport, provider):
            self.assertEqual((channel_id, transport, provider), ("salebot:project", "salebot", "salebot"))
            return {"channel_id": channel_id, "transport": transport, "provider": provider}

        async def admin(_operator_name):
            return {"id": 1, "wazzup_user_id": "streams-1", "name": "Никита Попов", "role": "employee"}

        async def send(client_id, text, attachment_url="", attachment_type=""):
            calls.append((client_id, text, attachment_url, attachment_type))
            return {"ok": True}

        originals = router._requested_channel, router._streams_admin, router._salebot_send
        router._requested_channel, router._streams_admin, router._salebot_send = channel, admin, send
        try:
            result = asyncio.run(router.service_streams_send(
                channel_id="salebot:project", transport="salebot", provider="salebot",
                chat_id="965776230", phone="79819793382", text="Проверка",
                operator_name="Никита Попов", email="mail.ru789@mail.ru",
                gc_user_id="505433216", name="Соколова Алина", record_communication=False,
            ))
        finally:
            router._requested_channel, router._streams_admin, router._salebot_send = originals

        self.assertTrue(result["ok"])
        self.assertEqual(calls, [("965776230", "Проверка", "", "")])

    def test_streams_send_uses_wazzup_content_uri_for_max_image(self):
        calls = []

        async def channel(channel_id, transport, provider):
            return {
                "channel_id": channel_id, "transport": transport, "provider": provider,
                "channel_transport": "max", "plain_id": "",
            }

        async def admin(_operator_name):
            return {"id": 1, "wazzup_user_id": "manager-1", "name": "Никита Попов", "role": "employee"}

        async def conversation(_channel_id, _transport, _phone, _limit):
            return "max-chat-1", True, []

        async def wazzup(method, path, body=None, **_kwargs):
            calls.append((method, path, body))
            return {"messageId": "max-image-1", "chatId": "max-chat-1"}

        originals = (
            router._requested_channel, router._streams_admin, router._conversation_rows,
            router._wazzup_request, router.resolve_client_identity,
        )
        original_db = router._db_path
        with tempfile.TemporaryDirectory() as temp_dir:
            router._db_path = Path(temp_dir) / "messenger-widget.db"
            asyncio.run(router._init_db())
            router._requested_channel, router._streams_admin = channel, admin
            router._conversation_rows, router._wazzup_request = conversation, wazzup
            router.resolve_client_identity = AsyncMock(return_value={})
            try:
                result = asyncio.run(router.service_streams_send(
                    channel_id="max-1", transport="max", provider="wazzup",
                    chat_id="max-chat-1", phone="+79991234567", text="",
                    operator_name="Никита Попов", name="Тест",
                    attachment_url="https://cdn.example.test/image.png", attachment_type="image/png",
                    idempotency_key="image-test", record_communication=False,
                ))
            finally:
                (
                    router._requested_channel, router._streams_admin, router._conversation_rows,
                    router._wazzup_request, router.resolve_client_identity,
                ) = originals
                router._db_path = original_db

        self.assertTrue(result["ok"])
        self.assertEqual(len(calls), 1)
        payload = calls[0][2]
        self.assertEqual(payload["contentUri"], "https://cdn.example.test/image.png")
        self.assertNotIn("text", payload)
        self.assertEqual(payload["chatId"], "max-chat-1")
        self.assertRegex(payload["crmMessageId"], r"^nexus-[0-9a-f]{32}-file$")

    def test_profile_refresh_runs_without_a_header_spinner(self):
        amo = (Path(__file__).resolve().parents[1] / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertNotIn("setRefreshTask('profiles'", amo)
        self.assertNotIn("status.setAttribute('aria-label','Уточняем остальные профили')", amo)
        self.assertIn(".profile-link.unverified::before{display:none}", amo)
        self.assertIn("profileRefreshTimer=setTimeout(()=>loadProfileLinks(expectedGeneration,true),4000)", amo)
        self.assertIn("['salebot','vk','telegram_personal','wazzup']", amo)

    def test_getcourse_widget_queues_access_changes_without_second_confirmation(self):
        module_dir = Path(__file__).resolve().parents[1]
        amo = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        panel = (module_dir / "panel" / "index.html").read_text(encoding="utf-8")
        for source in (amo, widget):
            self.assertIn("Проверить и применить", source)
            self.assertIn("Передаём команду", source)
            self.assertIn("Проверяем актуальные доступы GetCourse", source)
            self.assertIn("Загружаем ДЗ и созвоны", source)
            self.assertIn("Тестовый период", source)
            self.assertIn("getcourse-test-period", source)
            self.assertIn("Проверяем возможность тестового периода", source)
            self.assertIn("Команда принята. Тестовый период будет выдан в фоне", source)
            self.assertIn("Доступы GetCourse применены и подтверждены", source)
            self.assertIn("operation_pending", source)
            self.assertIn("Забрать тестовый период", source)
            self.assertIn("Выдать повторно", source)
            self.assertIn("Задача принята. Окно можно закрыть", source)
            self.assertIn("gcShowsProgress", source)
            self.assertIn("стандарт", source.lower())
            self.assertNotIn("Следующая автоматическая попытка", source)
            self.assertNotIn("Проверьте изменения", source)
            self.assertNotIn("Данные ДЗ пока не найдены", source)
        self.assertIn("showGcOperationNotice", amo)
        self.assertIn("action:'revoke'", amo)
        self.assertIn("Операции", amo)
        self.assertIn("operationSettings", amo)
        self.assertIn("gcCardVisible=false", amo)
        self.assertIn("Операции", widget)
        self.assertIn('action:"revoke"', widget)
        self.assertIn("showDrawerOperations", widget)
        self.assertIn("Изменения выполняются в GetCourse", widget)
        self.assertNotIn("if(!confirm((details", amo)
        self.assertNotIn("window.confirm((text", widget)
        self.assertIn("replace(/^(\\d+)[.,]0$/", amo)
        self.assertIn("replace(/^(\\d+)[.,]0$/", widget)
        self.assertIn('{"client_id":"123456","text":"Здравствуйте"}', panel)
        self.assertNotIn('"message_id":"sb-987"', panel)

    def test_amocrm_rich_text_escapes_bare_ampersands_before_html_parsing(self):
        amo = (Path(__file__).resolve().parents[1] / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("replace(/&(?!(?:amp|lt|gt|quot|apos|#\\d+|#x[0-9a-f]+);)/gi,'&amp;')", amo)

    def test_notification_graph_recovers_exact_amocrm_deal(self):
        class Index:
            def resolve(self, context):
                self_assert.assertEqual((context["service"], context["entity_id"]), ("vk", "668744625"))
                return {
                    "status": "resolved",
                    "accounts": [
                        {"service": "amo", "platform_id": "18222853", "updated_at": "2026-08-21"},
                        {"service": "vk", "platform_id": "668744625", "updated_at": "2026-08-21"},
                    ],
                }

        self_assert = self
        previous = router._identity_index, router._amo_deal_delivery_details, router._amo_origin
        router._identity_index = Index()
        router._amo_deal_delivery_details = lambda lead_id: {
            "entity_url": "https://sobakovodpro.amocrm.ru/leads/detail/" + lead_id,
        }
        router._amo_origin = lambda: "https://sobakovodpro.amocrm.ru"
        try:
            result = asyncio.run(router._identity_amo_notification_context("vk", "668744625"))
        finally:
            router._identity_index, router._amo_deal_delivery_details, router._amo_origin = previous
        self.assertEqual(result, {
            "platform": "amocrm", "entity_type": "lead", "entity_id": "18222853",
            "entity_url": "https://sobakovodpro.amocrm.ru/leads/detail/18222853",
        })

    def test_getcourse_card_uses_email_recovered_from_identity_graph(self):
        class Index:
            def resolve(self, _context):
                return {
                    "variables": {
                        "contact.email": {"value": "client@example.test"},
                    },
                }

        async def no_rules(_context):
            return None

        received = {}

        async def student(**kwargs):
            received.update(kwargs)
            return {"ok": True, "found": True, "item": {"email": kwargs["email"]}}

        previous = router._identity_index, router._apply_identity_rules, router._module_service
        router._identity_index = Index()
        router._apply_identity_rules = no_rules
        router._module_service = lambda _module_id, _service: student
        try:
            result = asyncio.run(router._widget_getcourse_card_data(
                {"entity_type": "lead", "entity_id": "18278741"},
                "amocrm", {"admin_name": "Татьяна"}, include_access=False, summary_only=True,
            ))
        finally:
            router._identity_index, router._apply_identity_rules, router._module_service = previous
        self.assertEqual(received["email"], "client@example.test")
        self.assertEqual(result["item"]["email"], "client@example.test")

    def test_parallel_widget_identity_reads_are_single_flight(self):
        class Index:
            def __init__(self):
                self.resolve_calls = 0
                self.exact_calls = 0

            def resolve(self, _context):
                self.resolve_calls += 1
                __import__("time").sleep(0.02)
                return {"accounts": [], "variables": {}}

            def provider_id_for_exact_context(self, _provider, _context):
                self.exact_calls += 1
                __import__("time").sleep(0.02)
                return "123"

        index = Index()
        previous = router._identity_index
        router._identity_index = index
        context = {
            "platform": "amocrm", "entity_type": "lead", "entity_id": "18278741",
            "phone": "+79990000000", "email": "client@example.test", "fields": {},
        }

        async def run_parallel():
            resolved = await asyncio.gather(*(
                router._resolve_identity_context(dict(context)) for _ in range(12)
            ))
            exact = await asyncio.gather(*(
                router._exact_provider_identity("vk", dict(context)) for _ in range(12)
            ))
            return resolved, exact

        try:
            resolved, exact = asyncio.run(run_parallel())
        finally:
            router._identity_index = previous
        self.assertEqual(len(resolved), 12)
        self.assertEqual(exact, ["123"] * 12)
        self.assertEqual(index.resolve_calls, 1)
        self.assertEqual(index.exact_calls, 1)

    def test_slow_profile_lookup_returns_pending_then_cached_result(self):
        calls = 0

        async def slow_links(_data, _mode, _device):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.08)
            return [{"kind": "getcourse", "label": "client@example.test", "url": "https://example.test/user/1"}]

        async def scenario():
            data = {"entity_type": "lead", "entity_id": "18278741"}
            device = {"id": 77, "admin_name": "Татьяна"}
            first = await router._cached_widget_profile_links(
                data, "amocrm", device, foreground_seconds=0.005,
            )
            await asyncio.sleep(0.09)
            second = await router._cached_widget_profile_links(
                data, "amocrm", device, foreground_seconds=0.005,
            )
            return first, second

        router._profile_links_cache.clear()
        router._profile_links_inflight.clear()
        with patch.object(router, "_widget_profile_links", new=slow_links):
            first, second = asyncio.run(scenario())
        self.assertEqual(first, ([], True))
        self.assertEqual(second[1], False)
        self.assertEqual(second[0][0]["kind"], "getcourse")
        self.assertEqual(calls, 1)

    def test_known_profile_is_returned_while_full_enrichment_runs(self):
        calls = 0

        async def quick_links(_data, _mode, _device):
            return [{"kind": "vk", "label": "VK", "url": "https://vk.com/id123"}]

        async def slow_links(_data, _mode, _device):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.08)
            return [
                {"kind": "vk", "label": "VK: Анна", "url": "https://vk.com/id123"},
                {"kind": "getcourse", "label": "GetCourse", "url": "https://example.test/user/1"},
            ]

        async def scenario():
            data = {"entity_type": "lead", "entity_id": "18278742"}
            device = {"id": 78, "admin_name": "Татьяна"}
            first = await router._cached_widget_profile_links(data, "amocrm", device)
            await asyncio.sleep(0.09)
            second = await router._cached_widget_profile_links(data, "amocrm", device)
            return first, second

        router._profile_links_cache.clear()
        router._profile_links_inflight.clear()
        with (
            patch.object(router, "_quick_widget_profile_links", new=quick_links),
            patch.object(router, "_widget_profile_links", new=slow_links),
        ):
            first, second = asyncio.run(scenario())
        self.assertEqual(first[0][0]["kind"], "vk")
        self.assertTrue(first[1])
        self.assertEqual([row["kind"] for row in second[0]], ["vk", "getcourse"])
        self.assertFalse(second[1])
        self.assertEqual(calls, 1)

    def test_operation_errors_are_short_and_understandable(self):
        self.assertEqual(
            router._friendly_operation_error("HTTP 429 Too Many Requests", "vk"),
            "Канал ограничил частоту отправки. Nexus повторит автоматически.",
        )
        self.assertEqual(
            router._friendly_operation_error("Connect timeout after 15 seconds", "telegram_personal"),
            "Канал временно не ответил. Nexus попробует ещё раз.",
        )
        self.assertNotIn(
            "stacktrace",
            router._friendly_operation_error("unexpected stacktrace from provider", "salebot").casefold(),
        )

    def test_max_communication_filter_uses_wazzup_max_transport_rows(self):
        self.assertEqual(
            router._communication_provider_filter("max"),
            ("c.provider=? AND c.chat_type IN (?,?)", ["wazzup", "max", "maxgroup"]),
        )
        self.assertEqual(router._communication_provider_filter("salebot"), ("c.provider=?", ["salebot"]))
        panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
        self.assertIn("providerLabel=item.provider==='wazzup'&&['max','maxgroup'].includes(transport)?'MAX'", panel)


if __name__ == "__main__":
    unittest.main()
