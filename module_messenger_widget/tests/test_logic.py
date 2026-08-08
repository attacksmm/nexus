import asyncio
import json
import shutil
import subprocess
import unittest
from pathlib import Path

import router


class GetCourseWazzupLogicTests(unittest.TestCase):
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

    def test_utm_term_supports_platform_and_salebot_ids(self):
        self.assertEqual(router._graph.parse_utm_term("platform_id=abc-123"), [("vk_platform", "abc-123")])
        self.assertEqual(router._graph.parse_utm_term("salebot_id:998877"), [("salebot", "998877")])
        self.assertEqual(router._graph.parse_utm_term("998877"), [("candidate", "998877")])

    def test_salebot_history_result_normalizes_directions_and_attachment(self):
        messages = router._salebot_messages({"result": [
            {"id": 1, "client_replica": True, "message_from_outside": 3, "text": "Вопрос"},
            {"id": 2, "client_replica": False, "message_from_outside": 0, "text": "Ответ", "attachment_url": "https://example.test/file.pdf"},
        ]})
        self.assertEqual([row["direction"] for row in messages], ["incoming", "outgoing"])
        self.assertEqual(messages[1]["attachments"][0]["content_uri"], "https://example.test/file.pdf")

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
        self.assertEqual(router._vk_callback_secret("Abc123"), "Abc123")
        generated = router._vk_callback_secret("bad-secret_with-symbols")
        self.assertTrue(generated.isalnum())
        self.assertLessEqual(len(generated), 50)

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
        self.assertIn("Сервер не ответил за 15 секунд. Повторите.", widget)
        self.assertIn('deferred_card = not thread and provider == TELEGRAM_PROVIDER', backend)
        self.assertIn("if not state and not refresh:\n        db = await _connect()", backend)
        self.assertNotIn("if not state and not refresh and _telegram_lock.locked()", backend)
        self.assertIn("has_chat = await _has_conversation(", backend)
        self.assertEqual(widget.count('await request("/link"'), 1)
        self.assertIn("function enableHistoryScroll(feed, loader)", widget)
        self.assertIn("var conversationCache = new Map();", widget)
        self.assertIn("loadInbox(true, true)", widget)
        self.assertNotIn('await request("/inbox/read"', widget)
        self.assertIn("function findVkId()", widget)
        self.assertIn("function visibleSourceFields()", widget)
        self.assertIn('document.querySelectorAll("tr")', widget)
        self.assertIn('loadInbox(false);', widget)
        self.assertIn("vk_id: findVkId()", widget)
        self.assertIn('image.className = "message-image"', widget)
        self.assertIn('>Написать</button>', widget)
        self.assertNotIn('>Написать через Wazzup</button>', widget)
        self.assertIn('inputWrap.className = "composer-input"', widget)
        self.assertIn('inputWrap.appendChild(menu)', widget)
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
        self.assertIn('async function sendComposerText(', widget)
        self.assertIn('body: JSON.stringify(Object.assign({}, payloadFor(targets[0]), { body: rawText }))', widget)
        self.assertIn('for (var index = 0; index < targets.length; index += 1)', widget)
        self.assertIn('event.stopPropagation();', widget)
        self.assertIn('Тема<div class="themes"', widget)
        self.assertIn('Палитра<div class="palettes"', widget)
        self.assertIn('showDrawerTemplateSettings', widget)
        self.assertIn('.chat-shell{min-width:0;', widget)
        self.assertIn('grid-template-columns:minmax(0,1fr);grid-template-rows:', widget)
        self.assertIn('.layer[data-drawer-size] .drawer{width:100%;', widget)
        self.assertIn('.head{min-height:56px;display:grid;grid-template-columns:minmax(0,1fr) auto auto auto auto', widget)
        self.assertIn('.drawer-send-all{grid-column:2;grid-row:1}.copy{grid-column:3;grid-row:1}.drawer-settings{grid-column:4;grid-row:1}', widget)
        self.assertIn('.channels{grid-column:1/-1;grid-row:2;', widget)
        self.assertIn('.drawer-send-all{grid-column:1;grid-row:2}', widget)
        self.assertIn('folder || "Без папки"', widget)
        self.assertIn("Виджет мессенжеров", panel)
        self.assertIn('id="templatesView"', panel)
        self.assertIn('id="identityView"', panel)
        self.assertIn('>Сотрудники <span id="adminCount">', panel)
        self.assertIn('class="input role-select"', panel)
        self.assertIn('class="input amo-select"', panel)
        self.assertIn("api('/staff/catalog')", panel)
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

    def test_protected_test_card_loads_the_real_widget_in_test_mode(self):
        module_dir = Path(__file__).resolve().parents[1]
        page = (module_dir / "panel" / "test.html").read_text(encoding="utf-8")
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        self.assertIn('class="user-call-to-phone"', page)
        self.assertIn('+7 (996) 415-85-37', page)
        self.assertIn("entity_id:'15462823'", page)
        self.assertIn('data-nexus-wazzup-test="1"', page)
        self.assertIn('src="../static/widget.js?v=583"', page)
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

    def test_amocrm_widget_has_one_size_setting_and_valid_manifest_settings(self):
        module_dir = Path(__file__).resolve().parents[1]
        page = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        manifest = json.loads((module_dir / "amocrm_widget" / "manifest.json").read_text(encoding="utf-8"))
        self.assertIn("Размер виджета", page)
        self.assertIn("more-wrap.open-up", page)
        self.assertIn("$('feed').innerHTML='<div class=\"empty\">'+esc(error.message)+'</div>'", page)
        self.assertEqual(manifest["settings"], {
            "nexus_url": {
                "name": "settings.nexus_url",
                "type": "text",
                "required": False,
            }
        })
        amo = (module_dir / "static" / "amocrm.html").read_text(encoding="utf-8")
        self.assertIn("[hidden]{display:none!important}", amo)
        self.assertIn("button.onclick=()=>openThread(row)", amo)
        self.assertIn("placeholder=\"Имя, телефон или ID\"", amo)
        self.assertIn("PAGE=12", amo)
        self.assertIn('id="more"', amo)
        self.assertIn('id="templateSettings"', amo)
        self.assertIn('id="authClose"', amo)
        self.assertIn("nexus-messenger-close", amo)
        self.assertIn("setInterval(refreshActive,5000)", amo)
        self.assertIn('id="sendAll" type="checkbox" checked>Отправить везде', amo)
        self.assertIn("async function sendText(raw,rows,attachment)", amo)
        self.assertIn("const preview=raw?await request('/template-preview',{body:raw,...threadFields()}):{text:''}", amo)
        self.assertIn("for(const row of targets)", amo)
        self.assertIn("event.stopPropagation()", amo)
        self.assertIn('Тема<div class="choices themes"', amo)
        self.assertIn('Палитра<div class="palettes"', amo)
        self.assertIn("del.type='button'", amo)
        self.assertIn("function placeMenu()", amo)
        self.assertIn(".shell{grid-template-rows:minmax(0,1fr)}", amo)
        self.assertIn(".shell .main{min-height:0;grid-template-rows:50px auto minmax(0,1fr) auto}", amo)
        self.assertIn("@media(max-width:680px){.shell{grid-template-columns:minmax(0,1fr)}", amo)
        self.assertIn("function showActiveChannel", amo)
        self.assertIn("function wheelX(node)", amo)
        self.assertIn("wheelX($('channels'))", amo)
        self.assertIn("REQUEST_TIMEOUT=15000", amo)
        self.assertIn("Сервер не ответил за 15 секунд. Повторите.", amo)
        self.assertIn("const conversationCache=new Map()", amo)
        self.assertIn("conversationCache.get(key)", amo)
        self.assertIn("channelRefreshAttempts>=3", amo)
        self.assertIn("clearTimeout(channelRefreshTimer)", amo)
        self.assertNotIn("},3500)}", amo)
        self.assertNotIn("await request('/inbox/read'", amo)
        self.assertNotIn("await openChannel(active)", amo)
        self.assertIn("insertAdjacentHTML('beforeend'", amo)
        self.assertIn("nexus-messenger-painted", amo)
        self.assertIn("mobile-inbox", amo)
        self.assertIn(".channels button:disabled::after", amo)
        script = (module_dir / "amocrm_widget" / "script.js").read_text(encoding="utf-8")
        self.assertIn("background:'#111c25'", script)
        self.assertIn("opacity:0", script)
        self.assertIn("height:'100dvh'", script)
        self.assertIn("const CONTEXT_TIMEOUT = 2000", script)
        self.assertIn("context:await fastContext()", script)
        self.assertIn("setTimeout(paint, 1200)", script)
        self.assertIn("function armFrameDeadline()", script)
        self.assertIn("Виджет не загрузился", script)
        self.assertIn("}, 15000);", script)
        self.assertIn("addEventListener('resize',()=>showActiveChannel())", amo)
        self.assertIn("resize:'both'", script)
        self.assertIn("variable-list", amo)
        self.assertIn("setRangeText", amo)
        self.assertNotIn("scrollIntoView", amo)

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


if __name__ == "__main__":
    unittest.main()
