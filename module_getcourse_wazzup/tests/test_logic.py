import unittest
from pathlib import Path

import router


class GetCourseWazzupLogicTests(unittest.TestCase):
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
        self.assertEqual(channels, [{"channel_id": "max-1", "transport": "max", "channel_transport": "max", "name": "Служба заботы", "plain_id": "79990000000"}])

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

    def test_protected_test_card_loads_the_real_widget_in_test_mode(self):
        module_dir = Path(__file__).resolve().parents[1]
        page = (module_dir / "panel" / "test.html").read_text(encoding="utf-8")
        widget = (module_dir / "static" / "widget.js").read_text(encoding="utf-8")
        self.assertIn('class="user-call-to-phone"', page)
        self.assertIn('+7 (910) 875-84-27', page)
        self.assertIn('data-nexus-wazzup-test="1"', page)
        self.assertIn('src="../static/widget.js?v=6"', page)
        self.assertIn('"X-Nexus-Wazzup-Test": "1"', widget)
        self.assertIn("TEST_SOURCE_URL", widget)
        self.assertIn('request("/channels"', widget)


if __name__ == "__main__":
    unittest.main()
