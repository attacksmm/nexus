import json
import unittest
from pathlib import Path
import importlib.util


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("bizon_reports_logic", ROOT / "logic.py")
logic = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(logic)


class BizonReportsLogicTest(unittest.TestCase):
    def test_forward_url_allowlist(self):
        self.assertTrue(logic.is_allowed_forward_url("https://vakas-tools.ru/base/report/8ff92ef/96690/"))
        self.assertFalse(logic.is_allowed_forward_url("http://vakas-tools.ru/base/report/8ff92ef/96690/"))
        self.assertFalse(logic.is_allowed_forward_url("https://evil.example/base/report/8ff92ef/96690/"))
        self.assertFalse(logic.is_allowed_forward_url("https://vakas-tools.ru.evil.example/base/report/8ff92ef/96690/"))

    def test_extracts_viewers_from_plain_webhook(self):
        payload = {
            "webinarId": "room:test*2026-07-01T10:00:00",
            "viewers": [
                {"chatUserId": "abc", "email": "a@example.com", "username": "Анна"},
                {"uid": "42", "phone": "+79990001122"},
            ],
        }
        records = logic.normalize_viewers(logic.extract_viewers(payload), logic.report_meta_from_payload(payload))
        self.assertEqual([r["platform_id"] for r in records], ["bizon:abc", "bizon_uid:42"])
        self.assertEqual(records[0]["custom_fields"]["webinarId"], payload["webinarId"])
        self.assertEqual(records[0]["custom_fields"]["platform"], "bizon365")

    def test_extracts_viewers_from_packed_bizon_report(self):
        packed = {
            "usersMeta": {
                "u1": {"chatUserId": "c1", "username": "User 1"},
                "u2": {"email": "u2@example.com"},
            },
            "messages": {"u1": ["привет"]},
            "messagesTS": {"u1": [12]},
        }
        payload = {"report": {"webinarId": "w1", "report": json.dumps(packed, ensure_ascii=False)}}
        viewers = logic.extract_viewers(payload)
        self.assertEqual(len(viewers), 2)
        self.assertEqual(viewers[0]["messages"], ["привет"])
        records = logic.normalize_viewers(viewers, logic.report_meta_from_payload(payload))
        self.assertEqual(records[0]["platform_id"], "bizon:c1")
        self.assertEqual(records[1]["platform_id"], "email:u2@example.com")

    def test_extracts_sibling_messages_from_full_report(self):
        payload = {
            "report": {
                "webinarId": "w1",
                "report": {"usersMeta": {"u1": {"chatUserId": "c1", "username": "User 1"}}},
                "messages": {"u1": ["10", "Мой вопрос"]},
                "messagesTS": {"u1": [52, 136]},
            }
        }
        viewers = logic.extract_viewers(payload)
        self.assertEqual(viewers[0]["messages"], ["10", "Мой вопрос"])
        attendance = logic.normalize_attendances(viewers, {"webinarId": "w1"})[0]["custom_fields"]
        self.assertEqual(attendance["chat_message_count"], 2)
        self.assertEqual(attendance["chat_messages_text"], "[00:52] 10\n[02:16] Мой вопрос")

    def test_merges_chat_from_repeated_profiles(self):
        records = logic.normalize_attendances(
            [
                {"phone": "+79990001122", "messages": ["Первое"], "messagesTS": [60]},
                {"phone": "89990001122", "messages": ["Второе"], "messagesTS": [120]},
            ],
            {"webinarId": "w1"},
        )
        fields = records[0]["custom_fields"]
        self.assertEqual(fields["chat_message_count"], 2)
        self.assertIn("[01:00] Первое", fields["chat_messages_text"])
        self.assertIn("[02:00] Второе", fields["chat_messages_text"])

    def test_sanitizes_secrets(self):
        self.assertEqual(logic.sanitize_payload({"secret": "x", "email": "a@example.com"}), {"email": "a@example.com"})

    def test_aggregates_repeated_entries_and_merges_overlaps(self):
        records = logic.normalize_attendances(
            [
                {"username": "Анна", "phone": "8 (999) 111-22-33", "vi": [{"s": 0, "e": 2400}]},
                {"username": "Анна 2", "phone": "+7 999 111 22 33", "vi": [{"s": 2300, "e": 4900}]},
            ],
            {"webinarId": "97242:puppy*2026-07-13T12:00:00", "roomid": "97242:puppy"},
        )
        self.assertEqual(len(records), 1)
        fields = records[0]["custom_fields"]
        self.assertEqual(fields["profile_count"], 2)
        self.assertEqual(fields["watch_seconds"], 4900)
        self.assertAlmostEqual(fields["watch_minutes"], 81.667, places=3)
        self.assertTrue(fields["watch_valid"])

    def test_attendance_key_is_stable_per_person_and_webinar(self):
        viewer = {"email": "USER@example.com", "vi": [{"s": 10, "e": 100}]}
        first = logic.normalize_attendances([viewer], {"webinarId": "web-1"})[0]
        same = logic.normalize_attendances([{**viewer, "username": "Новое имя"}], {"webinarId": "web-1"})[0]
        other = logic.normalize_attendances([viewer], {"webinarId": "web-2"})[0]
        self.assertEqual(first["platform_id"], same["platform_id"])
        self.assertNotEqual(first["platform_id"], other["platform_id"])

    def test_attendance_key_does_not_change_when_full_report_adds_bizon_user_id(self):
        brief = logic.normalize_attendances(
            [{"chatUserId": "chat-42", "phone": "+79990001122"}],
            {"webinarId": "web-1"},
        )[0]
        full = logic.normalize_attendances(
            [{"chatUserId": "chat-42", "bizon_user_id": "chat-42", "phone": "+79990001122"}],
            {"webinarId": "web-1"},
        )[0]
        self.assertEqual(brief["platform_id"], full["platform_id"])
        self.assertEqual(full["custom_fields"]["person_key"], "chatUserId:chat-42")

    def test_invalid_absolute_duration_is_rejected(self):
        record = logic.normalize_attendances(
            [{"phone": "+79990000000", "view": 1_700_000_000_000, "viewTill": 1_900_000_000_000}],
            {"webinarId": "web-1"},
        )[0]["custom_fields"]
        self.assertFalse(record["watch_valid"])
        self.assertEqual(record["watch_error"], "duration_out_of_range")

    def test_derives_webinar_time_and_click_flags(self):
        record = logic.normalize_attendances(
            [{"email": "test@example.com", "buttons": [{"id": "offer"}], "banners": []}],
            {"webinarId": "97242:puppy*2026-07-13T19:00:00"},
        )[0]["custom_fields"]
        self.assertEqual(record["webinar_at"], "2026-07-13T19:00:00")
        self.assertTrue(record["clicked_button"])
        self.assertFalse(record["clicked_banner"])

    def test_uses_referer_attribution_only_as_fallback(self):
        viewer = {
            "chatUserId": "user-42",
            "utm_source": "explicit",
            "referer": (
                "https://start.bizon365.ru/room/97242/puppy?utm_source=url"
                "&utm_medium=cpc&utm_campaign=summer%20sale&utm_content=banner&utm_term=42"
                "&param1=ym-123&param2=segment"
            ),
        }
        client = logic.normalize_viewer(viewer)["custom_fields"]
        attendance = logic.normalize_attendances([viewer], {"webinarId": "web-1"})[0]["custom_fields"]
        expected = {
            "utm_source": "explicit",
            "utm_medium": "cpc",
            "utm_campaign": "summer sale",
            "utm_content": "banner",
            "utm_term": "42",
            "p1": "ym-123",
            "p2": "segment",
        }
        self.assertEqual({key: client[key] for key in expected}, expected)
        self.assertEqual({key: attendance[key] for key in expected}, expected)


if __name__ == "__main__":
    unittest.main()
