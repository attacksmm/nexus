from datetime import timezone
import unittest
import asyncio

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

    def test_random_id_is_stable_and_nonzero(self):
        first = router._stable_random_id("campaign", "42")
        self.assertEqual(first, router._stable_random_id("campaign", "42"))
        self.assertNotEqual(first, router._stable_random_id("campaign", "43"))
        self.assertTrue(0 < first <= 0x7FFFFFFF)

    def test_schedule_is_normalized_to_utc(self):
        value = router._parse_schedule("2026-06-19T15:00:00+03:00")
        self.assertEqual(value, "2026-06-19T12:00:00Z")
        parsed = router.datetime.fromisoformat(value.replace("Z", "+00:00"))
        self.assertEqual(parsed.tzinfo, timezone.utc)

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


if __name__ == "__main__":
    unittest.main()
