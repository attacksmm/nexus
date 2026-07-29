import os
import unittest

os.environ.setdefault("NEXUS_SECRET", "unit-test-secret-at-least-32-characters")

import router


class ProxyLogicTests(unittest.TestCase):
    def test_public_base_requires_https(self):
        self.assertEqual(router._public_base("https://example.test/nexus/"), "https://example.test/nexus")
        with self.assertRaises(ValueError):
            router._public_base("http://example.test/nexus")

    def test_only_salebot_targets_are_allowed(self):
        self.assertTrue(router._is_salebot_url("https://chatter.salebot.pro/tg/secret"))
        self.assertTrue(router._is_salebot_url("https://api.salebot.pro/hook"))
        self.assertFalse(router._is_salebot_url("https://salebot.pro.evil.test/hook"))
        self.assertFalse(router._is_salebot_url("https://127.0.0.1/hook"))

    def test_secret_values_round_trip_and_are_masked(self):
        value = "123456:secret-bot-token"
        encrypted = router._encrypt(value)
        self.assertNotIn(value, encrypted)
        self.assertEqual(router._decrypt(encrypted), value)
        masked = router._mask_url("https://chatter.salebot.pro/telegram/a-very-secret-path?token=secret")
        self.assertEqual(masked, "https://chatter.salebot.pro/telegram/••••")
        self.assertNotIn("secret", masked)

    def test_event_metadata_does_not_store_message_text(self):
        payload = {
            "update_id": 42,
            "message": {
                "text": "private message",
                "chat": {"id": 1001},
                "from": {"id": 2002},
            },
        }
        self.assertEqual(router._event_meta(payload), ("message", "1001", "2002"))

    def test_salebot_callback_maps_telegram_update(self):
        payload = {
            "update_id": 43,
            "message": {
                "text": "hello",
                "chat": {"id": 1001},
                "from": {"id": 2002},
            },
        }

        callback = router._salebot_callback_payload({"username": "@example_bot"}, payload)

        self.assertEqual(callback["message"], "hello")
        self.assertEqual(callback["user_id"], "2002")
        self.assertEqual(callback["group_id"], "example_bot")
        self.assertEqual(callback["telegram_update_id"], "43")
        self.assertTrue(callback["resume_bot"])
        self.assertEqual(__import__("json").loads(callback["tg_request"]), payload)

    def test_salebot_callback_maps_button_press_without_message_text(self):
        payload = {
            "update_id": 44,
            "callback_query": {
                "id": "callback-id",
                "from": {"id": 2003},
                "data": "button-value",
                "message": {"chat": {"id": 1002}},
            },
        }

        callback = router._salebot_callback_payload({"username": "example_bot"}, payload)

        self.assertEqual(callback["message"], "button-value")
        self.assertEqual(callback["user_id"], "2003")


if __name__ == "__main__":
    unittest.main()
