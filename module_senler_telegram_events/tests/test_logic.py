import unittest

import router


class TelegramEventDescriptionTests(unittest.TestCase):
    def test_all_bot_api_10_2_update_types_are_registered(self):
        expected = {
            "message", "edited_message", "channel_post", "edited_channel_post",
            "business_connection", "business_message", "edited_business_message",
            "deleted_business_messages", "guest_message", "message_reaction",
            "message_reaction_count", "inline_query", "chosen_inline_result",
            "callback_query", "shipping_query", "pre_checkout_query",
            "purchased_paid_media", "poll", "poll_answer", "my_chat_member",
            "chat_member", "chat_join_request", "chat_boost", "removed_chat_boost",
            "managed_bot", "subscription",
        }
        self.assertEqual(router.TELEGRAM_BOT_API_VERSION, "10.2")
        self.assertEqual(len(router.UPDATE_TYPES), 26)
        self.assertEqual(set(router.UPDATE_TYPES), expected)

    def test_text_message_contains_username_and_text(self):
        result = router.describe_update(
            {
                "update_id": 10,
                "message": {
                    "message_id": 22,
                    "from": {"id": 100, "username": "ivan", "first_name": "Иван"},
                    "chat": {"id": 100, "type": "private", "username": "ivan"},
                    "text": "Привет, Nexus!",
                },
            }
        )
        self.assertEqual(result["event_type"], "message")
        self.assertEqual(result["summary"], "Сообщение от @ivan: Привет, Nexus!")
        self.assertEqual(result["actor_id"], "100")
        self.assertEqual(result["actor_username"], "ivan")
        self.assertEqual(result["message_id"], "22")

    def test_voice_message_contains_duration(self):
        result = router.describe_update(
            {
                "update_id": 11,
                "message": {
                    "from": {"id": 101, "first_name": "Анна"},
                    "chat": {"id": 101, "type": "private"},
                    "voice": {"duration": 78, "file_id": "voice-file"},
                },
            }
        )
        self.assertEqual(result["summary"], "Голосовое сообщение · 01:18 от Анна")

    def test_photo_caption_and_callback_are_human_readable(self):
        photo = router.describe_update(
            {
                "update_id": 12,
                "message": {
                    "from": {"id": 102, "username": "photo_user"},
                    "chat": {"id": 102, "type": "private"},
                    "photo": [{"file_id": "small"}, {"file_id": "large"}],
                    "caption": "Смотрите фото",
                },
            }
        )
        callback = router.describe_update(
            {
                "update_id": 13,
                "callback_query": {
                    "from": {"id": 103, "username": "button_user"},
                    "data": "confirm_order",
                    "message": {"message_id": 9, "chat": {"id": 103, "type": "private"}},
                },
            }
        )
        self.assertEqual(photo["summary"], "Фото: Смотрите фото от @photo_user")
        self.assertEqual(callback["summary"], "Нажатие кнопки от @button_user: confirm_order")

    def test_member_change_and_unknown_update(self):
        member = router.describe_update(
            {
                "update_id": 14,
                "my_chat_member": {
                    "chat": {"id": -1001, "type": "supergroup", "title": "Клуб"},
                    "from": {"id": 104, "username": "admin"},
                    "old_chat_member": {"status": "member", "user": {"id": 999, "username": "bot"}},
                    "new_chat_member": {"status": "kicked", "user": {"id": 999, "username": "bot"}},
                },
            }
        )
        unknown = router.describe_update({"update_id": 15, "brand_new_update": {"value": 1}})
        self.assertEqual(member["summary"], "Статус бота в Клуб: member → kicked")
        self.assertIn("brand_new_update", unknown["summary"])
        self.assertEqual(unknown["event_type"], "brand_new_update")

    def test_current_message_content_and_service_events_are_human_readable(self):
        cases = {
            "checklist": ({"title": "Запуск"}, "Чек-лист · Запуск от @tester"),
            "chat_owner_changed": ({}, "Владелец чата изменён от @tester"),
            "checklist_tasks_added": ({"checklist_message_id": 3}, "В чек-лист добавлены задачи от @tester"),
            "managed_bot_created": ({"managed_bot_user_id": 9}, "Создан управляемый бот от @tester"),
            "poll_option_added": ({"option": {"text": "Да"}}, "В опрос добавлен вариант ответа от @tester"),
            "suggested_post_paid": ({"currency": "XTR"}, "Предложенная публикация оплачена от @tester"),
        }
        for field, (value, expected) in cases.items():
            with self.subTest(field=field):
                result = router.describe_update(
                    {
                        "update_id": 100,
                        "message": {
                            "message_id": 7,
                            "from": {"id": 1, "username": "tester"},
                            "chat": {"id": 1, "type": "private"},
                            field: value,
                        },
                    }
                )
                self.assertEqual(result["summary"], expected)

    def test_secret_hash_is_source_bound_and_does_not_contain_secret(self):
        first = router._secret_hash("source-a", "top-secret")
        second = router._secret_hash("source-b", "top-secret")
        self.assertNotEqual(first, second)
        self.assertNotIn("top-secret", first)


if __name__ == "__main__":
    unittest.main()
