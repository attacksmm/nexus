import unittest

from module_openrouter import router


class ReplayBonusFormatTests(unittest.TestCase):
    def test_formats_only_replay_and_bonus_boundaries_for_senler_prompt(self):
        answer = (
            "Основной текст. 😊 Запись:\n"
            "https://example.test/replay Бонусы:\n"
            "Видео 1\thttps://example.test/one\n"
            "Видео 2\thttps://example.test/two"
        )

        self.assertEqual(
            router._format_replay_bonus_blocks(answer, "prompts/senler/puppy_gpt5_senler.txt"),
            (
                "Основной текст. 😊\n\n"
                "Запись:\n"
                "https://example.test/replay\n\n"
                "Бонусы:\n"
                "Видео 1\thttps://example.test/one\n"
                "Видео 2\thttps://example.test/two"
            ),
        )

    def test_formats_salebot_prompt(self):
        answer = "Текст. Запись: https://example.test/replay Бонусы: Видео https://example.test/bonus"

        formatted = router._format_replay_bonus_blocks(answer, "prompts/puppy_gpt5.txt")

        self.assertEqual(
            formatted,
            "Текст.\n\nЗапись:\nhttps://example.test/replay\n\nБонусы:\nВидео https://example.test/bonus",
        )

    def test_other_prompts_are_byte_for_byte_unchanged(self):
        answer = "Текст. Запись: https://example.test/replay Бонусы: Видео https://example.test/bonus"

        self.assertIs(
            router._format_replay_bonus_blocks(answer, "prompts/senler/dog_gpt5_senler.txt"),
            answer,
        )

    def test_missing_label_is_unchanged(self):
        answer = "Текст. Запись: https://example.test/replay"

        self.assertIs(
            router._format_replay_bonus_blocks(answer, "prompts/senler/puppy_gpt5_senler.txt"),
            answer,
        )

    def test_already_formatted_answer_is_idempotent(self):
        answer = "Текст.\n\nЗапись:\nhttps://example.test/replay\n\nБонусы:\nВидео https://example.test/bonus"

        self.assertEqual(
            router._format_replay_bonus_blocks(answer, "prompts/puppy_gpt5.txt"),
            answer,
        )


if __name__ == "__main__":
    unittest.main()
