import unittest

from module_openrouter import router


class ClientSearchTests(unittest.TestCase):
    def test_search_is_case_insensitive_and_treats_yo_as_ye(self):
        summary = "**Собака:** Ёжик, корги, 7 месяцев."

        self.assertIsNotNone(router._client_summary_match("ежик", summary))
        self.assertIsNotNone(router._client_summary_match("КОРГИ 7", summary))

    def test_all_query_terms_must_be_in_one_summary(self):
        summary = "Собака Лана, лабрадор, 2 месяца."

        self.assertIsNotNone(router._client_summary_match("Лана лабрадор", summary))
        self.assertIsNone(router._client_summary_match("Лана корги", summary))
        self.assertIsNone(router._client_summary_match("Лана", "У клиента нет плана занятий."))

    def test_platform_id_can_be_found_directly(self):
        score = router._client_summary_match("470465", "Собака Лана.", "470465197")

        self.assertGreaterEqual(score, 19_000)

    def test_account_links_follow_real_message_sources(self):
        links = router._client_account_links("470465197", ["senler"], {})

        self.assertEqual(
            [item["url"] for item in links],
            [
                "https://vk.com/gim225075265/convo/470465197",
                "https://vk.com/id470465197",
            ],
        )
        self.assertEqual(router._client_account_links("470465197", ["api"], {}), [])

    def test_avito_customer_links_are_reused_and_sanitized(self):
        links = router._client_account_links(
            "u2i-example",
            ["avito"],
            {
                "avito_url": "https://www.avito.ru/profile/messenger/channel/u2i-example",
                "salebot_url": "https://salebot.pro/projects/397724/clients/123",
            },
        )

        self.assertEqual([item["kind"] for item in links], ["avito", "salebot"])


if __name__ == "__main__":
    unittest.main()
