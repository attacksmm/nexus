import unittest
from pathlib import Path


class EmailGuardUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        module = Path(__file__).resolve().parents[1]
        cls.getcourse = (module / "static" / "widget.js").read_text(encoding="utf-8")
        cls.amocrm = (module / "static" / "amocrm.html").read_text(encoding="utf-8")

    def test_both_widgets_confirm_the_same_three_recommendations(self):
        recommendations = (
            "В письме понятно, откуда у нас контакт.",
            "Тема честная, без обмана и кликбейта.",
            "Письмо — обычный текст без вложений и тяжёлых файлов.",
        )
        for source in (self.getcourse, self.amocrm):
            for recommendation in recommendations:
                self.assertIn(recommendation, source)
            self.assertIn(">Да, отправить</button>", source)
            self.assertIn(">Нет, отменить</button>", source)

    def test_confirmation_covers_every_email_send_surface(self):
        self.assertEqual(self.getcourse.count("!await confirmEmailRecommendations("), 2)
        self.assertEqual(self.amocrm.count("!await confirmEmailRecommendations("), 1)
        self.assertIn("emailIsAmongSendTargets([channel])", self.getcourse)
        self.assertIn("emailIsAmongSendTargets(targets)", self.getcourse)
        self.assertIn("emailNeedsRecommendations(targets)", self.amocrm)

    def test_email_acknowledgement_is_versioned_and_email_only(self):
        for source in (self.getcourse, self.amocrm):
            self.assertIn("email_guidelines_confirmed", source)
            self.assertIn("email_guidelines_version", source)
            self.assertIn("2026-09-01", source)
        self.assertIn('channel.provider === "email" ?', self.getcourse)
        self.assertIn("row.provider==='email'?", self.amocrm)

    def test_send_controls_have_spinner_and_errors_are_live(self):
        self.assertIn(".send.busy:before", self.getcourse)
        self.assertIn(".send.busy::before", self.amocrm)
        self.assertIn('setAttribute("role", "alert")', self.getcourse)
        self.assertIn('role="alert" aria-live="polite"', self.amocrm)

    def test_same_card_reopen_preserves_state_and_pauses_polling(self):
        bootstrap = (Path(__file__).resolve().parents[1] / "amocrm_widget" / "script.js").read_text(encoding="utf-8")
        self.assertIn("REMOTE_CACHE_WINDOW_MS", bootstrap)
        self.assertIn("function saveDraft()", self.amocrm)
        self.assertIn("active_channel_key", self.amocrm)
        self.assertIn("cards:v2", self.amocrm)
        self.assertIn("function pauseConversationPoll()", self.getcourse)
        self.assertNotIn("drawer.host.remove();\n      drawer = null", self.getcourse)

    def test_email_toolbar_belongs_to_message_and_send_stretches(self):
        composer = self.amocrm.split('<div class="composer">', 1)[1].split('</section>', 1)[0]
        self.assertLess(composer.index('id="emailSubject"'), composer.index('class="message-wrap"'))
        self.assertIn(".composer>.send{height:auto;align-self:stretch}", self.amocrm)
        self.assertIn(".composer>.send{height:auto;align-self:stretch}", self.getcourse)
        self.assertIn('class="email-subject"', self.getcourse)

    def test_channel_order_is_normalized_before_paint(self):
        self.assertIn("normalizeChannels(data.channels)", self.getcourse)
        self.assertIn("normalizeChannels(data.channels)", self.amocrm)
        self.assertIn("channelIdentity(channel) === channelIdentity(selected)", self.getcourse)


if __name__ == "__main__":
    unittest.main()
