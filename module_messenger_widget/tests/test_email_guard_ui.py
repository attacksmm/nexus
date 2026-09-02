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
        self.assertIn("let modalState = null", bootstrap)
        self.assertIn("postVisibility(false)", bootstrap)
        self.assertIn("modalState.layer.attr('aria-hidden', 'false').css('display', 'grid')", bootstrap)
        self.assertNotIn("$('#nexus-messenger-modal').remove()", bootstrap)
        self.assertIn("CARD_RESUME_TTL=5*60*1000", self.amocrm)
        self.assertNotIn("if(widgetVisible)refreshActive(true)", self.amocrm)
        self.assertNotIn("fetchConversation(activeChannel, feed, true).catch", self.getcourse)

    def test_email_toolbar_belongs_to_message_and_send_stretches(self):
        composer = self.amocrm.split('<div class="composer">', 1)[1].split('</section>', 1)[0]
        self.assertLess(composer.index('id="emailSubject"'), composer.index('class="message-wrap"'))
        self.assertIn(".composer>.send{height:auto;align-self:stretch}", self.amocrm)
        self.assertIn(".composer>.send{height:auto;align-self:stretch}", self.getcourse)
        self.assertIn(".composer .message-wrap textarea{display:block", self.amocrm)
        self.assertIn(".composer-input textarea{display:block", self.getcourse)
        self.assertIn(".composer>.error:empty{display:none}", self.amocrm)
        self.assertIn('class="email-subject"', self.getcourse)

    def test_channel_order_is_normalized_before_paint(self):
        self.assertIn("normalizeChannels(data.channels)", self.getcourse)
        self.assertIn("normalizeChannels(data.channels)", self.amocrm)
        self.assertIn("channelIdentity(channel) === channelIdentity(selected)", self.getcourse)

    def test_protected_mobile_workspace_has_explicit_auth_and_loading_states(self):
        module = Path(__file__).resolve().parents[1]
        page = (module / "static" / "mobile.html").read_text(encoding="utf-8")
        script = (module / "static" / "mobile.js").read_text(encoding="utf-8")
        self.assertIn('name="robots" content="noindex,nofollow,noarchive,nosnippet"', page)
        self.assertIn("Проверяем безопасный доступ…", page)
        self.assertIn("Доступ разрешён только сотрудникам отдела продаж", page)
        self.assertIn('autocomplete="one-time-code"', page)
        self.assertIn("/mobile-context", script)
        self.assertIn("Authorization':'Bearer '+token", script)
        self.assertNotIn("innerHTML", script)
        self.assertIn("syncMobileLink()", self.amocrm)

    def test_fullscreen_messenger_has_inbox_search_and_responsive_sidebar(self):
        module = Path(__file__).resolve().parents[1]
        page = (module / "static" / "mobile.html").read_text(encoding="utf-8")
        script = (module / "static" / "mobile.js").read_text(encoding="utf-8")
        self.assertIn('id="sidebar"', page)
        self.assertIn('id="search"', page)
        self.assertIn('id="filters"', page)
        self.assertIn('aria-label="Диалоги"', page)
        self.assertIn("@media(max-width:760px)", page)
        self.assertNotIn("maximum-scale", page)
        self.assertNotIn("user-scalable=no", page)
        self.assertIn("request('/inbox'", script)
        self.assertIn("request('/inbox/read'", script)
        self.assertIn("inbox_thread:thread", script)
        self.assertIn("FRAME+'?standalone=1'", script)

    def test_standalone_mode_does_not_change_embedded_widget_defaults(self):
        self.assertIn("STANDALONE=new URLSearchParams(location.search).get('standalone')==='1'", self.amocrm)
        self.assertIn("theme=STANDALONE?'dark'", self.amocrm)
        self.assertIn("if(!STANDALONE)parent.postMessage({type:'nexus-messenger-resize'", self.amocrm)
        self.assertIn("context?.inbox_thread||null", self.amocrm)
        self.assertIn("scope:'inbox'", self.amocrm)
        self.assertIn('html[data-standalone="1"] #close{display:none}', self.amocrm)


if __name__ == "__main__":
    unittest.main()
