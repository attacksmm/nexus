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

    def test_send_everywhere_really_includes_email_and_fails_closed(self):
        legacy_filter = "channels.length === 1 || channel.provider !== \"email\""
        legacy_compact_filter = "rows.length===1||row.provider!=='email'"
        self.assertNotIn(legacy_filter, self.getcourse)
        self.assertNotIn(legacy_compact_filter, self.amocrm)
        for source in (self.getcourse, self.amocrm):
            self.assertIn("send_all_allowed", source)
            self.assertIn("Укажите тему Email", source)
            self.assertIn("Уберите изображение или отключите", source)
            self.assertIn("!result.failed.length", source)

    def test_send_everywhere_always_confirms_email_and_avoids_duplicate_retry(self):
        self.assertIn("sendEverywhere ? hasEmailSendTarget(targets) : emailIsAmongSendTargets(targets)", self.getcourse)
        self.assertIn("sendEverywhere?hasEmailTarget(targets):emailNeedsRecommendations(targets)", self.amocrm)
        self.assertIn("function sendResultAccepted(result)", self.getcourse)
        self.assertIn("if (sendResultAccepted(result))", self.getcourse)
        self.assertIn("accepted=Boolean(result.sent.length||result.queued.length)", self.amocrm)
        for source in (self.getcourse, self.amocrm):
            self.assertIn("Не отправлено:", source)

    def test_delivery_failure_reason_is_visible_and_refreshes(self):
        for source in (self.getcourse, self.amocrm):
            self.assertIn("delivery-error", source)
            self.assertIn("error_message", source)

    def test_send_controls_have_spinner_and_errors_are_live(self):
        self.assertIn(".send.busy:before", self.getcourse)
        self.assertIn(".send.busy::before", self.amocrm)
        self.assertIn('setAttribute("role", "alert")', self.getcourse)
        self.assertIn('role="alert" aria-live="polite"', self.amocrm)

    def test_send_result_contract_and_status_colors_are_consistent(self):
        self.assertIn("return{sent,queued,failed,status:", self.amocrm)
        self.assertIn("function sendResultSucceeded(result)", self.getcourse)
        self.assertIn('node.classList.toggle("success"', self.getcourse)
        self.assertIn("node.classList.toggle('success'", self.amocrm)
        self.assertIn(".error.success{color:#278a45}", self.amocrm)
        self.assertIn("showError(result.status,success?'success':'error')", self.amocrm)
        self.assertEqual(self.getcourse.count("setComposeStatus(errorNode, result.status, success)"), 2)

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

    def test_fullscreen_messenger_is_scoped_to_the_linked_deal(self):
        module = Path(__file__).resolve().parents[1]
        page = (module / "static" / "mobile.html").read_text(encoding="utf-8")
        script = (module / "static" / "mobile.js").read_text(encoding="utf-8")
        self.assertNotIn('id="sidebar"', page)
        self.assertNotIn('id="search"', page)
        self.assertNotIn('id="filters"', page)
        self.assertIn('id="clientName"', page)
        self.assertIn('id="back"', page)
        self.assertNotIn("maximum-scale", page)
        self.assertNotIn("user-scalable=no", page)
        self.assertNotIn("request('/inbox'", script)
        self.assertNotIn("request('/inbox/read'", script)
        self.assertIn("request('/mobile-context'", script)
        self.assertIn("FRAME+'?standalone=1&v=5210'", script)
        self.assertNotIn('id="appearanceButton"', page)
        self.assertNotIn('id="appearanceClose"', page)
        self.assertNotIn('id="refreshInbox"', page)
        self.assertNotIn('Каналы подключены', page)
        self.assertIn('data-theme-choice="light"', page)
        self.assertIn('data-scale-choice="xlarge"', page)
        self.assertNotIn("loadInbox", script)
        self.assertNotIn("scheduleInboxPoll", script)
        self.assertIn("nexus-messenger-fullscreen-preferences", script)
        self.assertIn("const timeoutMs=15000", script)
        self.assertIn("lastPostedContext", script)
        self.assertIn("signature!==lastPostedContext", script)
        self.assertIn("controller.abort(),5000", script)

    def test_standalone_mode_does_not_change_embedded_widget_defaults(self):
        self.assertIn("STANDALONE=new URLSearchParams(location.search).get('standalone')==='1'", self.amocrm)
        self.assertIn("nexus-messenger-fullscreen-preferences", self.amocrm)
        self.assertIn("document.documentElement.dataset.uiScale=scale", self.amocrm)
        self.assertNotIn("theme=STANDALONE?'dark'", self.amocrm)
        self.assertIn("if(!STANDALONE)parent.postMessage({type:'nexus-messenger-resize'", self.amocrm)
        self.assertIn("context?.inbox_thread||null", self.amocrm)
        self.assertIn("scope:'inbox'", self.amocrm)
        self.assertIn('html[data-standalone="1"] #close{display:none}', self.amocrm)
        self.assertIn("if(!STANDALONE)loadProfileLinks", self.amocrm)
        self.assertIn("if(token&&!STANDALONE)syncMobileLink()", self.amocrm)
        self.assertIn("while(expectedGeneration===bootGeneration&&token&&widgetVisible)", self.amocrm)
        self.assertIn("if(bootPaused){bootPaused=false;boot();return}", self.amocrm)

    def test_fullscreen_layout_is_telegram_like_without_css_zoom(self):
        module = Path(__file__).resolve().parents[1]
        page = (module / "static" / "mobile.html").read_text(encoding="utf-8")
        script = (module / "static" / "mobile.js").read_text(encoding="utf-8")
        self.assertIn('id="telegram-fullscreen-v2"', page)
        self.assertIn('id="telegram-standalone-v2"', self.amocrm)
        self.assertIn("zoom:1!important", self.amocrm)
        self.assertIn(".head{display:none!important}", self.amocrm)
        self.assertNotIn("request('/channels'", script)
        self.assertIn('id="conversationSettings"', page)
        self.assertIn("$('conversationSettings').onclick=toggleAppearance", script)
        self.assertNotIn("nexus-messenger-open-settings", script)
        self.assertIn("data:image/svg+xml", self.amocrm)

    def test_amocrm_enriched_context_replaces_incomplete_fast_context(self):
        bootstrap = (Path(__file__).resolve().parents[1] / "amocrm_widget" / "script.js").read_text(encoding="utf-8")
        self.assertIn("startEnrichment()", bootstrap)
        self.assertIn("completeness:completeness", bootstrap)
        self.assertIn("postContext(enrichedContext, 'enriched')", bootstrap)
        self.assertIn("function mergeContext", self.amocrm)
        self.assertIn("function contextRoutingSignature", self.amocrm)
        self.assertIn("if(routingChanged||!snapshotMatches)enrichCardContext(context)", self.amocrm)

    def test_getcourse_does_not_boot_the_global_inbox(self):
        boot = self.getcourse.split("function boot()", 1)[1].split("if (document.readyState", 1)[0]
        self.assertNotIn("ensureInbox()", boot)
        self.assertNotIn("scheduleInboxPoll()", boot)
        self.assertIn("if (staleInbox) staleInbox.remove()", self.getcourse)

    def test_fullscreen_conversation_is_bounded_to_the_visible_viewport(self):
        module = Path(__file__).resolve().parents[1]
        page = (module / "static" / "mobile.html").read_text(encoding="utf-8")
        router = (module / "router.py").read_text(encoding="utf-8")
        self.assertIn(".workspace{height:100vh!important;height:100dvh!important;overflow:hidden!important}", page)
        self.assertIn(".frame{position:absolute!important", page)
        self.assertIn("height:100%!important", page)
        self.assertIn("ORDER BY sent_at DESC,id DESC LIMIT 200", router)
        self.assertIn("base=STANDALONE?(innerWidth<=680?48:52):76", self.amocrm)


if __name__ == "__main__":
    unittest.main()
