import unittest
from pathlib import Path


PANEL = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")


class BizonReportsPanelTest(unittest.TestCase):
    def test_mobile_layout_has_cards_and_full_width_controls(self):
        self.assertIn("@media(max-width:600px)", PANEL)
        self.assertIn('tr.event-row{display:grid', PANEL)
        self.assertIn('.side.is-open{display:grid}', PANEL)
        self.assertIn('.f-input,.btn{min-height:40px}', PANEL)
        self.assertNotIn('style="grid-template-columns:', PANEL)

    def test_event_rows_are_keyboard_accessible(self):
        self.assertIn('tabindex="0" role="button" aria-label="Событие', PANEL)
        self.assertIn("event.key === 'Enter' || event.key === ' '", PANEL)
        self.assertIn('class="btn event-back"', PANEL)

    def test_settings_do_not_render_secrets_or_env_metadata(self):
        self.assertIn('id="webhookSecret" class="f-input mono" type="password"', PANEL)
        self.assertIn("if(secret) payload.webhook_secret = secret", PANEL)
        render_settings = PANEL.split("function renderSettings(){", 1)[1].split("async function loadSettings", 1)[0]
        self.assertNotIn("settings.feed_token", render_settings)
        self.assertNotIn("settings.env_path", render_settings)
        self.assertNotIn("$('webhookSecret').value = settings.webhook_secret", render_settings)


if __name__ == "__main__":
    unittest.main()
