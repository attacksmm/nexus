import unittest
from pathlib import Path


PANEL = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")


class BizonGoogleSheetsPanelTest(unittest.TestCase):
    def test_mobile_events_have_labels_and_touch_controls(self):
        self.assertIn('@media(max-width:720px)', PANEL)
        self.assertIn('.row>span::before{content:attr(data-label)', PANEL)
        self.assertIn('data-label="Attendance key"', PANEL)
        self.assertIn('button{min-height:40px', PANEL)

    def test_retry_is_confirmed_and_bounded(self):
        self.assertIn("Math.min(10,count)", PANEL)
        self.assertIn("confirm(`Повторить ${batch} событий?`)", PANEL)
        self.assertIn("/events/retry?limit=10", PANEL)

    def test_settings_do_not_prefill_feed_token_or_render_raw_metadata(self):
        self.assertIn("$('feed_token').value=''", PANEL)
        self.assertIn("if(token)body.feed_token=token", PANEL)
        self.assertNotIn("$('meta').textContent=JSON.stringify", PANEL)
        self.assertIn('id="meta" class="status-grid full"', PANEL)


if __name__ == "__main__":
    unittest.main()
