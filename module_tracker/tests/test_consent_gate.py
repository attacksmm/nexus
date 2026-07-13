import ast
import html
import http.server
import pathlib
import re
import shutil
import subprocess
import tempfile
import threading
import unittest


MODULE_DIR = pathlib.Path(__file__).resolve().parents[1]
ROUTER_PATH = MODULE_DIR / "router.py"


def source_constant(name: str) -> str:
    tree = ast.parse(ROUTER_PATH.read_text(encoding="utf-8"))
    node = next(
        item
        for item in tree.body
        if isinstance(item, ast.Assign)
        and len(item.targets) == 1
        and isinstance(item.targets[0], ast.Name)
        and item.targets[0].id == name
    )
    value = node.value
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute) and value.func.attr == "strip":
        value = value.func.value
    return ast.literal_eval(value).strip()


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        return


class ConsentGateTests(unittest.TestCase):
    def test_standalone_snippet_does_not_bundle_third_party_metrics(self):
        source = ROUTER_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        function = next(item for item in tree.body if isinstance(item, ast.AsyncFunctionDef) and item.name == "snippet")
        snippet_source = ast.get_source_segment(source, function) or ""
        for forbidden in (
            "SOBAKOVOD_CONSENT_SCRIPTS",
            "96682515",
            "3565736",
            "VK-RTRG-1970487-gNb2D",
            "nexus/senler/api/track",
        ):
            self.assertNotIn(forbidden, snippet_source)
        self.assertIn("CONSENT_GATE_SCRIPT", snippet_source)
        self.assertIn('data-consent="managed"', snippet_source)
        self.assertIn('data-consent-domain=".sobakovod.pro"', snippet_source)

    def test_scripts_are_valid_javascript(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        for name in ("CONSENT_GATE_SCRIPT", "TRACKER_SCRIPT"):
            result = subprocess.run(
                [node, "--check", "-"],
                input=source_constant(name),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_standalone_gate_holds_then_releases_selected_categories(self):
        chrome = shutil.which("google-chrome")
        if not chrome:
            self.skipTest("google-chrome is unavailable")

        gate = source_constant("CONSENT_GATE_SCRIPT")
        tracker = source_constant("TRACKER_SCRIPT")
        harness = f"""<!doctype html><html><head><meta charset=\"utf-8\">
<script>
window.__metricCalls=[];
window.fetch=function(url, options){{
  window.__metricCalls.push(String(url));
  return Promise.resolve({{ok:true,json:function(){{return Promise.resolve({{}});}}}});
}};
</script>
<script data-policy-url=\"https://example.test/policy\">{gate}</script>
<script type=\"text/plain\" data-nexus-consent=\"analytics\" data-site=\"test\" data-consent=\"managed\">{tracker}</script>
<script>
window.__analyticsBootstrap=true;
localStorage.setItem('metric_analytics_storage','1');
document.cookie='metric_analytics_cookie=1; Path=/';
setTimeout(function(){{window.__analyticsTimer=true;}},5);
window.fetch('/functional-widget-submit').then(function(){{window.__functionalWidgetSubmitted=true;}});
window.fetch('https://junior.sobakovod.pro/nexus/senler/api/track');
var metricFrame=document.createElement('iframe');
metricFrame.srcdoc='<script>parent.__frameMetricRan=true;window.__metricEndpoint="https://mc.yandex.ru/watch/test";<\\/script><noscript><img src="/nexus/tracker/api/pixel-test"></noscript>';
document.documentElement.appendChild(metricFrame);
</script>
<script>
window.__advertisingBootstrap=true;
localStorage.setItem('metric_advertising_storage','1');
document.cookie='metric_advertising_cookie=1; Path=/';
setTimeout(function(){{window.__advertisingTimer=true;}},5);
window.__advertisingEndpoint='https://vk.com/rtrg?p=test';
</script>
</head><body><a id=\"cookie-settings-link\" href=\"#cookie-settings\">Cookie</a><pre id=\"result\"></pre><script>
document.addEventListener('DOMContentLoaded',function(){{
  var before={{
    calls:window.__metricCalls.length,
    functionalWidget:window.__functionalWidgetSubmitted===true,
    analyticsBootstrap:window.__analyticsBootstrap===true,
    advertisingBootstrap:window.__advertisingBootstrap===true,
    analyticsTimer:window.__analyticsTimer===true,
    advertisingTimer:window.__advertisingTimer===true,
    analyticsStorage:localStorage.getItem('metric_analytics_storage'),
    advertisingStorage:localStorage.getItem('metric_advertising_storage'),
    analyticsCookie:document.cookie.indexOf('metric_analytics_cookie=1')>=0,
    advertisingCookie:document.cookie.indexOf('metric_advertising_cookie=1')>=0,
    frameMetric:window.__frameMetricRan===true,
    framePixel:!!(metricFrame.contentDocument&&metricFrame.contentDocument.querySelector('img[src*="/nexus/tracker/api/"]')),
    tracker:typeof window.NexusTracker,
    state:localStorage.getItem('nexus_tracker_state_v1'),
    preferences:window.NexusMetricGate.getPreferences(),
    banner:!document.getElementById('nexus-consent-banner').hidden,
    settingsButtonExists:!!document.getElementById('nexus-consent-settings')
  }};
  document.getElementById('nexus-consent-reject').click();
  var rejected={{
    calls:window.__metricCalls.length,
    analyticsTimer:window.__analyticsTimer===true,
    advertisingTimer:window.__advertisingTimer===true,
    analyticsStorage:localStorage.getItem('metric_analytics_storage'),
    advertisingStorage:localStorage.getItem('metric_advertising_storage'),
    analyticsCookie:document.cookie.indexOf('metric_analytics_cookie=1')>=0,
    advertisingCookie:document.cookie.indexOf('metric_advertising_cookie=1')>=0,
    frameMetric:window.__frameMetricRan===true,
    framePixel:!!(metricFrame.contentDocument&&metricFrame.contentDocument.querySelector('img[src*="/nexus/tracker/api/"]')),
    tracker:typeof window.NexusTracker,
    preferences:window.NexusMetricGate.getPreferences(),
    manageExists:!!document.getElementById('nexus-consent-manage')
  }};
  document.getElementById('cookie-settings-link').click();
  rejected.settingsOpened=!document.getElementById('nexus-consent-modal').hidden;
  document.getElementById('nexus-consent-close').click();
  window.NexusMetricGate.savePreferences({{analytics:true,advertising:false}});
  setTimeout(function(){{
    var analyticsOnly={{
      calls:window.__metricCalls.length,
      analyticsTimer:window.__analyticsTimer===true,
      advertisingTimer:window.__advertisingTimer===true,
      analyticsStorage:localStorage.getItem('metric_analytics_storage'),
      advertisingStorage:localStorage.getItem('metric_advertising_storage'),
      analyticsCookie:document.cookie.indexOf('metric_analytics_cookie=1')>=0,
      advertisingCookie:document.cookie.indexOf('metric_advertising_cookie=1')>=0,
      frameMetric:window.__frameMetricRan===true,
      framePixel:!!(metricFrame.contentDocument&&metricFrame.contentDocument.querySelector('img[src*="/nexus/tracker/api/"]')),
      tracker:typeof window.NexusTracker,
      state:!!localStorage.getItem('nexus_tracker_state_v1'),
      consent:window.NexusTracker && window.NexusTracker.hasConsent(),
      preferences:window.NexusMetricGate.getPreferences()
    }};
    window.NexusMetricGate.acceptAll();
    setTimeout(function(){{
      document.getElementById('result').textContent=JSON.stringify({{
        before:before,
        rejected:rejected,
        analyticsOnly:analyticsOnly,
        afterAll:{{
          advertisingTimer:window.__advertisingTimer===true,
          advertisingStorage:localStorage.getItem('metric_advertising_storage'),
          advertisingCookie:document.cookie.indexOf('metric_advertising_cookie=1')>=0,
          preferences:window.NexusMetricGate.getPreferences()
        }},
        pending:window.NexusMetricGate.pendingCount()
      }});
    }},30);
  }},80);
}});
</script></body></html>"""

        with tempfile.TemporaryDirectory() as tmp:
            pathlib.Path(tmp, "index.html").write_text(harness, encoding="utf-8")
            server = http.server.ThreadingHTTPServer(
                ("127.0.0.1", 0),
                lambda *args, **kwargs: QuietHandler(*args, directory=tmp, **kwargs),
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                result = subprocess.run(
                    [
                        chrome,
                        "--headless=new",
                        "--no-sandbox",
                        "--disable-gpu",
                        "--disable-background-networking",
                        "--virtual-time-budget=1000",
                        "--dump-dom",
                        f"http://127.0.0.1:{server.server_port}/index.html",
                    ],
                    text=True,
                    capture_output=True,
                    timeout=20,
                    check=False,
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

        self.assertEqual(result.returncode, 0, result.stderr)
        match = re.search(r'<pre id="result">(.*?)</pre>', result.stdout, re.DOTALL)
        self.assertIsNotNone(match, result.stdout)
        payload = __import__("json").loads(html.unescape(match.group(1)))

        self.assertEqual(payload["before"]["calls"], 1)
        self.assertTrue(payload["before"]["functionalWidget"])
        self.assertTrue(payload["before"]["analyticsBootstrap"])
        self.assertTrue(payload["before"]["advertisingBootstrap"])
        self.assertFalse(payload["before"]["analyticsTimer"])
        self.assertFalse(payload["before"]["advertisingTimer"])
        self.assertIsNone(payload["before"]["analyticsStorage"])
        self.assertIsNone(payload["before"]["advertisingStorage"])
        self.assertFalse(payload["before"]["analyticsCookie"])
        self.assertFalse(payload["before"]["advertisingCookie"])
        self.assertFalse(payload["before"]["frameMetric"])
        self.assertFalse(payload["before"]["framePixel"])
        self.assertEqual(payload["before"]["tracker"], "undefined")
        self.assertIsNone(payload["before"]["state"])
        self.assertIsNone(payload["before"]["preferences"])
        self.assertTrue(payload["before"]["banner"])
        self.assertFalse(payload["before"]["settingsButtonExists"])

        self.assertEqual(payload["rejected"]["calls"], 1)
        self.assertFalse(payload["rejected"]["analyticsTimer"])
        self.assertFalse(payload["rejected"]["advertisingTimer"])
        self.assertIsNone(payload["rejected"]["analyticsStorage"])
        self.assertIsNone(payload["rejected"]["advertisingStorage"])
        self.assertFalse(payload["rejected"]["analyticsCookie"])
        self.assertFalse(payload["rejected"]["advertisingCookie"])
        self.assertFalse(payload["rejected"]["frameMetric"])
        self.assertFalse(payload["rejected"]["framePixel"])
        self.assertEqual(payload["rejected"]["tracker"], "undefined")
        self.assertEqual(payload["rejected"]["preferences"], {"analytics": False, "advertising": False})
        self.assertFalse(payload["rejected"]["manageExists"])
        self.assertTrue(payload["rejected"]["settingsOpened"])

        self.assertGreaterEqual(payload["analyticsOnly"]["calls"], 3)
        self.assertTrue(payload["analyticsOnly"]["analyticsTimer"])
        self.assertFalse(payload["analyticsOnly"]["advertisingTimer"])
        self.assertEqual(payload["analyticsOnly"]["analyticsStorage"], "1")
        self.assertIsNone(payload["analyticsOnly"]["advertisingStorage"])
        self.assertTrue(payload["analyticsOnly"]["analyticsCookie"])
        self.assertFalse(payload["analyticsOnly"]["advertisingCookie"])
        self.assertTrue(payload["analyticsOnly"]["frameMetric"])
        self.assertTrue(payload["analyticsOnly"]["framePixel"])
        self.assertEqual(payload["analyticsOnly"]["tracker"], "object")
        self.assertTrue(payload["analyticsOnly"]["state"])
        self.assertTrue(payload["analyticsOnly"]["consent"])
        self.assertEqual(payload["analyticsOnly"]["preferences"], {"analytics": True, "advertising": False})
        self.assertTrue(payload["afterAll"]["advertisingTimer"])
        self.assertEqual(payload["afterAll"]["advertisingStorage"], "1")
        self.assertTrue(payload["afterAll"]["advertisingCookie"])
        self.assertEqual(payload["afterAll"]["preferences"], {"analytics": True, "advertising": True})
        self.assertEqual(payload["pending"], 0)

if __name__ == "__main__":
    unittest.main()
