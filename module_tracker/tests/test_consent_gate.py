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

        sobakovod = source_constant("SOBAKOVOD_CONSENT_SCRIPTS")
        self.assertNotIn("<noscript", sobakovod.lower())
        embedded = re.findall(r'<script\b[^>]*>(.*?)</script>', sobakovod, re.DOTALL | re.IGNORECASE)
        self.assertEqual(len(embedded), 4)
        for script in embedded:
            result = subprocess.run(
                [node, "--check", "-"],
                input=script,
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
<script type=\"text/plain\" data-nexus-consent=\"analytics\">
window.__analyticsRan=true;
window.fetch('https://junior.sobakovod.pro/nexus/senler/api/track');
</script>
<script type=\"text/plain\" data-nexus-consent=\"advertising\">window.__advertisingRan=true;</script>
</head><body><pre id=\"result\"></pre><script>
document.addEventListener('DOMContentLoaded',function(){{
  var before={{
    calls:window.__metricCalls.length,
    analytics:window.__analyticsRan===true,
    advertising:window.__advertisingRan===true,
    tracker:typeof window.NexusTracker,
    state:localStorage.getItem('nexus_tracker_state_v1'),
    preferences:window.NexusMetricGate.getPreferences(),
    banner:!document.getElementById('nexus-consent-banner').hidden
  }};
  document.getElementById('nexus-consent-reject').click();
  var rejected={{
    calls:window.__metricCalls.length,
    analytics:window.__analyticsRan===true,
    advertising:window.__advertisingRan===true,
    tracker:typeof window.NexusTracker,
    preferences:window.NexusMetricGate.getPreferences(),
    manage:!document.getElementById('nexus-consent-manage').hidden
  }};
  window.NexusMetricGate.savePreferences({{analytics:true,advertising:false}});
  setTimeout(function(){{
    var analyticsOnly={{
      calls:window.__metricCalls.length,
      analytics:window.__analyticsRan===true,
      advertising:window.__advertisingRan===true,
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
          advertising:window.__advertisingRan===true,
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

        self.assertEqual(payload["before"]["calls"], 0)
        self.assertFalse(payload["before"]["analytics"])
        self.assertFalse(payload["before"]["advertising"])
        self.assertEqual(payload["before"]["tracker"], "undefined")
        self.assertIsNone(payload["before"]["state"])
        self.assertIsNone(payload["before"]["preferences"])
        self.assertTrue(payload["before"]["banner"])

        self.assertEqual(payload["rejected"]["calls"], 0)
        self.assertFalse(payload["rejected"]["analytics"])
        self.assertFalse(payload["rejected"]["advertising"])
        self.assertEqual(payload["rejected"]["tracker"], "undefined")
        self.assertEqual(payload["rejected"]["preferences"], {"analytics": False, "advertising": False})
        self.assertTrue(payload["rejected"]["manage"])

        self.assertGreaterEqual(payload["analyticsOnly"]["calls"], 2)
        self.assertTrue(payload["analyticsOnly"]["analytics"])
        self.assertFalse(payload["analyticsOnly"]["advertising"])
        self.assertEqual(payload["analyticsOnly"]["tracker"], "object")
        self.assertTrue(payload["analyticsOnly"]["state"])
        self.assertTrue(payload["analyticsOnly"]["consent"])
        self.assertEqual(payload["analyticsOnly"]["preferences"], {"analytics": True, "advertising": False})
        self.assertTrue(payload["afterAll"]["advertising"])
        self.assertEqual(payload["afterAll"]["preferences"], {"analytics": True, "advertising": True})
        self.assertEqual(payload["pending"], 0)


if __name__ == "__main__":
    unittest.main()
