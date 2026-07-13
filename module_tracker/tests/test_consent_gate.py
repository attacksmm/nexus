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

    def test_t972_gate_holds_metrics_and_managed_tracker(self):
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
<script>{gate}</script>
<script data-site=\"test\" data-consent=\"managed\">{tracker}</script>
</head><body><pre id=\"result\"></pre><script>
(function(){{
  var analyticsNode=document.createElement('div');
  analyticsNode.id='held-analytics';
  analyticsNode.setAttribute('data-tilda-cookie-type','analytics');
  document.body.appendChild(analyticsNode);
  var advertisingNode=document.createElement('div');
  advertisingNode.id='held-advertising';
  advertisingNode.setAttribute('data-tilda-cookie-type','advertising');
  document.body.appendChild(advertisingNode);
  window.fetch('https://junior.sobakovod.pro/nexus/senler/api/track');
  var before={{
    calls:window.__metricCalls.length,
    analytics:!!document.getElementById('held-analytics'),
    advertising:!!document.getElementById('held-advertising'),
    state:localStorage.getItem('nexus_tracker_state_v1'),
    consent:window.NexusTracker.hasConsent()
  }};
  window.NexusMetricGate.release('analytics');
  setTimeout(function(){{
    var afterAnalytics={{
      calls:window.__metricCalls.length,
      analytics:!!document.getElementById('held-analytics'),
      advertising:!!document.getElementById('held-advertising'),
      state:!!localStorage.getItem('nexus_tracker_state_v1'),
      consent:window.NexusTracker.hasConsent()
    }};
    window.NexusMetricGate.release('advertising');
    document.getElementById('result').textContent=JSON.stringify({{
      before:before,
      afterAnalytics:afterAnalytics,
      afterAdvertising:!!document.getElementById('held-advertising'),
      pending:window.NexusMetricGate.pendingCount()
    }});
  }},50);
}})();
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
        self.assertIsNone(payload["before"]["state"])
        self.assertFalse(payload["before"]["consent"])

        self.assertGreaterEqual(payload["afterAnalytics"]["calls"], 3)
        self.assertTrue(payload["afterAnalytics"]["analytics"])
        self.assertFalse(payload["afterAnalytics"]["advertising"])
        self.assertTrue(payload["afterAnalytics"]["state"])
        self.assertTrue(payload["afterAnalytics"]["consent"])
        self.assertTrue(payload["afterAdvertising"])
        self.assertEqual(payload["pending"], 0)


if __name__ == "__main__":
    unittest.main()
