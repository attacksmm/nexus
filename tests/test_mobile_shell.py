from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class MobileShellTests(unittest.TestCase):
    def test_mobile_drawer_contract_is_wired(self):
        shell = (ROOT / "templates" / "shell.html").read_text(encoding="utf-8")
        styles = (ROOT / "static" / "styles.css").read_text(encoding="utf-8")
        script = (ROOT / "static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="mobileModulesBtn"', shell)
        self.assertIn('id="moduleSidebar"', shell)
        self.assertIn('id="mobileSidebarBackdrop"', shell)
        self.assertIn("@media (max-width: 760px)", styles)
        self.assertIn(".workspace--mobile-nav .sidebar", styles)
        self.assertIn("width: 100%; height: 100%; min-width: 0", styles)
        self.assertIn("function setMobileModulesOpen(open)", script)
        self.assertIn('setMobileModulesOpen(false);', script)

    def test_global_module_loader_contract_is_wired(self):
        shell = (ROOT / "templates" / "shell.html").read_text(encoding="utf-8")
        styles = (ROOT / "static" / "styles.css").read_text(encoding="utf-8")
        script = (ROOT / "static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="moduleLoading"', shell)
        self.assertIn('id="moduleLoadingRetry"', shell)
        self.assertIn(".module-loading", styles)
        self.assertIn("function startModuleLoading()", script)
        self.assertIn('data.type === "nexus:module-load"', script)
        self.assertIn("!moduleLoadBridgeSeen", script)
        self.assertIn("&nexus_loader=1", script)

    def test_module_panel_bridge_is_central_and_idempotent(self):
        import main

        html = "<!doctype html><html><head><title>x</title></head><body></body></html>"
        injected = main._inject_module_load_bridge(html)
        self.assertIn(main.MODULE_LOAD_BRIDGE_MARKER, injected)
        self.assertIn(main.MODULE_THEME_STYLE_MARKER, injected)
        self.assertLess(injected.index(main.MODULE_LOAD_BRIDGE_MARKER), injected.index("<title>"))
        self.assertLess(injected.index(main.MODULE_LOAD_BRIDGE_MARKER), injected.index(main.MODULE_THEME_STYLE_MARKER))
        self.assertIn('getItem("nexus-theme")', injected)
        self.assertIn('data-nexus-theme', injected)
        self.assertIn('data.type==="nexus:theme"', injected)
        self.assertIn('filter:invert(.18) contrast(1.1)', injected)
        self.assertIn('filter:contrast(.9)', injected)
        self.assertIn('filter:invert(1) hue-rotate(180deg) contrast(.9)', injected)
        self.assertIn('autocomplete","one-time-code', injected)
        self.assertIn("data-nexus-autofill-guard", injected)
        self.assertIn("MutationObserver", injected)
        self.assertEqual(main._inject_module_load_bridge(injected), injected)
        self.assertTrue(main._is_module_panel_index("/messenger-widget/panel/index.html"))
        self.assertFalse(main._is_module_panel_index("/messenger-widget/panel/app.js"))

    def test_three_theme_contract_is_central_and_persistent(self):
        shell = (ROOT / "templates" / "shell.html").read_text(encoding="utf-8")
        styles = (ROOT / "static" / "styles.css").read_text(encoding="utf-8")
        script = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
        settings = (ROOT / "templates" / "settings.html").read_text(encoding="utf-8")
        login = (ROOT / "templates" / "login.html").read_text(encoding="utf-8")

        self.assertIn('id="themeToggle"', shell)
        self.assertLess(shell.index("localStorage.getItem('nexus-theme')"), shell.index('rel="stylesheet"'))
        self.assertIn(':root[data-theme="light"]', styles)
        self.assertIn(':root[data-theme="gray"]', styles)
        self.assertIn('const THEMES = ["light", "gray", "dark"]', script)
        self.assertIn('localStorage.setItem("nexus-theme", value)', script)
        self.assertIn('localStorage.setItem("nexus-streams-theme", value)', script)
        self.assertIn('postMessage({type: "nexus:theme", theme}', script)
        self.assertLess(settings.index("localStorage.getItem('nexus-theme')"), settings.index('rel="stylesheet"'))
        self.assertLess(login.index("localStorage.getItem('nexus-theme')"), login.index('rel="stylesheet"'))


if __name__ == "__main__":
    unittest.main()
