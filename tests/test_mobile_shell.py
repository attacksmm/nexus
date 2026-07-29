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


if __name__ == "__main__":
    unittest.main()
