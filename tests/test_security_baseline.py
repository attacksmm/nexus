import unittest

from starlette.requests import Request

import main


def request(method: str, *, host: str = "junior.sobakovod.pro", origin: str = "", fetch_site: str = "") -> Request:
    headers = [(b"host", host.encode()), (b"cookie", b"nexus_token=session")]
    if origin:
        headers.append((b"origin", origin.encode()))
    if fetch_site:
        headers.append((b"sec-fetch-site", fetch_site.encode()))
    return Request({"type": "http", "method": method, "path": "/api/settings/users", "headers": headers})


class SecurityBaselineTests(unittest.TestCase):
    def test_production_schema_endpoints_are_disabled(self):
        self.assertIsNone(main.app.docs_url)
        self.assertIsNone(main.app.redoc_url)
        self.assertIsNone(main.app.openapi_url)

    def test_cookie_mutations_reject_cross_origin_browser_requests(self):
        self.assertTrue(
            main._cross_origin_cookie_request(
                request("POST", origin="https://evil.example", fetch_site="cross-site")
            )
        )
        self.assertFalse(
            main._cross_origin_cookie_request(
                request("POST", origin="https://junior.sobakovod.pro", fetch_site="same-origin")
            )
        )

    def test_non_browser_clients_remain_compatible(self):
        self.assertFalse(main._cross_origin_cookie_request(request("POST")))
        self.assertFalse(
            main._cross_origin_cookie_request(
                Request({"type": "http", "method": "POST", "path": "/webhook", "headers": []})
            )
        )

    def test_root_path_is_removed_for_security_routing(self):
        scoped = request("GET")
        scoped.scope["root_path"] = "/nexus"
        scoped.scope["path"] = "/nexus/login"
        self.assertEqual(main._app_request_path(scoped), "/login")


if __name__ == "__main__":
    unittest.main()
