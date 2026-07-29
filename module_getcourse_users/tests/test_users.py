import importlib.util
import sys
import types
import unittest
from pathlib import Path


def load_router():
    if "aiosqlite" not in sys.modules:
        aiosqlite = types.ModuleType("aiosqlite")
        aiosqlite.connect = lambda *_a, **_k: None
        sys.modules["aiosqlite"] = aiosqlite
    if "httpx" not in sys.modules:
        httpx = types.ModuleType("httpx")
        httpx.AsyncClient = object
        sys.modules["httpx"] = httpx
    if "fastapi" not in sys.modules:
        fastapi = types.ModuleType("fastapi")
        class APIRouter:
            def get(self, *_a, **_k): return lambda fn: fn
            def post(self, *_a, **_k): return lambda fn: fn
            def api_route(self, *_a, **_k): return lambda fn: fn
        class HTTPException(Exception):
            def __init__(self, status_code, detail): self.status_code, self.detail = status_code, detail
        fastapi.APIRouter, fastapi.HTTPException, fastapi.Request = APIRouter, HTTPException, object
        sys.modules["fastapi"] = fastapi
    auth = types.ModuleType("orchestrator.auth")
    auth._read_env_values = lambda: {}
    auth.can_access_module = lambda *_a: True
    async def verify(_request): return {"role": "admin"}
    auth.verify_token_from_request = verify
    sys.modules.setdefault("orchestrator", types.ModuleType("orchestrator"))
    sys.modules["orchestrator.auth"] = auth
    path = Path(__file__).parents[1] / "router.py"
    spec = importlib.util.spec_from_file_location("getcourse_users_router", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


r = load_router()


class NormalizeTests(unittest.TestCase):
    def test_normalizes_identity_and_utm(self):
        uid, fields = r._normalize_user({
            "gc_user_id": " 505011365 ", "email": "user@example.ru",
            "first_name": "Иван", "last_name": "Иванов",
            "utm_source": "avito", "sb_id": "sb-1", "vka_id": "vk-2",
        })
        self.assertEqual(uid, "505011365")
        self.assertEqual(fields["name"], "Иван Иванов")
        self.assertEqual(fields["utm_source"], "avito")
        self.assertEqual(fields["salebot_id"], "sb-1")
        self.assertEqual(fields["vka_id"], "vk-2")
        self.assertEqual(fields["vk_platform_id"], "vk-2")

    def test_platform_id_is_getcourse_id(self):
        uid, fields = r._normalize_user({"id": 123, "email": "a@b.ru"})
        self.assertEqual(uid, "123")
        self.assertEqual(fields["gc_user_id"], "123")

    def test_missing_id_rejected(self):
        with self.assertRaisesRegex(ValueError, "gc_user_id"):
            r._normalize_user({"email": "a@b.ru"})

    def test_empty_callback_does_not_erase_fields(self):
        _, fields = r._normalize_user({"id": "1", "email": "", "utm_source": ""})
        self.assertNotIn("email", fields)
        self.assertNotIn("utm_source", fields)

    def test_callback_template_has_identity_bridges(self):
        body = r._callback_body()
        keys = (
            "gc_user_id={object.id}", "salebot_id={object.sb_id}",
            "vka_id={object.vka_id}", "vk_id={object.VK-ID}",
            "utm_source={object.create_session.utm_source}",
        )
        for key in keys:
            self.assertIn(key, body)

    def test_callback_url_is_complete_get_request(self):
        request = types.SimpleNamespace(
            scope={"root_path": "/nexus"},
            headers={"host": "junior.sobakovod.pro", "x-forwarded-proto": "https"},
            url=types.SimpleNamespace(scheme="http"),
        )
        relative, full = r._callback_urls(request, "test-secret")
        self.assertTrue(relative.startswith("/nexus/getcourse-users/api/webhook?secret=test-secret&"))
        self.assertTrue(full.startswith("https://junior.sobakovod.pro/nexus/getcourse-users/api/webhook?"))
        self.assertIn("gc_user_id={object.id}", full)
        self.assertIn("utm_source={object.create_session.utm_source}", full)


if __name__ == "__main__":
    unittest.main()
