import asyncio
import json
import unittest
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import httpx

from module_token_vault import router as vault


class FakeRequest:
    def __init__(self, query=None):
        self.query_params = query or {}
        self.scope = {"root_path": "/nexus"}
        self.url = SimpleNamespace(scheme="https", netloc="junior.sobakovod.pro")
        self.client = None


class VkIdHelpersTest(unittest.TestCase):
    def test_pkce_matches_rfc_7636_example(self):
        verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        self.assertEqual(
            vault._pkce_challenge(verifier),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        )

    def test_callback_accepts_payload_and_direct_values(self):
        request = FakeRequest(
            {
                "payload": json.dumps({"code": "code-1", "device_id": "device-1"}),
                "state": "state-1",
            }
        )
        self.assertEqual(
            vault._callback_values(request),
            {
                "payload": json.dumps({"code": "code-1", "device_id": "device-1"}),
                "state": "state-1",
                "code": "code-1",
                "device_id": "device-1",
            },
        )

    def test_token_response_requires_matching_state_and_rotated_pair(self):
        body = {
            "state": "state-1",
            "access_token": "access-1",
            "refresh_token": "refresh-1",
            "expires_in": 3600,
            "user_id": 123,
            "scope": "vkid.personal_info groups",
        }
        updates = vault._token_updates(body, device_id="device-1", expected_state="state-1")
        self.assertEqual(updates["VK_USER_TOKEN"], "access-1")
        self.assertEqual(updates["VK_ID_REFRESH_TOKEN"], "refresh-1")
        self.assertEqual(updates["VK_ID_DEVICE_ID"], "device-1")
        self.assertEqual(updates["VK_ID_REFRESH_BLOCKED"], "0")
        with self.assertRaisesRegex(RuntimeError, "state"):
            vault._token_updates(body, device_id="device-1", expected_state="other")


class VkIdFlowTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.originals = {
            name: getattr(vault, name)
            for name in (
                "_require_admin",
                "_current_value",
                "_token_request",
                "_save_token_updates",
                "_block_refresh",
            )
        }
        vault._oauth_attempts.clear()

    async def asyncTearDown(self):
        for name, value in self.originals.items():
            setattr(vault, name, value)
        vault._oauth_attempts.clear()

    async def test_start_url_contains_pkce_but_no_server_secret(self):
        values = {
            "VK_ID_APP_ID": "123456",
            "VK_ID_SERVICE_TOKEN": "service-secret",
            "VK_ID_SCOPES": "vkid.personal_info groups",
            "VK_ID_REDIRECT_URI": "",
        }

        async def require_admin(_request):
            return {"username": "tester"}

        vault._require_admin = require_admin
        vault._current_value = lambda key: values.get(key, "")
        result = await vault.vk_id_start(FakeRequest())
        query = parse_qs(urlparse(result["authorization_url"]).query)
        self.assertEqual(query["client_id"], ["123456"])
        self.assertEqual(query["code_challenge_method"], ["s256"])
        self.assertEqual(
            query["redirect_uri"],
            ["https://junior.sobakovod.pro/nexus/token-vault/api/vk/callback"],
        )
        self.assertEqual(query["scope"], ["vkid.personal_info groups"])
        self.assertNotIn("code_verifier", query)
        self.assertNotIn("service_token", query)
        self.assertEqual(len(vault._oauth_attempts), 1)

    async def test_callback_keeps_vkid_alias(self):
        paths = {route.path for route in vault.router.routes}
        self.assertIn("/vk/callback", paths)
        self.assertIn("/vk-id/callback", paths)

    async def test_refresh_rotates_pair_once_and_passes_service_token(self):
        values = {
            "VK_ID_REFRESH_BLOCKED": "0",
            "VK_ID_APP_ID": "123456",
            "VK_ID_REFRESH_TOKEN": "refresh-old",
            "VK_ID_DEVICE_ID": "device-1",
            "VK_ID_ACCESS_EXPIRES_AT": "0",
            "VK_ID_SERVICE_TOKEN": "service-secret",
        }
        requests = []
        saved = []

        async def token_request(data):
            requests.append(dict(data))
            return {
                "state": data["state"],
                "access_token": "access-new",
                "refresh_token": "refresh-new",
                "expires_in": 3600,
                "user_id": 123,
                "scope": "vkid.personal_info",
            }

        async def save_updates(updates):
            saved.append(dict(updates))
            return {"ok": True, "restarted": 2, "failed": 0, "modules": []}

        vault._current_value = lambda key: values.get(key, "")
        vault._token_request = token_request
        vault._save_token_updates = save_updates
        result = await vault._refresh_vk_id(force=True)
        self.assertTrue(result["refreshed"])
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["refresh_token"], "refresh-old")
        self.assertEqual(requests[0]["service_token"], "service-secret")
        self.assertEqual(saved[0]["VK_USER_TOKEN"], "access-new")
        self.assertEqual(saved[0]["VK_ID_REFRESH_TOKEN"], "refresh-new")

    async def test_ambiguous_transport_failure_blocks_reuse(self):
        values = {
            "VK_ID_REFRESH_BLOCKED": "0",
            "VK_ID_APP_ID": "123456",
            "VK_ID_REFRESH_TOKEN": "refresh-old",
            "VK_ID_DEVICE_ID": "device-1",
            "VK_ID_ACCESS_EXPIRES_AT": "0",
            "VK_ID_SERVICE_TOKEN": "service-secret",
        }
        blocked = []

        async def token_request(_data):
            raise httpx.ReadTimeout("lost")

        async def block(message):
            blocked.append(message)

        vault._current_value = lambda key: values.get(key, "")
        vault._token_request = token_request
        vault._block_refresh = block
        result = await vault._refresh_vk_id(force=True)
        self.assertTrue(result["reauthorize"])
        self.assertEqual(len(blocked), 1)
        self.assertIn("нельзя использовать повторно", blocked[0])


if __name__ == "__main__":
    unittest.main()
