import asyncio
import os
import sys
import types
import unittest

sys.modules.setdefault("aiosqlite", types.SimpleNamespace())
sys.modules.setdefault("httpx", types.SimpleNamespace())


class _RouterStub:
    def get(self, *args, **kwargs):
        return lambda func: func

    def post(self, *args, **kwargs):
        return lambda func: func

    def put(self, *args, **kwargs):
        return lambda func: func

    def delete(self, *args, **kwargs):
        return lambda func: func


class _HTTPException(Exception):
    pass


class _JSONResponse(dict):
    def __init__(self, content=None, status_code=200, **kwargs):
        super().__init__(content or {})
        self.status_code = status_code


sys.modules.setdefault(
    "fastapi",
    types.SimpleNamespace(APIRouter=lambda: _RouterStub(), HTTPException=_HTTPException, Request=object),
)
sys.modules.setdefault("fastapi.responses", types.SimpleNamespace(JSONResponse=_JSONResponse))
sys.modules.setdefault(
    "orchestrator.auth",
    types.SimpleNamespace(can_access_module=lambda user, module_id: True, verify_token_from_request=lambda request: {"role": "admin"}),
)

from module_vk_dialog_labels import router


class VkDialogLabelsLogicTest(unittest.TestCase):
    def test_flat_amo_payload_is_nested_and_iterated(self):
        flat = {
            "leads[status][0][id]": "17692053",
            "leads[status][0][pipeline_id]": "8061498",
            "leads[status][0][status_id]": "142",
            "leads[status][0][old_status_id]": "66041242",
            "leads[status][0][utm_term]": "1105209997",
        }
        payload = router._flat_payload_to_nested(flat)
        events = router._iter_lead_events(payload)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["id"], "17692053")
        self.assertEqual(events[0]["status_id"], "142")
        self.assertEqual(events[0]["_action"], "status")

    def test_creation_update_is_treated_as_add(self):
        payload = {
            "leads": {
                "update": {
                    "0": {
                        "id": "1",
                        "date_create": "100",
                        "last_modified": "110",
                        "old_status_id": "",
                    }
                }
            }
        }
        events = router._iter_lead_events(payload)
        self.assertEqual(events[0]["_action"], "add")
        self.assertEqual(events[0]["_source_action"], "update")

    def test_custom_field_extracts_query_value(self):
        source = {"landing": "https://example.test/?utm_source=x&utm_term=1105209997"}
        self.assertEqual(router._custom_field_value(source, "utm_term"), "1105209997")

    def test_peer_id_from_vk_url_or_plain_value(self):
        self.assertEqual(router._peer_id_from_vk_id("https://vk.com/gim225075265/convo/1105209997"), "1105209997")
        self.assertEqual(router._peer_id_from_vk_id("vk id: 1105209997"), "1105209997")

    def test_vk_token_prefers_group_token(self):
        keys = ["VK_GROUP_TOKEN", "VK_USER_TOKEN"]
        original = {key: os.environ.get(key) for key in keys}
        try:
            os.environ["VK_GROUP_TOKEN"] = "group"
            os.environ["VK_USER_TOKEN"] = "user"
            self.assertEqual(router._vk_token(), "group")
            os.environ.pop("VK_GROUP_TOKEN", None)
            self.assertEqual(router._vk_token(), "user")
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_apply_official_action_handles_unknown_action(self):
        ok, error, details = asyncio.run(router._apply_official_action("1105209997", "custom_label", {"request_timeout": "5"}))
        self.assertFalse(ok)
        self.assertIn("неизвестное", error)
        self.assertEqual(details, {})

    def test_actions_are_mapped_to_vk_methods(self):
        calls = []
        original = router._vk_api_call

        async def fake_vk_api_call(method, params, settings):
            calls.append((method, params))
            return 1, "", {"params": params}

        router._vk_api_call = fake_vk_api_call
        try:
            ok, error, details = asyncio.run(router._apply_official_action("1105209997", "important_on", {"request_timeout": "5"}))
            self.assertTrue(ok)
            self.assertEqual(error, "")
            self.assertEqual(calls[0][0], "messages.markAsImportantConversation")
            self.assertEqual(calls[0][1]["important"], 1)
            self.assertEqual(details["method"], "messages.markAsImportantConversation")
        finally:
            router._vk_api_call = original

    def test_test_dialog_rejects_any_other_peer(self):
        original_require = router._require_panel_user

        class Request:
            async def json(self):
                return {"group_id": router.TEST_GROUP_ID, "peer_id": "1", "official_action": "none"}

        async def fake_require(request):
            return {"role": "admin"}

        router._require_panel_user = fake_require
        try:
            response = asyncio.run(router.test_dialog(Request()))
            self.assertEqual(response.status_code, 400)
            self.assertIn("только", response["error"])
        finally:
            router._require_panel_user = original_require


if __name__ == "__main__":
    unittest.main()
