import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import router


class MobileLinkSyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="messenger-mobile-sync-")
        self.previous_db = router._db_path
        router._db_path = Path(self.tmp.name) / "module.db"
        router._logger = logging.getLogger("messenger-mobile-sync-tests")
        router._amo_mobile_field_cache = (0.0, 0)
        router._amo_mobile_sync_cache.clear()
        await router._init_db()

    async def asyncTearDown(self):
        router._db_path = self.previous_db
        self.tmp.cleanup()

    async def test_sync_patches_only_conversation_field_and_advances_cursor(self):
        calls = []

        async def fake_request(_client, method, url, _token, *, payload=None, params=None):
            calls.append((method, url, payload, params))
            if url.endswith("/custom_fields"):
                return {"_embedded": {"custom_fields": [
                    {"id": 1030709, "name": "Переписка", "type": "url"},
                ]}}
            if method == "GET":
                return {"_embedded": {"leads": [{
                    "id": 18222863, "created_at": 2000, "status_id": 777,
                    "responsible_user_id": 888, "custom_fields_values": [],
                }]}, "_links": {}}
            self.assertEqual(payload.keys(), {"custom_fields_values"})
            self.assertNotIn("status_id", payload)
            self.assertNotIn("responsible_user_id", payload)
            self.assertEqual(payload["custom_fields_values"][0]["field_id"], 1030709)
            self.assertIn("/lead/18222863/", payload["custom_fields_values"][0]["values"][0]["value"])
            return {"id": 18222863}

        with (
            patch.object(router, "_amo_credentials", return_value=("https://example.amocrm.ru", "token")),
            patch.object(router, "_amo_task_api_request", new=fake_request),
        ):
            result = await router._amo_mobile_sync_once(since=1000, until=3000)
        self.assertEqual((result["scanned"], result["updated"], result["failed"]), (1, 1, 0))
        self.assertEqual(await router._setting("amo_mobile_link_sync_cursor"), "3000")
        lead_query = next(call for call in calls if call[0] == "GET" and call[1].endswith("/leads"))
        self.assertEqual(lead_query[3]["filter[created_at][from]"], 700)
        self.assertEqual(lead_query[3]["filter[created_at][to]"], 3000)

    async def test_sync_skips_an_exact_existing_link(self):
        secret = await router._setting("webhook_secret")
        expected = router._amo_mobile_link_with_secret("18222864", secret)
        patches = []

        async def fake_request(_client, method, url, _token, *, payload=None, params=None):
            if url.endswith("/custom_fields"):
                return {"_embedded": {"custom_fields": [
                    {"id": 1030709, "name": "Переписка", "type": "url"},
                ]}}
            if method == "GET":
                return {"_embedded": {"leads": [{
                    "id": 18222864, "created_at": 2000,
                    "custom_fields_values": [{
                        "field_id": 1030709, "values": [{"value": expected}],
                    }],
                }]}, "_links": {}}
            patches.append(payload)
            return {}

        with (
            patch.object(router, "_amo_credentials", return_value=("https://example.amocrm.ru", "token")),
            patch.object(router, "_amo_task_api_request", new=fake_request),
        ):
            result = await router._amo_mobile_sync_once(since=1000, until=3000)
        self.assertEqual((result["scanned"], result["skipped"], result["updated"]), (1, 1, 0))
        self.assertEqual(patches, [])


if __name__ == "__main__":
    unittest.main()
