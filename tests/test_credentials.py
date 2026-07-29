import os
import unittest

from orchestrator.credentials import inventory, save_value


class CredentialsTests(unittest.IsolatedAsyncioTestCase):
    def test_inventory_combines_manifest_requirements(self):
        items = inventory(
            [
                {
                    "id": "chat",
                    "name": "Chat",
                    "status": "active",
                    "manifest_json": '{"env_required":["VK_GROUP_TOKEN"],"env_vars":{"VK_GROUP_TOKEN":"VK"}}',
                }
            ],
            read_env=lambda: {},
        )

        item = next(item for item in items if item["key"] == "VK_GROUP_TOKEN")
        self.assertTrue(item["required"])
        self.assertEqual(item["provider"], "vk_messages")
        self.assertEqual(item["modules"][0]["id"], "chat")

    async def test_save_writes_env_and_restarts_dependents(self):
        written = []
        restarted = []
        previous = os.environ.get("VK_USER_TOKEN")

        async def restart(key):
            restarted.append(key)
            return {
                "ok": True,
                "key": key,
                "modules": [],
                "restarted": 0,
                "failed": 0,
            }

        try:
            result = await save_value(
                "VK_USER_TOKEN",
                "secret",
                validation={"status": "ok", "message": "users.get"},
                read_env=lambda: {},
                write_env=lambda values: written.append(dict(values)),
                restart=restart,
            )
        finally:
            if previous is None:
                os.environ.pop("VK_USER_TOKEN", None)
            else:
                os.environ["VK_USER_TOKEN"] = previous

        self.assertTrue(result["saved"])
        self.assertTrue(result["ok"])
        self.assertEqual(written, [{"VK_USER_TOKEN": "secret"}])
        self.assertEqual(restarted, ["VK_USER_TOKEN"])


if __name__ == "__main__":
    unittest.main()
