import asyncio
import os
import unittest

from module_token_vault import router as token_vault


class FakeRequest:
    async def json(self):
        return {
            "key": "VK_USER_TOKEN",
            "value": "new-token",
            "validate": True,
        }


class TokenVaultRestartTest(unittest.TestCase):
    def test_successful_save_restarts_dependents(self):
        asyncio.run(self._run_case())

    async def _run_case(self):
        written = []
        restarted = []
        old_value = os.environ.get("VK_USER_TOKEN")

        async def require_admin(_request):
            return {"username": "tester"}

        async def validate(_key, _value):
            return {"status": "ok", "message": "users.get"}

        async def restart(key):
            restarted.append(key)
            return {
                "ok": True,
                "key": key,
                "modules": [{"id": "chat-moderator", "status": "active"}],
                "restarted": 1,
                "failed": 0,
            }

        async def status_item(item, **_kwargs):
            return item

        originals = {
            "_require_admin": token_vault._require_admin,
            "_validate_known": token_vault._validate_known,
            "_read_env_values": token_vault._read_env_values,
            "_write_env_values": token_vault._write_env_values,
            "_restart_modules_for_env": token_vault._restart_modules_for_env,
            "_status_item": token_vault._status_item,
        }
        token_vault._require_admin = require_admin
        token_vault._validate_known = validate
        token_vault._read_env_values = lambda: {}
        token_vault._write_env_values = lambda env: written.append(dict(env))
        token_vault._restart_modules_for_env = restart
        token_vault._status_item = status_item
        try:
            result = await token_vault.save_env(FakeRequest())
        finally:
            for name, value in originals.items():
                setattr(token_vault, name, value)
            if old_value is None:
                os.environ.pop("VK_USER_TOKEN", None)
            else:
                os.environ["VK_USER_TOKEN"] = old_value

        self.assertEqual(written, [{"VK_USER_TOKEN": "new-token"}])
        self.assertEqual(restarted, ["VK_USER_TOKEN"])
        self.assertTrue(result["saved"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["restart"]["restarted"], 1)


if __name__ == "__main__":
    unittest.main()
