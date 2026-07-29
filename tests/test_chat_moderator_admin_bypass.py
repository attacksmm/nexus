import asyncio
import time
from types import SimpleNamespace
from unittest.mock import patch

from module_chat_moderator import router as club_module
from module_chat_moderators import router as training_module


def _runtime(module, messages):
    runtime = object.__new__(module.VKModeratorRuntime)
    runtime.settings = {"vk_allowed_admins": ""}
    runtime.user_admin_cache = {}
    runtime.vk = SimpleNamespace(messages=messages)
    return runtime


def test_vk_role_field_is_recognized_as_admin_in_both_moderators() -> None:
    class Messages:
        def getConversationMembers(self, **_kwargs):
            return {"items": [{"member_id": 123, "role": "admin"}]}

    for module in (club_module, training_module):
        runtime = _runtime(module, Messages())
        with patch.object(module, "_record_action"):
            assert asyncio.run(runtime.is_chat_admin(2000000017, 123)) is True


def test_vk_admin_lookup_failure_fails_open_in_both_moderators() -> None:
    class Messages:
        def getConversationMembers(self, **_kwargs):
            raise RuntimeError("VK unavailable")

    for module in (club_module, training_module):
        runtime = _runtime(module, Messages())
        with patch.object(module, "_record_action"):
            assert asyncio.run(runtime.is_chat_admin(2000000017, 123)) is True


def test_recent_non_admin_cache_expires_quickly_after_role_change() -> None:
    class Messages:
        def getConversationMembers(self, **_kwargs):
            return {"items": [{"member_id": 123, "role": "admin"}]}

    runtime = _runtime(training_module, Messages())
    runtime.user_admin_cache[(2000000017, 123)] = {
        "is_admin": False,
        "ts": time.time() - 31,
    }
    with patch.object(training_module, "_record_action"):
        assert asyncio.run(runtime.is_chat_admin(2000000017, 123)) is True
