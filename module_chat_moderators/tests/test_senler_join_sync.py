import asyncio
import os
from types import SimpleNamespace
from unittest.mock import patch

from module_chat_moderators import router as module


def test_senler_add_verifies_every_requested_vk_user() -> None:
    calls: list[str] = []

    class Response:
        is_success = True

        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, url, json):
            calls.append(url.rsplit("/", 1)[-1])
            if url.endswith("/add"):
                return Response({"success": True})
            return Response(
                {"success": True, "items": [{"vk_user_id": value} for value in json["vk_user_id"]]}
            )

    env = {
        "SENLER_ACCESS_TOKEN": "test-token",
        "SENLER_GROUP_ID": "225075265",
        "SENLER_COURSE_CHAT_SUBSCRIPTION_ID": "3801272",
    }
    with patch.dict(os.environ, env), patch.object(module.httpx, "AsyncClient", return_value=Client()):
        result = asyncio.run(module._senler_add_course_chat_users([123, 456, 123]))

    assert result["ok"] is True
    assert result["users"] == 2
    assert result["verified"] == 2
    assert calls == ["add", "get"]


def test_join_sync_requires_community_admin_and_verifies_senler() -> None:
    class Messages:
        @staticmethod
        def getConversationMembers(**_params):
            return {"items": [{"member_id": -225075265, "is_admin": True}]}

    with patch.object(module, "_secret_value", return_value=""):
        runtime = module.VKModeratorRuntime(analyzer=None)
    runtime.vk = SimpleNamespace(messages=Messages())
    runtime.own_id = -225075265
    calls: list[list[int]] = []

    async def add(ids):
        calls.append(list(ids))
        return {"ok": True, "status": "synced", "verified": 1, "subscription_id": "3801272"}

    with (
        patch.object(module, "_senler_add_course_chat_users", add),
        patch.object(module, "_remember_vk_course_member"),
        patch.object(module, "_set_vk_course_member_senler_status"),
        patch.object(module, "_record_action"),
    ):
        asyncio.run(
            runtime._sync_joined_user_to_senler(
                2000000017,
                123,
                "training_stream",
                "57. 30.07.2026 - Курс Щенок. Современный Собаковод",
            )
        )

    assert calls == [[123]]


def test_private_message_retries_pending_course_member() -> None:
    runtime = object.__new__(module.VKModeratorRuntime)
    calls: list[list[int]] = []
    statuses: list[tuple[int, bool]] = []

    async def add(ids):
        calls.append(list(ids))
        if len(calls) == 1:
            return {"ok": False, "status": "partial_error", "verified": 0}
        return {"ok": True, "status": "synced", "verified": 1}

    async def no_wait(_seconds):
        return None

    event = SimpleNamespace(peer_id=123, message_id=1, from_id=123)
    with (
        patch.object(module, "_vk_course_member_pending", return_value=True),
        patch.object(module, "_senler_add_course_chat_users", add),
        patch.object(module.asyncio, "sleep", no_wait),
        patch.object(
            module,
            "_set_vk_course_member_senler_status",
            side_effect=lambda user_id, verified: statuses.append((user_id, verified)),
        ),
        patch.object(module, "_record_action"),
    ):
        asyncio.run(runtime.process_message(event))

    assert calls == [[123], [123]]
    assert statuses == [(123, True)]


def test_senler_chat_filter_excludes_test_streams_and_non_course_chats() -> None:
    assert module._senler_chat_is_eligible(
        "training_stream", "57. 30.07.2026 - Курс Щенок. Современный Собаковод"
    )
    assert module._senler_chat_is_eligible(
        "closed_club", "55. 03.08.2026 - Современный Собаковод - закрытый чат"
    )
    assert not module._senler_chat_is_eligible(
        "training_stream", "998. 27.07.2026 - Курс Щенок. Современный Собаковод"
    )
    assert not module._senler_chat_is_eligible(
        "training_stream", "88. 01.01.1990 - Курс Щенок. Современный Собаковод"
    )
    assert not module._senler_chat_is_eligible(
        "training_stream", "888. Курс Щенок. Современный Собаковод (старт 01.01.0101)"
    )
    assert not module._senler_chat_is_eligible("logs", "Логи модераторов")


def test_telegram_log_copy_uses_configured_vk_log_chat() -> None:
    sent: list[tuple[int, str]] = []
    vk_runtime = SimpleNamespace(
        subscription=SimpleNamespace(running=True),
        log_chat_id=2000000040,
        _send_message=lambda peer_id, text: sent.append((peer_id, text)),
    )
    manager = SimpleNamespace(vk=vk_runtime)

    with patch.object(module, "_runtime", manager), patch.object(module, "_record_action"):
        module._copy_telegram_moderation_log_to_vk(
            "TG log", category="техпод", chat_id=-1001, user_id=123
        )

    assert sent == [(2000000040, "TG log")]
