import asyncio
from types import SimpleNamespace
from unittest.mock import patch

from module_chat_moderators import router as module
from orchestrator.vk_group_poll import adapt_group_message_event


def test_group_join_action_sends_moderator_welcome() -> None:
    async def scenario() -> None:
        runtime = object.__new__(module.VKModeratorRuntime)
        runtime.get_chat_zone = lambda _peer_id: "training_stream"
        runtime._get_chat_title = lambda _peer_id: "999. Курс Щенок. Современный Собаковод"
        runtime._should_send_join_greeting = lambda _peer_id, _user_id: True
        sent: list[tuple[int, int, str]] = []

        async def send_welcome(peer_id: int, user_id: int, zone: str) -> None:
            sent.append((peer_id, user_id, zone))

        runtime._send_welcome_message = send_welcome
        raw = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "id": 0,
                    "conversation_message_id": 5,
                    "peer_id": 2000000017,
                    "from_id": 1105209997,
                    "text": "",
                    "out": 0,
                    "action": {
                        "type": "chat_invite_user_by_link",
                        "member_id": 1105209997,
                    },
                }
            },
        )
        event = adapt_group_message_event(raw)
        assert event is not None
        assert event.message_id == 0

        with (
            patch.object(module, "_record_action"),
            patch.object(module, "_upsert_chat"),
        ):
            await runtime.process_message(event)

        assert sent == [(2000000017, 1105209997, "training_stream")]

    asyncio.run(scenario())


def test_initial_bulk_member_action_does_not_send_moderator_welcome() -> None:
    async def scenario() -> None:
        runtime = object.__new__(module.VKModeratorRuntime)
        runtime.get_chat_zone = lambda _peer_id: "training_stream"
        runtime._get_chat_title = lambda _peer_id: "999. Курс Щенок. Современный Собаковод"
        runtime._should_send_join_greeting = lambda _peer_id, _user_id: True
        sent: list[tuple[int, int, str]] = []

        async def send_welcome(peer_id: int, user_id: int, zone: str) -> None:
            sent.append((peer_id, user_id, zone))

        runtime._send_welcome_message = send_welcome
        raw = SimpleNamespace(
            type="message_new",
            object={
                "message": {
                    "id": 0,
                    "conversation_message_id": 5,
                    "peer_id": 2000000017,
                    "from_id": -225075265,
                    "text": "",
                    "out": 0,
                    "action": {"type": "chat_invite_user", "member_id": 1105209997},
                }
            },
        )
        event = adapt_group_message_event(raw)
        assert event is not None

        with patch.object(module, "_record_action") as record_action, patch.object(module, "_upsert_chat"):
            await runtime.process_message(event)

        assert sent == []
        assert any(call.kwargs.get("action") == "skip_initial_member_welcome" for call in record_action.call_args_list)

    asyncio.run(scenario())


def test_vk_moderation_categories_keep_existing_actions_with_cmid() -> None:
    async def scenario() -> None:
        class Analyzer:
            category = "нейтрально"

            async def analyze_vk(self, _text: str) -> str:
                return self.category

        analyzer = Analyzer()
        runtime = object.__new__(module.VKModeratorRuntime)
        runtime.analyzer = analyzer
        runtime.settings = {"dry_run": "false"}
        runtime._get_user_name = lambda _user_id: "Участник"
        forwarded: list[tuple[int, int, str, int, int | None]] = []
        deleted: list[dict] = []
        sent: list[tuple[int, str]] = []

        async def forward(user_id, peer_id, category, message_id, *, cmid=None):
            forwarded.append((user_id, peer_id, category, message_id, cmid))

        runtime.forward_to_log = forward
        runtime._delete_chat_message = lambda **kwargs: deleted.append(kwargs)
        runtime._send_message = lambda peer_id, text: sent.append((peer_id, text)) or {"message_id": None, "cmid": None}

        with (
            patch.object(module, "_record_action"),
            patch.object(module, "_template_enabled", return_value=True),
            patch.object(module, "_template_value", return_value="{user_mention}, поддержка возвратов"),
        ):
            analyzer.category = "негатив"
            await runtime._moderate_regular_member_message(
                from_id=123, peer_id=2000000017, zone="training_stream", text="негатив", message_id=0, cmid=21
            )
            assert forwarded[-1] == (123, 2000000017, "негатив", 0, 21)
            assert deleted[-1] == {"peer_id": 2000000017, "message_id": 0, "cmid": 21}
            assert sent == []

            analyzer.category = "возврат"
            await runtime._moderate_regular_member_message(
                from_id=123, peer_id=2000000017, zone="training_stream", text="возврат", message_id=0, cmid=22
            )
            assert forwarded[-1] == (123, 2000000017, "возврат", 0, 22)
            assert deleted[-1]["cmid"] == 22
            assert sent[-1][1].endswith("поддержка возвратов")

            delete_count = len(deleted)
            analyzer.category = "техпод"
            await runtime._moderate_regular_member_message(
                from_id=123, peer_id=2000000017, zone="training_stream", text="техпод", message_id=0, cmid=23
            )
            assert forwarded[-1] == (123, 2000000017, "техпод", 0, 23)
            assert len(deleted) == delete_count

    asyncio.run(scenario())


def test_vk_log_forward_and_delete_use_community_cmids() -> None:
    class Messages:
        def __init__(self) -> None:
            self.sent: list[dict] = []
            self.deleted: list[dict] = []

        def send(self, **params):
            self.sent.append(params)
            return 1

        def delete(self, **params):
            self.deleted.append(params)
            return 1

    messages = Messages()
    runtime = object.__new__(module.VKModeratorRuntime)
    runtime.settings = {"dry_run": "false"}
    runtime.log_chat_id = 2000000020
    runtime.vk = SimpleNamespace(messages=messages)
    runtime._get_user_name = lambda _user_id: "Участник"
    runtime._get_chat_title = lambda _peer_id: "Поток"

    with patch.object(module, "_record_action"):
        asyncio.run(runtime.forward_to_log(123, 2000000017, "негатив", 0, cmid=24))
        runtime._delete_chat_message(peer_id=2000000017, message_id=0, cmid=24)

    assert messages.sent[0]["peer_id"] == 2000000020
    assert '"conversation_message_ids": [24]' in messages.sent[0]["forward"]
    assert messages.deleted == [{"delete_for_all": 1, "peer_id": 2000000017, "cmids": [24]}]
