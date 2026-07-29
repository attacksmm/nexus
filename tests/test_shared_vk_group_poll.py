import asyncio
from types import SimpleNamespace
from typing import Any

from orchestrator.vk_group_poll import (
    SharedVkGroupPollHub,
    VkGroupPollSubscription,
    adapt_group_message_event,
)


class FakeGroupWorker:
    def __init__(self, token_key: str, token: str, group_id: int) -> None:
        self.token_key = token_key
        self.token = token
        self.group_id = group_id
        self.own_id = -group_id
        self.vk = None
        self.running = False
        self.stopped = False
        self.stop_timeout: float | None = None
        self.subscriptions: dict[str, VkGroupPollSubscription] = {}

    @property
    def subscriber_count(self) -> int:
        return len(self.subscriptions)

    def initialize(self) -> None:
        self.vk = object()

    def create_api_client(self) -> object:
        return object()

    def add_subscription(self, subscription: VkGroupPollSubscription) -> None:
        self.subscriptions[subscription.subscriber_id] = subscription

    def remove_subscription(self, subscriber_id: str) -> None:
        self.subscriptions.pop(subscriber_id, None)

    def start(self) -> None:
        self.running = True

    def stop(self, timeout: float = 30.0, *, warn_if_alive: bool = True) -> None:
        self.running = False
        self.stopped = True
        self.stop_timeout = timeout

    def _subscriber_snapshot(self) -> list[VkGroupPollSubscription]:
        return list(self.subscriptions.values())

    def snapshot(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "running": self.running,
            "subscriber_metrics": [sub.snapshot() for sub in self._subscriber_snapshot()],
        }

    def emit(self, value: Any) -> None:
        for subscription in self._subscriber_snapshot():
            subscription.enqueue_event(value)


def test_message_new_adapter_preserves_moderator_fields() -> None:
    raw = SimpleNamespace(
        type="message_new",
        object={
            "message": {
                "id": 71,
                "conversation_message_id": 12,
                "peer_id": 2000000042,
                "from_id": 123,
                "text": "Тест",
                "out": 0,
                "action": {"type": "chat_invite_user_by_link", "member_id": 123},
            }
        },
    )
    event = adapt_group_message_event(raw)
    assert event is not None
    assert event.type == "MESSAGE_NEW"
    assert event.peer_id == 2000000042
    assert event.user_id == 123
    assert event.message_id == 71
    assert event.text == "Тест"
    assert event.message_payload["action"]["member_id"] == 123
    assert event.extra_values["conversation_message_id"] == 12
    assert event.from_me is False
    assert adapt_group_message_event(SimpleNamespace(type="message_reply", object={})) is None


def test_same_community_uses_one_connection_and_isolates_subscribers() -> None:
    async def scenario() -> None:
        workers: list[FakeGroupWorker] = []

        def factory(token_key: str, token: str, group_id: int) -> FakeGroupWorker:
            worker = FakeGroupWorker(token_key, token, group_id)
            workers.append(worker)
            return worker

        hub = SharedVkGroupPollHub(worker_factory=factory)
        first_events: list[str] = []
        second_events: list[str] = []

        async def first(value: str) -> None:
            first_events.append(value)

        async def second(value: str) -> None:
            second_events.append(value)

        first_sub = await hub.subscribe(
            subscriber_id="club", token="same", group_id=225075265, on_event=first
        )
        second_sub = await hub.subscribe(
            subscriber_id="course", token="same", group_id=225075265, on_event=second
        )
        first_sub.activate()
        second_sub.activate()
        assert len(workers) == 1
        assert first_sub.vk is not second_sub.vk
        assert first_sub.own_id == second_sub.own_id == -225075265
        workers[0].emit("one")
        workers[0].emit("two")
        await asyncio.wait_for(
            asyncio.gather(first_sub._queue.join(), second_sub._queue.join()), timeout=2
        )
        assert first_events == ["one", "two"]
        assert second_events == ["one", "two"]
        assert hub.snapshot()["connections"] == 1
        await first_sub.close()
        assert workers[0].running is True
        await second_sub.close()
        assert workers[0].stopped is True

    asyncio.run(scenario())


def test_same_token_for_different_communities_uses_separate_connections() -> None:
    async def scenario() -> None:
        workers: list[FakeGroupWorker] = []

        def factory(token_key: str, token: str, group_id: int) -> FakeGroupWorker:
            worker = FakeGroupWorker(token_key, token, group_id)
            workers.append(worker)
            return worker

        hub = SharedVkGroupPollHub(worker_factory=factory)

        async def ignore(_value: Any) -> None:
            return None

        first = await hub.subscribe(subscriber_id="one", token="same", group_id=1, on_event=ignore)
        second = await hub.subscribe(subscriber_id="two", token="same", group_id=2, on_event=ignore)
        first.activate()
        second.activate()
        assert len(workers) == 2
        await hub.shutdown()
        assert all(worker.stopped for worker in workers)
        assert all(worker.stop_timeout == 2 for worker in workers)

    asyncio.run(scenario())
