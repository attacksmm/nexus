import asyncio
from typing import Any

from orchestrator.vk_poll import (
    SharedVkPollHub,
    VkPollSubscription,
    _requires_manual_vk_intervention,
    _VkPollWorker,
)


class FakeWorker:
    def __init__(self, token_key: str, token: str) -> None:
        self.token_key = token_key
        self.token = token
        self.vk = None
        self.own_id = 0
        self.running = False
        self.stopped = False
        self.stop_timeout: float | None = None
        self.subscriptions: dict[str, VkPollSubscription] = {}

    @property
    def subscriber_count(self) -> int:
        return len(self.subscriptions)

    def initialize(self) -> None:
        self.vk = object()
        self.own_id = 42

    def create_api_client(self) -> object:
        return object()

    def add_subscription(self, subscription: VkPollSubscription) -> None:
        if subscription.subscriber_id in self.subscriptions:
            raise RuntimeError("duplicate subscriber")
        self.subscriptions[subscription.subscriber_id] = subscription

    def remove_subscription(self, subscriber_id: str) -> None:
        self.subscriptions.pop(subscriber_id, None)

    def start(self) -> None:
        self.running = True

    def stop(self, timeout: float = 30.0, *, warn_if_alive: bool = True) -> None:
        self.running = False
        self.stopped = True
        self.stop_timeout = timeout

    def _subscriber_snapshot(self) -> list[VkPollSubscription]:
        return list(self.subscriptions.values())

    def snapshot(self) -> dict[str, Any]:
        subscribers = self._subscriber_snapshot()
        return {
            "token": self.token_key[:10],
            "running": self.running,
            "thread_alive": self.running,
            "own_id": self.own_id,
            "subscribers": [subscription.subscriber_id for subscription in subscribers],
            "subscriber_metrics": [subscription.snapshot() for subscription in subscribers],
            "started_at": "",
            "last_event_at": "",
            "last_error_at": "",
            "last_reconnect_at": "",
            "events_dispatched": 0,
            "poll_errors": 0,
            "reconnections": 0,
        }

    def emit(self, value: Any) -> None:
        for subscription in self._subscriber_snapshot():
            subscription.enqueue_event(value)

    def fail(self, error: Exception) -> None:
        for subscription in self._subscriber_snapshot():
            subscription.enqueue_error(error)


def test_same_token_uses_one_connection_and_preserves_each_subscriber_order() -> None:
    async def scenario() -> None:
        workers: list[FakeWorker] = []

        def factory(token_key: str, token: str) -> FakeWorker:
            worker = FakeWorker(token_key, token)
            workers.append(worker)
            return worker

        hub = SharedVkPollHub(worker_factory=factory)
        first_events: list[str] = []
        second_events: list[str] = []

        async def first(value: str) -> None:
            first_events.append(value)

        async def second(value: str) -> None:
            second_events.append(value)

        first_sub = await hub.subscribe(subscriber_id="club", token="same-token", on_event=first)
        second_sub = await hub.subscribe(subscriber_id="course", token="same-token", on_event=second)
        assert len(workers) == 1
        assert first_sub.vk is not second_sub.vk
        assert first_sub.own_id == second_sub.own_id == 42
        first_sub.activate()
        second_sub.activate()

        workers[0].emit("one")
        workers[0].emit("two")
        await asyncio.wait_for(
            asyncio.gather(first_sub._queue.join(), second_sub._queue.join()), timeout=2
        )
        assert first_events == ["one", "two"]
        assert second_events == ["one", "two"]
        assert hub.snapshot()["connections"] == 1
        assert hub.snapshot()["subscribers"] == 2
        metrics = hub.snapshot()["workers"][0]["subscriber_metrics"]
        assert {row["events_processed"] for row in metrics} == {2}
        assert {row["queue_depth"] for row in metrics} == {0}
        assert {row["handler_failures"] for row in metrics} == {0}

        await first_sub.close()
        assert workers[0].running is True
        await second_sub.close()
        assert workers[0].stopped is True
        assert hub.snapshot()["connections"] == 0

    asyncio.run(scenario())


def test_different_tokens_use_different_connections() -> None:
    async def scenario() -> None:
        workers: list[FakeWorker] = []

        def factory(token_key: str, token: str) -> FakeWorker:
            worker = FakeWorker(token_key, token)
            workers.append(worker)
            return worker

        hub = SharedVkPollHub(worker_factory=factory)

        async def ignore(_value: Any) -> None:
            return None

        first = await hub.subscribe(subscriber_id="one", token="token-a", on_event=ignore)
        second = await hub.subscribe(subscriber_id="two", token="token-b", on_event=ignore)
        first.activate()
        second.activate()
        assert len(workers) == 2
        assert hub.snapshot()["connections"] == 2
        await hub.shutdown()
        assert all(worker.stopped for worker in workers)
        assert all(worker.stop_timeout == 2 for worker in workers)

    asyncio.run(scenario())


def test_transport_errors_are_fanned_out_without_stopping_other_subscribers() -> None:
    async def scenario() -> None:
        workers: list[FakeWorker] = []

        def factory(token_key: str, token: str) -> FakeWorker:
            worker = FakeWorker(token_key, token)
            workers.append(worker)
            return worker

        hub = SharedVkPollHub(worker_factory=factory)
        errors: dict[str, list[str]] = {"one": [], "two": []}

        async def ignore(_value: Any) -> None:
            return None

        async def first_error(error: Exception) -> None:
            errors["one"].append(str(error))

        async def second_error(error: Exception) -> None:
            errors["two"].append(str(error))

        first = await hub.subscribe(
            subscriber_id="one", token="shared", on_event=ignore, on_error=first_error
        )
        second = await hub.subscribe(
            subscriber_id="two", token="shared", on_event=ignore, on_error=second_error
        )
        first.activate()
        second.activate()
        workers[0].fail(RuntimeError("temporary failure"))
        await asyncio.wait_for(
            asyncio.gather(first._queue.join(), second._queue.join()), timeout=2
        )
        assert errors == {"one": ["temporary failure"], "two": ["temporary failure"]}
        assert workers[0].running is True
        metrics = hub.snapshot()["workers"][0]["subscriber_metrics"]
        assert {row["errors_processed"] for row in metrics} == {1}
        await hub.shutdown()

    asyncio.run(scenario())


def test_vk_auth_and_account_challenges_require_manual_intervention() -> None:
    assert _requires_manual_vk_intervention(
        RuntimeError("[5] User authorization failed: user is blocked.")
    )
    assert _requires_manual_vk_intervention(RuntimeError("[17] Validation required"))
    assert _requires_manual_vk_intervention(RuntimeError("invalid access_token (4)"))
    assert not _requires_manual_vk_intervention(RuntimeError("temporary network failure"))


def test_worker_halts_after_first_permanent_auth_error() -> None:
    class BlockedLongPoll:
        def listen(self):
            raise RuntimeError("[5] User authorization failed: user is blocked.")

    worker = _VkPollWorker("test-token-key", "test-token")
    worker.vk = object()
    worker._longpoll = BlockedLongPoll()

    worker._run()

    snapshot = worker.snapshot()
    assert snapshot["poll_errors"] == 1
    assert snapshot["reconnections"] == 0
    assert snapshot["halted_at"]
    assert "user is blocked" in snapshot["halt_reason"]
