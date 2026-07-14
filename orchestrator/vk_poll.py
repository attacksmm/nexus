"""Shared VK user long-poll transport for dynamically loaded Nexus modules.

Business modules keep their own filters and handlers.  This module owns only the
network connection and fans each VK event out to subscribers that use the same
user token.  A separate connection is created only when a genuinely different
token is registered.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any


EventHandler = Callable[[Any], Awaitable[None]]
ErrorHandler = Callable[[Exception], Awaitable[None]]
WorkerFactory = Callable[[str, str], "_VkPollWorker"]

_logger = logging.getLogger("nexus.vk-poll")


def _token_key(token: str) -> str:
    """Return a stable non-secret identity for connection de-duplication."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _run_handler(handler: Callable[[Any], Awaitable[None]], value: Any) -> None:
    """Keep blocking vk_api work outside the Nexus event loop.

    Existing VK handlers intentionally mix async OpenRouter calls with the
    synchronous vk_api client.  Running each subscriber in a worker thread
    preserves the old isolation while the network long-poll itself is shared.
    """

    asyncio.run(handler(value))


class VkPollSubscription:
    """One ordered, isolated consumer of the shared VK event stream."""

    def __init__(
        self,
        *,
        hub: "SharedVkPollHub",
        worker: "_VkPollWorker",
        subscriber_id: str,
        vk: Any,
        on_event: EventHandler,
        on_error: ErrorHandler | None,
    ) -> None:
        self._hub = hub
        self._worker = worker
        self.subscriber_id = subscriber_id
        self._vk = vk
        self._on_event = on_event
        self._on_error = on_error
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._active = asyncio.Event()
        self._closed = False
        self._consumer_task = asyncio.create_task(
            self._consume(), name=f"vk-poll-consumer:{subscriber_id}"
        )

    @property
    def vk(self) -> Any:
        return self._vk

    @property
    def own_id(self) -> int:
        return self._worker.own_id

    @property
    def running(self) -> bool:
        return not self._closed and self._worker.running

    def activate(self) -> None:
        """Allow delivery after the subscriber has copied shared client state."""

        if not self._closed:
            self._active.set()

    def enqueue_event(self, event: Any) -> None:
        self._enqueue_from_thread("event", event)

    def enqueue_error(self, error: Exception) -> None:
        self._enqueue_from_thread("error", error)

    def _enqueue_from_thread(self, kind: str, value: Any) -> None:
        if self._closed or self._loop.is_closed():
            return
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, (kind, value))
        except RuntimeError:
            return

    async def _consume(self) -> None:
        while True:
            kind, value = await self._queue.get()
            try:
                if kind == "close":
                    return
                await self._active.wait()
                if kind == "event":
                    await asyncio.to_thread(_run_handler, self._on_event, value)
                elif kind == "error" and self._on_error is not None:
                    await asyncio.to_thread(_run_handler, self._on_error, value)
            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception(
                    "VK poll subscriber failed subscriber=%s kind=%s",
                    self.subscriber_id,
                    kind,
                )
            finally:
                self._queue.task_done()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._active.set()
        await self._hub.unsubscribe(self)
        self._queue.put_nowait(("close", None))
        try:
            await asyncio.wait_for(self._consumer_task, timeout=10)
        except TimeoutError:
            self._consumer_task.cancel()
            await asyncio.gather(self._consumer_task, return_exceptions=True)


class _VkPollWorker:
    """One blocking vk_api long-poll thread shared by matching subscribers."""

    def __init__(self, token_key: str, token: str) -> None:
        self.token_key = token_key
        self._token = token
        self._subscribers: dict[str, VkPollSubscription] = {}
        self._subscribers_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._vk_session: Any | None = None
        self._longpoll: Any | None = None
        self.vk: Any | None = None
        self.own_id = 0

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive() and not self._stop_event.is_set())

    @property
    def subscriber_count(self) -> int:
        with self._subscribers_lock:
            return len(self._subscribers)

    def initialize(self) -> None:
        import vk_api
        from vk_api.longpoll import VkLongPoll

        self._vk_session = vk_api.VkApi(token=self._token)
        self.vk = self._vk_session.get_api()
        self._longpoll = VkLongPoll(self._vk_session)
        response = self.vk.users.get()
        if not isinstance(response, list) or not response:
            raise RuntimeError("VK users.get returned an empty response")
        self.own_id = int(response[0]["id"])

    def create_api_client(self) -> Any:
        """Give each subscriber its own requests session for outgoing VK API calls."""

        import vk_api

        return vk_api.VkApi(token=self._token).get_api()

    def add_subscription(self, subscription: VkPollSubscription) -> None:
        with self._subscribers_lock:
            if subscription.subscriber_id in self._subscribers:
                raise RuntimeError(f"VK poll subscriber already registered: {subscription.subscriber_id}")
            self._subscribers[subscription.subscriber_id] = subscription

    def remove_subscription(self, subscriber_id: str) -> None:
        with self._subscribers_lock:
            self._subscribers.pop(subscriber_id, None)

    def start(self) -> None:
        if self.running:
            return
        if self._longpoll is None or self.vk is None:
            raise RuntimeError("VK poll worker is not initialized")
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"nexus-vk-poll-{self.token_key[:10]}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(30)
        if thread and thread.is_alive():
            _logger.warning("VK poll thread did not stop before timeout token=%s", self.token_key[:10])
        self._thread = None

    def _subscriber_snapshot(self) -> list[VkPollSubscription]:
        with self._subscribers_lock:
            return list(self._subscribers.values())

    def _dispatch_event(self, event: Any) -> None:
        for subscriber in self._subscriber_snapshot():
            subscriber.enqueue_event(event)

    def _dispatch_error(self, error: Exception) -> None:
        for subscriber in self._subscriber_snapshot():
            subscriber.enqueue_error(error)

    def _recreate_longpoll(self) -> None:
        if self._vk_session is None or self._longpoll is None:
            raise RuntimeError("VK poll session is not initialized")
        self._longpoll = self._longpoll.__class__(self._vk_session)

    def _run(self) -> None:
        _logger.info(
            "VK poll started token=%s subscribers=%s own_id=%s",
            self.token_key[:10],
            self.subscriber_count,
            self.own_id,
        )
        while not self._stop_event.is_set():
            try:
                for event in self._longpoll.listen():
                    if self._stop_event.is_set():
                        break
                    self._dispatch_event(event)
            except Exception as error:
                if self._stop_event.is_set():
                    break
                _logger.warning("VK poll error token=%s: %s", self.token_key[:10], error)
                self._dispatch_error(error)
                if self._stop_event.wait(5):
                    break
                try:
                    self._recreate_longpoll()
                except Exception as recreate_error:
                    _logger.warning(
                        "VK poll reconnect failed token=%s: %s",
                        self.token_key[:10],
                        recreate_error,
                    )
                    self._dispatch_error(recreate_error)
        _logger.info("VK poll stopped token=%s", self.token_key[:10])


class SharedVkPollHub:
    """Registry that guarantees one long-poll connection per VK token."""

    def __init__(self, worker_factory: WorkerFactory | None = None) -> None:
        self._worker_factory = worker_factory or _VkPollWorker
        self._workers: dict[str, _VkPollWorker] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        *,
        subscriber_id: str,
        token: str,
        on_event: EventHandler,
        on_error: ErrorHandler | None = None,
    ) -> VkPollSubscription:
        clean_id = str(subscriber_id or "").strip()
        clean_token = str(token or "").strip()
        if not clean_id:
            raise ValueError("subscriber_id is required")
        if not clean_token:
            raise ValueError("VK token is required")
        token_key = _token_key(clean_token)
        async with self._lock:
            worker = self._workers.get(token_key)
            created = worker is None
            if worker is None:
                worker = self._worker_factory(token_key, clean_token)
                await asyncio.to_thread(worker.initialize)
                self._workers[token_key] = worker
            try:
                subscriber_vk = await asyncio.to_thread(worker.create_api_client)
            except Exception:
                if created:
                    self._workers.pop(token_key, None)
                    await asyncio.to_thread(worker.stop)
                raise
            subscription = VkPollSubscription(
                hub=self,
                worker=worker,
                subscriber_id=clean_id,
                vk=subscriber_vk,
                on_event=on_event,
                on_error=on_error,
            )
            try:
                worker.add_subscription(subscription)
                if created:
                    worker.start()
            except Exception:
                subscription._closed = True
                subscription._consumer_task.cancel()
                await asyncio.gather(subscription._consumer_task, return_exceptions=True)
                worker.remove_subscription(clean_id)
                if created:
                    self._workers.pop(token_key, None)
                    await asyncio.to_thread(worker.stop)
                raise
            _logger.info(
                "VK poll subscriber added subscriber=%s token=%s subscribers=%s",
                clean_id,
                token_key[:10],
                worker.subscriber_count,
            )
            return subscription

    async def unsubscribe(self, subscription: VkPollSubscription) -> None:
        worker = subscription._worker
        should_stop = False
        async with self._lock:
            worker.remove_subscription(subscription.subscriber_id)
            if worker.subscriber_count == 0 and self._workers.get(worker.token_key) is worker:
                self._workers.pop(worker.token_key, None)
                should_stop = True
        if should_stop:
            await asyncio.to_thread(worker.stop)
        _logger.info(
            "VK poll subscriber removed subscriber=%s token=%s remaining=%s",
            subscription.subscriber_id,
            worker.token_key[:10],
            worker.subscriber_count,
        )

    async def shutdown(self) -> None:
        async with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        subscriptions = [sub for worker in workers for sub in worker._subscriber_snapshot()]
        for subscription in subscriptions:
            subscription._closed = True
        if workers:
            await asyncio.gather(*(asyncio.to_thread(worker.stop) for worker in workers))
        if subscriptions:
            for subscription in subscriptions:
                subscription._consumer_task.cancel()
            await asyncio.gather(*(sub._consumer_task for sub in subscriptions), return_exceptions=True)

    def snapshot(self) -> dict[str, Any]:
        return {
            "connections": len(self._workers),
            "subscribers": sum(worker.subscriber_count for worker in self._workers.values()),
            "workers": [
                {
                    "token": worker.token_key[:10],
                    "running": worker.running,
                    "own_id": worker.own_id,
                    "subscribers": [sub.subscriber_id for sub in worker._subscriber_snapshot()],
                }
                for worker in self._workers.values()
            ],
        }


shared_vk_poll_hub = SharedVkPollHub()
