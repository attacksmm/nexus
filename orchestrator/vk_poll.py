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
from datetime import datetime, timezone
from collections.abc import Awaitable, Callable
from typing import Any


EventHandler = Callable[[Any], Awaitable[None]]
ErrorHandler = Callable[[Exception], Awaitable[None]]
WorkerFactory = Callable[[str, str], "_VkPollWorker"]

_logger = logging.getLogger("nexus.vk-poll")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        self._started_at = _now()
        self._last_enqueued_at = ""
        self._last_processed_at = ""
        self._events_enqueued = 0
        self._errors_enqueued = 0
        self._events_processed = 0
        self._errors_processed = 0
        self._handler_failures = 0
        self._peak_queue_depth = 0
        self._processing = False
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
            self._loop.call_soon_threadsafe(self._enqueue_nowait, kind, value)
        except RuntimeError:
            return

    def _enqueue_nowait(self, kind: str, value: Any) -> None:
        if self._closed:
            return
        self._queue.put_nowait((kind, value))
        self._last_enqueued_at = _now()
        if kind == "event":
            self._events_enqueued += 1
        elif kind == "error":
            self._errors_enqueued += 1
        self._peak_queue_depth = max(self._peak_queue_depth, self._queue.qsize())

    async def _consume(self) -> None:
        while True:
            kind, value = await self._queue.get()
            try:
                if kind == "close":
                    return
                await self._active.wait()
                self._processing = True
                if kind == "event":
                    await asyncio.to_thread(_run_handler, self._on_event, value)
                    self._events_processed += 1
                elif kind == "error" and self._on_error is not None:
                    await asyncio.to_thread(_run_handler, self._on_error, value)
                    self._errors_processed += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                self._handler_failures += 1
                _logger.exception(
                    "VK poll subscriber failed subscriber=%s kind=%s",
                    self.subscriber_id,
                    kind,
                )
            finally:
                self._processing = False
                self._last_processed_at = _now()
                self._queue.task_done()

    def snapshot(self) -> dict[str, Any]:
        return {
            "subscriber_id": self.subscriber_id,
            "running": self.running,
            "started_at": self._started_at,
            "queue_depth": self._queue.qsize(),
            "peak_queue_depth": self._peak_queue_depth,
            "processing": self._processing,
            "events_enqueued": self._events_enqueued,
            "events_processed": self._events_processed,
            "errors_enqueued": self._errors_enqueued,
            "errors_processed": self._errors_processed,
            "handler_failures": self._handler_failures,
            "last_enqueued_at": self._last_enqueued_at,
            "last_processed_at": self._last_processed_at,
        }

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
        self._metrics_lock = threading.Lock()
        self._started_at = ""
        self._last_event_at = ""
        self._last_error_at = ""
        self._last_reconnect_at = ""
        self._events_dispatched = 0
        self._poll_errors = 0
        self._reconnections = 0

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
        with self._metrics_lock:
            self._started_at = _now()
        self._thread = threading.Thread(
            target=self._run,
            name=f"nexus-vk-poll-{self.token_key[:10]}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 30.0, *, warn_if_alive: bool = True) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(max(0.0, timeout))
        if thread and thread.is_alive() and warn_if_alive:
            _logger.warning("VK poll thread did not stop before timeout token=%s", self.token_key[:10])
        if not thread or not thread.is_alive():
            self._thread = None

    def _subscriber_snapshot(self) -> list[VkPollSubscription]:
        with self._subscribers_lock:
            return list(self._subscribers.values())

    def _dispatch_event(self, event: Any) -> None:
        with self._metrics_lock:
            self._events_dispatched += 1
            self._last_event_at = _now()
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
                with self._metrics_lock:
                    self._poll_errors += 1
                    self._last_error_at = _now()
                _logger.warning("VK poll error token=%s: %s", self.token_key[:10], error)
                self._dispatch_error(error)
                if self._stop_event.wait(5):
                    break
                try:
                    self._recreate_longpoll()
                    with self._metrics_lock:
                        self._reconnections += 1
                        self._last_reconnect_at = _now()
                except Exception as recreate_error:
                    _logger.warning(
                        "VK poll reconnect failed token=%s: %s",
                        self.token_key[:10],
                        recreate_error,
                    )
                    self._dispatch_error(recreate_error)
        _logger.info("VK poll stopped token=%s", self.token_key[:10])

    def snapshot(self) -> dict[str, Any]:
        with self._metrics_lock:
            metrics = {
                "started_at": self._started_at,
                "last_event_at": self._last_event_at,
                "last_error_at": self._last_error_at,
                "last_reconnect_at": self._last_reconnect_at,
                "events_dispatched": self._events_dispatched,
                "poll_errors": self._poll_errors,
                "reconnections": self._reconnections,
            }
        subscribers = self._subscriber_snapshot()
        return {
            "token": self.token_key[:10],
            "running": self.running,
            "thread_alive": bool(self._thread and self._thread.is_alive()),
            "own_id": self.own_id,
            "subscribers": [subscriber.subscriber_id for subscriber in subscribers],
            "subscriber_metrics": [subscriber.snapshot() for subscriber in subscribers],
            **metrics,
        }


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

    async def shutdown(self, stop_timeout: float = 2.0) -> None:
        async with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        subscriptions = [sub for worker in workers for sub in worker._subscriber_snapshot()]
        for subscription in subscriptions:
            subscription._closed = True
        if workers:
            await asyncio.gather(
                *(
                    asyncio.to_thread(
                        worker.stop,
                        stop_timeout,
                        warn_if_alive=False,
                    )
                    for worker in workers
                )
            )
        if subscriptions:
            for subscription in subscriptions:
                subscription._consumer_task.cancel()
            await asyncio.gather(*(sub._consumer_task for sub in subscriptions), return_exceptions=True)

    def snapshot(self) -> dict[str, Any]:
        return {
            "connections": len(self._workers),
            "subscribers": sum(worker.subscriber_count for worker in self._workers.values()),
            "workers": [
                worker.snapshot()
                for worker in self._workers.values()
            ],
        }


shared_vk_poll_hub = SharedVkPollHub()
