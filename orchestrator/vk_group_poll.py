"""Shared VK Bots Long Poll transport for Nexus community modules.

One blocking connection is kept for each unique community/token pair. Business
modules receive isolated ordered queues and keep their own chat filters and
handlers. The user Long Poll transport remains separate for legacy modules.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

from orchestrator.vk_poll import (
    VkPollSubscription,
    _VkPollWorker,
)


EventHandler = Callable[[Any], Awaitable[None]]
ErrorHandler = Callable[[Exception], Awaitable[None]]
GroupWorkerFactory = Callable[[str, str, int], "_VkGroupPollWorker"]
VkGroupPollSubscription = VkPollSubscription

_logger = logging.getLogger("nexus.vk-group-poll")


def _community_key(token: str, group_id: int) -> str:
    return hashlib.sha256(f"{int(group_id)}:{token}".encode("utf-8")).hexdigest()


def _value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    getter = getattr(source, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except TypeError:
            pass
    return getattr(source, key, default)


def adapt_group_message_event(event: Any) -> Any | None:
    """Convert a Bots Long Poll ``message_new`` event to the legacy handler shape."""

    event_type = getattr(getattr(event, "type", ""), "value", getattr(event, "type", ""))
    if str(event_type or "").strip().lower().split(".")[-1] != "message_new":
        return None
    payload = getattr(event, "object", None) or {}
    message = _value(payload, "message", {}) or {}
    peer_id = int(_value(message, "peer_id", 0) or 0)
    from_id = int(_value(message, "from_id", 0) or 0)
    message_id = int(_value(message, "id", 0) or 0)
    cmid = _value(message, "conversation_message_id")
    extra_values = {
        "from": from_id,
        "from_id": from_id,
        "user_id": from_id,
        "conversation_message_id": cmid,
        "cmid": cmid,
    }
    return SimpleNamespace(
        type="MESSAGE_NEW",
        from_me=bool(int(_value(message, "out", 0) or 0)),
        peer_id=peer_id,
        user_id=from_id,
        text=str(_value(message, "text", "") or ""),
        message_id=message_id,
        message_payload=message,
        extra_values=extra_values,
        raw=event,
    )


class _VkGroupPollWorker(_VkPollWorker):
    """One ``VkBotLongPoll`` worker for a community access token."""

    def __init__(self, token_key: str, token: str, group_id: int) -> None:
        super().__init__(token_key, token)
        self.group_id = int(group_id)

    def initialize(self) -> None:
        import vk_api
        from vk_api.bot_longpoll import VkBotLongPoll

        self._vk_session = vk_api.VkApi(token=self._token)
        self.vk = self._vk_session.get_api()
        self._longpoll = VkBotLongPoll(self._vk_session, self.group_id)
        self.own_id = -self.group_id

    def _recreate_longpoll(self) -> None:
        if self._vk_session is None:
            raise RuntimeError("VK community poll session is not initialized")
        from vk_api.bot_longpoll import VkBotLongPoll

        self._longpoll = VkBotLongPoll(self._vk_session, self.group_id)

    def snapshot(self) -> dict[str, Any]:
        return {**super().snapshot(), "group_id": self.group_id, "transport": "bots_long_poll"}


class SharedVkGroupPollHub:
    """Registry that guarantees one Bots Long Poll connection per community/token."""

    def __init__(self, worker_factory: GroupWorkerFactory | None = None) -> None:
        self._worker_factory = worker_factory or _VkGroupPollWorker
        self._workers: dict[str, _VkGroupPollWorker] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        *,
        subscriber_id: str,
        token: str,
        group_id: int,
        on_event: EventHandler,
        on_error: ErrorHandler | None = None,
    ) -> VkGroupPollSubscription:
        clean_id = str(subscriber_id or "").strip()
        clean_token = str(token or "").strip()
        clean_group_id = int(group_id or 0)
        if not clean_id:
            raise ValueError("subscriber_id is required")
        if not clean_token:
            raise ValueError("VK community token is required")
        if clean_group_id <= 0:
            raise ValueError("VK community ID is required")
        token_key = _community_key(clean_token, clean_group_id)
        async with self._lock:
            worker = self._workers.get(token_key)
            created = worker is None
            if worker is None:
                worker = self._worker_factory(token_key, clean_token, clean_group_id)
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
                "VK community poll subscriber added subscriber=%s group=%s token=%s subscribers=%s",
                clean_id,
                clean_group_id,
                token_key[:10],
                worker.subscriber_count,
            )
            return subscription

    async def unsubscribe(self, subscription: VkGroupPollSubscription) -> None:
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
            "VK community poll subscriber removed subscriber=%s group=%s remaining=%s",
            subscription.subscriber_id,
            worker.group_id,
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
                    asyncio.to_thread(worker.stop, stop_timeout, warn_if_alive=False)
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
            "workers": [worker.snapshot() for worker in self._workers.values()],
        }


shared_vk_group_poll_hub = SharedVkGroupPollHub()
