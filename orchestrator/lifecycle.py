"""Lifecycle ownership for background asyncio tasks created by Nexus modules."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Coroutine, Iterator


_current_lifecycle: contextvars.ContextVar[ModuleLifecycle | None] = contextvars.ContextVar(
    "nexus_module_lifecycle", default=None
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_error(error: BaseException) -> str:
    message = str(error).replace("\n", " ").strip()
    return f"{type(error).__name__}: {message}"[:500]


def _is_recoverable_error(error: BaseException) -> bool:
    """Return true for short-lived transport failures that a worker can replace."""

    error_name = type(error).__name__.casefold()
    if error_name in {"timeouterror", "timedout", "networkerror", "connecterror"}:
        return True
    message = str(error).casefold()
    return any(
        marker in message
        for marker in (
            "bad gateway",
            "connection reset",
            "connection refused",
            "connection aborted",
            "network is unreachable",
            "temporarily unavailable",
            "timed out",
            "timeout",
            "http 502",
            "http 503",
            "http 504",
        )
    )


class ModuleLifecycle:
    """Tracks tasks owned by one loaded instance of a dynamic module."""

    def __init__(self, module_id: str, logger: logging.Logger) -> None:
        self.module_id = module_id
        self._logger = logger
        self._tasks: dict[asyncio.Task[Any], float] = {}
        self._state = "starting"
        self._started_at = _now()
        self._stopped_at = ""
        self._last_activity_at = self._started_at
        self._last_error = ""
        self._total_started = 0
        self._total_completed = 0
        self._total_cancelled = 0
        self._total_failed = 0
        self._recovered_failures = 0
        self._recoverable_failures_pending = 0
        self._unrecoverable_failures = 0
        self._last_recoverable_failure_started = 0.0
        self._last_recovered_at = ""
        self._shutdown_timeouts = 0

    @contextmanager
    def activate(self) -> Iterator["ModuleLifecycle"]:
        token = _current_lifecycle.set(self)
        try:
            yield self
        finally:
            _current_lifecycle.reset(token)

    def mark_running(self) -> None:
        if self._state == "starting":
            self._state = "running"
            self._last_activity_at = _now()

    def track(self, task: asyncio.Task[Any]) -> None:
        if task in self._tasks:
            return
        if self._state in {"stopping", "stopped"}:
            task.cancel()
            self._logger.warning(
                "Rejected background task for inactive module=%s task=%s",
                self.module_id,
                task.get_name(),
            )
            return
        self._tasks[task] = time.monotonic()
        self._total_started += 1
        self._last_activity_at = _now()
        task.add_done_callback(self._task_done)

    def create_task(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Explicit helper for modules; automatic task-factory tracking is preferred."""

        with self.activate():
            task = asyncio.create_task(coroutine, name=name)
        self.track(task)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        started = self._tasks.pop(task, None) or 0.0
        self._last_activity_at = _now()
        if task.cancelled():
            self._total_cancelled += 1
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            self._total_cancelled += 1
            return
        if error is None:
            self._total_completed += 1
            if (
                self._recoverable_failures_pending
                and started >= self._last_recoverable_failure_started
            ):
                self._recovered_failures += self._recoverable_failures_pending
                self._recoverable_failures_pending = 0
                self._last_recovered_at = self._last_activity_at
                if not self._unrecoverable_failures:
                    self._last_error = ""
            return
        self._total_failed += 1
        self._last_error = _safe_error(error)
        if _is_recoverable_error(error):
            self._recoverable_failures_pending += 1
            self._last_recoverable_failure_started = time.monotonic()
        else:
            self._unrecoverable_failures += 1
        self._logger.error(
            "Background task failed module=%s task=%s error=%s",
            self.module_id,
            task.get_name(),
            self._last_error,
        )

    async def shutdown(self, timeout: float = 15.0) -> None:
        if self._state == "stopped":
            return
        self._state = "stopping"
        current = asyncio.current_task()
        pending = [task for task in self._tasks if task is not current and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            _, still_pending = await asyncio.wait(pending, timeout=max(0.1, timeout))
            if still_pending:
                self._shutdown_timeouts += 1
                names = sorted(task.get_name() for task in still_pending)
                self._logger.error(
                    "Background tasks did not stop module=%s tasks=%s",
                    self.module_id,
                    names,
                )
                for task in still_pending:
                    task.cancel()
        self._state = "stopped"
        self._stopped_at = _now()
        self._last_activity_at = self._stopped_at

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        active = [
            {
                "name": task.get_name(),
                "age_seconds": round(max(0.0, now - started), 3),
            }
            for task, started in self._tasks.items()
            if not task.done()
        ]
        active.sort(key=lambda row: (row["name"], row["age_seconds"]))
        duplicate_names = sorted(
            name for name in {row["name"] for row in active}
            if sum(1 for row in active if row["name"] == name) > 1
        )
        return {
            "module_id": self.module_id,
            "state": self._state,
            "health": "degraded" if (
                self._recoverable_failures_pending
                or self._unrecoverable_failures
                or self._shutdown_timeouts
            ) else "ok",
            "started_at": self._started_at,
            "stopped_at": self._stopped_at,
            "last_activity_at": self._last_activity_at,
            "active_tasks": active,
            "active_count": len(active),
            "duplicate_task_names": duplicate_names,
            "total_started": self._total_started,
            "total_completed": self._total_completed,
            "total_cancelled": self._total_cancelled,
            "total_failed": self._total_failed,
            "recovered_failures": self._recovered_failures,
            "unrecovered_failures": self._recoverable_failures_pending + self._unrecoverable_failures,
            "shutdown_timeouts": self._shutdown_timeouts,
            "last_error": self._last_error,
            "last_recovered_at": self._last_recovered_at,
        }


class LifecycleSupervisor:
    """Global task-factory bridge and registry for loaded module instances."""

    def __init__(self) -> None:
        self._lifecycles: dict[str, ModuleLifecycle] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._previous_task_factory: Any = None
        self._installed_task_factory: Any = None

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is loop:
            return
        if self._loop is not None:
            raise RuntimeError("Lifecycle supervisor is already installed on another event loop")
        self._loop = loop
        self._previous_task_factory = loop.get_task_factory()
        self._installed_task_factory = self._task_factory
        loop.set_task_factory(self._installed_task_factory)

    def uninstall(self) -> None:
        loop, self._loop = self._loop, None
        if loop is not None and not loop.is_closed() and loop.get_task_factory() is self._installed_task_factory:
            loop.set_task_factory(self._previous_task_factory)
        self._previous_task_factory = None
        self._installed_task_factory = None

    def _task_factory(self, loop: asyncio.AbstractEventLoop, coroutine: Any, **kwargs: Any):
        if self._previous_task_factory is not None:
            task = self._previous_task_factory(loop, coroutine, **kwargs)
        else:
            task = asyncio.tasks.Task(coroutine, loop=loop, **kwargs)
        context = kwargs.get("context")
        lifecycle = context.get(_current_lifecycle) if context is not None else _current_lifecycle.get()
        if lifecycle is not None:
            if task.get_name().startswith("Task-"):
                coroutine_name = str(
                    getattr(coroutine, "__qualname__", "")
                    or getattr(getattr(coroutine, "cr_code", None), "co_name", "")
                    or "background"
                )
                task.set_name(f"{lifecycle.module_id}:{coroutine_name}"[:200])
            lifecycle.track(task)
        return task

    def register(self, module_id: str, logger: logging.Logger) -> ModuleLifecycle:
        existing = self._lifecycles.get(module_id)
        if existing is not None and existing.snapshot()["state"] != "stopped":
            raise RuntimeError(f"Module lifecycle already registered: {module_id}")
        lifecycle = ModuleLifecycle(module_id, logger)
        self._lifecycles[module_id] = lifecycle
        return lifecycle

    def get(self, module_id: str) -> ModuleLifecycle | None:
        return self._lifecycles.get(module_id)

    async def unregister(self, module_id: str, lifecycle: ModuleLifecycle, timeout: float = 15.0) -> None:
        await lifecycle.shutdown(timeout=timeout)
        if self._lifecycles.get(module_id) is lifecycle:
            self._lifecycles.pop(module_id, None)

    def snapshot(self) -> list[dict[str, Any]]:
        return [self._lifecycles[module_id].snapshot() for module_id in sorted(self._lifecycles)]
