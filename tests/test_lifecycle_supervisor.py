import asyncio
import logging

from orchestrator.lifecycle import LifecycleSupervisor


def test_supervisor_tracks_descendant_tasks_and_cancels_them() -> None:
    async def scenario() -> None:
        supervisor = LifecycleSupervisor()
        supervisor.install()
        lifecycle = supervisor.register("demo", logging.getLogger("test.lifecycle.demo"))
        child_started = asyncio.Event()
        child_stopped = asyncio.Event()

        async def child() -> None:
            child_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                child_stopped.set()

        async def parent() -> None:
            asyncio.create_task(child(), name="demo-child")
            await asyncio.Event().wait()

        try:
            with lifecycle.activate():
                parent_task = asyncio.create_task(parent(), name="demo-parent")
            lifecycle.mark_running()
            await asyncio.wait_for(child_started.wait(), timeout=1)
            snapshot = lifecycle.snapshot()
            assert snapshot["state"] == "running"
            assert snapshot["active_count"] == 2
            assert {row["name"] for row in snapshot["active_tasks"]} == {
                "demo-child",
                "demo-parent",
            }

            await supervisor.unregister("demo", lifecycle, timeout=1)
            assert parent_task.cancelled()
            assert child_stopped.is_set()
            assert supervisor.snapshot() == []
        finally:
            supervisor.uninstall()

    asyncio.run(scenario())


def test_supervisor_records_background_failures() -> None:
    async def scenario() -> None:
        supervisor = LifecycleSupervisor()
        supervisor.install()
        lifecycle = supervisor.register("broken", logging.getLogger("test.lifecycle.broken"))

        async def fail() -> None:
            raise RuntimeError("intentional failure")

        try:
            with lifecycle.activate():
                task = asyncio.create_task(fail(), name="broken-worker")
            lifecycle.mark_running()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)
            snapshot = lifecycle.snapshot()
            assert snapshot["active_count"] == 0
            assert snapshot["total_failed"] == 1
            assert snapshot["last_error"] == "RuntimeError: intentional failure"
            await supervisor.unregister("broken", lifecycle, timeout=1)
        finally:
            supervisor.uninstall()

    asyncio.run(scenario())


def test_supervisor_recovers_transient_failure_after_new_successful_work() -> None:
    async def scenario() -> None:
        supervisor = LifecycleSupervisor()
        supervisor.install()
        lifecycle = supervisor.register("telegram", logging.getLogger("test.lifecycle.telegram"))

        class TimedOut(Exception):
            pass

        async def fail_transiently() -> None:
            raise TimedOut("Timed out")

        async def replacement_cycle() -> None:
            await asyncio.sleep(0)

        try:
            with lifecycle.activate():
                failed = asyncio.create_task(fail_transiently(), name="telegram-poll")
            lifecycle.mark_running()
            await asyncio.gather(failed, return_exceptions=True)
            await asyncio.sleep(0)
            degraded = lifecycle.snapshot()
            assert degraded["health"] == "degraded"
            assert degraded["total_failed"] == 1
            assert degraded["unrecovered_failures"] == 1

            with lifecycle.activate():
                recovered = asyncio.create_task(replacement_cycle(), name="telegram-poll")
            await recovered
            await asyncio.sleep(0)
            healthy = lifecycle.snapshot()
            assert healthy["health"] == "ok"
            assert healthy["total_failed"] == 1
            assert healthy["recovered_failures"] == 1
            assert healthy["unrecovered_failures"] == 0
            assert healthy["last_error"] == ""
            assert healthy["last_recovered_at"]
            await supervisor.unregister("telegram", lifecycle, timeout=1)
        finally:
            supervisor.uninstall()

    asyncio.run(scenario())


def test_supervisor_does_not_clear_programming_failure_on_later_success() -> None:
    async def scenario() -> None:
        supervisor = LifecycleSupervisor()
        supervisor.install()
        lifecycle = supervisor.register("broken", logging.getLogger("test.lifecycle.permanent"))

        async def fail() -> None:
            raise RuntimeError("invalid worker state")

        async def succeed() -> None:
            await asyncio.sleep(0)

        try:
            with lifecycle.activate():
                failed = asyncio.create_task(fail(), name="critical-worker")
            lifecycle.mark_running()
            await asyncio.gather(failed, return_exceptions=True)
            await asyncio.sleep(0)
            with lifecycle.activate():
                successful = asyncio.create_task(succeed(), name="unrelated-work")
            await successful
            await asyncio.sleep(0)
            snapshot = lifecycle.snapshot()
            assert snapshot["health"] == "degraded"
            assert snapshot["unrecovered_failures"] == 1
            assert snapshot["recovered_failures"] == 0
            assert snapshot["last_error"] == "RuntimeError: invalid worker state"
            await supervisor.unregister("broken", lifecycle, timeout=1)
        finally:
            supervisor.uninstall()

    asyncio.run(scenario())
