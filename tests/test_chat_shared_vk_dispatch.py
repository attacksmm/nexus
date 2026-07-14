import asyncio
import importlib
from types import SimpleNamespace


def _exercise_module(module_name: str) -> None:
    module = importlib.import_module(module_name)
    calls: list[tuple[str, object]] = []
    actions: list[dict[str, object]] = []

    async def process_message(event: object) -> None:
        calls.append(("message", event))

    async def process_chat_update(event: object) -> None:
        calls.append(("chat_update", event))

    original_record_action = module._record_action
    original_secret_value = module._secret_value
    module._record_action = lambda **kwargs: actions.append(kwargs)
    module._secret_value = lambda _key: ""
    runtime = module.VKModeratorRuntime(analyzer=None)
    runtime.process_message = process_message
    runtime.process_chat_update = process_chat_update
    try:
        message = SimpleNamespace(type="MESSAGE_NEW", from_me=False)
        own_message = SimpleNamespace(type="MESSAGE_NEW", from_me=True)
        update = SimpleNamespace(type="CHAT_UPDATE", from_me=False)
        asyncio.run(runtime._handle_shared_event(message))
        asyncio.run(runtime._handle_shared_event(own_message))
        asyncio.run(runtime._handle_shared_event(update))
        assert calls == [("message", message), ("chat_update", update)]

        async def fail(_event: object) -> None:
            raise RuntimeError("handler failure")

        runtime.process_message = fail
        asyncio.run(runtime._handle_shared_event(message))
        assert actions[-1]["action"] == "runtime_event_error"
        assert actions[-1]["status"] == "error"
    finally:
        module._record_action = original_record_action
        module._secret_value = original_secret_value


def test_club_moderator_shared_vk_dispatch() -> None:
    _exercise_module("module_chat_moderator.router")


def test_course_curator_shared_vk_dispatch() -> None:
    _exercise_module("module_chat_moderators.router")


def _exercise_idempotent_telegram_stop(module_name: str) -> None:
    module = importlib.import_module(module_name)
    calls: list[str] = []

    class FakeUpdater:
        running = False

        async def stop(self) -> None:
            raise AssertionError("stopped updater must not be stopped twice")

    class FakeApplication:
        updater = FakeUpdater()
        running = True
        _initialized = True

        async def stop(self) -> None:
            calls.append("application.stop")
            self.running = False

        async def shutdown(self) -> None:
            calls.append("application.shutdown")
            self._initialized = False

    runtime = module.TelegramModeratorRuntime(analyzer=None)
    runtime.app = FakeApplication()
    runtime.running = True
    asyncio.run(runtime.stop())
    assert calls == ["application.stop", "application.shutdown"]
    assert runtime.app is None
    assert runtime.running is False


def test_club_moderator_telegram_stop_is_idempotent() -> None:
    _exercise_idempotent_telegram_stop("module_chat_moderator.router")


def test_course_curator_telegram_stop_is_idempotent() -> None:
    _exercise_idempotent_telegram_stop("module_chat_moderators.router")
