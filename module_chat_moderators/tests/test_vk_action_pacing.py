from types import SimpleNamespace

from module_chat_moderators import router as module


def test_vk_delete_uses_group_client_and_pacing_under_two_seconds(monkeypatch):
    calls = []
    client = SimpleNamespace(messages=SimpleNamespace(delete=lambda **payload: calls.append(payload)))
    runtime = module.VKModeratorRuntime(SimpleNamespace())
    runtime.vk = client
    runtime.settings = {"dry_run": "false"}
    runtime.last_mutating_action_at = 99.0
    sleeps = []
    monkeypatch.setattr(module.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(module.random, "uniform", lambda _low, _high: 1.75)
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    runtime._delete_chat_message(peer_id=2000000017, cmid=3)

    assert sleeps == [0.75]
    assert sleeps[0] <= 1.8
    assert calls == [{"delete_for_all": 1, "peer_id": 2000000017, "cmids": [3]}]
