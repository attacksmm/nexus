import asyncio
import json
import sqlite3

from module_amocrm_db import router as module


def _reset_runtime(tmp_path) -> None:
    module._db_path = str(tmp_path / "amocrm-db.db")
    module._amo_rate_lock = asyncio.Lock()
    module._amo_next_request_at = 0.0
    module._amo_state.update(
        requests=0,
        rate_limit_responses=0,
        retries=0,
        last_429_at="",
        last_error="",
    )


def test_init_requeues_only_unrecovered_legacy_429(tmp_path) -> None:
    db_path = tmp_path / "amocrm-db.db"
    with sqlite3.connect(db_path) as db:
        db.executescript(
            """
            CREATE TABLE settings (key TEXT PRIMARY KEY,value TEXT NOT NULL DEFAULT '');
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                action TEXT NOT NULL DEFAULT '',
                deal_id TEXT NOT NULL DEFAULT '',
                pipeline_id TEXT NOT NULL DEFAULT '',
                status_id TEXT NOT NULL DEFAULT '',
                old_status_id TEXT NOT NULL DEFAULT '',
                responsible_user_id TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                ignored INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                raw_payload TEXT NOT NULL DEFAULT ''
            );
            INSERT INTO events(received_at,deal_id,error) VALUES
                ('2026-07-18T08:00:00Z','needs-replay','amoCRM HTTP 429: limited'),
                ('2026-07-18T08:00:00Z','already-recovered','amoCRM HTTP 429: limited'),
                ('2026-07-18T08:00:00Z','permanent','amoCRM HTTP 404: missing'),
                ('2026-07-01T08:00:00Z','old-rate-limit','amoCRM HTTP 429: limited');
            INSERT INTO events(received_at,deal_id,success) VALUES
                ('2026-07-18T08:01:00Z','already-recovered',1);
            """
        )
    _reset_runtime(tmp_path)
    asyncio.run(module._init_db())
    with sqlite3.connect(db_path) as db:
        rows = {
            deal_id: status
            for deal_id, status in db.execute(
                "SELECT deal_id,queue_status FROM events WHERE error<>''"
            )
        }
        columns = {row[1] for row in db.execute("PRAGMA table_info(events)")}
    assert {"queue_status", "attempts", "next_attempt_at", "updated_at"} <= columns
    assert rows == {
        "needs-replay": "retry",
        "already-recovered": "completed",
        "permanent": "completed",
        "old-rate-limit": "completed",
    }


def test_enqueue_coalesces_and_claims_latest_event(tmp_path) -> None:
    async def scenario() -> None:
        _reset_runtime(tmp_path)
        await module._init_db()
        settings = module.DEFAULT_SETTINGS.copy()
        settings["debounce_seconds"] = "0"
        first = await module._store_event({"action": "update", "deal_id": "42"})
        await module._enqueue_event(first, "42", settings)
        second = await module._store_event({"action": "status", "deal_id": "42"})
        await module._enqueue_event(second, "42", settings)
        claimed = await module._claim_event()
        assert claimed is not None
        assert claimed["id"] == second
        with sqlite3.connect(module._db_path) as db:
            older = db.execute(
                "SELECT queue_status,ignored,error FROM events WHERE id=?", (first,)
            ).fetchone()
        assert older == ("ignored", 1, "superseded by newer webhook")

    asyncio.run(scenario())


def test_amo_get_retries_429_and_honors_retry_after(tmp_path, monkeypatch) -> None:
    class Response:
        def __init__(self, status_code, body, headers=None):
            self.status_code = status_code
            self.text = body
            self.headers = headers or {}

        def json(self):
            return {"id": 42}

    responses = [Response(429, "limited", {"Retry-After": "0"}), Response(200, "{}")]

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            return responses.pop(0)

    async def scenario() -> None:
        _reset_runtime(tmp_path)
        monkeypatch.setattr(module, "_env", lambda: {
            "amo_base_url": "https://amo.invalid",
            "amo_token": "test-token",
            "webhook_secret": "",
            "customer_db_path": "",
        })
        monkeypatch.setattr(module.httpx, "AsyncClient", Client)
        settings = module.DEFAULT_SETTINGS.copy()
        settings["amo_min_interval_seconds"] = "0.1"
        body, error = await module._amo_get("/api/v4/leads/42", settings)
        assert error == ""
        assert body == {"id": 42}
        assert module._amo_state["rate_limit_responses"] == 1
        assert module._amo_state["retries"] == 1
        assert module._amo_state["requests"] == 2

    asyncio.run(scenario())


def test_concurrent_amo_get_calls_are_spaced(tmp_path, monkeypatch) -> None:
    call_times = []

    class Response:
        status_code = 200
        text = "{}"
        headers = {}

        def json(self):
            return {}

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args, **_kwargs):
            call_times.append(asyncio.get_running_loop().time())
            return Response()

    async def scenario() -> None:
        _reset_runtime(tmp_path)
        monkeypatch.setattr(module, "_env", lambda: {
            "amo_base_url": "https://amo.invalid",
            "amo_token": "test-token",
            "webhook_secret": "",
            "customer_db_path": "",
        })
        monkeypatch.setattr(module.httpx, "AsyncClient", Client)
        settings = module.DEFAULT_SETTINGS.copy()
        settings["amo_min_interval_seconds"] = "0.1"
        await asyncio.gather(*(module._amo_get(f"/api/v4/leads/{idx}", settings) for idx in range(5)))
        gaps = [right - left for left, right in zip(call_times, call_times[1:])]
        assert len(call_times) == 5
        assert all(gap >= 0.09 for gap in gaps)

    asyncio.run(scenario())


def test_transient_queue_failure_is_durable_until_success(tmp_path, monkeypatch) -> None:
    outcomes = [
        {"ok": False, "error": "amoCRM HTTP 429: limited"},
        {"ok": True, "storage": {"action": "updated"}, "record": {"status_name": "Новый"}},
    ]

    async def fake_sync(*_args, **_kwargs):
        return outcomes.pop(0)

    async def scenario() -> None:
        _reset_runtime(tmp_path)
        await module._init_db()
        monkeypatch.setattr(module, "_sync_deal", fake_sync)
        event_id = await module._store_event({"action": "update", "deal_id": "42"})
        settings = module.DEFAULT_SETTINGS.copy()
        settings["debounce_seconds"] = "0"
        await module._enqueue_event(event_id, "42", settings)
        first = await module._claim_event()
        assert first is not None
        await module._process_queue_event(first)
        with sqlite3.connect(module._db_path) as db:
            row = db.execute(
                "SELECT queue_status,attempts,error FROM events WHERE id=?", (event_id,)
            ).fetchone()
            db.execute("UPDATE events SET next_attempt_at='' WHERE id=?", (event_id,))
            db.commit()
        assert row == ("retry", 1, "amoCRM HTTP 429: limited")
        second = await module._claim_event()
        assert second is not None
        await module._process_queue_event(second)
        with sqlite3.connect(module._db_path) as db:
            row = db.execute(
                "SELECT queue_status,attempts,success,error FROM events WHERE id=?", (event_id,)
            ).fetchone()
        assert row == ("completed", 2, 1, "")

    asyncio.run(scenario())


def test_transient_error_classification_and_queue_backoff() -> None:
    assert module._is_transient_amo_error("amoCRM HTTP 429: limited")
    assert module._is_transient_amo_error("amoCRM HTTP 503: unavailable")
    assert module._is_transient_amo_error("amoCRM transport ReadTimeout")
    assert not module._is_transient_amo_error("amoCRM HTTP 404: missing")
    assert module._queue_retry_delay(1) == 30
    assert module._queue_retry_delay(2) == 60
    assert module._queue_retry_delay(99) == 3600


def test_successful_manager_lookup_uses_only_won_deals(tmp_path, monkeypatch) -> None:
    customer_db = tmp_path / "customer-db.db"
    with sqlite3.connect(customer_db) as db:
        db.execute(
            """
            CREATE TABLE cdb_amo_deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id TEXT NOT NULL DEFAULT '',
                custom_fields TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        db.executemany(
            "INSERT INTO cdb_amo_deals(platform_id,custom_fields,updated_at) VALUES(?,?,?)",
            [
                (
                    "won-1",
                    json.dumps({
                        "status_id": "142",
                        "responsible_user_id": "42",
                        "emails": ["student@example.com"],
                        "phones": ["+7 999 111-22-33"],
                        "deal_url": "https://amo.invalid/leads/detail/won-1",
                    }),
                    "2026-08-01T10:00:00Z",
                ),
                (
                    "lost-newer",
                    json.dumps({
                        "status_id": "143",
                        "responsible_user_id": "99",
                        "emails": ["student@example.com"],
                    }),
                    "2026-08-02T10:00:00Z",
                ),
                (
                    "won-history",
                    json.dumps({
                        "status_name": "Успешно реализовано",
                        "responsible_user_name": "Каракчиев Андрей Владимирович",
                        "emails": ["history@example.com"],
                        "deal_url": "https://amo.invalid/leads/detail/won-history",
                    }),
                    "2026-08-03T10:00:00Z",
                ),
            ],
        )

    async def user_names():
        return {"42": "Татьяна Воробьева", "99": "Не брать"}

    monkeypatch.setattr(module, "_customer_db_path", lambda: customer_db)
    monkeypatch.setattr(module, "_amo_user_names", user_names)
    _reset_runtime(tmp_path)
    asyncio.run(module._init_db())
    assert module._rebuild_successful_manager_index_sync(module._db_path, customer_db) == 3
    result = asyncio.run(module.service_successful_managers(identities=[
        {"key": "student-1", "email": "student@example.com", "phone": ""},
        {"key": "student-2", "email": "history@example.com", "phone": ""},
    ]))
    assert result == {
        "ok": True,
        "items": [{
            "key": "student-1",
            "deal_id": "won-1",
            "deal_url": "https://amo.invalid/leads/detail/won-1",
            "manager_id": "42",
            "manager_name": "Татьяна Воробьева",
        }, {
            "key": "student-2",
            "deal_id": "won-history",
            "deal_url": "https://amo.invalid/leads/detail/won-history",
            "manager_id": "",
            "manager_name": "Каракчиев Андрей Владимирович",
        }],
    }


def test_manager_name_removes_technical_auto_suffix() -> None:
    assert module._manager_name("Татьяна Воробьева (Авто)") == "Татьяна Воробьева"
    assert module._manager_name("Татьяна Воробьева(Auto)") == "Татьяна Воробьева"


def test_manager_index_updates_when_deal_status_changes(tmp_path) -> None:
    _reset_runtime(tmp_path)
    asyncio.run(module._init_db())
    won = {
        "status_id": "142",
        "responsible_user_id": "42",
        "emails": ["student@example.com"],
        "deal_url": "https://amo.invalid/leads/detail/10",
        "updated_at": "2026-08-03T10:00:00Z",
    }
    asyncio.run(module._refresh_successful_manager_deal("10", won))
    with sqlite3.connect(module._db_path) as db:
        assert db.execute("SELECT identity,deal_id FROM successful_manager_identities").fetchall() == [
            ("email:student@example.com", "10")
        ]
    asyncio.run(module._refresh_successful_manager_deal("10", {**won, "status_id": "143"}))
    with sqlite3.connect(module._db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM successful_manager_identities").fetchone()[0] == 0
