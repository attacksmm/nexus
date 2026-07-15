import asyncio
import json
import sqlite3

from module_getcourse_chat_fields import router as module


def _candidate(*, stream: str = "53", email: str = "dog@example.com", order_id: str = "order-1"):
    fields = {
        "Поток": stream,
        "Ссылка на чат ВК": "https://vk.example/chat",
        "Ссылка на чат ТГ": "https://t.me/example",
        "Номер куратора": "Куратор 1",
    }
    return {
        "email": email,
        "gc_user_id": "user-1",
        "order_id": order_id,
        "deal_number": "deal-1",
        "fields": fields,
        "user_fields": fields,
        "flow": {"stream": stream, "change_reason": "field_write_reconciliation"},
    }


def test_error_classification_and_exponential_backoff() -> None:
    settings = module.DEFAULT_SETTINGS.copy()
    assert module._gc_error_classification("Ошибка обновления заказа") == "terminal"
    assert module._gc_error_classification("HTTP 503") == "transient"
    assert module._gc_error_classification("лимит GetCourse API для модуля исчерпан") == "quota"
    assert module._gc_retry_delay_seconds(settings, 1, "transient") == 300
    assert module._gc_retry_delay_seconds(settings, 2, "transient") == 600
    assert module._gc_retry_delay_seconds(settings, 20, "transient") == 21600
    assert module._gc_retry_delay_seconds(settings, 1, "quota") == 900


def test_processed_state_waits_for_source_change_or_retry_time() -> None:
    settings = module.DEFAULT_SETTINGS.copy()
    quarantined = {
        "source_hash": "same",
        "status": "quarantined",
        "error": "course not detected",
        "details_json": "{}",
    }
    assert module._should_skip_state(quarantined, "same", settings, gc_ready=True)
    assert not module._should_skip_state(quarantined, "changed", settings, gc_ready=True)

    deferred = {
        "source_hash": "same",
        "status": "customer_only",
        "error": "лимит GetCourse API для модуля исчерпан",
        "details_json": json.dumps(
            {"retry": {"classification": "quota", "next_retry_at": "2099-01-01T00:00:00Z"}}
        ),
    }
    assert module._should_skip_state(deferred, "same", settings, gc_ready=True)
    deferred["details_json"] = json.dumps(
        {"retry": {"classification": "quota", "next_retry_at": "2000-01-01T00:00:00Z"}}
    )
    assert not module._should_skip_state(deferred, "same", settings, gc_ready=True)


def test_exhausted_payload_is_not_requeued_until_payload_changes(tmp_path) -> None:
    async def scenario() -> None:
        module._db_path = tmp_path / "getcourse-chat-fields.db"
        await module._init_db()
        first = await module._enqueue_gc_fields_write_items([_candidate()], force=True)
        assert first["queued"] == 1

        with sqlite3.connect(module._db_path) as db:
            db.execute(
                "UPDATE gc_fields_write_jobs SET status='failed_exhausted', attempts=3, last_error='HTTP 503'"
            )
            db.commit()

        same = await module._enqueue_gc_fields_write_items([_candidate()], force=True)
        assert same["queued"] == 0
        with sqlite3.connect(module._db_path) as db:
            row = db.execute("SELECT status,attempts FROM gc_fields_write_jobs").fetchone()
        assert row == ("failed_exhausted", 3)

        changed = await module._enqueue_gc_fields_write_items([_candidate(stream="54")], force=True)
        assert changed["queued"] == 1
        with sqlite3.connect(module._db_path) as db:
            row = db.execute("SELECT status,attempts,payload_json FROM gc_fields_write_jobs").fetchone()
        assert row[0:2] == ("pending", 0)
        assert json.loads(row[2])["fields"]["Поток"] == "54"

    asyncio.run(scenario())


def test_init_quarantines_known_deterministic_deal_failure(tmp_path) -> None:
    async def scenario() -> None:
        module._db_path = tmp_path / "getcourse-chat-fields.db"
        await module._init_db()
        await module._enqueue_gc_fields_write_items([_candidate()], force=True)
        with sqlite3.connect(module._db_path) as db:
            db.execute(
                """
                UPDATE gc_fields_write_jobs
                SET status='failed_exhausted', attempts=3,
                    last_error='deal: Ошибка обновления заказа'
                """
            )
            db.commit()
        await module._init_db()
        with sqlite3.connect(module._db_path) as db:
            row = db.execute("SELECT status,attempts,last_error FROM gc_fields_write_jobs").fetchone()
        assert row == ("quarantined", 3, "deal: Ошибка обновления заказа")

    asyncio.run(scenario())


def test_new_write_job_is_claimed_before_retry(tmp_path) -> None:
    async def scenario() -> None:
        module._db_path = tmp_path / "getcourse-chat-fields.db"
        await module._init_db()
        await module._enqueue_gc_fields_write_items(
            [
                _candidate(email="retry@example.com", order_id="retry"),
                _candidate(email="new@example.com", order_id="new"),
            ],
            force=True,
        )
        with sqlite3.connect(module._db_path) as db:
            db.execute(
                "UPDATE gc_fields_write_jobs SET status='failed', attempts=1 WHERE order_id='retry'"
            )
            db.commit()
        claimed = await module._claim_gc_fields_write_job(module.DEFAULT_SETTINGS.copy())
        assert claimed is not None
        assert claimed["order_id"] == "new"

    asyncio.run(scenario())
