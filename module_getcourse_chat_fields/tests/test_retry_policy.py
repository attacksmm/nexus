import asyncio
import json
import sqlite3
from pathlib import Path

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


def test_sheet_chat_is_never_replaced_by_created_link() -> None:
    sheet = {"stream_number": "54", "link": "https://vk.me/join/manual-250", "source": "flow_students_sheet"}
    created = {"stream_number": "54", "link": "https://vk.me/join/generated", "title": "54. Новый"}

    exact = module._prefer_sheet_chat(sheet, created, "54")
    empty = module._prefer_sheet_chat({**sheet, "link": ""}, created, "54")

    assert exact["link"] == "https://vk.me/join/manual-250"
    assert exact["source"] == "flow_students_sheet"
    assert empty == {**sheet, "link": ""}


def test_latest_chats_returns_only_complete_same_stream_pair(tmp_path: Path) -> None:
    db_path = tmp_path / "course-chat-creator.db"
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY, platform TEXT, title TEXT,
                stream_number TEXT, date_start TEXT, course_key TEXT,
                test_mode INTEGER, status TEXT, link TEXT, chat_id TEXT,
                created_at INTEGER
            )
            """
        )
        db.executemany(
            "INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "vk", "56 VK", "56", "22.07.2026", "puppy", 0, "ok", "https://vk.me/56", "1", 100),
                (2, "telegram", "56 TG", "56", "22.07.2026", "puppy", 0, "ok", "https://t.me/56", "", 101),
                (3, "vk", "1000 VK", "1000", "28.07.2026", "puppy", 0, "needs_members", "https://vk.me/1000", "2", 200),
            ],
        )

    original = module._course_chat_db_path
    module._course_chat_db_path = lambda: db_path
    try:
        result = asyncio.run(module._latest_chats())
    finally:
        module._course_chat_db_path = original

    assert result["puppy"]["vk"]["stream_number"] == "56"
    assert result["puppy"]["telegram"]["stream_number"] == "56"


def test_active_flow_requires_manual_vk_and_telegram_pair() -> None:
    result = module._active_chats_from_flows(
        {
            "items": [
                {
                    "course_key": "puppy",
                    "stream": "56",
                    "vk_title": "Щ56",
                    "vk_link": "https://vk.me/join/manual-250",
                    "tg_title": "Щ56",
                    "tg_link": "https://t.me/puppy56",
                },
                {
                    "course_key": "puppy",
                    "stream": "57",
                    "vk_title": "Щ57",
                    "vk_link": "https://vk.me/join/manual-250-new",
                    "tg_title": "Щ57",
                    "tg_link": "",
                },
                {
                    "course_key": "dog",
                    "stream": "54",
                    "vk_title": "С54",
                    "vk_link": "https://vk.me/join/dog54",
                    "tg_title": "С54",
                    "tg_link": "https://t.me/dog54",
                },
            ]
        }
    )
    assert result["puppy"]["vk"]["stream_number"] == "56"
    assert result["puppy"]["telegram"]["link"] == "https://t.me/puppy56"
    assert result["dog"]["vk"]["stream_number"] == "54"


def test_chat_entitlement_accepts_positive_partial_premium_and_combo() -> None:
    premium = module._chat_entitlement(
        {
            "title": "Первые шаги к воспитанию. Тариф Премиум",
            "payment_state": "partial",
            "payed_money": "13 300,00",
            "status": "Новый",
        }
    )
    combo = module._chat_entitlement(
        {
            "positions": "Первые шаги к воспитанию + Послушная собака 1/3",
            "offer_tags": "Щ+С",
            "payment_state": "partial",
            "payed_money": "13300",
        }
    )
    assert premium["eligible"] is True
    assert premium["course_key"] == "puppy"
    assert premium["payment"]["kind"] == "partial"
    assert combo["eligible"] is True
    assert combo["course_key"] == "puppy"
    assert combo["product_kind"] == "combo"


def test_chat_entitlement_rejects_standard_module_and_zero_partial() -> None:
    standard = module._chat_entitlement(
        {
            "title": "Послушная собака. Тариф Стандарт",
            "status": "Завершен",
            "payment_state": "paid",
        }
    )
    module_order = module._chat_entitlement(
        {
            "title": "Модуль курса Первые шаги к воспитанию. Тариф Премиум",
            "status": "Завершен",
            "payment_state": "paid",
        }
    )
    unpaid = module._chat_entitlement(
        {
            "title": "Послушная собака. Тариф VIP",
            "payment_state": "partial",
            "payed_money": "0",
        }
    )
    assert standard["eligible"] is False
    assert standard["reason"] == "standard_no_chat"
    assert module_order["eligible"] is False
    assert module_order["reason"] == "excluded_product:module"
    assert unpaid["eligible"] is False
    assert unpaid["reason"] == "payment_not_entitled"


def test_error_classification_and_exponential_backoff() -> None:
    settings = module.DEFAULT_SETTINGS.copy()
    assert module._gc_error_classification("Ошибка обновления заказа") == "terminal"
    assert module._gc_error_classification("HTTP 503") == "transient"
    assert module._gc_error_classification("лимит GetCourse API для модуля исчерпан") == "quota"
    assert module._gc_retry_delay_seconds(settings, 1, "transient") == 300
    assert module._gc_retry_delay_seconds(settings, 2, "transient") == 600
    assert module._gc_retry_delay_seconds(settings, 20, "transient") == 21600
    assert module._gc_retry_delay_seconds(settings, 1, "quota") == 900


def test_getcourse_json_error_is_not_treated_as_export() -> None:
    assert module._getcourse_response_error({"success": True, "error": False, "info": {"export_id": 1}}) == ""


def test_getcourse_export_resumes_existing_file(monkeypatch) -> None:
    module._gc_pending_exports.clear()
    starts = 0
    polls = 0

    async def get(path, params, settings, purpose):
        nonlocal starts, polls
        if purpose.endswith(":start"):
            starts += 1
            return True, {"info": {"export_id": "42"}}, ""
        polls += 1
        if polls == 1:
            return True, {"status": "Файл еще не создан"}, ""
        return True, {"info": {"items": [{"id": "1", "email": "x@y.ru"}]}}, ""

    monkeypatch.setattr(module, "_getcourse_export_get", get)
    settings = {"gc_export_lookup_poll_attempts": "1", "gc_export_lookup_poll_delay_seconds": "0"}
    first = asyncio.run(module._getcourse_export_rows("/pl/api/account/users", {"email": "x@y.ru"}, settings, "test"))
    second = asyncio.run(module._getcourse_export_rows("/pl/api/account/users", {"email": "x@y.ru"}, settings, "test"))
    assert first[0] == []
    assert second[0][0]["id"] == "1"
    assert starts == 1
    assert polls == 2
    assert module._getcourse_response_error({"success": False, "error": True}) == "GetCourse временно не принял запрос"
    assert module._getcourse_response_error({"success": False, "error_message": "API limit"}) == "API limit"


def test_getcourse_payload_includes_required_resolved_identity() -> None:
    fields = {"Поток": "53"}
    user_payload = module._getcourse_user_payload(
        "user-1", fields, email="duplicate@example.com", phone="79990000000"
    )
    deal_payload = module._getcourse_deal_payload(
        "user-1", "deal-1", fields, email="duplicate@example.com", phone="79990000000"
    )
    assert user_payload["user"] == {
        "id": "user-1",
        "email": "duplicate@example.com",
        "addfields": fields,
    }
    assert deal_payload["user"] == {"id": "user-1", "email": "duplicate@example.com"}


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


def test_registry_curator_reconciliation_queues_actual_mismatch_once(tmp_path) -> None:
    async def scenario() -> None:
        module._db_path = tmp_path / "getcourse-chat-fields.db"
        await module._init_db()
        with sqlite3.connect(module._db_path) as db:
            db.execute(
                """
                INSERT INTO processed_orders(
                    source_record_id,platform_id,order_id,gc_user_id,status,details_json
                ) VALUES(13682,'873857315','873857315','511441775','processed',?)
                """,
                (json.dumps({"output_fields": {"Номер куратора": "Куратор 1"}}, ensure_ascii=False),),
            )
            db.commit()
        flow = {
            "course_key": "puppy", "course": "Щенок", "stream": "57",
            "vk_link": "https://vk.example/57", "tg_link": "https://t.me/57",
            "students": [{
                "source_record_id": 13682, "order_id": "873857315", "deal_number": "873857315",
                "gc_user_id": "511441775", "email": "student@example.com", "teacher_code": "Куратор 3",
            }],
        }
        first = await module.service_reconcile_registry_curators(flows=[flow])
        second = await module.service_reconcile_registry_curators(flows=[flow])
        assert first["queued"] == 1
        assert second["queued"] == 0
        assert await module._fields_write_reconciliation_candidates(module.DEFAULT_SETTINGS.copy()) == []
        with sqlite3.connect(module._db_path) as db:
            payload = json.loads(db.execute("SELECT payload_json FROM gc_fields_write_jobs").fetchone()[0])
        assert payload["fields"]["Номер куратора"] == "Куратор 3"
        assert payload["flow"]["change_reason"] == "registry_curator_reconciliation"

    asyncio.run(scenario())
