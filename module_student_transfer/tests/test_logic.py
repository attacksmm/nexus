import asyncio
import json
from pathlib import Path

import aiosqlite
from starlette.requests import Request

from module_student_transfer import router as module


def test_default_operators_match_staff_profiles():
    assert "Андрей Карачкиев" in module.DEFAULT_OPERATORS
    assert "Татьяна Истратова" in module.DEFAULT_OPERATORS
    assert "Андрей Каракчиев" not in module.DEFAULT_OPERATORS


class IdentityService:
    async def service_transfer_recipients(self, **kwargs):
        return {"ok": True, "status": "resolved", "telegram": "123", "vk": "456", "conflicts": []}


class ChatService:
    def service_transfer_chat_readiness(self, course_key, stream):
        return {
            "vk": {"status": "legacy_inaccessible", "manageable": False},
            "telegram": {"status": "ready", "manageable": True},
        }


def snapshot():
    student = {
        "row": 8,
        "name": "Иван",
        "email": "ivan@example.com",
        "gc_user_id": "100",
        "order_id": "200",
        "deal_number": "D-200",
    }
    return {
        "ok": True,
        "items": [
            {
                "course_key": "puppy",
                "course": "Щенок",
                "stream": "55",
                "sheet_title": "Щ55 (10.07)",
                "curator_value": "Куратор 1",
                "students": [student],
            },
            {
                "course_key": "puppy",
                "course": "Щенок",
                "stream": "56",
                "curator_value": "Куратор 2",
                "vk_link": "https://vk.example/56",
                "tg_link": "https://t.me/56",
                "students": [],
            },
        ],
    }


def test_preview_maps_curator_offer_and_warns_for_legacy_vk(monkeypatch):
    async def fake_snapshot(refresh=False):
        return snapshot()

    monkeypatch.setattr(module, "_snapshot", fake_snapshot)
    monkeypatch.setattr(
        module,
        "_module",
        lambda module_id, service: IdentityService() if module_id == "messenger-widget" else ChatService(),
    )
    result = asyncio.run(
        module._preview(
            module.TransferRef(
                email="ivan@example.com",
                source_course_key="puppy",
                source_stream="55",
                source_row=8,
                target_course_key="puppy",
                target_stream="56",
            )
        )
    )
    assert result["can_transfer"] is True
    assert result["target"]["offer_id"] == 8593081
    assert result["sheet"] == {"found": True, "title": "Щ55 (10.07)", "row": 8, "move": True}
    assert any("VK" in warning for warning in result["warnings"])


def test_preview_uses_table_only_repair_when_getcourse_already_matches(monkeypatch):
    current = snapshot()
    current["items"][0]["students"][0]["source_record_id"] = 13659
    current["items"][0]["students"][0]["enrollment_id"] = "order:13659"
    current["items"].append({
        "course_key": "dog", "course": "Собака", "stream": "54", "curator_value": "Куратор 3",
        "vk_link": "https://vk.example/54", "tg_link": "https://t.me/54", "students": [],
    })

    class Fields:
        async def service_order_identities(self, **_kwargs):
            return {"ok": True, "items": [{
                "key": "", "assignment": {},
            }, {
                "key": "order:13659",
                "assignment": {
                    "course_key": "dog", "stream": "54", "curator": "Куратор 3",
                    "vk_link": "https://vk.example/54", "tg_link": "https://t.me/54",
                },
            }]}

    async def fake_snapshot(refresh=False):
        return current

    def service(module_id, _name):
        if module_id == "getcourse-chat-fields":
            return Fields()
        raise AssertionError(f"unexpected service: {module_id}")

    monkeypatch.setattr(module, "_snapshot", fake_snapshot)
    monkeypatch.setattr(module, "_module", service)
    result = asyncio.run(module._preview(module.TransferRef(
        enrollment_id="order:13659", email="ivan@example.com", source_course_key="puppy", source_stream="55",
        source_row=8, target_course_key="dog", target_stream="54",
    )))
    assert result["action"] == "registry_repair"
    assert result["can_transfer"] is True
    assert result["chat_readiness"] == {}
    assert result["warnings"] == ["GetCourse и чаты не изменяются"]


def test_table_only_repair_skips_getcourse_messages_and_chats(tmp_path, monkeypatch):
    calls = []

    class Fields:
        async def service_transfer_move_student(self, **kwargs):
            calls.append(("sheet", kwargs["target_course_key"], kwargs["target_stream"]))
            return {"ok": True, "status": "moved"}

    async def commit(transfer, **_kwargs):
        calls.append(("registry", transfer["target_course_key"], transfer["target_stream"]))
        return {"ok": True, "status": "mirrored"}

    monkeypatch.setattr(module, "_module", lambda module_id, _service: Fields())
    monkeypatch.setattr(module, "_commit_registry_transfer", commit)
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """
                INSERT INTO transfers(
                    id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                    target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                    student_json,steps_json,error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "repair1", "queued", "ivan@example.com", "100", "Иван", "puppy", "57", 8,
                    "dog", "54", "Куратор 3", 8593084, 1, "Оператор", "{}",
                    json.dumps({"preview": {"action": "registry_repair"}}), "", now, now,
                ),
            )
            await db.commit()
        await module._run_transfer("repair1")
        async with aiosqlite.connect(module._must_db()) as db:
            return await (await db.execute("SELECT status,steps_json FROM transfers WHERE id='repair1'")).fetchone()

    row = asyncio.run(scenario())
    assert row[0] == "completed"
    assert calls == [("sheet", "dog", "54"), ("registry", "dog", "54")]
    assert set(json.loads(row[1])) == {"preview", "sheet", "registry"}


def test_transfer_does_not_send_links_and_still_removes_old_chat(tmp_path, monkeypatch):
    class Fields:
        async def service_transfer_move_student(self, **kwargs):
            return {"ok": True, "status": "moved"}

        async def service_transfer_write_getcourse(self, **kwargs):
            return {
                "ok": True,
                "target": {"course": "Щенок", "tg_link": "https://t.me/56", "vk_link": "https://vk/56"},
            }

        async def service_create_curator_order(self, **kwargs):
            return {"ok": True, "status": "created"}

    class Removal:
        called = 0

        async def service_remove_transfer_member(self, **kwargs):
            self.called += 1
            return {"ok": True}

    removal = Removal()
    services = {
        "getcourse-chat-fields": Fields(),
        "messenger-widget": IdentityService(),
        "course-chat-creator": removal,
    }
    monkeypatch.setattr(module, "_module", lambda module_id, service: services[module_id])
    async def commit(*_args, **_kwargs):
        return {"ok": True, "status": "updated"}
    monkeypatch.setattr(module, "_commit_registry_transfer", commit)
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """
                INSERT INTO transfers(
                    id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                    target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                    student_json,steps_json,error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "op1", "queued", "ivan@example.com", "100", "Иван", "puppy", "55", 8,
                    "puppy", "56", "Куратор 2", 8593081, 1, "Оператор",
                    json.dumps({"order_id": "200", "deal_number": "D-200"}),
                    json.dumps({"preview": {"chat_readiness": {
                        "vk": {"manageable": True}, "telegram": {"manageable": True},
                    }}}), "",
                    module._now(), module._now(),
                ),
            )
            await db.commit()
        await module._run_transfer("op1")
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute("SELECT status,error FROM transfers WHERE id='op1'")).fetchone()
        return row

    row = asyncio.run(scenario())
    assert row == ("completed", "")
    assert removal.called == 2


def test_google_429_keeps_transfer_in_retry_queue(tmp_path, monkeypatch):
    class Fields:
        async def service_transfer_write_getcourse(self, **_kwargs):
            raise RuntimeError("429 Client Error: Too Many Requests")

    monkeypatch.setattr(module, "_module", lambda *_args: Fields())
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """INSERT INTO transfers(
                    id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                    target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                    student_json,steps_json,error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ("retry-429", "queued", "student@example.com", "1", "Иван", "puppy", "35", 8,
                 "dog", "55", "Куратор 1", 8593080, 1, "Оператор", "{}", "{}", "", now, now),
            )
            await db.commit()
        await module._run_transfer("retry-429")
        async with aiosqlite.connect(module._must_db()) as db:
            return await (await db.execute(
                "SELECT status,error,steps_json FROM transfers WHERE id='retry-429'"
            )).fetchone()

    status, error, raw_steps = asyncio.run(scenario())
    assert status == "waiting"
    assert "поставлен в очередь" in error
    assert json.loads(raw_steps)["retry"]["attempts"] == 1


def test_same_person_is_not_globally_locked():
    schema = module._init_db.__code__.co_consts
    assert not any("UNIQUE(email" in value for value in schema if isinstance(value, str))


def test_pending_access_uses_expected_groups_and_bounded_retry():
    current = {
        "ok": True,
        "source": "cache",
        "items": [
            {"group_id": "1", "enabled": True},
            {"group_id": "2", "enabled": True},
            {"group_id": "3", "enabled": False},
        ],
        "current_groups": [{"group_id": "1"}, {"group_id": "2"}],
    }
    result = module._access_target_view(current, [{"group_id": "1"}, {"group_id": "3"}], "2026-08-03T18:00:00Z")
    assert [item["enabled"] for item in result["items"]] == [True, False, True]
    assert result["source"] == "pending"
    assert result["pending"] is True
    assert module._retry_delay(0) == 120
    assert module._retry_delay(99) == 3600


def test_duplicate_active_transfer_reuses_operation(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())
    preview = {
        "action": "registry_repair",
        "source": {
            "enrollment_id": "order:1", "email": "ivan@example.com", "gc_user_id": "100",
            "name": "Иван", "course_key": "puppy", "stream": "57", "row": 8,
        },
        "target": {"course_key": "dog", "stream": "54", "curator": "Куратор 3", "offer_id": 8593084},
    }
    operator = {"id": 1, "display_name": "Оператор", "login": "operator"}

    async def scenario():
        first = await module._queue_operation(preview, operator)
        second = await module._queue_operation(preview, operator)
        async with aiosqlite.connect(module._must_db()) as db:
            count = (await (await db.execute("SELECT COUNT(*) FROM transfers")).fetchone())[0]
        return first, second, count

    first, second, count = asyncio.run(scenario())
    assert first["id"] == second["id"]
    assert second["existing"] is True
    assert count == 1


def test_registry_transfer_updates_target_row_without_full_sync(tmp_path, monkeypatch):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def forbidden_sync(*_args, **_kwargs):
        raise AssertionError("full sync must not run")

    monkeypatch.setattr(module, "_sync_registry", forbidden_sync)

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                "INSERT INTO flow_registry(course_key,stream,course,teacher,teacher_code,status,updated_at) VALUES(?,?,?,?,?,'ready',?)",
                ("dog", "54", "Собака", "Настасья", "Куратор 3", now),
            )
            await db.execute(
                """
                INSERT INTO enrollments(id,email,course_key,course,stream,status,source_json,created_at,updated_at)
                VALUES('order:1','ivan@example.com','puppy','Щенок','57','assigned','{"row":8}',?,?)
                """,
                (now, now),
            )
            await db.executemany(
                """
                INSERT INTO enrollments(id,email,course_key,course,stream,status,source_json,created_at,updated_at)
                VALUES(?,?,?,?,?,'assigned',?,?,?)
                """,
                [
                    ("before", "before@example.com", "puppy", "Щенок", "57", '{"row":7}', now, now),
                    ("after", "after@example.com", "puppy", "Щенок", "57", '{"row":13}', now, now),
                ],
            )
            await db.commit()
        result = await module._commit_registry_transfer({
            "enrollment_id": "order:1", "source_course_key": "puppy", "source_stream": "57", "source_row": 8,
            "target_course_key": "dog", "target_stream": "54",
        }, target_row=12, source_row_deleted=True)
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute(
                "SELECT course_key,stream,teacher_code,source_json FROM enrollments WHERE id='order:1'"
            )).fetchone()
            shifted = await (await db.execute(
                "SELECT id,source_json FROM enrollments WHERE id IN ('before','after') ORDER BY id"
            )).fetchall()
        return result, row, shifted

    result, row, shifted = asyncio.run(scenario())
    assert result == {"ok": True, "status": "updated", "row": 12}
    assert row[:3] == ("dog", "54", "Куратор 3")
    assert json.loads(row[3])["row"] == 12
    assert [(item[0], json.loads(item[1])["row"]) for item in shifted] == [("after", 12), ("before", 7)]


def test_standalone_cookie_uses_public_streams_path():
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/student-transfer/api/login",
            "root_path": "/nexus",
            "headers": [(b"x-forwarded-prefix", b"/streams")],
        }
    )
    assert module._cookie_path(request) == "/streams"


def test_student_row_curator_overrides_flow_curator():
    flow = {"course_key": "puppy", "course": "Щенок", "stream": "55", "curator_value": "Куратор 1"}
    student = {"row": 8, "email": "ivan@example.com", "responsible_curator": "Куратор 3"}
    result = module._student_result(flow, student)
    assert result["curator"] == "Куратор 3"
    assert result["curator_name"] == "Настасья"


def test_student_result_keeps_tariff_and_sheet_column_order():
    flow = {"course_key": "puppy", "course": "Щенок", "stream": "57"}
    student = {
        "email": "student@example.com",
        "tariff": "Премиум",
        "lessons": [
            {"key": "AA", "label": "Отзыв", "value": False},
            {"key": "H", "label": "Доб. в купивших", "value": True},
            {"key": "Z", "label": "8.0", "value": False},
        ],
    }
    result = module._student_result(flow, student)
    assert result["tariff"] == "Премиум"
    assert [lesson["key"] for lesson in result["lessons"]] == ["H", "Z", "AA"]


def test_students_filters_tariff_and_returns_flow_summary(monkeypatch):
    async def allow(_request):
        return {"id": 1}

    async def current_snapshot(refresh=False):
        return {
            "updated_at": "now",
            "items": [{
                "course_key": "puppy",
                "course": "Щенок",
                "stream": "51",
                "students": [
                    {"email": "standard@example.com", "tariff": "Стандарт", "responsible_curator": "Куратор 1"},
                    {"email": "premium@example.com", "tariff": "Premium", "responsible_curator": "Куратор 2"},
                    {"email": "vip@example.com", "tariff": "ВИП", "responsible_curator": "Куратор 2"},
                ],
            }],
        }

    async def no_enrichment(_items):
        return None

    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_snapshot", current_snapshot)
    monkeypatch.setattr(module, "_enrich_order_identities", no_enrichment)
    monkeypatch.setattr(module, "_enrich_successful_managers", no_enrichment)
    result = asyncio.run(module.students(Request({"type": "http"}), course_key="puppy", stream="51", tariff="premium"))
    assert result["total"] == 1
    assert [item["email"] for item in result["items"]] == ["premium@example.com"]
    assert result["summary"]["tariffs"] == {"standard": 1, "premium": 1, "vip": 1, "other": 0}
    assert next(item for item in result["summary"]["vip_by_curator"] if item["value"] == "Куратор 2")["count"] == 1


def test_fullscreen_panel_exposes_mobile_filters_themes_counts_and_tariff_colours():
    panel = (Path(__file__).resolve().parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    for marker in ('id="tariffFilter"', 'id="curatorFilter"', 'id="summaryBtn"', 'id="themeBtn"'):
        assert marker in panel
    assert panel.count('class="theme-toggle"') == 1
    assert "querySelectorAll('[data-theme]')" not in panel
    assert ".btn.mobile-menu{display:none}" in panel
    assert ".table tr.tariff-standard td{background:var(--standardRow)}" in panel
    assert "premiumRow" not in panel
    assert ".table tr.tariff-vip td{background:var(--vipRow)}" in panel
    assert ".toolbar .hide-mobile{display:block}" in panel


def test_panel_queues_latest_filter_change_while_students_are_loading():
    panel = (Path(__file__).resolve().parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")

    assert "if(state.loading){state.loadQueued=true" in panel
    assert "if(state.loadQueued){const queuedRefresh=state.loadQueuedRefresh" in panel


def test_manager_enrichment_is_read_only_and_optional(monkeypatch):
    class Amo:
        async def service_successful_managers(self, **kwargs):
            assert kwargs["identities"][0]["email"] == "student@example.com"
            return {
                "ok": True,
                "items": [{
                    "key": "student-1",
                    "manager_name": "Татьяна Воробьева",
                    "manager_id": "42",
                    "deal_id": "873",
                    "deal_url": "https://amo.invalid/leads/detail/873",
                }],
            }

    monkeypatch.setattr(module, "_module", lambda module_id, service: Amo())
    items = [{"enrollment_id": "student-1", "email": "student@example.com", "phone": ""}]
    asyncio.run(module._enrich_successful_managers(items))
    assert items[0]["manager_name"] == "Татьяна Воробьева"
    assert items[0]["amo_deal_id"] == "873"


def test_order_identity_enrichment_adds_phone_before_amo_lookup(monkeypatch):
    class Fields:
        async def service_order_identities(self, **kwargs):
            assert kwargs["identities"][0]["gc_user_id"] == "511441775"
            return {"ok": True, "items": [{"key": "student-1", "phone": "+7 999 111-22-33", "tariff": "Premium"}]}

    monkeypatch.setattr(module, "_module", lambda module_id, service: Fields())
    items = [{
        "enrollment_id": "student-1",
        "email": "student@example.com",
        "gc_user_id": "511441775",
        "source_record_id": 0,
        "order_id": "",
        "phone": "",
        "tariff": "",
    }]
    asyncio.run(module._enrich_order_identities(items))
    assert items[0]["phone"] == "+7 999 111-22-33"
    assert items[0]["tariff"] == "Premium"


def test_access_view_separates_getcourse_groups_from_sheet_progress():
    catalog = [
        {"group_id": 4059685, "name": "Знакомство. Щенок", "course_key": "puppy", "group_kind": "root", "managed": True},
        {"group_id": 4306384, "name": "Выдача Щенка без процесса", "course_key": "puppy", "group_kind": "bridge", "managed": True},
        {"group_id": 4059687, "name": "1 модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": 1, "managed": True},
    ]
    result = module._access_view(
        {"ok": True, "source": "live", "groups": [{"group_id": "4059687", "name": "1 модуль. Щенок"}]},
        catalog,
    )
    assert [item["group_id"] for item in result["items"]] == ["4059685", "4059687"]
    assert result["items"][1]["enabled"] is True


def test_missing_access_snapshot_refreshes_and_exposes_module_nine(monkeypatch):
    class Fields:
        calls = []

        async def service_getcourse_access_snapshot(self, **kwargs):
            self.calls.append(bool(kwargs.get("live")))
            if not kwargs.get("live"):
                return {"ok": False, "refresh_due": True, "groups": [], "error": "missing"}
            return {
                "ok": True,
                "source": "live",
                "groups": [{"group_id": "4059705", "name": "9 модуль (бонусный). Щенок"}],
            }

    class Access:
        def service_access_catalog(self):
            return {
                "ok": True,
                "items": [
                    {
                        "group_id": 4059705,
                        "name": "9 модуль (бонусный). Щенок",
                        "course_key": "puppy",
                        "group_kind": "module",
                        "module_index": 9,
                        "managed": True,
                    }
                ],
            }

    fields, access = Fields(), Access()
    monkeypatch.setattr(
        module,
        "_module",
        lambda module_id, service: fields if module_id == "getcourse-chat-fields" else access,
    )
    result = asyncio.run(module._get_access_view({"gc_user_id": "511", "email": "student@example.com"}, live=False))
    assert fields.calls == [False, True]
    assert result["items"][0]["module_index"] == 9
    assert result["items"][0]["enabled"] is True


def test_only_complete_link_pair_receives_new_clients(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        flows = await module._upsert_flows(
            {
                "items": [
                    {"course_key": "puppy", "course": "Щенок", "stream": "55", "date_start": "2026-07-10", "vk_link": "https://vk/55", "tg_link": "https://t.me/55"},
                    {"course_key": "puppy", "course": "Щенок", "stream": "56", "date_start": "2026-07-22", "vk_link": "", "tg_link": "https://t.me/56"},
                ]
            },
            {
                "items": [
                    {"course_key": "puppy", "stream": "55", "teacher": "Ирина", "teacher_id": 1, "offer_id": 8593080},
                    {"course_key": "puppy", "stream": "56", "teacher": "Слава", "teacher_id": 2, "offer_id": 8593081},
                ]
            },
        )
        await module._assign_new_orders(
            {"items": [{"source_record_id": 901, "order_id": "901", "email": "new@example.com", "date": "2026-07-20T12:00:00Z", "course_key": "puppy", "course": "Щенок"}]},
            flows,
        )
        async with aiosqlite.connect(module._must_db()) as db:
            flow_rows = await (await db.execute("SELECT stream,status FROM flow_registry ORDER BY stream")).fetchall()
            student = await (await db.execute("SELECT stream,status FROM enrollments WHERE source_record_id=901")).fetchone()
        return flow_rows, student

    flows, student = asyncio.run(scenario())
    assert flows == [("55", "ready"), ("56", "draft")]
    assert student == ("55", "assigned")


def test_sheet_reconciliation_does_not_move_combined_order_across_courses(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                "INSERT INTO enrollments(id,source_record_id,order_id,email,course_key,course,stream,tariff,status,source_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                ("order:9956", 9956, "708569132", "artimida444@yandex.ru", "puppy", "Щенок", "18", "Премиум", "assigned", json.dumps({"row": 50, "order_id": "708569132", "name": "Анастасия Леус"}, ensure_ascii=False), now, now),
            )
            await db.execute(
                "INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at) VALUES(?,?,?,?,?,?,?)",
                ("order:9956", "H", "Чат", 1, 1, 0, now),
            )
            await db.commit()
        flows = [
            {"course_key": "puppy", "course": "Щенок", "stream": "18", "teacher": "Ирина", "teacher_code": "Куратор 1"},
            {"course_key": "dog", "course": "Собака", "stream": "35", "teacher": "Слава", "teacher_code": "Куратор 2"},
        ]
        changed = await module._reconcile_sheet_assignments({"items": [
            {"course_key": "puppy", "course": "Щенок", "stream": "18", "sheet_title": "Щ18 (11.08)", "sheet_id": 1, "students": [{"email": "artimida444@yandex.ru", "row": 50}]},
            {"course_key": "dog", "course": "Собака", "stream": "35", "sheet_title": "С35 (01.12)", "sheet_id": 2, "students": [{"email": "artimida444@yandex.ru", "row": 8, "tariff": "Премиум"}]},
        ]}, flows)
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute("SELECT course_key,stream,teacher_code,tariff,source_json FROM enrollments WHERE id='order:9956'" )).fetchone()
            lessons = await (await db.execute("SELECT COUNT(*) FROM lesson_progress WHERE enrollment_id='order:9956'" )).fetchone()
        return changed, row, lessons[0]

    changed, row, lessons = asyncio.run(scenario())
    assert changed == 0
    assert row[:4] == ("puppy", "18", "Куратор 1", "Премиум")
    assert json.loads(row[4])["row"] == 50
    assert json.loads(row[4])["sheet_title"] == "Щ18 (11.08)"
    assert [item["sheet_title"] for item in json.loads(row[4])["course_assignments"]] == ["Щ18 (11.08)", "С35 (01.12)"]
    assert json.loads(row[4])["order_id"] == "708569132"
    assert json.loads(row[4])["name"] == "Анастасия Леус"
    assert lessons == 1


def test_historical_order_is_not_put_into_latest_flow(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        flows = await module._upsert_flows(
            {"items": [{"course_key": "puppy", "course": "Щенок", "stream": "57", "date_start": "2026-07-31", "vk_link": "https://vk/57", "tg_link": "https://t.me/57"}]},
            {"items": []},
        )
        await module._assign_new_orders(
            {"items": [{"source_record_id": 902, "email": "old@example.com", "date": "2025-09-04T14:31:34Z", "course_key": "puppy", "course": "Щенок"}]},
            flows,
        )
        async with aiosqlite.connect(module._must_db()) as db:
            return await (await db.execute("SELECT stream,status FROM enrollments WHERE source_record_id=902")).fetchone()

    assert asyncio.run(scenario()) == ("", "pending")


def test_pending_client_remains_visible(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                "INSERT INTO enrollments(id,email,course_key,course,stream,status,created_at,updated_at) VALUES('pending-client','old@example.com','puppy','Щенок','','pending',?,?)",
                (now, now),
            )
            await db.commit()
        return await module._registry_snapshot()

    snapshot = asyncio.run(scenario())
    pending = next(item for item in snapshot["items"] if item.get("status") == "pending")
    assert pending["stream"] == ""
    assert pending["students"][0]["email"] == "old@example.com"


def test_curator_sources_update_matching_students_and_keep_override(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                "INSERT INTO flow_registry(course_key,stream,course,teacher,teacher_code,updated_at) VALUES('puppy','57','Щенок','Ирина','Куратор 1',?)",
                (now,),
            )
            await db.executemany(
                "INSERT INTO enrollments(id,email,course_key,course,stream,teacher,teacher_code,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [
                    ("follows-flow", "flow@example.com", "puppy", "Щенок", "57", "Ирина", "Куратор 1", "assigned", now, now),
                    ("override", "override@example.com", "puppy", "Щенок", "57", "Слава", "Куратор 2", "assigned", now, now),
                ],
            )
            await db.commit()
        previous = {("puppy", "57"): {"teacher_code": "Куратор 1"}}
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                "UPDATE flow_registry SET teacher='Настасья',teacher_code='Куратор 3' WHERE course_key='puppy' AND stream='57'"
            )
            await db.commit()
        current = await module._flow_rows()
        changed = await module._propagate_flow_curator_changes(previous, current, {("puppy", "57")})
        await module._apply_sheet_curators(
            {"items": [{
                "course_key": "puppy", "stream": "57", "curator_value": "Куратор 1",
                "students": [{"email": "flow@example.com", "responsible_curator": "Куратор 1"}],
            }]},
            {("puppy", "57")},
            changed,
        )
        async with aiosqlite.connect(module._must_db()) as db:
            return await (await db.execute("SELECT id,teacher_code FROM enrollments ORDER BY id")).fetchall()

    assert asyncio.run(scenario()) == [("follows-flow", "Куратор 3"), ("override", "Куратор 2")]


def test_sheet_lesson_is_authoritative_over_old_nexus_edit(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """
                INSERT INTO enrollments(id,email,course_key,course,stream,status,created_at,updated_at)
                VALUES('lesson-client','lesson@example.com','puppy','Щенок','55','assigned',?,?)
                """,
                (now, now),
            )
            await db.execute(
                """
                INSERT INTO lesson_progress(enrollment_id,lesson_key,label,value,sheet_value,dirty,updated_at)
                VALUES('lesson-client','H','Урок 1',1,0,1,?)
                """,
                (now,),
            )
            await db.commit()
        changed = await module._import_sheet_lessons(
            {"items": [{"course_key": "puppy", "stream": "55", "lesson_columns": [{"key": "H", "label": "Урок 1"}], "students": [{"email": "lesson@example.com", "lessons": {"H": False}}]}]}
        )
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute("SELECT value,sheet_value,dirty FROM lesson_progress")).fetchone()
        return changed, row

    changed, row = asyncio.run(scenario())
    assert changed == 1
    assert row == (0, 0, 0)


def test_sheet_lesson_duplicate_email_uses_seeded_row(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """
                INSERT INTO enrollments(
                    id,email,course_key,course,stream,status,source_json,created_at,updated_at
                ) VALUES('duplicate-client','duplicate@example.com','puppy','Щенок','18','assigned','{"row":10}',?,?)
                """,
                (now, now),
            )
            await db.commit()
        snapshot = {
            "items": [{
                "course_key": "puppy",
                "stream": "18",
                "lesson_columns": [{"key": "H", "label": "Урок 1"}],
                "students": [
                    {"row": 10, "email": "duplicate@example.com", "lessons": {"H": True}},
                    {"row": 47, "email": "duplicate@example.com", "lessons": {"H": False}},
                ],
            }]
        }
        first = await module._import_sheet_lessons(snapshot)
        second = await module._import_sheet_lessons(snapshot)
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute("SELECT value,sheet_value,dirty FROM lesson_progress")).fetchone()
        return first, second, row

    assert asyncio.run(scenario()) == (1, 0, (1, 1, 0))


def test_registry_sync_never_calls_google_mirror(tmp_path, monkeypatch):
    class Fields:
        async def service_flow_catalog(self):
            return {"ok": True, "items": [{"course_key": "puppy", "course": "Щенок", "stream": "55", "vk_link": "https://vk/55", "tg_link": "https://t.me/55"}]}

        async def service_transfer_snapshot(self, **kwargs):
            return {"ok": True, "items": [{"course_key": "puppy", "course": "Щенок", "stream": "55", "students": [{"email": "safe@example.com"}]}]}

        async def service_entitled_orders(self, **kwargs):
            return {"ok": True, "items": [], "cursor": 10, "max_source_record_id": 10}

        async def service_registry_sheet_snapshot(self, **kwargs):
            return {"ok": True, "items": []}

        async def service_registry_sheet_mirror(self, **kwargs):
            raise AssertionError("reverse Google mirror must stay disabled")

        async def service_reconcile_registry_curators(self, **kwargs):
            return {"queued": 0}

    class Creator:
        def service_flow_catalog(self):
            return {"ok": True, "items": [{"course_key": "puppy", "stream": "55", "teacher": "Ирина", "offer_id": 8593080}]}

    fields, creator = Fields(), Creator()
    monkeypatch.setattr(module, "_module", lambda module_id, service: fields if module_id == "getcourse-chat-fields" else creator)
    module._db_path = tmp_path / "student-transfer.db"
    module._last_registry_sync = 0
    module._registry_retry_at = 0
    asyncio.run(module._init_db())

    result = asyncio.run(module._sync_registry(force=True))
    assert result["ok"] is True, result
    assert result["mirror"] == {"ok": True, "paused": True, "reason": "google_is_source"}


def test_curator_change_keeps_flow_and_skips_messengers(tmp_path, monkeypatch):
    class Fields:
        calls = []

        async def service_transfer_write_curator(self, **kwargs):
            self.calls.append("getcourse")
            return {"ok": True, "status": "updated"}

        async def service_transfer_update_student_curator(self, **kwargs):
            self.calls.append("sheet")
            return {"ok": True, "status": "updated"}

    fields = Fields()

    def service(module_id, name):
        assert module_id == "getcourse-chat-fields"
        return fields

    monkeypatch.setattr(module, "_module", service)
    async def synced(force=False):
        return {"ok": True, "status": "completed"}

    monkeypatch.setattr(module, "_sync_registry", synced)
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        async with aiosqlite.connect(module._must_db()) as db:
            now = module._now()
            await db.execute(
                """
                INSERT INTO enrollments(
                    id,source_record_id,order_id,deal_number,gc_user_id,name,email,course_key,course,
                    stream,teacher,teacher_code,status,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "legacy-ivan", 0, "200", "D-200", "100", "Иван", "ivan@example.com",
                    "puppy", "Щенок", "55", "Ирина", "Куратор 1", "assigned", now, now,
                ),
            )
            await db.execute(
                """
                INSERT INTO transfers(
                    id,status,email,gc_user_id,student_name,source_course_key,source_stream,source_row,
                    target_course_key,target_stream,curator,offer_id,operator_id,operator_name,
                    student_json,steps_json,error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "curator-op", "queued", "ivan@example.com", "100", "Иван", "puppy", "55", 8,
                    "puppy", "55", "Куратор 3", 8593084, 1, "Оператор",
                    json.dumps({"order_id": "200", "deal_number": "D-200"}),
                    json.dumps({"preview": {"action": "curator_change"}}), "", module._now(), module._now(),
                ),
            )
            await db.commit()
        await module._run_transfer("curator-op")
        async with aiosqlite.connect(module._must_db()) as db:
            transfer = await (await db.execute("SELECT status FROM transfers WHERE id='curator-op'")).fetchone()
            enrollment = await (
                await db.execute("SELECT stream,teacher_code FROM enrollments WHERE id='legacy-ivan'")
            ).fetchone()
            return transfer, enrollment

    transfer, enrollment = asyncio.run(scenario())
    assert transfer[0] == "completed"
    assert enrollment == ("55", "Куратор 3")
    assert fields.calls == ["getcourse", "sheet"]


def test_fullscreen_panel_can_change_curator():
    panel = (Path(__file__).parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    assert 'id="curatorBtn"' in panel
    assert 'id="targetCurator"' in panel
    assert "api('/curator-changes'" in panel
    assert "GetCourse ↗" in panel
    assert "accessChip(root,'Курс'" not in panel
    assert 'id="sendChatsBtn"' in panel
    assert "Выслать чаты" in panel
    assert "chat-delivery" in panel
    assert "item.phone" in panel
    assert "waiting:'Ожидает повтора'" in panel


def test_binding_created_sheet_row_initializes_progress(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """
                INSERT INTO enrollments(id,email,course_key,course,stream,status,source_json,created_at,updated_at)
                VALUES('created-row','row@example.com','puppy','Щенок','57','assigned','{}',?,?)
                """,
                (now, now),
            )
            await db.commit()
        await module._bind_sheet_row(
            "created-row", 12,
            [{"key": "I", "label": "Чат"}, {"key": "J", "label": "ВИП1"}],
        )
        async with aiosqlite.connect(module._must_db()) as db:
            source = await (await db.execute("SELECT source_json FROM enrollments WHERE id='created-row'" )).fetchone()
            lessons = await (await db.execute(
                "SELECT lesson_key,label,value,sheet_value,dirty FROM lesson_progress ORDER BY lesson_key"
            )).fetchall()
        return json.loads(source[0]), lessons

    source, lessons = asyncio.run(scenario())
    assert source["row"] == 12
    assert lessons == [("I", "Чат", 0, 0, 0), ("J", "ВИП1", 0, 0, 0)]


def test_fullscreen_panel_writes_progress_and_creates_missing_rows():
    panel = (Path(__file__).parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    assert "/sheet-row" in panel
    assert "expected_value" in panel
    assert "data-lesson" in panel
    assert "Модуль выдан" not in panel
    assert "ДЗ принято" in panel
    assert "progressButton(stage.issued" not in panel
    assert "lessonGuardUntil" in panel
    assert "Добавляем в таблицу" in panel
    assert "classList.add('busy')" in panel
    assert "Только таблица" in panel
    assert "Проверяем…" in panel
    assert "Запускаем…" in panel


def test_fullscreen_panel_has_entity_links_and_homework_workspace():
    panel = (Path(__file__).parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    assert 'data-view="homework"' in panel
    assert "if(params.client)url.search=`?client=${encodeURIComponent(params.client).replace(/%40/gi,'@')}`" in panel
    assert "String(id).includes('@')" in panel
    assert 'id="studentLinkBtn"' not in panel
    assert "!data?.pending||!data.next_check_at" in panel
    assert "data?.pending||data?.refresh_due" not in panel
    assert "flowUrl=flow=>" in panel
    assert "history[replace?'replaceState':'pushState']" in panel
    assert "view==='homework'" in panel
    assert "toggleHomework" in panel
    assert "expected_value:Boolean(lesson.value)" in panel
    assert any(route.path == "/students/{enrollment_id}" for route in module.router.routes)


def test_access_verification_waits_for_getcourse(monkeypatch):
    calls = []

    async def view(identity, *, live, force=False):
        calls.append(("view", identity, live, force))
        return {"ok": True}

    monkeypatch.setattr(module, "_get_access_view", view)
    result = asyncio.run(module._get_access_after_write({"email": "student@example.com"}))
    assert result == {"ok": True}
    assert calls == [("view", {"email": "student@example.com"}, True, True)]


def test_registry_snapshot_is_reused_for_fast_reads(monkeypatch):
    calls = []

    async def registry_snapshot(*, refresh=False):
        calls.append(refresh)
        return {"ok": True, "items": []}

    monkeypatch.setattr(module, "_registry_snapshot", registry_snapshot)
    module._clear_snapshot_cache()

    async def scenario():
        assert await module._snapshot() == {"ok": True, "items": []}
        assert await module._snapshot() == {"ok": True, "items": []}

    asyncio.run(scenario())
    assert calls == [False]
    module._clear_snapshot_cache()


def test_sheet_row_payload_keeps_purchase_date():
    result = module._student_result(
        {"course_key": "puppy", "course": "Щенок", "stream": "57"},
        {"enrollment_id": "x", "email": "x@example.com", "date": "2026-08-02T17:59:53Z"},
    )
    assert result["date"] == "2026-08-02T17:59:53Z"
