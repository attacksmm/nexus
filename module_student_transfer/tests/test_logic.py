import asyncio
import json
import sqlite3
from pathlib import Path

import aiosqlite
import pytest
from fastapi import HTTPException
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


def test_combo_copy_preserves_old_chats(tmp_path, monkeypatch):
    class Fields:
        async def service_transfer_move_student(self, **kwargs):
            assert kwargs["move"] is False
            return {"ok": True, "status": "copied", "target_row": 35, "source_row_deleted": False}

        async def service_transfer_write_getcourse(self, **_kwargs):
            return {"ok": True}

    class Removal:
        called = 0

        async def service_remove_transfer_member(self, **_kwargs):
            self.called += 1
            return {"ok": True}

    removal = Removal()
    services = {
        "getcourse-chat-fields": Fields(),
        "messenger-widget": IdentityService(),
        "course-chat-creator": removal,
    }
    monkeypatch.setattr(module, "_module", lambda module_id, _service: services[module_id])

    async def commit(*_args, **_kwargs):
        return {"ok": True, "status": "updated"}

    monkeypatch.setattr(module, "_commit_registry_transfer", commit)
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
                (
                    "copy1", "queued", "student@example.com", "100", "Иван", "puppy", "58", 8,
                    "dog", "55", "Куратор 2", 8593081, 1, "Оператор", "{}",
                    json.dumps({"preview": {"sheet": {"move": False}}}), "", now, now,
                ),
            )
            await db.commit()
        await module._run_transfer("copy1")
        async with aiosqlite.connect(module._must_db()) as db:
            return await (await db.execute(
                "SELECT status,error,steps_json FROM transfers WHERE id='copy1'"
            )).fetchone()

    status, error, raw_steps = asyncio.run(scenario())
    assert status == "completed"
    assert error == ""
    assert json.loads(raw_steps)["chat_removal"]["status"] == "preserved"
    assert removal.called == 0


def test_combo_chat_delivery_contains_both_course_flows(monkeypatch):
    async def snapshot(*_args, **_kwargs):
        return {"items": [
            {
                "course_key": "puppy", "course": "Щенок", "stream": "58",
                "vk_link": "https://vk.example/puppy58", "tg_link": "https://t.me/puppy58",
            },
            {
                "course_key": "dog", "course": "Собака", "stream": "55",
                "vk_link": "https://vk.example/dog55", "tg_link": "https://t.me/dog55",
            },
        ]}

    class Messenger:
        async def service_transfer_delivery_target(self, **_kwargs):
            return {"ok": True, "provider": "vk", "recipient_id": "100"}

    monkeypatch.setattr(module, "_snapshot", snapshot)
    monkeypatch.setattr(module, "_module", lambda *_args: Messenger())
    result = asyncio.run(module._chat_delivery_view({
        "email": "student@example.com", "gc_user_id": "1", "tariff": "premium",
        "product_kind": "combo", "course": "Собака", "course_key": "dog", "stream": "55",
        "course_assignments": [
            {"course_key": "puppy", "stream": "58"},
            {"course_key": "dog", "stream": "55"},
        ],
    }))

    assert result["can_send"] is True
    assert [item["stream"] for item in result["flow_links"]] == ["58", "55"]
    assert "Щенок · поток 58" in result["content"]
    assert "https://vk.example/puppy58" in result["content"]
    assert "Собака · поток 55" in result["content"]
    assert "https://t.me/dog55" in result["content"]


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


def test_existing_operator_can_login_by_name_without_nexus_identity(tmp_path, monkeypatch):
    module._db_path = tmp_path / "student-transfer.db"
    module._module_dir = tmp_path
    asyncio.run(module._init_db())

    async def anonymous(_request):
        return None

    monkeypatch.setattr(module, "verify_token_from_request", anonymous)
    request = Request({
        "type": "http", "method": "POST", "path": "/streams/login",
        "headers": [
            (b"host", b"junior.sobakovod.pro"),
            (b"origin", b"https://junior.sobakovod.pro"),
            (b"x-forwarded-prefix", b"/streams"),
        ],
    })
    response = asyncio.run(module.login(module.LoginIn(login="Никита Попов"), request))
    cookie = response.headers["set-cookie"]
    assert module.SESSION_COOKIE in cookie
    assert "Path=/streams" in cookie
    assert "HttpOnly" in cookie


def test_nexus_identity_does_not_authorize_or_provision_streams_operator(tmp_path, monkeypatch):
    module._db_path = tmp_path / "student-transfer.db"
    module._module_dir = tmp_path
    asyncio.run(module._init_db())

    async def nexus_user(_request):
        return {"username": "secure.operator", "role": "editor", "module_access": '["student-transfer"]'}

    monkeypatch.setattr(module, "verify_token_from_request", nexus_user)
    request = Request({
        "type": "http", "method": "GET", "path": "/streams/me",
        "headers": [(b"cookie", b"nexus_token=still-valid")],
    })
    with pytest.raises(HTTPException) as error:
        asyncio.run(module._require_operator(request))
    assert error.value.status_code == 401
    with sqlite3.connect(module._must_db()) as db:
        assert db.execute("SELECT 1 FROM operators WHERE login_key='secure.operator'").fetchone() is None


def test_streams_password_is_required_once_configured(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    module._module_dir = tmp_path
    asyncio.run(module._init_db())
    password_hash = module._password_ctx.hash("correct-horse")
    with sqlite3.connect(module._must_db()) as db:
        db.execute("UPDATE operators SET password_hash=? WHERE login_key=?", (password_hash, module._norm("Никита Попов")))
        db.commit()
    request = Request({
        "type": "http", "method": "POST", "path": "/streams/login",
        "headers": [(b"host", b"junior.sobakovod.pro"), (b"origin", b"https://junior.sobakovod.pro")],
    })
    with pytest.raises(HTTPException) as missing:
        asyncio.run(module.login(module.LoginIn(login="Никита Попов"), request))
    assert missing.value.status_code == 401
    with pytest.raises(HTTPException) as wrong:
        asyncio.run(module.login(module.LoginIn(login="Никита Попов", password="wrong-pass"), request))
    assert wrong.value.status_code == 401
    response = asyncio.run(module.login(module.LoginIn(login="Никита Попов", password="correct-horse"), request))
    assert response.status_code == 200


def test_passwordless_operator_rejects_unexpected_password(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    module._module_dir = tmp_path
    asyncio.run(module._init_db())
    request = Request({
        "type": "http", "method": "POST", "path": "/streams/login",
        "headers": [(b"host", b"junior.sobakovod.pro"), (b"origin", b"https://junior.sobakovod.pro")],
    })
    with pytest.raises(HTTPException) as error:
        asyncio.run(module.login(module.LoginIn(login="Никита Попов", password="not-configured"), request))
    assert error.value.status_code == 401


def test_cross_site_mutation_is_rejected():
    request = Request({
        "type": "http", "method": "POST", "path": "/streams/sync",
        "headers": [
            (b"host", b"junior.sobakovod.pro"),
            (b"origin", b"https://evil.example"),
            (b"sec-fetch-site", b"cross-site"),
        ],
    })
    with pytest.raises(HTTPException) as error:
        module._require_same_origin(request)
    assert error.value.status_code == 403


def test_fullscreen_app_uses_hashed_script_csp_and_security_headers():
    module._module_dir = Path(__file__).resolve().parents[1]
    response = asyncio.run(module.fullscreen_app())
    csp = response.headers["content-security-policy"]
    script_policy = next(item for item in csp.split("; ") if item.startswith("script-src"))
    assert "sha256-" in script_policy
    assert "unsafe-inline" not in script_policy
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_operator_admin_panel_manages_streams_passwords_without_exposing_hashes():
    panel = (Path(__file__).resolve().parents[1] / "panel" / "index.html").read_text(encoding="utf-8")
    assert 'id="password" type="password"' in panel
    assert 'id="clearPassword"' in panel
    assert "password_set?'Установлен':'Без пароля'" in panel
    assert "clear_password:$('clearPassword').checked" in panel
    assert "Загружаем операторов…" in panel


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


def test_students_searches_enriched_phone_in_any_russian_format(monkeypatch):
    async def allow(_request):
        return {"id": 1}

    async def current_snapshot(refresh=False):
        return {
            "updated_at": "now",
            "items": [{
                "course_key": "puppy", "course": "Щенок", "stream": "58",
                "students": [{
                    "enrollment_id": "student-1", "email": "phone@example.com",
                    "gc_user_id": "403378908", "order_id": "726039707",
                }],
            }],
        }

    async def enrich(items):
        for item in items:
            item["phone"] = "+7 (916) 827-13-03"

    async def no_enrichment(_items):
        return None

    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_snapshot", current_snapshot)
    monkeypatch.setattr(module, "_enrich_order_identities", enrich)
    monkeypatch.setattr(module, "_enrich_successful_managers", no_enrichment)
    result = asyncio.run(module.students(Request({"type": "http"}), q="8 916 827 13 03"))
    assert result["total"] == 1
    assert result["items"][0]["email"] == "phone@example.com"
    assert result["items"][0]["phone"] == "+7 (916) 827-13-03"


def test_students_email_search_does_not_use_digits_as_a_short_phone_query(monkeypatch):
    async def allow(_request):
        return {"id": 1}

    async def current_snapshot(refresh=False):
        return {
            "updated_at": "now",
            "items": [{
                "course_key": "puppy", "course": "Щенок", "stream": "51",
                "students": [
                    {
                        "enrollment_id": "target", "name": "Алина",
                        "email": "mail.ru789@mail.ru", "phone": "79819793382",
                    },
                    {
                        "enrollment_id": "other", "name": "Анна",
                        "email": "anna@example.com", "phone": "+79220478938",
                    },
                ],
            }],
        }

    async def no_enrichment(_items):
        return None

    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_snapshot", current_snapshot)
    monkeypatch.setattr(module, "_enrich_order_identities", no_enrichment)
    monkeypatch.setattr(module, "_enrich_successful_managers", no_enrichment)
    result = asyncio.run(module.students(Request({"type": "http"}), q="mail.ru789@mail.ru"))
    assert result["total"] == 1
    assert [item["enrollment_id"] for item in result["items"]] == ["target"]


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
    assert 'class="table mobile-cards student-cards"' in panel
    assert 'class="student-mobile-details"' in panel
    assert ".table.student-cards td:nth-child(n+2):nth-child(-n+6){display:none}" in panel
    assert ".student-mobile-meta span+span::before{content:'·'" in panel
    assert 'class="toolbar student-toolbar" id="studentToolbar"' in panel
    assert ".student-toolbar:not(.filters-open)>:not(#searchInput){display:none}" in panel
    assert "function setMobileFilters(open)" in panel
    assert "if(event.key==='Escape')" in panel
    assert 'class="table mobile-cards flow-cards"' in panel
    assert 'class="table mobile-cards operation-cards"' in panel
    assert 'class="flow-mobile-details"' in panel
    assert 'class="operation-mobile-details"' in panel
    assert ".table.flow-cards td:nth-child(n+2):nth-child(-n+6){display:none}" in panel
    assert ".table.operation-cards td:nth-child(n+2):nth-child(-n+4){display:none}" in panel
    assert "function shortDateTime(value)" in panel
    assert 'id="sidebarBackdrop"' in panel
    assert 'aria-controls="sidebar" aria-expanded="false"' in panel
    assert "function setMobileMenu(open)" in panel
    assert "mobileNav.addEventListener?.('change',()=>setMobileMenu(false))" in panel
    assert "$('sidebarBackdrop').onclick=()=>{closeMobile();$('menuBtn').focus()}" in panel
    assert 'placeholder="Имя, email, телефон или ID"' in panel
    assert '>Ученики</button>' in panel
    assert '<h1 id="pageTitle">Ученики</h1>' in panel
    assert '← Ученики' in panel
    assert '.student-actions .btn:last-child{grid-column:1/-1}' not in panel
    assert '.student-meta{grid-template-columns:repeat(2,minmax(0,1fr))}' in panel
    assert "['4842617','Намордник']" in panel
    assert "['4842619','Намордник + ОС']" in panel
    assert "['4443745','За 15 минут']" in panel
    assert 'За 15 минут + ОС' not in panel


def test_messenger_dialog_is_wide_theme_aware_and_help_covers_full_workflow():
    panel = (Path(__file__).resolve().parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    assert "dialog.messenger-dialog{width:min(1540px" in panel
    assert ".messenger-channel.is-active,.messenger-channel.is-active:hover" in panel
    assert "background:#f6f7f9" not in panel.split(".messenger-channel.is-active", 1)[1].split("}", 1)[0]
    assert ".messenger-shell{height:100%;min-height:0;display:grid;grid-template-rows:48px 58px minmax(0,1fr) auto" in panel
    assert ".messenger-compose{min-width:0;display:grid;grid-template-columns:minmax(0,1fr) auto" in panel
    assert '<div class="messenger-main">' not in panel
    assert 'id="messengerFeed"' in panel
    assert "feed.scrollTop=feed.scrollHeight" in panel
    assert "active.scrollIntoView({block:'nearest',inline:'nearest'})" in panel
    assert "function updateMessengerSelection(item,data,selectedId='',revealTab=false)" in panel
    assert "renderMessenger(item,data,button.dataset.messengerChannel)" not in panel
    assert 'class="messenger-attachment-media busy"' not in panel  # created safely with DOM methods
    assert "frame.className='messenger-attachment-media busy'" in panel
    assert "Загружаем изображение…" in panel
    assert "Файл не удалось показать здесь." in panel
    assert "event.target!==$('actionDialog')" in panel
    assert "if(outside)closeActionDialog()" in panel
    assert "function appendMessengerRichText(node,html)" in panel
    assert "Отправить везде" in panel
    assert "★ Избранное" in panel
    assert "Вложение" in panel
    assert 'placeholder="Сообщение…"' in panel
    assert 'class="messenger-trigger" id="messengerBtn"' in panel
    assert 'disabled title="${esc(value.send_reason' in panel
    assert 'id="studentToolbar"' in panel.split('id="studentsView"', 1)[0]
    assert "function syncTopbar()" in panel
    assert "messenger-history-count" in panel
    assert "Загружаем каналы, профили и сообщения…" in panel
    assert 'id="help-start"' in panel
    assert 'id="help-messages"' in panel
    assert 'id="help-operations"' in panel
    assert "от входа до проверки результата" in panel
    assert "comboAdd=item.product_kind==='combo'&&target.course_key!==item.course_key" in panel
    assert "move_sheet_row:!comboAdd" in panel
    assert "Удалить из старого потока" in panel
    assert "Старая строка останется. В новом потоке прогресс начнётся с нуля." in panel
    assert "Старая строка будет удалена. Прогресс перенесётся." in panel
    assert "function showFlowList()" in panel
    assert "if(view==='flows'&&route)showFlowList()" in panel
    assert "Загружаем потоки…" in panel
    assert "Потоки не загрузились. Нажмите «Повторить»." in panel
    assert "if(state.flowLoading)" in panel
    assert "Показаны последние данные:" in panel
    assert "switchView('flows',false,false);showFlowList();if(state.flowLoadError&&!state.flows.length)renderFlowLoadError();else renderFlows()" in panel
    assert "if(initialParams.get('flow')||initialView==='flows'){switchView('flows',false,false);showFlowList()}" in panel
    assert "flowsLoaded=await flowsPromise" in panel
    assert "await applyRoute();" in panel
    assert "Promise.allSettled([studentsPromise,operationsPromise]);" in panel
    assert "api('/access-operations?limit=30&compact=1')" in panel
    assert "function openAccessOperation(id)" in panel
    assert "Выдача доступов" in panel
    assert "Загружаем ход проверки…" in panel
    assert "if(flowsLoaded!==false)setStatus('Готово','ok')" in panel
    assert '<tbody id="studentRows"></tbody>\n            </table>' in panel
    assert 'id="workspaceLoading"' in panel
    assert 'class="workspace-spinner"' in panel
    assert "beginLoading('flows',refresh?'Обновляем потоки…':'Загружаем потоки…')" in panel
    assert "endLoading('flows')" in panel
    assert "api('/logout',{method:'POST'});location.reload()" in panel
    assert "/nexus/logout" not in panel
    assert "/nexus/login" not in panel
    assert 'id="loginForm"' in panel
    assert 'id="loginPassword"' in panel
    assert "form.action='/logout'" not in panel


def test_panel_queues_latest_filter_change_while_students_are_loading():
    panel = (Path(__file__).resolve().parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")

    assert "if(state.loading){state.loadQueued=true" in panel
    assert "if(state.loadQueued){const queuedRefresh=state.loadQueuedRefresh" in panel


def test_manager_enrichment_is_read_only_and_optional(monkeypatch):
    class Amo:
        async def service_successful_managers(self, **kwargs):
            assert kwargs["identities"][0]["email"] == "student@example.com"
            assert kwargs["identities"][0]["order_id"] == "order-42"
            assert kwargs["identities"][0]["deal_number"] == "deal-42"
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
    items = [{
        "enrollment_id": "student-1", "email": "student@example.com", "phone": "",
        "order_id": "order-42", "deal_number": "deal-42",
    }]
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


def test_sheet_account_link_uses_verified_telegram_username_then_vk(monkeypatch):
    class Messenger:
        async def service_transfer_recipients(self, **kwargs):
            assert kwargs["email"] == "student@example.com"
            return {"telegram_username": "@anna_dog", "vk": "123456789"}

    monkeypatch.setattr(module, "_module", lambda module_id, service: Messenger())
    student = {"email": "student@example.com", "name": "Анна", "tg_account": ""}
    assert asyncio.run(module._resolve_student_profile_link(student)) == "https://t.me/anna_dog"

    class VkOnlyMessenger:
        async def service_transfer_recipients(self, **_kwargs):
            return {"telegram_username": "", "vk": "123456789"}

    monkeypatch.setattr(module, "_module", lambda module_id, service: VkOnlyMessenger())
    assert asyncio.run(module._resolve_student_profile_link(student)) == "https://vk.com/id123456789"
    assert asyncio.run(module._resolve_student_profile_link({"tg_account": "https://vk.com/existing"})) == "https://vk.com/existing"


def test_access_view_separates_getcourse_groups_from_sheet_progress():
    catalog = [
        {"group_id": 4059685, "name": "Знакомство. Щенок", "course_key": "puppy", "group_kind": "root", "managed": True},
        {"group_id": 4306384, "name": "Выдача Щенка без процесса", "course_key": "puppy", "group_kind": "bridge", "managed": True},
        {"group_id": 4059687, "name": "1 модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": 1, "managed": True},
        {"group_id": 4999999, "name": "Помодульно. Щенок", "course_key": "puppy", "group_kind": "package", "package_key": "module_standard", "managed": True},
    ]
    result = module._access_view(
        {"ok": True, "source": "live", "groups": [{"group_id": "4059687", "name": "1 модуль. Щенок"}]},
        catalog,
    )
    assert [item["group_id"] for item in result["items"]] == ["4059685", "4059687"]
    assert result["items"][1]["enabled"] is True


def test_partial_payment_marks_order_tariff_without_adding_a_fake_group():
    catalog = [
        {"group_id": 4059658, "name": "Премиум. Щенок", "course_key": "puppy", "group_kind": "package", "package_key": "premium", "managed": True},
        {"group_id": 4059686, "name": "0 модуль. Щенок", "course_key": "puppy", "group_kind": "module", "module_index": 0, "managed": True},
        {"group_id": 4116079, "name": "Частичные оплаты. Щенок", "course_key": "puppy", "group_kind": None, "managed": False},
    ]
    result = module._access_view(
        {
            "ok": True,
            "source": "cache",
            "groups": [
                {"group_id": "4059686", "name": "0 модуль. Щенок"},
                {"group_id": "4116079", "name": "Частичные оплаты. Щенок"},
            ],
        },
        catalog,
        {"course_key": "puppy", "tariff": "Premium", "payment_state": "partial"},
    )
    premium = next(item for item in result["items"] if item.get("package_key") == "premium")
    assert premium["enabled"] is True
    assert premium["inferred"] is True
    assert {str(item["group_id"]) for item in result["current_groups"]} == {"4059686", "4116079"}


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


def test_live_access_prefers_authenticated_browser_without_export_budget(monkeypatch):
    class Browser:
        async def service_getcourse_browser_access_snapshot(self, **kwargs):
            assert kwargs == {"gc_user_id": "511"}
            return {
                "ok": True,
                "source": "browser",
                "groups": [{"group_id": "4059687", "name": "1 модуль. Щенок"}],
            }

    class Fields:
        async def service_getcourse_access_snapshot(self, **_kwargs):
            raise AssertionError("Export API must stay a fallback when the browser succeeds")

    class Access:
        def service_access_catalog(self):
            return {"ok": True, "items": [{
                "group_id": 4059687,
                "name": "1 модуль. Щенок",
                "course_key": "puppy",
                "group_kind": "module",
                "module_index": 1,
                "managed": True,
            }]}

    services = {
        "getcourse-onboarding": Browser(),
        "getcourse-chat-fields": Fields(),
        "chat-moderators": Access(),
    }
    monkeypatch.setattr(module, "_module", lambda module_id, _service: services[module_id])
    result = asyncio.run(module._get_access_view(
        {"gc_user_id": "511", "email": "student@example.com"}, live=True, force=True,
    ))
    assert result["source"] == "browser"
    assert result["items"][0]["enabled"] is True


def test_access_preview_uses_cached_groups_when_getcourse_verification_is_busy(monkeypatch):
    calls = []

    async def allow(_request):
        return {"id": 7}

    async def identity(_enrollment_id):
        return {"gc_user_id": "511", "email": "student@example.com"}

    async def view(_identity, *, live, force=False, allow_stale=False):
        calls.append((live, force, allow_stale))
        return {
            "ok": True,
            "source": "cache",
            "stale": True,
            "current_groups": [{"group_id": "4059685", "name": "Щенок"}],
            "items": [],
        }

    class Fields:
        async def service_getcourse_access_budget(self):
            return {"requests_left_2h": 5, "needed_for_verification": 6, "next_at": "2026-08-11 16:00:00"}

    class Access:
        def service_prepare_access_change(self, **kwargs):
            assert kwargs["current_groups"][0]["group_id"] == "4059685"
            return {"request_id": "request1", "added": ["2 модуль. Щенок"], "removed": []}

    fields, access = Fields(), Access()
    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_access_identity", identity)
    monkeypatch.setattr(module, "_get_access_view", view)
    monkeypatch.setattr(module, "enforce_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_module",
        lambda module_id, _service: fields if module_id == "getcourse-chat-fields" else access,
    )
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "client": ("127.0.0.1", 1)})
    result = asyncio.run(module.preview_student_access(
        "student-1",
        module.AccessPreviewIn(changes=[{"group_id": "4059687", "enabled": True}]),
        request,
    ))

    assert calls == [(False, False, True)]
    assert result["request_id"] == "request1"
    assert result["verification_delayed"] is True
    assert result["next_check_at"] == "2026-08-11 16:00:00"


def test_access_apply_is_not_blocked_when_getcourse_verification_is_busy(monkeypatch):
    scheduled = {}

    async def allow(_request):
        return {"id": 7}

    async def identity(_enrollment_id):
        return {"gc_user_id": "511", "email": "student@example.com"}

    async def view(_identity, *, live, force=False, allow_stale=False):
        return {"ok": True, "source": "cache", "items": [{"group_id": "4059687", "enabled": False}]}

    class Access:
        def service_access_change_request(self, **kwargs):
            assert kwargs == {"request_id": "request1", "requester_user_id": "7"}
            return {"gc_user_id": "511", "identifier": "student@example.com"}

        def service_schedule_access_apply(self, **kwargs):
            scheduled.update(kwargs)
            return {
                "next_check_at": "2026-08-11T16:10:00Z",
                "ready_by": "2026-08-11T16:15:00Z",
                "target_groups": [{"group_id": "4059687", "name": "2 модуль. Щенок"}],
            }

    access = Access()
    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_access_identity", identity)
    monkeypatch.setattr(module, "_get_access_view", view)
    monkeypatch.setattr(module, "enforce_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_module",
        lambda _module_id, _service: access,
    )
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "client": ("127.0.0.1", 1)})
    result = asyncio.run(module.apply_student_access(
        "student-1", module.AccessApplyIn(request_id="request1"), request,
    ))

    assert result["queued"] is True
    assert result["applied"] is False
    assert result["verification_pending"] is True
    assert result["verification_delayed"] is True
    assert result["access"]["pending"] is True
    assert result["ready_by"] == "2026-08-11T16:15:00Z"
    assert scheduled == {"request_id": "request1", "requester_user_id": "7", "delay_seconds": 2}


def test_access_apply_rejects_a_request_prepared_for_another_student(monkeypatch):
    applied = False

    async def allow(_request):
        return {"id": 7}

    async def identity(_enrollment_id):
        return {"gc_user_id": "511", "email": "student@example.com"}

    class Fields:
        async def service_getcourse_access_budget(self):
            return {"requests_left_2h": 10, "needed_for_verification": 6}

    class Access:
        def service_access_change_request(self, **_kwargs):
            return {"gc_user_id": "999", "identifier": "other@example.com"}

        def service_apply_access_change(self, **_kwargs):
            nonlocal applied
            applied = True
            return {}

    fields, access = Fields(), Access()
    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_access_identity", identity)
    monkeypatch.setattr(module, "enforce_rate_limit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "_module",
        lambda module_id, _service: fields if module_id == "getcourse-chat-fields" else access,
    )
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "client": ("127.0.0.1", 1)})

    with pytest.raises(HTTPException) as error:
        asyncio.run(module.apply_student_access(
            "student-1", module.AccessApplyIn(request_id="request1"), request,
        ))

    assert error.value.status_code == 409
    assert "другого ученика" in str(error.value.detail)
    assert applied is False


def test_streams_endpoint_and_widget_service_use_the_same_access_pipeline(monkeypatch):
    preview_calls = []
    apply_calls = []

    async def allow(_request):
        return {"id": 7}

    async def preview_core(**kwargs):
        preview_calls.append(kwargs)
        return {"ok": True, "request_id": "shared-preview"}

    async def apply_core(**kwargs):
        apply_calls.append(kwargs)
        return {"ok": True, "applied": True, "operation_id": kwargs["request_id"]}

    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_preview_access_change", preview_core)
    monkeypatch.setattr(module, "_apply_access_change", apply_core)
    monkeypatch.setattr(module, "enforce_rate_limit", lambda *_args, **_kwargs: None)
    request = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "client": ("127.0.0.1", 1)})

    endpoint_preview = asyncio.run(module.preview_student_access(
        "student-1",
        module.AccessPreviewIn(changes=[{"group_id": "4059688", "enabled": True}]),
        request,
    ))
    widget_preview = asyncio.run(module.service_widget_access_preview(
        enrollment_id="student-1",
        changes=[{"group_id": "4059688", "enabled": True}],
        requester_user_id="messenger:1",
    ))
    endpoint_apply = asyncio.run(module.apply_student_access(
        "student-1", module.AccessApplyIn(request_id="shared-preview"), request,
    ))
    widget_apply = asyncio.run(module.service_widget_access_apply(
        enrollment_id="student-1", request_id="shared-preview", requester_user_id="messenger:1",
    ))

    assert endpoint_preview == widget_preview == {"ok": True, "request_id": "shared-preview"}
    assert endpoint_apply == widget_apply == {"ok": True, "applied": True, "operation_id": "shared-preview"}
    assert [call["requester_user_id"] for call in preview_calls] == ["7", "messenger:1"]
    assert [call["requester_user_id"] for call in apply_calls] == ["7", "messenger:1"]


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


def test_manual_vk_link_is_not_overwritten_by_stale_sheet_catalog(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        correct = "https://vk.me/join/correct="
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """INSERT INTO flow_jobs(
                       id,status,course_key,stream,date_start,teacher_id,operator_id,operator_name,
                       result_json,error,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "job-56", "completed", "dog", "56", "2026-08-13", 1, 1, "operator",
                    json.dumps({
                        "manual_link": {"ok": True, "link": correct},
                        "chats": {"telegram": {"group_link": "https://t.me/local-stream56"}},
                    }), "", now, now,
                ),
            )
            await db.commit()
        flows = await module._upsert_flows(
            {
                "items": [{
                    "course_key": "dog", "course": "Собака", "stream": "56",
                    "vk_link": "https://vk.me/join/stale=", "tg_link": "https://t.me/stream56",
                }]
            },
            {"items": []},
        )
        return next(item for item in flows if item["course_key"] == "dog" and item["stream"] == "56")

    flow = asyncio.run(scenario())
    assert flow["vk_link"] == "https://vk.me/join/correct="
    assert flow["tg_link"] == "https://t.me/local-stream56"
    assert flow["status"] == "ready"


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


def test_sheet_reconciliation_adopts_new_homework_student(tmp_path, monkeypatch):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    class Fields:
        async def service_order_identities(self, **kwargs):
            assert kwargs["identities"] == [{"key": "kim@example.com", "email": "kim@example.com"}]
            return {"ok": True, "items": [{
                "key": "kim@example.com", "source_record_id": 13844, "order_id": "876349578",
                "deal_number": "876349578", "gc_user_id": "512067241", "name": "Татьяна",
                "email": "kim@example.com", "date": "2026-08-07T12:00:00Z", "tariff": "Standard",
                "assignment": {"course_key": "dog", "stream": "55", "curator": "Куратор 2"},
            }]}

    monkeypatch.setattr(module, "_module", lambda module_id, _service: Fields() if module_id == "getcourse-chat-fields" else None)

    async def scenario():
        flows = [{
            "course_key": "dog", "course": "Собака", "stream": "55",
            "teacher": "Слава", "teacher_code": "Куратор 2",
        }]
        snapshot = {"items": [{
            "course_key": "dog", "course": "Собака", "stream": "55", "sheet_title": "С55 (03.08)",
            "sheet_id": 55, "students": [{"email": "kim@example.com", "name": "Татьяна", "row": 27,
            "tariff": "Стандарт", "lessons": {"J": True, "K": True}}],
        }]}
        changed = await module._reconcile_sheet_assignments(snapshot, flows)
        await module._import_sheet_lessons(snapshot)
        async with aiosqlite.connect(module._must_db()) as db:
            student = await (await db.execute(
                "SELECT source_record_id,order_id,gc_user_id,course_key,stream,teacher,teacher_code,status,source_json "
                "FROM enrollments WHERE lower(email)='kim@example.com'"
            )).fetchone()
            lessons = await (await db.execute(
                "SELECT lesson_key,value FROM lesson_progress WHERE enrollment_id=? ORDER BY lesson_key", (module._enrollment_id(13844, "dog", "55", "kim@example.com"),)
            )).fetchall()
        return changed, student, lessons

    changed, student, lessons = asyncio.run(scenario())
    assert changed == 1
    assert student[:8] == (13844, "876349578", "512067241", "dog", "55", "Слава", "Куратор 2", "assigned")
    assert json.loads(student[8])["row"] == 27
    assert dict(lessons)["J"] == 1
    assert dict(lessons)["K"] == 1


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
    assert "amoCRM ↗" in panel
    assert '>Менеджер</th>' in panel
    assert 'id="noteBtn"' in panel
    assert 'id="studentNote"' in panel
    assert '/note`' in panel
    assert "accessChip(root,'Курс'" not in panel
    assert 'id="sendChatsBtn"' in panel
    assert "Выслать чаты" in panel
    assert "chat-delivery" in panel
    assert "item.phone" in panel
    assert "поток ${flow.stream}" in panel
    assert "ссылки на чаты: ${esc(flows)}" in panel
    assert "waiting:'Ожидает повтора'" in panel
    assert "['Смена куратора',statusFor('getcourse')]" in panel
    assert "['Таблица',statusFor('sheet')]" in panel
    assert "['Поток на GetCourse',statusFor('getcourse')]" in panel
    assert "['Перенос в таблице',statusFor('sheet')]" in panel
    assert "Операция автоматически продолжится в ${retryAt}" in panel
    assert "item.status!=='waiting'" in panel


def test_widget_paid_summary_matches_unique_exact_phone_without_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "_db_path", tmp_path / "student-transfer.db")
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """INSERT INTO enrollments(
                   id,gc_user_id,name,email,course_key,course,stream,status,source_json,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "paid-exact-phone", "505433216", "Екатерина", "ek@example.test", "puppy", "Щенок", "59",
                    "assigned", json.dumps({"phone": "+7 (999) 144-09-95"}), now, now,
                ),
            )
            await db.commit()
        return await module.service_widget_student(phone="89991440995", include_access=False, summary_only=True)

    result = asyncio.run(scenario())
    assert result["found"] is True
    assert result["paid_access"] is True
    assert result["gc_user_id"] == "505433216"
    assert result["profile_url"].endswith("/user/control/user/update/id/505433216")
    assert result["item"]["enrollment_id"] == "paid-exact-phone"
    assert result["item"]["phone"] == "+7 (999) 144-09-95"
    assert result["item"]["lessons"] == []


def test_student_note_is_saved_and_returned(tmp_path, monkeypatch):
    module._db_path = tmp_path / "student-transfer.db"
    module._module_dir = tmp_path
    asyncio.run(module._init_db())

    async def allow(_request):
        return {"id": 7, "login": "operator", "display_name": "Оператор"}

    async def existing_student(_enrollment_id):
        return {"enrollment_id": "order:42"}

    monkeypatch.setattr(module, "_require_operator", allow)
    monkeypatch.setattr(module, "_student_by_id", existing_student)
    request = Request({
        "type": "http", "method": "PUT", "path": "/streams/students/order:42/note",
        "headers": [], "client": ("127.0.0.1", 1),
    })
    result = asyncio.run(module.save_student_note(
        "order:42", module.StudentNoteIn(note="  Telegram: @student\r\nПозвонить вечером  "), request,
    ))
    item = {"enrollment_id": "order:42"}
    asyncio.run(module._enrich_student_notes([item]))

    assert result["ok"] is True
    assert result["note"] == "Telegram: @student\nПозвонить вечером"
    assert result["updated_by"] == "Оператор"
    assert item["student_note"] == result["note"]
    assert item["student_note_updated_by"] == "Оператор"


def test_student_note_input_rejects_extra_fields():
    with pytest.raises(Exception):
        module.StudentNoteIn(note="ok", is_admin=True)


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


def test_module_connection_waits_for_short_write_lock(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        async with module._connect() as setup_db:
            await setup_db.execute("CREATE TABLE lock_test(value TEXT NOT NULL)")
            await setup_db.commit()
        holder = await aiosqlite.connect(module._must_db())
        await holder.execute("BEGIN IMMEDIATE")

        async def release():
            await asyncio.sleep(0.15)
            await holder.rollback()
            await holder.close()

        release_task = asyncio.create_task(release())
        async with module._connect() as db:
            await db.execute(
                "INSERT INTO lock_test(value) VALUES(?)",
                ("ok",),
            )
            await db.commit()
        await release_task

    asyncio.run(scenario())


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
    assert "GetCourse сейчас занят другой проверкой" in panel
    assert "Ничего делать не нужно — Nexus повторит сам" in panel
    assert "if(current?.ok)current.warning=message" in panel
    assert 'target_flow=(steps.get("preview") or {}).get("target") or {}' in (
        Path(__file__).parents[1] / "router.py"
    ).read_text(encoding="utf-8")


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
    assert "Проверяем доступы…" in panel
    assert "Проверить и применить" in panel
    assert "Проверяем и ставим изменения в очередь" in panel
    assert "Запрос принят" in panel
    assert 'id="toastStack"' in panel
    assert "Поток успешно создан" in panel
    assert "250 последних сообщений" in panel
    assert "Только администраторы" in panel
    assert "VK от группы" in panel
    assert "Намордник" in panel
    assert "Это окно можно закрыть" in panel
    assert "Лимит GetCourse API: показан последний снимок" not in panel
    assert "API ${data.requests_left_2h}" not in panel
    assert any(route.path == "/students/{enrollment_id}" for route in module.router.routes)


def test_fullscreen_panel_has_safe_flow_creation_and_locked_standard_homework():
    panel = (Path(__file__).parents[1] / "panel" / "app" / "index.html").read_text(encoding="utf-8")
    assert "/flows/preflight" in panel
    assert "Проверяем Google, Telegram, VK" in panel
    assert "С156 (13.08)" not in panel  # the UI must use the server-provided title, not one hard-coded flow
    assert "Создание ещё не закончено" in panel
    assert "Streams сам запишет VK-ссылку" in panel
    assert "Системные уведомления выключены" in panel
    assert "Открываем операцию создания потока…" in panel
    assert "flowJobAdminUrl(job)" in panel
    assert "if(!/^https?:\\/\\//i.test(raw))return''" in panel
    assert "homework-cell locked" in panel
    assert "Домашние задания не входят в тариф Стандарт" in panel
    assert 'title="Открыть карточку GetCourse"' in panel
    assert 'title="Открыть сделку amoCRM"' in panel
    assert "openMessenger(item)" in panel
    paths = {route.path for route in module.router.routes}
    assert "/flows/preflight" in paths
    assert "/flows/jobs/{job_id}/retry" in paths
    assert "/students/{enrollment_id}/messenger" in paths


def test_created_flow_is_persisted_without_google_catalog(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())
    job = {
        "course_key": "dog", "stream": "56", "date_start": "2026-08-13", "teacher_id": 4,
    }
    teacher = {"id": 4, "name": "Ирина", "offer_id": 8593080}
    created = {
        "telegram": {"group_link": "https://t.me/+dog56"},
        "vk": {
            "group_link": "https://vk.me/join/temporary",
            "owner_group_id": 225075265,
            "chat_id": 88,
        },
        "catalog": {"ok": True, "items": []},
    }

    result = asyncio.run(module._persist_created_flow(job, teacher, created))
    assert result["vk_admin_url"] == "https://vk.ru/gim225075265?sel=c88"
    with sqlite3.connect(module._must_db()) as db:
        row = db.execute(
            "SELECT teacher,teacher_code,curator_source,tg_link,vk_admin_url,status FROM flow_registry WHERE course_key='dog' AND stream='56'"
        ).fetchone()
    assert row == (
        "Ирина", "Куратор 1", "streams", "https://t.me/+dog56",
        "https://vk.ru/gim225075265?sel=c88", "draft",
    )

    asyncio.run(module._persist_created_flow(
        job, teacher, created, final_vk_link="https://vk.me/join/final", ready=True,
    ))
    with sqlite3.connect(module._must_db()) as db:
        ready = db.execute(
            "SELECT vk_link,status FROM flow_registry WHERE course_key='dog' AND stream='56'"
        ).fetchone()
    assert ready == ("https://vk.me/join/final", "ready")


def test_flow_job_ui_result_drops_heavy_sync_diagnostics():
    result = module._flow_job_ui_result(
        {
            "stages": {"manual_vk": "running"},
            "sync": {"reconciled": [{"email": f"student-{index}@example.com"} for index in range(1000)]},
            "create": {
                "vk": {"chat_id": 88, "owner_group_id": 225075265, "members_result": {"huge": "value"}},
                "catalog": {"items": [
                    {"course_key": "puppy", "stream": "56", "vk_admin_url": "https://vk/other"},
                    {"course_key": "dog", "stream": "56", "vk_admin_url": "https://vk/dog56"},
                ]},
            },
        },
        "dog",
        "56",
    )
    assert "sync" not in result
    assert "members_result" not in result["create"]["vk"]
    assert result["create"]["catalog"]["items"] == [
        {"course_key": "dog", "stream": "56", "vk_admin_url": "https://vk/dog56"}
    ]


def test_sheet_curator_cannot_override_streams_owned_flow(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"
    asyncio.run(module._init_db())

    async def scenario():
        now = module._now()
        async with aiosqlite.connect(module._must_db()) as db:
            await db.execute(
                """INSERT INTO flow_registry(
                   course_key,stream,course,teacher,teacher_code,curator_source,updated_at
                   ) VALUES('dog','56','Собака','Ирина','Куратор 1','streams',?)""",
                (now,),
            )
            await db.commit()
        result = await module._apply_sheet_curators(
            {"items": [{"course_key": "dog", "stream": "56", "curator_value": "Куратор 2", "students": []}]},
            set(),
            set(),
        )
        async with aiosqlite.connect(module._must_db()) as db:
            row = await (await db.execute(
                "SELECT teacher,teacher_code,curator_source FROM flow_registry WHERE course_key='dog' AND stream='56'"
            )).fetchone()
        return result, tuple(row)

    result, row = asyncio.run(scenario())
    assert result["flows"] == 0
    assert row == ("Ирина", "Куратор 1", "streams")


def test_access_verification_waits_for_getcourse(monkeypatch):
    calls = []

    async def view(identity, *, live, force=False):
        calls.append(("view", identity, live, force))
        return {"ok": True}

    monkeypatch.setattr(module, "_get_access_view", view)
    result = asyncio.run(module._get_access_after_write({"email": "student@example.com"}))
    assert result == {"ok": True}
    assert calls == [("view", {"email": "student@example.com"}, True, True)]


def test_access_operation_view_shows_grant_and_verification_progress():
    item = {
        "request_id": "request1",
        "requester_user_id": "7",
        "identifier": "student@example.com",
        "gc_user_id": "511",
        "status": "applied",
        "created_at": 1_700_000_000,
        "applied_at": 1_700_000_060,
        "current_groups": [{"name": "Курс"}],
        "target_groups": [{"name": "Курс"}, {"name": "2 модуль"}],
        "apply_result": {
            "verification_pending": True,
            "verification_attempts": 2,
            "verification_next_at": 1_700_000_120,
            "verification_error": "GetCourse обновляет список групп",
        },
    }
    view = module._access_operation_view(
        item,
        student={"name": "Ученица", "email": "student@example.com"},
        operator_name="Никита Попов",
    )
    assert view["status"] == "verifying"
    assert view["action"] == "access_grant"
    assert view["student_name"] == "Ученица"
    assert view["operator_name"] == "Никита Попов"
    assert view["added"] == ["2 модуль"]
    assert view["removed"] == []
    assert view["verification_attempts"] == 2
    assert view["next_check_at"].endswith("Z")

    item["apply_result"] = {"verification": {"verified": True}}
    complete = module._access_operation_view(item, compact=True)
    assert complete["status"] == "completed"
    assert "added" not in complete


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


def test_expired_registry_snapshot_is_returned_while_refresh_runs(monkeypatch):
    refreshed = asyncio.Event()

    async def registry_snapshot(*, refresh=False):
        await asyncio.sleep(0)
        refreshed.set()
        return {"ok": True, "items": [{"stream": "58"}]}

    monkeypatch.setattr(module, "_registry_snapshot", registry_snapshot)
    module._snapshot_cache.update(data={"ok": True, "items": [{"stream": "57"}]}, expires_at=0.0)
    module._snapshot_refresh_task = None

    async def scenario():
        assert await module._snapshot() == {"ok": True, "items": [{"stream": "57"}]}
        await asyncio.wait_for(refreshed.wait(), timeout=1)
        await module._snapshot_refresh_task
        assert await module._snapshot() == {"ok": True, "items": [{"stream": "58"}]}

    asyncio.run(scenario())
    module._snapshot_refresh_task = None
    module._clear_snapshot_cache()


def test_student_stream_display_always_has_course_prefix():
    puppy = module._student_result(
        {"course_key": "puppy", "course": "Щенок", "stream": "57"},
        {"enrollment_id": "p", "email": "p@example.com"},
    )
    dog = module._student_result(
        {"course_key": "dog", "course": "Собака", "stream": "С55"},
        {"enrollment_id": "d", "email": "d@example.com"},
    )
    combo = module._student_result(
        {"course_key": "dog", "course": "Собака", "stream": "55"},
        {
            "enrollment_id": "c",
            "email": "c@example.com",
            "course_assignments": [
                {"course_key": "puppy", "stream": "Щ58"},
                {"course_key": "dog", "stream": "55"},
            ],
        },
    )

    assert puppy["stream_display"] == "Щ57"
    assert dog["stream_display"] == "С55"
    assert combo["stream_display"] == "Щ58 / С55"


def test_sheet_row_payload_keeps_purchase_date():
    result = module._student_result(
        {"course_key": "puppy", "course": "Щенок", "stream": "57"},
        {"enrollment_id": "x", "email": "x@example.com", "date": "2026-08-02T17:59:53Z"},
    )
    assert result["date"] == "2026-08-02T17:59:53Z"
