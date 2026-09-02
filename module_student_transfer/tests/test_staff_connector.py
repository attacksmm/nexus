import asyncio
import json

import aiosqlite

from module_student_transfer import router as module


def test_staff_connector_password_is_write_only_idempotent_and_deactivation_preserves_operator(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"

    async def scenario():
        await module._init_db()
        employee = {
            "id": "employee-streams-1",
            "full_name": "Ольга Реестрова",
            "display_name": "Ольга",
            "roles": ["curator"],
            "identities": [],
            "source_links": {},
        }
        created = await module.service_staff_apply(
            employee=employee,
            config={
                "login": "olga.registry",
                "display_name": "Ольга",
                "active": True,
                "password": "first-secret",
                "curator": True,
            },
            operation="upsert",
            idempotency_key="streams-create-1",
        )
        operator_id = int(created["local_id"])
        async with module._connect() as db:
            stored_hash = (await (await db.execute(
                "SELECT password_hash FROM operators WHERE id=?", (operator_id,)
            )).fetchone())[0]
            await db.execute(
                "INSERT INTO sessions(token,operator_id,expires_at,created_at) VALUES(?,?,?,?)",
                ("registry-session", operator_id, "2099-01-01T00:00:00Z", module._now()),
            )
            await db.commit()
        replay = await module.service_staff_apply(
            employee=employee,
            config={"login": "olga.registry", "password": "second-secret", "curator": True},
            operation="upsert",
            idempotency_key="streams-create-1",
        )
        async with module._connect() as db:
            replay_hash = (await (await db.execute(
                "SELECT password_hash FROM operators WHERE id=?", (operator_id,)
            )).fetchone())[0]
            session_count = (await (await db.execute(
                "SELECT COUNT(*) FROM sessions WHERE operator_id=?", (operator_id,)
            )).fetchone())[0]
        linked_employee = {
            **employee,
            "source_links": {module.MODULE_ID: {"local_id": str(operator_id)}},
        }
        deactivated = await module.service_staff_apply(
            employee=linked_employee,
            config={},
            operation="deactivate",
            idempotency_key="streams-deactivate-1",
        )
        items = await module.service_staff_list()
        async with aiosqlite.connect(module._db_path) as db:
            persisted = await (await db.execute(
                "SELECT active,password_hash FROM operators WHERE id=?", (operator_id,)
            )).fetchone()
            sessions_after = (await (await db.execute(
                "SELECT COUNT(*) FROM sessions WHERE operator_id=?", (operator_id,)
            )).fetchone())[0]
        return created, stored_hash, replay, replay_hash, session_count, deactivated, items, persisted, sessions_after

    created, stored_hash, replay, replay_hash, session_count, deactivated, items, persisted, sessions_after = asyncio.run(scenario())
    assert created["changed"] is True
    assert created["config"]["curator"] is True
    assert created["snapshot"]["role_metadata"]["flow_assignments_managed"] is False
    assert created["warnings"]
    assert "password" not in json.dumps(created).casefold()
    assert stored_hash and stored_hash != "first-secret"
    assert replay["changed"] is False
    assert replay["idempotent_replay"] is True
    assert replay_hash == stored_hash
    assert session_count == 1
    assert deactivated["changed"] is True
    assert deactivated["config"]["active"] is False
    assert persisted[0] == 0
    assert persisted[1] == stored_hash
    assert sessions_after == 0
    serialized = json.dumps(items).casefold()
    assert "password_hash" not in serialized
    assert "first-secret" not in serialized


def test_staff_connector_does_not_match_display_name(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"

    async def scenario():
        await module._init_db()
        created = await module.service_staff_apply(
            employee={"id": "one", "display_name": "Точный дубль", "identities": [], "source_links": {}},
            config={"login": "exact.one", "display_name": "Точный дубль", "password": "exact-password"},
            operation="upsert",
        )
        missing = await module.service_staff_snapshot(employee={
            "id": "two", "display_name": "Точный дубль", "identities": [], "source_links": {},
        })
        linked = await module.service_staff_snapshot(employee={
            "id": "one", "identities": [], "source_links": {module.MODULE_ID: created["local_id"]},
        })
        return missing, linked

    missing, linked = asyncio.run(scenario())
    assert missing["found"] is False
    assert linked["found"] is True
    assert module.service_staff_connector()["role_metadata"]["curator"]["flow_assignments_managed"] is False


def test_staff_connector_requests_transient_password_for_new_operator(tmp_path):
    module._db_path = tmp_path / "student-transfer.db"

    async def scenario():
        await module._init_db()
        result = await module.service_staff_apply(
            employee={"id": "new", "display_name": "Новый оператор", "identities": [], "source_links": {}},
            config={"login": "new.operator"},
            operation="upsert",
        )
        async with module._connect() as db:
            count = (await (await db.execute(
                "SELECT COUNT(*) FROM operators WHERE login_key=?", (module._norm("new.operator"),)
            )).fetchone())[0]
        return result, count

    result, count = asyncio.run(scenario())
    assert result["ok"] is False
    assert result["needs_input"] == "password"
    assert "password_hash" not in json.dumps(result).casefold()
    assert count == 0
