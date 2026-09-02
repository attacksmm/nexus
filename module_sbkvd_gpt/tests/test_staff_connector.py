import asyncio

import aiosqlite
import pytest

from module_sbkvd_gpt import router as module


def test_staff_connector_manages_access_and_preserves_history(tmp_path):
    async def scenario():
        module._db_path = tmp_path / "sbkvd-gpt.db"
        module._module_dir = tmp_path / "sbkvd-gpt"
        await module._init_db()

        created = await module.service_staff_apply(
            employee={"id": "employee-1", "full_name": "Мария", "source_links": {}},
            config={
                "login": "maria",
                "display_name": "Мария",
                "active": True,
                "models": ["openai/gpt-4.1-mini", "openai/gpt-5.2"],
                "default_model": "openai/gpt-4.1-mini",
            },
            operation="upsert",
        )
        assert created["operation"] == "upsert"
        assert created["result"] == "created"
        assert created["config"]["models"] == ["openai/gpt-4.1-mini", "openai/gpt-5.2"]
        local_id = int(created["local_id"])

        linked = {"id": "employee-1", "source_links": {module.MODULE_ID: {"local_id": str(local_id)}}}
        repeated = await module.service_staff_apply(
            employee=linked,
            config={
                "login": "maria",
                "display_name": "Мария",
                "active": True,
                "models": ["openai/gpt-5.2", "openai/gpt-4.1-mini"],
                "default_model": "openai/gpt-4.1-mini",
            },
            operation="upsert",
        )
        assert repeated["changed"] is False

        async with aiosqlite.connect(module._must_db()) as db:
            now = module._now()
            await db.execute(
                "INSERT INTO threads(id,account_id,created_at,updated_at) VALUES('thread-1',?,?,?)",
                (local_id, now, now),
            )
            await db.execute(
                "INSERT INTO messages(thread_id,account_id,role,content,created_at) VALUES('thread-1',?,'user','hello',?)",
                (local_id, now),
            )
            await db.execute(
                "INSERT INTO sessions(token,account_id,expires_at,created_at) VALUES('session',?,?,?)",
                (local_id, "2999-01-01T00:00:00Z", now),
            )
            await db.commit()

        deactivated = await module.service_staff_apply(employee=linked, config={}, operation="remove")
        assert deactivated["operation"] == "deactivate"
        assert deactivated["snapshot"]["status"] == "inactive"
        async with aiosqlite.connect(module._must_db()) as db:
            assert (await (await db.execute("SELECT COUNT(*) FROM accounts WHERE id=?", (local_id,))).fetchone())[0] == 1
            assert (await (await db.execute("SELECT COUNT(*) FROM threads WHERE account_id=?", (local_id,))).fetchone())[0] == 1
            assert (await (await db.execute("SELECT COUNT(*) FROM messages WHERE account_id=?", (local_id,))).fetchone())[0] == 1
            assert (await (await db.execute("SELECT COUNT(*) FROM sessions WHERE account_id=?", (local_id,))).fetchone())[0] == 0

        exported = next(item for item in await module.service_staff_list() if item["local_id"] == str(local_id))
        assert exported["config"]["default_model"] == "openai/gpt-4.1-mini"
        assert "created_at" not in exported

    asyncio.run(scenario())


def test_staff_connector_rejects_inaccessible_default_model(tmp_path):
    async def scenario():
        module._db_path = tmp_path / "sbkvd-gpt.db"
        module._module_dir = tmp_path / "sbkvd-gpt"
        await module._init_db()
        with pytest.raises(ValueError, match="default_model"):
            await module.service_staff_apply(
                employee={"id": "employee-1", "source_links": {}},
                config={"login": "maria", "models": ["openai/gpt-5.2"], "default_model": "openai/gpt-4.1-mini"},
                operation="upsert",
            )

    asyncio.run(scenario())
