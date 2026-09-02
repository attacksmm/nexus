import asyncio

import aiosqlite

from module_sales_chats import router as module


def test_staff_connector_exact_identity_idempotence_and_soft_deactivation(tmp_path):
    async def scenario():
        module._db_path = tmp_path / "sales-chats.db"
        module._module_dir = tmp_path / "sales-chats"
        await module._init_db()

        first = await module.service_staff_apply(
            employee={"id": "employee-1", "full_name": "Анна Смирнова", "source_links": {}},
            config={"login": "anna", "display_name": "Анна", "active": True},
            operation="upsert",
            idempotency_key="create-1",
        )
        assert first["operation"] == "upsert"
        assert first["result"] == "created"
        assert first["changed"] is True

        # A display-name match must never be used as an identity match.
        second = await module.service_staff_apply(
            employee={"id": "employee-2", "full_name": "Анна", "source_links": {}},
            config={"login": "anna.second", "display_name": "Анна", "active": True},
            operation="upsert",
        )
        assert second["local_id"] != first["local_id"]

        linked = {"id": "employee-1", "source_links": {module.MODULE_ID: first["local_id"]}}
        repeat = await module.service_staff_apply(
            employee=linked,
            config={"login": "anna", "display_name": "Анна", "active": True},
            operation="upsert",
            idempotency_key="create-1",
        )
        assert repeat["changed"] is False

        async with module._connect() as db:
            now = module._now()
            thread = await db.execute(
                "INSERT INTO threads(channel,recipient_id,created_at,updated_at) VALUES('vk','42',?,?)",
                (now, now),
            )
            await db.execute(
                "INSERT INTO messages(thread_id,channel,direction,account_id,created_at) VALUES(?,?,?,?,?)",
                (int(thread.lastrowid), "vk", "out", int(first["local_id"]), now),
            )
            await db.execute(
                "INSERT INTO sessions(token,account_id,expires_at,created_at) VALUES('session',?,?,?)",
                (int(first["local_id"]), "2999-01-01T00:00:00Z", now),
            )
            await db.commit()

        deactivated = await module.service_staff_apply(
            employee=linked,
            config={},
            operation="delete",
        )
        assert deactivated["operation"] == "deactivate"
        assert deactivated["snapshot"]["status"] == "inactive"
        async with aiosqlite.connect(module._must_db()) as db:
            assert (await (await db.execute("SELECT COUNT(*) FROM accounts WHERE id=?", (int(first["local_id"]),))).fetchone())[0] == 1
            assert (await (await db.execute("SELECT COUNT(*) FROM messages WHERE account_id=?", (int(first["local_id"]),))).fetchone())[0] == 1
            assert (await (await db.execute("SELECT COUNT(*) FROM sessions WHERE account_id=?", (int(first["local_id"]),))).fetchone())[0] == 0

        exported = await module.service_staff_list()
        exported_account = next(item for item in exported if item["local_id"] == first["local_id"])
        assert exported_account["active"] is False
        assert set(exported_account) == {
            "module_id", "local_id", "full_name", "display_name", "identities", "config", "active"
        }

    asyncio.run(scenario())


def test_staff_connector_descriptor_declares_soft_deactivation():
    descriptor = module.service_staff_connector()
    assert descriptor["identity"]["match"] == "exact"
    assert descriptor["deactivation"]["preserves_history"] is True
