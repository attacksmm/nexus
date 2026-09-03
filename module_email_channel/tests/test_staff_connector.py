import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("_nexus_mod_email_staff_connector", ROOT / "router.py")
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class Lifecycle:
    def create_task(self, coro, name=""):
        coro.close()


@pytest.fixture
def ready(tmp_path):
    asyncio.run(mod.setup(SimpleNamespace(db_path=tmp_path / "email.db", logger=None, lifecycle=Lifecycle())))
    return mod


def test_email_upsert_requires_explicit_messenger_link(ready):
    employee = {"id":"employee-1", "display_name":"Анна", "status":"active", "identities":[], "source_links":{}}
    with pytest.raises(ValueError, match="Messenger"):
        asyncio.run(ready.service_staff_apply(employee=employee, config={}, operation="upsert"))

    async def count_profiles():
        db = await ready._connect()
        try:
            return (await (await db.execute("SELECT COUNT(*) FROM sender_profiles")).fetchone())[0]
        finally:
            await db.close()
    assert asyncio.run(count_profiles()) == 0


def test_email_profile_follows_messenger_id_and_deactivation_preserves_history(ready):
    async def run():
        employee = {
            "id":"employee-2", "full_name":"Татьяна Воробьева", "display_name":"Татьяна Воробьева",
            "status":"active", "identities":[], "source_links":{"messenger-widget":{"local_id":"42"}},
        }
        first = await ready.service_staff_apply(
            employee=employee, config={"local_part":"tatiana.sales", "enabled":True},
            operation="upsert", idempotency_key="email-42-v1",
        )
        assert first["local_id"] == "42"
        assert first["snapshot"]["email"].startswith("tatiana.sales@")
        assert first["changed"] is True
        employee["source_links"]["email-channel"] = {"local_id":"42"}
        again = await ready.service_staff_apply(
            employee=employee, config={"local_part":"tatiana.sales", "enabled":True},
            operation="upsert", idempotency_key="email-42-v1",
        )
        assert again["changed"] is False

        db = await ready._connect()
        try:
            now = ready._iso()
            mailbox = await (await db.execute("SELECT id FROM mailboxes LIMIT 1")).fetchone()
            thread = await db.execute(
                "INSERT INTO email_threads(public_token,mailbox_id,client_email,created_at,updated_at) VALUES(?,?,?,?,?)",
                ("history-token", mailbox["id"], "client@example.com", now, now),
            )
            await db.execute(
                """INSERT INTO email_messages(
                   thread_id,nexus_message_id,direction,status,from_email,to_email,subject,manager_id,manager_name,
                   created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (int(thread.lastrowid), "history-message", "outgoing", "delivered",
                 "tatiana.sales@support.sobakovod.pro", "client@example.com", "История", "42",
                 "Татьяна Воробьева", now, now),
            )
            await db.commit()
        finally:
            await db.close()

        disabled = await ready.service_staff_apply(
            employee=employee, config={}, operation="deactivate", idempotency_key="email-42-off",
        )
        assert disabled["snapshot"]["status"] == "inactive"
        db = await ready._connect()
        try:
            profile_count = (await (await db.execute(
                "SELECT COUNT(*) FROM sender_profiles WHERE manager_id='42'",
            )).fetchone())[0]
            history_count = (await (await db.execute(
                "SELECT COUNT(*) FROM email_messages WHERE manager_id='42'",
            )).fetchone())[0]
        finally:
            await db.close()
        assert profile_count == 1
        assert history_count == 1
    asyncio.run(run())


def test_email_list_is_read_only_and_contains_only_profiles(ready):
    async def run():
        employee = {
            "id":"employee-3", "display_name":"Мария", "status":"active", "identities":[],
            "source_links":{"messenger-widget":"77"},
        }
        await ready.service_staff_apply(employee=employee, config={"local_part":"maria"}, operation="upsert")
        rows = await ready.service_staff_list()
        assert rows == [{
            "module_id":"email-channel", "local_id":"77", "full_name":"Мария", "display_name":"Мария",
            "identities":[{"provider":"messenger-widget", "external_id":"77"}],
            "config":{"local_part":"maria", "enabled":True, "manager_id":"77"}, "active":True,
        }]
        assert "messages" not in rows[0]
        assert "credentials" not in rows[0]
    asyncio.run(run())
