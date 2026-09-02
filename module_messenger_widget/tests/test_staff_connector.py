import asyncio
import logging
import sys
import tempfile
import unittest
from pathlib import Path

import aiosqlite

from module_messenger_widget import router


class StaffConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="messenger-staff-")
        self.previous_db = router._db_path
        router._db_path = Path(self.tmp.name) / "module.db"
        router._logger = logging.getLogger("messenger-staff-tests")
        await router._init_db()

    async def asyncTearDown(self):
        router._db_path = self.previous_db
        self.tmp.cleanup()

    async def _insert_admin(self, wazzup_id: str, name: str) -> int:
        now = router._iso()
        db = await router._connect()
        try:
            cursor = await db.execute(
                """INSERT INTO admins(wazzup_user_id,name,enabled,created_at,updated_at)
                   VALUES(?,?,1,?,?)""",
                (wazzup_id, name, now, now),
            )
            await db.commit()
            return int(cursor.lastrowid)
        finally:
            await db.close()

    async def test_upsert_never_matches_an_existing_employee_by_name(self):
        unrelated_id = await self._insert_admin("legacy-anna", "Анна")
        employee = {
            "id": "employee-a",
            "full_name": "Анна",
            "display_name": "Анна",
            "phone": "+7 (999) 111-22-33",
            "status": "active",
            "identities": [
                {"provider": "getcourse", "external_id": "701", "email": "anna@example.com"},
                {"provider": "amocrm", "external_id": "801", "email": "anna@example.com"},
            ],
            "source_links": {},
        }
        result = await router.service_staff_apply(
            employee=employee,
            config={"role": "admin", "amo_task_enabled": False, "course_chat_notifications": True},
            operation="upsert",
            idempotency_key="staff-a-v1",
        )
        self.assertNotEqual(int(result["local_id"]), unrelated_id)
        self.assertTrue(result["changed"])
        self.assertEqual(result["snapshot"]["config"]["phone"], "+79991112233")
        self.assertEqual(result["snapshot"]["config"]["role"], "admin")
        self.assertEqual(
            [(row["platform"], row["platform_user_id"]) for row in result["snapshot"]["config"]["bindings"]],
            [("amocrm", "801"), ("getcourse", "701")],
        )

        employee["source_links"] = {"messenger-widget": {"local_id": result["local_id"]}}
        again = await router.service_staff_apply(
            employee=employee,
            config={"role": "admin", "amo_task_enabled": False, "course_chat_notifications": True},
            operation="upsert",
            idempotency_key="staff-a-v1",
        )
        self.assertFalse(again["changed"])
        db = await router._connect()
        try:
            count = (await (await db.execute("SELECT COUNT(*) FROM admins")).fetchone())[0]
        finally:
            await db.close()
        self.assertEqual(count, 2)

    async def test_deactivate_revokes_devices_but_keeps_employee_and_history(self):
        admin_id = await self._insert_admin("staff-b", "Борис")
        now = router._iso()
        db = await router._connect()
        try:
            cursor = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (admin_id, "hash-b", "hint", now, now, "2999-01-01T00:00:00Z"),
            )
            await db.execute(
                """INSERT INTO events(admin_id,device_id,action,status,created_at)
                   VALUES(?,?,?,'ok',?)""",
                (admin_id, int(cursor.lastrowid), "historical_action", now),
            )
            await db.commit()
        finally:
            await db.close()
        employee = {
            "id": "employee-b", "display_name": "Новое имя не применяется при отключении",
            "identities": [], "source_links": {"messenger-widget": str(admin_id)},
        }
        result = await router.service_staff_apply(
            employee=employee, config={}, operation="deactivate", idempotency_key="offboard-b",
        )
        self.assertTrue(result["changed"])
        self.assertEqual(result["snapshot"]["status"], "inactive")
        db = await router._connect()
        try:
            admin = await (await db.execute("SELECT name,enabled FROM admins WHERE id=?", (admin_id,))).fetchone()
            revoked = await (await db.execute("SELECT revoked_at FROM devices WHERE admin_id=?", (admin_id,))).fetchone()
            history = (await (await db.execute("SELECT COUNT(*) FROM events WHERE admin_id=?", (admin_id,))).fetchone())[0]
        finally:
            await db.close()
        self.assertEqual((admin["name"], admin["enabled"]), ("Борис", 0))
        self.assertTrue(revoked["revoked_at"])
        self.assertEqual(history, 1)

    async def test_list_exports_only_safe_staff_configuration(self):
        admin_id = await self._insert_admin("safe-user", "Светлана")
        now = router._iso()
        db = await router._connect()
        try:
            await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (admin_id, "secret-hash", "secret-hint", now, now, "2999-01-01T00:00:00Z"),
            )
            await db.commit()
        finally:
            await db.close()
        rows = await router.service_staff_list()
        self.assertEqual(rows[0]["local_id"], str(admin_id))
        self.assertNotIn("devices", rows[0])
        self.assertNotIn("token_hash", str(rows[0]))
        self.assertEqual(rows[0]["identities"][0], {"provider": "wazzup", "external_id": "safe-user"})

    async def test_central_access_issues_code_and_revokes_only_owned_device(self):
        admin_id = await self._insert_admin("access-user", "Алексей")
        other_id = await self._insert_admin("other-user", "Ольга")
        now = router._iso()
        db = await router._connect()
        try:
            own = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (admin_id, "own-hash", "own", now, now, "2999-01-01T00:00:00Z"),
            )
            other = await db.execute(
                """INSERT INTO devices(admin_id,token_hash,token_hint,created_at,last_used_at,expires_at)
                   VALUES(?,?,?,?,?,?)""",
                (other_id, "other-hash", "other", now, now, "2999-01-01T00:00:00Z"),
            )
            await db.commit()
        finally:
            await db.close()
        employee = {"source_links": {"messenger-widget": {"local_id": str(admin_id)}}, "identities": []}
        access = await router.service_staff_access(employee=employee)
        self.assertEqual([item["id"] for item in access["devices"]], [int(own.lastrowid)])
        issued = await router.service_staff_issue_activation_code(employee=employee)
        self.assertTrue(issued["code"])
        with self.assertRaisesRegex(ValueError, "не найдено"):
            await router.service_staff_revoke_device(employee=employee, device_id=int(other.lastrowid))
        revoked = await router.service_staff_revoke_device(employee=employee, device_id=int(own.lastrowid))
        self.assertEqual(revoked["device_id"], int(own.lastrowid))

    async def test_notification_recipients_are_managed_centrally(self):
        source_id = await self._insert_admin("route-source", "Источник")
        recipient_id = await self._insert_admin("route-recipient", "Получатель")
        employee = {
            "id": "route-employee", "full_name": "Источник", "display_name": "Источник",
            "status": "active", "identities": [],
            "source_links": {"messenger-widget": {"local_id": str(source_id)}},
        }
        result = await router.service_staff_apply(
            employee=employee,
            config={"role": "employee", "notification_recipient_ids": [str(recipient_id)]},
            operation="upsert",
        )
        self.assertEqual(result["snapshot"]["config"]["notification_recipient_ids"], [str(recipient_id)])
        self.assertTrue(result["snapshot"]["config"]["notification_routing_configured"])

        defaulted = await router.service_staff_apply(
            employee=employee,
            config={"role": "employee", "notification_recipient_ids": [], "notification_routing_configured": False},
            operation="upsert",
        )
        self.assertEqual(defaulted["snapshot"]["config"]["notification_recipient_ids"], [str(source_id)])

    def test_legacy_staff_mutations_are_locked_when_registry_is_loaded(self):
        previous = sys.modules.get(router.STAFF_REGISTRY_MODULE_NAME)
        sys.modules[router.STAFF_REGISTRY_MODULE_NAME] = object()
        try:
            with self.assertRaisesRegex(Exception, "едином реестре"):
                router._ensure_local_staff_mutation_allowed()
        finally:
            if previous is None:
                sys.modules.pop(router.STAFF_REGISTRY_MODULE_NAME, None)
            else:
                sys.modules[router.STAFF_REGISTRY_MODULE_NAME] = previous


if __name__ == "__main__":
    unittest.main()
