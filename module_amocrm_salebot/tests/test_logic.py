import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from module_amocrm_salebot import router


class AmoCrmSaleBotLogicTests(unittest.IsolatedAsyncioTestCase):
    def test_parses_flat_amocrm_webhook(self) -> None:
        payload = {
            "leads[update][0][id]": "17974383",
            "leads[update][0][status_id]": "66041242",
            "leads[responsible][0][id]": "17974383",
        }
        self.assertEqual(
            [
                {"action": "update", "deal_id": "17974383"},
                {"action": "responsible", "deal_id": "17974383"},
            ],
            router._lead_events(payload),
        )

    def test_builds_legacy_variables_and_all_custom_fields(self) -> None:
        deal = {
            "id": 17974383,
            "name": "Тестовая сделка",
            "price": 13900,
            "responsible_user_id": 6295879,
            "pipeline_id": 8863914,
            "status_id": 66368622,
            "created_at": 1784100000,
            "updated_at": 1784100100,
            "custom_fields_values": [
                {"field_code": "UTM_TERM", "field_name": "utm_term", "values": [{"value": "988380293"}]},
                {"field_code": None, "field_name": "Был минут", "values": [{"value": 74}]},
            ],
            "_embedded": {"contacts": [{"id": 42, "is_main": True}]},
        }
        variables = router._build_variables(deal, dict(router.DEFAULT_SETTINGS))
        self.assertEqual("17974383", variables["amo_deal_id"])
        self.assertEqual("988380293", variables["amo_utm_term"])
        self.assertEqual("74", variables["amo_был_минут"])
        self.assertEqual("42", variables["amo_contacts"])
        self.assertEqual("42", variables["amo_main_contact"])
        self.assertIn("СДЕЛКА #17974383", variables["amo_deal_info"])

    async def test_customer_db_requires_explicit_salebot_field_and_ignores_amo_deals_guess(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "customer-db.db"
            with sqlite3.connect(path) as db:
                for table in ("cdb_amo_deals", "cdb_avito_clients"):
                    db.execute(
                        f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT)"
                    )
                db.execute(
                    "INSERT INTO cdb_amo_deals(platform_id,custom_fields) VALUES(?,?)",
                    ("1", json.dumps({"possible_accounts": {"salebot_id": "111111"}})),
                )
                db.execute(
                    "INSERT INTO cdb_avito_clients(platform_id,custom_fields) VALUES(?,?)",
                    ("avito-1", json.dumps({"possible_accounts": {"salebot_id": "222222"}})),
                )
            with patch.dict(os.environ, {"AMO_SALEBOT_CUSTOMER_DB_PATH": str(path)}):
                guessed, _, _ = await router._identity_from_customer_db("111111")
                verified, source, details = await router._identity_from_customer_db("222222")
            self.assertFalse(guessed)
            self.assertTrue(verified)
            self.assertEqual("customer_db", source)
            self.assertEqual("cdb_avito_clients", details["table"])

    async def test_openrouter_salebot_message_confirms_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "openrouter.db"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY,platform_id TEXT,source TEXT,conversation_id TEXT,created_at TEXT)")
                db.execute("CREATE TABLE outbound_jobs(id INTEGER PRIMARY KEY,source TEXT,payload_json TEXT,created_at TEXT)")
                db.execute(
                    "INSERT INTO messages(platform_id,source,conversation_id,created_at) VALUES(?,?,?,?)",
                    ("988380293", "salebot", "conv-1", "2026-07-15T12:00:00Z"),
                )
            with patch.dict(os.environ, {"AMO_SALEBOT_OPENROUTER_DB_PATH": str(path)}):
                verified, source, details = await router._identity_from_openrouter("988380293")
            self.assertTrue(verified)
            self.assertEqual("openrouter_messages", source)
            self.assertEqual("conv-1", details["conversation_id"])

    async def test_strict_mode_does_not_use_salebot_api_as_identity_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            module_db = Path(tmp) / "module.db"
            customer_db = Path(tmp) / "missing-customer.db"
            openrouter_db = Path(tmp) / "missing-openrouter.db"
            previous_db = router._db_path
            router._db_path = str(module_db)
            try:
                await router._init_db()
                settings = dict(router.DEFAULT_SETTINGS)
                settings["identity_mode"] = "strict"
                with patch.dict(
                    os.environ,
                    {
                        "AMO_SALEBOT_CUSTOMER_DB_PATH": str(customer_db),
                        "AMO_SALEBOT_OPENROUTER_DB_PATH": str(openrouter_db),
                    },
                ), patch.object(router, "_identity_from_salebot_api") as api_check:
                    verified, source, details = await router._resolve_identity("1105209997", settings)
                self.assertFalse(verified)
                self.assertEqual("", source)
                self.assertEqual("salebot_identity_not_confirmed", details["reason"])
                api_check.assert_not_awaited()
            finally:
                router._db_path = previous_db

    def test_same_snapshot_has_same_fingerprint(self) -> None:
        variables = {"amo_deal_id": "1", "amo_status": "2"}
        first = router._fingerprint("988380293", variables, "callback_amoCRM")
        second = router._fingerprint("988380293", dict(reversed(list(variables.items()))), "callback_amoCRM")
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
