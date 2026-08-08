import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("messenger_identity_graph", MODULE_DIR / "identity_graph.py")
graph = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(graph)


class IdentityGraphTests(unittest.TestCase):
    def test_disk_index_resolves_related_accounts_and_variables(self):
        with tempfile.TemporaryDirectory() as temp:
            customer_db = Path(temp) / "customer.db"
            index_db = Path(temp) / "identity-index.db"
            with sqlite3.connect(customer_db) as db:
                for table in ("cdb_getcourse_orders", "cdb_vk_clients", "cdb_amo_deals"):
                    db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)")
                db.execute(
                    "INSERT INTO cdb_getcourse_orders VALUES(1,'gc-order-1',?,?,?)",
                    (json.dumps({"name":"Анна Петрова","phone":"+79990001122","utm_term":"platform_id=vk-77"}), "2026-01-01", "2026-01-02"),
                )
                db.execute(
                    "INSERT INTO cdb_vk_clients VALUES(1,'vk-77',?,?,?)",
                    (json.dumps({"name":"Анна VK","phone":"+79990001122"}), "2026-01-01", "2026-01-03"),
                )
                db.execute(
                    "INSERT INTO cdb_amo_deals VALUES(1,'amo-42',?,?,?)",
                    (json.dumps({"contact_name":"Анна Петрова","phone":"+79990001122","utm_source":"vk"}), "2026-01-01", "2026-01-04"),
                )
                db.commit()

            index = graph.IdentityIndex(customer_db, index_db)
            built = index.build_if_changed(force=True)
            self.assertEqual(built["status"], "rebuilt")
            self.assertEqual(built["records"], 3)
            result = index.resolve({
                "platform":"amocrm", "entity_type":"lead", "entity_id":"amo-42",
                "name":"Анна Петрова", "phone":"+79990001122", "fields":{"utm_source":"vk"},
            })
            self.assertEqual(result["status"], "resolved")
            self.assertEqual(result["variables"]["contact.name"]["value"], "Анна Петрова")
            self.assertEqual(result["variables"]["utm.source"]["value"], "vk")
            self.assertEqual({item["service"] for item in result["accounts"]}, {"getcourse_order", "vk", "amo"})

    def test_template_replaces_missing_variables_with_empty_text(self):
        rendered = graph.render_template("Привет, {{contact.name}} — {{unknown}}", {"contact.name":{"value":"Анна"}})
        self.assertEqual(rendered["text"], "Привет, Анна — ")
        self.assertEqual(rendered["missing"], ["unknown"])
        self.assertTrue(rendered["ready"])

    def test_amocrm_markers_work_in_shared_templates(self):
        current = graph.build_variables([], {
            "platform": "amocrm", "entity_type": "lead", "entity_id": "42",
            "name": "Анна", "manager_name": "Никита",
            "fields": {"contact_id": "99", "responsible_user_id": "7", "lead.123": "VIP"},
        })
        self.assertEqual(current["name"]["value"], "Анна")
        self.assertEqual(current["lead.id"]["value"], "42")
        self.assertEqual(current["contact.id"]["value"], "99")
        self.assertEqual(current["lead.cf.123"]["value"], "VIP")
        self.assertEqual(current["lead.responsible.name"]["value"], "Никита")

        related = graph.build_variables([{
            "service": "amo", "platform_id": "42", "fields": {
                "responsible_user_id": "7",
                "custom_fields_values": [{"field_id": 123, "values": [{"value": "VIP"}]}],
            },
        }], {"platform": "getcourse", "entity_type": "user", "entity_id": "500", "name": "Анна"})
        self.assertEqual(related["lead.id"]["value"], "42")
        self.assertEqual(related["lead.cf.123"]["value"], "VIP")

    def test_crm_utm_requires_exact_contact_and_ignores_possible_accounts(self):
        with tempfile.TemporaryDirectory() as temp:
            customer_db = Path(temp) / "customer.db"
            index_db = Path(temp) / "identity-index.db"
            with sqlite3.connect(customer_db) as db:
                for table in ("cdb_getcourse_users", "cdb_amo_deals"):
                    db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)")
                db.execute("INSERT INTO cdb_getcourse_users VALUES(1,'500',?,?,?)", (json.dumps({"phone":"79227784154","email":"t@example.test"}), "", "3"))
                db.execute("INSERT INTO cdb_amo_deals VALUES(1,'10',?,?,?)", (json.dumps({"contact_fields":{"phone":"+79227784154"},"utms":{"utm_term":"939964780","utm_source":"instagram"}}), "", "3"))
                db.execute("INSERT INTO cdb_amo_deals VALUES(2,'11',?,?,?)", (json.dumps({"contact_fields":{"phone":"+79990000000"},"possible_accounts":{"phone":"+79227784154","utm_term":"wrong"},"utms":{"utm_term":"467144030"}}), "", "4"))
                db.commit()
            index = graph.IdentityIndex(customer_db, index_db)
            self.assertEqual(index.build_if_changed(force=True)["status"], "rebuilt")
            result = index.resolve({"platform":"getcourse", "entity_id":"500", "phone":"79227784154"})
            self.assertEqual(result["variables"]["utm.term"]["value"], "939964780")
            self.assertEqual(result["variables"]["utm.source"]["value"], "instagram")

    def test_crm_utm_is_empty_without_exact_crm_contact(self):
        current = graph.build_variables(
            [{"service":"amo", "platform_id":"wrong", "fields":{"utm_term":"467144030"}}],
            {"platform":"getcourse", "entity_id":"500"},
            {},
        )
        self.assertNotIn("utm.term", current)

    def test_failed_provider_lookup_is_not_an_identity(self):
        tokens = graph.identity_tokens("getcourse", "500", {
            "salebot_id": '{"status":"not_found"}',
            "getcourse_user_id": "none",
            "phone": "+79227784154",
        })
        self.assertNotIn('salebot:{"status":"not_found"}', tokens)
        self.assertEqual({value for value in tokens if value.startswith("phone:")}, {"phone:+79227784154"})

    def test_service_platform_lookup_does_not_confuse_vk_and_telegram_ids(self):
        with tempfile.TemporaryDirectory() as temp:
            customer_db = Path(temp) / "customer.db"
            index_db = Path(temp) / "identity-index.db"
            with sqlite3.connect(customer_db) as db:
                for table in ("cdb_vk_clients", "cdb_telegram_clients"):
                    db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)")
                db.execute("INSERT INTO cdb_vk_clients VALUES(1,'215204074','{}','','')")
                db.execute("INSERT INTO cdb_telegram_clients VALUES(1,'789663225','{}','','')")
                db.commit()
            index = graph.IdentityIndex(customer_db, index_db)
            self.assertEqual(index.build_if_changed(force=True)["status"], "rebuilt")
            self.assertEqual(index.platform_id_for_service("vk", "215204074"), "215204074")
            self.assertEqual(index.platform_id_for_service("vk", "789663225"), "")

    def test_provider_lookup_survives_unrelated_identity_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            customer_db = Path(temp) / "customer.db"
            index_db = Path(temp) / "identity-index.db"
            with sqlite3.connect(customer_db) as db:
                for table in ("cdb_getcourse_orders", "cdb_vk_clients", "cdb_amo_deals"):
                    db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)")
                db.execute("INSERT INTO cdb_getcourse_orders VALUES(1,'13777',?,?,?)", (json.dumps({"gc_user_id":"511750277","phone":"+79885611989"}), "", ""))
                db.execute("INSERT INTO cdb_vk_clients VALUES(1,'685881742',?,?,?)", (json.dumps({"phone":"+79885611989"}), "", ""))
                db.execute("INSERT INTO cdb_amo_deals VALUES(1,'17726289',?,?,?)", (json.dumps({"phone":"+79895438272"}), "", ""))
                db.commit()
            index = graph.IdentityIndex(customer_db, index_db)
            self.assertEqual(index.build_if_changed(force=True)["status"], "rebuilt")
            context = {
                "platform": "getcourse", "entity_id": "511750277",
                "getcourse_user_id": "511750277", "phone": "+79895438272",
                "fields": {"getcourse_user_id": "511750277"},
            }
            self.assertEqual(index.resolve(context)["status"], "conflict")
            self.assertEqual(index.platform_id_for_context("vk", context), "685881742")

    def test_exact_card_provider_lookup_uses_bare_utm_without_merged_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            customer_db = Path(temp) / "customer.db"
            index_db = Path(temp) / "identity-index.db"
            with sqlite3.connect(customer_db) as db:
                for table in ("cdb_getcourse_users", "cdb_amo_deals", "cdb_vk_clients", "cdb_telegram_clients"):
                    db.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY,platform_id TEXT,custom_fields TEXT,created_at TEXT,updated_at TEXT)")
                db.execute("INSERT INTO cdb_getcourse_users VALUES(1,'509380639',?,?,?)", (json.dumps({"phone":"79519349313","utm_term":"491847957"}), "", "3"))
                db.execute("INSERT INTO cdb_getcourse_users VALUES(2,'464555326',?,?,?)", (json.dumps({"phone":"79645552554","utm_term":"778155898"}), "", "3"))
                db.execute("INSERT INTO cdb_amo_deals VALUES(1,'18152963',?,?,?)", (json.dumps({"phone":"79788565190","utms":{"utm_term":"1001756083"}}), "", "3"))
                db.execute("INSERT INTO cdb_vk_clients VALUES(1,'491847957','{}','','2')")
                db.execute("INSERT INTO cdb_telegram_clients VALUES(1,'1956029416',?,?,?)", (json.dumps({"salebot_id":"1001756083"}), "", "2"))
                db.execute("INSERT INTO cdb_telegram_clients VALUES(2,'653335302',?,?,?)", (json.dumps({"possible_accounts":{"salebot_id":"778155898"}}), "", "2"))
                db.commit()
            index = graph.IdentityIndex(customer_db, index_db)
            self.assertEqual(index.build_if_changed(force=True)["status"], "rebuilt")
            self.assertEqual(index.provider_id_for_exact_context("vk", {"service":"getcourse","entity_id":"509380639"}), "491847957")
            self.assertEqual(index.provider_id_for_exact_context("salebot", {"service":"amo","entity_id":"18152963"}), "1001756083")
            self.assertEqual(index.provider_id_for_exact_context("telegram", {"service":"amo","entity_id":"18152963"}), "1956029416")
            self.assertEqual(index.provider_id_for_exact_context("telegram", {"service":"getcourse","entity_id":"464555326"}), "653335302")


if __name__ == "__main__":
    unittest.main()
