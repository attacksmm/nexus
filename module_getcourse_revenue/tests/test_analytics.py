from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from datetime import date
from pathlib import Path


try:
    import aiosqlite  # noqa: F401
    from fastapi import HTTPException
except ModuleNotFoundError:
    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class _APIRouter:
        def get(self, _path):
            return lambda function: function

        def post(self, _path):
            return lambda function: function

    class _Request:
        pass

    fastapi_stub = types.ModuleType("fastapi")
    fastapi_stub.APIRouter = _APIRouter
    fastapi_stub.HTTPException = HTTPException
    fastapi_stub.Request = _Request
    sys.modules["fastapi"] = fastapi_stub

    class _AsyncCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        async def fetchone(self):
            return self.cursor.fetchone()

        async def fetchall(self):
            return self.cursor.fetchall()

    class _AsyncConnection:
        def __init__(self, path, **kwargs):
            raw_path = str(path)
            if raw_path.startswith("file:"):
                raw_path = raw_path[5:].split("?", 1)[0]
            self.connection = sqlite3.connect(raw_path)

        @property
        def row_factory(self):
            return self.connection.row_factory

        @row_factory.setter
        def row_factory(self, value):
            self.connection.row_factory = value

        async def execute(self, sql, params=()):
            return _AsyncCursor(self.connection.execute(sql, params))

        async def executescript(self, sql):
            self.connection.executescript(sql)

        async def commit(self):
            self.connection.commit()

        async def __aenter__(self):
            return self

        async def __aexit__(self, _kind, _value, _traceback):
            self.connection.close()

    aiosqlite_stub = types.ModuleType("aiosqlite")
    aiosqlite_stub.Connection = _AsyncConnection
    aiosqlite_stub.Row = sqlite3.Row
    aiosqlite_stub.connect = lambda path, **kwargs: _AsyncConnection(path, **kwargs)
    sys.modules["aiosqlite"] = aiosqlite_stub

    httpx_stub = types.ModuleType("httpx")
    httpx_stub.AsyncClient = object
    sys.modules["httpx"] = httpx_stub

    orchestrator_stub = types.ModuleType("orchestrator")
    auth_stub = types.ModuleType("orchestrator.auth")
    auth_stub.can_access_module = lambda user, module_id: user.get("role") == "admin" or module_id in json.loads(user.get("module_access") or "[]")

    async def _no_user(_request):
        return None

    auth_stub.verify_token_from_request = _no_user
    sys.modules["orchestrator"] = orchestrator_stub
    sys.modules["orchestrator.auth"] = auth_stub

from module_getcourse_revenue import router


class RevenueAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.module_dir = self.root / "modules" / "getcourse-revenue"
        self.customer_db = self.root / "modules" / "customer-db" / "data" / "customer-db.db"
        self.orders_db = self.root / "modules" / "getcourse-orders" / "data" / "getcourse-orders.db"
        self.tracker_db = self.root / "modules" / "tracker" / "data" / "tracker.db"
        self.finance_db = self.module_dir / "data" / "getcourse-revenue.db"
        self.archive_db = self.customer_db.parent / "archive" / "customer-db-archive.db"
        for path in (self.customer_db, self.orders_db, self.tracker_db, self.archive_db, self.finance_db):
            path.parent.mkdir(parents=True, exist_ok=True)
        self.module_dir.mkdir(parents=True, exist_ok=True)
        router._module_dir = self.module_dir
        router._db_path = self.finance_db
        router._funnel_cache = {"built_at": time.monotonic(), "entities": []}
        os.environ.pop("GETCOURSE_REVENUE_CUSTOMER_DB_PATH", None)
        os.environ.pop("GETCOURSE_REVENUE_ORDERS_DB_PATH", None)
        os.environ.pop("GETCOURSE_REVENUE_TRACKER_DB_PATH", None)
        self._init_databases()
        asyncio.run(router._init_db())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _init_databases(self) -> None:
        with sqlite3.connect(self.customer_db) as db:
            db.executescript(
                """
                CREATE TABLE cdb_getcourse_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_id TEXT NOT NULL,
                    custom_fields TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE cdb_vk_clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_id TEXT NOT NULL,
                    custom_fields TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE cdb_telegram_clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_id TEXT NOT NULL,
                    custom_fields TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE cdb_amo_deals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform_id TEXT NOT NULL,
                    custom_fields TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
        with sqlite3.connect(self.orders_db) as db:
            db.execute(
                """
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    platform_id TEXT NOT NULL,
                    payment_state TEXT NOT NULL
                )
                """
            )
        with sqlite3.connect(self.archive_db) as db:
            db.execute(
                """
                CREATE TABLE archive_records (
                    table_name TEXT NOT NULL,
                    id INTEGER NOT NULL,
                    platform_id TEXT NOT NULL,
                    custom_fields TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    archived_at TEXT NOT NULL
                )
                """
            )
        with sqlite3.connect(self.tracker_db) as db:
            db.executescript(
                """
                CREATE TABLE profiles (
                    visit_id TEXT PRIMARY KEY,
                    first_utm_source TEXT NOT NULL DEFAULT '',
                    last_utm_source TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    created_ts REAL NOT NULL,
                    visit_id TEXT NOT NULL,
                    confirmed INTEGER NOT NULL DEFAULT 0,
                    utm_source TEXT NOT NULL DEFAULT ''
                );
                """
            )

    def _live(self, platform_id: str, fields: dict, created: str = "2026-07-01T00:00:00Z", updated: str = "2026-07-18T00:00:00Z") -> None:
        with sqlite3.connect(self.customer_db) as db:
            db.execute(
                "INSERT INTO cdb_getcourse_orders(platform_id,custom_fields,created_at,updated_at) VALUES(?,?,?,?)",
                (platform_id, json.dumps(fields, ensure_ascii=False), created, updated),
            )

    def _archive(self, platform_id: str, fields: dict, updated: str = "2026-07-10T00:00:00Z") -> None:
        with sqlite3.connect(self.archive_db) as db:
            db.execute(
                "INSERT INTO archive_records(table_name,id,platform_id,custom_fields,created_at,updated_at,archived_at) VALUES(?,?,?,?,?,?,?)",
                ("getcourse_orders", 1, platform_id, json.dumps(fields, ensure_ascii=False), "2026-06-01T00:00:00Z", updated, "2026-07-11T00:00:00Z"),
            )

    def _event(self, platform_id: str, state: str, received: str) -> None:
        with sqlite3.connect(self.orders_db) as db:
            db.execute(
                "INSERT INTO events(received_at,platform_id,payment_state) VALUES(?,?,?)",
                (received, platform_id, state),
            )

    def _finance(self, order_id: str, earned: float, paid: float = 0, commission: float = 0) -> None:
        with sqlite3.connect(self.finance_db) as db:
            db.execute(
                "INSERT INTO finance_cache(order_id,paid,earned,payment_commission,synced_at) VALUES(?,?,?,?,?)",
                (order_id, paid, earned, commission, "2026-07-18T10:00:00Z"),
            )

    def _registration(self, visit_id: str, source: str, created: str, *, confirmed: int = 1) -> None:
        timestamp = __import__("datetime").datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()
        with sqlite3.connect(self.tracker_db) as db:
            db.execute("INSERT OR IGNORE INTO profiles(visit_id,first_utm_source,last_utm_source) VALUES(?,?,?)", (visit_id, source, source))
            db.execute("INSERT INTO events(created_at,created_ts,visit_id,confirmed,utm_source) VALUES(?,?,?,?,?)", (created, timestamp, visit_id, confirmed, source))

    def _messenger_client(self, channel: str, platform_id: str, created: str, fields: dict | None = None) -> None:
        table = "cdb_vk_clients" if channel == "vk" else "cdb_telegram_clients"
        with sqlite3.connect(self.customer_db) as db:
            db.execute(
                f"INSERT INTO {table}(platform_id,custom_fields,created_at,updated_at) VALUES(?,?,?,?)",
                (platform_id, json.dumps(fields or {}, ensure_ascii=False), created, created),
            )

    def _amo_deal(self, platform_id: str, pipeline: str, status: str, email: str) -> None:
        fields = {
            "pipeline_name": pipeline,
            "status_name": status,
            "email": email,
            "created_at_ts": "2026-07-17T10:00:00+03:00",
            "closed_at_ts": "2026-07-17T12:00:00+03:00",
        }
        with sqlite3.connect(self.customer_db) as db:
            db.execute(
                "INSERT INTO cdb_amo_deals(platform_id,custom_fields,created_at,updated_at) VALUES(?,?,?,?)",
                (platform_id, json.dumps(fields, ensure_ascii=False), "2026-07-17T07:00:00Z", "2026-07-17T09:00:00Z"),
            )

    def _run(self, payload: dict) -> dict:
        return asyncio.run(router._build_analytics(payload, today=date(2026, 7, 18)))

    def test_paid_revenue_uses_first_paid_event_and_moscow_day(self) -> None:
        self._live("paid-1", {"payment_state": "paid", "payed_money": 1250.5, "cost_money": 1800})
        self._event("paid-1", "paid", "2026-07-17T21:30:00Z")
        self._event("paid-1", "paid", "2026-07-18T08:00:00Z")

        result = self._run({"preset": "7d", "statuses": ["paid"], "amount_basis": "paid"})

        point = next(item for item in result["points"] if item["date"] == "2026-07-18")
        self.assertEqual(point, {"date": "2026-07-18", "orders": 1, "amount": 1250.5})
        self.assertEqual(result["totals"]["orders"], 1)
        self.assertEqual(result["totals"]["amount"], 1250.5)
        self.assertEqual(result["data_quality"]["exact_dates"], 1)
        self.assertEqual(result["data_quality"]["approximate_dates"], 0)

    def test_csv_order_uses_export_payment_and_partial_status_dates(self) -> None:
        paid_day, paid_approximate = router._record_date({
            "platform_id": "paid-archive",
            "fields": {"paid_at": "2025-08-14 19:30:00"},
            "created_at": "2026-07-18T20:00:00Z", "updated_at": "2026-07-18T20:00:00Z",
        }, "paid", {})
        partial_day, partial_approximate = router._record_date({
            "platform_id": "partial-archive",
            "fields": {"status_changed_at": "2025-09-03 10:15:00"},
            "created_at": "2026-07-18T20:00:00Z", "updated_at": "2026-07-18T20:00:00Z",
        }, "partial", {})

        self.assertEqual(paid_day.isoformat(), "2025-08-14")
        self.assertEqual(partial_day.isoformat(), "2025-09-03")
        self.assertFalse(paid_approximate)
        self.assertFalse(partial_approximate)

    def test_status_amount_and_utm_product_filters(self) -> None:
        self._live("paid-1", {
            "payment_state": "paid", "payed_money": 1000, "cost_money": 1400,
            "title": "Мини-курс Послушная собака", "utm_source": "VK",
            "order_utm_medium": "Irina",
        })
        self._event("paid-1", "paid", "2026-07-17T09:00:00Z")
        self._live("unpaid-1", {
            "payment_state": "unpaid", "payed_money": 0, "cost_money": 900,
            "title": "Другой курс", "date_add": "17.07.2026 13:00:00", "utm_source": "telegram",
        })

        result = self._run({
            "preset": "custom", "date_from": "2026-07-17", "date_to": "2026-07-18",
            "statuses": ["paid", "unpaid"], "amount_basis": "cost",
            "filters": {
                "profile_utm": {"utm_source": ["vk"]},
                "order_utm": {"order_utm_medium": ["irina"]},
                "product": "послушная",
            },
        })

        self.assertEqual(result["totals"]["orders"], 1)
        self.assertEqual(result["totals"]["amount"], 1000.0)
        self.assertEqual(result["filter_options"]["profile_utm"]["utm_source"][0]["count"], 1)
        self.assertEqual({item["value"] for item in result["filter_options"]["products"]}, {"Мини-курс Послушная собака", "Другой курс"})

    def test_live_record_wins_archive_duplicate_and_missing_event_is_flagged(self) -> None:
        self._archive("legacy-paid", {
            "payment_state": "paid", "payed_money": 100, "received_at": "2026-07-15T10:00:00Z",
        })
        self._live("legacy-paid", {
            "payment_state": "paid", "payed_money": 250, "received_at": "2026-07-16T21:30:00Z",
        }, updated="2026-07-17T00:00:00Z")

        result = self._run({"preset": "7d", "statuses": ["paid"]})

        self.assertEqual(result["totals"]["orders"], 1)
        self.assertEqual(result["totals"]["amount"], 250.0)
        self.assertEqual(result["data_quality"]["duplicates"], 1)
        self.assertEqual(result["data_quality"]["approximate_dates"], 1)
        self.assertTrue(result["warnings"])

    def test_period_is_inclusive_and_zero_filled(self) -> None:
        self._live("paid-1", {"payment_state": "paid", "payed_money": 99})
        self._event("paid-1", "paid", "2026-07-15T10:00:00Z")

        result = self._run({"preset": "7d", "statuses": ["paid"]})

        self.assertEqual(len(result["points"]), 7)
        self.assertEqual(result["period"]["date_from"], "2026-07-12")
        self.assertEqual(result["period"]["date_to"], "2026-07-18")
        self.assertEqual(sum(item["orders"] for item in result["points"]), 1)

    def test_invalid_custom_period_and_empty_status_are_rejected(self) -> None:
        with self.assertRaises(HTTPException) as period_error:
            self._run({"preset": "custom", "date_from": "2026-07-19", "date_to": "2026-07-18"})
        self.assertEqual(period_error.exception.status_code, 400)
        with self.assertRaises(HTTPException) as status_error:
            self._run({"preset": "7d", "statuses": []})
        self.assertEqual(status_error.exception.status_code, 400)

    def test_exact_earned_comes_from_export_cache_and_missing_is_not_estimated(self) -> None:
        self._live("866550775", {"payment_state": "paid", "payed_money": 18900})
        self._event("866550775", "paid", "2026-07-14T17:23:10Z")
        self._finance("866550775", earned=17388, paid=18900, commission=1512)
        self._live("without-export", {"payment_state": "paid", "payed_money": 1000})
        self._event("without-export", "paid", "2026-07-14T18:00:00Z")

        result = self._run({"preset": "7d", "statuses": ["paid"]})

        self.assertEqual(result["charts"]["paid"]["total"], 19900.0)
        self.assertEqual(result["charts"]["earned"]["total"], 17388.0)
        self.assertEqual(result["charts"]["earned"]["exact_orders"], 1)
        self.assertEqual(result["charts"]["earned"]["missing_orders"], 1)

    def test_export_list_table_parser_reads_getcourse_finance_columns(self) -> None:
        payload = {"success": True, "info": [
            ["ID заказа", "Дата оплаты", "Оплачено", "Комиссия платежной системы", "Получено", "Другие комиссии", "Заработано", "Платежная система"],
            ["866550775", "14.07.2026 20:23:10", "18 900 руб.", "1 512 руб.", "17 388 руб.", "0", "17 388 руб.", "Оплата по частям"],
        ]}

        rows = router._extract_export_rows(payload)
        finance = router._finance_from_export_row(rows[0])

        self.assertEqual(finance["order_id"], "866550775")
        self.assertEqual(finance["paid"], 18900.0)
        self.assertEqual(finance["payment_commission"], 1512.0)
        self.assertEqual(finance["earned"], 17388.0)

    def test_registrations_use_unique_messenger_clients_not_utm_family(self) -> None:
        self._messenger_client("vk", "vk-one", "2026-07-17T21:30:00Z")
        self._messenger_client("vk", "vk-one", "2026-07-18T08:00:00Z")
        self._messenger_client("tg", "tg-one", "2026-07-18T09:00:00Z", {
            "first_contact_at": "2026-07-18T07:00:00+03:00",
            "source": "salebot_export_397724_2026-07-17",
        })
        self._registration("tracker-tg", "telegram_blog", "2026-07-18T09:00:00Z")

        result = self._run({"preset": "7d", "statuses": ["paid"]})

        registrations = result["charts"]["registrations"]
        self.assertEqual(registrations["totals"], {"vk": 1, "tg": 1, "total": 2})
        day = next(item for item in registrations["points"] if item["date"] == "2026-07-18")
        self.assertEqual(day, {"date": "2026-07-18", "vk": 1, "tg": 1, "total": 2})

    def test_identity_tokens_link_services_only_by_strong_identifiers(self) -> None:
        vk = router._identity_tokens("vk", "123", {
            "phone": "+7 (999) 111-22-33", "email": "DOG@example.com", "salebot_id": "sb-7",
            "manager_phone": "+7 999 000-00-00", "manager_email": "manager@example.com",
        })
        bizon = router._identity_tokens("bizon", "attendance-1", {
            "phone": "8 999 111 22 33", "identity_tokens": ["email:dog@example.com"],
        })
        self.assertIn("phone:79991112233", vk & bizon)
        self.assertIn("email:dog@example.com", vk & bizon)
        self.assertNotIn("name:Иван", vk)
        self.assertNotIn("phone:79990000000", vk)
        self.assertNotIn("email:manager@example.com", vk)

    def test_vk_platform_id_links_getcourse_and_amo_only_against_known_ids(self) -> None:
        known = {"senler-platform-77"}
        vk = router._identity_tokens("vk", "senler-platform-77", {})
        getcourse = router._identity_tokens(
            "getcourse", "order-1", {"utm_term": "senler-platform-77"},
            known_vk_platform_ids=known,
        )
        unrelated = router._identity_tokens(
            "amo", "deal-1", {"platform_id": "random-order-id"},
            known_vk_platform_ids=known,
        )

        self.assertIn("vk_platform:senler-platform-77", vk & getcourse)
        self.assertNotIn("vk_platform:random-order-id", unrelated)

    def test_journey_source_uses_only_avito_or_actual_utm(self) -> None:
        self.assertEqual(router._journey_source("amo", {"source": "amoCRM"}), "Прямой / без UTM")
        self.assertEqual(router._journey_source("bizon", {"source": "bizon365_report"}), "Прямой / без UTM")
        self.assertEqual(router._journey_source("avito", {}), "Avito")
        self.assertEqual(router._journey_source("vk", {"utm_source": "yandex_dk2_ai"}), "yandex_dk2_ai")

    def test_registration_stage_accepts_only_vk_and_telegram_services(self) -> None:
        self.assertEqual(router._service_funnel_flags("avito"), router.FUNNEL_CONTACT)
        self.assertEqual(router._service_funnel_flags("tracker"), router.FUNNEL_CONTACT)
        for service in ("vk", "telegram"):
            self.assertTrue(router._service_funnel_flags(service) & router.FUNNEL_REGISTRATION)

    def test_amo_events_include_only_incoming_pipeline(self) -> None:
        self._amo_deal("incoming", "  1.   Входящая ", "Успешно реализовано", "incoming@example.com")
        self._amo_deal("outgoing", "2. Исходящая", "Успешно реализовано", "outgoing@example.com")
        self._amo_deal("short-outgoing", "3. Исходящие менее 85 мин", "Оплатили", "short@example.com")

        router._funnel_cache = None
        router._build_funnel_store("test-fixture")
        snapshot = router._load_funnel_snapshot(
            date(2026, 7, 17), date(2026, 7, 17), {"paid"}, "quantitative",
        )
        funnel = router._quantitative_funnel(snapshot, date(2026, 7, 17), date(2026, 7, 17), {"paid"})
        with sqlite3.connect(router._funnel_store_path()) as db:
            persisted_tokens = [row[0] for row in db.execute("SELECT token FROM tokens")]

        self.assertEqual(funnel["stages"]["contacts"], 1)
        self.assertEqual(funnel["stages"]["applications"], 1)
        self.assertEqual(funnel["details"]["amo_sale_users"], 1)
        self.assertNotIn("incoming@example.com", " ".join(persisted_tokens))
        self.assertTrue(all(len(token) == 32 for token in persisted_tokens))

    def test_full_tariff_classifier_excludes_small_products_and_payment_parts(self) -> None:
        self.assertTrue(router._is_full_tariff({
            "title": "«Первые шаги к воспитанию». Тариф «Премиум». Автооплата",
            "cost_money": 18900,
        }))
        self.assertTrue(router._is_full_tariff({
            "title": "Курс: «Современный собаковод. Пакет VIP.»",
            "cost_money": 69900,
        }))
        for title, cost in (
            ("Мини-курс «Поводок»", 990),
            ("«Первые шаги к воспитанию». Доплата до VIP", 15000),
            ("Курс: «Современный собаковод. Пакет премиум.» 1/4", 18900),
            ("Курс: «Современный собаковод. Пакет премиум.» 2 модуль", 18900),
        ):
            self.assertFalse(router._is_full_tariff({"title": title, "cost_money": cost}))
        self.assertTrue(router._is_full_tariff({"is_full_tariff": True, "title": "archive"}))

    def test_bizon_event_date_uses_webinar_time_and_never_row_creation(self) -> None:
        webinar = router._service_event_date(
            "bizon",
            {
                "date_web": "2026-06-03",
                "webinar_at": "2026-07-18T10:45:00Z",
                "created": "2026-07-18T10:45:00Z",
            },
            "2026-07-18T10:46:00Z",
            "2026-07-18T10:47:00Z",
        )
        embedded = router._service_event_date(
            "bizon",
            {"webinarId": "97242:lesson*2026-05-20T12:00:00"},
            "2026-07-18T10:46:00Z",
            "2026-07-18T10:47:00Z",
        )
        missing = router._service_event_date(
            "bizon", {}, "2026-07-18T10:46:00Z", "2026-07-18T10:47:00Z",
        )

        self.assertEqual(webinar.astimezone(router.MOSCOW_TZ).date().isoformat(), "2026-06-03")
        self.assertEqual(embedded.astimezone(router.MOSCOW_TZ).date().isoformat(), "2026-05-20")
        self.assertIsNone(missing)

    def test_funnel_is_nested_and_reports_direct_sales_separately(self) -> None:
        router._funnel_cache = {"built_at": time.monotonic(), "built_at_iso": "2026-07-18T10:00:00Z", "entities": [
            {"date": "2026-07-17", "source": "yandex_dk2_ai", "segment": "ai", "flags": 1 | 2 | 4 | 8 | 16, "linked": True},
            {"date": "2026-07-17", "source": "yandex_az_baza", "segment": "baza", "flags": 1 | 2 | 8 | 16, "linked": True},
            {"date": "2026-07-17", "source": "Прямой / без UTM", "segment": "", "flags": 1, "linked": False},
        ]}

        funnel = asyncio.run(router._funnel_for_period(date(2026, 7, 17), date(2026, 7, 18)))

        self.assertEqual(funnel["stages"], {
            "contacts": 3, "registrations": 2, "live": 1,
            "applications": 1, "sales": 1, "sales_any": 2,
        })
        self.assertEqual(funnel["dropoffs"], {
            "before_registration": 1, "before_live": 1,
            "before_application": 0, "before_sale": 0,
        })
        self.assertEqual(funnel["stage_percentages"]["applications"], 33.3)
        self.assertEqual(funnel["sales_segments"], {
            "ai": {"count": 1, "percent": 50.0},
            "baza": {"count": 1, "percent": 50.0},
            "total": 2,
        })
        self.assertEqual(funnel["identity"]["coverage"], 66.7)

    def test_quantitative_mode_uses_each_event_date_and_selected_sale_status(self) -> None:
        router._funnel_cache = {"built_at": time.monotonic(), "built_at_iso": "2026-07-18T10:00:00Z", "entities": [
            {
                "date": "2026-06-01", "source": "yandex_dk2_ai", "segment": "ai",
                "flags": 1 | 8 | 16, "linked": True,
                "stage_dates": {"applications": ["2026-06-01"], "sales_paid": ["2026-07-17"]},
                "record_dates": {"getcourse_sale_paid": ["2026-07-17"], "amo_sale_paid": ["2026-07-17"]},
            },
            {
                "date": "2026-07-17", "source": "Avito", "segment": "",
                "flags": 1 | 2 | 4 | 8 | 32, "linked": True,
                "registration_channels": ["vk"],
                "stage_dates": {
                    "registrations": ["2026-07-17"], "live": ["2026-07-17"],
                    "applications": ["2026-07-17"], "sales_partial": ["2026-07-17"],
                },
                "record_dates": {
                    "vk_registration": ["2026-07-17"],
                    "getcourse_application": ["2026-07-17"],
                    "getcourse_sale_partial": ["2026-07-17"],
                },
            },
        ]}

        paid = asyncio.run(router._funnel_for_period(
            date(2026, 7, 17), date(2026, 7, 18), {"paid"}, "quantitative",
        ))
        partial = asyncio.run(router._funnel_for_period(
            date(2026, 7, 17), date(2026, 7, 18), {"partial"}, "quantitative",
        ))

        self.assertEqual(paid["stages"], {
            "contacts": 1, "registrations": 1, "live": 1,
            "applications": 1, "sales": 1, "sales_any": 1,
        })
        self.assertEqual(paid["details"]["getcourse_sale_orders"], 1)
        self.assertEqual(paid["details"]["amo_sale_users"], 1)
        self.assertEqual(paid["sales_segments"]["ai"], {"count": 1, "percent": 100.0})
        self.assertEqual(partial["stages"]["sales"], 1)
        self.assertEqual(partial["details"]["getcourse_sale_orders"], 1)
        self.assertEqual(partial["details"]["amo_sale_users"], 0)
        self.assertEqual(partial["details"]["registration_vk"], 1)
        self.assertEqual(partial["details"]["registration_telegram"], 0)

    def test_panel_auth_rejects_missing_user_and_accepts_admin(self) -> None:
        original = router.verify_token_from_request

        async def missing(_request):
            return None

        async def admin(_request):
            return {"role": "admin"}

        try:
            router.verify_token_from_request = missing
            with self.assertRaises(HTTPException) as denied:
                asyncio.run(router._require_panel_user(object()))
            self.assertEqual(denied.exception.status_code, 401)
            router.verify_token_from_request = admin
            self.assertEqual(asyncio.run(router._require_panel_user(object())), {"role": "admin"})
        finally:
            router.verify_token_from_request = original


if __name__ == "__main__":
    unittest.main()
