from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from urllib.parse import parse_qsl


def load_router():
    aiosqlite = types.ModuleType("aiosqlite")
    aiosqlite.connect = lambda *args, **kwargs: None
    sys.modules.setdefault("aiosqlite", aiosqlite)
    httpx = types.ModuleType("httpx")
    httpx.AsyncClient = object
    sys.modules.setdefault("httpx", httpx)
    fastapi = types.ModuleType("fastapi")

    class APIRouter:
        def __getattr__(self, _name):
            return lambda *args, **kwargs: (lambda func: func)

    class HTTPException(Exception):
        pass

    fastapi.APIRouter = APIRouter
    fastapi.HTTPException = HTTPException
    fastapi.Request = object
    sys.modules.setdefault("fastapi", fastapi)
    responses = types.ModuleType("fastapi.responses")
    responses.JSONResponse = dict
    sys.modules.setdefault("fastapi.responses", responses)
    auth = types.ModuleType("orchestrator.auth")
    auth.can_access_module = lambda *args: True

    async def verify(*args):
        return {"role": "admin"}

    auth.verify_token_from_request = verify
    sys.modules.setdefault("orchestrator.auth", auth)
    spec = importlib.util.spec_from_file_location(
        "getcourse_amocrm_router",
        Path(__file__).resolve().parents[1] / "router.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


router = load_router()


def sample_order(process="created"):
    return {
        "order_id": "2001",
        "number": "1001",
        "lead_name": "ЗАКАЗ №1001 | Анна | 14.07.2026",
        "contact_name": "Анна",
        "first_name": "Анна",
        "last_name": "",
        "email": "anna@example.test",
        "phone": "+79990001122",
        "title": "Тариф Стандарт",
        "payment_link": "https://example.test/pay",
        "user_link": "https://example.test/user/1",
        "order_link": "https://example.test/order/2001",
        "cost_money": 17900.0,
        "left_cost_money": 17900.0 if process == "created" else 0.0,
        "payed_money": 0.0 if process == "created" else 17900.0,
        "status": "",
        "date_add": "14.07.2026",
        "budget_money": 0.0,
        "pay_field_user_ym_uid": "",
        "reg_field_user_ym_uid": "",
        "utm": {},
        "yclid": "",
        "ym_uid": "",
        "vk_dialog": "",
        "raw": {},
        "process": process,
    }


class GetCourseAmoLogicTest(unittest.TestCase):
    def test_floating_attempt_identity_uses_normalized_phone_and_email_only_as_fallback(self):
        order = sample_order()
        order["phone"] = "8 (999) 000-11-22"
        self.assertEqual(router._floating_identity_keys(order), ["floating:phone:79990001122"])
        order["phone"] = ""
        order["email"] = " Anna@Example.Test "
        self.assertEqual(router._floating_identity_keys(order), ["floating:email:anna@example.test"])

    def test_customer_db_hash_ignores_only_volatile_chat_timestamp(self):
        original = {"number": "1001", "payed_money": 0, "chat_fields_updated_at": "2026-07-14T14:00:00Z"}
        touched = {**original, "chat_fields_updated_at": "2026-07-14T14:05:00Z"}
        paid = {**touched, "payed_money": 1000}

        self.assertEqual(router._customer_db_source_hash(original), router._customer_db_source_hash(touched))
        self.assertEqual(router._customer_db_source_hash(original), router._customer_db_source_hash(json.dumps(touched)))
        self.assertNotEqual(router._customer_db_source_hash(touched), router._customer_db_source_hash(paid))

    def test_customer_db_file_import_sources_are_excluded(self):
        self.assertTrue(router._customer_db_file_import({"source": "getcourse_csv_export"}))
        self.assertTrue(router._customer_db_file_import({"source": "future-file-import"}))
        self.assertFalse(router._customer_db_file_import({"source": "getcourse_webhook"}))
        self.assertFalse(router._customer_db_file_import({}))

    def test_customer_db_payload_uses_export_creation_date(self):
        payload = router._payload_from_customer_db({"date_creation": "2025-02-06 14:30:00"})
        self.assertEqual(payload["date_add"], "2025-02-06 14:30:00")

    def test_customer_db_sync_marks_file_import_without_amo_processing(self):
        marked = []

        async def fake_settings():
            return {"cdb_sync_bootstrapped": "1", "bindings_paused": "0"}

        async def fake_rows(limit=1000, offset=0):
            if offset:
                return []
            return [{
                "id": 7,
                "custom_fields": json.dumps({"source": "getcourse_csv_export", "order_id": "old-7"}),
                "updated_at": "2026-07-18T20:00:00Z",
            }]

        async def fake_states(_ids):
            return {}

        async def fake_mark(record_id, updated_at, source_hash, result):
            marked.append((record_id, updated_at, source_hash, result))

        async def forbidden_process(*_args, **_kwargs):
            raise AssertionError("file import must not reach amo processing")

        originals = (
            router._settings_map,
            router._customer_db_rows,
            router._sync_state_for,
            router._mark_cdb_sync,
            router._process_order_payload,
        )
        (
            router._settings_map,
            router._customer_db_rows,
            router._sync_state_for,
            router._mark_cdb_sync,
            router._process_order_payload,
        ) = (fake_settings, fake_rows, fake_states, fake_mark, forbidden_process)
        try:
            result = asyncio.run(router._sync_customer_db_once_unlocked(limit=1))
        finally:
            (
                router._settings_map,
                router._customer_db_rows,
                router._sync_state_for,
                router._mark_cdb_sync,
                router._process_order_payload,
            ) = originals
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["import_skipped"], 1)
        self.assertTrue(marked[0][3]["ok"])

    def test_customer_db_sync_scans_past_completed_first_page(self):
        processed_orders = []

        old_fields = json.dumps({"order_id": "old"})
        new_fields = json.dumps({"order_id": "new", "payment_state": "created"})
        old_hash = router._customer_db_source_hash(old_fields)

        async def fake_settings():
            return {"cdb_sync_bootstrapped": "1", "bindings_paused": "0"}

        async def fake_rows(limit=1000, offset=0):
            return {
                0: [{"id": 1, "custom_fields": old_fields, "updated_at": "2026-07-18T20:00:00Z"}],
                1: [{"id": 2, "custom_fields": new_fields, "updated_at": "2026-07-19T10:00:00Z"}],
            }.get(offset, [])

        async def fake_states(ids):
            return {1: {"success": 1, "source_hash": old_hash, "source_updated_at": "2026-07-18T20:00:00Z"}} if ids == [1] else {}

        async def fake_process(payload, *_args):
            processed_orders.append(payload["order_id"])
            return {"ok": True, "lead_id": "9"}

        async def fake_mark(*_args):
            return None

        originals = (
            router._settings_map,
            router._customer_db_rows,
            router._sync_state_for,
            router._process_order_payload,
            router._mark_cdb_sync,
        )
        (
            router._settings_map,
            router._customer_db_rows,
            router._sync_state_for,
            router._process_order_payload,
            router._mark_cdb_sync,
        ) = (fake_settings, fake_rows, fake_states, fake_process, fake_mark)
        try:
            result = asyncio.run(router._sync_customer_db_once_unlocked(limit=1))
        finally:
            (
                router._settings_map,
                router._customer_db_rows,
                router._sync_state_for,
                router._process_order_payload,
                router._mark_cdb_sync,
            ) = originals
        self.assertEqual(processed_orders, ["new"])
        self.assertEqual(result["source_rows"], 2)
        self.assertEqual(result["processed"], 1)

    def test_autopayment_filter_accepts_title_tag_or_explicit_tag_branch(self):
        order = sample_order()
        order["title"] = "Тариф Премиум. АВТООПЛАТА"
        self.assertEqual(router._autopayment_match({}, order), (True, "title"))

        order["title"] = "Тариф Премиум"
        self.assertEqual(router._autopayment_match({"order_tags": "VIP|Автооплата"}, order), (True, "tag"))
        self.assertEqual(router._autopayment_match({"autopayment": "1"}, order), (True, "tag_condition"))

    def test_autopayment_filter_rejects_unrelated_order_and_similar_tag(self):
        order = sample_order()
        order["title"] = "Курс Послушная собака"
        self.assertEqual(router._autopayment_match({"tags": "Не автооплата сегодня"}, order), (False, ""))

    def test_non_autopayment_phone_duplicate_returns_all_exact_contact_deals_newest_first(self):
        async def fake_request(method, path, settings, payload=None):
            if path.startswith("/api/v4/contacts?"):
                return {"_embedded": {"contacts": [
                    {
                        "id": 5,
                        "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79990001122"}]}],
                        "_embedded": {"leads": [{"id": 10}, {"id": 20}]},
                    },
                    {
                        "id": 6,
                        "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79990001123"}]}],
                        "_embedded": {"leads": [{"id": 30}]},
                    },
                ]}}, "", 200
            if path == "/api/v4/leads/10":
                return {"id": 10, "updated_at": 100}, "", 200
            if path == "/api/v4/leads/20":
                return {"id": 20, "updated_at": 200}, "", 200
            raise AssertionError(path)

        original = router._amo_request
        router._amo_request = fake_request
        try:
            leads, contacts, error = asyncio.run(router._find_non_autopayment_duplicate_by_phone(sample_order(), {}))
        finally:
            router._amo_request = original
        self.assertEqual(error, "")
        self.assertEqual([lead["id"] for lead in leads], [20, 10])
        self.assertEqual([contact["id"] for contact in contacts], [5])

    def test_non_autopayment_phone_duplicate_adds_cross_contact_customer_db_deals(self):
        async def fake_customer_db(_phone):
            return ["30"]

        async def fake_request(method, path, settings, payload=None):
            if path.startswith("/api/v4/contacts?"):
                return {"_embedded": {"contacts": []}}, "", 200
            if path == "/api/v4/leads/30":
                return {"id": 30, "updated_at": 300}, "", 200
            raise AssertionError(path)

        originals = router._amo_request, router._customer_db_deal_ids_for_phone
        router._amo_request, router._customer_db_deal_ids_for_phone = fake_request, fake_customer_db
        try:
            leads, _contacts, error = asyncio.run(router._find_non_autopayment_duplicate_by_phone(sample_order(), {}))
        finally:
            router._amo_request, router._customer_db_deal_ids_for_phone = originals
        self.assertEqual(error, "")
        self.assertEqual([lead["id"] for lead in leads], [30])

    def test_non_autopayment_process_adds_notes_to_every_deal_without_updates(self):
        noted = []

        async def fake_settings():
            return {**router.DEFAULT_SETTINGS, "bindings_paused": "0"}

        async def fake_find(_order, _settings):
            return [
                {"id": 20, "name": "Upsell", "price": 9000, "status_id": 142, "responsible_user_id": 10},
                {"id": 10, "name": "Основная", "price": 19000, "status_id": 100, "responsible_user_id": 20},
            ], [{"id": 5}], ""

        async def fake_note(lead_id, _order, _settings):
            noted.append(lead_id)
            return {"ok": True}, ""

        async def fake_store(_event):
            return 77

        originals = router._settings_map, router._find_non_autopayment_duplicate_by_phone, router._add_order_note, router._store_event
        router._settings_map, router._find_non_autopayment_duplicate_by_phone = fake_settings, fake_find
        router._add_order_note, router._store_event = fake_note, fake_store
        try:
            result = asyncio.run(router._process_order_payload(
                {
                    "order_id": "2001",
                    "number": "1001",
                    "positions": "Обычный тариф",
                    "phone": "+79990001122",
                    "name": "Анна",
                    "payment_state": "paid",
                },
                "{}",
                "customer-db",
                "paid",
            ))
        finally:
            router._settings_map, router._find_non_autopayment_duplicate_by_phone, router._add_order_note, router._store_event = originals
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "noted_non_autopayment_contact_deals")
        self.assertEqual(noted, ["20", "10"])

    def test_int_conversion_was_not_lost_during_round_robin_refactor(self):
        self.assertEqual(router._int_or_none("6295879"), 6295879)
        self.assertIsNone(router._int_or_none(""))

    def test_default_processes_use_required_statuses(self):
        statuses = {item["process"]: item["status_id"] for item in router.DEFAULT_BINDINGS}
        self.assertEqual(statuses, {
            "created": "83350598",
            "partial": "83350598",
            "paid": "142",
            "surcharge_created": "83350598",
            "surcharge_paid": "142",
        })

    def test_special_order_routing_requires_title_and_paid_minicourse(self):
        settings = dict(router.DEFAULT_SETTINGS)
        order = sample_order("created")
        order["title"] = "Доплата до VIP"
        self.assertEqual(router._route_order(order, "surcharge_created", settings), "surcharge_created")
        self.assertEqual(router._route_order(order, "surcharge_paid", settings), "surcharge_paid")

        order["title"] = "Мини-курс «Поводок»"
        self.assertEqual(router._route_order(order, "created", settings), "ignore_minicourse_unpaid")
        self.assertEqual(router._route_order(order, "partial", settings), "ignore_minicourse_unpaid")
        self.assertEqual(router._route_order(order, "paid", settings), "minicourse_paid")

        order["title"] = "Обычный заказ"
        self.assertEqual(router._route_order(order, "surcharge_created", settings), "ignore_surcharge_title")

    def test_hybrid_utm_keeps_profile_fields_and_uses_order_medium_for_curator(self):
        settings = {**router.DEFAULT_SETTINGS, "minicourse_curator_mediums": "irina\nslava\nnastasia"}
        order = sample_order("paid")
        order.update({
            "process": "minicourse_paid",
            "profile_utm": {
                "utm_source": "profile-source",
                "utm_medium": "profile-medium",
                "utm_campaign": "profile-campaign",
                "utm_content": "profile-content",
                "utm_term": "12345",
            },
            "order_utm_medium": "seller-slava-july",
        })
        router._apply_attribution(order, settings)
        self.assertEqual(order["utm"], {
            "utm_source": "profile-source",
            "utm_medium": "seller-slava-july",
            "utm_campaign": "profile-campaign",
            "utm_content": "profile-content",
            "utm_term": "12345",
        })
        self.assertTrue(order["curator_medium_match"])

        order["order_utm_medium"] = "unknown-seller"
        router._apply_attribution(order, settings)
        self.assertEqual(order["utm"]["utm_medium"], "profile-medium")
        self.assertFalse(order["curator_medium_match"])

    def test_all_order_utm_values_are_received_but_only_order_medium_is_selected(self):
        order = router._normalize_order({
            "number": "1001",
            "order_id": "2001",
            "positions": "Мини-курс «Поводок»",
            "utmS": "profile-source",
            "utmM": "profile-medium",
            "utmCa": "profile-campaign",
            "utmCo": "profile-content",
            "utmT": "profile-term",
            "orderUtmS": "order-source",
            "orderUtmM": "irina",
            "orderUtmCa": "order-campaign",
            "orderUtmCo": "order-content",
            "orderUtmT": "order-term",
        }, router.DEFAULT_SETTINGS)
        order["process"] = "minicourse_paid"
        router._apply_attribution(order, router.DEFAULT_SETTINGS)
        self.assertEqual(order["order_utm"], {
            "utm_source": "order-source",
            "utm_medium": "irina",
            "utm_campaign": "order-campaign",
            "utm_content": "order-content",
            "utm_term": "order-term",
        })
        self.assertEqual(order["utm"], {
            "utm_source": "profile-source",
            "utm_medium": "irina",
            "utm_campaign": "profile-campaign",
            "utm_content": "profile-content",
            "utm_term": "profile-term",
        })

    def test_generated_getcourse_url_appends_all_order_utm_values(self):
        pairs = parse_qsl(router._getcourse_url_params("secret"), keep_blank_values=True)
        self.assertEqual([name for name, _value in pairs[-5:]], [
            "orderUtmS", "orderUtmM", "orderUtmCa", "orderUtmCo", "orderUtmT",
        ])
        self.assertEqual(dict(pairs)["orderUtmM"], "{object.create_session.utm_medium}")

    def test_surcharge_always_uses_order_medium_and_special_tags_replace_autopayment(self):
        order = sample_order("created")
        order.update({
            "process": "surcharge_created",
            "profile_utm": {key: "profile" for key, _field, _code in router.UTM_SPECS},
            "order_utm_medium": "irina",
        })
        router._apply_attribution(order, router.DEFAULT_SETTINGS)
        self.assertEqual(order["utm"]["utm_medium"], "irina")
        self.assertEqual(
            [item["name"] for item in router._tags({"tags": "GC\nАвтооплата"}, order)],
            ["GC", "Доплата"],
        )
        order["process"] = "minicourse_paid"
        self.assertEqual(
            [item["name"] for item in router._tags({"tags": "GC\nАвтооплата"}, order)],
            ["GC", "Мини-курс"],
        )

    def test_minicourse_binding_is_fixed_to_pipeline_and_andrey_without_tasks(self):
        binding = asyncio.run(router._binding_for_process("minicourse_paid", router.DEFAULT_SETTINGS))
        self.assertEqual(binding["pipeline_id"], "8493006")
        self.assertEqual(binding["status_id"], "69046790")
        self.assertEqual(router._selected_responsible_ids({"responsible_user_ids_json": '["99"]'}, binding), ["6269974"])
        self.assertEqual(binding["task_enabled"], 0)

    def test_lead_fields_fill_getcourse_id_date_and_minicourse_tariff(self):
        fields = [
            {"id": 990979, "name": "Дата создания", "type": "text"},
            {"id": 990983, "name": "ГК ID Заказа", "type": "text"},
            {"id": 961039, "name": "Тариф", "type": "select", "enums": [{"id": 604135, "value": "Мини курс"}]},
        ]
        order = sample_order("paid")
        order.update({"process": "minicourse_paid", "utm": {}})
        values = router._lead_field_values(fields, order)
        by_id = {item["field_id"]: item["values"][0] for item in values}
        self.assertEqual(by_id[990979]["value"], "14.07.2026")
        self.assertEqual(by_id[990983]["value"], "2001")
        self.assertEqual(by_id[961039]["enum_id"], 604135)

    def test_paid_minicourse_end_to_end_uses_special_binding_and_never_creates_task(self):
        captured = {}

        async def fake_settings():
            return {**router.DEFAULT_SETTINGS, "responsible_user_ids_json": '["99"]'}

        async def fake_find(_order, _settings, _binding):
            return "", ""

        async def fake_responsible(_settings, binding):
            captured["responsible_binding"] = binding
            return "6269974"

        async def fake_create(order, _settings, binding, responsible):
            captured.update(order=order, binding=binding, responsible=responsible)
            return {"lead_id": "777", "contact_id": "888"}, ""

        async def fake_note(_lead_id, _order, _settings):
            return {"ok": True}, ""

        async def fake_store(_event):
            return 91

        async def fake_advance(_settings, _binding):
            return None

        async def forbidden_task(*_args, **_kwargs):
            raise AssertionError("mini-course task must not be created")

        originals = (
            router._settings_map,
            router._find_existing_lead,
            router._new_responsible,
            router._create_lead,
            router._add_order_note,
            router._store_event,
            router._advance_responsible_cursor,
            router._create_task_for_lead,
        )
        (
            router._settings_map,
            router._find_existing_lead,
            router._new_responsible,
            router._create_lead,
            router._add_order_note,
            router._store_event,
            router._advance_responsible_cursor,
            router._create_task_for_lead,
        ) = (
            fake_settings,
            fake_find,
            fake_responsible,
            fake_create,
            fake_note,
            fake_store,
            fake_advance,
            forbidden_task,
        )
        try:
            result = asyncio.run(router._process_order_payload(
                {
                    "order_id": "3001",
                    "number": "2001",
                    "positions": "Мини-курс «Намордник» 4388265",
                    "payedMoney": "990",
                    "leftCostMoney": "0",
                    "name": "Анна",
                    "date_add": "05.06.2026",
                    "utmS": "profile-source",
                    "utmM": "profile-medium",
                    "orderUtmM": "sale-slava",
                    "utmCa": "profile-campaign",
                },
                "{}",
                "test",
                "paid",
            ))
        finally:
            (
                router._settings_map,
                router._find_existing_lead,
                router._new_responsible,
                router._create_lead,
                router._add_order_note,
                router._store_event,
                router._advance_responsible_cursor,
                router._create_task_for_lead,
            ) = originals
        self.assertTrue(result["ok"])
        self.assertEqual(captured["order"]["process"], "minicourse_paid")
        self.assertEqual(captured["order"]["lead_name"], "ЗАКАЗ №2001 | Анна | 05.06.2026")
        self.assertEqual(captured["order"]["utm"]["utm_medium"], "sale-slava")
        self.assertEqual(captured["binding"]["pipeline_id"], "8493006")
        self.assertEqual(captured["responsible"], "6269974")

    def test_successful_different_order_gets_separate_unpaid_attempt(self):
        existing = {
            "status_id": 142,
            "custom_fields_values": [
                {"field_id": 1006689, "values": [{"value": "old-order"}]},
                {"field_id": 1006697, "values": [{"value": 17900}]},
                {"field_id": 1006699, "values": [{"value": 0}]},
            ],
        }
        self.assertEqual(router._duplicate_action(existing, sample_order("created")), "create_new_unpaid_attempt")

    def test_successful_different_order_creates_new_deal_after_payment(self):
        existing = {
            "status_id": 142,
            "custom_fields_values": [
                {"field_id": 1006689, "values": [{"value": "old-order"}]},
                {"field_id": 1006697, "values": [{"value": 17900}]},
                {"field_id": 1006699, "values": [{"value": 0}]},
            ],
        }
        self.assertEqual(router._duplicate_action(existing, sample_order("partial")), "create_new_paid_order")
        self.assertEqual(router._duplicate_action(existing, sample_order("paid")), "create_new_paid_order")

    def test_lower_payment_state_never_overwrites_paid_data(self):
        existing = {
            "status_id": 142,
            "custom_fields_values": [
                {"field_id": 1006689, "values": [{"value": "1001"}]},
                {"field_id": 1006697, "values": [{"value": 17900}]},
                {"field_id": 1006699, "values": [{"value": 0}]},
            ],
        }
        self.assertEqual(router._duplicate_action(existing, sample_order("created")), "note_only_locked_payment")
        self.assertEqual(router._duplicate_action(existing, sample_order("paid")), "note_only_locked_payment")

    def test_partial_payment_locks_attempt_until_paid_transition(self):
        existing = {
            "status_id": 83350598,
            "custom_fields_values": [
                {"field_id": 1006689, "values": [{"value": "1001"}]},
                {"field_id": 1006697, "values": [{"value": 5000}]},
                {"field_id": 1006699, "values": [{"value": 12900}]},
            ],
        }
        self.assertEqual(router._existing_payment_rank(existing), 2)
        self.assertEqual(router._duplicate_action(existing, sample_order("partial")), "note_only_locked_payment")
        self.assertEqual(router._duplicate_action(existing, sample_order("paid")), "update_payment_transition")

    def test_note_matches_template_and_omits_empty_test_identifiers(self):
        text = router._format_order_template(router.DEFAULT_NOTE_TEMPLATE, sample_order())
        self.assertIn("ГЕТКУРС ЗАКАЗ №1001", text)
        self.assertIn("Название тарифа Тариф Стандарт", text)
        self.assertLess(
            text.index("ссылка на оплату: https://example.test/pay"),
            text.index("Название тарифа Тариф Стандарт"),
        )
        self.assertEqual(text.count("ссылка на оплату:"), 1)
        self.assertIn("Телефон: +79990001122", text)
        self.assertNotIn("Тестовые примечания", text)
        self.assertNotIn("{", text)

    def test_note_includes_optional_test_identifiers_when_present(self):
        order = sample_order()
        order["pay_field_user_ym_uid"] = "pay-ym"
        text = router._format_order_template(router.DEFAULT_NOTE_TEMPLATE, order)
        self.assertIn("Тестовые примечания", text)
        self.assertIn("pay-ym", text)

    def test_budget_can_use_paid_cost_none_or_explicit_net_value(self):
        order = sample_order("partial")
        self.assertEqual(router._budget_value(order, {"budget_source": "paid"}), 17900)
        self.assertEqual(router._budget_value(order, {"budget_source": "cost"}), 17900)
        self.assertEqual(router._budget_value(order, {"budget_source": "none"}), 0)
        order["budget_money"] = 17416
        self.assertEqual(router._budget_value(order, {"budget_source": "paid"}), 17416)

    def test_selected_responsibles_are_unique_and_nested_rights_are_honored(self):
        settings = {"responsible_user_ids_json": '["10","20","10"]'}
        self.assertEqual(router._selected_responsible_ids(settings), ["10", "20"])
        self.assertEqual(
            router._selected_responsible_ids(settings, {"responsible_user_id": "6269974"}),
            ["6269974"],
        )
        users = [
            {"id": 10, "rights": {"is_active": True}},
            {"id": 20, "rights": {"is_active": False}},
            {"id": 30},
        ]
        self.assertEqual(router._active_amo_user_ids(users), {"10", "30"})

    def test_surcharge_tasks_are_configurable_and_task_owner_overrides_deal_owner(self):
        self.assertFalse(router._tasks_forbidden("surcharge_created"))
        self.assertFalse(router._tasks_forbidden("surcharge_paid"))
        self.assertTrue(router._tasks_forbidden("minicourse_paid"))
        self.assertEqual(
            router._task_responsible_id(
                {"responsible_user_id": "6269974", "task_responsible_user_id": "6295879"},
                "6269974",
            ),
            6295879,
        )

    def test_update_preserves_owner_and_only_moves_from_allowed_status(self):
        calls = []

        async def fake_fields(entity, _settings):
            return [], ""

        async def fake_contact(_order, _settings):
            return None, ""

        async def fake_request(method, path, settings, payload=None):
            calls.append((method, path, payload))
            return {"id": 777}, "", 200

        async def fake_remember(_order, _lead_id, *_args, **_kwargs):
            return None

        originals = router._amo_fields, router._find_contact_for_order, router._amo_request, router._remember_lead
        router._amo_fields, router._find_contact_for_order, router._amo_request, router._remember_lead = fake_fields, fake_contact, fake_request, fake_remember
        try:
            binding = {"status_id": "142", "move_from_statuses": ["83350598"]}
            asyncio.run(router._update_lead("777", sample_order("paid"), {}, binding, {"status_id": 83350598, "responsible_user_id": 99}))
            allowed_payload = calls[-1][2]
            calls.clear()
            asyncio.run(router._update_lead("777", sample_order("paid"), {}, binding, {"status_id": 83350606, "responsible_user_id": 99}))
            blocked_payload = calls[-1][2]
        finally:
            router._amo_fields, router._find_contact_for_order, router._amo_request, router._remember_lead = originals
        self.assertEqual(allowed_payload["status_id"], 142)
        self.assertNotIn("status_id", blocked_payload)
        self.assertNotIn("responsible_user_id", allowed_payload)
        self.assertNotIn("responsible_user_id", blocked_payload)

    def test_partial_search_uses_order_number_and_never_phone_fallback(self):
        calls = []

        async def fake_mapped(_order):
            return ""

        async def fake_request(method, path, settings, payload=None):
            calls.append(path)
            return {"_embedded": {"leads": []}}, "", 200

        originals = router._mapped_lead_id, router._amo_request
        router._mapped_lead_id, router._amo_request = fake_mapped, fake_request
        try:
            order = sample_order("partial")
            lead_id, _source = asyncio.run(router._find_existing_lead(order, {}, {"pipeline_id": "10566818"}))
        finally:
            router._mapped_lead_id, router._amo_request = originals
        self.assertEqual(lead_id, "")
        self.assertTrue(any(path.startswith("/api/v4/leads?query=") for path in calls))
        self.assertFalse(any(path.startswith("/api/v4/contacts?") for path in calls))

    def test_unpaid_persistent_identity_map_wins_before_eventual_amo_contact_search(self):
        calls = []

        async def fake_mapped(_order):
            return "77"

        async def fake_request(method, path, settings, payload=None):
            calls.append(path)
            if path == "/api/v4/leads/77":
                return {"id": 77, "pipeline_id": 10566818, "custom_fields_values": []}, "", 200
            raise AssertionError(path)

        originals = router._mapped_lead_id, router._amo_request
        router._mapped_lead_id, router._amo_request = fake_mapped, fake_request
        try:
            lead_id, source = asyncio.run(router._find_existing_lead(sample_order("created"), {}, {"pipeline_id": "10566818"}))
        finally:
            router._mapped_lead_id, router._amo_request = originals
        self.assertEqual((lead_id, source), ("77", "local_map"))
        self.assertEqual(calls, ["/api/v4/leads/77"])

    def test_unpaid_phone_search_ignores_locked_payment_deals(self):
        async def fake_mapped(_order):
            return ""

        async def fake_remember(_order, _lead_id, *_args, **_kwargs):
            return None

        async def fake_request(method, path, settings, payload=None):
            if path.startswith("/api/v4/contacts?"):
                return {"_embedded": {"contacts": [{
                    "id": 1,
                    "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79990001122"}]}],
                    "_embedded": {"leads": [{"id": 10}, {"id": 20}]},
                }]}}, "", 200
            if path == "/api/v4/leads/10":
                return {
                    "id": 10,
                    "pipeline_id": 10566818,
                    "updated_at": 20,
                    "custom_fields_values": [
                        {"field_id": 1006697, "values": [{"value": 5000}]},
                        {"field_id": 1006699, "values": [{"value": 12900}]},
                    ],
                }, "", 200
            if path == "/api/v4/leads/20":
                return {"id": 20, "pipeline_id": 10566818, "updated_at": 10, "custom_fields_values": []}, "", 200
            raise AssertionError(path)

        originals = router._mapped_lead_id, router._remember_lead, router._amo_request
        router._mapped_lead_id, router._remember_lead, router._amo_request = fake_mapped, fake_remember, fake_request
        try:
            lead_id, source = asyncio.run(router._find_existing_lead(sample_order("created"), {}, {"pipeline_id": "10566818"}))
        finally:
            router._mapped_lead_id, router._remember_lead, router._amo_request = originals
        self.assertEqual((lead_id, source), ("20", "contacts:PHONE"))

    def test_task_is_skipped_when_same_phone_has_open_task_on_another_deal(self):
        async def fake_customer_db(_phone):
            return ["20", "10"]

        async def fake_request(method, path, settings, payload=None):
            if path == "/api/v4/tasks?filter[entity_id]=10&limit=250":
                return {"_embedded": {"tasks": []}}, "", 200
            if path == "/api/v4/tasks?filter[entity_id]=20&limit=250":
                return {"_embedded": {"tasks": [{"id": 900, "is_completed": False, "text": "Связаться заказ ГК"}]}}, "", 200
            raise AssertionError((method, path, payload))

        originals = router._customer_db_deal_ids_for_phone, router._amo_request
        router._customer_db_deal_ids_for_phone, router._amo_request = fake_customer_db, fake_request
        try:
            result, error = asyncio.run(router._create_task_for_lead(
                "10",
                sample_order(),
                {},
                {"task_enabled": 1, "task_text": "Связаться заказ ГК", "task_type_id": 1},
                "99",
            ))
        finally:
            router._customer_db_deal_ids_for_phone, router._amo_request = originals
        self.assertEqual(error, "")
        self.assertTrue(result["skipped"])
        self.assertEqual(result["task_lead_id"], "20")


if __name__ == "__main__":
    unittest.main()
