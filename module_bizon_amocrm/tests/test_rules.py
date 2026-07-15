from __future__ import annotations

import importlib.util
import asyncio
import sys
import types
import unittest
from pathlib import Path


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
    fastapi.Query = lambda default=None, **kwargs: default
    fastapi.Request = object
    sys.modules.setdefault("fastapi", fastapi)
    pydantic = types.ModuleType("pydantic")
    pydantic.BaseModel = type("BaseModel", (), {})
    sys.modules.setdefault("pydantic", pydantic)
    auth = types.ModuleType("orchestrator.auth")
    auth.can_access_module = lambda *args: True
    async def verify(*args): return {"role": "admin"}
    auth.verify_token_from_request = verify
    sys.modules.setdefault("orchestrator.auth", auth)
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    spec = importlib.util.spec_from_file_location("bizon_amocrm_router", Path(__file__).resolve().parents[1] / "router.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


router = load_router()


class BizonAmoRulesTest(unittest.TestCase):
    def test_all_rooms_binding_matches_without_room_value(self):
        self.assertTrue(router._binding_matches(
            {"match_type": "all", "match_value": ""},
            {"webinarId": "97242:any-room*2026-07-14T19:00:00"},
        ))

    def test_room_binding_matches_dynamic_webinar_id(self):
        self.assertTrue(router._binding_matches(
            {"match_type": "room", "match_value": "97242:puppy"},
            {"webinarId": "97242:puppy*2026-07-13T12:00:00"},
        ))

    def test_regex_error_never_matches(self):
        self.assertFalse(router._binding_matches(
            {"match_type": "regex", "match_value": "["},
            {"roomid": "97242:puppy"},
        ))

    def test_phone_comparison_is_normalized(self):
        self.assertTrue(router._same("+7 (999) 111-22-33", "89991112233"))

    def test_closed_duplicate_is_found_inside_selected_pipeline(self):
        binding = {"pipeline_scope": ["10", "20"], "status_scope": []}
        self.assertFalse(router._lead_allowed({"pipeline_id": 30, "status_id": 100}, binding))
        self.assertTrue(router._lead_allowed({"pipeline_id": 10, "status_id": 142}, binding))
        self.assertTrue(router._lead_allowed({"pipeline_id": 20, "status_id": 101}, binding))

    def test_exact_contact_deals_are_found_and_sorted_newest_first(self):
        async def fake_request(method, path, payload=None):
            if path.startswith("/api/v4/contacts?"):
                return {"_embedded": {"contacts": [
                    {
                        "id": 5,
                        "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79991112233"}]}],
                        "_embedded": {"leads": [{"id": 10}, {"id": 20}]},
                    },
                    {
                        "id": 6,
                        "custom_fields_values": [{"field_code": "PHONE", "values": [{"value": "+79991112234"}]}],
                        "_embedded": {"leads": [{"id": 30}]},
                    },
                ]}}, "", 200
            if path == "/api/v4/leads/10":
                return {"id": 10, "name": "Основная", "updated_at": 100}, "", 200
            if path == "/api/v4/leads/20":
                return {"id": 20, "name": "Upsell", "updated_at": 200}, "", 200
            raise AssertionError(path)

        original = router._amo_request
        original_cdb = router._customer_db_deal_ids_for_attendance
        router._amo_request = fake_request
        async def no_cdb(_attendance): return []
        router._customer_db_deal_ids_for_attendance = no_cdb
        try:
            leads, error = asyncio.run(router._find_all_contact_leads(
                {"phone": "+79991112233"},
                {"duplicate_rules": [{"entity": "contacts", "field_code": "PHONE", "source": "phone"}]},
            ))
        finally:
            router._amo_request = original
            router._customer_db_deal_ids_for_attendance = original_cdb
        self.assertEqual(error, "")
        self.assertEqual([lead["id"] for lead in leads], [20, 10])

    def test_duplicate_sort_prefers_active_then_latest(self):
        rows = router._sort_existing_leads([
            {"id": 30, "status_id": 143, "updated_at": 500},
            {"id": 20, "status_id": 101, "updated_at": 200},
            {"id": 10, "status_id": 101, "updated_at": 100},
        ])
        self.assertEqual([row["id"] for row in rows], [20, 10, 30])

    def test_customer_db_identity_match_is_exact(self):
        rows = [
            ("20", '{"deal_id":"20","phones":["79991112233"],"emails":[]}'),
            ("30", '{"deal_id":"30","phones":["79991112234"],"emails":[]}'),
        ]
        class Cursor:
            async def fetchall(self): return rows
        class DB:
            async def execute(self, *_args): return Cursor()
        class Context:
            async def __aenter__(self): return DB()
            async def __aexit__(self, *_args): return None
        original_connect, original_path = router.aiosqlite.connect, router._customer_db_path
        router.aiosqlite.connect = lambda *_args, **_kwargs: Context()
        router._customer_db_path = lambda: Path(__file__)
        try:
            found = asyncio.run(router._customer_db_deal_ids_for_attendance({"phone": "8 (999) 111-22-33"}))
        finally:
            router.aiosqlite.connect, router._customer_db_path = original_connect, original_path
        self.assertEqual(found, [20])

    def test_webinar_note_is_idempotent(self):
        attendance = {"webinarId": "web-1", "phone": "+79991112233", "watch_minutes": 61}
        binding = {"note_template": "Вебинар {webinarId}; {watch_minutes} мин."}
        text = router._format_note(binding["note_template"], attendance)
        calls = []

        async def fake_request(method, path, payload=None):
            calls.append((method, path, payload))
            return {"_embedded": {"notes": [{"note_type": "common", "params": {"text": text}}]}}, "", 200

        original = router._amo_request
        router._amo_request = fake_request
        try:
            result, error = asyncio.run(router._add_note("20", attendance, binding))
        finally:
            router._amo_request = original
        self.assertEqual(error, "")
        self.assertTrue(result["skipped"])
        self.assertEqual([method for method, _path, _payload in calls], ["GET"])

    def test_time_ranges_split_exactly_at_sixty(self):
        base = {"watch_valid": True}
        below = {"min_minutes": 0, "max_minutes": 60}
        above = {"min_minutes": 60, "max_minutes": None}
        self.assertTrue(router._binding_time_matches(below, {**base, "watch_minutes": 59.999}))
        self.assertFalse(router._binding_time_matches(below, {**base, "watch_minutes": 60}))
        self.assertTrue(router._binding_time_matches(above, {**base, "watch_minutes": 60}))

    def test_ignored_status_forces_note_only(self):
        existing = {"id": 1, "status_id": 142, "responsible_user_id": 999}
        binding = {"duplicate_action": "merge_empty", "note_only_status_ids": ["142"]}
        self.assertEqual(router._duplicate_plan(existing, binding), "note_only")
        self.assertEqual(router._duplicate_plan({**existing, "status_id": 101}, binding), "merge_empty")

    def test_lead_name_contains_moscow_webinar_time_minutes_and_name(self):
        attendance = {"webinar_at": 1784010240, "watch_minutes": 6.917, "username": "Никита"}
        binding = {"lead_name_template": "{webinar_date} | {webinar_time} | {watch_minutes_round}м | {username}"}
        self.assertEqual(router._lead_name(attendance, binding), "14.07.2026 | 09:24 | 7м | Никита")

    def test_tracking_identifiers_use_bizon_and_ym_fallbacks(self):
        attendance = {
            "person_key": "chatUserId:fallback-person",
            "profiles": [{"chatUserId": "bizon-user-42", "param1": "ym-from-param1"}],
        }
        self.assertEqual(router._source_value(attendance, "source_user_id"), "bizon-user-42")
        self.assertEqual(router._source_value(attendance, "_ym_uid"), "ym-from-param1")
        self.assertEqual(router._source_value({"person_key": "uid:abc"}, "source_user_id"), "abc")

    def test_ym_uid_prefers_explicit_value_over_param1(self):
        attendance = {"ym_uid": "explicit-ym", "param1": "fallback-ym", "p1": "last-resort"}
        self.assertEqual(router._source_value(attendance, "_ym_uid"), "explicit-ym")

    def test_messenger_fields_are_conditional_and_url_encoded(self):
        async def fake_lookup(value):
            return "telegram" if value == "tg/user" else "vk" if value == "vk user" else ""
        original = router._messenger_for_utm_term
        router._messenger_for_utm_term = fake_lookup
        try:
            telegram = asyncio.run(router._with_messenger_fields({"utm_term": "tg/user"}))
            vk = asyncio.run(router._with_messenger_fields({"utm_term": "vk user"}))
            unknown = asyncio.run(router._with_messenger_fields({"utm_term": "missing"}))
        finally:
            router._messenger_for_utm_term = original
        self.assertEqual(telegram["messenger_type"], "telegram")
        self.assertEqual(telegram["dialog_salebot_url"], "https://salebot.pro/projects/397724/clients/tg%2Fuser")
        self.assertEqual(telegram["dialog_vk_url"], "")
        self.assertEqual(vk["messenger_type"], "vk")
        self.assertEqual(vk["dialog_salebot_url"], "")
        self.assertEqual(vk["dialog_vk_url"], "https://vk.com/gim225075265/convo/vk%20user")
        self.assertEqual(unknown["dialog_salebot_url"], "")
        self.assertEqual(unknown["dialog_vk_url"], "")

    def test_messenger_lookup_rejects_ambiguous_and_unknown_ids(self):
        class Cursor:
            def __init__(self, found): self.found = found
            async def fetchone(self): return (1,) if self.found else None
        class DB:
            async def execute(self, query, params):
                term = params[0]
                if term == "both": return Cursor(True)
                if term == "telegram-only": return Cursor("telegram" in query)
                if term == "vk-only": return Cursor("vk_clients" in query)
                return Cursor(False)
        class Context:
            async def __aenter__(self): return DB()
            async def __aexit__(self, *_args): return None
        original_connect, original_path = router.aiosqlite.connect, router._customer_db_path
        router.aiosqlite.connect = lambda *_args, **_kwargs: Context()
        router._customer_db_path = lambda: Path(__file__)
        try:
            self.assertEqual(asyncio.run(router._messenger_for_utm_term("telegram-only")), "telegram")
            self.assertEqual(asyncio.run(router._messenger_for_utm_term("vk-only")), "vk")
            self.assertEqual(asyncio.run(router._messenger_for_utm_term("both")), "")
            self.assertEqual(asyncio.run(router._messenger_for_utm_term("missing")), "")
        finally:
            router.aiosqlite.connect, router._customer_db_path = original_connect, original_path

    def test_rounded_minutes_map_as_integer(self):
        attendance = {"watch_minutes": 6.917}
        binding = {"field_mappings": [{"entity": "leads", "field_id": 938437, "source": "watch_minutes_round"}]}
        catalog = [{"id": 938437, "type": "numeric", "code": None}]
        values = asyncio.run(router._mapped_field_values(attendance, binding, "leads", catalog))
        self.assertEqual(values, [{"field_id": 938437, "values": [{"value": 7}]}])

    def test_note_is_multiline_and_omits_missing_optional_data(self):
        text = router._format_note(
            "ОТЧЕТ:\\nИмя: {username}\\nГород: {city}\\n"
            "Был минут: {watch_minutes_round}; с {view_from_text} до {view_till_text}\\n"
            "ID Bizon: {source_user_id}\\n_ym_uid: {_ym_uid}",
            {
                "username": "Никита",
                "watch_minutes": 6.917,
                "view": 1784010241460,
                "viewTill": 1784010656476,
                "chatUserId": "bizon-42",
            },
        )
        self.assertNotIn("\\n", text)
        self.assertNotIn("Город:", text)
        self.assertNotIn("_ym_uid:", text)
        self.assertIn("Был минут: 7; с 14.07.2026 09:24 до 14.07.2026 09:30", text)
        self.assertIn("ID Bizon: bizon-42", text)

    def test_note_includes_chat_messages_when_available(self):
        text = router._format_note(
            "Имя: {username}\nСообщения и ответы в чате: {chat_messages_text}",
            {"username": "Анна", "chat_messages_text": "[00:52] 10\n[02:16] Мой вопрос"},
        )
        self.assertIn("Сообщения и ответы в чате: [00:52] 10", text)
        self.assertIn("[02:16] Мой вопрос", text)

    def test_round_robin_skips_inactive_and_rotates(self):
        users = ["10", "20", "30"]
        active = {"10", "30"}
        self.assertEqual(router._select_responsible(users, 0, active), "10")
        self.assertEqual(router._select_responsible(users, 1, active), "30")
        self.assertEqual(router._select_responsible(users, 2, active), "10")

    def test_both_time_branches_share_one_employee_pool(self):
        above = {"responsible_user_ids": ["10", "20", "30"], "min_minutes": 60}
        below = {"responsible_user_ids": ["10", "20", "30"], "max_minutes": 60}
        self.assertEqual(router._round_robin_pool_key(above), router._round_robin_pool_key(below))

    def test_active_users_support_amocrm_nested_rights(self):
        users = [
            {"id": 10, "rights": {"is_active": True}},
            {"id": 20, "rights": {"is_active": False}},
            {"id": 30, "is_active": False},
            {"id": 40},
        ]
        self.assertEqual(router._active_amo_user_ids(users), {"10", "40"})

    def test_amocrm_field_types_are_coerced(self):
        self.assertEqual(router._coerce_amo_field_value("75", "numeric"), 75)
        self.assertTrue(router._coerce_amo_field_value("true", "checkbox"))
        self.assertEqual(
            router._coerce_amo_field_value("2026-07-12T19:00:00", "date_time"),
            1783872000,
        )

    def test_constructor_maps_lead_and_contact_fields(self):
        attendance = {"watch_minutes": 75.5, "phone": "79990001122", "username": "Анна"}
        binding = {"field_mappings": [
            {"entity": "leads", "field_id": 10, "source": "watch_minutes"},
            {"entity": "contacts", "field_id": 20, "source": "phone"},
            {"entity": "contacts", "target": "name", "source": "username"},
        ]}
        lead_catalog = [{"id": 10, "type": "numeric", "code": None}]
        contact_catalog = [{"id": 20, "type": "text", "code": "PHONE"}]
        lead_values = asyncio.run(router._mapped_field_values(attendance, binding, "leads", lead_catalog))
        contact_values = asyncio.run(router._mapped_field_values(attendance, binding, "contacts", contact_catalog))
        self.assertEqual(lead_values, [{"field_id": 10, "values": [{"value": 75.5}]}])
        self.assertEqual(contact_values, [{"field_id": 20, "values": [{"value": "79990001122", "enum_code": "WORK"}]}])
        self.assertEqual(router._mapped_entity_name(attendance, binding, "contacts"), "Анна")

    def test_complex_bizon_value_is_serialized_for_text_field(self):
        self.assertEqual(router._scalar_for_amo({"answer": "да"}, "text"), '{"answer": "да"}')

    def test_merge_duplicate_preserves_owner_and_occupied_fields(self):
        calls = []
        async def fake_mapped(_attendance, _binding, entity, catalog=None):
            if entity == "leads":
                return [
                    {"field_id": 300, "values": [{"value": "новое"}]},
                    {"field_id": 301, "values": [{"value": "добавить"}]},
                ]
            return []
        async def fake_request(method, path, payload=None):
            calls.append((method, path, payload))
            if method == "GET":
                return {"_embedded": {"contacts": []}}, "", 200
            return {"id": 777}, "", 200
        original_mapped, original_request = router._mapped_field_values, router._amo_request
        router._mapped_field_values, router._amo_request = fake_mapped, fake_request
        try:
            result, error = asyncio.run(router._merge_empty_lead(
                {
                    "id": 777,
                    "name": "Существующая сделка",
                    "responsible_user_id": 999,
                    "pipeline_id": 10,
                    "status_id": 142,
                    "custom_fields_values": [{"field_id": 300, "values": [{"value": "занято"}]}],
                },
                {"username": "Анна"},
                {"field_mappings": []},
            ))
        finally:
            router._mapped_field_values, router._amo_request = original_mapped, original_request
        self.assertEqual(error, "")
        self.assertEqual(result["preserved"]["responsible_user_id"], 999)
        patch = next(payload for method, path, payload in calls if method == "PATCH" and "/leads/" in path)
        self.assertEqual(set(patch), {"custom_fields_values"})
        self.assertEqual([field["field_id"] for field in patch["custom_fields_values"]], [301])

    def test_bizon_catalog_has_unique_codes(self):
        codes = [item[0] for item in router.BIZON_FIELD_DEFINITIONS]
        self.assertEqual(len(codes), len(set(codes)))

    def test_create_payload_contains_lead_and_contact_mappings(self):
        calls = []
        async def fake_catalog(entity):
            if entity == "contacts":
                return [
                    {"id": 100, "type": "multitext", "code": "PHONE"},
                    {"id": 200, "type": "text", "code": None},
                ], ""
            return [{"id": 300, "type": "numeric", "code": None}], ""
        async def fake_request(method, path, payload=None):
            calls.append((method, path, payload))
            return [{"id": 777}], "", 200
        original_catalog, original_request = router._catalog, router._amo_request
        router._catalog, router._amo_request = fake_catalog, fake_request
        try:
            result, error = asyncio.run(router._create_lead(
                {"username": "Анна", "phone": "79990001122", "city": "Москва", "watch_minutes": 61},
                {
                    "pipeline_id": "10", "status_id": "20", "tags": ["Bizon"],
                    "field_mappings": [
                        {"entity": "leads", "field_id": 300, "source": "watch_minutes"},
                        {"entity": "contacts", "field_id": 200, "source": "city"},
                    ],
                },
                "30",
            ))
        finally:
            router._catalog, router._amo_request = original_catalog, original_request
        self.assertEqual(error, "")
        self.assertEqual(result["lead_id"], "777")
        lead = calls[0][2][0]
        self.assertEqual(lead["custom_fields_values"][0]["values"][0]["value"], 61)
        contact_fields = {item["field_id"] for item in lead["_embedded"]["contacts"][0]["custom_fields_values"]}
        self.assertEqual(contact_fields, {100, 200})

    def test_threshold_edges_and_missing_contact(self):
        base = {"watch_valid": True, "phone": "79990000000"}
        self.assertEqual(router._qualification_reason({**base, "watch_minutes": 59.999}, 60), "below_minimum")
        self.assertEqual(router._qualification_reason({**base, "watch_minutes": 60}, 60), "eligible")
        self.assertEqual(router._qualification_reason({**base, "watch_minutes": 79.999}, 80), "below_minimum")
        self.assertEqual(router._qualification_reason({**base, "watch_minutes": 80}, 80), "eligible")
        self.assertEqual(router._qualification_reason({"watch_valid": True, "watch_minutes": 80}, 80), "missing_contact")
        self.assertEqual(router._qualification_reason({"watch_valid": False, "watch_minutes": None}, 60), "invalid_duration")


if __name__ == "__main__":
    unittest.main()
